import pandas as pd


class dividend_hold_strategy:
    def __init__(
        self,
        display_name="dividend_hold",
        base_position=0.5,
        core_position=0.9,
        min_dividend_yield=3.0,
        strong_dividend_yield=4.5,
        min_dividend_stability=0.8,
        min_health_score=0.45,
        min_quality_score=50,
        min_roe=8,
        min_profit_yoy=-10,
        min_revenue_yoy=-5,
        exit_pb_ceiling=12,
        buy_pb_floor=65,
        deep_buy_pb_floor=82,
        rebalance_cooldown=90,
    ):
        self.display_name = display_name
        self.base_position = base_position
        self.core_position = core_position
        self.min_dividend_yield = min_dividend_yield
        self.strong_dividend_yield = strong_dividend_yield
        self.min_dividend_stability = min_dividend_stability
        self.min_health_score = min_health_score
        self.min_quality_score = min_quality_score
        self.min_roe = min_roe
        self.min_profit_yoy = min_profit_yoy
        self.min_revenue_yoy = min_revenue_yoy
        self.exit_pb_ceiling = exit_pb_ceiling
        self.buy_pb_floor = buy_pb_floor
        self.deep_buy_pb_floor = deep_buy_pb_floor
        self.rebalance_cooldown = rebalance_cooldown

    @staticmethod
    def _to_num(value):
        series = pd.to_numeric(pd.Series([value]), errors="coerce")
        return series.iloc[0]

    @staticmethod
    def _safe_float(value, fallback=0.0):
        if pd.isna(value):
            return float(fallback)
        return float(value)

    def trading_strategy(self, df):
        df = df.copy()
        df["ma20"] = df["close"].rolling(window=20, min_periods=10).mean()
        df["ma60"] = df["close"].rolling(window=60, min_periods=20).mean()
        df["ma120"] = df["close"].rolling(window=120, min_periods=40).mean()
        df["ret_20"] = df["close"].pct_change(20, fill_method=None)
        if "sh_index_close" in df.columns:
            df["market_ret_20"] = df["sh_index_close"].pct_change(20, fill_method=None)
            df["relative_strength_20"] = df["ret_20"] - df["market_ret_20"]
        else:
            df["relative_strength_20"] = pd.NA

        df["signal"] = 0
        df["reason"] = "观望"
        df["target_position"] = pd.NA
        df["dividend_score"] = pd.NA

        current_target = 0.0
        warmup = 60
        last_rebalance_index = -10_000
        for i in range(warmup, len(df) - 1):
            row = df.iloc[i]
            next_index = i + 1

            executed_signal = row.get("signal", 0)
            executed_target = row.get("target_position", pd.NA)
            if pd.notna(executed_target) and executed_signal in {1, -1}:
                current_target = self._safe_float(executed_target, 0.0)

            dividend_yield = self._to_num(row.get("value_dv_ttm", pd.NA))
            dividend_stability = self._to_num(row.get("value_dividend_stability", pd.NA))
            health_score = self._to_num(row.get("financial_health_score", pd.NA))
            quality_score = self._to_num(row.get("financial_quality_score", pd.NA))
            roe = self._to_num(row.get("financial_roe", pd.NA))
            profit_yoy = self._to_num(row.get("financial_profit_yoy", pd.NA))
            revenue_yoy = self._to_num(row.get("financial_revenue_yoy", pd.NA))
            pb_pct = self._to_num(row.get("value_pb_percentile_3y", pd.NA))
            ret20 = self._to_num(row.get("ret_20", pd.NA))
            rs20 = self._to_num(row.get("relative_strength_20", pd.NA))
            close_price = self._to_num(row.get("close", pd.NA))
            ma60 = self._to_num(row.get("ma60", pd.NA))
            ma120 = self._to_num(row.get("ma120", pd.NA))

            score = 0.0
            if pd.notna(dividend_yield):
                score += min(40.0, max(0.0, dividend_yield) * 6.0)
            if pd.notna(dividend_stability):
                score += float(dividend_stability) * 25.0
            if pd.notna(health_score):
                score += float(health_score) * 20.0
            if pd.notna(quality_score):
                score += min(15.0, max(0.0, quality_score) / 100.0 * 15.0)
            df.loc[i, "dividend_score"] = round(score, 2)

            dividend_ok = (
                pd.notna(dividend_yield)
                and dividend_yield >= self.min_dividend_yield
                and (pd.isna(dividend_stability) or dividend_stability >= self.min_dividend_stability)
            )
            strong_dividend_ok = pd.notna(dividend_yield) and dividend_yield >= self.strong_dividend_yield
            financial_ok = (
                (pd.isna(health_score) or health_score >= self.min_health_score)
                and (pd.isna(quality_score) or quality_score >= self.min_quality_score)
                and (pd.isna(roe) or roe >= self.min_roe)
                and (pd.isna(profit_yoy) or profit_yoy >= self.min_profit_yoy)
                and (pd.isna(revenue_yoy) or revenue_yoy >= self.min_revenue_yoy)
            )
            trend_ok = (
                (pd.isna(ma120) or close_price >= ma120 * 0.94)
                and (pd.isna(ma60) or close_price >= ma60 * 0.96)
                and (pd.isna(ret20) or ret20 >= -0.08)
                and (pd.isna(rs20) or rs20 >= -0.05)
            )
            expensive_exit = pd.notna(pb_pct) and pb_pct <= self.exit_pb_ceiling
            cheap_ok = pd.notna(pb_pct) and pb_pct >= self.buy_pb_floor
            deep_cheap_ok = pd.notna(pb_pct) and pb_pct >= self.deep_buy_pb_floor
            financial_warning = (
                (pd.notna(profit_yoy) and profit_yoy <= -20)
                or (pd.notna(revenue_yoy) and revenue_yoy <= -10)
                or (pd.notna(roe) and roe <= 5)
            )
            cooldown_ready = (i - last_rebalance_index) >= self.rebalance_cooldown
            low_point_ready = (
                trend_ok
                and (
                    (pd.notna(close_price) and pd.notna(ma60) and close_price <= ma60 * 1.03)
                    or (pd.notna(ret20) and ret20 <= 0.03)
                )
            )

            next_target = current_target
            reason = "继续持有等待分红和慢牛修复"

            if current_target > 0 and (financial_warning or expensive_exit):
                next_target = 0.0
                reason = "财报明显转弱或估值极度高估，结束这轮红利持有"
            elif cooldown_ready and current_target <= 0 and dividend_ok and financial_ok and cheap_ok and low_point_ready:
                next_target = self.core_position if strong_dividend_ok and deep_cheap_ok else self.base_position
                reason = "低估值高股息且财报健康，低位建立长期红利仓"

            if next_target > current_target + 1e-8:
                df.loc[next_index, "signal"] = 1
                df.loc[next_index, "target_position"] = next_target
                df.loc[next_index, "reason"] = reason
                current_target = next_target
                last_rebalance_index = i
            elif next_target < current_target - 1e-8:
                df.loc[next_index, "signal"] = -1
                df.loc[next_index, "target_position"] = next_target
                df.loc[next_index, "reason"] = reason
                current_target = next_target
                last_rebalance_index = i

        return df

    def predict_next_signal(self, df):
        trade_df = df[df["trade_date"].notna()].copy().reset_index(drop=True)
        if len(trade_df) < 2:
            raise ValueError("数据不足，无法预测下一交易日信号")

        next_row = trade_df.tail(1).copy()
        next_row.loc[:, "trade_date"] = f"{trade_df['trade_date'].iloc[-1]}_NEXT"
        next_row.loc[:, "signal"] = 0
        next_row.loc[:, "reason"] = "观望"
        last_targets = trade_df["target_position"].dropna() if "target_position" in trade_df.columns else pd.Series(dtype="float64")
        current_target = float(last_targets.iloc[-1]) if not last_targets.empty else 0.0
        next_row.loc[:, "target_position"] = current_target
        preview_df = pd.concat([trade_df, next_row], ignore_index=True)
        preview_df = self.trading_strategy(preview_df)

        last_row = preview_df.iloc[-2]
        future_row = preview_df.iloc[-1]
        next_target = self._safe_float(future_row.get("target_position", current_target), current_target)

        if next_target > current_target + 1e-8:
            signal = "加仓" if current_target > 0 else "建仓"
        elif next_target < current_target - 1e-8:
            signal = "减仓" if next_target > 0 else "清仓"
        elif current_target > 0:
            signal = "持仓"
        else:
            signal = "观望"

        return {
            "signal": signal,
            "reason": future_row["reason"],
            "last_trade_date": last_row["trade_date"],
            "target_position": next_target,
            "dividend_score": last_row.get("dividend_score", pd.NA),
        }
