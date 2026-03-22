import pandas as pd


class value_quality_hold_strategy:
    def __init__(
        self,
        display_name="value_quality_hold",
        trial_position=0.3,
        add_position=0.6,
        core_position=0.9,
        min_health_score=0.58,
        min_peer_score=0.52,
        min_quality_score=45,
        min_roe=8,
        min_profit_yoy=-10,
        min_revenue_yoy=-5,
        cheap_percentile_floor=70,
        deep_cheap_percentile_floor=85,
        very_expensive_percentile_ceiling=15,
        min_dividend_yield=1.5,
        rebalance_cooldown=120,
    ):
        self.display_name = display_name
        self.trial_position = trial_position
        self.add_position = add_position
        self.core_position = core_position
        self.min_health_score = min_health_score
        self.min_peer_score = min_peer_score
        self.min_quality_score = min_quality_score
        self.min_roe = min_roe
        self.min_profit_yoy = min_profit_yoy
        self.min_revenue_yoy = min_revenue_yoy
        self.cheap_percentile_floor = cheap_percentile_floor
        self.deep_cheap_percentile_floor = deep_cheap_percentile_floor
        self.very_expensive_percentile_ceiling = very_expensive_percentile_ceiling
        self.min_dividend_yield = min_dividend_yield
        self.rebalance_cooldown = rebalance_cooldown

    @staticmethod
    def _safe_float(value, fallback=0.0):
        if pd.isna(value):
            return float(fallback)
        return float(value)

    @staticmethod
    def _industry_profile(industry_name):
        industry_name = str(industry_name or "")
        if industry_name in {"白酒"}:
            return "liquor"
        if industry_name in {"中成药", "化学制药", "生物制药", "医疗保健", "医药商业"}:
            return "pharma"
        if industry_name in {"火力发电", "新型电力", "供气供热", "银行"}:
            return "utility"
        return "general"

    def trading_strategy(self, df):
        df = df.copy()
        df["ma20"] = df["close"].rolling(window=20, min_periods=10).mean()
        df["ma60"] = df["close"].rolling(window=60, min_periods=20).mean()
        df["ma120"] = df["close"].rolling(window=120, min_periods=40).mean()
        df["ma20_slope_5"] = df["ma20"] - df["ma20"].shift(5)
        df["ret_5"] = df["close"].pct_change(5, fill_method=None)
        df["ret_20"] = df["close"].pct_change(20, fill_method=None)
        df["low_20"] = df["close"].rolling(window=20, min_periods=10).min().shift(1)
        df["high_60"] = df["close"].rolling(window=60, min_periods=20).max().shift(1)
        if "sh_index_close" in df.columns:
            df["market_ret_20"] = df["sh_index_close"].pct_change(20, fill_method=None)
            df["market_ret_5"] = df["sh_index_close"].pct_change(5, fill_method=None)
        else:
            df["market_ret_20"] = pd.NA
            df["market_ret_5"] = pd.NA
        df["relative_strength_20"] = df["ret_20"] - df["market_ret_20"]
        df["relative_strength_5"] = df["ret_5"] - df["market_ret_5"]
        df["signal"] = 0
        df["reason"] = "观望"
        df["target_position"] = pd.NA
        df["value_score"] = pd.NA
        df["overvalue_score"] = pd.NA

        current_target = 0.0
        warmup = 20
        last_rebalance_index = -10_000

        for i in range(warmup, len(df) - 1):
            row = df.iloc[i]
            next_index = i + 1

            executed_signal = row.get("signal", 0)
            executed_target = row.get("target_position", pd.NA)
            if executed_signal == 1 and pd.notna(executed_target):
                current_target = float(executed_target or 0.0)
            elif executed_signal == -1 and pd.notna(executed_target):
                current_target = float(executed_target or 0.0)

            pb_pct = pd.to_numeric(pd.Series([row.get("value_pb_percentile_3y", pd.NA)]), errors="coerce").iloc[0]
            pe_pct = pd.to_numeric(pd.Series([row.get("value_pe_percentile_3y", pd.NA)]), errors="coerce").iloc[0]
            health_score = pd.to_numeric(pd.Series([row.get("financial_health_score", pd.NA)]), errors="coerce").iloc[0]
            peer_score = pd.to_numeric(pd.Series([row.get("peer_financial_score", pd.NA)]), errors="coerce").iloc[0]
            quality_score = pd.to_numeric(pd.Series([row.get("financial_quality_score", pd.NA)]), errors="coerce").iloc[0]
            roe = pd.to_numeric(pd.Series([row.get("financial_roe", pd.NA)]), errors="coerce").iloc[0]
            profit_yoy = pd.to_numeric(pd.Series([row.get("financial_profit_yoy", pd.NA)]), errors="coerce").iloc[0]
            revenue_yoy = pd.to_numeric(pd.Series([row.get("financial_revenue_yoy", pd.NA)]), errors="coerce").iloc[0]
            dividend_yield = pd.to_numeric(pd.Series([row.get("value_dv_ttm", pd.NA)]), errors="coerce").iloc[0]
            industry_name = row.get("stock_industry", "")
            industry_profile = self._industry_profile(industry_name)
            close_price = pd.to_numeric(pd.Series([row.get("close", pd.NA)]), errors="coerce").iloc[0]
            ma20 = pd.to_numeric(pd.Series([row.get("ma20", pd.NA)]), errors="coerce").iloc[0]
            ma60 = pd.to_numeric(pd.Series([row.get("ma60", pd.NA)]), errors="coerce").iloc[0]
            ma120 = pd.to_numeric(pd.Series([row.get("ma120", pd.NA)]), errors="coerce").iloc[0]
            ma20_slope_5 = pd.to_numeric(pd.Series([row.get("ma20_slope_5", pd.NA)]), errors="coerce").iloc[0]
            ret5 = pd.to_numeric(pd.Series([row.get("ret_5", pd.NA)]), errors="coerce").iloc[0]
            ret20 = pd.to_numeric(pd.Series([row.get("ret_20", pd.NA)]), errors="coerce").iloc[0]
            rs20 = pd.to_numeric(pd.Series([row.get("relative_strength_20", pd.NA)]), errors="coerce").iloc[0]
            rs5 = pd.to_numeric(pd.Series([row.get("relative_strength_5", pd.NA)]), errors="coerce").iloc[0]
            low20 = pd.to_numeric(pd.Series([row.get("low_20", pd.NA)]), errors="coerce").iloc[0]
            high60 = pd.to_numeric(pd.Series([row.get("high_60", pd.NA)]), errors="coerce").iloc[0]
            industry_flow_pct = pd.to_numeric(
                pd.Series([row.get("industry_flow_pct_change", pd.NA)]), errors="coerce"
            ).iloc[0]
            industry_flow_net_rate = pd.to_numeric(
                pd.Series([row.get("industry_flow_net_amount_rate", pd.NA)]), errors="coerce"
            ).iloc[0]

            cheap_scores = []
            expensive_scores = []
            for metric in [pb_pct, pe_pct]:
                if pd.notna(metric):
                    cheap_scores.append(float(metric) / 100.0)
                    expensive_scores.append((100.0 - float(metric)) / 100.0)

            if not cheap_scores:
                continue

            cheap_score = sum(cheap_scores) / len(cheap_scores)
            expensive_score = sum(expensive_scores) / len(expensive_scores)

            quality_norms = []
            if pd.notna(health_score):
                quality_norms.append(float(health_score))
            if pd.notna(peer_score):
                quality_norms.append(float(peer_score))
            if pd.notna(quality_score):
                quality_norms.append(min(1.0, max(0.0, float(quality_score) / 100.0)))
            if pd.notna(roe):
                quality_norms.append(min(1.0, max(0.0, (float(roe) - 5.0) / 15.0)))
            quality_norm = sum(quality_norms) / len(quality_norms) if quality_norms else pd.NA

            dividend_norm = pd.NA
            if pd.notna(dividend_yield):
                dividend_norm = min(1.0, max(0.0, float(dividend_yield) / 6.0))

            value_score_items = [cheap_score * 0.55]
            if pd.notna(quality_norm):
                value_score_items.append(float(quality_norm) * 0.35)
            if pd.notna(dividend_norm):
                value_score_items.append(float(dividend_norm) * 0.10)
            value_score = sum(value_score_items)
            overvalue_score = expensive_score
            df.loc[i, "value_score"] = value_score
            df.loc[i, "overvalue_score"] = overvalue_score

            financial_ok = (
                (pd.isna(health_score) or health_score >= self.min_health_score)
                and (pd.isna(peer_score) or peer_score >= self.min_peer_score)
                and (pd.isna(quality_score) or quality_score >= self.min_quality_score)
                and (pd.isna(roe) or roe >= self.min_roe)
                and (pd.isna(profit_yoy) or profit_yoy >= self.min_profit_yoy)
                and (pd.isna(revenue_yoy) or revenue_yoy >= self.min_revenue_yoy)
            )
            deep_cheap_ok = (
                any(pd.notna(metric) and metric >= self.deep_cheap_percentile_floor for metric in [pb_pct, pe_pct])
                or cheap_score >= 0.82
            )
            cheap_ok = (
                any(pd.notna(metric) and metric >= self.cheap_percentile_floor for metric in [pb_pct, pe_pct])
                or cheap_score >= 0.70
            )
            very_expensive_ok = (
                all(pd.notna(metric) and metric <= self.very_expensive_percentile_ceiling for metric in [pb_pct, pe_pct] if pd.notna(metric))
                and len([metric for metric in [pb_pct, pe_pct] if pd.notna(metric)]) >= 1
            )
            dividend_support_ok = pd.isna(dividend_yield) or dividend_yield >= self.min_dividend_yield
            not_falling_fast = pd.isna(ret20) or ret20 >= -0.15
            above_medium_trend = (
                (pd.notna(close_price) and pd.notna(ma60) and close_price >= ma60 * 0.95)
                or (pd.notna(close_price) and pd.notna(ma120) and close_price >= ma120 * 0.90)
            )
            rebound_from_low = (
                pd.notna(close_price)
                and pd.notna(low20)
                and low20 > 0
                and close_price >= low20 * 1.05
            )
            not_far_from_high = (
                pd.notna(close_price)
                and pd.notna(high60)
                and high60 > 0
                and close_price / high60 >= 0.72
            )
            ma20_support = pd.notna(close_price) and pd.notna(ma20) and close_price >= ma20 * 0.99
            medium_trend_recovered = pd.notna(close_price) and pd.notna(ma60) and close_price >= ma60
            long_trend_recovered = pd.notna(close_price) and pd.notna(ma120) and close_price >= ma120 * 0.97
            short_momentum_turn = (pd.notna(ret5) and ret5 >= 0) or (pd.notna(ma20_slope_5) and ma20_slope_5 >= 0)
            short_term_recovered = pd.notna(ret20) and ret20 >= -0.03
            medium_momentum_ok = pd.isna(ret20) or ret20 >= 0.0
            relative_strength_ok = pd.isna(rs20) or rs20 >= -0.05
            short_relative_strength_ok = pd.isna(rs5) or rs5 >= -0.03
            industry_flow_ok = (
                (pd.isna(industry_flow_pct) or industry_flow_pct >= -1.5)
                and (pd.isna(industry_flow_net_rate) or industry_flow_net_rate >= -8)
            )

            if industry_profile == "liquor":
                timing_ok = (
                    not_falling_fast
                    and ma20_support
                    and medium_trend_recovered
                    and short_momentum_turn
                    and (pd.notna(ret20) and ret20 >= 0.03)
                    and not_far_from_high
                    and (pd.notna(rs20) and rs20 >= 0)
                    and short_relative_strength_ok
                    and industry_flow_ok
                )
                add_timing_ok = timing_ok and long_trend_recovered and (pd.notna(rs5) and rs5 >= 0)
                core_timing_ok = add_timing_ok and medium_momentum_ok and close_price >= ma20 * 1.01
            elif industry_profile == "pharma":
                timing_ok = (
                    not_falling_fast
                    and (ma20_support or rebound_from_low)
                    and (short_term_recovered or medium_trend_recovered)
                    and short_momentum_turn
                    and relative_strength_ok
                    and industry_flow_ok
                )
                add_timing_ok = timing_ok and medium_trend_recovered and not_far_from_high and short_relative_strength_ok
                core_timing_ok = add_timing_ok and medium_momentum_ok and relative_strength_ok
            elif industry_profile == "utility":
                timing_ok = (
                    not_falling_fast
                    and (rebound_from_low or above_medium_trend or ma20_support)
                    and short_momentum_turn
                )
                add_timing_ok = timing_ok and (medium_trend_recovered or long_trend_recovered) and relative_strength_ok
                core_timing_ok = add_timing_ok and (medium_momentum_ok or dividend_support_ok)
            else:
                timing_ok = (
                    not_falling_fast
                    and (ma20_support or rebound_from_low or above_medium_trend)
                    and (short_term_recovered or medium_trend_recovered)
                    and short_momentum_turn
                    and relative_strength_ok
                )
                add_timing_ok = (
                    timing_ok
                    and (medium_trend_recovered or long_trend_recovered)
                    and not_far_from_high
                    and short_relative_strength_ok
                )
                core_timing_ok = (
                    add_timing_ok
                    and medium_momentum_ok
                    and ((pd.notna(ma20) and pd.notna(ma60) and ma20 >= ma60 * 0.98) or long_trend_recovered)
                )
            cooldown_ready = (i - last_rebalance_index) >= self.rebalance_cooldown

            next_target = current_target
            reason = "估值和财报未形成新的低估/高估拐点"

            if not financial_ok and current_target > 0:
                next_target = 0.0
                reason = "财报健康度明显转弱，先退出等待下一次低估机会"
            elif very_expensive_ok and current_target > 0:
                next_target = 0.0
                reason = "估值已经进入极度高估区，结束这轮价值持有"
            elif cooldown_ready and current_target <= 0 and financial_ok and cheap_ok and dividend_support_ok and timing_ok:
                next_target = self.core_position if deep_cheap_ok and core_timing_ok else self.add_position
                if deep_cheap_ok and core_timing_ok:
                    reason = "估值极低且财报健康，低点一次建立核心长期仓位"
                else:
                    reason = "估值处在历史低位、财报健康且价格开始企稳，建立长期持有仓位"

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
        current_target = self._safe_float(last_targets.iloc[-1], 0.0) if not last_targets.empty else 0.0
        next_row.loc[:, "target_position"] = current_target
        preview_df = pd.concat([trade_df, next_row], ignore_index=True)
        preview_df = self.trading_strategy(preview_df)

        last_row = preview_df.iloc[-2]
        future_row = preview_df.iloc[-1]
        next_target = self._safe_float(future_row.get("target_position", pd.NA), current_target)

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
            "value_score": last_row.get("value_score", pd.NA),
            "overvalue_score": last_row.get("overvalue_score", pd.NA),
        }
