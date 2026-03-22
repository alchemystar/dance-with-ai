import pandas as pd

from bank_t_strategy import calculate_macd, calculate_rsi
from macd_with_regime_filter import (
    calculate_market_activity_features,
    calculate_market_regime_features,
)


class state_owned_dividend_strategy:
    def __init__(
        self,
        base_position=0.7,
        add_step=0.2,
        trim_step=0.3,
        max_position=0.9,
        min_position=0.0,
        pb_buy_floor=20,
        pb_strong_buy_floor=35,
        pb_trim_ceiling=10,
        min_dividend_yield=3.8,
        min_dividend_stability=0.8,
        min_financial_health_score=0.6,
        min_financial_quality_score=15,
        min_profit_yoy=-15,
        min_revenue_yoy=-8,
        min_roe=7,
        weak_profit_yoy_exit=-25,
        weak_revenue_yoy_exit=-12,
        weak_roe_exit=5,
        market_flow_floor=-4.0,
        rebalance_cooldown=90,
        allow_recovery_entry=False,
        allow_add_on_weakness=True,
        allow_partial_trim=True,
        allow_watch_downgrade=True,
        exit_on_market_breakdown=True,
        exit_on_valuation_extreme=False,
        display_name=None,
    ):
        self.base_position = base_position
        self.add_step = add_step
        self.trim_step = trim_step
        self.max_position = max_position
        self.min_position = min_position
        self.pb_buy_floor = pb_buy_floor
        self.pb_strong_buy_floor = pb_strong_buy_floor
        self.pb_trim_ceiling = pb_trim_ceiling
        self.min_dividend_yield = min_dividend_yield
        self.min_dividend_stability = min_dividend_stability
        self.min_financial_health_score = min_financial_health_score
        self.min_financial_quality_score = min_financial_quality_score
        self.min_profit_yoy = min_profit_yoy
        self.min_revenue_yoy = min_revenue_yoy
        self.min_roe = min_roe
        self.weak_profit_yoy_exit = weak_profit_yoy_exit
        self.weak_revenue_yoy_exit = weak_revenue_yoy_exit
        self.weak_roe_exit = weak_roe_exit
        self.market_flow_floor = market_flow_floor
        self.rebalance_cooldown = rebalance_cooldown
        self.allow_recovery_entry = allow_recovery_entry
        self.allow_add_on_weakness = allow_add_on_weakness
        self.allow_partial_trim = allow_partial_trim
        self.allow_watch_downgrade = allow_watch_downgrade
        self.exit_on_market_breakdown = exit_on_market_breakdown
        self.exit_on_valuation_extreme = exit_on_valuation_extreme
        self.display_name = display_name or self.__class__.__name__

    def trading_strategy(self, df):
        df = df.copy()
        df = calculate_macd(df)
        df = calculate_rsi(df)
        df = calculate_market_regime_features(df, "sh_index")
        df = calculate_market_regime_features(df, "csi300")
        df = calculate_market_activity_features(df)

        df["ma20"] = df["close"].rolling(window=20).mean()
        df["ma60"] = df["close"].rolling(window=60).mean()
        df["ma120"] = df["close"].rolling(window=120).mean()
        df["ma250"] = df["close"].rolling(window=250).mean()
        df["std60"] = df["close"].rolling(window=60).std()
        df["zscore60"] = (df["close"] - df["ma60"]) / df["std60"].replace(0, pd.NA)
        df["rolling_high_120"] = df["close"].rolling(window=120).max()
        df["drawdown_from_high_120"] = (
            df["close"] / df["rolling_high_120"].replace(0, pd.NA) - 1
        ) * 100

        df["signal"] = 0
        df["reason"] = "观望"
        df["target_position"] = pd.NA
        df["market_regime_score"] = pd.NA
        df["financial_health_score"] = pd.NA

        warmup = 120
        last_rebalance_index = -10_000
        current_target = 0.0
        for i in range(warmup, len(df) - 1):
            prev_row = df.iloc[i - 1]
            row = df.iloc[i]
            next_index = i + 1
            executed_signal = row.get("signal", 0)
            executed_target = row.get("target_position", pd.NA)
            if executed_signal in {1, -1} and pd.notna(executed_target):
                current_target = float(executed_target)
            next_target = current_target
            reason = "继续持有等待分红和慢牛修复"

            market_checks = []
            if pd.notna(row.get("sh_index_close", pd.NA)) and pd.notna(row.get("sh_index_ema_slow", pd.NA)):
                market_checks.append(row.get("sh_index_close", pd.NA) >= row.get("sh_index_ema_slow", pd.NA) * 0.985)
            if pd.notna(row.get("csi300_close", pd.NA)) and pd.notna(row.get("csi300_ema_slow", pd.NA)):
                market_checks.append(row.get("csi300_close", pd.NA) >= row.get("csi300_ema_slow", pd.NA) * 0.985)
            if pd.notna(row.get("sh_index_ret_5", pd.NA)):
                market_checks.append(row.get("sh_index_ret_5", pd.NA) > -0.04)
            if pd.notna(row.get("csi300_ret_5", pd.NA)):
                market_checks.append(row.get("csi300_ret_5", pd.NA) > -0.05)
            if pd.notna(row.get("market_amount_ratio", pd.NA)):
                market_checks.append(row.get("market_amount_ratio", pd.NA) >= 0.85)
            if pd.notna(row.get("market_net_amount_rate", pd.NA)):
                market_checks.append(row.get("market_net_amount_rate", pd.NA) > self.market_flow_floor)
            market_score = sum(1 for item in market_checks if item)

            pb_percentile = row.get("bank_pb_percentile_3y", pd.NA)
            dividend_yield = row.get("bank_dv_ttm", pd.NA)
            dividend_stability = row.get("bank_dividend_stability", pd.NA)

            pb_buy_ok = pd.notna(pb_percentile) and pb_percentile >= self.pb_buy_floor
            pb_strong_buy_ok = pd.notna(pb_percentile) and pb_percentile >= self.pb_strong_buy_floor
            valuation_expensive = pd.notna(pb_percentile) and pb_percentile <= self.pb_trim_ceiling
            dividend_yield_ok = pd.notna(dividend_yield) and dividend_yield >= self.min_dividend_yield
            dividend_stability_ok = (
                pd.isna(dividend_stability)
                or dividend_stability >= self.min_dividend_stability
            )
            value_support_ok = (
                (pb_buy_ok and dividend_stability_ok)
                or (dividend_yield_ok and dividend_stability_ok)
            )

            financial_checks = []
            if pd.notna(row.get("financial_quality_score", pd.NA)):
                financial_checks.append(
                    row.get("financial_quality_score", pd.NA) >= self.min_financial_quality_score
                )
            if pd.notna(row.get("financial_profit_yoy", pd.NA)):
                financial_checks.append(row.get("financial_profit_yoy", pd.NA) >= self.min_profit_yoy)
            if pd.notna(row.get("financial_revenue_yoy", pd.NA)):
                financial_checks.append(row.get("financial_revenue_yoy", pd.NA) >= self.min_revenue_yoy)
            if pd.notna(row.get("financial_roe", pd.NA)):
                financial_checks.append(row.get("financial_roe", pd.NA) >= self.min_roe)
            if pd.notna(row.get("financial_eps", pd.NA)):
                financial_checks.append(row.get("financial_eps", pd.NA) > 0)
            if pd.notna(row.get("financial_bps", pd.NA)):
                financial_checks.append(row.get("financial_bps", pd.NA) > 0)
            if financial_checks:
                financial_health_score = sum(1 for item in financial_checks if item) / len(financial_checks)
            else:
                financial_health_score = 1.0
            financial_support_ok = financial_health_score >= self.min_financial_health_score
            financial_warning = (
                (
                    pd.notna(row.get("financial_profit_yoy", pd.NA))
                    and row.get("financial_profit_yoy", pd.NA) <= self.weak_profit_yoy_exit
                )
                or (
                    pd.notna(row.get("financial_revenue_yoy", pd.NA))
                    and row.get("financial_revenue_yoy", pd.NA) <= self.weak_revenue_yoy_exit
                )
                or (
                    pd.notna(row.get("financial_roe", pd.NA))
                    and row.get("financial_roe", pd.NA) <= self.weak_roe_exit
                )
            )

            trend_stable = (
                pd.notna(row["ma120"])
                and row["close"] >= row["ma120"] * 0.96
                and pd.notna(row["ma60"])
                and row["ma60"] >= row["ma120"] * 0.98
            )
            trend_recovery = (
                pd.notna(row["ma20"])
                and pd.notna(row["ma60"])
                and row["close"] >= row["ma20"] * 0.99
                and row["ma20"] >= row["ma60"] * 0.99
            )
            pullback_buy = (
                pd.notna(row["zscore60"])
                and row["zscore60"] <= -0.8
                and pd.notna(row["rsi"])
                and row["rsi"] <= 45
                and trend_stable
            )
            deep_value_buy = (
                pb_strong_buy_ok
                and value_support_ok
                and financial_support_ok
                and market_score >= 1
                and (
                    trend_stable
                    or trend_recovery
                    or (
                        pd.notna(row.get("drawdown_from_high_120", pd.NA))
                        and row.get("drawdown_from_high_120", pd.NA) <= -10
                        and trend_stable
                    )
                )
            )
            add_on_weakness = (
                pb_strong_buy_ok
                and value_support_ok
                and financial_support_ok
                and market_score >= 2
                and pullback_buy
                and row["close"] >= row["ma20"] * 0.98
            )
            market_breakdown = (
                market_score <= 0
                and pd.notna(row["ma120"])
                and row["close"] < row["ma120"] * 0.90
            )
            defensive_exit = financial_warning or (self.exit_on_market_breakdown and market_breakdown)
            trim_signal = (
                current_target > self.base_position
                and valuation_expensive
                and (
                    (pd.notna(row["rsi"]) and row["rsi"] >= 72)
                    or (
                        pd.notna(row["drawdown_from_high_120"])
                        and row["drawdown_from_high_120"] >= -0.5
                        and row["close"] < row["ma20"] * 0.995
                    )
                )
            )
            cooldown_ready = (i - last_rebalance_index) >= self.rebalance_cooldown

            if defensive_exit and current_target > 0:
                next_target = self.min_position if (financial_warning or not self.allow_watch_downgrade) else max(self.min_position, self.base_position * 0.5)
                reason = "财报明显转弱或保护条件被破坏，结束这轮红利持有"
            elif cooldown_ready and current_target <= 0 and deep_value_buy:
                next_target = min(self.max_position, self.base_position)
                reason = "低PB高股息且财报稳定，适合建立红利底仓长期持有"
            elif (
                cooldown_ready
                and current_target <= 0
                and value_support_ok
                and financial_support_ok
                and market_score >= 1
                and (trend_stable or (self.allow_recovery_entry and trend_recovery))
            ):
                next_target = min(self.max_position, self.base_position * 0.7)
                reason = "估值和股息进入可配置区，先建立观察底仓"
            elif cooldown_ready and current_target < self.base_position and value_support_ok and financial_support_ok and market_score >= 2 and trend_recovery:
                next_target = min(self.max_position, self.base_position)
                reason = "估值与财报支撑仍在，回补到标准红利底仓"
            elif self.allow_add_on_weakness and cooldown_ready and current_target < self.max_position and add_on_weakness and current_target <= self.base_position:
                next_target = min(self.max_position, current_target + self.add_step)
                reason = "国有大行处在深价值区，分批加到核心红利仓"
            elif self.allow_partial_trim and cooldown_ready and trim_signal:
                next_target = max(self.base_position, current_target - self.trim_step)
                reason = "估值明显修复且短线过热，减仓锁定一部分收益"
            elif self.exit_on_valuation_extreme and current_target > 0 and valuation_expensive:
                next_target = self.min_position
                reason = "估值进入极度高估区，结束这轮银行红利持有"
            elif self.allow_watch_downgrade and cooldown_ready and current_target > 0 and not value_support_ok and market_score <= 1 and row["close"] < row["ma60"] * 0.98:
                next_target = max(self.min_position, min(current_target, self.base_position * 0.5))
                reason = "估值和市场保护都在减弱，先把仓位降回观察档"

            df.loc[next_index, "market_regime_score"] = market_score
            df.loc[next_index, "financial_health_score"] = financial_health_score
            if next_target > current_target + 1e-8:
                df.loc[next_index, "signal"] = 1
                df.loc[next_index, "target_position"] = next_target
                last_rebalance_index = i
                current_target = next_target
            elif next_target < current_target - 1e-8:
                df.loc[next_index, "signal"] = -1
                df.loc[next_index, "target_position"] = next_target
                last_rebalance_index = i
                current_target = next_target
            else:
                df.loc[next_index, "signal"] = 0
            df.loc[next_index, "reason"] = reason

        return df

    def predict_next_signal(self, df):
        trade_df = df[df["trade_date"].notna()].copy().reset_index(drop=True)
        if len(trade_df) < 2:
            raise ValueError("数据不足，无法预测下一交易日信号")

        next_row = trade_df.tail(1).copy()
        next_row.loc[:, "trade_date"] = f"{trade_df['trade_date'].iloc[-1]}_NEXT"
        next_row.loc[:, "signal"] = 0
        next_row.loc[:, "reason"] = "观望"
        last_targets = trade_df["target_position"].dropna()
        current_target = float(last_targets.iloc[-1]) if not last_targets.empty else 0.0
        next_row.loc[:, "target_position"] = current_target
        preview_df = pd.concat([trade_df, next_row], ignore_index=True)
        preview_df = self.trading_strategy(preview_df)

        last_row = preview_df.iloc[-2]
        future_row = preview_df.iloc[-1]
        last_targets = preview_df.iloc[:-1]["target_position"].dropna()
        current_target = float(last_targets.iloc[-1]) if not last_targets.empty else 0.0
        future_target = future_row.get("target_position", pd.NA)
        next_target = float(future_target) if pd.notna(future_target) else current_target

        if next_target > current_target + 1e-8:
            signal = "加仓" if current_target > 0 else "建底仓"
        elif next_target < current_target - 1e-8:
            signal = "减仓" if next_target > 0 else "清仓"
        elif current_target > 0:
            signal = "持仓"
        else:
            signal = "观望"

        return {
            "signal": signal,
            "reason": future_row["reason"],
            "dif": last_row["dif"],
            "dea": last_row["dea"],
            "macd": last_row["macd"],
            "last_trade_date": last_row["trade_date"],
            "target_position": next_target,
        }
