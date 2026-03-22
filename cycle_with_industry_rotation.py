import pandas as pd

from macd_with_regime_filter import (
    calculate_atr,
    calculate_industry_flow_features,
    calculate_macd,
    calculate_market_activity_features,
    calculate_market_regime_features,
    calculate_rsi,
)


def _safe_gt(left, right):
    return pd.notna(left) and pd.notna(right) and left > right


class cycle_with_industry_rotation_strategy:
    def __init__(
        self,
        profile="balanced",
        display_name=None,
        trend_fast=20,
        trend_slow=60,
        breakout_lookback=30,
        breakout_buffer=0.98,
        pullback_buffer=1.025,
        min_rsi=50,
        max_rsi=78,
        atr_period=14,
        atr_stop_multiplier=2.0,
        atr_trail_multiplier=2.8,
        max_gap_pct=0.04,
        market_flow_floor=-2.0,
        industry_flow_floor=-2.0,
        industry_big_order_floor=-2.5,
        min_relative_strength=-0.03,
        valuation_buy_floor=55,
        valuation_sell_ceiling=20,
    ):
        self.profile = profile
        self.display_name = display_name or f"cycle_{profile}"
        self.trend_fast = trend_fast
        self.trend_slow = trend_slow
        self.breakout_lookback = breakout_lookback
        self.breakout_buffer = breakout_buffer
        self.pullback_buffer = pullback_buffer
        self.min_rsi = min_rsi
        self.max_rsi = max_rsi
        self.atr_period = atr_period
        self.atr_stop_multiplier = atr_stop_multiplier
        self.atr_trail_multiplier = atr_trail_multiplier
        self.max_gap_pct = max_gap_pct
        self.market_flow_floor = market_flow_floor
        self.industry_flow_floor = industry_flow_floor
        self.industry_big_order_floor = industry_big_order_floor
        self.min_relative_strength = min_relative_strength
        self.valuation_buy_floor = valuation_buy_floor
        self.valuation_sell_ceiling = valuation_sell_ceiling
        self.chain_configs = {
            "nonferrous": {
                "valuation_buy_floor": valuation_buy_floor - 10,
                "industry_rank_ceiling": 45,
                "low_zone_zscore": 0.15,
                "deep_low_zone_zscore": -0.6,
                "ma60_floor": 0.95,
                "deep_ma60_floor": 0.92,
                "min_timing_score": 1,
                "commodity_macro_ret20_floor": -0.05,
                "commodity_proxy_ret20_floor": -0.08,
                "commodity_proxy_volume_floor": 0.8,
                "industry_pct_floor": -4.0,
            },
            "chemical": {
                "valuation_buy_floor": valuation_buy_floor - 5,
                "industry_rank_ceiling": 45,
                "low_zone_zscore": -0.05,
                "deep_low_zone_zscore": -0.7,
                "ma60_floor": 0.92,
                "deep_ma60_floor": 0.88,
                "min_timing_score": 1,
                "commodity_macro_ret20_floor": -0.08,
                "commodity_proxy_ret20_floor": -0.14,
                "commodity_proxy_volume_floor": 0.7,
                "industry_pct_floor": -6.0,
            },
            "black": {
                "valuation_buy_floor": valuation_buy_floor,
                "industry_rank_ceiling": 25,
                "low_zone_zscore": -0.25,
                "deep_low_zone_zscore": -0.85,
                "ma60_floor": 0.93,
                "deep_ma60_floor": 0.9,
                "min_timing_score": 1,
                "commodity_macro_ret20_floor": -0.05,
                "commodity_proxy_ret20_floor": -0.08,
                "commodity_proxy_volume_floor": 0.8,
                "industry_pct_floor": -4.0,
            },
            "generic": {
                "valuation_buy_floor": valuation_buy_floor,
                "industry_rank_ceiling": 35,
                "low_zone_zscore": -0.2,
                "deep_low_zone_zscore": -0.9,
                "ma60_floor": 0.92,
                "deep_ma60_floor": 0.9,
                "min_timing_score": 1,
                "commodity_macro_ret20_floor": -0.05,
                "commodity_proxy_ret20_floor": -0.08,
                "commodity_proxy_volume_floor": 0.8,
                "industry_pct_floor": -4.0,
            },
        }
        self.leader_soft_support_score = 35
        self.leader_strength_score = 50
        self.leader_hold_score = 55
        self.leader_fade_score = 20
        self.leader_fade_rank = 15
        self.leader_support_rank = 8
        self.leader_strength_rank = 5
        self.leader_hold_rank = 3
        self.valuation_sell_trigger_streak = 2
        self.leader_fade_exit_streak = 3
        self.leader_fade_weak_exit_streak = 2

        if self.profile == "leader_hold":
            self.chain_configs["nonferrous"].update(
                {
                    "valuation_buy_floor": valuation_buy_floor - 20,
                    "industry_rank_ceiling": 60,
                    "low_zone_zscore": 0.35,
                    "deep_low_zone_zscore": -0.2,
                    "ma60_floor": 0.97,
                    "deep_ma60_floor": 0.94,
                    "min_timing_score": 1,
                    "commodity_macro_ret20_floor": -0.1,
                    "commodity_proxy_ret20_floor": -0.12,
                    "commodity_proxy_volume_floor": 0.65,
                    "industry_pct_floor": -7.0,
                }
            )
            self.leader_soft_support_score = 30
            self.leader_strength_score = 45
            self.leader_hold_score = 50
            self.leader_fade_score = 15
            self.leader_fade_rank = 18
            self.leader_support_rank = 10
            self.leader_strength_rank = 6
            self.leader_hold_rank = 4
            self.valuation_sell_trigger_streak = 3
            self.leader_fade_exit_streak = 4
            self.leader_fade_weak_exit_streak = 3
        elif self.profile == "swing":
            self.chain_configs["chemical"].update(
                {
                    "valuation_buy_floor": valuation_buy_floor - 8,
                    "industry_rank_ceiling": 50,
                    "low_zone_zscore": 0.0,
                    "deep_low_zone_zscore": -0.6,
                    "ma60_floor": 0.91,
                    "deep_ma60_floor": 0.87,
                    "min_timing_score": 1,
                    "commodity_macro_ret20_floor": -0.1,
                    "commodity_proxy_ret20_floor": -0.16,
                    "commodity_proxy_volume_floor": 0.65,
                    "industry_pct_floor": -7.0,
                }
            )
            self.chain_configs["black"].update(
                {
                    "valuation_buy_floor": valuation_buy_floor - 5,
                    "industry_rank_ceiling": 30,
                    "low_zone_zscore": -0.1,
                    "deep_low_zone_zscore": -0.75,
                    "ma60_floor": 0.92,
                    "deep_ma60_floor": 0.89,
                    "min_timing_score": 1,
                }
            )
            self.leader_soft_support_score = 30
            self.leader_strength_score = 45
            self.leader_hold_score = 50
            self.leader_support_rank = 10
            self.leader_strength_rank = 6
            self.leader_hold_rank = 4
            self.leader_fade_exit_streak = 2
            self.leader_fade_weak_exit_streak = 2

    def _resolve_cycle_chain(self, row):
        proxy_symbol = row.get("commodity_proxy_symbol", "")
        industry_name = str(row.get("commodity_proxy_industry", "") or "")
        if proxy_symbol in {"CU0", "AL0"} or industry_name in {"铜", "铝", "小金属"}:
            return "nonferrous"
        if proxy_symbol in {"MA0", "UR0"} or industry_name in {"化工原料", "农药化肥"}:
            return "chemical"
        if proxy_symbol in {"RB0", "HC0", "J0", "JM0", "FG0"} or industry_name in {
            "普钢",
            "水泥",
            "玻璃",
            "煤炭开采",
            "工程机械",
            "建筑工程",
        }:
            return "black"
        return "generic"

    def trading_strategy(self, df):
        df = df.copy()
        df = calculate_macd(df)
        df = calculate_rsi(df)
        df = calculate_atr(df, self.atr_period)
        df = calculate_market_regime_features(df, "sh_index")
        df = calculate_market_regime_features(df, "csi300")
        df = calculate_market_activity_features(df)
        df = calculate_industry_flow_features(df)

        df["ema_fast_trend"] = df["close"].ewm(span=self.trend_fast, adjust=False).mean()
        df["ema_slow_trend"] = df["close"].ewm(span=self.trend_slow, adjust=False).mean()
        df["rolling_high"] = df["close"].rolling(window=self.breakout_lookback).max().shift(1)
        df["rolling_low_30"] = df["close"].rolling(window=30).min().shift(1)
        df["ma20"] = df["close"].rolling(window=20).mean()
        df["ma60"] = df["close"].rolling(window=60).mean()
        df["std20"] = df["close"].rolling(window=20).std()
        df["zscore20"] = (df["close"] - df["ma20"]) / df["std20"].replace(0, pd.NA)
        if "amount" in df.columns:
            df["amount_ma20"] = df["amount"].rolling(window=20).mean()
            df["amount_ratio"] = df["amount"] / df["amount_ma20"].replace(0, pd.NA)
        if "sh_index_close" in df.columns:
            df["relative_strength_20"] = (
                df["close"].pct_change(20, fill_method=None)
                - df["sh_index_close"].pct_change(20, fill_method=None)
            )
        else:
            df["relative_strength_20"] = 0.0
        if "commodity_macro_close" in df.columns:
            df["commodity_macro_ema20"] = df["commodity_macro_close"].ewm(span=20, adjust=False).mean()
            df["commodity_macro_ema60"] = df["commodity_macro_close"].ewm(span=60, adjust=False).mean()
            df["commodity_macro_ret20"] = df["commodity_macro_close"].pct_change(20, fill_method=None)
        if "commodity_proxy_close" in df.columns:
            df["commodity_proxy_ema20"] = df["commodity_proxy_close"].ewm(span=20, adjust=False).mean()
            df["commodity_proxy_ema60"] = df["commodity_proxy_close"].ewm(span=60, adjust=False).mean()
            df["commodity_proxy_ret20"] = df["commodity_proxy_close"].pct_change(20, fill_method=None)
        if "commodity_proxy_volume" in df.columns:
            df["commodity_proxy_volume_ma20"] = df["commodity_proxy_volume"].rolling(window=20).mean()
        if "commodity_proxy_open_interest" in df.columns:
            df["commodity_proxy_oi_ma10"] = df["commodity_proxy_open_interest"].rolling(window=10).mean()

        df["signal"] = 0
        df["reason"] = "观望"
        df["cycle_value_score"] = pd.NA
        df["cycle_buy_timing_score"] = pd.NA
        df["cycle_sell_timing_score"] = pd.NA
        df["cycle_leader_score"] = pd.NA
        df["cycle_leader_rank"] = pd.NA

        in_position = False
        entry_price = None
        entry_atr = None
        highest_close = None
        pending_entry_atr = None
        leader_fade_streak = 0
        leader_hold_streak = 0

        warmup = max(60, self.trend_slow, self.breakout_lookback, self.atr_period)
        for i in range(warmup, len(df) - 1):
            prev_row = df.iloc[i - 1]
            row = df.iloc[i]
            next_index = i + 1
            chain = self._resolve_cycle_chain(row)
            chain_config = self.chain_configs.get(chain, self.chain_configs["generic"])

            if df["signal"].iloc[i] == 1 and not in_position:
                in_position = True
                entry_price = df["open"].iloc[i]
                entry_atr = pending_entry_atr if pending_entry_atr is not None else row["atr"]
                highest_close = max(entry_price, row["close"])
                pending_entry_atr = None
                leader_fade_streak = 0
                leader_hold_streak = 0
            elif df["signal"].iloc[i] == -1 and in_position:
                in_position = False
                entry_price = None
                entry_atr = None
                highest_close = None
                pending_entry_atr = None
                leader_fade_streak = 0
                leader_hold_streak = 0

            if pd.isna(row["atr"]) or pd.isna(row["rsi"]) or pd.isna(row["rolling_high"]):
                continue

            has_market_data = all(
                col in df.columns
                for col in [
                    "sh_index_close",
                    "sh_index_ema_fast",
                    "sh_index_ema_slow",
                    "sh_index_ret_5",
                    "csi300_close",
                    "csi300_ema_slow",
                    "csi300_ret_5",
                ]
            )
            if has_market_data:
                market_trend_ok = (
                    row.get("sh_index_close", pd.NA) >= row.get("sh_index_ema_slow", pd.NA) * 0.99
                    and row.get("csi300_close", pd.NA) >= row.get("csi300_ema_slow", pd.NA) * 0.985
                )
                market_risk_ok = (
                    pd.notna(row.get("sh_index_ret_5", pd.NA))
                    and pd.notna(row.get("csi300_ret_5", pd.NA))
                    and row.get("sh_index_ret_5", pd.NA) > -0.05
                    and row.get("csi300_ret_5", pd.NA) > -0.06
                )
            else:
                market_trend_ok = True
                market_risk_ok = True

            if "market_net_amount_rate" in df.columns and pd.notna(row.get("market_net_amount_rate", pd.NA)):
                market_flow_ok = row.get("market_net_amount_rate", pd.NA) > self.market_flow_floor
                if pd.notna(row.get("market_net_amount_rate_ma3", pd.NA)):
                    market_flow_ok = (
                        market_flow_ok
                        and row.get("market_net_amount_rate_ma3", pd.NA) > self.market_flow_floor + 0.6
                    )
            else:
                market_flow_ok = True

            if "market_amount_ratio" in df.columns and pd.notna(row.get("market_amount_ratio", pd.NA)):
                market_liquidity_ok = row.get("market_amount_ratio", pd.NA) >= 0.85
            else:
                market_liquidity_ok = True

            commodity_macro_ok = True
            if "commodity_macro_close" in df.columns and pd.notna(row.get("commodity_macro_close", pd.NA)):
                commodity_macro_ok = (
                    pd.notna(row.get("commodity_macro_ema20", pd.NA))
                    and row.get("commodity_macro_close", pd.NA) >= row.get("commodity_macro_ema20", pd.NA) * 0.985
                )
                if pd.notna(row.get("commodity_macro_ema60", pd.NA)):
                    commodity_macro_ok = (
                        commodity_macro_ok
                        and pd.notna(row.get("commodity_macro_ema20", pd.NA))
                        and row.get("commodity_macro_ema20", pd.NA) >= row.get("commodity_macro_ema60", pd.NA) * 0.995
                    )
                if pd.notna(row.get("commodity_macro_ret20", pd.NA)):
                    commodity_macro_ok = (
                        commodity_macro_ok
                        and row.get("commodity_macro_ret20", pd.NA) > chain_config["commodity_macro_ret20_floor"]
                    )

            commodity_proxy_ok = True
            if "commodity_proxy_close" in df.columns and pd.notna(row.get("commodity_proxy_close", pd.NA)):
                commodity_proxy_ok = (
                    pd.notna(row.get("commodity_proxy_ema20", pd.NA))
                    and row.get("commodity_proxy_close", pd.NA) >= row.get("commodity_proxy_ema20", pd.NA) * 0.99
                )
                if pd.notna(row.get("commodity_proxy_ema60", pd.NA)):
                    commodity_proxy_ok = (
                        commodity_proxy_ok
                        and pd.notna(row.get("commodity_proxy_ema20", pd.NA))
                        and row.get("commodity_proxy_ema20", pd.NA) >= row.get("commodity_proxy_ema60", pd.NA) * 0.99
                    )
                if pd.notna(row.get("commodity_proxy_ret20", pd.NA)):
                    commodity_proxy_ok = (
                        commodity_proxy_ok
                        and row.get("commodity_proxy_ret20", pd.NA) > chain_config["commodity_proxy_ret20_floor"]
                    )
                if pd.notna(row.get("commodity_proxy_volume_ma20", pd.NA)):
                    commodity_proxy_ok = (
                        commodity_proxy_ok
                        and row.get("commodity_proxy_volume", pd.NA)
                        >= row.get("commodity_proxy_volume_ma20", pd.NA) * chain_config["commodity_proxy_volume_floor"]
                    )

            industry_flow_ok = True
            industry_rotation_ok = True
            if "industry_flow_net_amount_rate" in df.columns and pd.notna(row.get("industry_flow_net_amount_rate", pd.NA)):
                industry_flow_ok = row.get("industry_flow_net_amount_rate", pd.NA) > self.industry_flow_floor
                if pd.notna(row.get("industry_flow_net_amount_rate_ma3", pd.NA)):
                    industry_flow_ok = (
                        industry_flow_ok
                        and row.get("industry_flow_net_amount_rate_ma3", pd.NA) > self.industry_flow_floor
                    )
                if pd.notna(row.get("industry_flow_big_order_rate_ma3", pd.NA)):
                    industry_flow_ok = (
                        industry_flow_ok
                        and row.get("industry_flow_big_order_rate_ma3", pd.NA) > self.industry_big_order_floor
                    )
                if pd.notna(row.get("industry_flow_pct_change", pd.NA)):
                    industry_rotation_ok = (
                        row.get("industry_flow_pct_change", pd.NA) > chain_config["industry_pct_floor"]
                    )
                if pd.notna(row.get("industry_flow_rank", pd.NA)):
                    industry_rotation_ok = (
                        industry_rotation_ok
                        and row.get("industry_flow_rank", pd.NA) <= chain_config["industry_rank_ceiling"]
                    )

            leader_score = row.get("leader_score", pd.NA)
            leader_rank = row.get("leader_rank_in_pool", pd.NA)
            leader_candidate = False
            if "leader_candidate" in df.columns and pd.notna(row.get("leader_candidate", pd.NA)):
                leader_candidate = bool(row.get("leader_candidate", False))
            leader_data_ready = pd.notna(leader_score)
            leader_support_ok = True
            leader_strength_ok = True
            leader_fade = False
            leader_hold_ok = False
            if leader_data_ready:
                leader_support_ok = (
                    leader_candidate
                    or leader_score >= self.leader_soft_support_score
                    or (pd.notna(leader_rank) and leader_rank <= self.leader_support_rank)
                )
                leader_strength_ok = (
                    leader_candidate
                    or leader_score >= self.leader_strength_score
                    or (pd.notna(leader_rank) and leader_rank <= self.leader_strength_rank)
                )
                leader_fade = (
                    leader_score < self.leader_fade_score
                    or (
                        pd.notna(leader_rank)
                        and leader_rank >= self.leader_fade_rank
                        and pd.notna(row.get("relative_strength_20", pd.NA))
                        and row.get("relative_strength_20", pd.NA) < -0.03
                    )
                )
                leader_hold_ok = (
                    leader_candidate
                    or leader_score >= self.leader_hold_score
                    or (pd.notna(leader_rank) and leader_rank <= self.leader_hold_rank)
                )

            cycle_fundamental_ok = commodity_macro_ok and commodity_proxy_ok
            financial_ok = True
            financial_score = 0
            if "financial_quality_score" in df.columns:
                checks = []
                if pd.notna(row.get("financial_quality_score", pd.NA)):
                    checks.append(row.get("financial_quality_score", pd.NA) >= 40)
                    if row.get("financial_quality_score", pd.NA) >= 60:
                        financial_score += 2
                    elif row.get("financial_quality_score", pd.NA) >= 40:
                        financial_score += 1
                if pd.notna(row.get("financial_profit_yoy", pd.NA)):
                    checks.append(row.get("financial_profit_yoy", pd.NA) > -15)
                    if row.get("financial_profit_yoy", pd.NA) > 0:
                        financial_score += 1
                if pd.notna(row.get("financial_revenue_yoy", pd.NA)):
                    checks.append(row.get("financial_revenue_yoy", pd.NA) > -10)
                    if row.get("financial_revenue_yoy", pd.NA) > 0:
                        financial_score += 1
                if pd.notna(row.get("financial_operating_cashflow", pd.NA)):
                    checks.append(row.get("financial_operating_cashflow", pd.NA) > 0)
                    if row.get("financial_operating_cashflow", pd.NA) > 0:
                        financial_score += 1
                if pd.notna(row.get("financial_roe", pd.NA)):
                    checks.append(row.get("financial_roe", pd.NA) >= 6)
                    if row.get("financial_roe", pd.NA) >= 8:
                        financial_score += 1
                if pd.notna(row.get("financial_debt_ratio", pd.NA)):
                    checks.append(row.get("financial_debt_ratio", pd.NA) <= 75)
                if checks:
                    financial_ok = sum(1 for item in checks if item) >= max(2, len(checks) - 1)

            gap_pct = (row["open"] - prev_row["close"]) / prev_row["close"]
            cross_up = prev_row["dif"] <= prev_row["dea"] and row["dif"] > row["dea"]
            cross_down = prev_row["dif"] >= prev_row["dea"] and row["dif"] < row["dea"]
            trend_ok = (
                row["close"] >= row["ema_slow_trend"] * 0.97
                and row["ema_fast_trend"] >= row["ema_slow_trend"] * 0.985
                and row["ema_fast_trend"] > prev_row["ema_fast_trend"] * 0.995
            )
            breakout_ok = row["close"] >= row["rolling_high"] * self.breakout_buffer
            pullback_rebound_ok = (
                row["close"] > row["ema_fast_trend"]
                and row["low"] <= row["ema_fast_trend"] * self.pullback_buffer
                and row["macd"] > prev_row["macd"]
                and row["dif"] >= row["dea"]
            )
            rsi_ok = self.min_rsi <= row["rsi"] <= self.max_rsi
            rs_ok = row.get("relative_strength_20", 0) >= self.min_relative_strength
            amount_ok = row.get("amount_ratio", 1) >= 0.85 if pd.notna(row.get("amount_ratio", pd.NA)) else True
            low_zone = (
                pd.notna(row.get("zscore20", pd.NA))
                and row.get("zscore20", pd.NA) <= chain_config["low_zone_zscore"]
                and pd.notna(row.get("ma60", pd.NA))
                and row["close"] >= row["ma60"] * chain_config["ma60_floor"]
            )
            deep_low_zone = (
                pd.notna(row.get("zscore20", pd.NA))
                and row.get("zscore20", pd.NA) <= chain_config["deep_low_zone_zscore"]
                and pd.notna(row.get("ma60", pd.NA))
                and row["close"] >= row["ma60"] * chain_config["deep_ma60_floor"]
            )
            low_point_ready = (
                (low_zone or deep_low_zone)
                and (
                    cross_up
                    or (row["dif"] >= row["dea"] and row["macd"] > prev_row["macd"])
                    or (
                        pd.notna(row.get("rolling_low_30", pd.NA))
                        and row["close"] >= row.get("rolling_low_30", pd.NA) * 1.03
                    )
                )
            )
            cycle_value_score = 0
            if cycle_fundamental_ok:
                cycle_value_score += 2
            if industry_flow_ok and industry_rotation_ok:
                cycle_value_score += 2
            if financial_ok:
                cycle_value_score += max(1, min(3, financial_score))
            if rs_ok:
                cycle_value_score += 1
            if trend_ok:
                cycle_value_score += 1
            if leader_data_ready:
                if leader_candidate:
                    cycle_value_score += 2
                elif leader_score >= 50:
                    cycle_value_score += 1
            timing_score = 0
            if low_zone:
                timing_score += 1
            if deep_low_zone:
                timing_score += 1
            if low_point_ready:
                timing_score += 1
            if cross_up or (row["dif"] >= row["dea"] and row["macd"] > prev_row["macd"]):
                timing_score += 1
            if leader_data_ready and leader_strength_ok and row["macd"] > prev_row["macd"]:
                timing_score += 1
            valuation_buy_ok = (
                (
                    pd.notna(row.get("cycle_pb_percentile_3y", pd.NA))
                    and row.get("cycle_pb_percentile_3y", pd.NA) >= chain_config["valuation_buy_floor"]
                )
                or (
                    pd.notna(row.get("cycle_pe_percentile_3y", pd.NA))
                    and row.get("cycle_pe_percentile_3y", pd.NA) >= chain_config["valuation_buy_floor"]
                )
            )
            valuation_sell_warning = (
                (
                    pd.notna(row.get("cycle_pb_percentile_3y", pd.NA))
                    and row.get("cycle_pb_percentile_3y", pd.NA) <= self.valuation_sell_ceiling
                )
                or (
                    pd.notna(row.get("cycle_pe_percentile_3y", pd.NA))
                    and row.get("cycle_pe_percentile_3y", pd.NA) <= self.valuation_sell_ceiling
                )
            )
            sell_timing_score = 0
            if valuation_sell_warning:
                sell_timing_score += 2
            if overbought := (pd.notna(row.get("zscore20", pd.NA)) and row.get("zscore20", pd.NA) >= 1.2):
                sell_timing_score += 1
            if pd.notna(row["rsi"]) and row["rsi"] >= 72:
                sell_timing_score += 1
            if cross_down or row["macd"] < prev_row["macd"]:
                sell_timing_score += 1

            if in_position:
                if leader_data_ready:
                    if leader_fade:
                        leader_fade_streak += 1
                    else:
                        leader_fade_streak = 0
                    if leader_hold_ok:
                        leader_hold_streak += 1
                    else:
                        leader_hold_streak = 0
                else:
                    leader_fade_streak = 0
                    leader_hold_streak = 0

                highest_close = max(highest_close, row["close"])
                trailing_stop = max(
                    entry_price - self.atr_stop_multiplier * entry_atr,
                    highest_close - self.atr_trail_multiplier * row["atr"],
                )
                trend_hold_ok = row["close"] >= row["ema_slow_trend"] * 0.98

                industry_breakdown = False
                if "industry_flow_net_amount_rate" in df.columns and pd.notna(row.get("industry_flow_net_amount_rate", pd.NA)):
                    industry_breakdown = row.get("industry_flow_net_amount_rate", pd.NA) < self.industry_flow_floor - 2.0
                if pd.notna(row.get("industry_flow_big_order_rate_ma3", pd.NA)):
                    industry_breakdown = (
                        industry_breakdown
                        or row.get("industry_flow_big_order_rate_ma3", pd.NA)
                        < self.industry_big_order_floor - 1.5
                    )

                proxy_breakdown = False
                if "commodity_proxy_close" in df.columns and pd.notna(row.get("commodity_proxy_close", pd.NA)):
                    proxy_breakdown = (
                        pd.notna(row.get("commodity_proxy_ema20", pd.NA))
                        and row.get("commodity_proxy_close", pd.NA) < row.get("commodity_proxy_ema20", pd.NA) * 0.97
                    )
                    if pd.notna(row.get("commodity_proxy_ret20", pd.NA)):
                        proxy_breakdown = proxy_breakdown or row.get("commodity_proxy_ret20", pd.NA) < -0.12

                if row["close"] < trailing_stop:
                    df.loc[next_index, "signal"] = -1
                    df.loc[next_index, "reason"] = f"跌破ATR保护位 {trailing_stop:.2f}，建议明日开盘卖出"
                    in_position = False
                    entry_price = None
                    entry_atr = None
                    highest_close = None
                    continue

                if industry_breakdown:
                    df.loc[next_index, "signal"] = -1
                    df.loc[next_index, "reason"] = "行业资金流明显转弱，周期景气可能降温，建议明日开盘卖出"
                    in_position = False
                    entry_price = None
                    entry_atr = None
                    highest_close = None
                    continue

                if proxy_breakdown:
                    df.loc[next_index, "signal"] = -1
                    df.loc[next_index, "reason"] = "商品价格代理走弱，周期景气代理转差，建议明日开盘卖出"
                    in_position = False
                    entry_price = None
                    entry_atr = None
                    highest_close = None
                    continue

                if cross_down and row["close"] < row["ema_fast_trend"]:
                    df.loc[next_index, "signal"] = -1
                    df.loc[next_index, "reason"] = "MACD死叉且跌回短趋势线下方，建议明日开盘卖出"
                    in_position = False
                    entry_price = None
                    entry_atr = None
                    highest_close = None
                    continue

                if row["close"] < row["ema_slow_trend"]:
                    df.loc[next_index, "signal"] = -1
                    df.loc[next_index, "reason"] = "中期趋势被破坏，建议明日开盘卖出"
                    in_position = False
                    entry_price = None
                    entry_atr = None
                    highest_close = None
                    continue

                if (
                    valuation_sell_warning
                    and sell_timing_score >= 3
                    and (
                        leader_fade_streak >= self.valuation_sell_trigger_streak
                        or (not leader_hold_ok and leader_hold_streak == 0)
                        or not cycle_fundamental_ok
                        or not industry_flow_ok
                        or not trend_hold_ok
                    )
                ):
                    df.loc[next_index, "signal"] = -1
                    if leader_fade_streak >= self.valuation_sell_trigger_streak:
                        df.loc[next_index, "reason"] = "个股高估且龙头强度已连续退潮，建议明日开盘卖出"
                    else:
                        df.loc[next_index, "reason"] = "周期股估值回到高位，且短线动能转弱，建议明日开盘卖出"
                    in_position = False
                    entry_price = None
                    entry_atr = None
                    highest_close = None
                    leader_fade_streak = 0
                    leader_hold_streak = 0
                    continue

                if leader_data_ready and (
                    leader_fade_streak >= self.leader_fade_exit_streak
                    or (
                        leader_fade_streak >= self.leader_fade_weak_exit_streak
                        and row["macd"] < prev_row["macd"]
                    )
                ):
                    df.loc[next_index, "signal"] = -1
                    if leader_fade_streak >= self.leader_fade_exit_streak:
                        df.loc[next_index, "reason"] = (
                            f"个股龙头强度已连续{self.leader_fade_exit_streak}日退潮，建议明日开盘卖出"
                        )
                    else:
                        df.loc[next_index, "reason"] = "个股龙头强度连续退潮，且短线动能转弱，建议明日开盘卖出"
                    in_position = False
                    entry_price = None
                    entry_atr = None
                    highest_close = None
                    leader_fade_streak = 0
                    leader_hold_streak = 0
                    continue

                if row["rsi"] > 82 and row["macd"] < prev_row["macd"]:
                    df.loc[next_index, "signal"] = -1
                    df.loc[next_index, "reason"] = "短线过热且动能回落，建议明日开盘卖出"
                    in_position = False
                    entry_price = None
                    entry_atr = None
                    highest_close = None
                    leader_fade_streak = 0
                    leader_hold_streak = 0
                    continue

            if (
                not in_position
                and trend_ok
                and rsi_ok
                and rs_ok
                and amount_ok
                and market_trend_ok
                and market_risk_ok
                and market_flow_ok
                and market_liquidity_ok
                and cycle_fundamental_ok
                and financial_ok
                and industry_flow_ok
                and industry_rotation_ok
                and abs(gap_pct) <= self.max_gap_pct
                and cycle_value_score >= 4
                and timing_score >= chain_config["min_timing_score"]
                and (leader_support_ok or cycle_value_score >= 6)
                and (valuation_buy_ok or cycle_value_score >= 6)
                and (low_point_ready or pullback_rebound_ok)
            ):
                df.loc[next_index, "signal"] = 1
                if leader_candidate and deep_low_zone:
                    df.loc[next_index, "reason"] = "周期景气与财报未坏，个股具备龙头特征，估值回到深低位后止跌，建议明日开盘低吸买入"
                elif leader_candidate:
                    df.loc[next_index, "reason"] = "周期景气与财报仍在，个股可能是板块龙头，估值偏低且回踩企稳，建议明日开盘买入"
                elif deep_low_zone:
                    df.loc[next_index, "reason"] = "周期景气与财报未坏，估值回到偏低区，股价在深低点止跌，建议明日开盘低吸买入"
                else:
                    df.loc[next_index, "reason"] = "周期景气与财报仍在，估值偏低且回踩企稳，建议明日开盘买入"
                pending_entry_atr = row["atr"]
            df.loc[next_index, "cycle_value_score"] = cycle_value_score
            df.loc[next_index, "cycle_buy_timing_score"] = timing_score
            df.loc[next_index, "cycle_sell_timing_score"] = sell_timing_score
            df.loc[next_index, "cycle_leader_score"] = leader_score
            df.loc[next_index, "cycle_leader_rank"] = leader_rank

        return df

    def predict_next_signal(self, df):
        trade_df = df[df["trade_date"].notna()].copy().reset_index(drop=True)
        if len(trade_df) < 2:
            raise ValueError("数据不足，无法预测下一交易日信号")

        next_row = trade_df.tail(1).copy()
        next_row.loc[:, "trade_date"] = f"{trade_df['trade_date'].iloc[-1]}_NEXT"
        next_row.loc[:, "signal"] = 0
        next_row.loc[:, "reason"] = "观望"
        preview_df = pd.concat([trade_df, next_row], ignore_index=True)
        preview_df = self.trading_strategy(preview_df)

        last_row = preview_df.iloc[-2]
        next_signal = preview_df.iloc[-1]["signal"]
        next_reason = preview_df.iloc[-1]["reason"]

        if next_signal == 1:
            signal = "买入"
        elif next_signal == -1:
            signal = "卖出"
        else:
            signal = "观望"
            if (
                last_row["close"] > last_row["ema_slow_trend"]
                and last_row["dif"] > last_row["dea"]
                and last_row.get("relative_strength_20", 0) >= 0
            ):
                if pd.notna(last_row.get("cycle_leader_score", pd.NA)) and last_row.get("cycle_leader_score", 0) >= 55:
                    next_reason = "个股强度接近板块龙头，若已经持有更适合继续拿住，等待龙头退潮或趋势破位再处理"
                else:
                    next_reason = "周期趋势仍偏强，但还没到理想突破或回踩买点，继续观察"
            else:
                next_reason = "周期景气、行业资金或个股趋势暂时没有同时共振，先观望"

        return {
            "signal": signal,
            "reason": next_reason,
            "dif": last_row["dif"],
            "dea": last_row["dea"],
            "macd": last_row["macd"],
            "last_trade_date": last_row["trade_date"],
        }
