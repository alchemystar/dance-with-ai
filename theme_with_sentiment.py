import pandas as pd

from macd_with_regime_filter import (
    calculate_atr,
    calculate_industry_flow_features,
    calculate_macd,
    calculate_market_activity_features,
    calculate_market_regime_features,
    calculate_rsi,
)


class theme_with_sentiment_strategy:
    def __init__(
        self,
        display_name="theme_with_sentiment",
        trend_fast=10,
        trend_slow=30,
        breakout_lookback=20,
        pullback_buffer=1.02,
        min_rsi=52,
        max_rsi=82,
        atr_period=14,
        atr_stop_multiplier=1.8,
        atr_trail_multiplier=2.4,
        max_gap_pct=0.06,
        min_sentiment_score=45,
        min_up_down_ratio=1.4,
        max_broken_rate=0.42,
        relaxed_sentiment_score=25,
        relaxed_up_down_ratio=1.3,
        relaxed_broken_rate=0.72,
        industry_rank_ceiling=30,
        min_relative_strength=0.02,
        min_volume_ratio=0.8,
        breakout_volume_ratio=1.05,
        rebound_volume_ratio=0.95,
        min_heat_score=24,
        min_highest_board=2,
        min_multi_board_count=2,
        leader_score_floor=45,
        leader_rank_ceiling=5,
        min_core_relative_strength=0.05,
        trial_position=0.5,
        core_entry_position=0.8,
        max_position=1.0,
        add_step=0.2,
        trim_to_trial_position=0.5,
        fast_fail_loss_limit=0.04,
        breakout_fail_loss_limit=0.035,
        max_fast_fail_holding_days=3,
    ):
        self.display_name = display_name
        self.trend_fast = trend_fast
        self.trend_slow = trend_slow
        self.breakout_lookback = breakout_lookback
        self.pullback_buffer = pullback_buffer
        self.min_rsi = min_rsi
        self.max_rsi = max_rsi
        self.atr_period = atr_period
        self.atr_stop_multiplier = atr_stop_multiplier
        self.atr_trail_multiplier = atr_trail_multiplier
        self.max_gap_pct = max_gap_pct
        self.min_sentiment_score = min_sentiment_score
        self.min_up_down_ratio = min_up_down_ratio
        self.max_broken_rate = max_broken_rate
        self.relaxed_sentiment_score = relaxed_sentiment_score
        self.relaxed_up_down_ratio = relaxed_up_down_ratio
        self.relaxed_broken_rate = relaxed_broken_rate
        self.industry_rank_ceiling = industry_rank_ceiling
        self.min_relative_strength = min_relative_strength
        self.min_volume_ratio = min_volume_ratio
        self.breakout_volume_ratio = breakout_volume_ratio
        self.rebound_volume_ratio = rebound_volume_ratio
        self.min_heat_score = min_heat_score
        self.min_highest_board = min_highest_board
        self.min_multi_board_count = min_multi_board_count
        self.leader_score_floor = leader_score_floor
        self.leader_rank_ceiling = leader_rank_ceiling
        self.min_core_relative_strength = min_core_relative_strength
        self.trial_position = trial_position
        self.core_entry_position = core_entry_position
        self.max_position = max_position
        self.add_step = add_step
        self.trim_to_trial_position = trim_to_trial_position
        self.fast_fail_loss_limit = fast_fail_loss_limit
        self.breakout_fail_loss_limit = breakout_fail_loss_limit
        self.max_fast_fail_holding_days = max_fast_fail_holding_days

    @staticmethod
    def _safe_target_value(value, fallback=0.0):
        if pd.isna(value):
            return float(fallback)
        return float(value)

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
        df["ma5"] = df["close"].rolling(window=5).mean()
        df["ma10"] = df["close"].rolling(window=10).mean()
        df["rolling_high"] = df["close"].rolling(window=self.breakout_lookback).max().shift(1)
        df["rolling_low_20"] = df["close"].rolling(window=20).min().shift(1)
        df["amount_ma10"] = df["amount"].rolling(window=10).mean() if "amount" in df.columns else pd.NA
        df["amount_ratio"] = df["amount"] / df["amount_ma10"].replace(0, pd.NA) if "amount" in df.columns else pd.NA
        if "vol" in df.columns:
            df["volume_ma5"] = df["vol"].rolling(window=5).mean().shift(1)
            df["volume_ma20"] = df["vol"].rolling(window=20).mean().shift(1)
            df["volume_ratio_5"] = df["vol"] / df["volume_ma5"].replace(0, pd.NA)
            df["volume_ratio_20"] = df["vol"] / df["volume_ma20"].replace(0, pd.NA)
            df["volume_std20"] = df["vol"].rolling(window=20).std().shift(1)
            df["volume_zscore_20"] = (
                (df["vol"] - df["volume_ma20"]) / df["volume_std20"].replace(0, pd.NA)
            )
        else:
            df["volume_ratio_5"] = pd.NA
            df["volume_ratio_20"] = pd.NA
            df["volume_zscore_20"] = pd.NA
        if "sh_index_close" in df.columns:
            df["relative_strength_10"] = (
                df["close"].pct_change(10, fill_method=None)
                - df["sh_index_close"].pct_change(10, fill_method=None)
            )
        else:
            df["relative_strength_10"] = 0.0

        if "theme_sentiment_score" in df.columns:
            df["theme_sentiment_score_ma3"] = df["theme_sentiment_score"].rolling(window=3).mean()
            df["theme_sentiment_score_ma5"] = df["theme_sentiment_score"].rolling(window=5).mean()
        if "theme_up_limit" in df.columns:
            df["theme_up_limit_ma5"] = df["theme_up_limit"].rolling(window=5).mean()
        if "theme_first_board" in df.columns:
            df["theme_first_board_ma5"] = df["theme_first_board"].rolling(window=5).mean()
        for col in [
            "theme_is_limit_up",
            "theme_is_broken",
            "theme_is_down_limit",
            "theme_is_first_board",
            "theme_is_second_board",
            "theme_is_multi_board",
            "theme_is_reseal",
            "theme_board_count",
        ]:
            if col not in df.columns:
                df[col] = 0
            df[col] = df[col].fillna(0)
        df["theme_limit_hit_count_5"] = df["theme_is_limit_up"].rolling(window=5).sum().shift(1)
        df["theme_limit_hit_count_20"] = df["theme_is_limit_up"].rolling(window=20).sum().shift(1)
        df["theme_broken_count_10"] = df["theme_is_broken"].rolling(window=10).sum().shift(1)
        df["theme_second_board_count_10"] = df["theme_is_second_board"].rolling(window=10).sum().shift(1)
        df["theme_reseal_count_10"] = df["theme_is_reseal"].rolling(window=10).sum().shift(1)
        for col in ["theme_heat_score", "theme_highest_board", "theme_multi_board_count", "theme_reseal_count"]:
            if col not in df.columns:
                df[col] = pd.NA
        if "theme_heat_score" in df.columns:
            df["theme_heat_score_ma3"] = df["theme_heat_score"].rolling(window=3).mean()
        if "theme_highest_board" in df.columns:
            df["theme_highest_board_ma3"] = df["theme_highest_board"].rolling(window=3).mean()
        if "theme_multi_board_count" in df.columns:
            df["theme_multi_board_ma3"] = df["theme_multi_board_count"].rolling(window=3).mean()
        if "theme_reseal_count" in df.columns:
            df["theme_reseal_count_ma3"] = df["theme_reseal_count"].rolling(window=3).mean()

        df["signal"] = 0
        df["reason"] = "观望"
        df["target_position"] = pd.NA

        in_position = False
        current_target = 0.0
        entry_price = None
        entry_atr = None
        highest_close = None
        pending_entry_atr = None
        pending_entry_style = None
        entry_style = None
        holding_days = 0
        leader_fade_streak = 0

        warmup = max(40, self.trend_slow, self.breakout_lookback, self.atr_period)
        for i in range(warmup, len(df) - 1):
            prev_row = df.iloc[i - 1]
            row = df.iloc[i]
            next_index = i + 1
            executed_signal = row.get("signal", 0)
            executed_target = row.get("target_position", pd.NA)
            if executed_signal == 1:
                next_target_after_fill = current_target if pd.isna(executed_target) else float(executed_target or 0.0)
                if next_target_after_fill > 0:
                    current_target = next_target_after_fill
                    if not in_position:
                        in_position = True
                        entry_price = df["open"].iloc[i]
                        entry_atr = pending_entry_atr if pending_entry_atr is not None else row["atr"]
                        entry_style = pending_entry_style or "standard"
                        highest_close = max(entry_price, row["close"])
                        holding_days = 0
                        leader_fade_streak = 0
                    pending_entry_atr = None
                    pending_entry_style = None
            elif executed_signal == -1:
                next_target_after_fill = 0.0 if pd.isna(executed_target) else float(executed_target or 0.0)
                current_target = next_target_after_fill
                if current_target <= 0 and in_position:
                    in_position = False
                    entry_price = None
                    entry_atr = None
                    highest_close = None
                    pending_entry_atr = None
                    pending_entry_style = None
                    entry_style = None
                    holding_days = 0
                    leader_fade_streak = 0

            if pd.isna(row.get("atr", pd.NA)) or pd.isna(row.get("rsi", pd.NA)) or pd.isna(row.get("rolling_high", pd.NA)):
                continue

            gap_pct = (row["open"] - prev_row["close"]) / prev_row["close"]
            cross_up = prev_row["dif"] <= prev_row["dea"] and row["dif"] > row["dea"]
            cross_down = prev_row["dif"] >= prev_row["dea"] and row["dif"] < row["dea"]

            trend_ok = (
                row["close"] >= row["ema_slow_trend"] * 0.99
                and row["ema_fast_trend"] >= row["ema_slow_trend"] * 0.995
                and row["ema_fast_trend"] > prev_row["ema_fast_trend"] * 0.998
            )
            volume_support_ok = (
                pd.isna(row.get("volume_ratio_20", pd.NA))
                or row.get("volume_ratio_20", 0) >= self.min_volume_ratio
            )
            breakout_volume_ok = (
                pd.isna(row.get("volume_ratio_20", pd.NA))
                or row.get("volume_ratio_20", 0) >= self.breakout_volume_ratio
                or row.get("volume_zscore_20", -99) >= 0.2
            )
            rebound_volume_ok = (
                pd.isna(row.get("volume_ratio_5", pd.NA))
                or row.get("volume_ratio_5", 0) >= self.rebound_volume_ratio
            )
            breakout_ok = (
                row["close"] >= row["rolling_high"] * 0.995
                and row.get("amount_ratio", 1.0) >= 1.1
                and breakout_volume_ok
            )
            pullback_reclaim_ok = (
                row["close"] >= row["ema_fast_trend"] * 0.995
                and row["low"] <= row["ema_fast_trend"] * self.pullback_buffer
                and row["macd"] > prev_row["macd"]
                and row["dif"] >= row["dea"]
                and rebound_volume_ok
            )
            strong_stock_ok = (
                row.get("relative_strength_10", 0.0) >= self.min_relative_strength
                and row.get("amount_ratio", 1.0) >= 0.9
                and volume_support_ok
            )
            leader_score = row.get("theme_leader_score", pd.NA)
            leader_rank = row.get("theme_leader_rank_in_pool", pd.NA)
            leader_candidate = False
            if "theme_leader_candidate" in df.columns and pd.notna(row.get("theme_leader_candidate", pd.NA)):
                leader_candidate = bool(row.get("theme_leader_candidate", False))
            leader_support_ok = (
                leader_candidate
                or (pd.notna(leader_score) and leader_score >= self.leader_score_floor)
                or (pd.notna(leader_rank) and leader_rank <= self.leader_rank_ceiling)
            )
            core_trend_candidate = (
                strong_stock_ok
                and pd.notna(row.get("relative_strength_10", pd.NA))
                and row.get("relative_strength_10", 0.0) >= self.min_core_relative_strength
                and row.get("amount_ratio", 0.0) >= 1.0
                and (
                    pd.isna(row.get("volume_ratio_20", pd.NA))
                    or row.get("volume_ratio_20", 0.0) >= 1.0
                )
            )
            leader_fade = (
                (pd.notna(leader_score) and leader_score < 35)
                or (pd.notna(leader_rank) and leader_rank >= 8)
            )
            board_core_ok = (
                row.get("theme_is_limit_up", 0) >= 1
                or row.get("theme_is_first_board", 0) >= 1
                or row.get("theme_is_second_board", 0) >= 1
                or row.get("theme_is_multi_board", 0) >= 1
                or row.get("theme_is_reseal", 0) >= 1
                or row.get("theme_limit_hit_count_5", 0) >= 1
            )
            ladder_front_ok = (
                row.get("theme_board_count", 0) >= 2
                or row.get("theme_is_second_board", 0) >= 1
                or row.get("theme_is_multi_board", 0) >= 1
                or row.get("theme_is_reseal", 0) >= 1
            )
            board_overheated = (
                row.get("theme_limit_hit_count_20", 0) >= 4
                or row.get("theme_broken_count_10", 0) >= 3
                or (
                    row.get("theme_board_count", 0) >= 5
                    and row.get("theme_broken_rate", 1) > self.relaxed_broken_rate
                )
            )
            board_support_relaxed = (
                board_core_ok
                or ladder_front_ok
                or row.get("theme_reseal_count_10", 0) >= 1
                or (pd.notna(leader_score) and leader_score >= 60)
            )

            heat_ok = True
            heat_improving = False
            heat_cooling = False
            ladder_heat_ok = False
            if pd.notna(row.get("theme_heat_score", pd.NA)):
                heat_ok = (
                    row.get("theme_heat_score", 0) >= self.min_heat_score
                    or row.get("theme_highest_board", 0) >= self.min_highest_board
                    or row.get("theme_multi_board_count", 0) >= self.min_multi_board_count
                )
                heat_improving = (
                    row.get("theme_heat_score", 0) >= prev_row.get("theme_heat_score", 0)
                    or row.get("theme_highest_board", 0) > prev_row.get("theme_highest_board", 0)
                    or row.get("theme_multi_board_count", 0) >= prev_row.get("theme_multi_board_count", 0)
                    or row.get("theme_reseal_count", 0) >= max(1, prev_row.get("theme_reseal_count", 0))
                )
                heat_cooling = (
                    row.get("theme_heat_score", 0) < prev_row.get("theme_heat_score", 0)
                    and row.get("theme_highest_board", 0) < prev_row.get("theme_highest_board", 0)
                    and row.get("theme_multi_board_count", 0) <= prev_row.get("theme_multi_board_count", 0)
                )
                ladder_heat_ok = (
                    heat_ok
                    and (
                        heat_improving
                        or row.get("theme_is_reseal", 0) >= 1
                        or row.get("theme_is_second_board", 0) >= 1
                        or row.get("theme_board_count", 0) >= 3
                    )
                )

            market_trend_ok = True
            if "sh_index_close" in df.columns and "sh_index_ema_slow" in df.columns:
                market_trend_ok = row.get("sh_index_close", pd.NA) >= row.get("sh_index_ema_slow", pd.NA) * 0.985
            market_risk_ok = True
            if "sh_index_ret_5" in df.columns:
                market_risk_ok = row.get("sh_index_ret_5", 0) > -0.06

            sentiment_ok = True
            sentiment_hot = False
            sentiment_thaw = False
            sentiment_follow_ok = False
            sentiment_relaxed_ok = False
            if "theme_sentiment_score" in df.columns and pd.notna(row.get("theme_sentiment_score", pd.NA)):
                sentiment_ok = (
                    row.get("theme_sentiment_score", 0) >= self.min_sentiment_score
                    and row.get("theme_up_down_ratio", 0) >= self.min_up_down_ratio
                    and row.get("theme_broken_rate", 1) <= self.max_broken_rate
                )
                sentiment_relaxed_ok = (
                    row.get("theme_sentiment_score", 0) >= self.relaxed_sentiment_score
                    and row.get("theme_up_down_ratio", 0) >= self.relaxed_up_down_ratio
                    and row.get("theme_broken_rate", 1) <= self.relaxed_broken_rate
                )
                sentiment_hot = (
                    row.get("theme_sentiment_score", 0) >= self.min_sentiment_score + 15
                    and row.get("theme_first_board", 0) >= 10
                )
                if pd.notna(row.get("theme_sentiment_score_ma3", pd.NA)):
                    sentiment_ok = sentiment_ok or (
                        row.get("theme_sentiment_score_ma3", 0) >= self.min_sentiment_score + 5
                    )
                    sentiment_thaw = (
                        pd.notna(prev_row.get("theme_sentiment_score_ma3", pd.NA))
                        and prev_row.get("theme_sentiment_score", 0) <= prev_row.get("theme_sentiment_score_ma3", 0)
                        and row.get("theme_sentiment_score", 0) > row.get("theme_sentiment_score_ma3", 0)
                        and row.get("theme_sentiment_score", 0) >= self.min_sentiment_score - 5
                    )
                if pd.notna(row.get("theme_up_limit_ma5", pd.NA)):
                    sentiment_thaw = sentiment_thaw or (
                        row.get("theme_up_limit", 0) >= row.get("theme_up_limit_ma5", 0) * 1.1
                        and row.get("theme_broken_rate", 1) <= self.max_broken_rate
                    )
                if pd.notna(row.get("theme_first_board_ma5", pd.NA)):
                    sentiment_thaw = sentiment_thaw or (
                        row.get("theme_first_board", 0) >= max(8, row.get("theme_first_board_ma5", 0))
                    )
                sentiment_follow_ok = (
                    sentiment_ok
                    and row.get("theme_sentiment_score", 0) >= self.min_sentiment_score + 8
                    and row.get("theme_up_down_ratio", 0) >= self.min_up_down_ratio + 0.5
                    and row.get("theme_broken_rate", 1) <= self.max_broken_rate - 0.05
                )

            industry_ok = True
            if "industry_flow_rank" in df.columns and pd.notna(row.get("industry_flow_rank", pd.NA)):
                industry_ok = row.get("industry_flow_rank", pd.NA) <= self.industry_rank_ceiling
            if "industry_flow_net_amount_rate_ma3" in df.columns and pd.notna(row.get("industry_flow_net_amount_rate_ma3", pd.NA)):
                industry_ok = industry_ok and row.get("industry_flow_net_amount_rate_ma3", pd.NA) > -1.5

            financial_ok = True
            if pd.notna(row.get("financial_health_score", pd.NA)):
                financial_ok = row.get("financial_health_score", pd.NA) >= 0.45
            leader_reclaim_ok = (
                (leader_support_ok or core_trend_candidate)
                and financial_ok
                and (sentiment_relaxed_ok or heat_ok)
                and strong_stock_ok
                and board_support_relaxed
                and not board_overheated
                and (
                    pullback_reclaim_ok
                    or (
                        row["close"] >= row["ma5"]
                        and row["dif"] >= row["dea"]
                        and row["macd"] > prev_row["macd"]
                        and row.get("amount_ratio", 1.0) >= 1.0
                        and rebound_volume_ok
                    )
                )
            )
            trend_relaxed_ok = (
                row["close"] >= row["ema_fast_trend"] * 0.99
                and row["close"] >= row["ema_slow_trend"] * 0.98
                and row["dif"] >= row["dea"] * 0.98
            )
            non_board_core_entry_ok = (
                financial_ok
                and core_trend_candidate
                and trend_relaxed_ok
                and market_risk_ok
                and (industry_ok or pd.notna(leader_rank) and leader_rank <= self.leader_rank_ceiling + 2)
                and heat_ok
                and not heat_cooling
                and not board_overheated
                and abs(gap_pct) <= self.max_gap_pct * 1.5
                and (breakout_ok or pullback_reclaim_ok)
            )
            strongest_core_ok = (
                (leader_candidate or (pd.notna(leader_rank) and leader_rank <= 2) or (pd.notna(leader_score) and leader_score >= 75))
                and core_trend_candidate
                and heat_ok
                and not heat_cooling
            )
            add_position_ok = (
                in_position
                and current_target < self.max_position - 1e-8
                and strongest_core_ok
                and market_risk_ok
                and abs(gap_pct) <= self.max_gap_pct * 1.5
                and (
                    breakout_ok
                    or (
                        row["close"] >= row["ma5"]
                        and row["macd"] >= prev_row["macd"]
                        and row.get("amount_ratio", 1.0) >= 1.0
                    )
                )
            )

            if in_position:
                holding_days += 1
                if leader_fade:
                    leader_fade_streak += 1
                else:
                    leader_fade_streak = 0
                highest_close = max(highest_close, row["close"])
                if (
                    current_target > self.trim_to_trial_position + 1e-8
                    and (
                        (row["rsi"] >= 80 and row["macd"] < prev_row["macd"])
                        or (heat_cooling and row["close"] < row["ma5"])
                        or (leader_fade and row["close"] < row["ema_fast_trend"])
                    )
                ):
                    next_target = max(self.trim_to_trial_position, current_target - self.add_step)
                    df.loc[next_index, "target_position"] = next_target
                    df.loc[next_index, "signal"] = -1
                    df.loc[next_index, "reason"] = "题材核心短线转弱，先降到试错仓位观察"
                    continue
                fast_fail_stop = entry_price * (1 - self.fast_fail_loss_limit)
                breakout_fail_stop = entry_price * (1 - self.breakout_fail_loss_limit)
                if (
                    holding_days <= self.max_fast_fail_holding_days
                    and row["close"] < fast_fail_stop
                    and row["close"] < row["ema_fast_trend"]
                ):
                    df.loc[next_index, "target_position"] = 0.0
                    df.loc[next_index, "signal"] = -1
                    df.loc[next_index, "reason"] = (
                        f"题材试错买入后快速走坏，收盘跌破快撤阈值 {fast_fail_stop:.2f}，建议明日开盘卖出"
                    )
                    in_position = False
                    entry_price = None
                    entry_atr = None
                    highest_close = None
                    entry_style = None
                    holding_days = 0
                    leader_fade_streak = 0
                    continue
                if (
                    entry_style == "breakout"
                    and holding_days <= self.max_fast_fail_holding_days
                    and row["close"] < breakout_fail_stop
                    and row["close"] < row["ma5"]
                ):
                    df.loc[next_index, "target_position"] = 0.0
                    df.loc[next_index, "signal"] = -1
                    df.loc[next_index, "reason"] = (
                        f"题材突破买入后未能站稳，收盘跌破失败阈值 {breakout_fail_stop:.2f}，建议明日开盘卖出"
                    )
                    in_position = False
                    entry_price = None
                    entry_atr = None
                    highest_close = None
                    entry_style = None
                    holding_days = 0
                    leader_fade_streak = 0
                    continue
                if (
                    holding_days <= self.max_fast_fail_holding_days
                    and row["close"] < entry_price
                    and row.get("amount_ratio", 1.0) < 0.95
                    and row["macd"] < prev_row["macd"]
                    and row["close"] < row["ema_fast_trend"]
                    and (
                        (
                            row.get("relative_strength_10", 0.0) < 0
                            and (
                                row["close"] < row["ema_slow_trend"]
                                or (pd.notna(leader_rank) and leader_rank > self.leader_rank_ceiling + 2)
                            )
                        )
                        or leader_fade
                        or (
                            heat_cooling
                            and row.get("theme_sentiment_score", 0) < self.relaxed_sentiment_score
                        )
                    )
                ):
                    df.loc[next_index, "target_position"] = 0.0
                    df.loc[next_index, "signal"] = -1
                    df.loc[next_index, "reason"] = "题材试错买入后量能未跟上且动能转弱，建议明日开盘卖出"
                    in_position = False
                    entry_price = None
                    entry_atr = None
                    highest_close = None
                    entry_style = None
                    holding_days = 0
                    leader_fade_streak = 0
                    continue
                trailing_stop = max(
                    entry_price - self.atr_stop_multiplier * entry_atr,
                    highest_close - self.atr_trail_multiplier * row["atr"],
                )
                if row["close"] < trailing_stop:
                    df.loc[next_index, "target_position"] = 0.0
                    df.loc[next_index, "signal"] = -1
                    df.loc[next_index, "reason"] = f"跌破题材股ATR保护位 {trailing_stop:.2f}，建议明日开盘卖出"
                    in_position = False
                    entry_price = None
                    entry_atr = None
                    highest_close = None
                    entry_style = None
                    holding_days = 0
                    continue
                if not sentiment_ok and row["close"] < row["ema_fast_trend"]:
                    df.loc[next_index, "target_position"] = 0.0
                    df.loc[next_index, "signal"] = -1
                    df.loc[next_index, "reason"] = "题材情绪明显降温，且个股跌回短趋势下方，建议明日开盘卖出"
                    in_position = False
                    entry_price = None
                    entry_atr = None
                    highest_close = None
                    entry_style = None
                    holding_days = 0
                    continue
                if leader_fade_streak >= 2 and row["close"] < row["ema_fast_trend"]:
                    df.loc[next_index, "target_position"] = 0.0
                    df.loc[next_index, "signal"] = -1
                    df.loc[next_index, "reason"] = "题材龙头强度连续回落，且跌回短趋势下方，建议明日开盘卖出"
                    in_position = False
                    entry_price = None
                    entry_atr = None
                    highest_close = None
                    entry_style = None
                    holding_days = 0
                    leader_fade_streak = 0
                    continue
                if (
                    row.get("theme_is_broken", 0) >= 1
                    and row.get("theme_is_reseal", 0) < 1
                    and row["close"] < row["ma5"]
                ):
                    df.loc[next_index, "target_position"] = 0.0
                    df.loc[next_index, "signal"] = -1
                    df.loc[next_index, "reason"] = "题材核心出现炸板且收盘跌回5日线下方，建议明日开盘卖出"
                    in_position = False
                    entry_price = None
                    entry_atr = None
                    highest_close = None
                    entry_style = None
                    holding_days = 0
                    leader_fade_streak = 0
                    continue
                if leader_fade_streak >= 2 and heat_cooling:
                    df.loc[next_index, "target_position"] = 0.0
                    df.loc[next_index, "signal"] = -1
                    df.loc[next_index, "reason"] = "题材龙头强度和连板热度同步走弱，建议明日开盘卖出"
                    in_position = False
                    entry_price = None
                    entry_atr = None
                    highest_close = None
                    entry_style = None
                    holding_days = 0
                    leader_fade_streak = 0
                    continue
                if cross_down and row["close"] < row["ma10"]:
                    df.loc[next_index, "target_position"] = 0.0
                    df.loc[next_index, "signal"] = -1
                    df.loc[next_index, "reason"] = "MACD死叉且跌破10日线，建议明日开盘卖出"
                    in_position = False
                    entry_price = None
                    entry_atr = None
                    highest_close = None
                    entry_style = None
                    holding_days = 0
                    continue
                if row["rsi"] >= 84 and row["macd"] < prev_row["macd"]:
                    df.loc[next_index, "signal"] = -1
                    if current_target > self.trim_to_trial_position + 1e-8:
                        df.loc[next_index, "target_position"] = max(self.trim_to_trial_position, current_target - self.add_step)
                        df.loc[next_index, "reason"] = "题材股短线过热且动能转弱，先减仓锁定利润"
                    else:
                        df.loc[next_index, "target_position"] = 0.0
                        df.loc[next_index, "reason"] = "题材股短线过热且动能转弱，建议明日开盘卖出"
                        in_position = False
                        entry_price = None
                        entry_atr = None
                        highest_close = None
                        entry_style = None
                        holding_days = 0
                    continue
                if add_position_ok:
                    next_target = min(self.max_position, max(self.core_entry_position, current_target + self.add_step))
                    if next_target > current_target + 1e-8:
                        df.loc[next_index, "target_position"] = next_target
                        df.loc[next_index, "signal"] = 1
                        df.loc[next_index, "reason"] = "题材最强核心继续强化，确认后加仓"
                        continue

            if (
                not in_position
                and trend_ok
                and self.min_rsi <= row["rsi"] <= self.max_rsi
                and market_trend_ok
                and market_risk_ok
                and (
                    sentiment_hot
                    or sentiment_thaw
                    or ladder_heat_ok
                    or (sentiment_follow_ok and leader_candidate and breakout_ok)
                )
                and industry_ok
                and strong_stock_ok
                and leader_support_ok
                and (board_core_ok or ladder_front_ok)
                and not board_overheated
                and abs(gap_pct) <= self.max_gap_pct
                and volume_support_ok
                and (breakout_ok or pullback_reclaim_ok)
            ):
                entry_target = self.core_entry_position if strongest_core_ok else self.trial_position
                df.loc[next_index, "target_position"] = min(self.max_position, entry_target)
                df.loc[next_index, "signal"] = 1
                if row.get("theme_is_reseal", 0) >= 1 and ladder_heat_ok:
                    df.loc[next_index, "reason"] = "题材梯队回封且连板热度回暖，先建仓跟踪"
                elif row.get("theme_is_second_board", 0) >= 1 and ladder_heat_ok:
                    df.loc[next_index, "reason"] = "题材核心晋级二板且梯队热度增强，先建仓跟踪"
                elif row.get("theme_is_first_board", 0) >= 1 and sentiment_thaw:
                    df.loc[next_index, "reason"] = "题材情绪转暖且个股走出首板，先半仓试错"
                elif sentiment_hot and breakout_ok and leader_candidate:
                    df.loc[next_index, "reason"] = "题材情绪回暖，板块最强核心放量突破，先建仓跟随"
                elif sentiment_follow_ok and leader_candidate and breakout_ok:
                    df.loc[next_index, "reason"] = "题材情绪偏暖，最强核心继续放量突破，先建仓跟随"
                elif row.get("theme_is_limit_up", 0) >= 1 and pullback_reclaim_ok:
                    df.loc[next_index, "reason"] = "题材核心涨停后保持强势，先建仓跟踪"
                elif sentiment_thaw and breakout_ok:
                    df.loc[next_index, "reason"] = "题材情绪从低位回暖，强势核心开始突破，先建仓买入"
                elif sentiment_hot and breakout_ok:
                    df.loc[next_index, "reason"] = "题材情绪回暖且强势股放量突破，先建仓跟随"
                elif pullback_reclaim_ok:
                    df.loc[next_index, "reason"] = "题材情绪回暖，强势核心回踩后重新走强，先半仓试错"
                else:
                    df.loc[next_index, "reason"] = "题材股趋势和板块情绪共振，先建仓试错"
                pending_entry_atr = row["atr"]
                pending_entry_style = "breakout" if breakout_ok else "reclaim"
            elif (
                not in_position
                and trend_ok
                and market_risk_ok
                and (industry_ok or (pd.notna(leader_score) and leader_score >= 80))
                and abs(gap_pct) <= self.max_gap_pct * 1.5
                and volume_support_ok
                and leader_reclaim_ok
                and (ladder_heat_ok or row.get("theme_is_reseal", 0) >= 1 or row.get("theme_board_count", 0) >= 2)
            ):
                df.loc[next_index, "target_position"] = self.trial_position
                df.loc[next_index, "signal"] = 1
                if row.get("theme_is_reseal", 0) >= 1:
                    df.loc[next_index, "reason"] = "题材核心炸板回封后量能恢复，且财报未坏，先半仓试错"
                else:
                    df.loc[next_index, "reason"] = "题材核心股回踩后量能恢复、梯队未退潮，且财报未坏，先半仓试错"
                pending_entry_atr = row["atr"]
                pending_entry_style = "reclaim"
            elif (
                not in_position
                and non_board_core_entry_ok
            ):
                df.loc[next_index, "target_position"] = self.trial_position
                df.loc[next_index, "signal"] = 1
                if breakout_ok:
                    df.loc[next_index, "reason"] = "题材强趋势核心放量突破，虽然未封板，但热度未退潮，先半仓试错"
                else:
                    df.loc[next_index, "reason"] = "题材强趋势核心回踩后量能恢复，虽然未封板，但板块热度仍在，先半仓试错"
                pending_entry_atr = row["atr"]
                pending_entry_style = "breakout" if breakout_ok else "reclaim"

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
        last_target_value = self._safe_target_value(last_targets.iloc[-1], 0.0) if not last_targets.empty else 0.0
        next_row.loc[:, "target_position"] = last_target_value
        preview_df = pd.concat([trade_df, next_row], ignore_index=True)
        preview_df = self.trading_strategy(preview_df)

        last_row = preview_df.iloc[-2]
        future_row = preview_df.iloc[-1]
        current_target = last_target_value
        future_target = future_row.get("target_position", pd.NA)
        next_target = self._safe_target_value(future_target, current_target)
        next_reason = future_row["reason"]

        if next_target > current_target + 1e-8:
            signal = "加仓" if current_target > 0 else "建仓"
        elif next_target < current_target - 1e-8:
            signal = "减仓" if next_target > 0 else "清仓"
        elif current_target > 0:
            signal = "持仓"
            next_reason = "题材核心仍在强势区间，继续持仓观察"
        else:
            signal = "观望"
            if (
                last_row["close"] > last_row["ema_slow_trend"]
                and last_row["dif"] > last_row["dea"]
                and last_row.get("relative_strength_10", 0) > 0
            ):
                next_reason = "题材强度还在，但情绪或位置还没给出理想上车点，继续观察"
            else:
                next_reason = "题材情绪、板块资金和个股趋势没有形成共振，先观望"

        return {
            "signal": signal,
            "reason": next_reason,
            "dif": last_row["dif"],
            "dea": last_row["dea"],
            "macd": last_row["macd"],
            "last_trade_date": last_row["trade_date"],
            "target_position": next_target,
        }
