import pandas as pd


def calculate_macd(df, fast=12, slow=26, signal=9):
    exp1 = df["close"].ewm(span=fast, adjust=False).mean()
    exp2 = df["close"].ewm(span=slow, adjust=False).mean()
    df["dif"] = exp1 - exp2
    df["dea"] = df["dif"].ewm(span=signal, adjust=False).mean()
    df["macd"] = (df["dif"] - df["dea"]) * 2
    return df


def calculate_rsi(df, period=14):
    delta = df["close"].diff()
    gain = delta.clip(lower=0).rolling(window=period).mean()
    loss = (-delta.clip(upper=0)).rolling(window=period).mean()
    rs = gain / loss.replace(0, pd.NA)
    df["rsi"] = 100 - (100 / (1 + rs))
    return df


def calculate_atr(df, period=14):
    prev_close = df["close"].shift(1)
    tr = pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    df["atr"] = tr.rolling(window=period).mean()
    return df


def calculate_market_regime_features(df, prefix):
    close_col = f"{prefix}_close"
    if close_col not in df.columns:
        return df

    ema_fast_col = f"{prefix}_ema_fast"
    ema_slow_col = f"{prefix}_ema_slow"
    ret_5_col = f"{prefix}_ret_5"
    dif_col = f"{prefix}_dif"
    dea_col = f"{prefix}_dea"
    macd_col = f"{prefix}_macd"

    df[ema_fast_col] = df[close_col].ewm(span=20, adjust=False).mean()
    df[ema_slow_col] = df[close_col].ewm(span=60, adjust=False).mean()
    df[ret_5_col] = df[close_col].pct_change(5, fill_method=None)

    exp1 = df[close_col].ewm(span=12, adjust=False).mean()
    exp2 = df[close_col].ewm(span=26, adjust=False).mean()
    df[dif_col] = exp1 - exp2
    df[dea_col] = df[dif_col].ewm(span=9, adjust=False).mean()
    df[macd_col] = (df[dif_col] - df[dea_col]) * 2
    return df


def calculate_market_activity_features(df):
    if "market_amount_total" in df.columns:
        df["market_amount_ma5"] = df["market_amount_total"].rolling(window=5).mean()
        df["market_amount_ma20"] = df["market_amount_total"].rolling(window=20).mean()
        df["market_amount_ratio"] = df["market_amount_total"] / df["market_amount_ma20"].replace(0, pd.NA)

    if "market_turnover_avg" in df.columns:
        df["market_turnover_ma5"] = df["market_turnover_avg"].rolling(window=5).mean()

    if "market_net_amount_rate" in df.columns:
        df["market_net_amount_rate_ma3"] = df["market_net_amount_rate"].rolling(window=3).mean()

    if all(
        col in df.columns
        for col in [
            "market_buy_elg_amount_rate",
            "market_buy_lg_amount_rate",
        ]
    ):
        df["market_big_order_rate"] = (
            df["market_buy_elg_amount_rate"] + df["market_buy_lg_amount_rate"]
        )
        df["market_big_order_rate_ma3"] = df["market_big_order_rate"].rolling(window=3).mean()

    return df


def calculate_industry_flow_features(df):
    if "industry_flow_net_amount_rate" in df.columns:
        df["industry_flow_net_amount_rate_ma3"] = (
            df["industry_flow_net_amount_rate"].rolling(window=3).mean()
        )

    if all(
        col in df.columns
        for col in [
            "industry_flow_buy_elg_amount_rate",
            "industry_flow_buy_lg_amount_rate",
        ]
    ):
        df["industry_flow_big_order_rate"] = (
            df["industry_flow_buy_elg_amount_rate"] + df["industry_flow_buy_lg_amount_rate"]
        )
        df["industry_flow_big_order_rate_ma3"] = (
            df["industry_flow_big_order_rate"].rolling(window=3).mean()
        )

    return df


def _safe_gt(left, right):
    return pd.notna(left) and pd.notna(right) and left > right


class macd_with_regime_filter_strategy:
    def __init__(
        self,
        trend_fast=20,
        trend_slow=60,
        breakout_lookback=20,
        breakout_buffer=0.99,
        min_rsi=50,
        max_rsi=68,
        atr_period=14,
        atr_stop_multiplier=2.2,
        atr_trail_multiplier=2.8,
        max_gap_pct=0.03,
    ):
        self.trend_fast = trend_fast
        self.trend_slow = trend_slow
        self.breakout_lookback = breakout_lookback
        self.breakout_buffer = breakout_buffer
        self.min_rsi = min_rsi
        self.max_rsi = max_rsi
        self.atr_period = atr_period
        self.atr_stop_multiplier = atr_stop_multiplier
        self.atr_trail_multiplier = atr_trail_multiplier
        self.max_gap_pct = max_gap_pct

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
        df["rolling_high"] = (
            df["close"].rolling(window=self.breakout_lookback).max().shift(1)
        )
        df["signal"] = 0
        df["reason"] = "观望"

        in_position = False
        entry_price = None
        entry_atr = None
        highest_close = None
        pending_entry_atr = None

        warmup = max(26, self.trend_slow, self.breakout_lookback, self.atr_period)
        for i in range(warmup, len(df) - 1):
            prev_row = df.iloc[i - 1]
            row = df.iloc[i]
            next_index = i + 1

            if df["signal"].iloc[i] == 1 and not in_position:
                in_position = True
                entry_price = df["open"].iloc[i]
                entry_atr = pending_entry_atr if pending_entry_atr is not None else row["atr"]
                highest_close = max(entry_price, row["close"])
                pending_entry_atr = None
            elif df["signal"].iloc[i] == -1 and in_position:
                in_position = False
                entry_price = None
                entry_atr = None
                highest_close = None
                pending_entry_atr = None

            if pd.isna(row["atr"]) or pd.isna(row["rsi"]) or pd.isna(row["rolling_high"]):
                continue

            if in_position:
                highest_close = max(highest_close, row["close"])
                trailing_stop = max(
                    entry_price - self.atr_stop_multiplier * entry_atr,
                    highest_close - self.atr_trail_multiplier * row["atr"],
                )

                if row["close"] < trailing_stop:
                    df.loc[next_index, "signal"] = -1
                    df.loc[next_index, "reason"] = (
                        f"跌破ATR保护位 {trailing_stop:.2f}，建议明日开盘卖出"
                    )
                    in_position = False
                    entry_price = None
                    entry_atr = None
                    highest_close = None
                    continue

                if prev_row["dif"] >= prev_row["dea"] and row["dif"] < row["dea"]:
                    df.loc[next_index, "signal"] = -1
                    df.loc[next_index, "reason"] = "MACD死叉，趋势走弱，建议明日开盘卖出"
                    in_position = False
                    entry_price = None
                    entry_atr = None
                    highest_close = None
                    continue

                if row["close"] < row["ema_fast_trend"] and row["rsi"] < 45:
                    df.loc[next_index, "signal"] = -1
                    df.loc[next_index, "reason"] = "跌回短期趋势线下方，建议明日开盘卖出"
                    in_position = False
                    entry_price = None
                    entry_atr = None
                    highest_close = None
                    continue

                if row["rsi"] > 78 and row["macd"] < prev_row["macd"]:
                    df.loc[next_index, "signal"] = -1
                    df.loc[next_index, "reason"] = "短线过热且动能回落，建议明日开盘卖出"
                    in_position = False
                    entry_price = None
                    entry_atr = None
                    highest_close = None
                    continue

            gap_pct = (row["open"] - prev_row["close"]) / prev_row["close"]
            cross_up = prev_row["dif"] <= prev_row["dea"] and row["dif"] > row["dea"]
            trend_ok = (
                row["close"] > row["ema_fast_trend"] > row["ema_slow_trend"]
            )
            breakout_ok = row["close"] >= row["rolling_high"] * self.breakout_buffer
            rsi_ok = self.min_rsi <= row["rsi"] <= self.max_rsi
            momentum_ok = row["macd"] > prev_row["macd"]
            has_market_data = all(
                col in df.columns
                for col in [
                    "sh_index_close",
                    "sh_index_ema_fast",
                    "sh_index_ema_slow",
                    "sh_index_dif",
                    "sh_index_dea",
                    "sh_index_ret_5",
                    "csi300_close",
                    "csi300_ema_slow",
                    "csi300_macd",
                    "csi300_ret_5",
                ]
            )
            if has_market_data:
                market_trend_ok = (
                    _safe_gt(row.get("sh_index_close", pd.NA), row.get("sh_index_ema_fast", pd.NA))
                    and _safe_gt(row.get("sh_index_ema_fast", pd.NA), row.get("sh_index_ema_slow", pd.NA))
                    and _safe_gt(row.get("csi300_close", pd.NA), row.get("csi300_ema_slow", pd.NA))
                )
                market_momentum_ok = (
                    _safe_gt(row.get("sh_index_dif", pd.NA), row.get("sh_index_dea", pd.NA))
                    and _safe_gt(row.get("csi300_macd", pd.NA), 0)
                )
                market_risk_ok = (
                    pd.notna(row.get("sh_index_ret_5", pd.NA))
                    and pd.notna(row.get("csi300_ret_5", pd.NA))
                    and row.get("sh_index_ret_5", pd.NA) > -0.03
                    and row.get("csi300_ret_5", pd.NA) > -0.04
                )
            else:
                market_trend_ok = True
                market_momentum_ok = True
                market_risk_ok = True

            has_liquidity_data = all(
                col in df.columns
                for col in [
                    "market_amount_total",
                    "market_amount_ma20",
                ]
            )
            if has_liquidity_data and pd.notna(row.get("market_amount_ma20", pd.NA)):
                market_liquidity_ok = (
                    row.get("market_amount_total", pd.NA) >= row.get("market_amount_ma20", pd.NA) * 0.9
                )
            else:
                market_liquidity_ok = True

            has_turnover_data = all(
                col in df.columns
                for col in [
                    "market_turnover_avg",
                    "market_turnover_ma5",
                ]
            )
            if has_turnover_data and pd.notna(row.get("market_turnover_ma5", pd.NA)):
                market_turnover_ok = (
                    row.get("market_turnover_avg", pd.NA) >= row.get("market_turnover_ma5", pd.NA) * 0.95
                )
            else:
                market_turnover_ok = True

            has_moneyflow_data = "market_net_amount_rate" in df.columns
            if has_moneyflow_data and pd.notna(row.get("market_net_amount_rate", pd.NA)):
                market_fund_flow_ok = row.get("market_net_amount_rate", pd.NA) > -1.5
                if pd.notna(row.get("market_net_amount_rate_ma3", pd.NA)):
                    market_fund_flow_ok = (
                        market_fund_flow_ok
                        and row.get("market_net_amount_rate_ma3", pd.NA) > -1.0
                    )
                if pd.notna(row.get("market_big_order_rate", pd.NA)):
                    market_fund_flow_ok = (
                        market_fund_flow_ok
                        and row.get("market_big_order_rate", pd.NA) > -2.0
                    )
                if pd.notna(row.get("market_big_order_rate_ma3", pd.NA)):
                    market_fund_flow_ok = (
                        market_fund_flow_ok
                        and row.get("market_big_order_rate_ma3", pd.NA) > -1.5
                    )
            else:
                market_fund_flow_ok = True

            has_industry_flow_data = "industry_flow_net_amount_rate" in df.columns
            if has_industry_flow_data and pd.notna(row.get("industry_flow_net_amount_rate", pd.NA)):
                industry_flow_ok = row.get("industry_flow_net_amount_rate", pd.NA) > -3.0
                if pd.notna(row.get("industry_flow_net_amount_rate_ma3", pd.NA)):
                    industry_flow_ok = (
                        industry_flow_ok
                        and row.get("industry_flow_net_amount_rate_ma3", pd.NA) > -2.0
                    )
                if pd.notna(row.get("industry_flow_big_order_rate", pd.NA)):
                    industry_flow_ok = (
                        industry_flow_ok
                        and row.get("industry_flow_big_order_rate", pd.NA) > -3.5
                    )
                if pd.notna(row.get("industry_flow_big_order_rate_ma3", pd.NA)):
                    industry_flow_ok = (
                        industry_flow_ok
                        and row.get("industry_flow_big_order_rate_ma3", pd.NA) > -2.5
                    )
                if pd.notna(row.get("industry_flow_pct_change", pd.NA)):
                    industry_flow_ok = (
                        industry_flow_ok
                        and row.get("industry_flow_pct_change", pd.NA) > -3.5
                    )
            else:
                industry_flow_ok = True

            if (
                not in_position
                and cross_up
                and trend_ok
                and breakout_ok
                and rsi_ok
                and momentum_ok
                and market_trend_ok
                and market_momentum_ok
                and market_risk_ok
                and market_liquidity_ok
                and market_turnover_ok
                and market_fund_flow_ok
                and industry_flow_ok
                and abs(gap_pct) <= self.max_gap_pct
            ):
                df.loc[next_index, "signal"] = 1
                df.loc[next_index, "reason"] = "个股趋势向上 + 大盘环境配合，建议明日开盘买入"
                pending_entry_atr = row["atr"]

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
        last_trade_date = last_row["trade_date"]

        if next_signal == 1:
            signal = "买入"
        elif next_signal == -1:
            signal = "卖出"
        else:
            signal = "观望"
            if last_row["close"] > last_row["ema_slow_trend"] and last_row["dif"] > last_row["dea"]:
                next_reason = "趋势仍偏强，但还没到理想入场点，继续观察"
            else:
                next_reason = "趋势未走强、个股动能不足或大盘环境不配合，先观望"

        return {
            "signal": signal,
            "reason": next_reason,
            "dif": last_row["dif"],
            "dea": last_row["dea"],
            "macd": last_row["macd"],
            "last_trade_date": last_trade_date,
        }
