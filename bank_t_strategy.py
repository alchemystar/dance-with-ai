import pandas as pd

from macd_with_regime_filter import (
    calculate_market_activity_features,
    calculate_market_regime_features,
)


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


class bank_t_strategy:
    def __init__(
        self,
        base_position=0.3,
        macd_bull_base=0.5,
        macd_neutral_base=0.3,
        macd_bear_base=0.1,
        min_position=0.0,
        max_position=0.9,
        add_step=0.1,
        trim_step=0.1,
        atr_period=14,
        oversold_zscore=-1.0,
        deep_oversold_zscore=-1.5,
        overbought_zscore=1.0,
        strong_overbought_zscore=1.7,
        oversold_rsi=40,
        deep_oversold_rsi=35,
        overbought_rsi=60,
        strong_overbought_rsi=70,
        market_flow_floor=-3.5,
        industry_flow_floor=-4.0,
        market_trend_buffer=0.985,
        defensive_ma60_buffer=0.97,
        pb_cheap_buy_floor=50,
        pb_expensive_trim_ceiling=20,
        min_dividend_yield=4.5,
        min_dividend_stability=0.8,
        min_financial_health_score=0.6,
        min_financial_quality_score=20,
        min_profit_yoy=-10,
        min_revenue_yoy=-5,
        min_roe=8,
        weak_profit_yoy_exit=-20,
        weak_revenue_yoy_exit=-10,
        weak_roe_exit=6,
        display_name=None,
    ):
        self.base_position = base_position
        self.macd_bull_base = macd_bull_base
        self.macd_neutral_base = macd_neutral_base
        self.macd_bear_base = macd_bear_base
        self.min_position = min_position
        self.max_position = max_position
        self.add_step = add_step
        self.trim_step = trim_step
        self.atr_period = atr_period
        self.oversold_zscore = oversold_zscore
        self.deep_oversold_zscore = deep_oversold_zscore
        self.overbought_zscore = overbought_zscore
        self.strong_overbought_zscore = strong_overbought_zscore
        self.oversold_rsi = oversold_rsi
        self.deep_oversold_rsi = deep_oversold_rsi
        self.overbought_rsi = overbought_rsi
        self.strong_overbought_rsi = strong_overbought_rsi
        self.market_flow_floor = market_flow_floor
        self.industry_flow_floor = industry_flow_floor
        self.market_trend_buffer = market_trend_buffer
        self.defensive_ma60_buffer = defensive_ma60_buffer
        self.pb_cheap_buy_floor = pb_cheap_buy_floor
        self.pb_expensive_trim_ceiling = pb_expensive_trim_ceiling
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
        self.display_name = display_name or self.__class__.__name__

    def trading_strategy(self, df):
        df = df.copy()
        df = calculate_macd(df)
        df = calculate_rsi(df)
        df = calculate_atr(df, self.atr_period)
        df = calculate_market_regime_features(df, "sh_index")
        df = calculate_market_regime_features(df, "csi300")
        df = calculate_market_activity_features(df)

        df["ma10"] = df["close"].rolling(window=10).mean()
        df["ma20"] = df["close"].rolling(window=20).mean()
        df["ma60"] = df["close"].rolling(window=60).mean()
        df["std20"] = df["close"].rolling(window=20).std()
        df["bb_upper"] = df["ma20"] + 2 * df["std20"]
        df["bb_lower"] = df["ma20"] - 2 * df["std20"]
        df["zscore20"] = (df["close"] - df["ma20"]) / df["std20"].replace(0, pd.NA)

        df["signal"] = 0
        df["reason"] = "观望"
        df["target_position"] = 0.0
        df["market_regime_score"] = pd.NA
        df["market_position_cap"] = pd.NA
        df["macd_regime"] = "neutral"
        df["macd_position_anchor"] = pd.NA
        df["bank_value_score"] = pd.NA
        df["bank_buy_timing_score"] = pd.NA

        warmup = 60
        for i in range(warmup, len(df) - 1):
            prev_row = df.iloc[i - 1]
            row = df.iloc[i]
            next_index = i + 1
            current_target = float(df["target_position"].iloc[i])
            next_target = current_target
            reason = "维持当前仓位"

            has_index_data = all(
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
            if has_index_data:
                market_trend_ok = (
                    pd.notna(row.get("sh_index_close", pd.NA))
                    and pd.notna(row.get("sh_index_ema_fast", pd.NA))
                    and pd.notna(row.get("sh_index_ema_slow", pd.NA))
                    and row.get("sh_index_close", pd.NA) >= row.get("sh_index_ema_fast", pd.NA) * 0.995
                    and row.get("sh_index_ema_fast", pd.NA) >= row.get("sh_index_ema_slow", pd.NA) * self.market_trend_buffer
                    and row.get("csi300_close", pd.NA) >= row.get("csi300_ema_slow", pd.NA) * self.market_trend_buffer
                )
                market_momentum_ok = (
                    row.get("sh_index_dif", pd.NA) >= row.get("sh_index_dea", pd.NA)
                    and row.get("csi300_macd", pd.NA) >= -0.02
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

            has_activity_data = all(
                col in df.columns
                for col in [
                    "market_amount_total",
                    "market_amount_ma20",
                    "market_turnover_avg",
                    "market_turnover_ma5",
                ]
            )
            if has_activity_data:
                market_liquidity_ok = (
                    pd.notna(row.get("market_amount_ma20", pd.NA))
                    and pd.notna(row.get("market_turnover_ma5", pd.NA))
                    and row.get("market_amount_total", pd.NA) >= row.get("market_amount_ma20", pd.NA) * 0.9
                    and row.get("market_turnover_avg", pd.NA) >= row.get("market_turnover_ma5", pd.NA) * 0.95
                )
            else:
                market_liquidity_ok = True

            has_moneyflow_data = "market_net_amount_rate" in df.columns
            if has_moneyflow_data and pd.notna(row.get("market_net_amount_rate", pd.NA)):
                market_flow_ok = row.get("market_net_amount_rate", pd.NA) > self.market_flow_floor
                if pd.notna(row.get("market_net_amount_rate_ma3", pd.NA)):
                    market_flow_ok = (
                        market_flow_ok
                        and row.get("market_net_amount_rate_ma3", pd.NA) > self.market_flow_floor + 0.8
                    )
                if pd.notna(row.get("market_big_order_rate_ma3", pd.NA)):
                    market_flow_ok = (
                        market_flow_ok
                        and row.get("market_big_order_rate_ma3", pd.NA) > -1.5
                    )
            else:
                market_flow_ok = True

            market_score = sum(
                [
                    1 if market_trend_ok else 0,
                    1 if market_momentum_ok else 0,
                    1 if market_risk_ok else 0,
                    1 if market_liquidity_ok else 0,
                    1 if market_flow_ok else 0,
                ]
            )
            if market_score >= 5:
                market_position_cap = self.max_position
            elif market_score == 4:
                market_position_cap = min(self.max_position, self.base_position + self.add_step * 2)
            elif market_score == 3:
                market_position_cap = min(self.max_position, self.base_position + self.add_step)
            elif market_score == 2:
                market_position_cap = self.base_position
            elif market_score == 1:
                market_position_cap = min(self.base_position, 0.2)
            else:
                market_position_cap = self.min_position

            industry_flow_ok = True
            if "industry_flow_net_amount_rate" in df.columns and pd.notna(row.get("industry_flow_net_amount_rate", pd.NA)):
                industry_flow_ok = row.get("industry_flow_net_amount_rate", pd.NA) > self.industry_flow_floor

            pb_cheap_ok = True
            if pd.notna(row.get("bank_pb_percentile_3y", pd.NA)):
                pb_cheap_ok = row.get("bank_pb_percentile_3y", pd.NA) >= self.pb_cheap_buy_floor

            dividend_yield_ok = True
            if pd.notna(row.get("bank_dv_ttm", pd.NA)):
                dividend_yield_ok = row.get("bank_dv_ttm", pd.NA) >= self.min_dividend_yield

            dividend_stability_ok = True
            if pd.notna(row.get("bank_dividend_stability", pd.NA)):
                dividend_stability_ok = row.get("bank_dividend_stability", pd.NA) >= self.min_dividend_stability

            value_support_ok = pb_cheap_ok or (dividend_yield_ok and dividend_stability_ok)
            valuation_trim_ok = True
            if pd.notna(row.get("bank_pb_percentile_3y", pd.NA)):
                valuation_trim_ok = row.get("bank_pb_percentile_3y", pd.NA) > self.pb_expensive_trim_ceiling

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

            value_score = 0
            if pd.notna(row.get("bank_pb_percentile_3y", pd.NA)):
                if row.get("bank_pb_percentile_3y", pd.NA) >= self.pb_cheap_buy_floor + 10:
                    value_score += 2
                elif row.get("bank_pb_percentile_3y", pd.NA) >= self.pb_cheap_buy_floor:
                    value_score += 1
            if dividend_yield_ok:
                value_score += 1
            if dividend_stability_ok:
                value_score += 1
            if financial_support_ok:
                value_score += 2
            elif financial_health_score >= max(0.5, self.min_financial_health_score - 0.1):
                value_score += 1
            if pd.notna(row.get("financial_quality_score", pd.NA)) and row.get("financial_quality_score", pd.NA) >= self.min_financial_quality_score + 20:
                value_score += 1

            macd_cross_up = prev_row["dif"] <= prev_row["dea"] and row["dif"] > row["dea"]
            macd_cross_down = prev_row["dif"] >= prev_row["dea"] and row["dif"] < row["dea"]
            macd_hist_rising = row["macd"] > prev_row["macd"]
            macd_hist_falling = row["macd"] < prev_row["macd"]
            price_trend_ok = pd.notna(row["ma20"]) and row["close"] >= row["ma20"]

            if row["dif"] > row["dea"] and row["macd"] >= 0 and macd_hist_rising and price_trend_ok:
                macd_regime = "bull"
                macd_position_anchor = max(self.base_position, self.macd_bull_base)
            elif row["dif"] > row["dea"] or macd_cross_up:
                macd_regime = "neutral"
                macd_position_anchor = max(self.base_position, self.macd_neutral_base)
            elif row["dif"] < row["dea"] and row["macd"] < 0 and macd_hist_falling and row["close"] < row["ma20"]:
                macd_regime = "bear"
                macd_position_anchor = min(self.base_position, self.macd_bear_base)
            else:
                macd_regime = "neutral"
                macd_position_anchor = max(self.base_position * 0.8, self.macd_neutral_base)

            if financial_warning:
                market_position_cap = min(market_position_cap, max(self.min_position, self.base_position * 0.5))
            elif not financial_support_ok:
                market_position_cap = min(market_position_cap, max(self.min_position, self.base_position))

            oversold = (
                pd.notna(row["zscore20"])
                and row["zscore20"] <= self.oversold_zscore
                and pd.notna(row["rsi"])
                and row["rsi"] <= self.oversold_rsi
            )
            deep_oversold = (
                pd.notna(row["zscore20"])
                and row["zscore20"] <= self.deep_oversold_zscore
                and pd.notna(row["rsi"])
                and row["rsi"] <= self.deep_oversold_rsi
            )
            overbought = (
                pd.notna(row["zscore20"])
                and row["zscore20"] >= self.overbought_zscore
                and pd.notna(row["rsi"])
                and row["rsi"] >= self.overbought_rsi
            )
            strong_overbought = (
                pd.notna(row["zscore20"])
                and row["zscore20"] >= self.strong_overbought_zscore
                and pd.notna(row["rsi"])
                and row["rsi"] >= self.strong_overbought_rsi
            )
            low_point_ready = (
                oversold
                and (
                    (pd.notna(row["ma60"]) and row["close"] >= row["ma60"] * 0.96)
                    or macd_cross_up
                    or (row["dif"] >= row["dea"] and macd_hist_rising)
                )
            )
            deep_low_point_ready = (
                deep_oversold
                and (
                    (pd.notna(row["ma60"]) and row["close"] >= row["ma60"] * 0.94)
                    or (row["dif"] >= row["dea"] and macd_hist_rising)
                )
            )
            timing_score = 0
            if oversold:
                timing_score += 1
            if deep_oversold:
                timing_score += 1
            if low_point_ready:
                timing_score += 1
            if macd_cross_up or (row["dif"] >= row["dea"] and macd_hist_rising):
                timing_score += 1

            defensive_exit = (
                pd.notna(row["ma60"])
                and row["close"] < row["ma60"] * self.defensive_ma60_buffer
                and macd_regime == "bear"
                and (
                    market_score <= 1
                    or not market_risk_ok
                    or not industry_flow_ok
                    or not dividend_stability_ok
                    or financial_warning
                )
            )

            if defensive_exit and current_target > 0:
                next_target = self.min_position
                reason = "MACD空头且大盘/行业/财报因子转弱，先清仓避险"
            elif financial_warning and current_target > self.min_position:
                next_target = max(self.min_position, min(market_position_cap, self.macd_bear_base))
                reason = "最新财报明显转弱，先把银行仓位降到防守档位"
            elif (
                current_target > 0
                and macd_regime == "bull"
                and current_target < min(market_position_cap, macd_position_anchor)
                and value_score >= 3
            ):
                next_target = min(market_position_cap, macd_position_anchor)
                reason = "持仓中的银行股继续走强，且财报低估逻辑未破，顺势把底仓提到多头档位"
            elif macd_regime == "bear" and current_target > min(market_position_cap, macd_position_anchor):
                next_target = min(market_position_cap, macd_position_anchor)
                reason = "MACD转弱或财报支撑不足，先把底仓降到防守档位"
            elif (
                current_target <= 0
                and low_point_ready
                and macd_regime != "bear"
                and market_score >= 2
                and industry_flow_ok
                and value_score >= 4
                and financial_support_ok
            ):
                next_target = min(market_position_cap, macd_position_anchor)
                reason = "财报和估值确认偏低估，且回踩到低点区域，开始建立银行底仓"
            elif (
                current_target < market_position_cap
                and deep_low_point_ready
                and macd_regime in {"bull", "neutral"}
                and market_score >= 3
                and industry_flow_ok
                and value_score >= 5
                and financial_support_ok
            ):
                next_target = min(market_position_cap, current_target + self.add_step)
                reason = "财报低估值优势明显，且价格再次回到深低点，分批加仓"
            elif (
                current_target > min(market_position_cap, macd_position_anchor)
                and (overbought or macd_cross_down)
                and (
                    market_score <= 2
                    or not value_support_ok
                    or not valuation_trim_ok
                    or macd_hist_falling
                    or not financial_support_ok
                )
            ):
                next_target = max(min(market_position_cap, macd_position_anchor), current_target - self.trim_step)
                reason = "MACD动能转弱，或估值/财报优势收敛，减仓锁定做T价差"
            elif (
                current_target > 0
                and strong_overbought
                and (market_score <= 3 or not value_support_ok or not valuation_trim_ok or not financial_support_ok)
            ):
                next_target = max(self.min_position, current_target - self.trim_step)
                reason = "银行股短线过热，且MACD/估值/财报/大盘不再配合，继续减仓等待回补"
            elif (
                current_target <= 0
                and market_score >= 3
                and macd_regime in {"bull", "neutral"}
                and industry_flow_ok
                and value_score >= 5
                and financial_support_ok
                and low_point_ready
                and row["dif"] > row["dea"]
            ):
                next_target = min(market_position_cap, macd_position_anchor)
                reason = "财报低估值和低点信号同时出现，大盘回稳后回补底仓"

            next_target = max(self.min_position, min(market_position_cap, next_target))
            df.loc[next_index, "target_position"] = next_target
            df.loc[next_index, "market_regime_score"] = market_score
            df.loc[next_index, "market_position_cap"] = market_position_cap
            df.loc[next_index, "macd_regime"] = macd_regime
            df.loc[next_index, "macd_position_anchor"] = macd_position_anchor
            df.loc[next_index, "bank_value_score"] = value_score
            df.loc[next_index, "bank_buy_timing_score"] = timing_score
            if next_target > current_target + 1e-8:
                df.loc[next_index, "signal"] = 1
            elif next_target < current_target - 1e-8:
                df.loc[next_index, "signal"] = -1
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
        next_row.loc[:, "target_position"] = trade_df["target_position"].iloc[-1]
        preview_df = pd.concat([trade_df, next_row], ignore_index=True)
        preview_df = self.trading_strategy(preview_df)

        last_row = preview_df.iloc[-2]
        next_row = preview_df.iloc[-1]
        current_target = float(last_row.get("target_position", 0) or 0)
        next_target = float(next_row.get("target_position", current_target) or current_target)

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
            "reason": next_row["reason"],
            "dif": last_row["dif"],
            "dea": last_row["dea"],
            "macd": last_row["macd"],
            "last_trade_date": last_row["trade_date"],
            "target_position": next_target,
        }
