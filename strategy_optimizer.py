import argparse
from itertools import product

import pandas as pd

from backtest import (
    DEFAULT_STOCK_NAMES,
    backtest,
    enrich_with_market_context,
    fetch_stock_data,
    get_date_range,
    load_bank_segment_pools,
    load_bank_stock_pool,
    load_cycle_stock_pool,
)
from bank_t_strategy import bank_t_strategy
from cycle_with_industry_rotation import cycle_with_industry_rotation_strategy
from macd_with_regime_filter import macd_with_regime_filter_strategy

def build_param_grid(mode, bank_segment="all"):
    if mode == "regime":
        return {
            "trend_fast": [15, 20],
            "trend_slow": [55, 89],
            "breakout_buffer": [0.985, 0.995],
            "min_rsi": [48, 52],
            "max_rsi": [66, 70],
            "atr_stop_multiplier": [1.8, 2.2],
            "atr_trail_multiplier": [2.6, 3.2],
        }

    if mode == "cycle":
        return {
            "trend_fast": [20],
            "trend_slow": [55, 60],
            "breakout_lookback": [30, 40],
            "breakout_buffer": [0.985, 0.99],
            "pullback_buffer": [1.015, 1.02],
            "min_rsi": [50, 52],
            "max_rsi": [76, 80],
            "atr_stop_multiplier": [1.8, 2.0],
            "atr_trail_multiplier": [2.6, 2.8],
            "industry_flow_floor": [-1.5, -1.0],
            "min_relative_strength": [0.0],
        }

    if mode == "bank_t":
        if bank_segment == "state_owned":
            return {
                "base_position": [0.25, 0.3, 0.35],
                "macd_bull_base": [0.45, 0.5],
                "macd_neutral_base": [0.35],
                "macd_bear_base": [0.05, 0.1],
                "add_step": [0.1],
                "trim_step": [0.1],
                "oversold_zscore": [-0.8, -1.0],
                "deep_oversold_zscore": [-1.5],
                "overbought_zscore": [0.9],
                "oversold_rsi": [42],
                "overbought_rsi": [58],
                "max_position": [0.9],
                "pb_cheap_buy_floor": [35, 40],
                "pb_expensive_trim_ceiling": [20],
                "min_dividend_yield": [4.0, 4.5],
                "min_dividend_stability": [0.8],
                "min_financial_health_score": [0.5, 0.6],
                "min_financial_quality_score": [15],
                "min_profit_yoy": [-15, -10],
                "min_revenue_yoy": [-8],
                "min_roe": [7, 8],
                "weak_profit_yoy_exit": [-20],
                "weak_revenue_yoy_exit": [-10],
                "weak_roe_exit": [5],
            }

        if bank_segment == "joint_stock":
            return {
                "base_position": [0.2, 0.25],
                "macd_bull_base": [0.45, 0.5],
                "macd_neutral_base": [0.2, 0.25],
                "macd_bear_base": [0.05, 0.1],
                "add_step": [0.1, 0.15],
                "trim_step": [0.1, 0.15],
                "oversold_zscore": [-1.0, -1.2],
                "deep_oversold_zscore": [-1.5],
                "overbought_zscore": [1.0, 1.2],
                "oversold_rsi": [40],
                "overbought_rsi": [60],
                "max_position": [0.9],
                "pb_cheap_buy_floor": [55],
                "pb_expensive_trim_ceiling": [25],
                "min_dividend_yield": [3.8],
                "min_dividend_stability": [0.75],
            }

        if bank_segment == "regional":
            return {
                "base_position": [0.15, 0.2],
                "macd_bull_base": [0.3, 0.35],
                "macd_neutral_base": [0.15, 0.2],
                "macd_bear_base": [0.0, 0.05],
                "add_step": [0.1],
                "trim_step": [0.1],
                "oversold_zscore": [-1.2, -1.4],
                "deep_oversold_zscore": [-1.6],
                "overbought_zscore": [1.1, 1.3],
                "oversold_rsi": [38],
                "overbought_rsi": [62],
                "max_position": [0.8],
                "pb_cheap_buy_floor": [55, 60],
                "pb_expensive_trim_ceiling": [25],
                "min_dividend_yield": [3.6],
                "min_dividend_stability": [0.6, 0.75],
            }

        return {
            "base_position": [0.3, 0.4, 0.5],
            "macd_bull_base": [0.45, 0.55],
            "macd_neutral_base": [0.25, 0.35],
            "macd_bear_base": [0.05, 0.1],
            "add_step": [0.1, 0.2],
            "trim_step": [0.1, 0.2],
            "oversold_zscore": [-1.0, -1.2],
            "overbought_zscore": [1.0, 1.2],
            "oversold_rsi": [40, 42],
            "max_position": [0.9],
            "deep_oversold_zscore": [-1.5],
            "overbought_rsi": [60],
            "pb_cheap_buy_floor": [50, 60],
            "pb_expensive_trim_ceiling": [20],
            "min_dividend_yield": [4.0, 4.5],
            "min_dividend_stability": [0.8],
        }

    raise ValueError(f"不支持的优化模式: {mode}")


def make_strategy(mode, params):
    if mode == "regime":
        return macd_with_regime_filter_strategy(**params)
    if mode == "cycle":
        return cycle_with_industry_rotation_strategy(**params)
    if mode == "bank_t":
        return bank_t_strategy(**params)
    raise ValueError(f"不支持的优化模式: {mode}")


def iter_param_sets(param_grid):
    keys = list(param_grid.keys())
    for values in product(*(param_grid[key] for key in keys)):
        yield dict(zip(keys, values))


def split_frame(df, train_ratio=0.7):
    split_index = int(len(df) * train_ratio)
    if split_index < 120 or len(df) - split_index < 60:
        return None, None
    train_df = df.iloc[:split_index].copy().reset_index(drop=True)
    valid_df = df.iloc[split_index:].copy().reset_index(drop=True)
    return train_df, valid_df


def evaluate_strategy(df, mode, params):
    strategy = make_strategy(mode, params)
    signal_df = strategy.trading_strategy(df.copy())
    final_value, transactions, total_return, annual_return, stats = backtest(signal_df)
    return {
        "final_value": final_value,
        "transactions": transactions,
        "total_return": total_return,
        "annual_return": annual_return,
        "stats": stats,
    }


def score_result(result, mode):
    stats = result["stats"]
    base_score = (
        result["annual_return"] * 100
        + stats["sharpe"] * 12
        + stats["win_rate"] * 20
        + stats["excess_return"] * 0.6
        + stats["max_drawdown"] * 0.4
    )
    if mode == "bank_t":
        # 银行做T更看重稳定性和相对收益，不过度追求绝对弹性。
        base_score += stats["total_trades"] * 0.2
        base_score += min(0, stats["max_drawdown"] + 8) * 1.5
    return base_score


def resolve_stock_pool(mode, stocks, bank_segment="all", bank_scope="developed", cycle_scope="leaders"):
    if stocks:
        return stocks
    if mode == "cycle":
        return load_cycle_stock_pool(scope=cycle_scope)
    if mode == "bank_t":
        return load_bank_segment_pools(area_scope=bank_scope).get(
            bank_segment,
            load_bank_stock_pool(area_scope=bank_scope),
        )
    return list(DEFAULT_STOCK_NAMES.keys())


def optimize(
    mode,
    stock_pool,
    start_date,
    end_date,
    top_n=10,
    force_refresh=False,
    bank_segment="all",
):
    cached_frames = {}
    for stock_code in stock_pool:
        cached_frames[stock_code] = fetch_stock_data(
            stock_code,
            start_date,
            end_date,
            force_refresh=force_refresh,
        )
        cached_frames[stock_code] = enrich_with_market_context(
            cached_frames[stock_code],
            start_date,
            end_date,
            force_refresh=force_refresh,
            stock_code=stock_code,
        )

    rows = []
    for params in iter_param_sets(build_param_grid(mode, bank_segment=bank_segment)):
        train_scores = []
        valid_scores = []
        valid_trade_counts = []
        valid_returns = []
        valid_drawdowns = []

        for stock_code, stock_df in cached_frames.items():
            train_df, valid_df = split_frame(stock_df)
            if train_df is None or valid_df is None:
                continue

            train_result = evaluate_strategy(train_df, mode, params)
            valid_result = evaluate_strategy(valid_df, mode, params)

            train_scores.append(score_result(train_result, mode))
            valid_scores.append(score_result(valid_result, mode))
            valid_trade_counts.append(valid_result["stats"]["total_trades"])
            valid_returns.append(valid_result["total_return"])
            valid_drawdowns.append(valid_result["stats"]["max_drawdown"])

        if not valid_scores:
            continue

        rows.append(
            {
                "mode": mode,
                "bank_segment": bank_segment,
                **params,
                "train_score": sum(train_scores) / len(train_scores),
                "valid_score": sum(valid_scores) / len(valid_scores),
                "valid_trades": sum(valid_trade_counts) / len(valid_trade_counts),
                "valid_return": sum(valid_returns) / len(valid_returns),
                "valid_drawdown": sum(valid_drawdowns) / len(valid_drawdowns),
            }
        )

    result_df = pd.DataFrame(rows)
    if result_df.empty:
        return result_df

    min_trade_count = 3 if mode == "bank_t" else 1
    result_df = result_df[result_df["valid_trades"] >= min_trade_count]
    return result_df.sort_values(["valid_score", "train_score"], ascending=False).head(top_n)


def parse_args():
    parser = argparse.ArgumentParser(description="策略参数扫描")
    parser.add_argument("--days", type=int, default=720, help="拉取历史数据天数")
    parser.add_argument("--top", type=int, default=10, help="输出前N组参数")
    parser.add_argument(
        "--mode",
        choices=["regime", "cycle", "bank_t"],
        default="regime",
        help="优化哪套策略：regime、cycle 或 bank_t",
    )
    parser.add_argument(
        "--stocks",
        nargs="*",
        default=None,
        help="待优化的股票池；bank_t 默认优化银行股池",
    )
    parser.add_argument(
        "--bank-segment",
        choices=["all", "state_owned", "joint_stock", "regional"],
        default="all",
        help="bank_t 模式下可只优化某个银行子池",
    )
    parser.add_argument(
        "--bank-scope",
        choices=["developed", "all"],
        default="developed",
        help="bank_t 模式下默认只看发达地区银行；如需全国银行可切到 all",
    )
    parser.add_argument(
        "--cycle-scope",
        choices=["leaders", "all"],
        default="leaders",
        help="cycle 模式下默认只看代表性周期龙头；如需扩到更多周期股可切到 all",
    )
    parser.add_argument("--refresh-cache", action="store_true", help="忽略本地缓存并重新拉取Tushare数据")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    stock_pool = resolve_stock_pool(
        args.mode,
        args.stocks,
        bank_segment=args.bank_segment,
        bank_scope=args.bank_scope,
        cycle_scope=args.cycle_scope,
    )
    start_date, end_date = get_date_range(args.days)
    result_df = optimize(
        args.mode,
        stock_pool,
        start_date,
        end_date,
        top_n=args.top,
        force_refresh=args.refresh_cache,
        bank_segment=args.bank_segment,
    )
    if result_df.empty:
        print("没有找到足够的数据来完成参数扫描")
    else:
        print(f"参数扫描模式: {args.mode}")
        print(f"参数扫描区间: {start_date} 至 {end_date}")
        if args.mode == "bank_t":
            print(f"银行范围: {args.bank_scope}")
        if args.mode == "cycle":
            print(f"周期股范围: {args.cycle_scope}")
        print(f"股票池: {', '.join(stock_pool)}")
        print(result_df.to_string(index=False))
