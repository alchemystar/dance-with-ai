from datetime import date

import pandas as pd

from backtest import (
    DEFAULT_RECIPIENTS,
    analyze_stock_pool,
    fetch_stock_data,
    get_date_range,
    screen_core_long_term_pool,
)
from dividend_hold_strategy import dividend_hold_strategy
from email_util import send_email
from hk_holdings_tracker import HK_HOLDING_PROFILES, build_hk_holding_cards
from state_owned_dividend_strategy import state_owned_dividend_strategy
from value_quality_hold_strategy import value_quality_hold_strategy


def _render_table(df: pd.DataFrame, title: str) -> str:
    if df.empty:
        return f"<h3>{title}</h3><p>暂无数据</p>"
    return f"""
    <h3>{title}</h3>
    {df.to_html(index=False, border=0, classes='report-table')}
    """


def _core_buy_sell_points(row: pd.Series):
    buy_hint = str(row.get("buy_hint", "") or "")
    final_status = str(row.get("final_status", "") or "")
    if buy_hint == "低点可买":
        buy_point = "接近 MA60 或近 20 日低位时分批买入，避免追高"
    elif buy_hint == "接近低点":
        buy_point = "先小仓跟踪，等回踩确认或横盘稳定后再加"
    elif buy_hint == "等待回踩":
        buy_point = "当前不追高，等回踩中期均线或涨幅收敛后再看"
    else:
        buy_point = "暂不急着买，继续等更低位置或更稳企稳信号"

    if "核心长期持有" in final_status:
        sell_point = "只有极度高估、财报明显恶化，或长期逻辑被证伪时才卖"
    else:
        sell_point = "估值明显高估、财报转弱，或中长期趋势破坏时减仓/卖出"
    return buy_point, sell_point


def _bank_dividend_core_strategy():
    return state_owned_dividend_strategy(
        base_position=0.8,
        add_step=0.2,
        trim_step=0.3,
        max_position=1.0,
        pb_buy_floor=15,
        pb_strong_buy_floor=30,
        pb_trim_ceiling=8,
        min_dividend_yield=3.6,
        min_dividend_stability=0.85,
        min_financial_health_score=0.5,
        min_financial_quality_score=20,
        min_profit_yoy=-15,
        min_revenue_yoy=-8,
        min_roe=6,
        weak_profit_yoy_exit=-25,
        weak_revenue_yoy_exit=-12,
        weak_roe_exit=5,
        rebalance_cooldown=120,
        allow_recovery_entry=True,
        allow_add_on_weakness=False,
        allow_partial_trim=False,
        allow_watch_downgrade=False,
        exit_on_market_breakdown=False,
        exit_on_valuation_extreme=True,
        display_name="bank_dividend_core",
    )


def _analyze_core_pool_results(core_df: pd.DataFrame, days: int):
    if core_df.empty:
        return pd.DataFrame()

    start_date, end_date = get_date_range(days)
    rows = []
    for _, row in core_df.iterrows():
        stock_code = row["stock_code"]
        source_tags = str(row.get("source_tags", "") or "")
        if "bank_dividend" in source_tags:
            strategy = _bank_dividend_core_strategy()
        elif "dividend" in source_tags:
            strategy = dividend_hold_strategy(display_name="dividend_hold")
        else:
            strategy = value_quality_hold_strategy(display_name="value_quality_hold")

        results = analyze_stock_pool(
            [stock_code],
            start_date,
            end_date,
            strategy,
            verbose=False,
            force_refresh=False,
        )
        if not results:
            continue
        result = results[0]
        stats = result["stats"]
        buy_point, sell_point = _core_buy_sell_points(row)
        rows.append(
            {
                "股票代码": stock_code,
                "股票名称": row["name"],
                "分层": row["final_status"],
                "买点提示": row["buy_hint"],
                "建议买点": buy_point,
                "建议卖点": sell_point,
                "建议仓位": row["suggested_position"],
                "累计收益": f"{result['total_return']:.2f}%",
                "最大回撤": f"{stats['max_drawdown']:.2f}%",
                "交易次数": stats["total_trades"],
                "盈利/亏损": f"{stats['profitable_trades']}/{stats['loss_trades']}",
                "超额收益": f"{stats['excess_return']:.2f}%",
                "策略": result["strategy_name"],
            }
        )
    return pd.DataFrame(rows)


def _summarize_buy_hold(stock_code: str, days: int):
    start_date, end_date = get_date_range(days)
    df = fetch_stock_data(stock_code, start_date, end_date, force_refresh=False)
    df = df.sort_values("trade_date").reset_index(drop=True)
    if df.empty:
        raise ValueError(f"{stock_code} 没有行情数据")
    equity = df["close"] / df["close"].iloc[0]
    peak = equity.cummax()
    drawdown = (equity - peak) / peak
    total_return = (equity.iloc[-1] - 1) * 100
    day_count = max(
        1,
        (
            pd.to_datetime(df["trade_date"].iloc[-1]) - pd.to_datetime(df["trade_date"].iloc[0])
        ).days,
    )
    annual_return = ((equity.iloc[-1]) ** (365 / day_count) - 1) * 100
    max_drawdown = drawdown.min() * 100
    return {
        "total_return": total_return,
        "annual_return": annual_return,
        "max_drawdown": max_drawdown,
    }


def _build_hk_report(days: int):
    stocks = list(HK_HOLDING_PROFILES.keys())
    tracker_df = build_hk_holding_cards(stocks=stocks, days=days, force_refresh=False)
    rows = []
    for _, row in tracker_df.iterrows():
        stock_code = row["stock_code"]
        try:
            stats = _summarize_buy_hold(stock_code, days)
            total_return = f"{stats['total_return']:.2f}%"
            annual_return = f"{stats['annual_return']:.2f}%"
            max_drawdown = f"{stats['max_drawdown']:.2f}%"
        except Exception:
            total_return = "N/A"
            annual_return = "N/A"
            max_drawdown = "N/A"

        rows.append(
            {
                "股票代码": stock_code,
                "股票名称": row["name"],
                "持有风格": row["style"],
                "当前动作": row["current_action"],
                "建议买点": row["add_rule"],
                "建议卖点": row["reduce_rule"],
                "累计收益": total_return,
                "年化收益": annual_return,
                "最大回撤": max_drawdown,
                "交易次数": 0,
                "盈利/亏损": "0/0",
                "重点跟踪": row["key_watch"],
            }
        )
    return pd.DataFrame(rows)


def build_html() -> str:
    core_pool_df = screen_core_long_term_pool(
        days=720,
        value_scope="all",
        value_limit=80,
        bank_scope="developed",
        top=10,
        force_refresh=False,
    ).copy()
    core_result_df = _analyze_core_pool_results(core_pool_df.head(8), days=720)
    hk_df = _build_hk_report(days=720)

    today = date.today().isoformat()
    summary_html = f"""
    <h2>长期持有跟踪邮件</h2>
    <p>日期：{today}</p>
    <p>本次重点：</p>
    <ul>
        <li>核心长期池已经带上累计收益、最大回撤、交易次数和盈利/亏损统计，格式回到你原来熟悉的风格。</li>
        <li>这次补上了“建议买点”和“建议卖点”，看邮件时就能直接判断是等回踩、分批买，还是继续拿住。</li>
        <li>港股栏位当前使用的是持有基线统计，更适合看“拿着不动的收益和回撤”，不把它误当成短线交易策略。</li>
        <li>吉利汽车偏成长趋势持有，安踏体育偏消费龙头低频持有，当前都更适合看回踩后的加仓机会。</li>
    </ul>
    """

    html = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; line-height: 1.6; }}
            .report-table {{ border-collapse: collapse; width: 100%; }}
            .report-table th, .report-table td {{ border: 1px solid black; padding: 8px; text-align: left; vertical-align: top; }}
            .report-table th {{ background-color: #f2f2f2; }}
            .report-table tr:nth-child(even) {{ background-color: #f9f9f9; }}
            h2, h3 {{ margin-bottom: 8px; }}
        </style>
    </head>
    <body>
        {summary_html}
        {_render_table(core_result_df, "核心长期持有池回测摘要")}
        {_render_table(hk_df, "港股持仓跟踪（持有基线统计）")}
    </body>
    </html>
    """
    return html


if __name__ == "__main__":
    html_content = build_html()
    send_email(DEFAULT_RECIPIENTS, html_content)
