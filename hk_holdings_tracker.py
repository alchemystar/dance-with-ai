import argparse
from typing import Dict, List

import pandas as pd

from backtest import fetch_stock_data, get_date_range


HK_HOLDING_PROFILES: Dict[str, Dict[str, str]] = {
    "00175.HK": {
        "name": "吉利汽车",
        "style": "成长趋势持有",
        "core_logic": "核心看销量、新能源渗透率和产品周期，不按红利股方式交易",
        "add_rule": "只在回踩 MA60/MA120 附近且 20 日涨幅不过热时分批加仓",
        "reduce_rule": "跌破 MA120 或连续两个月销量/景气明显降速时降仓",
        "key_watch": "月销量、新能源占比、出口、单车盈利",
    },
    "02020.HK": {
        "name": "安踏体育",
        "style": "消费龙头低频持有",
        "core_logic": "核心看品牌矩阵、收入利润质量和估值消化，不适合高频来回折腾",
        "add_rule": "优先等业绩确认后回踩 MA60/MA120 再加，不追高",
        "reduce_rule": "财报显著低于预期或跌破 MA120 且基本面转弱时降仓",
        "key_watch": "年报/中报、主品牌与 FILA 增长、库存周转、经营利润率",
    },
    "00700.HK": {
        "name": "腾讯控股",
        "style": "平台龙头低频持有",
        "core_logic": "核心看广告、游戏、视频号、回购和自由现金流，不适合短线来回折腾",
        "add_rule": "优先等回踩 MA60/MA120 或估值消化后再加，不追高突破日",
        "reduce_rule": "核心业务明显降速或跌破 MA120 且基本面转弱时减仓",
        "key_watch": "广告增速、游戏流水、视频号商业化、回购力度、资本开支",
    },
    "09988.HK": {
        "name": "阿里巴巴",
        "style": "价值修复 + 业务重估持有",
        "core_logic": "核心看电商变现、云业务、回购和资本配置，适合等低估修复而不是高频交易",
        "add_rule": "估值回到低位区并回踩中期均线时分批加仓",
        "reduce_rule": "云业务与核心电商同时弱化，或跌破 MA120 且盈利预期下修时减仓",
        "key_watch": "淘天 GMV/CMR、云增速、回购、利润率、资本开支",
    },
    "03690.HK": {
        "name": "美团",
        "style": "消费互联网成长持有",
        "core_logic": "核心看到店、外卖、闪购和新业务亏损收窄，属于景气成长，不按红利逻辑管理",
        "add_rule": "回踩 MA60 附近且 20 日涨幅不过热时分批加仓",
        "reduce_rule": "核心业务利润率转弱或跌破 MA120 且竞争加剧时减仓",
        "key_watch": "外卖单量、到店利润率、闪购、骑手成本、竞争格局",
    },
    "01810.HK": {
        "name": "小米集团",
        "style": "硬科技成长持有",
        "core_logic": "核心看手机、汽车、IoT 和生态协同，属于成长股，适合趋势中低频持有",
        "add_rule": "等汽车或手机催化后的回踩，再在 MA60/MA120 附近分批加",
        "reduce_rule": "汽车交付或手机利润明显不及预期，或跌破 MA120 时减仓",
        "key_watch": "SU7/后续车型交付、手机 ASP、IoT 增速、利润率、库存",
    },
}


def parse_args():
    parser = argparse.ArgumentParser(description="港股持仓跟踪卡")
    parser.add_argument(
        "--stocks",
        nargs="*",
        default=list(HK_HOLDING_PROFILES.keys()),
        help="默认跟踪 00175.HK 和 02020.HK",
    )
    parser.add_argument("--days", type=int, default=720, help="使用的历史窗口天数")
    parser.add_argument("--refresh-cache", action="store_true", help="忽略本地缓存并刷新数据")
    return parser.parse_args()


def _safe_num(value):
    return pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]


def _build_price_snapshot(stock_code: str, days: int, force_refresh: bool = False):
    start_date, end_date = get_date_range(days)
    df = fetch_stock_data(stock_code, start_date, end_date, force_refresh=force_refresh)
    df = df.sort_values("trade_date").reset_index(drop=True)
    if len(df) < 120:
        raise ValueError(f"{stock_code} 历史数据不足")

    df["ma20"] = df["close"].rolling(window=20, min_periods=10).mean()
    df["ma60"] = df["close"].rolling(window=60, min_periods=20).mean()
    df["ma120"] = df["close"].rolling(window=120, min_periods=40).mean()
    df["ret_20"] = df["close"].pct_change(20, fill_method=None)
    df["ret_60"] = df["close"].pct_change(60, fill_method=None)
    df["high_120"] = df["close"].rolling(window=120, min_periods=60).max()
    df["drawdown_from_high_120"] = df["close"] / df["high_120"] - 1

    latest = df.iloc[-1]
    close_price = _safe_num(latest.get("close", pd.NA))
    ma20 = _safe_num(latest.get("ma20", pd.NA))
    ma60 = _safe_num(latest.get("ma60", pd.NA))
    ma120 = _safe_num(latest.get("ma120", pd.NA))
    ret20 = _safe_num(latest.get("ret_20", pd.NA))
    ret60 = _safe_num(latest.get("ret_60", pd.NA))
    dd120 = _safe_num(latest.get("drawdown_from_high_120", pd.NA))

    return {
        "trade_date": latest["trade_date"],
        "close": close_price,
        "ma20": ma20,
        "ma60": ma60,
        "ma120": ma120,
        "ret20": ret20,
        "ret60": ret60,
        "drawdown120": dd120,
    }


def _evaluate_geely(snapshot: Dict[str, float]):
    close_price = snapshot["close"]
    ma60 = snapshot["ma60"]
    ma120 = snapshot["ma120"]
    ret20 = snapshot["ret20"]
    ret60 = snapshot["ret60"]
    dd120 = snapshot["drawdown120"]

    if pd.notna(close_price) and pd.notna(ma120) and close_price < ma120 * 0.94:
        return "减仓观察", "已经跌破中期趋势，先控制仓位"
    if pd.notna(ret60) and ret60 <= -0.12:
        return "减仓观察", "60 日趋势过弱，先等销量或价格重新修复"
    if (
        pd.notna(close_price)
        and pd.notna(ma60)
        and ma60 > 0
        and ma60 * 0.97 <= close_price <= ma60 * 1.05
        and pd.notna(ret20)
        and -0.08 <= ret20 <= 0.10
    ):
        return "接近加仓区", "更适合等回踩中期均线附近再分批加"
    if pd.notna(dd120) and dd120 >= -0.05 and pd.notna(ret20) and ret20 >= 0.12:
        return "不追高，继续持有", "离阶段高点太近，持有即可"
    return "持有观察", "景气主线没坏，暂时按趋势持有"


def _evaluate_anta(snapshot: Dict[str, float]):
    close_price = snapshot["close"]
    ma60 = snapshot["ma60"]
    ma120 = snapshot["ma120"]
    ret20 = snapshot["ret20"]
    ret60 = snapshot["ret60"]
    dd120 = snapshot["drawdown120"]

    if pd.notna(close_price) and pd.notna(ma120) and close_price < ma120 * 0.93:
        return "减仓观察", "消费龙头如果跌破长期趋势，优先保护仓位"
    if pd.notna(ret60) and ret60 <= -0.15:
        return "减仓观察", "60 日回撤过大，先等业绩或价格重新修复"
    if (
        pd.notna(close_price)
        and pd.notna(ma60)
        and ma60 > 0
        and ma60 * 0.98 <= close_price <= ma60 * 1.04
        and pd.notna(ret20)
        and -0.06 <= ret20 <= 0.08
    ):
        return "接近加仓区", "更适合等业绩确认后在均线附近低频加仓"
    if pd.notna(dd120) and dd120 >= -0.04 and pd.notna(ret20) and ret20 >= 0.10:
        return "不追高，继续持有", "位置偏高，先持有等回踩"
    return "持有观察", "消费龙头主逻辑未坏，先低频持有"


def _evaluate_platform_growth(snapshot: Dict[str, float], softer=False):
    close_price = snapshot["close"]
    ma60 = snapshot["ma60"]
    ma120 = snapshot["ma120"]
    ret20 = snapshot["ret20"]
    ret60 = snapshot["ret60"]
    dd120 = snapshot["drawdown120"]

    ma120_floor = 0.95 if softer else 0.94
    ret60_floor = -0.14 if softer else -0.12
    if pd.notna(close_price) and pd.notna(ma120) and close_price < ma120 * ma120_floor:
        return "减仓观察", "已经跌破中长期趋势，先控制仓位"
    if pd.notna(ret60) and ret60 <= ret60_floor:
        return "减仓观察", "60 日趋势转弱，先等基本面或价格重新修复"
    if (
        pd.notna(close_price)
        and pd.notna(ma60)
        and ma60 > 0
        and ma60 * 0.97 <= close_price <= ma60 * 1.05
        and pd.notna(ret20)
        and -0.08 <= ret20 <= 0.10
    ):
        return "接近加仓区", "更适合等回踩中期均线附近再低频加仓"
    if pd.notna(dd120) and dd120 >= -0.05 and pd.notna(ret20) and ret20 >= 0.12:
        return "不追高，继续持有", "离阶段高点太近，持有即可"
    return "持有观察", "主线逻辑没坏，先按低频持有"


def build_hk_holding_cards(stocks: List[str], days: int, force_refresh: bool = False):
    rows = []
    for stock_code in stocks:
        profile = HK_HOLDING_PROFILES.get(stock_code, {})
        name = profile.get("name", stock_code)
        style = profile.get("style", "低频持有")
        try:
            snapshot = _build_price_snapshot(stock_code, days=days, force_refresh=force_refresh)
            if stock_code == "00175.HK":
                action, note = _evaluate_geely(snapshot)
            elif stock_code == "02020.HK":
                action, note = _evaluate_anta(snapshot)
            elif stock_code in {"00700.HK", "09988.HK", "03690.HK", "01810.HK"}:
                action, note = _evaluate_platform_growth(
                    snapshot,
                    softer=stock_code in {"00700.HK", "09988.HK"},
                )
            else:
                action, note = "持有观察", "暂无专用规则"

            rows.append(
                {
                    "stock_code": stock_code,
                    "name": name,
                    "style": style,
                    "last_trade_date": snapshot["trade_date"],
                    "close": round(float(snapshot["close"]), 2) if pd.notna(snapshot["close"]) else pd.NA,
                    "ret_20_pct": round(float(snapshot["ret20"]) * 100, 2) if pd.notna(snapshot["ret20"]) else pd.NA,
                    "ret_60_pct": round(float(snapshot["ret60"]) * 100, 2) if pd.notna(snapshot["ret60"]) else pd.NA,
                    "drawdown_120_pct": round(float(snapshot["drawdown120"]) * 100, 2)
                    if pd.notna(snapshot["drawdown120"])
                    else pd.NA,
                    "current_action": action,
                    "action_note": note,
                    "core_logic": profile.get("core_logic", ""),
                    "key_watch": profile.get("key_watch", ""),
                    "add_rule": profile.get("add_rule", ""),
                    "reduce_rule": profile.get("reduce_rule", ""),
                }
            )
        except Exception as exc:
            rows.append(
                {
                    "stock_code": stock_code,
                    "name": name,
                    "style": style,
                    "last_trade_date": pd.NA,
                    "close": pd.NA,
                    "ret_20_pct": pd.NA,
                    "ret_60_pct": pd.NA,
                    "drawdown_120_pct": pd.NA,
                    "current_action": "等待数据",
                    "action_note": f"暂时没有拉到行情数据: {str(exc)}",
                    "core_logic": profile.get("core_logic", ""),
                    "key_watch": profile.get("key_watch", ""),
                    "add_rule": profile.get("add_rule", ""),
                    "reduce_rule": profile.get("reduce_rule", ""),
                }
            )

    return pd.DataFrame(rows)


if __name__ == "__main__":
    args = parse_args()
    result_df = build_hk_holding_cards(
        stocks=args.stocks,
        days=args.days,
        force_refresh=args.refresh_cache,
    )
    print(result_df.to_string(index=False))
