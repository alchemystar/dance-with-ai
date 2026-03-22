import argparse
import math
import os
import re
from functools import lru_cache
from pathlib import Path
from requests.exceptions import ConnectionError as RequestsConnectionError
from time import sleep
from datetime import datetime, timedelta

import pandas as pd
import akshare as ak
import tushare as ts

from email_util import generate_html_table, send_email
from financial_quality_screener import AkshareCache, get_metric_row, to_numeric, safe_pct
from bank_t_strategy import bank_t_strategy
from cycle_with_industry_rotation import cycle_with_industry_rotation_strategy
from dividend_hold_strategy import dividend_hold_strategy
from macd_with_deepdown import macd_with_deepdown
from macd_with_optimize_sell import macd_with_optimize_sell_strategy
from macd_with_regime_filter import macd_with_regime_filter_strategy
from print_util import print_transactions
from state_owned_dividend_strategy import state_owned_dividend_strategy
from stragegy_for_600345 import stragegy_for_600345
from theme_with_sentiment import theme_with_sentiment_strategy
from value_quality_hold_strategy import value_quality_hold_strategy

DEFAULT_TOKEN = "c0f992e8369579bfec7bf8481dc0bcc304ac66ab5b1dd12c1d154325"

# 优先使用环境变量，方便后续切换账号或本地私有配置。
ts.set_token(os.getenv("TUSHARE_TOKEN", DEFAULT_TOKEN))
pro = ts.pro_api()

DEFAULT_STOCK_NAMES = {
    "600919.SH": "江苏银行",
    "600345.SH": "长江通信",
    "000001.SZ": "平安银行",
    "512480.SH": "半导体ETF",
    "515650.SH": "消费ETF",
    "600161.SH": "天坛生物",
    "002270.SZ": "华明装备",
    "300762.SZ": "上海瀚讯",
    "03692.HK": "翰森制药",
    "00175.HK": "吉利汽车",
    "03690.HK": "美团点评",
    "00700.HK": "腾讯控股",
    "09988.HK": "阿里巴巴",
    "01810.HK": "小米集团",
    "603583.SH": "捷昌驱动",
    "00981.HK": "中芯国际",
}

DEFAULT_RECIPIENTS = ["652433935@qq.com"]
CACHE_DIR = Path(__file__).resolve().parent / ".cache" / "tushare"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
COMMODITY_CACHE_DIR = Path(__file__).resolve().parent / ".cache" / "commodities"
COMMODITY_CACHE_DIR.mkdir(parents=True, exist_ok=True)
LOCAL_STOCK_INFO = Path(__file__).resolve().parent / "basic" / "all_stocks_info.csv"
HK_FETCH_SLEEP_SECONDS = 30
STATE_OWNED_BIG_BANKS = {
    "601288.SH",
    "601328.SH",
    "601398.SH",
    "601658.SH",
    "601939.SH",
    "601988.SH",
}
JOINT_STOCK_BANKS = {
    "000001.SZ",
    "600000.SH",
    "600015.SH",
    "600016.SH",
    "600036.SH",
    "601166.SH",
    "601818.SH",
    "601916.SH",
    "601998.SH",
}
DEVELOPED_BANK_AREAS = ("北京", "上海", "江苏", "浙江", "深圳", "福建")
CYCLE_INDUSTRIES = {
    "化工原料",
    "农药化肥",
    "工程机械",
    "建筑工程",
    "铝",
    "铜",
    "小金属",
    "普钢",
    "水泥",
    "玻璃",
    "煤炭开采",
}
CYCLE_LEADER_CODES = [
    "000157.SZ",
    "000422.SZ",
    "000425.SZ",
    "000528.SZ",
    "000792.SZ",
    "000807.SZ",
    "000830.SZ",
    "000893.SZ",
    "002532.SZ",
    "600019.SH",
    "600031.SH",
    "600176.SH",
    "600188.SH",
    "600309.SH",
    "600362.SH",
    "600426.SH",
    "600585.SH",
    "601088.SH",
    "601186.SH",
    "601390.SH",
    "601600.SH",
    "601668.SH",
    "601800.SH",
    "601899.SH",
    "603993.SH",
]
THEME_INDUSTRIES = {
    "电气设备",
    "元器件",
    "软件服务",
    "专用机械",
    "半导体",
    "通信设备",
    "互联网",
    "IT设备",
}
THEME_LEADER_CODES = [
    "300308.SZ",
    "300502.SZ",
    "002463.SZ",
    "603019.SH",
    "603986.SH",
    "688041.SH",
    "688111.SH",
    "002371.SZ",
    "000063.SZ",
    "300274.SZ",
    "300762.SZ",
    "600345.SH",
    "603583.SH",
    "002270.SZ",
]
CYCLE_COMMODITY_PROXY_MAP = {
    "化工原料": "MA0",
    "农药化肥": "UR0",
    "工程机械": "RB0",
    "建筑工程": "RB0",
    "铝": "AL0",
    "铜": "CU0",
    "小金属": "CU0",
    "普钢": "RB0",
    "水泥": "RB0",
    "玻璃": "FG0",
    "煤炭开采": "JM0",
}
CYCLE_STOCK_PROXY_MAP = {
    "000157.SZ": "HC0",
    "000422.SZ": "UR0",
    "000425.SZ": "HC0",
    "000528.SZ": "HC0",
    "000792.SZ": "UR0",
    "000807.SZ": "AL0",
    "000830.SZ": "MA0",
    "000893.SZ": "UR0",
    "002532.SZ": "AL0",
    "600019.SH": "HC0",
    "600031.SH": "HC0",
    "600176.SH": "FG0",
    "600188.SH": "J0",
    "600309.SH": "MA0",
    "600362.SH": "CU0",
    "600426.SH": "UR0",
    "600585.SH": "RB0",
    "601088.SH": "J0",
    "601186.SH": "RB0",
    "601390.SH": "RB0",
    "601600.SH": "AL0",
    "601668.SH": "RB0",
    "601800.SH": "RB0",
    "601899.SH": "CU0",
    "603993.SH": "CU0",
}
INDUSTRY_FLOW_ALIASES = {
    "电气设备": ["电力设备", "配电设备"],
    "生物制药": ["医药生物", "生物制品", "化学制药"],
    "软件服务": ["计算机", "IT服务Ⅱ", "IT服务Ⅲ"],
    "互联网": ["互联网服务", "IT服务Ⅱ", "IT服务Ⅲ"],
    "元器件": ["电子", "半导体"],
}

CYCLE_CHAIN_LABELS = {
    "nonferrous": "有色链",
    "chemical": "化工链",
    "black": "黑色链",
    "generic": "综合链",
}
UNIFIED_FINANCIAL_HEALTH_FLOOR = 0.52
UNIFIED_PEER_FINANCIAL_FLOOR = 0.45
UNIFIED_MIN_FINANCIAL_QUALITY_SCORE = 35
UNIFIED_MIN_INDUSTRY_PEERS = 3
VALUE_SAMPLE_CODES = [
    "000001.SZ",
    "000002.SZ",
    "000333.SZ",
    "000651.SZ",
    "002142.SZ",
    "002415.SZ",
    "002475.SZ",
    "300274.SZ",
    "300308.SZ",
    "300502.SZ",
    "300750.SZ",
    "600036.SH",
    "600519.SH",
    "600900.SH",
    "601012.SH",
    "601318.SH",
    "601398.SH",
    "601899.SH",
    "603259.SH",
    "603288.SH",
]
VALUE_PREFERRED_INDUSTRIES = {
    "家用电器",
    "食品",
    "乳制品",
    "白酒",
    "中成药",
    "化学制药",
    "生物制药",
    "医疗保健",
    "医药商业",
    "银行",
    "供气供热",
    "火力发电",
    "新型电力",
    "仓储物流",
    "家居用品",
}
VALUE_CORE_INDUSTRIES = {
    "家用电器",
    "食品",
    "乳制品",
    "中成药",
    "化学制药",
    "生物制药",
    "医疗保健",
    "医药商业",
    "银行",
    "供气供热",
    "火力发电",
    "新型电力",
}
VALUE_TRAP_INDUSTRIES = {
    "全国地产",
    "区域地产",
    "煤炭开采",
    "普钢",
    "钢加工",
    "工程机械",
    "水泥",
    "玻璃",
    "铝",
    "铜",
    "小金属",
    "农药化肥",
    "化工原料",
}
VALUE_AVOID_NAME_KEYWORDS = ("ST", "*ST", "退")


def _cache_path_for(data_kind, ts_code, start_date, end_date):
    safe_code = ts_code.replace(".", "_")
    return CACHE_DIR / f"{data_kind}_{safe_code}_{start_date}_{end_date}.pkl"


def _load_cached_frame(cache_path, force_refresh=False):
    if cache_path.exists() and not force_refresh:
        return pd.read_pickle(cache_path)
    return None


def _save_sorted_frame(df, cache_path):
    if df.empty:
        return pd.DataFrame()
    df = df.sort_values(by="trade_date", ascending=True).reset_index(drop=True)
    df.to_pickle(cache_path)
    return df


def _call_tushare_with_retry(fetcher, retries=4, retry_sleep=1.5):
    last_error = None
    for attempt in range(retries):
        try:
            return fetcher()
        except (RequestsConnectionError, TimeoutError, OSError) as exc:
            last_error = exc
            if attempt == retries - 1:
                break
            sleep(retry_sleep * (attempt + 1))
    if last_error is not None:
        raise last_error
    return fetcher()


def _shift_date(date_str, days):
    return (pd.to_datetime(date_str) + pd.Timedelta(days=days)).strftime("%Y%m%d")


def _cheap_percentile(series):
    values = [value for value in series if pd.notna(value)]
    if len(values) < 20:
        return pd.NA
    current = values[-1]
    return sum(value > current for value in values) / len(values) * 100


def _merge_trade_date_asof(left_df, right_df):
    if left_df.empty or right_df.empty or "trade_date" not in left_df.columns or "trade_date" not in right_df.columns:
        return left_df

    left = left_df.copy()
    right = right_df.copy()
    left["_trade_dt"] = pd.to_datetime(left["trade_date"], format="%Y%m%d", errors="coerce")
    right["_trade_dt"] = pd.to_datetime(right["trade_date"], format="%Y%m%d", errors="coerce")
    left = left[left["_trade_dt"].notna()].sort_values("_trade_dt")
    right = right[right["_trade_dt"].notna()].sort_values("_trade_dt")
    merged = pd.merge_asof(
        left,
        right.drop(columns=["trade_date"]),
        on="_trade_dt",
        direction="backward",
    )
    return merged.drop(columns=["_trade_dt"])


def _numeric_series(frame, column_name):
    if column_name in frame.columns:
        return pd.to_numeric(frame[column_name], errors="coerce")
    return pd.Series(float("nan"), index=frame.index, dtype="float64")


def _scaled_score(series, lower, upper):
    numeric = pd.to_numeric(series, errors="coerce")
    if upper == lower:
        return pd.Series(float("nan"), index=numeric.index, dtype="float64")
    return ((numeric - lower) / (upper - lower)).clip(lower=0, upper=1)


def _weighted_average_score(score_items, index):
    weighted_sum = pd.Series(0.0, index=index, dtype="float64")
    total_weight = pd.Series(0.0, index=index, dtype="float64")
    for score_series, weight in score_items:
        valid_mask = score_series.notna()
        weighted_sum.loc[valid_mask] += score_series.loc[valid_mask] * weight
        total_weight.loc[valid_mask] += weight
    result = weighted_sum / total_weight.where(total_weight > 0)
    return result.where(total_weight > 0)


def _is_domestic_equity(stock_code):
    return (
        isinstance(stock_code, str)
        and stock_code.endswith((".SH", ".SZ"))
        and not stock_code.startswith(("5", "15"))
    )


@lru_cache(maxsize=1)
def load_stock_info():
    if not LOCAL_STOCK_INFO.exists():
        return pd.DataFrame()

    stock_info = pd.read_csv(LOCAL_STOCK_INFO, dtype={"symbol": str})
    required_cols = {"ts_code", "name", "industry"}
    if not required_cols.issubset(stock_info.columns):
        return pd.DataFrame()
    return stock_info.copy()


@lru_cache(maxsize=1)
def load_stock_industry_map():
    stock_info = load_stock_info()
    if stock_info.empty:
        return {}

    stock_info = stock_info[stock_info["ts_code"].notna() & stock_info["industry"].notna()].copy()
    return stock_info.set_index("ts_code")["industry"].to_dict()


@lru_cache(maxsize=1)
def load_stock_name_map():
    stock_info = load_stock_info()
    if stock_info.empty:
        return DEFAULT_STOCK_NAMES.copy()

    name_map = DEFAULT_STOCK_NAMES.copy()
    name_map.update(
        stock_info[stock_info["ts_code"].notna() & stock_info["name"].notna()]
        .set_index("ts_code")["name"]
        .to_dict()
    )
    return name_map


def resolve_cycle_chain(stock_code):
    industry_name = load_stock_industry_map().get(stock_code, "")
    proxy_symbol = CYCLE_STOCK_PROXY_MAP.get(stock_code) or CYCLE_COMMODITY_PROXY_MAP.get(industry_name, "")
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


def summarize_cycle_results(results):
    chain_summary = {}
    for result in results:
        chain = result.get("cycle_chain")
        if not chain:
            continue
        bucket = chain_summary.setdefault(
            chain,
            {
                "count": 0,
                "total_return": 0.0,
                "excess_return": 0.0,
                "max_drawdown": 0.0,
                "trades": 0,
                "positive": 0,
                "beat": 0,
            },
        )
        bucket["count"] += 1
        bucket["total_return"] += result["total_return"]
        bucket["excess_return"] += result["stats"]["excess_return"]
        bucket["max_drawdown"] += result["stats"]["max_drawdown"]
        bucket["trades"] += result["stats"]["total_trades"]
        if result["total_return"] > 0:
            bucket["positive"] += 1
        if result["stats"]["excess_return"] > 0:
            bucket["beat"] += 1
    return chain_summary


@lru_cache(maxsize=1)
def load_stock_area_map():
    stock_info = load_stock_info()
    if stock_info.empty or "area" not in stock_info.columns:
        return {}

    stock_info = stock_info[stock_info["ts_code"].notna() & stock_info["area"].notna()].copy()
    return stock_info.set_index("ts_code")["area"].to_dict()


@lru_cache(maxsize=4)
def load_bank_stock_pool(area_scope="all"):
    stock_info = load_stock_info()
    if stock_info.empty:
        return ["600919.SH", "000001.SZ"]

    bank_df = stock_info[stock_info["industry"] == "银行"].copy()
    if area_scope == "developed" and "area" in bank_df.columns:
        bank_df = bank_df[bank_df["area"].isin(DEVELOPED_BANK_AREAS)].copy()

    banks = bank_df["ts_code"].dropna().astype(str).drop_duplicates().sort_values().tolist()
    return banks or ["600919.SH", "000001.SZ"]


@lru_cache(maxsize=4)
def load_bank_segment_pools(area_scope="all"):
    all_banks = load_bank_stock_pool(area_scope=area_scope)
    state_owned = [code for code in all_banks if code in STATE_OWNED_BIG_BANKS]
    joint_stock = [code for code in all_banks if code in JOINT_STOCK_BANKS]
    regional = [code for code in all_banks if code not in STATE_OWNED_BIG_BANKS | JOINT_STOCK_BANKS]
    return {
        "state_owned": state_owned,
        "joint_stock": joint_stock,
        "regional": regional,
        "all": all_banks,
    }


def prepare_bank_stock_frames(start_date, end_date, area_scope="developed", force_refresh=False):
    stock_pool = load_bank_stock_pool(area_scope=area_scope)
    prepared_frames = {}
    for stock_code in stock_pool:
        try:
            stock_df = fetch_stock_data(stock_code, start_date, end_date, force_refresh=force_refresh)
            prepared_frames[stock_code] = enrich_with_market_context(
                stock_df,
                start_date,
                end_date,
                force_refresh=force_refresh,
                stock_code=stock_code,
            )
        except Exception as exc:
            print(f"处理银行股票 {stock_code} 时出错: {str(exc)}")

    prepared_frames = add_financial_peer_context(prepared_frames)
    return prepared_frames


def _build_bank_dividend_snapshot(stock_code, df):
    frame = df.copy().reset_index(drop=True)
    if len(frame) < 120:
        return None

    frame["ret_20"] = frame["close"].pct_change(20, fill_method=None)
    frame["ret_60"] = frame["close"].pct_change(60, fill_method=None)
    frame["ma20"] = frame["close"].rolling(window=20, min_periods=10).mean()
    frame["ma60"] = frame["close"].rolling(window=60, min_periods=20).mean()
    frame["low_20"] = frame["close"].rolling(window=20, min_periods=10).min().shift(1)
    if "sh_index_close" in frame.columns:
        frame["market_ret_20"] = frame["sh_index_close"].pct_change(20, fill_method=None)
        frame["bank_rs_20"] = frame["ret_20"] - frame["market_ret_20"]
    else:
        frame["bank_rs_20"] = pd.NA

    latest = frame.iloc[-1]
    pb_pct = latest.get("bank_pb_percentile_3y", pd.NA)
    dividend_yield = latest.get("bank_dv_ttm", pd.NA)
    dividend_stability = latest.get("bank_dividend_stability", pd.NA)
    health_score = latest.get("financial_health_score", pd.NA)
    peer_score = latest.get("peer_financial_score", pd.NA)
    quality_score = latest.get("financial_quality_score", pd.NA)
    roe = latest.get("financial_roe", pd.NA)
    profit_yoy = latest.get("financial_profit_yoy", pd.NA)
    revenue_yoy = latest.get("financial_revenue_yoy", pd.NA)
    ret20 = latest.get("ret_20", pd.NA)
    ret60 = latest.get("ret_60", pd.NA)
    rs20 = latest.get("bank_rs_20", pd.NA)
    total_mv = latest.get("value_total_mv", pd.NA)
    area = latest.get("stock_area", pd.NA)
    close_price = latest.get("close", pd.NA)
    ma60 = latest.get("ma60", pd.NA)
    low20 = latest.get("low_20", pd.NA)
    close_to_ma60_pct = pd.NA
    close_to_low20_pct = pd.NA
    if pd.notna(close_price) and pd.notna(ma60) and ma60:
        close_to_ma60_pct = (float(close_price) / float(ma60) - 1) * 100
    if pd.notna(close_price) and pd.notna(low20) and low20:
        close_to_low20_pct = (float(close_price) / float(low20) - 1) * 100

    pick_score = 0.0
    pick_score += _theme_score_clip(pb_pct, 20, 95) * 28
    pick_score += _theme_score_clip(dividend_yield, 2.5, 7.5) * 20
    pick_score += _theme_score_clip(dividend_stability, 0.4, 1.0) * 15
    pick_score += _theme_score_clip(health_score, 0.4, 0.95) * 14
    pick_score += _theme_score_clip(peer_score, 0.25, 0.9) * 10
    pick_score += _theme_score_clip(quality_score, 20, 95) * 7
    pick_score += _theme_score_clip(roe, 5, 18) * 8
    pick_score += _theme_score_clip(profit_yoy, -12, 20) * 4
    pick_score += _theme_score_clip(revenue_yoy, -8, 15) * 2
    pick_score += _theme_score_clip(total_mv, 800000, 12000000) * 10
    pick_score += _theme_score_clip(ret20, -0.15, 0.12) * 2
    pick_score += _theme_score_clip(ret60, -0.2, 0.2) * 2
    pick_score += _theme_score_clip(rs20, -0.08, 0.08) * 3

    segment = "regional"
    segment_bias = -4.0
    if stock_code in STATE_OWNED_BIG_BANKS:
        segment = "state_owned"
        segment_bias = 10.0
    elif stock_code in JOINT_STOCK_BANKS:
        segment = "joint_stock"
        segment_bias = 6.0
    pick_score += segment_bias

    reasons = []
    if pd.notna(dividend_yield) and float(dividend_yield) < 3.0:
        reasons.append("股息率偏低")
    if pd.notna(dividend_stability) and float(dividend_stability) < 0.75:
        reasons.append("分红稳定性不足")
    if pd.notna(health_score) and float(health_score) < 0.45:
        reasons.append("财报健康度偏弱")
    if pd.notna(peer_score) and float(peer_score) < 0.3:
        reasons.append("同行财报对比偏弱")
    if pd.notna(quality_score) and float(quality_score) < 25:
        reasons.append("财报质量偏弱")
    if pd.notna(roe) and float(roe) < 6:
        reasons.append("ROE 偏低")
    if pd.notna(profit_yoy) and float(profit_yoy) < -20:
        reasons.append("利润下滑过深")
    if pd.notna(revenue_yoy) and float(revenue_yoy) < -10:
        reasons.append("营收下滑过深")
    if pd.notna(pb_pct) and float(pb_pct) < 15:
        reasons.append("当前估值不够便宜")

    min_core_mv = 5000000 if segment in {"state_owned", "joint_stock"} else 3000000
    core_candidate = (
        not reasons
        and pd.notna(dividend_yield)
        and float(dividend_yield) >= 4.0
        and pd.notna(dividend_stability)
        and float(dividend_stability) >= 0.85
        and pd.notna(health_score)
        and float(health_score) >= 0.5
        and pd.notna(peer_score)
        and float(peer_score) >= 0.35
        and pd.notna(pb_pct)
        and float(pb_pct) >= 25
        and pd.notna(roe)
        and float(roe) >= 6.5
        and pd.notna(total_mv)
        and float(total_mv) >= min_core_mv
    )

    if core_candidate:
        status = "核心红利"
    elif not reasons and pick_score >= 72:
        status = "重点红利"
    elif not reasons and pick_score >= 58:
        status = "观察"
    else:
        status = "剔除"

    return {
        "stock_code": stock_code,
        "trade_date": latest["trade_date"],
        "bank_dividend_score": round(pick_score, 2),
        "bank_segment": segment,
        "bank_area": area,
        "bank_pb_percentile_3y": round(float(pb_pct), 2) if pd.notna(pb_pct) else pd.NA,
        "bank_dv_ttm": round(float(dividend_yield), 2) if pd.notna(dividend_yield) else pd.NA,
        "bank_dividend_stability": round(float(dividend_stability), 2) if pd.notna(dividend_stability) else pd.NA,
        "financial_health_score": round(float(health_score), 2) if pd.notna(health_score) else pd.NA,
        "peer_financial_score": round(float(peer_score), 2) if pd.notna(peer_score) else pd.NA,
        "financial_quality_score": round(float(quality_score), 2) if pd.notna(quality_score) else pd.NA,
        "financial_roe": round(float(roe), 2) if pd.notna(roe) else pd.NA,
        "financial_profit_yoy": round(float(profit_yoy), 2) if pd.notna(profit_yoy) else pd.NA,
        "financial_revenue_yoy": round(float(revenue_yoy), 2) if pd.notna(revenue_yoy) else pd.NA,
        "value_total_mv": round(float(total_mv), 2) if pd.notna(total_mv) else pd.NA,
        "bank_ret_20": round(float(ret20) * 100, 2) if pd.notna(ret20) else pd.NA,
        "bank_ret_60": round(float(ret60) * 100, 2) if pd.notna(ret60) else pd.NA,
        "bank_rs_20": round(float(rs20) * 100, 2) if pd.notna(rs20) else pd.NA,
        "bank_close_to_ma60_pct": round(float(close_to_ma60_pct), 2) if pd.notna(close_to_ma60_pct) else pd.NA,
        "bank_close_to_low20_pct": round(float(close_to_low20_pct), 2) if pd.notna(close_to_low20_pct) else pd.NA,
        "screen_reason": "；".join(reasons) if reasons else "高股息、低估值和财报健康度匹配较好",
        "status": status,
    }


@lru_cache(maxsize=8)
def load_cycle_stock_pool(scope="leaders", chain_scope="all"):
    stock_info = load_stock_info()
    if stock_info.empty:
        cycle_codes = CYCLE_LEADER_CODES.copy()
    else:
        available_codes = set(stock_info["ts_code"].dropna().astype(str))
        if scope == "all":
            cycle_df = stock_info[stock_info["industry"].isin(CYCLE_INDUSTRIES)].copy()
            cycle_codes = (
                cycle_df["ts_code"].dropna().astype(str).drop_duplicates().sort_values().tolist()
            )
            cycle_codes = cycle_codes or [code for code in CYCLE_LEADER_CODES if code in available_codes]
        else:
            cycle_codes = [code for code in CYCLE_LEADER_CODES if code in available_codes]
            cycle_codes = cycle_codes or CYCLE_LEADER_CODES.copy()

    if chain_scope == "all":
        return cycle_codes
    if chain_scope == "nonferrous":
        return [code for code in cycle_codes if resolve_cycle_chain(code) == "nonferrous"]
    if chain_scope == "swing":
        return [code for code in cycle_codes if resolve_cycle_chain(code) in {"chemical", "black", "generic"}]
    if chain_scope == "chemical":
        return [code for code in cycle_codes if resolve_cycle_chain(code) == "chemical"]
    if chain_scope == "black":
        return [code for code in cycle_codes if resolve_cycle_chain(code) == "black"]
    return cycle_codes


@lru_cache(maxsize=2)
def load_theme_stock_pool(scope="leaders"):
    stock_info = load_stock_info()
    if stock_info.empty:
        return THEME_LEADER_CODES.copy()

    available_codes = set(stock_info["ts_code"].dropna().astype(str))
    if scope == "all":
        theme_df = stock_info[stock_info["industry"].isin(THEME_INDUSTRIES)].copy()
        theme_codes = (
            theme_df["ts_code"].dropna().astype(str).drop_duplicates().sort_values().tolist()
        )
        return theme_codes or [code for code in THEME_LEADER_CODES if code in available_codes]

    leader_codes = [code for code in THEME_LEADER_CODES if code in available_codes]
    return leader_codes or THEME_LEADER_CODES.copy()


@lru_cache(maxsize=4)
def load_value_stock_pool(scope="sample", limit=0):
    stock_info = load_stock_info()
    already_limited = False
    if stock_info.empty:
        codes = [code for code in VALUE_SAMPLE_CODES if _is_domestic_equity(code)]
    else:
        equity_df = stock_info[stock_info["ts_code"].apply(_is_domestic_equity)].copy()
        if scope == "all":
            equity_df["name"] = equity_df["name"].fillna("").astype(str)
            equity_df["industry"] = equity_df["industry"].fillna("").astype(str)
            equity_df["value_pool_priority"] = 1
            equity_df.loc[equity_df["industry"].isin(VALUE_PREFERRED_INDUSTRIES), "value_pool_priority"] = 0
            equity_df.loc[equity_df["industry"].isin(VALUE_TRAP_INDUSTRIES), "value_pool_priority"] = 2
            equity_df.loc[
                equity_df["name"].apply(lambda text: any(keyword in text for keyword in VALUE_AVOID_NAME_KEYWORDS)),
                "value_pool_priority",
            ] = 3
            equity_df = equity_df.sort_values(
                by=["value_pool_priority", "industry", "ts_code"],
                ascending=[True, True, True],
            ).reset_index(drop=True)
            if limit and limit > 0:
                grouped = {}
                for _, row in equity_df.iterrows():
                    industry = row["industry"] or "未分类"
                    grouped.setdefault(industry, []).append(str(row["ts_code"]))
                industry_order = (
                    equity_df.groupby("industry", dropna=False)["value_pool_priority"]
                    .min()
                    .sort_values(kind="stable")
                    .index.tolist()
                )
                diversified_codes = []
                cursor = 0
                while len(diversified_codes) < limit:
                    picked = False
                    for industry in industry_order:
                        industry_codes = grouped.get(industry, [])
                        if cursor < len(industry_codes):
                            diversified_codes.append(industry_codes[cursor])
                            picked = True
                            if len(diversified_codes) >= limit:
                                break
                    if not picked:
                        break
                    cursor += 1
                codes = diversified_codes
                already_limited = True
            else:
                codes = (
                    equity_df["ts_code"]
                    .dropna()
                    .astype(str)
                    .drop_duplicates()
                    .tolist()
                )
        else:
            available_codes = set(equity_df["ts_code"].dropna().astype(str))
            codes = [code for code in VALUE_SAMPLE_CODES if code in available_codes]
            if not codes:
                codes = (
                    equity_df["ts_code"].dropna().astype(str).drop_duplicates().sort_values().head(200).tolist()
                )
    if limit and limit > 0 and not already_limited:
        return codes[:limit]
    return codes


def fetch_stock_data(stock_code, start_date, end_date, force_refresh=False):
    """
    获取股票数据，根据股票代码判断是A股、ETF还是港股。
    """
    cache_path = _cache_path_for("stock", stock_code, start_date, end_date)
    cached = _load_cached_frame(cache_path, force_refresh=force_refresh)
    if cached is not None:
        return cached

    if ".HK" in stock_code:
        # 港股接口有频率限制，保持原有保护逻辑。
        sleep(HK_FETCH_SLEEP_SECONDS)
        df = _call_tushare_with_retry(
            lambda: pro.hk_daily(ts_code=stock_code, start_date=start_date, end_date=end_date)
        )
    elif stock_code.startswith("5") or stock_code.startswith("15"):
        df = _call_tushare_with_retry(
            lambda: pro.fund_daily(ts_code=stock_code, start_date=start_date, end_date=end_date)
        )
    elif ".SZ" in stock_code or ".SH" in stock_code:
        df = _call_tushare_with_retry(
            lambda: pro.daily(ts_code=stock_code, start_date=start_date, end_date=end_date)
        )
    else:
        raise ValueError(f"不支持的股票代码格式: {stock_code}")

    if df.empty:
        raise ValueError(f"{stock_code} 在区间 {start_date}-{end_date} 没有拉到数据")

    return _save_sorted_frame(df, cache_path)


def _commodity_cache_path(data_kind, proxy_name, start_date, end_date):
    return COMMODITY_CACHE_DIR / f"{data_kind}_{proxy_name}_{start_date}_{end_date}.pkl"


def fetch_index_data(index_code, start_date, end_date, force_refresh=False):
    cache_path = _cache_path_for("index", index_code, start_date, end_date)
    cached = _load_cached_frame(cache_path, force_refresh=force_refresh)
    if cached is not None:
        return cached

    try:
        df = _call_tushare_with_retry(
            lambda: pro.index_daily(ts_code=index_code, start_date=start_date, end_date=end_date)
        )
    except Exception:
        return pd.DataFrame()
    if df.empty:
        return pd.DataFrame()

    return _save_sorted_frame(df, cache_path)


def fetch_macro_commodity_index(start_date, end_date, force_refresh=False):
    cache_path = _commodity_cache_path("macro_index", "ccpi", start_date, end_date)
    cached = _load_cached_frame(cache_path, force_refresh=force_refresh)
    if cached is not None:
        return cached

    try:
        df = _call_tushare_with_retry(lambda: ak.macro_china_commodity_price_index())
    except Exception:
        return pd.DataFrame()
    if df.empty or "日期" not in df.columns:
        return pd.DataFrame()

    df = df.rename(
        columns={
            "日期": "trade_date",
            "最新值": "commodity_macro_close",
            "涨跌幅": "commodity_macro_pct_change",
            "近3月涨跌幅": "commodity_macro_ret_3m",
            "近6月涨跌幅": "commodity_macro_ret_6m",
            "近1年涨跌幅": "commodity_macro_ret_1y",
        }
    )
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.strftime("%Y%m%d")
    df = df[df["trade_date"].notna()].copy()
    df = df[(df["trade_date"] >= start_date) & (df["trade_date"] <= end_date)].copy()
    if df.empty:
        return pd.DataFrame()
    return _save_sorted_frame(df, cache_path)


def fetch_cycle_proxy_data(proxy_symbol, start_date, end_date, force_refresh=False):
    cache_path = _commodity_cache_path("futures_proxy", proxy_symbol, start_date, end_date)
    cached = _load_cached_frame(cache_path, force_refresh=force_refresh)
    if cached is not None:
        return cached

    try:
        df = _call_tushare_with_retry(
            lambda: ak.futures_main_sina(
                symbol=proxy_symbol,
                start_date=start_date,
                end_date=end_date,
            )
        )
    except Exception:
        return pd.DataFrame()
    if df.empty or "日期" not in df.columns:
        return pd.DataFrame()

    df = df.rename(
        columns={
            "日期": "trade_date",
            "开盘价": "commodity_proxy_open",
            "最高价": "commodity_proxy_high",
            "最低价": "commodity_proxy_low",
            "收盘价": "commodity_proxy_close",
            "成交量": "commodity_proxy_volume",
            "持仓量": "commodity_proxy_open_interest",
            "动态结算价": "commodity_proxy_settle",
        }
    )
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.strftime("%Y%m%d")
    df = df[df["trade_date"].notna()].copy()
    if df.empty:
        return pd.DataFrame()
    return _save_sorted_frame(df, cache_path)


def build_cycle_commodity_frame(stock_code, start_date, end_date, force_refresh=False):
    industry_name = load_stock_industry_map().get(stock_code)
    proxy_symbol = CYCLE_STOCK_PROXY_MAP.get(stock_code) or CYCLE_COMMODITY_PROXY_MAP.get(industry_name)
    if not proxy_symbol:
        return pd.DataFrame()

    proxy_df = fetch_cycle_proxy_data(
        proxy_symbol,
        start_date,
        end_date,
        force_refresh=force_refresh,
    )
    if proxy_df.empty:
        return pd.DataFrame()

    proxy_df["commodity_proxy_symbol"] = proxy_symbol
    proxy_df["commodity_proxy_industry"] = industry_name
    keep_cols = [
        "trade_date",
        "commodity_proxy_symbol",
        "commodity_proxy_industry",
        "commodity_proxy_open",
        "commodity_proxy_high",
        "commodity_proxy_low",
        "commodity_proxy_close",
        "commodity_proxy_volume",
        "commodity_proxy_open_interest",
        "commodity_proxy_settle",
    ]
    return proxy_df[[col for col in keep_cols if col in proxy_df.columns]].reset_index(drop=True)


def _financial_effective_date(period):
    period = str(period)
    if len(period) != 8 or not period.isdigit():
        return None

    year = int(period[:4])
    mmdd = period[4:]
    if mmdd == "0331":
        return f"{year}0430"
    if mmdd == "0630":
        return f"{year}0831"
    if mmdd == "0930":
        return f"{year}1031"
    if mmdd == "1231":
        return f"{year + 1}0430"
    return None


def build_financial_timeline(stock_code, force_refresh=False):
    stock_info = load_stock_info()
    if stock_info.empty:
        return pd.DataFrame()

    matched = stock_info[stock_info["ts_code"] == stock_code]
    if matched.empty:
        return pd.DataFrame()

    cache = AkshareCache()
    try:
        abstract_df = cache.fetch_stock_abstract(stock_code, force_refresh=force_refresh)
        income_df = cache.fetch_stock_report(stock_code, "利润表", force_refresh=force_refresh)
    except Exception:
        return pd.DataFrame()
    if abstract_df.empty:
        return pd.DataFrame()

    period_cols = sorted([col for col in abstract_df.columns if str(col).isdigit()])
    if not period_cols:
        return pd.DataFrame()

    announcement_map = {}
    if not income_df.empty and "报告日" in income_df.columns and "公告日期" in income_df.columns:
        report_view = income_df[["报告日", "公告日期"]].copy()
        report_view["报告日"] = report_view["报告日"].astype(str).str.replace("-", "", regex=False)
        report_view["公告日期"] = report_view["公告日期"].astype(str).str.replace("-", "", regex=False)
        report_view = report_view[
            report_view["报告日"].str.len().eq(8) & report_view["公告日期"].str.len().eq(8)
        ].copy()
        # 同一报告期如果存在更正/补充公告，优先使用首次披露时间，避免把后续更正时间误当成首次可见时间。
        announcement_map = (
            report_view.dropna(subset=["报告日", "公告日期"])
            .groupby("报告日", as_index=True)["公告日期"]
            .min()
            .to_dict()
        )

    metric_names = [
        "归母净利润",
        "营业总收入",
        "经营现金流量净额",
        "净资产收益率(ROE)",
        "销售净利率",
        "毛利率",
        "资产负债率",
        "每股净资产",
        "基本每股收益",
    ]
    rows = []
    for index, period in enumerate(period_cols):
        effective_date = announcement_map.get(str(period)) or _financial_effective_date(period)
        if not effective_date:
            continue

        previous_same_period = None
        for candidate in reversed(period_cols[:index]):
            if str(candidate)[4:] == str(period)[4:]:
                previous_same_period = candidate
                break

        item = {
            "trade_date": effective_date,
            "financial_latest_period": str(period),
        }
        for name in metric_names:
            metric_row = get_metric_row(abstract_df, name)
            if metric_row is None:
                continue
            current_value = to_numeric(metric_row.get(period))
            item_map = {
                "归母净利润": "financial_profit",
                "营业总收入": "financial_revenue",
                "经营现金流量净额": "financial_operating_cashflow",
                "净资产收益率(ROE)": "financial_roe",
                "销售净利率": "financial_net_margin",
                "毛利率": "financial_gross_margin",
                "资产负债率": "financial_debt_ratio",
                "每股净资产": "financial_bps",
                "基本每股收益": "financial_eps",
            }
            target_key = item_map.get(name)
            if target_key:
                item[target_key] = current_value

            if name in {"归母净利润", "营业总收入", "经营现金流量净额"} and previous_same_period:
                prev_value = to_numeric(metric_row.get(previous_same_period))
                yoy_key = {
                    "归母净利润": "financial_profit_yoy",
                    "营业总收入": "financial_revenue_yoy",
                    "经营现金流量净额": "financial_operating_cashflow_yoy",
                }[name]
                item[yoy_key] = safe_pct(current_value, prev_value)

        score = 0
        if pd.notna(item.get("financial_profit_yoy")) and item["financial_profit_yoy"] > 0:
            score += 25
        if pd.notna(item.get("financial_revenue_yoy")) and item["financial_revenue_yoy"] > 0:
            score += 20
        if pd.notna(item.get("financial_operating_cashflow")) and item["financial_operating_cashflow"] > 0:
            score += 20
        if pd.notna(item.get("financial_roe")) and item["financial_roe"] >= 8:
            score += 20
        if pd.notna(item.get("financial_debt_ratio")) and item["financial_debt_ratio"] <= 70:
            score += 15
        item["financial_quality_score"] = score
        rows.append(item)

    if not rows:
        return pd.DataFrame()

    timeline_df = pd.DataFrame(rows)
    timeline_df = timeline_df.sort_values(
        by=["trade_date", "financial_latest_period"],
        ascending=[True, True],
    ).drop_duplicates(subset=["trade_date"], keep="last").reset_index(drop=True)
    return timeline_df


def fetch_market_daily_info(start_date, end_date, force_refresh=False):
    cache_path = _cache_path_for("daily_info", "market", start_date, end_date)
    cached = _load_cached_frame(cache_path, force_refresh=force_refresh)
    if cached is not None:
        return cached

    try:
        df = _call_tushare_with_retry(
            lambda: pro.daily_info(start_date=start_date, end_date=end_date, exchange="SZ,SH")
        )
    except Exception:
        return pd.DataFrame()
    if df.empty:
        return pd.DataFrame()

    return _save_sorted_frame(df, cache_path)


def build_market_activity_frame(start_date, end_date, force_refresh=False):
    daily_info_df = fetch_market_daily_info(
        start_date,
        end_date,
        force_refresh=force_refresh,
    )
    if daily_info_df.empty:
        return pd.DataFrame()

    market_df = daily_info_df[daily_info_df["ts_code"].isin(["SH_MARKET", "SZ_MARKET"])].copy()
    if market_df.empty:
        return pd.DataFrame()

    grouped = market_df.groupby("trade_date", as_index=False).agg(
        market_listing_count=("com_count", "sum"),
        market_total_mv=("total_mv", "sum"),
        market_float_mv=("float_mv", "sum"),
        market_amount_total=("amount", "sum"),
        market_vol_total=("vol", "sum"),
        market_turnover_avg=("tr", "mean"),
        market_pe_avg=("pe", "mean"),
    )
    return grouped.sort_values(by="trade_date", ascending=True).reset_index(drop=True)


def fetch_market_moneyflow(start_date, end_date, force_refresh=False):
    cache_path = _cache_path_for("moneyflow_mkt_dc", "all", start_date, end_date)
    cached = _load_cached_frame(cache_path, force_refresh=force_refresh)
    if cached is not None:
        return cached

    try:
        df = _call_tushare_with_retry(
            lambda: pro.moneyflow_mkt_dc(start_date=start_date, end_date=end_date)
        )
    except Exception:
        return pd.DataFrame()
    if df.empty:
        return pd.DataFrame()

    return _save_sorted_frame(df, cache_path)


def fetch_theme_market_sentiment(start_date, end_date, force_refresh=False):
    required_cols = {
        "trade_date",
        "theme_up_limit",
        "theme_down_limit",
        "theme_broken_limit",
        "theme_first_board",
        "theme_second_board",
        "theme_multi_board_count",
        "theme_highest_board",
        "theme_reseal_count",
        "theme_broken_rate",
        "theme_up_down_ratio",
        "theme_sentiment_score",
        "theme_heat_score",
    }
    cache_path = _cache_path_for("theme_sentiment", "market", start_date, end_date)
    cached = _load_cached_frame(cache_path, force_refresh=force_refresh)
    if cached is not None and not required_cols.issubset(cached.columns):
        upgraded = _upgrade_legacy_theme_sentiment_frame(cached)
        if required_cols.issubset(upgraded.columns):
            return _save_sorted_frame(upgraded, cache_path)
    if cached is not None and required_cols.issubset(cached.columns):
        return cached

    try:
        trade_cal = _call_tushare_with_retry(
            lambda: pro.trade_cal(start_date=start_date, end_date=end_date)
        )
    except Exception:
        return pd.DataFrame()
    if trade_cal.empty:
        return pd.DataFrame()

    trade_dates = (
        trade_cal[trade_cal["is_open"] == 1]["cal_date"]
        .dropna()
        .astype(str)
        .sort_values()
        .tolist()
    )
    if not trade_dates:
        return pd.DataFrame()

    records = []
    for trade_date in trade_dates:
        day_cache = _cache_path_for("theme_sentiment_day", "market", trade_date, trade_date)
        day_cached = _load_cached_frame(day_cache, force_refresh=force_refresh)
        if day_cached is not None and not day_cached.empty and not required_cols.issubset(day_cached.columns):
            day_cached = _upgrade_legacy_theme_sentiment_frame(day_cached)
            if required_cols.issubset(day_cached.columns):
                _save_sorted_frame(day_cached, day_cache)
        if (
            day_cached is not None
            and not day_cached.empty
            and required_cols.issubset(day_cached.columns)
        ):
            records.append(day_cached.iloc[0].to_dict())
            continue

        day_df = fetch_theme_board_day_raw(trade_date, force_refresh=force_refresh)

        df_up = day_df[day_df["tag"] == "涨停"].copy() if not day_df.empty else pd.DataFrame()
        df_broken = day_df[day_df["tag"] == "炸板"].copy() if not day_df.empty else pd.DataFrame()
        df_down = day_df[day_df["tag"] == "跌停"].copy() if not day_df.empty else pd.DataFrame()
        up_count = len(df_up) if df_up is not None else 0
        broken_count = len(df_broken) if df_broken is not None else 0
        down_count = len(df_down) if df_down is not None else 0
        first_board = int(df_up["theme_is_first_board"].sum()) if not df_up.empty else 0
        second_board = int(df_up["theme_is_second_board"].sum()) if not df_up.empty else 0
        multi_board_count = int(df_up["theme_is_multi_board"].sum()) if not df_up.empty else 0
        highest_board = int(df_up["theme_board_count"].max()) if not df_up.empty else 0
        reseal_count = 0
        if not day_df.empty:
            reseal_count = int(
                day_df.groupby("ts_code")["tag"]
                .apply(lambda tags: int({"涨停", "炸板"}.issubset(set(tags))))
                .sum()
            )
        broken_rate = broken_count / up_count if up_count > 0 else 0.0
        up_down_ratio = up_count / max(down_count, 1)
        sentiment_score = min(
            100.0,
            max(
                0.0,
                up_count * 1.1
                + first_board * 0.8
                - down_count * 1.3
                - broken_count * 0.6,
            ),
        )
        heat_score = min(
            100.0,
            max(
                0.0,
                highest_board * 7.0
                + multi_board_count * 1.4
                + second_board * 0.8
                + first_board * 0.3
                + reseal_count * 1.2
                - broken_count * 0.5
                - down_count * 0.7,
            ),
        )

        day_row = pd.DataFrame(
            [
                {
                    "trade_date": trade_date,
                    "theme_up_limit": up_count,
                    "theme_down_limit": down_count,
                    "theme_broken_limit": broken_count,
                    "theme_first_board": first_board,
                    "theme_second_board": second_board,
                    "theme_multi_board_count": multi_board_count,
                    "theme_highest_board": highest_board,
                    "theme_reseal_count": reseal_count,
                    "theme_broken_rate": broken_rate,
                    "theme_up_down_ratio": up_down_ratio,
                    "theme_sentiment_score": sentiment_score,
                    "theme_heat_score": heat_score,
                }
            ]
        )
        _save_sorted_frame(day_row, day_cache)
        records.append(day_row.iloc[0].to_dict())

    if not records:
        return pd.DataFrame()

    sentiment_df = pd.DataFrame(records)
    return _save_sorted_frame(sentiment_df, cache_path)


def _parse_theme_board_count(status_text):
    text = str(status_text or "").strip()
    if not text:
        return 0
    if "首板" in text:
        return 1
    match = re.search(r"(\d+)\s*连板", text)
    if match:
        return int(match.group(1))
    match = re.search(r"(\d+)\s*板", text)
    if match:
        return int(match.group(1))
    return 0


def _upgrade_legacy_theme_sentiment_frame(df):
    upgraded = df.copy()
    if "theme_second_board" not in upgraded.columns:
        residual_multi = (
            upgraded.get("theme_up_limit", 0).fillna(0) - upgraded.get("theme_first_board", 0).fillna(0)
        ).clip(lower=0)
        upgraded["theme_second_board"] = residual_multi.clip(upper=3)
    if "theme_multi_board_count" not in upgraded.columns:
        upgraded["theme_multi_board_count"] = (
            upgraded.get("theme_up_limit", 0).fillna(0) - upgraded.get("theme_first_board", 0).fillna(0)
        ).clip(lower=0)
    if "theme_highest_board" not in upgraded.columns:
        upgraded["theme_highest_board"] = 0
        upgraded.loc[upgraded.get("theme_up_limit", 0).fillna(0) > 0, "theme_highest_board"] = 1
        upgraded.loc[upgraded.get("theme_multi_board_count", 0).fillna(0) > 0, "theme_highest_board"] = 2
    if "theme_reseal_count" not in upgraded.columns:
        upgraded["theme_reseal_count"] = 0
    if "theme_heat_score" not in upgraded.columns:
        upgraded["theme_heat_score"] = (
            upgraded.get("theme_highest_board", 0).fillna(0) * 7.0
            + upgraded.get("theme_multi_board_count", 0).fillna(0) * 1.4
            + upgraded.get("theme_second_board", 0).fillna(0) * 0.8
            + upgraded.get("theme_first_board", 0).fillna(0) * 0.3
            + upgraded.get("theme_reseal_count", 0).fillna(0) * 1.2
            - upgraded.get("theme_broken_limit", 0).fillna(0) * 0.5
            - upgraded.get("theme_down_limit", 0).fillna(0) * 0.7
        ).clip(lower=0, upper=100)
    return upgraded


def _upgrade_legacy_theme_board_state_frame(df):
    upgraded = df.copy()
    if "theme_board_count" not in upgraded.columns:
        upgraded["theme_board_count"] = upgraded.get("theme_board_status", "").apply(_parse_theme_board_count)
    if "theme_is_second_board" not in upgraded.columns:
        upgraded["theme_is_second_board"] = (
            (upgraded.get("theme_is_limit_up", 0).fillna(0) >= 1)
            & (upgraded["theme_board_count"] == 2)
        ).astype(int)
    if "theme_is_multi_board" not in upgraded.columns:
        upgraded["theme_is_multi_board"] = (
            (upgraded.get("theme_is_limit_up", 0).fillna(0) >= 1)
            & (upgraded["theme_board_count"] >= 2)
        ).astype(int)
    if "theme_is_reseal" not in upgraded.columns:
        upgraded["theme_is_reseal"] = 0
    return upgraded


def fetch_theme_board_day_raw(trade_date, force_refresh=False):
    required_cols = {
        "trade_date",
        "ts_code",
        "name",
        "tag",
        "status",
        "theme_board_count",
        "theme_is_limit_up",
        "theme_is_broken",
        "theme_is_down_limit",
        "theme_is_first_board",
        "theme_is_second_board",
        "theme_is_multi_board",
    }
    cache_path = _cache_path_for("theme_kpl_day_raw", "market", trade_date, trade_date)
    cached = _load_cached_frame(cache_path, force_refresh=force_refresh)
    if cached is not None and required_cols.issubset(cached.columns):
        return cached

    try:
        df_up = _call_tushare_with_retry(
            lambda: pro.kpl_list(
                trade_date=trade_date,
                tag="涨停",
                fields="ts_code,name,trade_date,tag,status",
            )
        )
        df_broken = _call_tushare_with_retry(
            lambda: pro.kpl_list(
                trade_date=trade_date,
                tag="炸板",
                fields="ts_code,name,trade_date,tag,status",
            )
        )
        df_down = _call_tushare_with_retry(
            lambda: pro.kpl_list(
                trade_date=trade_date,
                tag="跌停",
                fields="ts_code,name,trade_date,tag,status",
            )
        )
    except Exception:
        return pd.DataFrame()

    day_rows = []
    for tag_name, source_df in [("涨停", df_up), ("炸板", df_broken), ("跌停", df_down)]:
        if source_df is None or source_df.empty:
            continue
        for _, row in source_df.iterrows():
            status_text = str(row.get("status", "") or "")
            board_count = _parse_theme_board_count(status_text) if tag_name == "涨停" else 0
            day_rows.append(
                {
                    "trade_date": str(row.get("trade_date", trade_date) or trade_date),
                    "ts_code": str(row.get("ts_code", "")).strip(),
                    "name": str(row.get("name", "") or ""),
                    "tag": tag_name,
                    "status": status_text,
                    "theme_board_count": board_count,
                    "theme_is_limit_up": 1 if tag_name == "涨停" else 0,
                    "theme_is_broken": 1 if tag_name == "炸板" else 0,
                    "theme_is_down_limit": 1 if tag_name == "跌停" else 0,
                    "theme_is_first_board": 1 if tag_name == "涨停" and board_count == 1 else 0,
                    "theme_is_second_board": 1 if tag_name == "涨停" and board_count == 2 else 0,
                    "theme_is_multi_board": 1 if tag_name == "涨停" and board_count >= 2 else 0,
                }
            )

    if not day_rows:
        day_df = pd.DataFrame(columns=sorted(required_cols))
    else:
        day_df = pd.DataFrame(day_rows)

    return _save_sorted_frame(day_df, cache_path)


def fetch_theme_stock_board_states(start_date, end_date, force_refresh=False):
    required_cols = {
        "trade_date",
        "ts_code",
        "theme_board_tag",
        "theme_board_status",
        "theme_board_count",
        "theme_is_limit_up",
        "theme_is_broken",
        "theme_is_down_limit",
        "theme_is_first_board",
        "theme_is_second_board",
        "theme_is_multi_board",
        "theme_is_reseal",
    }
    cache_path = _cache_path_for("theme_board_states", "market", start_date, end_date)
    cached = _load_cached_frame(cache_path, force_refresh=force_refresh)
    if cached is not None and not required_cols.issubset(cached.columns):
        upgraded = _upgrade_legacy_theme_board_state_frame(cached)
        if required_cols.issubset(upgraded.columns):
            return _save_sorted_frame(upgraded, cache_path)
    if cached is not None and required_cols.issubset(cached.columns):
        return cached

    try:
        trade_cal = _call_tushare_with_retry(
            lambda: pro.trade_cal(start_date=start_date, end_date=end_date)
        )
    except Exception:
        return pd.DataFrame()
    if trade_cal.empty:
        return pd.DataFrame()

    trade_dates = (
        trade_cal[trade_cal["is_open"] == 1]["cal_date"]
        .dropna()
        .astype(str)
        .sort_values()
        .tolist()
    )
    if not trade_dates:
        return pd.DataFrame()

    theme_codes = set(load_theme_stock_pool(scope="leaders") + load_theme_stock_pool(scope="all"))
    records = []
    for trade_date in trade_dates:
        day_cache = _cache_path_for("theme_board_day", "market", trade_date, trade_date)
        day_cached = _load_cached_frame(day_cache, force_refresh=force_refresh)
        if day_cached is not None and not day_cached.empty and not required_cols.issubset(day_cached.columns):
            day_cached = _upgrade_legacy_theme_board_state_frame(day_cached)
            if required_cols.issubset(day_cached.columns):
                _save_sorted_frame(day_cached, day_cache)
        if day_cached is not None and required_cols.issubset(day_cached.columns):
            if not day_cached.empty:
                records.extend(day_cached.to_dict("records"))
            continue

        raw_day_df = fetch_theme_board_day_raw(trade_date, force_refresh=force_refresh)
        if raw_day_df.empty:
            day_df = pd.DataFrame(columns=sorted(required_cols))
            day_df.to_pickle(day_cache)
            continue

        day_df = raw_day_df[raw_day_df["ts_code"].isin(theme_codes)].copy()
        if day_df.empty:
            day_df = pd.DataFrame(columns=sorted(required_cols))
        else:
            day_df["tag_priority"] = day_df["tag"].map({"跌停": 1, "炸板": 2, "涨停": 3}).fillna(0)
            status_view = (
                day_df.sort_values(
                    by=["trade_date", "ts_code", "theme_board_count", "tag_priority"],
                    ascending=[True, True, True, True],
                )
                .groupby(["trade_date", "ts_code"], as_index=False)
                .last()[["trade_date", "ts_code", "tag", "status"]]
                .rename(columns={"tag": "theme_board_tag", "status": "theme_board_status"})
            )
            day_df = (
                day_df.groupby(["trade_date", "ts_code"], as_index=False)
                .agg(
                    theme_is_limit_up=("theme_is_limit_up", "max"),
                    theme_is_broken=("theme_is_broken", "max"),
                    theme_is_down_limit=("theme_is_down_limit", "max"),
                    theme_board_count=("theme_board_count", "max"),
                    theme_is_first_board=("theme_is_first_board", "max"),
                    theme_is_second_board=("theme_is_second_board", "max"),
                    theme_is_multi_board=("theme_is_multi_board", "max"),
                )
            )
            day_df["theme_is_reseal"] = (
                (day_df["theme_is_limit_up"] >= 1) & (day_df["theme_is_broken"] >= 1)
            ).astype(int)
            day_df = day_df.merge(status_view, on=["trade_date", "ts_code"], how="left")
            day_df.loc[day_df["theme_is_reseal"] >= 1, "theme_board_tag"] = "回封"
            records.extend(day_df.to_dict("records"))
        day_df.to_pickle(day_cache)

    if not records:
        return pd.DataFrame()

    result_df = pd.DataFrame(records)
    return _save_sorted_frame(result_df, cache_path)


def fetch_industry_moneyflow(start_date, end_date, force_refresh=False):
    cache_path = _cache_path_for("moneyflow_ind_dc", "industry", start_date, end_date)
    cached = _load_cached_frame(cache_path, force_refresh=force_refresh)
    if cached is not None:
        return cached

    try:
        df = _call_tushare_with_retry(
            lambda: pro.moneyflow_ind_dc(
                start_date=start_date,
                end_date=end_date,
                content_type="行业",
            )
        )
    except Exception:
        return pd.DataFrame()
    if df.empty:
        return pd.DataFrame()

    return _save_sorted_frame(df, cache_path)


def fetch_daily_basic_data(stock_code, start_date, end_date, force_refresh=False):
    cache_path = _cache_path_for("daily_basic", stock_code, start_date, end_date)
    cached = _load_cached_frame(cache_path, force_refresh=force_refresh)
    if cached is not None:
        return cached

    try:
        df = _call_tushare_with_retry(
            lambda: pro.daily_basic(
                ts_code=stock_code,
                start_date=start_date,
                end_date=end_date,
                fields="ts_code,trade_date,close,pb,pe,dv_ratio,dv_ttm,total_mv",
            )
        )
    except Exception:
        return pd.DataFrame()
    if df.empty:
        return pd.DataFrame()

    return _save_sorted_frame(df, cache_path)


def fetch_dividend_data(stock_code, force_refresh=False):
    cache_path = _cache_path_for("dividend", stock_code, "full", "full")
    cached = _load_cached_frame(cache_path, force_refresh=force_refresh)
    if cached is not None:
        return cached

    try:
        df = _call_tushare_with_retry(lambda: pro.dividend(ts_code=stock_code))
    except Exception:
        return pd.DataFrame()
    if df.empty:
        return pd.DataFrame()

    df.to_pickle(cache_path)
    return df


def build_bank_valuation_frame(stock_code, start_date, end_date, force_refresh=False):
    if load_stock_industry_map().get(stock_code) != "银行":
        return pd.DataFrame()

    valuation_start = _shift_date(start_date, -1100)
    daily_basic_df = fetch_daily_basic_data(
        stock_code,
        valuation_start,
        end_date,
        force_refresh=force_refresh,
    )
    if daily_basic_df.empty:
        return pd.DataFrame()

    daily_basic_df = daily_basic_df.sort_values(by="trade_date", ascending=True).reset_index(drop=True)
    daily_basic_df["bank_pb_percentile_3y"] = daily_basic_df["pb"].rolling(
        window=750,
        min_periods=60,
    ).apply(_cheap_percentile, raw=False)
    daily_basic_df["bank_dv_ttm"] = daily_basic_df["dv_ttm"]
    daily_basic_df["bank_dv_ratio"] = daily_basic_df["dv_ratio"]
    daily_basic_df["bank_pe"] = daily_basic_df["pe"]
    daily_basic_df["bank_pb"] = daily_basic_df["pb"]

    dividend_df = fetch_dividend_data(stock_code, force_refresh=force_refresh)
    stability = pd.NA
    latest_cash_div_tax = pd.NA
    if not dividend_df.empty:
        implemented_df = dividend_df[
            dividend_df["div_proc"].astype(str).str.contains("实施", na=False)
            & dividend_df["cash_div_tax"].notna()
            & (dividend_df["cash_div_tax"] > 0)
        ].copy()
        if not implemented_df.empty:
            implemented_df["fiscal_year"] = implemented_df["end_date"].astype(str).str[:4]
            annual_dividend = implemented_df.groupby("fiscal_year", as_index=False)["cash_div_tax"].sum()
            annual_dividend = annual_dividend.sort_values(by="fiscal_year", ascending=True).reset_index(drop=True)
            recent = annual_dividend.tail(5)
            if not recent.empty:
                stability = (recent["cash_div_tax"] > 0).mean()
                latest_cash_div_tax = recent["cash_div_tax"].iloc[-1]

    keep_cols = [
        "trade_date",
        "bank_pb",
        "bank_pe",
        "bank_dv_ratio",
        "bank_dv_ttm",
        "bank_pb_percentile_3y",
    ]
    bank_df = daily_basic_df[keep_cols].copy()
    bank_df["bank_dividend_stability"] = stability
    bank_df["bank_latest_cash_div_tax"] = latest_cash_div_tax
    bank_df = bank_df[
        (bank_df["trade_date"] >= start_date)
        & (bank_df["trade_date"] <= end_date)
    ].reset_index(drop=True)
    return bank_df


def build_cycle_valuation_frame(stock_code, start_date, end_date, force_refresh=False):
    industry_name = load_stock_industry_map().get(stock_code)
    if industry_name not in CYCLE_INDUSTRIES and stock_code not in CYCLE_LEADER_CODES:
        return pd.DataFrame()

    valuation_start = _shift_date(start_date, -1100)
    daily_basic_df = fetch_daily_basic_data(
        stock_code,
        valuation_start,
        end_date,
        force_refresh=force_refresh,
    )
    if daily_basic_df.empty:
        return pd.DataFrame()

    daily_basic_df = daily_basic_df.sort_values(by="trade_date", ascending=True).reset_index(drop=True)
    daily_basic_df["pb"] = pd.to_numeric(daily_basic_df["pb"], errors="coerce")
    daily_basic_df["pe"] = pd.to_numeric(daily_basic_df["pe"], errors="coerce")
    daily_basic_df.loc[daily_basic_df["pb"] <= 0, "pb"] = pd.NA
    daily_basic_df.loc[daily_basic_df["pe"] <= 0, "pe"] = pd.NA
    daily_basic_df["cycle_pb_percentile_3y"] = daily_basic_df["pb"].rolling(
        window=750,
        min_periods=60,
    ).apply(_cheap_percentile, raw=False)
    daily_basic_df["cycle_pe_percentile_3y"] = daily_basic_df["pe"].rolling(
        window=750,
        min_periods=60,
    ).apply(_cheap_percentile, raw=False)
    daily_basic_df["cycle_pb"] = daily_basic_df["pb"]
    daily_basic_df["cycle_pe"] = daily_basic_df["pe"]

    keep_cols = [
        "trade_date",
        "cycle_pb",
        "cycle_pe",
        "cycle_pb_percentile_3y",
        "cycle_pe_percentile_3y",
    ]
    valuation_df = daily_basic_df[keep_cols].copy()
    valuation_df = valuation_df[
        (valuation_df["trade_date"] >= start_date)
        & (valuation_df["trade_date"] <= end_date)
    ].reset_index(drop=True)
    return valuation_df


def build_value_valuation_frame(stock_code, start_date, end_date, force_refresh=False):
    if not _is_domestic_equity(stock_code):
        return pd.DataFrame()

    valuation_start = _shift_date(start_date, -1100)
    daily_basic_df = fetch_daily_basic_data(
        stock_code,
        valuation_start,
        end_date,
        force_refresh=force_refresh,
    )
    if daily_basic_df.empty:
        return pd.DataFrame()

    daily_basic_df = daily_basic_df.sort_values(by="trade_date", ascending=True).reset_index(drop=True)
    daily_basic_df["pb"] = pd.to_numeric(daily_basic_df["pb"], errors="coerce")
    daily_basic_df["pe"] = pd.to_numeric(daily_basic_df["pe"], errors="coerce")
    daily_basic_df["dv_ttm"] = pd.to_numeric(daily_basic_df["dv_ttm"], errors="coerce")
    daily_basic_df["dv_ratio"] = pd.to_numeric(daily_basic_df["dv_ratio"], errors="coerce")
    daily_basic_df.loc[daily_basic_df["pb"] <= 0, "pb"] = pd.NA
    daily_basic_df.loc[daily_basic_df["pe"] <= 0, "pe"] = pd.NA

    daily_basic_df["value_pb_percentile_3y"] = daily_basic_df["pb"].rolling(
        window=750,
        min_periods=60,
    ).apply(_cheap_percentile, raw=False)
    daily_basic_df["value_pe_percentile_3y"] = daily_basic_df["pe"].rolling(
        window=750,
        min_periods=60,
    ).apply(_cheap_percentile, raw=False)
    daily_basic_df["value_pb"] = daily_basic_df["pb"]
    daily_basic_df["value_pe"] = daily_basic_df["pe"]
    daily_basic_df["value_dv_ttm"] = daily_basic_df["dv_ttm"]
    daily_basic_df["value_dv_ratio"] = daily_basic_df["dv_ratio"]
    daily_basic_df["value_total_mv"] = pd.to_numeric(daily_basic_df["total_mv"], errors="coerce")

    dividend_stability = pd.NA
    latest_cash_div_tax = pd.NA
    dividend_df = fetch_dividend_data(stock_code, force_refresh=force_refresh)
    if not dividend_df.empty and "cash_div_tax" in dividend_df.columns and "end_date" in dividend_df.columns:
        implemented_df = dividend_df[
            dividend_df["div_proc"].astype(str).str.contains("实施", na=False)
            & dividend_df["cash_div_tax"].notna()
            & (pd.to_numeric(dividend_df["cash_div_tax"], errors="coerce") > 0)
        ].copy()
        if not implemented_df.empty:
            implemented_df["cash_div_tax"] = pd.to_numeric(implemented_df["cash_div_tax"], errors="coerce")
            implemented_df["fiscal_year"] = implemented_df["end_date"].astype(str).str[:4]
            annual_dividend = implemented_df.groupby("fiscal_year", as_index=False)["cash_div_tax"].sum()
            annual_dividend = annual_dividend.sort_values(by="fiscal_year", ascending=True).reset_index(drop=True)
            recent = annual_dividend.tail(5)
            if not recent.empty:
                dividend_stability = float((recent["cash_div_tax"] > 0).mean())
                latest_cash_div_tax = recent["cash_div_tax"].iloc[-1]

    keep_cols = [
        "trade_date",
        "value_pb",
        "value_pe",
        "value_dv_ttm",
        "value_dv_ratio",
        "value_total_mv",
        "value_pb_percentile_3y",
        "value_pe_percentile_3y",
    ]
    valuation_df = daily_basic_df[keep_cols].copy()
    valuation_df["value_dividend_stability"] = dividend_stability
    valuation_df["value_latest_cash_div_tax"] = latest_cash_div_tax
    valuation_df = valuation_df[
        (valuation_df["trade_date"] >= start_date)
        & (valuation_df["trade_date"] <= end_date)
    ].reset_index(drop=True)
    return valuation_df


def resolve_industry_board_name(stock_code, industry_flow_df):
    industry_name = load_stock_industry_map().get(stock_code)
    if not industry_name or industry_flow_df.empty or "name" not in industry_flow_df.columns:
        return None

    available_names = {
        str(name).strip()
        for name in industry_flow_df["name"].dropna().astype(str).unique()
    }
    candidates = [industry_name] + INDUSTRY_FLOW_ALIASES.get(industry_name, [])

    for candidate in candidates:
        if candidate in available_names:
            return candidate

    fuzzy_matches = []
    for candidate in candidates:
        for available_name in available_names:
            if candidate in available_name or available_name in candidate:
                fuzzy_matches.append(available_name)
    if fuzzy_matches:
        return sorted(
            fuzzy_matches,
            key=lambda name: (abs(len(name) - len(industry_name)), len(name), name),
        )[0]

    return None


def build_industry_flow_frame(stock_code, start_date, end_date, force_refresh=False):
    industry_flow_df = fetch_industry_moneyflow(
        start_date,
        end_date,
        force_refresh=force_refresh,
    )
    if industry_flow_df.empty:
        return pd.DataFrame()

    board_name = resolve_industry_board_name(stock_code, industry_flow_df)
    if not board_name:
        return pd.DataFrame()

    industry_df = industry_flow_df[industry_flow_df["name"] == board_name].copy()
    if industry_df.empty:
        return pd.DataFrame()

    rename_map = {
        "name": "industry_flow_name",
        "pct_change": "industry_flow_pct_change",
        "close": "industry_flow_close",
        "net_amount": "industry_flow_net_amount",
        "net_amount_rate": "industry_flow_net_amount_rate",
        "buy_elg_amount": "industry_flow_buy_elg_amount",
        "buy_elg_amount_rate": "industry_flow_buy_elg_amount_rate",
        "buy_lg_amount": "industry_flow_buy_lg_amount",
        "buy_lg_amount_rate": "industry_flow_buy_lg_amount_rate",
        "buy_md_amount": "industry_flow_buy_md_amount",
        "buy_md_amount_rate": "industry_flow_buy_md_amount_rate",
        "buy_sm_amount": "industry_flow_buy_sm_amount",
        "buy_sm_amount_rate": "industry_flow_buy_sm_amount_rate",
        "rank": "industry_flow_rank",
    }
    keep_cols = ["trade_date"] + [col for col in rename_map.keys() if col in industry_df.columns]
    return industry_df[keep_cols].rename(columns=rename_map).reset_index(drop=True)


def enrich_with_market_context(df, start_date, end_date, force_refresh=False, stock_code=None):
    market_specs = {
        "sh_index": "000001.SH",
        "csi300": "399300.SZ",
    }
    enriched = df.copy()

    for prefix, index_code in market_specs.items():
        index_df = fetch_index_data(index_code, start_date, end_date, force_refresh=force_refresh).copy()
        if index_df.empty:
            continue
        rename_map = {
            "open": f"{prefix}_open",
            "high": f"{prefix}_high",
            "low": f"{prefix}_low",
            "close": f"{prefix}_close",
            "pct_chg": f"{prefix}_pct_chg",
            "vol": f"{prefix}_vol",
            "amount": f"{prefix}_amount",
        }
        keep_cols = ["trade_date"] + [col for col in rename_map.keys() if col in index_df.columns]
        index_df = index_df[keep_cols].rename(columns=rename_map)
        enriched = enriched.merge(index_df, on="trade_date", how="left")

    market_activity_df = build_market_activity_frame(
        start_date,
        end_date,
        force_refresh=force_refresh,
    )
    if not market_activity_df.empty:
        enriched = enriched.merge(market_activity_df, on="trade_date", how="left")

    market_moneyflow_df = fetch_market_moneyflow(
        start_date,
        end_date,
        force_refresh=force_refresh,
    )
    if not market_moneyflow_df.empty:
        rename_map = {
            "close_sh": "market_close_sh",
            "pct_change_sh": "market_pct_change_sh",
            "close_sz": "market_close_sz",
            "pct_change_sz": "market_pct_change_sz",
            "net_amount": "market_net_amount",
            "net_amount_rate": "market_net_amount_rate",
            "buy_elg_amount": "market_buy_elg_amount",
            "buy_elg_amount_rate": "market_buy_elg_amount_rate",
            "buy_lg_amount": "market_buy_lg_amount",
            "buy_lg_amount_rate": "market_buy_lg_amount_rate",
            "buy_md_amount": "market_buy_md_amount",
            "buy_md_amount_rate": "market_buy_md_amount_rate",
            "buy_sm_amount": "market_buy_sm_amount",
            "buy_sm_amount_rate": "market_buy_sm_amount_rate",
        }
        keep_cols = ["trade_date"] + [col for col in rename_map.keys() if col in market_moneyflow_df.columns]
        market_moneyflow_df = market_moneyflow_df[keep_cols].rename(columns=rename_map)
        enriched = enriched.merge(market_moneyflow_df, on="trade_date", how="left")

    if stock_code and (
        load_stock_industry_map().get(stock_code) in THEME_INDUSTRIES
        or stock_code in THEME_LEADER_CODES
    ):
        theme_sentiment_df = fetch_theme_market_sentiment(
            start_date,
            end_date,
            force_refresh=force_refresh,
        )
        if not theme_sentiment_df.empty:
            enriched = enriched.merge(theme_sentiment_df, on="trade_date", how="left")
        theme_board_df = fetch_theme_stock_board_states(
            start_date,
            end_date,
            force_refresh=force_refresh,
        )
        if not theme_board_df.empty:
            stock_board_df = (
                theme_board_df[theme_board_df["ts_code"] == stock_code]
                .drop(columns=["ts_code"])
                .reset_index(drop=True)
            )
            if not stock_board_df.empty:
                enriched = enriched.merge(stock_board_df, on="trade_date", how="left")

    macro_commodity_df = fetch_macro_commodity_index(
        start_date,
        end_date,
        force_refresh=force_refresh,
    )
    if not macro_commodity_df.empty:
        enriched = _merge_trade_date_asof(enriched, macro_commodity_df)

    if stock_code:
        enriched["stock_industry"] = load_stock_industry_map().get(stock_code)
        industry_flow_df = build_industry_flow_frame(
            stock_code,
            start_date,
            end_date,
            force_refresh=force_refresh,
        )
        if not industry_flow_df.empty:
            enriched = enriched.merge(industry_flow_df, on="trade_date", how="left")

        cycle_commodity_df = build_cycle_commodity_frame(
            stock_code,
            start_date,
            end_date,
            force_refresh=force_refresh,
        )
        if not cycle_commodity_df.empty:
            enriched = enriched.merge(cycle_commodity_df, on="trade_date", how="left")

        cycle_valuation_df = build_cycle_valuation_frame(
            stock_code,
            start_date,
            end_date,
            force_refresh=force_refresh,
        )
        if not cycle_valuation_df.empty:
            enriched = enriched.merge(cycle_valuation_df, on="trade_date", how="left")

        value_valuation_df = build_value_valuation_frame(
            stock_code,
            start_date,
            end_date,
            force_refresh=force_refresh,
        )
        if not value_valuation_df.empty:
            enriched = enriched.merge(value_valuation_df, on="trade_date", how="left")

        financial_timeline_df = build_financial_timeline(
            stock_code,
            force_refresh=force_refresh,
        )
        if not financial_timeline_df.empty:
            enriched = _merge_trade_date_asof(enriched, financial_timeline_df)

        bank_valuation_df = build_bank_valuation_frame(
            stock_code,
            start_date,
            end_date,
            force_refresh=force_refresh,
        )
        if not bank_valuation_df.empty:
            enriched = enriched.merge(bank_valuation_df, on="trade_date", how="left")

    return enriched


def _prepare_backtest_frame(df):
    trade_df = df.copy()
    if "trade_date" in trade_df.columns:
        trade_df = trade_df[trade_df["trade_date"].notna()].copy()
    trade_df = trade_df.reset_index(drop=True)
    if "signal" not in trade_df.columns:
        trade_df["signal"] = 0
    if "reason" not in trade_df.columns:
        trade_df["reason"] = "观望"
    return trade_df


def add_financial_peer_context(stock_frames):
    records = []
    industry_map = load_stock_industry_map()
    required_peer_cols = [
        "financial_quality_score",
        "financial_health_score",
        "financial_profit_yoy",
        "financial_revenue_yoy",
        "financial_operating_cashflow_yoy",
        "financial_roe",
    ]

    for stock_code, stock_df in stock_frames.items():
        if stock_df.empty or not _is_domestic_equity(stock_code):
            continue
        industry_name = industry_map.get(stock_code)
        if not industry_name:
            continue

        frame = stock_df.copy()
        frame["stock_code"] = stock_code
        frame["industry"] = industry_name

        quality_norm = (_numeric_series(frame, "financial_quality_score") / 100).clip(lower=0, upper=1)
        profit_norm = _scaled_score(_numeric_series(frame, "financial_profit_yoy"), -30, 40)
        revenue_norm = _scaled_score(_numeric_series(frame, "financial_revenue_yoy"), -20, 30)
        cashflow_yoy_norm = _scaled_score(_numeric_series(frame, "financial_operating_cashflow_yoy"), -30, 40)
        cashflow_positive = (_numeric_series(frame, "financial_operating_cashflow") > 0).astype("float64") * 0.7
        cashflow_norm = cashflow_yoy_norm.where(cashflow_yoy_norm.notna(), cashflow_positive)
        roe_norm = _scaled_score(_numeric_series(frame, "financial_roe"), 5, 20)

        frame["financial_health_score"] = _weighted_average_score(
            [
                (quality_norm, 0.35),
                (profit_norm, 0.2),
                (revenue_norm, 0.15),
                (cashflow_norm, 0.15),
                (roe_norm, 0.15),
            ],
            frame.index,
        )

        keep_cols = [
            "trade_date",
            "stock_code",
            "industry",
            "financial_quality_score",
            "financial_health_score",
            "financial_profit_yoy",
            "financial_revenue_yoy",
            "financial_operating_cashflow_yoy",
            "financial_roe",
        ]
        records.append(frame[[col for col in keep_cols if col in frame.columns]].copy())

    if not records:
        return stock_frames

    peer_df = pd.concat(records, ignore_index=True)
    peer_df = peer_df.dropna(subset=["trade_date", "industry"]).copy()
    if peer_df.empty:
        return stock_frames

    for col in required_peer_cols:
        if col not in peer_df.columns:
            peer_df[col] = pd.NA

    peer_group = peer_df.groupby(["trade_date", "industry"])
    peer_df["industry_peer_count"] = peer_group["stock_code"].transform("nunique")

    peer_rank_specs = [
        ("financial_quality_score", "peer_quality_pct"),
        ("financial_health_score", "peer_health_pct"),
        ("financial_profit_yoy", "peer_profit_pct"),
        ("financial_revenue_yoy", "peer_revenue_pct"),
        ("financial_operating_cashflow_yoy", "peer_cashflow_pct"),
        ("financial_roe", "peer_roe_pct"),
    ]
    for source_col, target_col in peer_rank_specs:
        if source_col not in peer_df.columns:
            continue
        source_series = pd.to_numeric(peer_df[source_col], errors="coerce")
        rank_series = (
            peer_df.assign(_source_value=source_series)
            .groupby(["trade_date", "industry"])["_source_value"]
            .rank(method="average", ascending=False, pct=True)
        )
        peer_df[target_col] = rank_series.where(peer_df["industry_peer_count"] >= UNIFIED_MIN_INDUSTRY_PEERS)

    peer_df["peer_financial_score"] = _weighted_average_score(
        [
            (_numeric_series(peer_df, "peer_quality_pct"), 0.25),
            (_numeric_series(peer_df, "peer_health_pct"), 0.2),
            (_numeric_series(peer_df, "peer_profit_pct"), 0.2),
            (_numeric_series(peer_df, "peer_revenue_pct"), 0.15),
            (_numeric_series(peer_df, "peer_cashflow_pct"), 0.1),
            (_numeric_series(peer_df, "peer_roe_pct"), 0.1),
        ],
        peer_df.index,
    )
    peer_df["peer_financial_rank_in_industry"] = (
        peer_group["peer_financial_score"].rank(method="dense", ascending=False)
    )
    peer_df["financial_gate_ok"] = (
        (peer_df["financial_health_score"] >= UNIFIED_FINANCIAL_HEALTH_FLOOR)
        & (peer_df["financial_quality_score"] >= UNIFIED_MIN_FINANCIAL_QUALITY_SCORE)
        & (
            (peer_df["industry_peer_count"] < UNIFIED_MIN_INDUSTRY_PEERS)
            | (peer_df["peer_financial_score"] >= UNIFIED_PEER_FINANCIAL_FLOOR)
        )
    )

    merge_cols = [
        "trade_date",
        "stock_code",
        "industry",
        "financial_health_score",
        "peer_financial_score",
        "peer_financial_rank_in_industry",
        "industry_peer_count",
        "financial_gate_ok",
    ]
    peer_df = peer_df[merge_cols].copy()

    enriched_frames = {}
    for stock_code, stock_df in stock_frames.items():
        if stock_df.empty:
            enriched_frames[stock_code] = stock_df
            continue
        peer_view = peer_df[peer_df["stock_code"] == stock_code].drop(columns=["stock_code"])
        if peer_view.empty:
            enriched_frames[stock_code] = stock_df
            continue
        enriched_frames[stock_code] = stock_df.merge(peer_view, on="trade_date", how="left")
    return enriched_frames


def _resolve_financial_gate_policy(strategy_instance):
    policy = {
        "health_floor": UNIFIED_FINANCIAL_HEALTH_FLOOR,
        "quality_floor": UNIFIED_MIN_FINANCIAL_QUALITY_SCORE,
        "peer_floor": UNIFIED_PEER_FINANCIAL_FLOOR,
        "use_peer_score": True,
        "allow_missing": False,
    }
    if isinstance(strategy_instance, bank_t_strategy):
        policy.update(
            {
                "health_floor": 0.35,
                "quality_floor": 15,
                "peer_floor": 0.2,
                "use_peer_score": True,
                "allow_missing": False,
            }
        )
    elif isinstance(strategy_instance, theme_with_sentiment_strategy):
        policy.update(
            {
                "health_floor": 0.32,
                "quality_floor": 15,
                "peer_floor": 0.0,
                "use_peer_score": False,
                "allow_missing": True,
            }
        )
    return policy


def _evaluate_financial_gate(row, stock_code, strategy_instance=None):
    if not _is_domestic_equity(stock_code):
        return True, None
    policy = _resolve_financial_gate_policy(strategy_instance)

    health_score = pd.to_numeric(pd.Series([row.get("financial_health_score", pd.NA)]), errors="coerce").iloc[0]
    peer_score = pd.to_numeric(pd.Series([row.get("peer_financial_score", pd.NA)]), errors="coerce").iloc[0]
    peer_rank = pd.to_numeric(pd.Series([row.get("peer_financial_rank_in_industry", pd.NA)]), errors="coerce").iloc[0]
    peer_count = pd.to_numeric(pd.Series([row.get("industry_peer_count", pd.NA)]), errors="coerce").iloc[0]
    quality_score = pd.to_numeric(pd.Series([row.get("financial_quality_score", pd.NA)]), errors="coerce").iloc[0]

    if pd.isna(health_score) or pd.isna(quality_score):
        if policy["allow_missing"]:
            return True, None
        return False, "财报数据不完整，暂不纳入买入候选"

    peer_requirement_failed = (
        policy["use_peer_score"]
        and pd.notna(peer_count)
        and peer_count >= UNIFIED_MIN_INDUSTRY_PEERS
        and (pd.isna(peer_score) or peer_score < policy["peer_floor"])
    )
    health_requirement_failed = (
        health_score < policy["health_floor"]
        or quality_score < policy["quality_floor"]
    )
    if not health_requirement_failed and not peer_requirement_failed:
        return True, None

    detail_parts = [
        f"财报健康度 {health_score:.2f}",
        f"财报质量分 {quality_score:.0f}",
    ]
    if (
        policy["use_peer_score"]
        and
        pd.notna(peer_count)
        and peer_count >= UNIFIED_MIN_INDUSTRY_PEERS
    ):
        if pd.notna(peer_score):
            detail_parts.append(f"同行分位 {peer_score:.2f}")
        if pd.notna(peer_rank):
            detail_parts.append(f"同行排名 {int(peer_rank)}/{int(peer_count)}")
    if policy["use_peer_score"]:
        return False, "财报健康度或同行业财报对比不足，取消买入: " + "，".join(detail_parts)
    return False, "财报健康度不足，取消买入: " + "，".join(detail_parts)


def apply_unified_financial_gate(trading_signal, stock_code, strategy_instance=None):
    gated_df = trading_signal.copy()
    if gated_df.empty or not _is_domestic_equity(stock_code):
        return gated_df

    has_target_position = "target_position" in gated_df.columns
    for idx in range(len(gated_df)):
        row = gated_df.iloc[idx]
        gate_ok, gate_reason = _evaluate_financial_gate(row, stock_code, strategy_instance=strategy_instance)
        if gate_ok:
            continue

        if has_target_position:
            current_target = 0.0
            if idx > 0:
                previous_targets = gated_df.iloc[:idx]["target_position"].dropna()
                if not previous_targets.empty:
                    current_target = float(previous_targets.iloc[-1] or 0.0)
            next_target = row.get("target_position", pd.NA)
            if pd.notna(next_target) and float(next_target) > current_target + 1e-8:
                gated_df.loc[idx, "target_position"] = current_target
                gated_df.loc[idx, "signal"] = 0
                gated_df.loc[idx, "reason"] = gate_reason
        elif row.get("signal", 0) == 1:
            gated_df.loc[idx, "signal"] = 0
            gated_df.loc[idx, "reason"] = gate_reason
    return gated_df


def apply_unified_financial_gate_to_prediction(prediction, trade_df, stock_code, strategy_instance=None):
    if not prediction or not _is_domestic_equity(stock_code):
        return prediction

    buy_like_signals = {"买入", "建底仓", "加仓"}
    if prediction.get("signal") not in buy_like_signals:
        return prediction

    trade_rows = trade_df[trade_df["trade_date"].notna()].copy().reset_index(drop=True)
    if trade_rows.empty:
        return prediction

    gate_ok, gate_reason = _evaluate_financial_gate(
        trade_rows.iloc[-1],
        stock_code,
        strategy_instance=strategy_instance,
    )
    if gate_ok:
        return prediction

    adjusted = prediction.copy()
    current_target = None
    if "target_position" in trade_rows.columns:
        previous_targets = trade_rows["target_position"].dropna()
        if not previous_targets.empty:
            current_target = float(previous_targets.iloc[-1] or 0.0)
    if current_target is not None:
        adjusted["target_position"] = current_target

    if current_target is not None and current_target > 0:
        adjusted["signal"] = "持仓"
    else:
        adjusted["signal"] = "观望"
    adjusted["reason"] = gate_reason
    return adjusted


def add_cycle_leader_context(stock_frames):
    records = []
    for stock_code, stock_df in stock_frames.items():
        if stock_df.empty:
            continue
        frame = stock_df.copy()
        frame["leader_ret_20"] = frame["close"].pct_change(20, fill_method=None)
        frame["leader_ret_5"] = frame["close"].pct_change(5, fill_method=None)
        if "amount" in frame.columns:
            frame["leader_amount_ma20"] = frame["amount"].rolling(window=20).mean().shift(1)
            frame["leader_amount_ratio"] = frame["amount"] / frame["leader_amount_ma20"].replace(0, pd.NA)
        else:
            frame["leader_amount_ratio"] = pd.NA
        frame["leader_prior_high_120"] = frame["close"].rolling(window=120).max().shift(1)
        frame["leader_high_ratio"] = frame["close"] / frame["leader_prior_high_120"].replace(0, pd.NA)
        frame["stock_code"] = stock_code
        keep_cols = [
            "trade_date",
            "stock_code",
            "leader_ret_20",
            "leader_ret_5",
            "leader_amount_ratio",
            "leader_high_ratio",
        ]
        if "industry_flow_net_amount_rate" in frame.columns:
            keep_cols.append("industry_flow_net_amount_rate")
        records.append(frame[[col for col in keep_cols if col in frame.columns]])

    if not records:
        return stock_frames

    leader_df = pd.concat(records, ignore_index=True)
    leader_df = leader_df.dropna(subset=["trade_date"]).copy()
    group = leader_df.groupby("trade_date")

    def _score_series(column_name):
        if column_name in leader_df.columns:
            return leader_df[column_name].fillna(0)
        return pd.Series(0.0, index=leader_df.index, dtype="float64")

    for source_col, target_col, ascending in [
        ("leader_ret_20", "leader_ret_20_pct", False),
        ("leader_ret_5", "leader_ret_5_pct", False),
        ("leader_amount_ratio", "leader_amount_ratio_pct", False),
        ("leader_high_ratio", "leader_high_ratio_pct", False),
        ("industry_flow_net_amount_rate", "leader_industry_flow_pct", False),
    ]:
        if source_col in leader_df.columns:
            leader_df[target_col] = group[source_col].rank(
                method="average",
                ascending=ascending,
                pct=True,
            )

    leader_df["leader_score"] = (
        _score_series("leader_ret_20_pct") * 35
        + _score_series("leader_ret_5_pct") * 20
        + _score_series("leader_amount_ratio_pct") * 20
        + _score_series("leader_high_ratio_pct") * 15
        + _score_series("leader_industry_flow_pct") * 10
    )
    leader_df["leader_candidate"] = (
        (leader_df["leader_score"] >= 60)
        & (_score_series("leader_ret_20_pct") >= 0.6)
        & (_score_series("leader_amount_ratio_pct") >= 0.4)
    )
    leader_df["leader_rank_in_pool"] = group["leader_score"].rank(
        method="dense",
        ascending=False,
    )
    merge_cols = [
        "trade_date",
        "stock_code",
        "leader_score",
        "leader_candidate",
        "leader_rank_in_pool",
    ]
    leader_df = leader_df[merge_cols].copy()

    enriched_frames = {}
    for stock_code, stock_df in stock_frames.items():
        leader_view = leader_df[leader_df["stock_code"] == stock_code].drop(columns=["stock_code"])
        enriched_frames[stock_code] = stock_df.merge(leader_view, on="trade_date", how="left")
    return enriched_frames


def add_theme_leader_context(stock_frames):
    records = []
    for stock_code, stock_df in stock_frames.items():
        if stock_df.empty:
            continue
        frame = stock_df.copy()
        frame["theme_leader_ret_10"] = frame["close"].pct_change(10, fill_method=None)
        frame["theme_leader_ret_5"] = frame["close"].pct_change(5, fill_method=None)
        if "amount" in frame.columns:
            frame["theme_leader_amount_ma10"] = frame["amount"].rolling(window=10).mean().shift(1)
            frame["theme_leader_amount_ratio"] = (
                frame["amount"] / frame["theme_leader_amount_ma10"].replace(0, pd.NA)
            )
        else:
            frame["theme_leader_amount_ratio"] = pd.NA
        frame["theme_prior_high_60"] = frame["close"].rolling(window=60).max().shift(1)
        frame["theme_leader_high_ratio"] = frame["close"] / frame["theme_prior_high_60"].replace(0, pd.NA)
        frame["stock_code"] = stock_code
        records.append(
            frame[
                [
                    "trade_date",
                    "stock_code",
                    "theme_leader_ret_10",
                    "theme_leader_ret_5",
                    "theme_leader_amount_ratio",
                    "theme_leader_high_ratio",
                ]
            ]
        )

    if not records:
        return stock_frames

    leader_df = pd.concat(records, ignore_index=True)
    leader_df = leader_df.dropna(subset=["trade_date"]).copy()
    group = leader_df.groupby("trade_date")

    def _score_series(column_name):
        if column_name in leader_df.columns:
            return leader_df[column_name].fillna(0)
        return pd.Series(0.0, index=leader_df.index, dtype="float64")

    for source_col, target_col in [
        ("theme_leader_ret_10", "theme_leader_ret_10_pct"),
        ("theme_leader_ret_5", "theme_leader_ret_5_pct"),
        ("theme_leader_amount_ratio", "theme_leader_amount_ratio_pct"),
        ("theme_leader_high_ratio", "theme_leader_high_ratio_pct"),
    ]:
        leader_df[target_col] = group[source_col].rank(
            method="average",
            ascending=False,
            pct=True,
        )

    leader_df["theme_leader_score"] = (
        _score_series("theme_leader_ret_10_pct") * 35
        + _score_series("theme_leader_ret_5_pct") * 25
        + _score_series("theme_leader_amount_ratio_pct") * 25
        + _score_series("theme_leader_high_ratio_pct") * 15
    )
    leader_df["theme_leader_candidate"] = (
        (leader_df["theme_leader_score"] >= 60)
        & (_score_series("theme_leader_ret_10_pct") >= 0.6)
        & (_score_series("theme_leader_amount_ratio_pct") >= 0.5)
    )
    leader_df["theme_leader_rank_in_pool"] = group["theme_leader_score"].rank(
        method="dense",
        ascending=False,
    )

    merge_cols = [
        "trade_date",
        "stock_code",
        "theme_leader_score",
        "theme_leader_candidate",
        "theme_leader_rank_in_pool",
    ]
    leader_df = leader_df[merge_cols].copy()

    enriched_frames = {}
    for stock_code, stock_df in stock_frames.items():
        leader_view = leader_df[leader_df["stock_code"] == stock_code].drop(columns=["stock_code"])
        enriched_frames[stock_code] = stock_df.merge(leader_view, on="trade_date", how="left")
    return enriched_frames


def _theme_score_clip(value, lower, upper):
    if pd.isna(value):
        return 0.0
    if upper == lower:
        return 0.0
    scaled = (float(value) - lower) / (upper - lower)
    return max(0.0, min(1.0, scaled))


def prepare_theme_stock_frames(start_date, end_date, scope="all", force_refresh=False):
    stock_pool = load_theme_stock_pool(scope=scope)
    prepared_frames = {}
    for stock_code in stock_pool:
        try:
            stock_df = fetch_stock_data(stock_code, start_date, end_date, force_refresh=force_refresh)
            prepared_frames[stock_code] = enrich_with_market_context(
                stock_df,
                start_date,
                end_date,
                force_refresh=force_refresh,
                stock_code=stock_code,
            )
        except Exception as exc:
            print(f"处理股票 {stock_code} 时出错: {str(exc)}")

    prepared_frames = add_theme_leader_context(prepared_frames)
    prepared_frames = add_financial_peer_context(prepared_frames)
    return prepared_frames


def _build_theme_snapshot(stock_code, df):
    frame = df.copy().reset_index(drop=True)
    if len(frame) < 30:
        return None

    frame["ret_5"] = frame["close"].pct_change(5, fill_method=None)
    frame["ret_20"] = frame["close"].pct_change(20, fill_method=None)
    if "amount" in frame.columns:
        frame["amount_ma10"] = frame["amount"].rolling(window=10).mean().shift(1)
        frame["amount_ratio_10"] = frame["amount"] / frame["amount_ma10"].replace(0, pd.NA)
    else:
        frame["amount_ratio_10"] = pd.NA
    if "vol" in frame.columns:
        frame["volume_ma10"] = frame["vol"].rolling(window=10).mean().shift(1)
        frame["volume_ma20"] = frame["vol"].rolling(window=20).mean().shift(1)
        frame["volume_ratio_10"] = frame["vol"] / frame["volume_ma10"].replace(0, pd.NA)
        frame["volume_ratio_20"] = frame["vol"] / frame["volume_ma20"].replace(0, pd.NA)
    else:
        frame["volume_ratio_10"] = pd.NA
        frame["volume_ratio_20"] = pd.NA

    for col in [
        "theme_is_limit_up",
        "theme_is_broken",
        "theme_is_first_board",
        "theme_is_second_board",
        "theme_is_reseal",
    ]:
        if col not in frame.columns:
            frame[col] = 0
        frame[col] = frame[col].fillna(0)

    frame["theme_limit_hit_count_10"] = frame["theme_is_limit_up"].rolling(window=10).sum().shift(1)
    frame["theme_first_board_count_10"] = frame["theme_is_first_board"].rolling(window=10).sum().shift(1)
    frame["theme_second_board_count_10"] = frame["theme_is_second_board"].rolling(window=10).sum().shift(1)
    frame["theme_reseal_count_10"] = frame["theme_is_reseal"].rolling(window=10).sum().shift(1)
    frame["theme_broken_count_10"] = frame["theme_is_broken"].rolling(window=10).sum().shift(1)

    latest = frame.iloc[-1]
    financial_health = latest.get("financial_health_score", pd.NA)
    peer_score = latest.get("peer_financial_score", pd.NA)
    leader_score = latest.get("theme_leader_score", pd.NA)
    industry_rank = latest.get("industry_flow_rank", pd.NA)
    sentiment_score = latest.get("theme_sentiment_score", pd.NA)
    amount_ratio = latest.get("amount_ratio_10", pd.NA)
    volume_ratio_10 = latest.get("volume_ratio_10", pd.NA)
    volume_ratio_20 = latest.get("volume_ratio_20", pd.NA)
    ret_20 = latest.get("ret_20", pd.NA)
    ret_5 = latest.get("ret_5", pd.NA)
    board_count = latest.get("theme_board_count", pd.NA)
    heat_score = latest.get("theme_heat_score", pd.NA)
    highest_board = latest.get("theme_highest_board", pd.NA)
    multi_board_count = latest.get("theme_multi_board_count", pd.NA)
    limit_hits = latest.get("theme_limit_hit_count_10", 0)
    first_board_hits = latest.get("theme_first_board_count_10", 0)
    second_board_hits = latest.get("theme_second_board_count_10", 0)
    reseal_hits = latest.get("theme_reseal_count_10", 0)
    broken_hits = latest.get("theme_broken_count_10", 0)

    pick_score = 0.0
    pick_score += _theme_score_clip(leader_score, 40, 90) * 25
    pick_score += _theme_score_clip(ret_20, -0.1, 0.5) * 18
    pick_score += _theme_score_clip(ret_5, -0.05, 0.2) * 8
    pick_score += _theme_score_clip(amount_ratio, 0.8, 2.2) * 12
    pick_score += _theme_score_clip(volume_ratio_10, 0.8, 2.0) * 7
    pick_score += _theme_score_clip(volume_ratio_20, 0.8, 1.8) * 8
    pick_score += _theme_score_clip(sentiment_score, 40, 85) * 10
    pick_score += _theme_score_clip(heat_score, 20, 75) * 8
    pick_score += _theme_score_clip(highest_board, 1, 5) * 5
    pick_score += _theme_score_clip(multi_board_count, 1, 16) * 6
    pick_score += _theme_score_clip(board_count, 1, 4) * 4
    pick_score += _theme_score_clip(financial_health, 0.3, 0.9) * 12
    pick_score += _theme_score_clip(peer_score, 0.2, 0.8) * 8
    if pd.notna(industry_rank):
        pick_score += max(0.0, min(10.0, (50 - float(industry_rank)) / 5))
    pick_score += min(8.0, float(limit_hits or 0) * 2.0)
    pick_score += min(7.0, float(first_board_hits or 0) * 3.5)
    pick_score += min(7.0, float(second_board_hits or 0) * 3.5)
    pick_score += min(8.0, float(reseal_hits or 0) * 4.0)
    pick_score -= min(12.0, float(broken_hits or 0) * 3.0)
    if pd.notna(ret_20) and float(ret_20) < 0:
        pick_score -= min(18.0, abs(float(ret_20)) * 40)
    if pd.notna(ret_5) and float(ret_5) < 0:
        pick_score -= min(8.0, abs(float(ret_5)) * 40)

    if pick_score >= 60:
        status = "重点跟踪"
    elif pick_score >= 45:
        status = "观察"
    else:
        status = "放弃"

    return {
        "stock_code": stock_code,
        "trade_date": latest["trade_date"],
        "theme_pick_score": round(pick_score, 2),
        "leader_score": round(float(leader_score), 2) if pd.notna(leader_score) else pd.NA,
        "ret_20": round(float(ret_20) * 100, 2) if pd.notna(ret_20) else pd.NA,
        "ret_5": round(float(ret_5) * 100, 2) if pd.notna(ret_5) else pd.NA,
        "amount_ratio_10": round(float(amount_ratio), 2) if pd.notna(amount_ratio) else pd.NA,
        "volume_ratio_10": round(float(volume_ratio_10), 2) if pd.notna(volume_ratio_10) else pd.NA,
        "volume_ratio_20": round(float(volume_ratio_20), 2) if pd.notna(volume_ratio_20) else pd.NA,
        "theme_sentiment_score": round(float(sentiment_score), 2) if pd.notna(sentiment_score) else pd.NA,
        "theme_heat_score": round(float(heat_score), 2) if pd.notna(heat_score) else pd.NA,
        "industry_flow_rank": round(float(industry_rank), 2) if pd.notna(industry_rank) else pd.NA,
        "theme_board_count": int(board_count or 0) if pd.notna(board_count) else 0,
        "theme_highest_board": int(highest_board or 0) if pd.notna(highest_board) else 0,
        "theme_multi_board_count": int(multi_board_count or 0) if pd.notna(multi_board_count) else 0,
        "limit_hits_10": int(limit_hits or 0),
        "first_board_hits_10": int(first_board_hits or 0),
        "second_board_hits_10": int(second_board_hits or 0),
        "reseal_hits_10": int(reseal_hits or 0),
        "broken_hits_10": int(broken_hits or 0),
        "financial_health_score": round(float(financial_health), 2) if pd.notna(financial_health) else pd.NA,
        "peer_financial_score": round(float(peer_score), 2) if pd.notna(peer_score) else pd.NA,
        "status": status,
    }


def screen_theme_stocks(days=120, scope="all", top=15, force_refresh=False):
    start_date, end_date = get_date_range(days)
    prepared_frames = prepare_theme_stock_frames(
        start_date,
        end_date,
        scope=scope,
        force_refresh=force_refresh,
    )

    snapshots = []
    for stock_code, stock_df in prepared_frames.items():
        if stock_df is None or stock_df.empty:
            continue
        snapshot = _build_theme_snapshot(stock_code, stock_df)
        if snapshot:
            snapshots.append(snapshot)

    if not snapshots:
        return pd.DataFrame()

    result_df = pd.DataFrame(snapshots)
    name_map = load_stock_name_map()
    industry_map = load_stock_industry_map()
    result_df["name"] = result_df["stock_code"].map(name_map).fillna(result_df["stock_code"])
    result_df["industry"] = result_df["stock_code"].map(industry_map)
    result_df = result_df.sort_values(
        by=["theme_pick_score", "leader_score", "financial_health_score"],
        ascending=[False, False, False],
    ).reset_index(drop=True)
    return result_df.head(top)


def load_screened_theme_stock_pool(days=120, scope="all", top=10, force_refresh=False):
    screened_df = screen_theme_stocks(
        days=days,
        scope=scope,
        top=top,
        force_refresh=force_refresh,
    )
    if screened_df.empty:
        return load_theme_stock_pool(scope="leaders")
    filtered_df = screened_df[screened_df["status"].isin(["重点跟踪", "观察"])].copy()
    if filtered_df.empty:
        filtered_df = screened_df.head(min(top, 5)).copy()
    return filtered_df["stock_code"].dropna().astype(str).tolist()


def prepare_value_stock_frames(start_date, end_date, scope="sample", limit=0, force_refresh=False):
    stock_pool = load_value_stock_pool(scope=scope, limit=limit)
    prepared_frames = {}
    for stock_code in stock_pool:
        try:
            stock_df = fetch_stock_data(stock_code, start_date, end_date, force_refresh=force_refresh)
            prepared_frames[stock_code] = enrich_with_market_context(
                stock_df,
                start_date,
                end_date,
                force_refresh=force_refresh,
                stock_code=stock_code,
            )
        except Exception as exc:
            print(f"处理股票 {stock_code} 时出错: {str(exc)}")

    prepared_frames = add_financial_peer_context(prepared_frames)
    return prepared_frames


def _build_value_snapshot(stock_code, df):
    frame = df.copy().reset_index(drop=True)
    if len(frame) < 60:
        return None

    frame["ret_20"] = frame["close"].pct_change(20, fill_method=None)
    frame["ret_60"] = frame["close"].pct_change(60, fill_method=None)
    frame["ma20"] = frame["close"].rolling(window=20, min_periods=10).mean()
    frame["ma60"] = frame["close"].rolling(window=60, min_periods=20).mean()
    frame["low_20"] = frame["close"].rolling(window=20, min_periods=10).min().shift(1)
    frame["high_60"] = frame["close"].rolling(window=60, min_periods=20).max().shift(1)
    if "sh_index_close" in frame.columns:
        frame["market_ret_20"] = frame["sh_index_close"].pct_change(20, fill_method=None)
        frame["value_rs_20"] = frame["ret_20"] - frame["market_ret_20"]
    else:
        frame["value_rs_20"] = pd.NA

    latest = frame.iloc[-1]
    pb_pct = latest.get("value_pb_percentile_3y", pd.NA)
    pe_pct = latest.get("value_pe_percentile_3y", pd.NA)
    health_score = latest.get("financial_health_score", pd.NA)
    peer_score = latest.get("peer_financial_score", pd.NA)
    quality_score = latest.get("financial_quality_score", pd.NA)
    roe = latest.get("financial_roe", pd.NA)
    profit_yoy = latest.get("financial_profit_yoy", pd.NA)
    revenue_yoy = latest.get("financial_revenue_yoy", pd.NA)
    dividend_yield = latest.get("value_dv_ttm", pd.NA)
    total_mv = latest.get("value_total_mv", pd.NA)
    dividend_stability = latest.get("value_dividend_stability", pd.NA)
    ret20 = latest.get("ret_20", pd.NA)
    ret60 = latest.get("ret_60", pd.NA)
    rs20 = latest.get("value_rs_20", pd.NA)
    close_price = latest.get("close", pd.NA)
    ma20 = latest.get("ma20", pd.NA)
    ma60 = latest.get("ma60", pd.NA)
    low20 = latest.get("low_20", pd.NA)
    high60 = latest.get("high_60", pd.NA)
    industry_flow_pct = latest.get("industry_flow_pct_change", pd.NA)
    industry_flow_net_rate = latest.get("industry_flow_net_amount_rate", pd.NA)
    close_to_ma60_pct = pd.NA
    close_to_low20_pct = pd.NA
    close_to_high60_pct = pd.NA
    if pd.notna(close_price) and pd.notna(ma60) and ma60:
        close_to_ma60_pct = (float(close_price) / float(ma60) - 1) * 100
    if pd.notna(close_price) and pd.notna(low20) and low20:
        close_to_low20_pct = (float(close_price) / float(low20) - 1) * 100
    if pd.notna(close_price) and pd.notna(high60) and high60:
        close_to_high60_pct = (float(close_price) / float(high60) - 1) * 100

    pick_score = 0.0
    pick_score += _theme_score_clip(pb_pct, 40, 95) * 30
    pick_score += _theme_score_clip(pe_pct, 35, 95) * 22
    pick_score += _theme_score_clip(health_score, 0.35, 0.9) * 18
    pick_score += _theme_score_clip(peer_score, 0.2, 0.85) * 12
    pick_score += _theme_score_clip(quality_score, 25, 95) * 10
    pick_score += _theme_score_clip(roe, 6, 20) * 5
    pick_score += _theme_score_clip(profit_yoy, -15, 35) * 6
    pick_score += _theme_score_clip(revenue_yoy, -10, 25) * 4
    pick_score += _theme_score_clip(dividend_yield, 0.5, 6.0) * 5
    pick_score += _theme_score_clip(total_mv, 800000, 8000000) * 8
    pick_score += _theme_score_clip(dividend_stability, 0.2, 1.0) * 5
    pick_score += _theme_score_clip(ret20, -0.2, 0.2) * 6
    pick_score += _theme_score_clip(ret60, -0.3, 0.35) * 6
    pick_score += _theme_score_clip(rs20, -0.15, 0.15) * 8
    pick_score += _theme_score_clip(industry_flow_pct, -3, 3) * 4
    pick_score += _theme_score_clip(industry_flow_net_rate, -15, 15) * 4
    if pd.notna(close_price) and pd.notna(ma20) and close_price >= ma20:
        pick_score += 3.0
    if pd.notna(close_price) and pd.notna(ma60) and close_price >= ma60:
        pick_score += 4.0

    if pick_score >= 68:
        status = "重点配置"
    elif pick_score >= 55:
        status = "跟踪"
    else:
        status = "观察"

    return {
        "stock_code": stock_code,
        "trade_date": latest["trade_date"],
        "value_pick_score": round(pick_score, 2),
        "value_pb_percentile_3y": round(float(pb_pct), 2) if pd.notna(pb_pct) else pd.NA,
        "value_pe_percentile_3y": round(float(pe_pct), 2) if pd.notna(pe_pct) else pd.NA,
        "financial_health_score": round(float(health_score), 2) if pd.notna(health_score) else pd.NA,
        "peer_financial_score": round(float(peer_score), 2) if pd.notna(peer_score) else pd.NA,
        "financial_quality_score": round(float(quality_score), 2) if pd.notna(quality_score) else pd.NA,
        "financial_roe": round(float(roe), 2) if pd.notna(roe) else pd.NA,
        "financial_profit_yoy": round(float(profit_yoy), 2) if pd.notna(profit_yoy) else pd.NA,
        "financial_revenue_yoy": round(float(revenue_yoy), 2) if pd.notna(revenue_yoy) else pd.NA,
        "value_dv_ttm": round(float(dividend_yield), 2) if pd.notna(dividend_yield) else pd.NA,
        "value_total_mv": round(float(total_mv), 2) if pd.notna(total_mv) else pd.NA,
        "value_dividend_stability": round(float(dividend_stability), 2) if pd.notna(dividend_stability) else pd.NA,
        "value_ret_20": round(float(ret20) * 100, 2) if pd.notna(ret20) else pd.NA,
        "value_ret_60": round(float(ret60) * 100, 2) if pd.notna(ret60) else pd.NA,
        "value_rs_20": round(float(rs20) * 100, 2) if pd.notna(rs20) else pd.NA,
        "value_close_to_ma60_pct": round(float(close_to_ma60_pct), 2) if pd.notna(close_to_ma60_pct) else pd.NA,
        "value_close_to_low20_pct": round(float(close_to_low20_pct), 2) if pd.notna(close_to_low20_pct) else pd.NA,
        "value_close_to_high60_pct": round(float(close_to_high60_pct), 2) if pd.notna(close_to_high60_pct) else pd.NA,
        "industry_flow_pct_change": round(float(industry_flow_pct), 2) if pd.notna(industry_flow_pct) else pd.NA,
        "industry_flow_net_amount_rate": round(float(industry_flow_net_rate), 2) if pd.notna(industry_flow_net_rate) else pd.NA,
        "status": status,
    }


def _apply_value_long_hold_filter(result_df):
    if result_df.empty:
        return result_df

    result_df = result_df.copy()
    result_df["industry_bias"] = 0.0
    result_df.loc[result_df["industry"].isin(VALUE_PREFERRED_INDUSTRIES), "industry_bias"] = 8.0
    result_df.loc[result_df["industry"].isin(VALUE_TRAP_INDUSTRIES), "industry_bias"] = -20.0

    trap_reasons = []
    adjusted_scores = []
    statuses = []
    for _, row in result_df.iterrows():
        reasons = []
        name = str(row.get("name", "") or "")
        industry = row.get("industry", "")
        if any(keyword in name for keyword in VALUE_AVOID_NAME_KEYWORDS):
            reasons.append("名称存在 ST/退市风险标记")
        if industry in VALUE_TRAP_INDUSTRIES:
            reasons.append("行业更像低估值陷阱，不适合长期耐心持有")

        health_score = row.get("financial_health_score", pd.NA)
        peer_score = row.get("peer_financial_score", pd.NA)
        quality_score = row.get("financial_quality_score", pd.NA)
        roe = row.get("financial_roe", pd.NA)
        profit_yoy = row.get("financial_profit_yoy", pd.NA)
        revenue_yoy = row.get("financial_revenue_yoy", pd.NA)
        total_mv = row.get("value_total_mv", pd.NA)
        dividend_stability = row.get("value_dividend_stability", pd.NA)
        ret20 = row.get("value_ret_20", pd.NA)
        ret60 = row.get("value_ret_60", pd.NA)
        rs20 = row.get("value_rs_20", pd.NA)
        industry_flow_pct = row.get("industry_flow_pct_change", pd.NA)
        industry_flow_net_rate = row.get("industry_flow_net_amount_rate", pd.NA)

        if pd.notna(health_score) and float(health_score) < 0.35:
            reasons.append("财报健康度太弱")
        if pd.notna(peer_score) and float(peer_score) < 0.25:
            reasons.append("同行财报对比明显落后")
        if pd.notna(quality_score) and float(quality_score) < 20:
            reasons.append("综合财报质量偏弱")
        if pd.notna(roe) and float(roe) < 5:
            reasons.append("ROE 偏低")
        if pd.notna(profit_yoy) and float(profit_yoy) < -25:
            reasons.append("利润下滑过深")
        if pd.notna(revenue_yoy) and float(revenue_yoy) < -15:
            reasons.append("营收下滑过深")
        if pd.notna(total_mv) and float(total_mv) < 500000:
            reasons.append("市值过小，不适合长期核心持有")
        if industry not in VALUE_PREFERRED_INDUSTRIES and pd.notna(total_mv) and float(total_mv) < 1500000:
            reasons.append("不属于长期优选行业且市值不够大")
        if pd.notna(dividend_stability) and float(dividend_stability) < 0.4 and industry in VALUE_PREFERRED_INDUSTRIES:
            reasons.append("分红稳定性偏弱")
        if pd.notna(ret20) and float(ret20) <= -12:
            reasons.append("最近20日仍明显走弱")
        if pd.notna(rs20) and float(rs20) <= -8:
            reasons.append("相对市场仍明显偏弱")
        if pd.notna(industry_flow_pct) and float(industry_flow_pct) <= -3:
            reasons.append("行业短期表现仍偏弱")
        if pd.notna(industry_flow_net_rate) and float(industry_flow_net_rate) <= -12:
            reasons.append("行业资金流仍明显偏弱")
        if industry == "白酒" and (
            (pd.notna(ret20) and float(ret20) < 0)
            or (pd.notna(rs20) and float(rs20) < 0)
            or (pd.notna(industry_flow_pct) and float(industry_flow_pct) < 0)
        ):
            reasons.append("白酒行业尚未走出弱势，不进入核心长期池")
        if industry in {"中成药", "化学制药", "生物制药", "医疗保健", "医药商业"} and (
            (pd.notna(ret20) and float(ret20) <= -8)
            or (pd.notna(rs20) and float(rs20) <= -3)
        ):
            reasons.append("医药当前仍偏弱，暂不作为核心长期池")
        if industry in {"火力发电", "新型电力", "供气供热", "银行"} and (
            pd.notna(dividend_stability) and float(dividend_stability) < 0.8
        ):
            reasons.append("红利稳定性还不够高，暂不进入核心长期池")

        adjusted_score = float(row["value_pick_score"]) + float(row.get("industry_bias", 0.0))
        if pd.notna(total_mv) and float(total_mv) >= 2000000:
            adjusted_score += 6.0
        elif pd.notna(total_mv) and float(total_mv) >= 1000000:
            adjusted_score += 3.0
        elif pd.notna(total_mv) and float(total_mv) < 800000:
            adjusted_score -= 8.0

        if pd.notna(dividend_stability) and float(dividend_stability) >= 0.8:
            adjusted_score += 3.0
        elif pd.notna(dividend_stability) and float(dividend_stability) < 0.4:
            adjusted_score -= 4.0

        if pd.notna(ret20) and float(ret20) > 0:
            adjusted_score += 5.0
        elif pd.notna(ret20) and float(ret20) < -8:
            adjusted_score -= 10.0

        if pd.notna(rs20) and float(rs20) > 3:
            adjusted_score += 6.0
        elif pd.notna(rs20) and float(rs20) < -5:
            adjusted_score -= 12.0

        if pd.notna(industry_flow_pct) and float(industry_flow_pct) > 1:
            adjusted_score += 3.0
        elif pd.notna(industry_flow_pct) and float(industry_flow_pct) < -2:
            adjusted_score -= 6.0
        adjusted_scores.append(round(adjusted_score, 2))
        trap_reasons.append("；".join(reasons) if reasons else "低估值与财报匹配度较好")

        core_candidate = False
        if not reasons and industry in VALUE_CORE_INDUSTRIES:
            mv_ok = pd.notna(total_mv) and float(total_mv) >= 2000000
            health_ok = pd.notna(health_score) and float(health_score) >= 0.45
            quality_ok = pd.notna(quality_score) and float(quality_score) >= 50
            rs_ok = pd.isna(rs20) or float(rs20) >= 0
            ret_ok = pd.isna(ret20) or float(ret20) >= 0
            if industry in {"中成药", "化学制药", "生物制药", "医疗保健", "医药商业"}:
                core_candidate = (
                    mv_ok
                    and pd.notna(total_mv)
                    and float(total_mv) >= 3000000
                    and pd.notna(health_score)
                    and float(health_score) >= 0.55
                    and pd.notna(quality_score)
                    and float(quality_score) >= 70
                    and rs_ok
                    and ret_ok
                    and adjusted_score >= 88
                )
            elif industry in {"供气供热", "火力发电", "新型电力", "银行"}:
                core_candidate = (
                    mv_ok
                    and health_ok
                    and quality_ok
                    and pd.notna(dividend_stability)
                    and float(dividend_stability) >= 0.8
                    and pd.notna(row.get("value_dv_ttm", pd.NA))
                    and float(row.get("value_dv_ttm", 0)) >= 2.5
                    and (pd.isna(rs20) or float(rs20) >= -1)
                    and adjusted_score >= 82
                )
            elif industry in {"家用电器", "食品", "乳制品"}:
                core_candidate = (
                    mv_ok
                    and pd.notna(total_mv)
                    and float(total_mv) >= 5000000
                    and pd.notna(health_score)
                    and float(health_score) >= 0.5
                    and pd.notna(quality_score)
                    and float(quality_score) >= 60
                    and rs_ok
                    and ret_ok
                    and adjusted_score >= 90
                )

        if reasons:
            statuses.append("剔除")
        elif core_candidate:
            statuses.append("核心池")
        elif adjusted_score >= 72:
            statuses.append("重点配置")
        elif adjusted_score >= 58:
            statuses.append("跟踪")
        else:
            statuses.append("观察")

    result_df["value_long_hold_score"] = adjusted_scores
    result_df["screen_reason"] = trap_reasons
    result_df["status"] = statuses
    result_df = result_df[result_df["status"] != "剔除"].copy()
    return result_df


def screen_value_stocks(days=720, scope="sample", top=20, limit=0, force_refresh=False):
    start_date, end_date = get_date_range(days)
    prepared_frames = prepare_value_stock_frames(
        start_date,
        end_date,
        scope=scope,
        limit=limit,
        force_refresh=force_refresh,
    )

    snapshots = []
    for stock_code, stock_df in prepared_frames.items():
        if stock_df is None or stock_df.empty:
            continue
        snapshot = _build_value_snapshot(stock_code, stock_df)
        if snapshot:
            snapshots.append(snapshot)

    if not snapshots:
        return pd.DataFrame()

    result_df = pd.DataFrame(snapshots)
    name_map = load_stock_name_map()
    industry_map = load_stock_industry_map()
    result_df["name"] = result_df["stock_code"].map(name_map).fillna(result_df["stock_code"])
    result_df["industry"] = result_df["stock_code"].map(industry_map)
    result_df = _apply_value_long_hold_filter(result_df)
    if result_df.empty:
        return result_df
    result_df = result_df.sort_values(
        by=["value_long_hold_score", "value_pick_score", "financial_health_score", "peer_financial_score"],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)
    return result_df.head(top)


def load_screened_value_stock_pool(days=720, scope="sample", top=12, limit=0, force_refresh=False):
    screening_limit = limit
    if scope == "all":
        screening_limit = max(limit or 0, 120)
    screened_df = screen_value_stocks(
        days=days,
        scope=scope,
        top=max(top, 30),
        limit=screening_limit,
        force_refresh=force_refresh,
    )
    if screened_df.empty:
        return load_value_stock_pool(scope=scope, limit=limit)
    core_df = screened_df[screened_df["status"] == "核心池"].copy()
    if not core_df.empty:
        return core_df["stock_code"].dropna().astype(str).head(top).tolist()

    filtered_df = screened_df[screened_df["status"].isin(["重点配置", "跟踪"])].copy()
    if filtered_df.empty:
        filtered_df = screened_df.head(min(top, 6)).copy()
    return filtered_df["stock_code"].dropna().astype(str).tolist()


def load_screened_dividend_stock_pool(days=720, scope="all", top=12, limit=0, force_refresh=False):
    screening_limit = limit
    if scope == "all":
        screening_limit = max(limit or 0, 120)
    screened_df = screen_value_stocks(
        days=days,
        scope=scope,
        top=max(top, 40),
        limit=screening_limit,
        force_refresh=force_refresh,
    )
    if screened_df.empty:
        return load_value_stock_pool(scope="sample", limit=top)

    dividend_df = screened_df.copy()
    if "value_dv_ttm" in dividend_df.columns:
        dividend_df = dividend_df[pd.to_numeric(dividend_df["value_dv_ttm"], errors="coerce") >= 3.0]
    if "value_dividend_stability" in dividend_df.columns:
        dividend_df = dividend_df[
            pd.to_numeric(dividend_df["value_dividend_stability"], errors="coerce").fillna(0) >= 0.8
        ]
    if "financial_health_score" in dividend_df.columns:
        dividend_df = dividend_df[
            pd.to_numeric(dividend_df["financial_health_score"], errors="coerce").fillna(0) >= 0.45
        ]
    if "status" in dividend_df.columns:
        dividend_df = dividend_df[dividend_df["status"].isin(["核心池", "重点配置", "跟踪"])]

    if dividend_df.empty:
        dividend_df = screened_df.head(min(top, 8)).copy()

    sort_cols = [col for col in ["value_dv_ttm", "value_dividend_stability", "value_long_hold_score"] if col in dividend_df.columns]
    if sort_cols:
        dividend_df = dividend_df.sort_values(by=sort_cols, ascending=[False] * len(sort_cols))
    return dividend_df["stock_code"].dropna().astype(str).head(top).tolist()


def screen_bank_dividend_core_stocks(days=720, area_scope="developed", top=12, force_refresh=False):
    start_date, end_date = get_date_range(days)
    prepared_frames = prepare_bank_stock_frames(
        start_date,
        end_date,
        area_scope=area_scope,
        force_refresh=force_refresh,
    )

    snapshots = []
    for stock_code, stock_df in prepared_frames.items():
        if stock_df is None or stock_df.empty:
            continue
        snapshot = _build_bank_dividend_snapshot(stock_code, stock_df)
        if snapshot:
            snapshots.append(snapshot)

    if not snapshots:
        return pd.DataFrame()

    result_df = pd.DataFrame(snapshots)
    name_map = load_stock_name_map()
    result_df["name"] = result_df["stock_code"].map(name_map).fillna(result_df["stock_code"])
    result_df = result_df[result_df["status"] != "剔除"].copy()
    if result_df.empty:
        return result_df

    result_df = result_df.sort_values(
        by=[
            "bank_dividend_score",
            "bank_dv_ttm",
            "bank_dividend_stability",
            "financial_health_score",
            "peer_financial_score",
        ],
        ascending=[False, False, False, False, False],
    ).reset_index(drop=True)
    return result_df.head(top)


def load_bank_dividend_core_pool(days=720, area_scope="developed", top=8, force_refresh=False):
    screened_df = screen_bank_dividend_core_stocks(
        days=days,
        area_scope=area_scope,
        top=max(top, 16),
        force_refresh=force_refresh,
    )
    if screened_df.empty:
        return load_bank_segment_pools(area_scope=area_scope)["state_owned"]

    core_df = screened_df[screened_df["status"] == "核心红利"].copy()
    if len(core_df) >= min(top, 4):
        return core_df["stock_code"].dropna().astype(str).head(top).tolist()

    focus_df = screened_df[screened_df["status"].isin(["重点红利", "观察"])].copy()
    supplement_df = pd.concat([core_df, focus_df], ignore_index=True)
    if supplement_df.empty:
        supplement_df = screened_df.head(min(top, 6)).copy()
    return supplement_df["stock_code"].dropna().astype(str).drop_duplicates().head(top).tolist()


def screen_core_long_term_pool(
    days=720,
    value_scope="all",
    value_limit=80,
    bank_scope="developed",
    top=20,
    force_refresh=False,
):
    frames = []

    value_df = screen_value_stocks(
        days=days,
        scope=value_scope,
        top=max(top * 4, 40),
        limit=max(value_limit, top * 4),
        force_refresh=force_refresh,
    )
    if not value_df.empty:
        value_candidates = value_df[value_df["status"].isin(["核心池", "重点配置", "跟踪"])].copy()
        if not value_candidates.empty:
            value_candidates["source"] = "value"
            value_candidates["core_long_term_score"] = (
                pd.to_numeric(value_candidates.get("value_long_hold_score"), errors="coerce").fillna(0) * 0.7
                + pd.to_numeric(value_candidates.get("financial_health_score"), errors="coerce").fillna(0) * 20
                + pd.to_numeric(value_candidates.get("peer_financial_score"), errors="coerce").fillna(0) * 10
            )
            value_candidates["bucket"] = value_candidates["status"].map(
                {"核心池": "价值核心", "重点配置": "价值观察", "跟踪": "价值观察"}
            ).fillna("价值观察")
            frames.append(
                value_candidates[
                    [
                        "stock_code",
                        "name",
                        "industry",
                        "source",
                        "bucket",
                        "status",
                        "core_long_term_score",
                        "financial_health_score",
                        "peer_financial_score",
                        "financial_quality_score",
                        "financial_roe",
                        "financial_profit_yoy",
                        "financial_revenue_yoy",
                        "value_pb_percentile_3y",
                        "value_pe_percentile_3y",
                        "value_dv_ttm",
                        "value_dividend_stability",
                        "value_total_mv",
                        "value_ret_20",
                        "value_rs_20",
                        "value_close_to_ma60_pct",
                        "value_close_to_low20_pct",
                        "value_close_to_high60_pct",
                        "screen_reason",
                    ]
                ].copy()
            )

        dividend_candidates = value_df.copy()
        if "value_dv_ttm" in dividend_candidates.columns:
            dividend_candidates = dividend_candidates[
                pd.to_numeric(dividend_candidates["value_dv_ttm"], errors="coerce").fillna(0) >= 3.0
            ]
        if "value_dividend_stability" in dividend_candidates.columns:
            dividend_candidates = dividend_candidates[
                pd.to_numeric(dividend_candidates["value_dividend_stability"], errors="coerce").fillna(0) >= 0.8
            ]
        if "status" in dividend_candidates.columns:
            dividend_candidates = dividend_candidates[
                dividend_candidates["status"].isin(["核心池", "重点配置", "跟踪"])
            ]
        if not dividend_candidates.empty:
            dividend_candidates = dividend_candidates.copy()
            dividend_candidates["source"] = "dividend"
            dividend_candidates["core_long_term_score"] = (
                pd.to_numeric(dividend_candidates.get("value_long_hold_score"), errors="coerce").fillna(0) * 0.55
                + pd.to_numeric(dividend_candidates.get("value_dv_ttm"), errors="coerce").fillna(0) * 4
                + pd.to_numeric(dividend_candidates.get("value_dividend_stability"), errors="coerce").fillna(0) * 18
                + pd.to_numeric(dividend_candidates.get("financial_health_score"), errors="coerce").fillna(0) * 18
            )
            dividend_candidates["bucket"] = "红利核心"
            frames.append(
                dividend_candidates[
                    [
                        "stock_code",
                        "name",
                        "industry",
                        "source",
                        "bucket",
                        "status",
                        "core_long_term_score",
                        "financial_health_score",
                        "peer_financial_score",
                        "financial_quality_score",
                        "financial_roe",
                        "financial_profit_yoy",
                        "financial_revenue_yoy",
                        "value_pb_percentile_3y",
                        "value_pe_percentile_3y",
                        "value_dv_ttm",
                        "value_dividend_stability",
                        "value_total_mv",
                        "value_ret_20",
                        "value_rs_20",
                        "value_close_to_ma60_pct",
                        "value_close_to_low20_pct",
                        "value_close_to_high60_pct",
                        "screen_reason",
                    ]
                ].copy()
            )

    bank_df = screen_bank_dividend_core_stocks(
        days=days,
        area_scope=bank_scope,
        top=max(top * 2, 16),
        force_refresh=force_refresh,
    )
    if not bank_df.empty:
        bank_candidates = bank_df[bank_df["status"].isin(["核心红利", "重点红利", "观察"])].copy()
        bank_candidates["industry"] = "银行"
        bank_candidates["source"] = "bank_dividend"
        bank_candidates["core_long_term_score"] = (
            pd.to_numeric(bank_candidates.get("bank_dividend_score"), errors="coerce").fillna(0) * 0.8
            + pd.to_numeric(bank_candidates.get("financial_health_score"), errors="coerce").fillna(0) * 20
            + pd.to_numeric(bank_candidates.get("peer_financial_score"), errors="coerce").fillna(0) * 12
        )
        bank_candidates["bucket"] = bank_candidates["status"].map(
            {"核心红利": "银行红利核心", "重点红利": "银行红利观察", "观察": "银行红利观察"}
        ).fillna("银行红利观察")
        bank_candidates["value_pe_percentile_3y"] = pd.NA
        bank_candidates["value_dividend_stability"] = bank_candidates.get("bank_dividend_stability", pd.NA)
        frames.append(
            bank_candidates[
                [
                    "stock_code",
                    "name",
                    "industry",
                    "source",
                    "bucket",
                    "status",
                    "core_long_term_score",
                    "financial_health_score",
                    "peer_financial_score",
                    "financial_quality_score",
                    "financial_roe",
                    "financial_profit_yoy",
                    "financial_revenue_yoy",
                    "bank_pb_percentile_3y",
                    "value_pe_percentile_3y",
                    "bank_dv_ttm",
                    "value_dividend_stability",
                    "value_total_mv",
                    "bank_ret_20",
                    "bank_rs_20",
                    "bank_close_to_ma60_pct",
                    "bank_close_to_low20_pct",
                    "screen_reason",
                ]
            ]
            .rename(
                columns={
                    "bank_pb_percentile_3y": "value_pb_percentile_3y",
                    "bank_dv_ttm": "value_dv_ttm",
                    "bank_ret_20": "value_ret_20",
                    "bank_rs_20": "value_rs_20",
                    "bank_close_to_ma60_pct": "value_close_to_ma60_pct",
                    "bank_close_to_low20_pct": "value_close_to_low20_pct",
                }
            )
            .copy()
        )

    frames = [frame for frame in frames if frame is not None and not frame.empty]
    if not frames:
        return pd.DataFrame()

    combined_records = []
    for frame in frames:
        combined_records.extend(frame.to_dict("records"))
    combined_df = pd.DataFrame(combined_records)
    combined_df["source"] = combined_df["source"].fillna("").astype(str)
    combined_df["bucket"] = combined_df["bucket"].fillna("").astype(str)

    grouped_rows = []
    for stock_code, group in combined_df.groupby("stock_code", dropna=True):
        group = group.sort_values(by="core_long_term_score", ascending=False).reset_index(drop=True)
        best = group.iloc[0].copy()
        source_tags = " / ".join(group["source"].dropna().astype(str).drop_duplicates().tolist())
        bucket_tags = " / ".join(group["bucket"].dropna().astype(str).drop_duplicates().tolist())
        if "银行红利核心" in bucket_tags or ("红利核心" in bucket_tags and "价值核心" in bucket_tags):
            final_status = "核心长期持有"
        elif "价值核心" in bucket_tags or "红利核心" in bucket_tags:
            final_status = "重点长期持有"
        elif float(best.get("core_long_term_score", 0) or 0) >= 72:
            final_status = "重点长期持有"
        else:
            final_status = "长期观察"

        best["source_tags"] = source_tags
        best["bucket_tags"] = bucket_tags
        best["final_status"] = final_status

        pb_pct = pd.to_numeric(pd.Series([best.get("value_pb_percentile_3y", pd.NA)]), errors="coerce").iloc[0]
        health_score = pd.to_numeric(pd.Series([best.get("financial_health_score", pd.NA)]), errors="coerce").iloc[0]
        ret20 = pd.to_numeric(pd.Series([best.get("value_ret_20", pd.NA)]), errors="coerce").iloc[0]
        rs20 = pd.to_numeric(pd.Series([best.get("value_rs_20", pd.NA)]), errors="coerce").iloc[0]
        close_to_ma60 = pd.to_numeric(pd.Series([best.get("value_close_to_ma60_pct", pd.NA)]), errors="coerce").iloc[0]
        close_to_low20 = pd.to_numeric(pd.Series([best.get("value_close_to_low20_pct", pd.NA)]), errors="coerce").iloc[0]
        close_to_high60 = pd.to_numeric(pd.Series([best.get("value_close_to_high60_pct", pd.NA)]), errors="coerce").iloc[0]

        buy_timing_score = 0.0
        if pd.notna(pb_pct):
            buy_timing_score += _theme_score_clip(pb_pct, 20, 90) * 40
        if pd.notna(health_score):
            buy_timing_score += _theme_score_clip(health_score, 0.45, 0.85) * 20
        if pd.notna(ret20):
            if -8 <= float(ret20) <= 8:
                buy_timing_score += 15
            elif float(ret20) > 8:
                buy_timing_score += max(0.0, 15 - (float(ret20) - 8) * 1.2)
            else:
                buy_timing_score += max(0.0, 12 - abs(float(ret20) + 8) * 1.2)
        if pd.notna(rs20):
            if -5 <= float(rs20) <= 5:
                buy_timing_score += 10
            elif float(rs20) > 5:
                buy_timing_score += max(0.0, 10 - (float(rs20) - 5))
            else:
                buy_timing_score += max(0.0, 8 - abs(float(rs20) + 5))
        if pd.notna(close_to_ma60):
            if -3 <= float(close_to_ma60) <= 5:
                buy_timing_score += 10
            elif float(close_to_ma60) > 5:
                buy_timing_score += max(0.0, 10 - (float(close_to_ma60) - 5))
            else:
                buy_timing_score += max(0.0, 8 - abs(float(close_to_ma60) + 3))
        if pd.notna(close_to_low20):
            if 2 <= float(close_to_low20) <= 12:
                buy_timing_score += 5
        if pd.notna(close_to_high60) and float(close_to_high60) > -5:
            buy_timing_score -= 8

        if buy_timing_score >= 72:
            buy_hint = "低点可买"
        elif buy_timing_score >= 58:
            buy_hint = "接近低点"
        elif pd.notna(ret20) and float(ret20) > 12:
            buy_hint = "等待回踩"
        else:
            buy_hint = "先观察"

        best["buy_timing_score"] = round(buy_timing_score, 2)
        best["buy_hint"] = buy_hint
        if final_status == "核心长期持有":
            if buy_hint == "低点可买":
                suggested_position = "30%-50%"
            elif buy_hint == "接近低点":
                suggested_position = "20%-30%"
            else:
                suggested_position = "10%-20%"
        elif final_status == "重点长期持有":
            if buy_hint == "低点可买":
                suggested_position = "15%-25%"
            elif buy_hint == "接近低点":
                suggested_position = "10%-15%"
            else:
                suggested_position = "0%-10%"
        else:
            suggested_position = "0%-10%" if buy_hint in {"低点可买", "接近低点"} else "0%"
        best["suggested_position"] = suggested_position
        grouped_rows.append(best)

    result_df = pd.DataFrame(grouped_rows)
    result_df = result_df.sort_values(
        by=[
            "final_status",
            "core_long_term_score",
            "financial_health_score",
            "peer_financial_score",
        ],
        ascending=[True, False, False, False],
    ).reset_index(drop=True)

    status_order = {
        "核心长期持有": 0,
        "重点长期持有": 1,
        "长期观察": 2,
    }
    result_df["status_rank"] = result_df["final_status"].map(status_order).fillna(9)
    result_df = result_df.sort_values(
        by=["status_rank", "core_long_term_score", "financial_health_score", "peer_financial_score"],
        ascending=[True, False, False, False],
    ).drop(columns=["status_rank"]).reset_index(drop=True)
    return result_df.head(top)


def _summarize_backtest(trade_df, equity_df, transactions, initial_cash):
    final_value = equity_df["equity"].iloc[-1]
    total_return = ((final_value - initial_cash) / initial_cash) * 100

    days = (
        pd.to_datetime(trade_df["trade_date"].iloc[-1])
        - pd.to_datetime(trade_df["trade_date"].iloc[0])
    ).days
    if days <= 0 or final_value <= 0:
        annual_return = 0.0
    else:
        annual_return = (final_value / initial_cash) ** (365 / days) - 1

    daily_returns = equity_df["daily_return"]
    daily_std = daily_returns.std()
    sharpe = 0.0 if pd.isna(daily_std) or daily_std == 0 else math.sqrt(252) * daily_returns.mean() / daily_std

    realized_transactions = [
        t for t in transactions if t["action"] in {"sell", "trim"}
    ]
    total_trades = len(realized_transactions)
    profitable_trades = len([t for t in realized_transactions if t.get("profit", 0) > 0])
    loss_trades = len([t for t in realized_transactions if t.get("profit", 0) < 0])
    win_rate = 0.0 if total_trades == 0 else profitable_trades / total_trades

    if realized_transactions:
        max_return = max(t["return_rate"] for t in realized_transactions)
        min_return = min(t["return_rate"] for t in realized_transactions)
        avg_return = sum(t["return_rate"] for t in realized_transactions) / len(realized_transactions)
        total_profit = sum(t.get("profit", 0) for t in realized_transactions)
    else:
        max_return = 0.0
        min_return = 0.0
        avg_return = 0.0
        total_profit = 0.0

    benchmark_return = (
        (trade_df["close"].iloc[-1] - trade_df["close"].iloc[0])
        / trade_df["close"].iloc[0]
        * 100
    )
    max_drawdown = equity_df["drawdown"].min() * 100

    trading_stats = {
        "total_trades": total_trades,
        "profitable_trades": profitable_trades,
        "loss_trades": loss_trades,
        "win_rate": win_rate,
        "max_return": max_return,
        "min_return": min_return,
        "avg_return": avg_return,
        "total_profit": total_profit,
        "max_drawdown": max_drawdown,
        "sharpe": sharpe,
        "benchmark_return": benchmark_return,
        "excess_return": total_return - benchmark_return,
    }

    return final_value, transactions, total_return, annual_return, trading_stats


def _build_equity_curve(equity_curve):
    equity_df = pd.DataFrame(equity_curve)
    equity_df["equity_peak"] = equity_df["equity"].cummax()
    equity_df["drawdown"] = (
        equity_df["equity"] - equity_df["equity_peak"]
    ) / equity_df["equity_peak"]
    equity_df["daily_return"] = equity_df["equity"].pct_change().fillna(0)
    return equity_df


def _backtest_with_target_position(
    trade_df,
    initial_cash,
    slippage,
    commission_rate,
    sell_tax_rate,
):
    cash = float(initial_cash)
    shares = 0.0
    avg_cost_per_share = 0.0
    transactions = []
    equity_curve = []

    for _, row in trade_df.iterrows():
        open_price = float(row["open"])
        close_price = float(row["close"])
        reason = row["reason"]
        target_position = row.get("target_position", pd.NA)
        if pd.isna(target_position):
            target_position = shares * open_price / (cash + shares * open_price) if (cash + shares * open_price) > 0 else 0.0
        target_position = max(0.0, min(1.0, float(target_position)))

        equity_at_open = cash + shares * open_price
        target_value = equity_at_open * target_position
        current_value = shares * open_price
        delta_value = target_value - current_value

        if delta_value > 1e-8 and cash > 0:
            execution_price = open_price * (1 + slippage)
            cost_per_share = execution_price * (1 + commission_rate)
            desired_shares = delta_value / cost_per_share
            affordable_shares = cash / cost_per_share
            shares_to_buy = min(desired_shares, affordable_shares)

            if shares_to_buy > 1e-8:
                buy_amount = shares_to_buy * execution_price
                buy_fee = buy_amount * commission_rate
                total_cost = buy_amount + buy_fee
                previous_shares = shares
                previous_cost = avg_cost_per_share * previous_shares
                cash -= total_cost
                shares += shares_to_buy
                avg_cost_per_share = 0.0 if shares <= 0 else (previous_cost + total_cost) / shares
                transactions.append(
                    {
                        "date": row["trade_date"],
                        "action": "buy" if previous_shares <= 1e-8 else "add",
                        "price": execution_price,
                        "amount": shares_to_buy,
                        "cost": total_cost,
                        "reason": reason,
                        "return_rate": 0.0,
                        "target_position": target_position,
                    }
                )

        elif delta_value < -1e-8 and shares > 0:
            execution_price = open_price * (1 - slippage)
            desired_sell_shares = min(shares, abs(delta_value) / open_price)
            if desired_sell_shares > 1e-8:
                sell_amount = desired_sell_shares * execution_price
                sell_fee = sell_amount * commission_rate
                sell_tax = sell_amount * sell_tax_rate
                proceeds = sell_amount - sell_fee - sell_tax
                cost_basis = avg_cost_per_share * desired_sell_shares
                profit = proceeds - cost_basis
                return_rate = 0.0 if cost_basis == 0 else ((proceeds - cost_basis) / cost_basis) * 100
                cash += proceeds
                shares -= desired_sell_shares
                if shares <= 1e-8:
                    shares = 0.0
                    avg_cost_per_share = 0.0
                transactions.append(
                    {
                        "date": row["trade_date"],
                        "action": "sell" if shares <= 1e-8 or target_position <= 1e-8 else "trim",
                        "price": execution_price,
                        "amount": desired_sell_shares,
                        "profit": profit,
                        "reason": reason,
                        "return_rate": return_rate,
                        "target_position": target_position,
                    }
                )

        equity_curve.append(
            {
                "trade_date": row["trade_date"],
                "equity": cash + shares * close_price,
            }
        )

    equity_df = _build_equity_curve(equity_curve)
    return _summarize_backtest(trade_df, equity_df, transactions, initial_cash)


def backtest(
    df,
    initial_cash=100000,
    slippage=0.001,
    commission_rate=0.0003,
    sell_tax_rate=0.001,
):
    """
    更接近真实交易的回测：
    - 买卖都考虑滑点和手续费
    - 卖出额外考虑印花税
    - 使用完整权益曲线计算最大回撤和夏普
    """
    trade_df = _prepare_backtest_frame(df)
    if len(trade_df) < 2:
        raise ValueError("回测数据不足，至少需要两个交易日")

    has_target_position = (
        "target_position" in trade_df.columns
        and trade_df["target_position"].notna().any()
    )
    if has_target_position:
        return _backtest_with_target_position(
            trade_df,
            initial_cash=initial_cash,
            slippage=slippage,
            commission_rate=commission_rate,
            sell_tax_rate=sell_tax_rate,
        )

    cash = float(initial_cash)
    shares = 0.0
    last_buy_price = 0.0
    last_buy_cost = 0.0
    transactions = []
    equity_curve = []

    for i in range(len(trade_df)):
        row = trade_df.iloc[i]
        signal = row["signal"]
        open_price = row["open"]
        close_price = row["close"]
        reason = row["reason"]

        if signal == 1 and cash > 0:
            execution_price = open_price * (1 + slippage)
            cost_per_share = execution_price * (1 + commission_rate)
            shares = cash / cost_per_share
            buy_amount = shares * execution_price
            buy_fee = buy_amount * commission_rate
            cash -= buy_amount + buy_fee
            last_buy_price = execution_price
            last_buy_cost = buy_amount + buy_fee
            transactions.append(
                {
                    "date": row["trade_date"],
                    "action": "buy",
                    "price": execution_price,
                    "amount": shares,
                    "cost": buy_amount + buy_fee,
                    "reason": reason,
                    "return_rate": 0,
                }
            )

        elif signal == -1 and shares > 0:
            execution_price = open_price * (1 - slippage)
            sell_amount = shares * execution_price
            sell_fee = sell_amount * commission_rate
            sell_tax = sell_amount * sell_tax_rate
            proceeds = sell_amount - sell_fee - sell_tax
            profit = proceeds - last_buy_cost
            return_rate = 0.0 if last_buy_cost == 0 else ((proceeds - last_buy_cost) / last_buy_cost) * 100
            cash += proceeds
            transactions.append(
                {
                    "date": row["trade_date"],
                    "action": "sell",
                    "price": execution_price,
                    "amount": shares,
                    "profit": profit,
                    "reason": reason,
                    "return_rate": return_rate,
                }
            )
            shares = 0.0
            last_buy_price = 0.0
            last_buy_cost = 0.0

        equity_curve.append(
            {
                "trade_date": row["trade_date"],
                "equity": cash + shares * close_price,
            }
        )

    equity_df = _build_equity_curve(equity_curve)
    return _summarize_backtest(trade_df, equity_df, transactions, initial_cash)


def analyze_stock_pool(stock_pool, start_date, end_date, strategy_instance, verbose=True, force_refresh=False):
    results = []
    prepared_frames = {}
    for stock_code in stock_pool:
        try:
            stock_data = fetch_stock_data(stock_code, start_date, end_date, force_refresh=force_refresh)
            prepared_frames[stock_code] = enrich_with_market_context(
                stock_data,
                start_date,
                end_date,
                force_refresh=force_refresh,
                stock_code=stock_code,
            )
        except Exception as exc:
            print(f"处理股票 {stock_code} 时出错: {str(exc)}")

    if isinstance(strategy_instance, cycle_with_industry_rotation_strategy):
        prepared_frames = add_cycle_leader_context(prepared_frames)
    if isinstance(strategy_instance, theme_with_sentiment_strategy):
        prepared_frames = add_theme_leader_context(prepared_frames)
    prepared_frames = add_financial_peer_context(prepared_frames)

    for stock_code in stock_pool:
        stock_data = prepared_frames.get(stock_code)
        if stock_data is None or stock_data.empty:
            continue
        try:
            trading_signal = strategy_instance.trading_strategy(stock_data.copy())
            trading_signal = apply_unified_financial_gate(
                trading_signal,
                stock_code,
                strategy_instance=strategy_instance,
            )
            final_value, transactions, total_return, annual_return, trading_stats = backtest(trading_signal)
            prediction = strategy_instance.predict_next_signal(trading_signal)
            prediction = apply_unified_financial_gate_to_prediction(
                prediction,
                trading_signal,
                stock_code,
                strategy_instance=strategy_instance,
            )
            if verbose:
                print_transactions(transactions)

            losses = trading_stats["loss_trades"]
            if losses == 0:
                ratio = float("inf")
            else:
                ratio = trading_stats["profitable_trades"] / losses

            results.append(
                {
                    "stock_code": stock_code,
                    "strategy_name": getattr(strategy_instance, "display_name", strategy_instance.__class__.__name__),
                    "total_return": total_return,
                    "annual_return": annual_return,
                    "stats": trading_stats,
                    "prediction": prediction,
                    "final_value": final_value,
                    "profit_loss_ratio": ratio,
                    "cycle_chain": resolve_cycle_chain(stock_code)
                    if isinstance(strategy_instance, cycle_with_industry_rotation_strategy)
                    else None,
                }
            )
        except Exception as exc:
            print(f"处理股票 {stock_code} 时出错: {str(exc)}")
    return results


def get_date_range(days=365):
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    return start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")


def build_strategy_jobs(
    mode,
    bank_scope="developed",
    cycle_scope="leaders",
    theme_scope="leaders",
    theme_top=10,
    value_scope="sample",
    value_top=12,
    value_limit=0,
    days=365,
    force_refresh=False,
):
    if mode == "legacy":
        return [
            (["600919.SH", "000001.SZ"], macd_with_optimize_sell_strategy(5, 0.01)),
            (["00981.HK"], macd_with_optimize_sell_strategy(5, 0.03)),
            (["600345.SH"], stragegy_for_600345()),
            (["515650.SH"], macd_with_optimize_sell_strategy(5, 0.01)),
            (["300762.SZ"], macd_with_optimize_sell_strategy(5, 0.08)),
            (["00175.HK"], macd_with_optimize_sell_strategy(5, 0.08)),
            (["600161.SH", "002270.SZ"], macd_with_deepdown()),
            (["09988.HK"], macd_with_optimize_sell_strategy(5, 0.06)),
            (["00700.HK"], macd_with_optimize_sell_strategy(5, 0.03)),
            (["03690.HK"], macd_with_optimize_sell_strategy(5, 0.08)),
            (["01810.HK"], macd_with_optimize_sell_strategy(5, 0.08)),
            (["603583.SH"], macd_with_optimize_sell_strategy(5, 0.02)),
        ]

    if mode == "regime":
        return [
            (
                list(DEFAULT_STOCK_NAMES.keys()),
                macd_with_regime_filter_strategy(),
            )
        ]

    if mode == "cycle":
        return [
            (
                load_cycle_stock_pool(scope=cycle_scope, chain_scope="all"),
                cycle_with_industry_rotation_strategy(
                    profile="balanced",
                    display_name="cycle_balanced",
                ),
            )
        ]

    if mode == "cycle_leader_hold":
        return [
            (
                load_cycle_stock_pool(scope=cycle_scope, chain_scope="nonferrous"),
                cycle_with_industry_rotation_strategy(
                    profile="leader_hold",
                    display_name="cycle_leader_hold",
                ),
            )
        ]

    if mode == "cycle_swing":
        return [
            (
                load_cycle_stock_pool(scope=cycle_scope, chain_scope="swing"),
                cycle_with_industry_rotation_strategy(
                    profile="swing",
                    display_name="cycle_swing",
                ),
            )
        ]

    if mode == "theme":
        if theme_scope == "screened":
            theme_pool = load_screened_theme_stock_pool(
                days=days,
                scope="all",
                top=theme_top,
                force_refresh=force_refresh,
            )
        else:
            theme_pool = load_theme_stock_pool(scope=theme_scope)
        return [
            (
                theme_pool,
                theme_with_sentiment_strategy(),
            )
        ]

    if mode == "value_quality":
        if value_scope == "screened":
            value_pool = load_screened_value_stock_pool(
                days=days,
                scope="all",
                top=value_top,
                limit=value_limit,
                force_refresh=force_refresh,
            )
        else:
            value_pool = load_value_stock_pool(scope=value_scope, limit=value_limit)
        return [
            (
                value_pool,
                value_quality_hold_strategy(
                    display_name="value_quality_hold",
                ),
            )
        ]

    if mode == "dividend_hold":
        dividend_pool = load_screened_dividend_stock_pool(
            days=days,
            scope="all",
            top=value_top,
            limit=value_limit,
            force_refresh=force_refresh,
        )
        return [
            (
                dividend_pool,
                dividend_hold_strategy(
                    display_name="dividend_hold",
                ),
            )
        ]

    if mode == "bank_dividend_core":
        bank_pool = load_bank_dividend_core_pool(
            days=days,
            area_scope=bank_scope,
            top=max(4, value_top),
            force_refresh=force_refresh,
        )
        return [
            (
                bank_pool,
                state_owned_dividend_strategy(
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
                ),
            ),
        ]

    if mode == "bank_t":
        bank_segments = load_bank_segment_pools(area_scope=bank_scope)
        return [
            (
                bank_segments["state_owned"],
                bank_t_strategy(
                    base_position=0.25,
                    macd_bull_base=0.45,
                    macd_neutral_base=0.35,
                    macd_bear_base=0.05,
                    add_step=0.1,
                    trim_step=0.1,
                    oversold_zscore=-1.0,
                    overbought_zscore=0.9,
                    oversold_rsi=42,
                    overbought_rsi=58,
                    min_dividend_yield=4.0,
                    pb_cheap_buy_floor=35,
                    max_position=0.8,
                    min_financial_health_score=0.6,
                    min_financial_quality_score=15,
                    min_profit_yoy=-15,
                    min_revenue_yoy=-8,
                    min_roe=7,
                    weak_profit_yoy_exit=-20,
                    weak_revenue_yoy_exit=-10,
                    weak_roe_exit=5,
                    display_name="bank_t_state_owned",
                ),
            ),
            (
                bank_segments["joint_stock"],
                bank_t_strategy(
                    base_position=0.25,
                    macd_bull_base=0.45,
                    macd_neutral_base=0.25,
                    macd_bear_base=0.1,
                    add_step=0.15,
                    trim_step=0.15,
                    oversold_zscore=-1.1,
                    overbought_zscore=1.1,
                    oversold_rsi=40,
                    overbought_rsi=60,
                    min_dividend_yield=3.8,
                    pb_cheap_buy_floor=45,
                    pb_expensive_trim_ceiling=25,
                    min_financial_health_score=0.45,
                    min_financial_quality_score=15,
                    min_profit_yoy=-20,
                    min_revenue_yoy=-10,
                    min_roe=5,
                    weak_profit_yoy_exit=-30,
                    weak_revenue_yoy_exit=-15,
                    weak_roe_exit=4,
                    display_name="bank_t_joint_stock",
                ),
            ),
            (
                bank_segments["regional"],
                bank_t_strategy(
                    base_position=0.2,
                    macd_bull_base=0.35,
                    macd_neutral_base=0.2,
                    macd_bear_base=0.05,
                    add_step=0.1,
                    trim_step=0.1,
                    oversold_zscore=-1.2,
                    overbought_zscore=1.2,
                    oversold_rsi=38,
                    overbought_rsi=62,
                    min_dividend_yield=3.5,
                    pb_cheap_buy_floor=50,
                    pb_expensive_trim_ceiling=25,
                    min_dividend_stability=0.6,
                    min_financial_health_score=0.45,
                    min_financial_quality_score=15,
                    min_profit_yoy=-18,
                    min_revenue_yoy=-10,
                    min_roe=5,
                    weak_profit_yoy_exit=-28,
                    weak_revenue_yoy_exit=-15,
                    weak_roe_exit=4,
                    display_name="bank_t_regional",
                ),
            ),
        ]

    if mode == "bank_t_state_owned_income":
        bank_segments = load_bank_segment_pools(area_scope=bank_scope)
        return [
            (
                bank_segments["state_owned"],
                bank_t_strategy(
                    base_position=0.4,
                    macd_bull_base=0.6,
                    macd_neutral_base=0.4,
                    macd_bear_base=0.15,
                    add_step=0.1,
                    trim_step=0.1,
                    oversold_zscore=-0.8,
                    overbought_zscore=0.9,
                    oversold_rsi=42,
                    overbought_rsi=58,
                    min_dividend_yield=4.5,
                    pb_cheap_buy_floor=45,
                    min_financial_health_score=0.6,
                    min_financial_quality_score=20,
                    min_profit_yoy=-10,
                    min_revenue_yoy=-5,
                    min_roe=8,
                    weak_profit_yoy_exit=-20,
                    weak_revenue_yoy_exit=-10,
                    weak_roe_exit=6,
                    display_name="bank_t_state_owned_income",
                ),
            ),
        ]

    if mode == "state_owned_dividend":
        bank_segments = load_bank_segment_pools(area_scope=bank_scope)
        return [
            (
                bank_segments["state_owned"],
                state_owned_dividend_strategy(
                    display_name="state_owned_dividend",
                ),
            ),
        ]

    raise ValueError(f"不支持的策略模式: {mode}")


def parse_args():
    parser = argparse.ArgumentParser(description="股票策略回测")
    parser.add_argument(
        "--mode",
        choices=[
            "legacy",
            "regime",
            "cycle",
            "cycle_leader_hold",
            "cycle_swing",
            "theme",
            "value_quality",
            "dividend_hold",
            "bank_dividend_core",
            "bank_t",
            "bank_t_state_owned_income",
            "state_owned_dividend",
        ],
        default="regime",
        help="回测模式：legacy 保持原策略；regime 使用趋势过滤后的新策略；cycle 使用周期股平衡版；cycle_leader_hold 使用有色龙头持有版；cycle_swing 使用黑色/化工波段版；theme 使用题材情绪策略；value_quality 使用低估值+财报健康的长期持有策略；dividend_hold 使用高股息稳定分红的低频持有策略；bank_dividend_core 使用银行高股息核心组合的超低频持有策略；bank_t 使用银行底仓做T策略；bank_t_state_owned_income 使用国有大行收益优先版；state_owned_dividend 使用国有大行长持红利版",
    )
    parser.add_argument("--days", type=int, default=365, help="回测天数")
    parser.add_argument("--email", action="store_true", help="是否发送邮件")
    parser.add_argument("--quiet", action="store_true", help="不打印逐笔交易")
    parser.add_argument(
        "--bank-scope",
        choices=["developed", "all"],
        default="developed",
        help="银行相关模式默认只看发达地区银行；如需全国银行可切到 all",
    )
    parser.add_argument(
        "--cycle-scope",
        choices=["leaders", "all"],
        default="leaders",
        help="cycle 模式下默认只看代表性周期龙头；如需扩到更多周期股可切到 all",
    )
    parser.add_argument(
        "--theme-scope",
        choices=["leaders", "all", "screened"],
        default="leaders",
        help="theme 模式下可选 leaders 核心名单、all 题材行业全池、screened 先用筛选器挑出当前观察池；screened 更适合当前选股，不适合严格无偏历史回测",
    )
    parser.add_argument(
        "--theme-top",
        type=int,
        default=10,
        help="theme-scope=screened 时保留前 N 只题材股",
    )
    parser.add_argument(
        "--value-scope",
        choices=["sample", "all", "screened"],
        default="sample",
        help="value_quality 模式下可选 sample 样本池、all 全市场、screened 先按低估值和财报健康度筛出观察池",
    )
    parser.add_argument(
        "--value-top",
        type=int,
        default=12,
        help="value-scope=screened 时保留前 N 只低估值健康股",
    )
    parser.add_argument(
        "--value-limit",
        type=int,
        default=0,
        help="value_quality 模式下限制股票池数量，0 表示不限制",
    )
    parser.add_argument("--refresh-cache", action="store_true", help="忽略本地缓存并重新拉取Tushare数据")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    stock_name_map = load_stock_name_map()
    start_date, end_date = get_date_range(args.days)
    print(f"回测区间: {start_date} 至 {end_date}")
    print(f"策略模式: {args.mode}")
    print("开始分析股票池...\n")

    results = []
    for stock_pool, strategy_instance in build_strategy_jobs(
        args.mode,
        bank_scope=args.bank_scope,
        cycle_scope=args.cycle_scope,
        theme_scope=args.theme_scope,
        theme_top=args.theme_top,
        value_scope=args.value_scope,
        value_top=args.value_top,
        value_limit=args.value_limit,
        days=args.days,
        force_refresh=args.refresh_cache,
    ):
        results.extend(
            analyze_stock_pool(
                stock_pool,
                start_date,
                end_date,
                strategy_instance,
                verbose=not args.quiet,
                force_refresh=args.refresh_cache,
            )
        )

    results.sort(key=lambda item: item["total_return"], reverse=True)

    if args.email and results:
        html_content = generate_html_table(results, DEFAULT_STOCK_NAMES)
        send_email(DEFAULT_RECIPIENTS, html_content)

    if args.mode.startswith("cycle") and results:
        chain_summary = summarize_cycle_results(results)
        if chain_summary:
            print("周期分链收益汇总:")
            for chain_key in ["nonferrous", "chemical", "black", "generic"]:
                summary = chain_summary.get(chain_key)
                if not summary or summary["count"] == 0:
                    continue
                count = summary["count"]
                chain_label = CYCLE_CHAIN_LABELS.get(chain_key, chain_key)
                print(
                    f"{chain_label}: 样本 {count} 只, "
                    f"平均总收益 {summary['total_return'] / count:.2f}%, "
                    f"平均超额 {summary['excess_return'] / count:.2f}%, "
                    f"平均最大回撤 {summary['max_drawdown'] / count:.2f}%, "
                    f"平均交易次数 {summary['trades'] / count:.2f}, "
                    f"正收益 {summary['positive']}/{count}, "
                    f"跑赢买入持有 {summary['beat']}/{count}"
                )
            print()

    for result in results:
        stock_code = result["stock_code"]
        stock_name = stock_name_map.get(stock_code, stock_code)
        print(f"\n========== {stock_code} {stock_name} ==========")
        print(f"策略: {result['strategy_name']}")
        if result.get("cycle_chain"):
            print(f"周期链条: {CYCLE_CHAIN_LABELS.get(result['cycle_chain'], result['cycle_chain'])}")
        pred = result["prediction"]
        print("\n明日操作建议:")
        print(f"建议操作: {pred['signal']}")
        print(f"原因: {pred['reason']}")
        if "target_position" in pred:
            print(f"目标仓位: {pred['target_position']:.0%}")
        if all(key in pred and pd.notna(pred[key]) for key in ["dif", "dea", "macd"]):
            print(f"MACD指标 - DIF: {pred['dif']:.3f}, DEA: {pred['dea']:.3f}, MACD: {pred['macd']:.3f}")
        if "value_score" in pred and pd.notna(pred["value_score"]):
            print(f"低估值综合分: {float(pred['value_score']):.2f}")
        if "overvalue_score" in pred and pd.notna(pred["overvalue_score"]):
            print(f"高估值风险分: {float(pred['overvalue_score']):.2f}")
        if "dividend_score" in pred and pd.notna(pred["dividend_score"]):
            print(f"红利综合分: {float(pred['dividend_score']):.2f}")
        print("=" * 50)
        print(f"总收益率: {result['total_return']:.2f}%")
        print(f"年化收益率: {result['annual_return'] * 100:.2f}%")
        print(f"基准收益率: {result['stats']['benchmark_return']:.2f}%")
        print(f"超额收益: {result['stats']['excess_return']:.2f}%")
        print(f"最终资金: {result['final_value']:.2f}")

        stats = result["stats"]
        print("\n交易统计:")
        print(f"总交易次数: {stats['total_trades']}")
        print(f"盈利/亏损: {stats['profitable_trades']}/{stats['loss_trades']}")
        print(f"胜率: {stats['win_rate']:.2%}")
        print(f"盈亏比: {result['profit_loss_ratio']:.2f}")
        print(f"最大单笔收益: {stats['max_return']:.2f}%")
        print(f"最大单笔回撤: {stats['min_return']:.2f}%")
        print(f"平均收益率: {stats['avg_return']:.2f}%")
        print(f"最大回撤: {stats['max_drawdown']:.2f}%")
        print(f"Sharpe: {stats['sharpe']:.2f}")
