import argparse
from datetime import datetime
from pathlib import Path

import akshare as ak
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent
LOCAL_STOCK_INFO = PROJECT_ROOT / "basic" / "all_stocks_info.csv"
FUNDAMENTAL_CACHE_DIR = PROJECT_ROOT / ".cache" / "fundamentals" / "akshare"
FUNDAMENTAL_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def load_stock_universe(industry=None):
    if not LOCAL_STOCK_INFO.exists():
        raise FileNotFoundError("缺少 basic/all_stocks_info.csv，无法做同行业对比")
    df = pd.read_csv(LOCAL_STOCK_INFO, dtype={"symbol": str})
    if industry:
        df = df[df["industry"] == industry].copy()
    return df.reset_index(drop=True)


def ts_code_to_plain(ts_code):
    return ts_code.split(".")[0]


def ts_code_to_akshare_symbol(ts_code):
    plain = ts_code_to_plain(ts_code)
    return ("SH" if ts_code.endswith(".SH") else "SZ") + plain


def to_numeric(value):
    if value in (None, "", "--"):
        return None
    try:
        return float(str(value).replace(",", ""))
    except Exception:
        return None


def safe_pct(current, previous):
    if current is None or previous in (None, 0):
        return None
    return (current - previous) / abs(previous) * 100


class AkshareCache:
    def __init__(self):
        self.root = FUNDAMENTAL_CACHE_DIR

    def _path(self, kind, key):
        return self.root / f"{kind}_{key}.pkl"

    def load_or_fetch(self, kind, key, fetcher, force_refresh=False):
        path = self._path(kind, key)
        if path.exists() and not force_refresh:
            return pd.read_pickle(path)
        df = fetcher()
        if df is None:
            return pd.DataFrame()
        df.to_pickle(path)
        return df

    def fetch_stock_abstract(self, ts_code, force_refresh=False):
        plain = ts_code_to_plain(ts_code)
        return self.load_or_fetch(
            "abstract",
            plain,
            lambda: ak.stock_financial_abstract(symbol=plain),
            force_refresh=force_refresh,
        )

    def fetch_stock_report(self, ts_code, report_name, force_refresh=False):
        plain = ts_code_to_plain(ts_code)
        return self.load_or_fetch(
            f"report_{report_name}",
            plain,
            lambda: ak.stock_financial_report_sina(stock=plain, symbol=report_name),
            force_refresh=force_refresh,
        )

    def fetch_growth_comparison(self, ts_code, force_refresh=False):
        symbol = ts_code_to_akshare_symbol(ts_code)
        return self.load_or_fetch(
            "growth_cmp",
            symbol,
            lambda: ak.stock_zh_growth_comparison_em(symbol=symbol),
            force_refresh=force_refresh,
        )

    def fetch_dupont_comparison(self, ts_code, force_refresh=False):
        symbol = ts_code_to_akshare_symbol(ts_code)
        return self.load_or_fetch(
            "dupont_cmp",
            symbol,
            lambda: ak.stock_zh_dupont_comparison_em(symbol=symbol),
            force_refresh=force_refresh,
        )

    def fetch_scale_comparison(self, ts_code, force_refresh=False):
        symbol = ts_code_to_akshare_symbol(ts_code)
        return self.load_or_fetch(
            "scale_cmp",
            symbol,
            lambda: ak.stock_zh_scale_comparison_em(symbol=symbol),
            force_refresh=force_refresh,
        )


def extract_latest_period_columns(abstract_df):
    period_cols = [col for col in abstract_df.columns if str(col).isdigit()]
    return sorted(period_cols, reverse=True)


def get_metric_row(abstract_df, metric_name):
    matched = abstract_df[abstract_df["指标"] == metric_name]
    if matched.empty:
        return None
    return matched.iloc[0]


def first_previous_same_period(latest_period, period_cols):
    suffix = latest_period[4:]
    for period in period_cols[1:]:
        if period.endswith(suffix):
            return period
    return period_cols[1] if len(period_cols) > 1 else None


def build_stock_snapshot(stock_row, cache, force_refresh=False):
    ts_code = stock_row["ts_code"]
    abstract_df = cache.fetch_stock_abstract(ts_code, force_refresh=force_refresh)
    if abstract_df.empty:
        raise RuntimeError(f"{ts_code} 财务摘要为空")

    period_cols = extract_latest_period_columns(abstract_df)
    if not period_cols:
        raise RuntimeError(f"{ts_code} 财务摘要没有可用财报期列")

    latest_period = period_cols[0]
    prev_period = first_previous_same_period(latest_period, period_cols)

    metrics = {}
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
    for name in metric_names:
        row = get_metric_row(abstract_df, name)
        if row is None:
            continue
        latest_value = to_numeric(row.get(latest_period))
        prev_value = to_numeric(row.get(prev_period)) if prev_period else None
        metrics[name] = latest_value
        if name in {"归母净利润", "营业总收入", "经营现金流量净额"} and prev_period:
            metrics[f"{name}_同比"] = safe_pct(latest_value, prev_value)

    # 三大报表也缓存下来，后续可继续扩展细化打分。
    for report_name in ["资产负债表", "利润表", "现金流量表"]:
        cache.fetch_stock_report(ts_code, report_name, force_refresh=force_refresh)

    return {
        "ts_code": ts_code,
        "name": stock_row["name"],
        "industry": stock_row["industry"],
        "latest_period": latest_period,
        "revenue": metrics.get("营业总收入"),
        "profit": metrics.get("归母净利润"),
        "operating_cashflow": metrics.get("经营现金流量净额"),
        "revenue_yoy": metrics.get("营业总收入_同比"),
        "profit_yoy": metrics.get("归母净利润_同比"),
        "operating_cashflow_yoy": metrics.get("经营现金流量净额_同比"),
        "roe": metrics.get("净资产收益率(ROE)"),
        "net_margin": metrics.get("销售净利率"),
        "gross_margin": metrics.get("毛利率"),
        "debt_ratio": metrics.get("资产负债率"),
        "bps": metrics.get("每股净资产"),
        "eps": metrics.get("基本每股收益"),
    }


def clean_peer_code(value):
    value = str(value).strip()
    if value in {"行业平均", "行业中值", "nan", "None"}:
        return value
    return value.zfill(6)


def percentile_from_rank(rank, total):
    if rank in (None, 0) or total in (None, 0):
        return None
    pct = (total - rank + 1) / total * 100
    return max(0.0, min(100.0, pct))


def get_total_count_from_rank_field(rank_value):
    if isinstance(rank_value, str) and "/" in rank_value:
        try:
            return int(rank_value.split("/")[-1])
        except Exception:
            return None
    return None


def build_peer_snapshots(industry_df, cache, force_refresh=False):
    representative = industry_df.iloc[0]["ts_code"]
    growth_df = cache.fetch_growth_comparison(representative, force_refresh=force_refresh).copy()
    dupont_df = cache.fetch_dupont_comparison(representative, force_refresh=force_refresh).copy()
    scale_df = cache.fetch_scale_comparison(representative, force_refresh=force_refresh).copy()

    if growth_df.empty or dupont_df.empty:
        raise RuntimeError(f"{industry_df.iloc[0]['industry']} 行业同行比较数据为空")

    growth_df["代码"] = growth_df["代码"].map(clean_peer_code)
    dupont_df["代码"] = dupont_df["代码"].map(clean_peer_code)
    if not scale_df.empty:
        scale_df["代码"] = scale_df["代码"].map(clean_peer_code)

    growth_actual = growth_df[~growth_df["代码"].isin(["行业平均", "行业中值"])].copy()
    dupont_actual = dupont_df[~dupont_df["代码"].isin(["行业平均", "行业中值"])].copy()
    scale_actual = (
        scale_df[~scale_df["代码"].isin(["行业平均", "行业中值"])].copy()
        if not scale_df.empty
        else pd.DataFrame()
    )

    total_growth = len(growth_actual)
    total_dupont = len(dupont_actual)

    rows = []
    for _, stock in industry_df.iterrows():
        code = ts_code_to_plain(stock["ts_code"])
        growth_row = growth_actual[growth_actual["代码"] == code]
        dupont_row = dupont_actual[dupont_actual["代码"] == code]
        scale_row = scale_actual[scale_actual["代码"] == code] if not scale_actual.empty else pd.DataFrame()

        item = {
            "ts_code": stock["ts_code"],
            "industry": stock["industry"],
            "peer_growth_score": None,
            "peer_dupont_score": None,
            "peer_scale_score": None,
        }

        if not growth_row.empty:
            row = growth_row.iloc[0]
            growth_scores = []
            for col in [
                "基本每股收益增长率-24A",
                "营业收入增长率-24A",
                "净利润增长率-24A",
                "基本每股收益增长率-TTM",
                "营业收入增长率-TTM",
                "净利润增长率-TTM",
            ]:
                if col in growth_actual.columns:
                    values = pd.to_numeric(growth_actual[col], errors="coerce")
                    current = to_numeric(row.get(col))
                    if current is not None and values.notna().sum() >= 3:
                        pct = (values <= current).mean() * 100
                        growth_scores.append(pct)

            rank_col = "基本每股收益增长率-3年复合排名"
            if rank_col in row and row.get(rank_col) == row.get(rank_col):
                growth_scores.append(percentile_from_rank(to_numeric(row.get(rank_col)), total_growth))

            if growth_scores:
                item["peer_growth_score"] = round(sum(growth_scores) / len(growth_scores), 2)

        if not dupont_row.empty:
            row = dupont_row.iloc[0]
            dupont_scores = []
            for col in ["ROE-24A", "净利率-24A", "总资产周转率-24A"]:
                if col in dupont_actual.columns:
                    values = pd.to_numeric(dupont_actual[col], errors="coerce")
                    current = to_numeric(row.get(col))
                    if current is not None and values.notna().sum() >= 3:
                        dupont_scores.append((values <= current).mean() * 100)

            leverage_col = "权益乘数-24A"
            if leverage_col in dupont_actual.columns:
                values = pd.to_numeric(dupont_actual[leverage_col], errors="coerce")
                current = to_numeric(row.get(leverage_col))
                if current is not None and values.notna().sum() >= 3:
                    dupont_scores.append((values >= current).mean() * 100)

            rank_col = "ROE-3年平均排名"
            if rank_col in row and row.get(rank_col) == row.get(rank_col):
                dupont_scores.append(percentile_from_rank(to_numeric(row.get(rank_col)), total_dupont))

            if dupont_scores:
                item["peer_dupont_score"] = round(sum(dupont_scores) / len(dupont_scores), 2)

        if not scale_row.empty:
            row = scale_row.iloc[0]
            scale_scores = []
            rank_candidates = [
                to_numeric(row.get("总市值排名")),
                to_numeric(row.get("流通市值排名")),
                to_numeric(row.get("营业收入排名")),
                to_numeric(row.get("净利润排名")),
            ]
            rank_candidates = [item for item in rank_candidates if item is not None]
            rank_total = get_total_count_from_rank_field(row.get("排名"))
            if rank_total is None and rank_candidates:
                rank_total = int(max(rank_candidates))
            for col in ["总市值排名", "流通市值排名", "营业收入排名", "净利润排名"]:
                current_rank = to_numeric(row.get(col))
                if current_rank is not None:
                    total = rank_total or len(scale_actual)
                    if total < current_rank:
                        total = int(current_rank)
                    pct = percentile_from_rank(current_rank, total)
                    if pct is not None:
                        scale_scores.append(pct)
            if scale_scores:
                item["peer_scale_score"] = round(sum(scale_scores) / len(scale_scores), 2)

        rows.append(item)

    return pd.DataFrame(rows)


def compute_quality_score(row):
    components = []

    for key, weight in [
        ("peer_growth_score", 0.35),
        ("peer_dupont_score", 0.35),
        ("peer_scale_score", 0.10),
    ]:
        value = row.get(key)
        if pd.notna(value):
            components.append((value, weight))

    bonus = 0
    if pd.notna(row.get("profit_yoy")) and row["profit_yoy"] > 0:
        bonus += 6
    if pd.notna(row.get("revenue_yoy")) and row["revenue_yoy"] > 0:
        bonus += 4
    if pd.notna(row.get("operating_cashflow")) and row["operating_cashflow"] > 0:
        bonus += 6
    if pd.notna(row.get("roe")) and row["roe"] >= 10:
        bonus += 6
    if pd.notna(row.get("net_margin")) and row["net_margin"] >= 10:
        bonus += 4
    if row.get("industry") != "银行" and pd.notna(row.get("debt_ratio")) and row["debt_ratio"] <= 65:
        bonus += 4

    if not components:
        return round(bonus, 2)

    base = sum(score * weight for score, weight in components) / sum(weight for _, weight in components)
    return round(min(base + bonus, 100), 2)


def label_reliability(row):
    if row["quality_score"] >= 80:
        return "高"
    if row["quality_score"] >= 65:
        return "中"
    return "低"


def screen_stocks(universe_df, top_n=30, force_refresh=False):
    cache = AkshareCache()
    snapshots = []
    peer_frames = []
    errors = []

    for industry, group in universe_df.groupby("industry"):
        try:
            peer_frames.append(build_peer_snapshots(group, cache, force_refresh=force_refresh))
        except Exception as exc:
            errors.append((industry, f"同行比较失败: {exc}"))

    peer_df = pd.concat(peer_frames, ignore_index=True) if peer_frames else pd.DataFrame()

    for _, stock in universe_df.iterrows():
        try:
            snapshots.append(build_stock_snapshot(stock, cache, force_refresh=force_refresh))
        except Exception as exc:
            errors.append((stock["ts_code"], f"财报摘要失败: {exc}"))

    if not snapshots:
        raise RuntimeError("未能成功拉取任何财报数据或同行比较数据")

    result_df = pd.DataFrame(snapshots)
    if not peer_df.empty:
        result_df = result_df.merge(peer_df, on=["ts_code", "industry"], how="left")

    result_df["quality_score"] = result_df.apply(compute_quality_score, axis=1)
    result_df["reliability_level"] = result_df.apply(label_reliability, axis=1)
    result_df = result_df.sort_values(
        ["quality_score", "peer_dupont_score", "peer_growth_score", "profit_yoy"],
        ascending=False,
    )
    return result_df.head(top_n), errors


def parse_args():
    parser = argparse.ArgumentParser(description="基于财报质量和同行业对比筛选靠谱股票")
    parser.add_argument("--industry", help="可选，按行业筛选，例如 银行、软件服务")
    parser.add_argument("--top", type=int, default=30, help="输出前N只股票")
    parser.add_argument("--limit", type=int, default=0, help="只处理前N只股票，0表示不限制")
    parser.add_argument("--force-refresh", action="store_true", help="忽略本地缓存并重新拉取数据")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    universe = load_stock_universe(args.industry)
    if args.limit > 0:
        universe = universe.head(args.limit).copy()

    try:
        results, errors = screen_stocks(
            universe,
            top_n=args.top,
            force_refresh=args.force_refresh,
        )
        display_cols = [
            "ts_code",
            "name",
            "industry",
            "latest_period",
            "quality_score",
            "reliability_level",
            "peer_growth_score",
            "peer_dupont_score",
            "peer_scale_score",
            "roe",
            "profit_yoy",
            "revenue_yoy",
            "operating_cashflow",
            "debt_ratio",
        ]
        display_cols = [col for col in display_cols if col in results.columns]
        print(results[display_cols].to_string(index=False))

        if errors:
            print(f"\n提示: 有 {len(errors)} 条记录拉取失败，首条为: {errors[0][0]} -> {errors[0][1]}")
    except RuntimeError as exc:
        print(f"财报筛选失败: {exc}")
