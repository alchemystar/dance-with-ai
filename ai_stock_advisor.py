import argparse
import json
import os
from datetime import datetime
from pathlib import Path

from openai import OpenAI

from backtest import DEFAULT_STOCK_NAMES, enrich_with_market_context, fetch_stock_data
from bank_t_strategy import bank_t_strategy
from macd_with_deepdown import macd_with_deepdown
from macd_with_optimize_sell import macd_with_optimize_sell_strategy
from macd_with_regime_filter import macd_with_regime_filter_strategy
from stragegy_for_600345 import stragegy_for_600345

DEFAULT_OPENAI_MODEL = "gpt-5.4"
AI_CACHE_DIR = Path(".cache/openai_cross_section")


def resolve_strategy(mode, stock_code):
    if mode == "regime":
        return macd_with_regime_filter_strategy()
    if mode == "bank_t":
        return bank_t_strategy()
    if mode == "optimize":
        return macd_with_optimize_sell_strategy()
    if mode == "deepdown":
        return macd_with_deepdown()
    if mode == "600345":
        return stragegy_for_600345()
    if mode == "auto":
        if stock_code in {"600919.SH", "000001.SZ"}:
            return bank_t_strategy()
        if stock_code == "600345.SH":
            return stragegy_for_600345()
        if stock_code in {"600161.SH", "002270.SZ"}:
            return macd_with_deepdown()
        return macd_with_regime_filter_strategy()
    raise ValueError(f"不支持的策略模式: {mode}")


def build_snapshot(df, signal_df, technical_prediction):
    latest = signal_df.iloc[-1]
    prev = signal_df.iloc[-2]

    snapshot = {
        "trade_date": latest["trade_date"],
        "close": round(float(latest["close"]), 4),
        "pct_chg": round(float(latest.get("pct_chg", 0)), 4),
        "volume": round(float(latest.get("vol", 0)), 4) if "vol" in latest else None,
        "return_5d": round(float(signal_df["close"].pct_change(5).iloc[-1] * 100), 2),
        "return_20d": round(float(signal_df["close"].pct_change(20).iloc[-1] * 100), 2),
        "dif": round(float(latest.get("dif", 0)), 4),
        "dea": round(float(latest.get("dea", 0)), 4),
        "macd": round(float(latest.get("macd", 0)), 4),
        "rsi": round(float(latest.get("rsi", 0)), 2) if "rsi" in latest else None,
        "technical_signal": technical_prediction["signal"],
        "technical_reason": technical_prediction["reason"],
        "yesterday_close": round(float(prev["close"]), 4),
    }

    if "target_position" in technical_prediction:
        snapshot["target_position"] = round(float(technical_prediction["target_position"]), 4)

    for col in [
        "ema_fast_trend",
        "ema_slow_trend",
        "sh_index_close",
        "sh_index_pct_chg",
        "csi300_close",
        "csi300_pct_chg",
    ]:
        if col in latest and latest[col] == latest[col]:
            snapshot[col] = round(float(latest[col]), 4)

    return snapshot


def infer_user_location(stock_code):
    if stock_code.endswith(".HK"):
        return {
            "type": "approximate",
            "country": "HK",
            "city": "Hong Kong",
            "region": "Hong Kong",
        }
    return {
        "type": "approximate",
        "country": "CN",
        "city": "Shanghai",
        "region": "Shanghai",
    }


def build_prompt(stock_code, stock_name, snapshot):
    today = datetime.now().strftime("%Y-%m-%d")
    return f"""
你是一名谨慎的股票研究员，请基于最新网络信息评估这只股票在未来 1-10 个交易日内是否值得买入。

今天日期: {today}
股票代码: {stock_code}
股票名称: {stock_name}

技术面上下文:
{json.dumps(snapshot, ensure_ascii=False, indent=2)}

任务要求:
1. 搜索这只股票最近的新闻、财报、业绩预告、交易所公告、监管问询、机构观点和舆情变化。
2. 优先参考官方公告、交易所披露、公司财报、权威财经媒体。
3. 重点看最近 90 天，若过去 30 天有重大事件要重点强调。
4. 结合给定技术面上下文，判断是否适合买入。
5. 如果信息不充分，也要明确说明不确定性。

请只返回合法 JSON，不要输出 Markdown。JSON 结构如下:
{{
  "decision": "strong_buy|buy|watch|avoid",
  "confidence": 0,
  "sentiment_score": 0,
  "summary": "",
  "bullish_factors": ["", ""],
  "bearish_factors": ["", ""],
  "key_dates": ["YYYY-MM-DD: event"],
  "technical_alignment": "",
  "risk_flags": ["", ""],
  "final_action": ""
}}
""".strip()


def extract_json(output_text):
    try:
        return json.loads(output_text)
    except json.JSONDecodeError:
        start = output_text.find("{")
        end = output_text.rfind("}")
        if start != -1 and end != -1 and end > start:
            return json.loads(output_text[start : end + 1])
        raise


def extract_sources(response):
    if hasattr(response, "model_dump"):
        payload = response.model_dump()
    else:
        payload = response

    stack = [payload]
    sources = []
    seen = set()

    while stack:
        item = stack.pop()
        if isinstance(item, dict):
            url = item.get("url")
            if isinstance(url, str) and url not in seen:
                sources.append(
                    {
                        "title": item.get("title") or item.get("name") or url,
                        "url": url,
                    }
                )
                seen.add(url)
            stack.extend(item.values())
        elif isinstance(item, list):
            stack.extend(item)

    return sources[:12]


def _normalize_model_name(model):
    return str(model).replace("/", "_").replace(":", "_")


def _get_ai_cache_path(stock_code, trade_date, model):
    AI_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return AI_CACHE_DIR / f"{stock_code}_{trade_date}_{_normalize_model_name(model)}.json"


def evaluate_with_gpt54(stock_code, stock_name, snapshot, model, use_cache=True):
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("缺少 OPENAI_API_KEY，无法调用 GPT-5.4")

    cache_path = _get_ai_cache_path(
        stock_code=stock_code,
        trade_date=snapshot.get("trade_date", datetime.now().strftime("%Y%m%d")),
        model=model,
    )
    if use_cache and cache_path.exists():
        return json.loads(cache_path.read_text(encoding="utf-8"))

    client = OpenAI(api_key=api_key)
    response = client.responses.create(
        model=model,
        reasoning={"effort": "medium"},
        tools=[
            {
                "type": "web_search",
                "user_location": infer_user_location(stock_code),
            }
        ],
        tool_choice="auto",
        include=["web_search_call.action.sources"],
        input=build_prompt(stock_code, stock_name, snapshot),
    )

    report = extract_json(response.output_text)
    report["sources"] = extract_sources(response)
    report["model"] = model
    cache_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report


def build_ai_report_for_stock(
    stock_code,
    mode="auto",
    start_date=None,
    end_date=None,
    refresh_cache=False,
    model=None,
):
    model = model or os.getenv("OPENAI_MODEL", DEFAULT_OPENAI_MODEL)
    end_date = end_date or datetime.now().strftime("%Y%m%d")
    start_date = start_date or "20240331"
    stock_name = DEFAULT_STOCK_NAMES.get(stock_code, stock_code)
    strategy = resolve_strategy(mode, stock_code)

    df = fetch_stock_data(stock_code, start_date, end_date, force_refresh=refresh_cache)
    df = enrich_with_market_context(
        df,
        start_date,
        end_date,
        force_refresh=refresh_cache,
        stock_code=stock_code,
    )
    signal_df = strategy.trading_strategy(df.copy())
    technical_prediction = strategy.predict_next_signal(signal_df)
    snapshot = build_snapshot(df, signal_df, technical_prediction)
    report = evaluate_with_gpt54(stock_code, stock_name, snapshot, model, use_cache=not refresh_cache)
    return {
        "stock_code": stock_code,
        "stock_name": stock_name,
        "strategy_name": strategy.__class__.__name__,
        "technical_prediction": technical_prediction,
        "snapshot": snapshot,
        "report": report,
    }


def print_report(stock_code, stock_name, strategy_name, technical_prediction, report):
    print(f"股票: {stock_code} {stock_name}")
    print(f"技术策略: {strategy_name}")
    print(f"技术信号: {technical_prediction['signal']}")
    print(f"技术原因: {technical_prediction['reason']}")
    print(f"AI模型: {report['model']}")
    print(f"AI结论: {report['decision']}")
    print(f"AI动作: {report['final_action']}")
    print(f"置信度: {report['confidence']}")
    print(f"情绪分数: {report['sentiment_score']}")
    print(f"总结: {report['summary']}")
    print("利多因素:")
    for item in report.get("bullish_factors", []):
        print(f"- {item}")
    print("利空因素:")
    for item in report.get("bearish_factors", []):
        print(f"- {item}")
    print("风险提示:")
    for item in report.get("risk_flags", []):
        print(f"- {item}")
    print("关键日期:")
    for item in report.get("key_dates", []):
        print(f"- {item}")
    print("参考来源:")
    for source in report.get("sources", []):
        print(f"- {source['title']}: {source['url']}")


def parse_args():
    parser = argparse.ArgumentParser(description="使用 GPT-5.4 + 最新新闻/财报/舆情评估是否买入")
    parser.add_argument("--stock", required=True, help="股票代码，例如 300762.SZ")
    parser.add_argument(
        "--mode",
        choices=["auto", "regime", "bank_t", "optimize", "deepdown", "600345"],
        default="auto",
        help="先跑哪套技术策略，再交给 GPT-5.4 做新闻/财报/舆情评估",
    )
    parser.add_argument("--start-date", default="20240331", help="技术分析起始日期")
    parser.add_argument("--end-date", default=datetime.now().strftime("%Y%m%d"), help="技术分析结束日期")
    parser.add_argument("--refresh-cache", action="store_true", help="忽略本地缓存并重新拉取Tushare数据")
    parser.add_argument("--model", default=os.getenv("OPENAI_MODEL", DEFAULT_OPENAI_MODEL), help="OpenAI 模型名称")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    bundle = build_ai_report_for_stock(
        stock_code=args.stock,
        mode=args.mode,
        start_date=args.start_date,
        end_date=args.end_date,
        refresh_cache=args.refresh_cache,
        model=args.model,
    )
    print_report(
        bundle["stock_code"],
        bundle["stock_name"],
        bundle["strategy_name"],
        bundle["technical_prediction"],
        bundle["report"],
    )
