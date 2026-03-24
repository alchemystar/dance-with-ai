import argparse
import logging
from time import perf_counter

from backtest import screen_core_long_term_pool
from runtime_logging import setup_runtime_logging

logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="核心长期持有池筛选器")
    parser.add_argument("--days", type=int, default=720, help="筛选使用的历史窗口天数")
    parser.add_argument(
        "--value-scope",
        choices=["sample", "all"],
        default="all",
        help="价值股候选范围，sample 看样本池，all 看更大范围",
    )
    parser.add_argument("--value-limit", type=int, default=80, help="价值股候选池限制数量")
    parser.add_argument(
        "--bank-scope",
        choices=["developed", "all"],
        default="developed",
        help="银行候选范围，developed 只看发达地区银行，all 看全国银行",
    )
    parser.add_argument("--top", type=int, default=20, help="输出前 N 只")
    parser.add_argument("--refresh-cache", action="store_true", help="忽略本地缓存并刷新数据")
    return parser.parse_args()


if __name__ == "__main__":
    setup_runtime_logging("core_long_term_pool.log")
    args = parse_args()
    started_at = perf_counter()
    logger.info(
        "开始筛选核心长期持有池，days=%s value_scope=%s value_limit=%s bank_scope=%s top=%s refresh_cache=%s",
        args.days,
        args.value_scope,
        args.value_limit,
        args.bank_scope,
        args.top,
        args.refresh_cache,
    )
    result_df = screen_core_long_term_pool(
        days=args.days,
        value_scope=args.value_scope,
        value_limit=args.value_limit,
        bank_scope=args.bank_scope,
        top=args.top,
        force_refresh=args.refresh_cache,
    )
    logger.info("核心长期持有池筛选完成，结果 %s 只，耗时 %.2fs", len(result_df), perf_counter() - started_at)
    if result_df.empty:
        logger.warning("没有筛出符合条件的核心长期持有池")
        print("没有筛出符合条件的核心长期持有池")
    else:
        logger.info("开始输出核心长期持有池结果")
        print(result_df.to_string(index=False))
