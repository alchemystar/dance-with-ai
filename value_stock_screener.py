import argparse

from backtest import screen_value_stocks


def parse_args():
    parser = argparse.ArgumentParser(description="低估值健康股筛选器")
    parser.add_argument("--days", type=int, default=720, help="筛选使用的历史窗口天数")
    parser.add_argument(
        "--scope",
        choices=["sample", "all"],
        default="sample",
        help="sample 先看代表性样本池，all 看全市场",
    )
    parser.add_argument("--top", type=int, default=20, help="输出前 N 只")
    parser.add_argument("--limit", type=int, default=0, help="限制处理股票数量，0 表示不限制")
    parser.add_argument("--refresh-cache", action="store_true", help="忽略本地缓存并刷新数据")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    result_df = screen_value_stocks(
        days=args.days,
        scope=args.scope,
        top=args.top,
        limit=args.limit,
        force_refresh=args.refresh_cache,
    )
    if result_df.empty:
        print("没有筛出符合条件的低估值健康股")
    else:
        print(result_df.to_string(index=False))
