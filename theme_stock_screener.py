import argparse

from backtest import screen_theme_stocks


def parse_args():
    parser = argparse.ArgumentParser(description="题材股筛选器")
    parser.add_argument("--days", type=int, default=120, help="筛选使用的历史窗口天数")
    parser.add_argument("--scope", choices=["leaders", "all"], default="all", help="leaders 只看核心名单，all 看整个题材行业池")
    parser.add_argument("--top", type=int, default=15, help="输出前 N 只")
    parser.add_argument("--refresh-cache", action="store_true", help="忽略本地缓存并刷新数据")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    result_df = screen_theme_stocks(
        days=args.days,
        scope=args.scope,
        top=args.top,
        force_refresh=args.refresh_cache,
    )
    if result_df.empty:
        print("没有筛出可用的题材股样本")
    else:
        print(result_df.to_string(index=False))
