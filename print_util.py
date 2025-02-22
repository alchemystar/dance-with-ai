
def print_transactions(transactions):
    """
    打印交易记录的格式化函数
    
    Args:
        transactions: 交易记录列表，每个记录包含date、action、price、return_rate和reason
    """
    if not transactions:
        print("\n没有交易记录")
        return
        
    print("\n交易记录:")
    print(f"{'日期':<12}{'操作':<6}{'价格':>10}{'收益率':>10}  {'交易原因'}")
    print("-" * 80)
    
    for trade in transactions:
        date = trade['date']
        action = "买入" if trade['action'] == 'buy' else "卖出"
        price = trade['price']
        return_rate = trade['return_rate']
        reason = trade['reason']
        
        # 格式化输出每条交易记录
        print(f"{date:<12}{action:<6}{price:>10.2f}{return_rate:>10.2f}%  {reason}")

def print_cross_signals(golden_cross_dates, death_cross_dates):
    """打印金叉和死叉信号点"""
    print("\n=== MACD金叉信号点 ===")
    print(f"{'日期':<12}{'收盘价':>10}{'DIF':>10}{'DEA':>10}")
    print("-" * 42)
    for cross in golden_cross_dates:
        print(f"{cross['date']:<12}{cross['close']:>10.2f}{cross['dif']:>10.3f}{cross['dea']:>10.3f}")
    
    print("\n=== MACD死叉信号点 ===")
    print(f"{'日期':<12}{'收盘价':>10}{'DIF':>10}{'DEA':>10}{'类型':>10}")
    print("-" * 52)
    for cross in death_cross_dates:
        print(f"{cross['date']:<12}{cross['close']:>10.2f}{cross['dif']:>10.3f}{cross['dea']:>10.3f}{cross['type']:>10}")
