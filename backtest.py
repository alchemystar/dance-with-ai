import tushare as ts
import pandas as pd
from datetime import datetime, timedelta
from macd_with_optimize_sell import macd_with_optimize_sell_strategy
from stragegy_for_600345 import stragegy_for_600345
from email_util import generate_html_table, send_email
from print_util import print_transactions
from macd_with_deepdown import macd_with_deepdown

# Initialize Tushare with your token
ts.set_token('c0f992e8369579bfec7bf8481dc0bcc304ac66ab5b1dd12c1d154325')
pro = ts.pro_api()

def fetch_stock_data(stock_code, start_date, end_date):
    """
    获取股票数据，根据股票代码判断是A股还是港股
    Args:
        stock_code: 股票代码（格式：A股为'000001.SZ'，港股为'00001.HK'）
        start_date: 开始日期
        end_date: 结束日期
    """
    if '.HK' in stock_code:
        # 获取港股数据
        df = pro.hk_daily(ts_code=stock_code, start_date=start_date, end_date=end_date)
    
    elif stock_code.startswith('5') or stock_code.startswith('15'):
        df = pro.fund_daily(ts_code=stock_code, start_date=start_date, end_date=end_date)
    elif '.SZ' in stock_code or '.SH' in stock_code:
        # 获取A股数据
        df = pro.daily(ts_code=stock_code, start_date=start_date, end_date=end_date)

     
    # 按照交易日期正序排列
    df = df.sort_values(by='trade_date', ascending=True)
    # 重置索引，保证索引是连续的
    df = df.reset_index(drop=True)
    return df

def backtest(df):
    initial_cash = 100000
    cash = initial_cash
    # 这边删除最后一行的原因是。df中包含了下一个交易中的信号数据
    df =  df.iloc[:-1]
    stock = 0
    transactions = []
    last_buy_price = 0
    total_profit = 0  # 追踪总盈利金额
    max_drawdown = 0  # 初始化最大回撤
    for i in range(len(df)):
        if df['signal'].iloc[i] == 1 and cash > 0:
            stock = cash / df['open'].iloc[i]
            last_buy_price = df['open'].iloc[i]
            cash = 0
            reason = df['reason'].iloc[i]
            transactions.append({
                'date': df['trade_date'].iloc[i],
                'action': 'buy',
                'price': df['open'].iloc[i],
                'amount': stock,  # 记录购买的股数
                'cost': last_buy_price * stock,  # 记录总成本
                'reason': reason,
                'return_rate': 0
            })
        elif df['signal'].iloc[i] == -1 and stock > 0:
            sell_price = df['open'].iloc[i]
            sell_amount = stock * sell_price
            profit = sell_amount - (last_buy_price * stock)  # 计算本次交易盈亏
            total_profit += profit  # 累加总盈亏
            current_return = (sell_price - last_buy_price) / last_buy_price * 100
            cash = sell_amount
            stock = 0
            
            reason = df['reason'].iloc[i]
            transactions.append({
                'date': df['trade_date'].iloc[i],
                'action': 'sell',
                'price': sell_price,
                'amount': stock,
                'profit': profit,  # 记录本次交易盈亏
                'reason': reason,
                'return_rate': current_return
            })

        # 计算当前总资产
        current_value = cash + (stock * df['close'].iloc[i])
        # 更新峰值
        if current_value < initial_cash:
            # 计算当前回撤
            drawdown = (current_value - initial_cash) / initial_cash
        # 更新最大回撤 是负值。。。。。所以用小于
            if drawdown < max_drawdown:
                max_drawdown = drawdown

    # 如果最后还持有股票，计算最终市值
    final_value = cash + (stock * df['close'].iloc[-1])
    
    # 计算总收益率
    total_return = ((final_value - initial_cash) / initial_cash) * 100
    
    # 计算年化收益率
    days = (pd.to_datetime(df['trade_date'].iloc[-1]) - pd.to_datetime(df['trade_date'].iloc[0])).days
    annual_return = (1 + total_return/100) ** (365/abs(days)) - 1

    # 计算交易统计信息
    total_trades = len([t for t in transactions if t['action'] == 'sell'])  # 只计算卖出次数
    profitable_trades = len([t for t in transactions if t['action'] == 'sell' and t.get('profit', 0) > 0])
    loss_trades = len([t for t in transactions if t['action'] == 'sell' and t.get('profit', 0) < 0])
    
    # 计算收益统计
    sell_transactions = [t for t in transactions if t['action'] == 'sell']
    if sell_transactions:
        max_return = max([t['return_rate'] for t in sell_transactions])
        min_return = min([t['return_rate'] for t in sell_transactions])
        avg_return = sum([t['return_rate'] for t in sell_transactions]) / len(sell_transactions)
    else:
        max_return = min_return = avg_return = 0

    trading_stats = {
        'total_trades': total_trades,
        'profitable_trades': profitable_trades,
        'loss_trades': loss_trades,
        'max_return': max_return,
        'min_return': min_return,
        'avg_return': avg_return,
        'total_profit': total_profit,  # 添加总盈利金额
        'max_drawdown': max_drawdown * 100  # 添加最大回撤，转换为百分比
    }

    return final_value, transactions, total_return, annual_return, trading_stats

def analyze_stock_pool(stock_pool, start_date, end_date,strategyClass):
    results = []
    for stock_code in stock_pool:
        try:
            # 获取数据并计算
            sockData = fetch_stock_data(stock_code, start_date, end_date)
            tradingSignal = strategyClass.trading_strategy(sockData)
            final_value, transactions, total_return, annual_return, trading_stats = backtest(tradingSignal)
            prediction = strategyClass.predict_next_signal(tradingSignal)
            print_transactions(transactions)
                        # 计算盈亏比
            profitable = trading_stats['profitable_trades']
            losses = trading_stats['loss_trades']
            # 如果亏损次数为0，设置一个很大的比率
            if losses == 0:
                ratio = float('inf')
            else:
                ratio = profitable / losses
            results.append({
                'stock_code': stock_code,
                'total_return': total_return,
                'annual_return': annual_return,
                'stats': trading_stats,
                'prediction': prediction,
                'final_value': final_value,
                'profit_loss_ratio': ratio
            })

        except Exception as e:
            print(f"处理股票 {stock_code} 时出错: {str(e)}")
            continue
    
    return results

def get_date_range(days=365):
    """获取回测日期范围"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    return start_date.strftime('%Y%m%d'), end_date.strftime('%Y%m%d')

if __name__ == "__main__":
    # 定义股票池和名称映射
    stock_names = {
        '600919.SH': '江苏银行',
        '600345.SH': '长江通信',
        '000001.SZ': '平安银行',
        '512480.SH':'半导体ETF',
        '515650.SH':'消费ETF',
        '600161.SH':'天坛生物',
        '002270.SZ':'华明装备',
        '300762.SZ':'上海瀚讯',
        '03692.HK':'瀚讯制药'
    }
    

    # 动态计算一年的回测区间
    start_date, end_date = get_date_range(365)
    print(f"回测区间: {start_date} 至 {end_date}")
    
    print("开始分析股票池...\n")
    results = []
    results.extend(analyze_stock_pool(['600919.SH','000001.SZ'], start_date, end_date, macd_with_optimize_sell_strategy(5,0.01))) # 5表示涨了500%
    results.extend(analyze_stock_pool(['600345.SH'], start_date, end_date, stragegy_for_600345())) # 长江通信
    results.extend(analyze_stock_pool(['515650.SH'], start_date, end_date, macd_with_optimize_sell_strategy(5,0.01))) 
    results.extend(analyze_stock_pool(['300762.SZ'], start_date, end_date, macd_with_optimize_sell_strategy(5,0.08)))
    results.extend(analyze_stock_pool(['600161.SH'], start_date, end_date, macd_with_deepdown()))

    # 按收益率排序
    results.sort(key=lambda x: x['total_return'], reverse=True)
    
    # 定义收件人列表
    recipients = ["652433935@qq.com"]
    
    # 生成HTML表格并发送邮件
    html_content = generate_html_table(results, stock_names)
    send_email(recipients, html_content)
    
    # 输出高收益股票信息
    for result in results:
        stock_code = result['stock_code']
        stock_name = stock_names[stock_code]
        print(f"\n========== {stock_code} {stock_name} ==========")
        pred = result['prediction']
        print(f"\n明日操作建议:")
        print(f"建议操作: {pred['signal']}")
        print(f"原因: {pred['reason']}")
        print(f"MACD指标 - DIF: {pred['dif']:.3f}, DEA: {pred['dea']:.3f}, MACD: {pred['macd']:.3f}")
        print("=" * 50)
        print(f"总收益率: {result['total_return']:.2f}%")
        print(f"年化收益率: {result['annual_return']*100:.2f}%")
        print(f"最终资金: {result['final_value']:.2f}")
        
        stats = result['stats']
        print(f"\n交易统计:")
        print(f"总交易次数: {stats['total_trades']}")
        print(f"盈利/亏损: {stats['profitable_trades']}/{stats['loss_trades']}")
        print(f"盈亏比: {result['profit_loss_ratio']:.2f}")
        print(f"最大单笔收益: {stats['max_return']:.2f}%")
        print(f"最大单笔回撤: {stats['min_return']:.2f}%")
        print(f"平均收益率: {stats['avg_return']:.2f}%")