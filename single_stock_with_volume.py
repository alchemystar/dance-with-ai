import tushare as ts
import pandas as pd
from datetime import datetime, timedelta
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header

# Initialize Tushare with your token
ts.set_token('c0f992e8369579bfec7bf8481dc0bcc304ac66ab5b1dd12c1d154325')
pro = ts.pro_api()

def fetch_stock_data(stock_code, start_date, end_date):
    df = pro.daily(ts_code=stock_code, start_date=start_date, end_date=end_date)
    # 按照交易日期正序排列
    df = df.sort_values(by='trade_date', ascending=True)
    # 重置索引，保证索引是连续的
    df = df.reset_index(drop=True)
    return df

def calculate_macd(df, fast=12, slow=26, signal=9):
    """计算MACD指标"""
    # 计算快速和慢速EMA
    exp1 = df['close'].ewm(span=fast, adjust=False).mean()
    exp2 = df['close'].ewm(span=slow, adjust=False).mean()
    
    # 计算DIF (MACD线)
    df['dif'] = exp1 - exp2
    # 计算DEA (信号线)
    df['dea'] = df['dif'].ewm(span=signal, adjust=False).mean()
    # 计算MACD柱状图
    df['macd'] = (df['dif'] - df['dea']) * 2
    return df

def trading_strategy(df):
    # 计算MACD指标
    df = calculate_macd(df)
    df['signal'] = 0
    
    for i in range(1, len(df)):
        # MACD金叉：DIF从下向上穿越DEA
        if (df['dif'].iloc[i-1] < df['dea'].iloc[i-1] and 
            df['dif'].iloc[i] > df['dea'].iloc[i]):
            df.loc[i, 'signal'] = 1
        
        # MACD死叉：DIF从上向下穿越DEA
        elif (df['dif'].iloc[i-1] > df['dea'].iloc[i-1] and 
              df['dif'].iloc[i] < df['dea'].iloc[i]):
            df.loc[i, 'signal'] = -1
    
    return df

def backtest(df, initial_cash=100000):
    cash = initial_cash
    stock = 0
    transactions = []
    last_buy_price = 0

    for i in range(len(df)):
        if df['signal'].iloc[i] == 1 and cash > 0:
            stock = cash / df['open'].iloc[i]
            last_buy_price = df['open'].iloc[i]
            cash = 0
            reason = (f"MACD金叉买入 - DIF: {df['dif'].iloc[i]:.3f}, "
                     f"DEA: {df['dea'].iloc[i]:.3f}, "
                     f"MACD: {df['macd'].iloc[i]:.3f}")
            transactions.append({
                'date': df['trade_date'].iloc[i],
                'action': 'buy',
                'price': df['open'].iloc[i],
                'reason': reason,
                'return_rate': 0
            })
        elif df['signal'].iloc[i] == -1 and stock > 0:
            cash = stock * df['open'].iloc[i]
            current_return = (df['open'].iloc[i] - last_buy_price) / last_buy_price * 100
            stock = 0
            reason = (f"MACD死叉卖出 - DIF: {df['dif'].iloc[i]:.3f}, "
                     f"DEA: {df['dea'].iloc[i]:.3f}, "
                     f"MACD: {df['macd'].iloc[i]:.3f}")
            transactions.append({
                'date': df['trade_date'].iloc[i],
                'action': 'sell',
                'price': df['open'].iloc[i],
                'reason': reason,
                'return_rate': current_return
            })

    final_value = cash + stock * df['close'].iloc[-1]
    total_return = (final_value - initial_cash) / initial_cash * 100
    
    # Calculate annualized return
    days = (pd.to_datetime(df['trade_date'].iloc[0]) - pd.to_datetime(df['trade_date'].iloc[-1])).days
    annual_return = (1 + total_return/100) ** (365/abs(days)) - 1

    # 计算交易统计信息
    total_trades = len(transactions)
    profitable_trades = len([t for t in transactions if t['return_rate'] > 0 and t['action'] == 'sell'])
    loss_trades = len([t for t in transactions if t['return_rate'] < 0 and t['action'] == 'sell'])
    
    # 计算收益统计
    sell_transactions = [t for t in transactions if t['action'] == 'sell']
    if sell_transactions:
        max_return = max([t['return_rate'] for t in sell_transactions])
        min_return = min([t['return_rate'] for t in sell_transactions])
        avg_return = sum([t['return_rate'] for t in sell_transactions]) / len(sell_transactions)
    else:
        max_return = min_return = avg_return = 0

    trading_stats = {
        'total_trades': total_trades // 2,  # 买卖一对算一次交易
        'profitable_trades': profitable_trades,
        'loss_trades': loss_trades,
        'max_return': max_return,
        'min_return': min_return,
        'avg_return': avg_return
    }

    return final_value, transactions, total_return, annual_return, trading_stats

def predict_next_signal(df):
    """预测明天的可能信号"""
    # 获取最近两天的数据
    last_dif = df['dif'].iloc[-1]
    last_dea = df['dea'].iloc[-1]
    prev_dif = df['dif'].iloc[-2]
    prev_dea = df['dea'].iloc[-2]
    
    # 判断今天是否形成金叉或死叉
    golden_cross = prev_dif < prev_dea and last_dif > last_dea
    death_cross = prev_dif > prev_dea and last_dif < last_dea
    
    # 确定信号和建议
    if golden_cross:
        signal = "买入"
        reason = "今日形成MACD金叉，建议明日开盘买入"
    elif death_cross:
        signal = "卖出"
        reason = "今日形成MACD死叉，建议明日开盘卖出"
    else:
        signal = "观望"
        if last_dif > last_dea:
            reason = "MACD处于多头区间，DIF在DEA上方，建议持股待涨"
        else:
            reason = "MACD处于空头区间，DIF在DEA下方，建议观望等待"
    
    return {
        'signal': signal,
        'reason': reason,
        'dif': last_dif,
        'dea': last_dea,
        'macd': (last_dif - last_dea) * 2,
        'golden_cross': golden_cross,
        'death_cross': death_cross
    }

def analyze_stock_pool(stock_pool, start_date, end_date, min_return=40, profit_loss_ratio=1.5):
    """
    分析股票池中的所有股票
    profit_loss_ratio: 盈利次数与亏损次数的最小比例要求，如1.5表示盈利次数至少是亏损次数的1.5倍
    """
    results = []
    
    for stock_code in stock_pool:
        try:
            # 获取数据并计算
            df = fetch_stock_data(stock_code, start_date, end_date)
            df = trading_strategy(df)
            final_value, transactions, total_return, annual_return, trading_stats = backtest(df)
            prediction = predict_next_signal(df)
            
            # 计算盈亏比
            profitable = trading_stats['profitable_trades']
            losses = trading_stats['loss_trades']
            
            # 如果亏损次数为0，设置一个很大的比率
            if losses == 0:
                ratio = float('inf')
            else:
                ratio = profitable / losses
            
            # 同时满足收益率和盈亏比要求
            if total_return > min_return and ratio >= profit_loss_ratio:
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

def generate_html_table(results, stock_names):
    """生成HTML格式的结果表格"""
    # 将结果分为有信号和无信号两组
    signal_stocks = []
    no_signal_stocks = []
    
    for result in results:
        if result['prediction']['signal'] in ['买入', '卖出']:
            signal_stocks.append(result)
        else:
            no_signal_stocks.append(result)
    
    # 先显示有信号的股票，再显示无信号的股票
    sorted_results = signal_stocks + no_signal_stocks
    
    html = """
    <html>
    <head>
        <style>
            table { border-collapse: collapse; width: 100%; }
            th, td { border: 1px solid black; padding: 8px; text-align: left; }
            th { background-color: #f2f2f2; }
            tr:nth-child(even) { background-color: #f9f9f9; }
            .buy { color: #008000; font-weight: bold; }  /* 买入使用绿色 */
            .sell { color: #FF0000; font-weight: bold; } /* 卖出使用红色 */
        </style>
    </head>
    <body>
        <h2>MACD策略回测结果</h2>
        <table>
            <tr>
                <th>股票代码</th>
                <th>股票名称</th>
                <th>明日操作</th>
                <th>操作原因</th>
                <th>总收益率</th>
                <th>年化收益率</th>
                <th>交易次数</th>
                <th>盈利/亏损</th>
                <th>盈亏比</th>
            </tr>
    """
    
    for result in sorted_results:
        stock_code = result['stock_code']
        stock_name = stock_names[stock_code]
        pred = result['prediction']
        stats = result['stats']
        
        # 根据操作类型设置颜色类名
        signal_class = 'buy' if pred['signal'] == '买入' else 'sell' if pred['signal'] == '卖出' else ''
        
        html += f"""
            <tr>
                <td>{stock_code}</td>
                <td>{stock_name}</td>
                <td class="{signal_class}">{pred['signal']}</td>
                <td>{pred['reason']}</td>
                <td>{result['total_return']:.2f}%</td>
                <td>{result['annual_return']*100:.2f}%</td>
                <td>{stats['total_trades']}</td>
                <td>{stats['profitable_trades']}/{stats['loss_trades']}</td>
                <td>{result['profit_loss_ratio']:.2f}</td>
            </tr>
        """
    
    html += """
        </table>
    </body>
    </html>
    """
    return html

def send_email(to_addrs, html_content):
    """发送邮件
    to_addrs: 收件人邮箱列表
    """
    smtp_server = "smtp.qq.com"
    smtp_port = 587
    from_addr = "652433935@qq.com"
    password = "toepnllhqbfbbffc"
    
    msg = MIMEMultipart()
    msg['From'] = Header(from_addr)
    msg['To'] = Header(','.join(to_addrs))  # 多个收件人用逗号分隔
    msg['Subject'] = Header('MACD策略回测结果', 'utf-8')
    
    msg.attach(MIMEText(html_content, 'html', 'utf-8'))
    
    try:
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(from_addr, password)
        server.sendmail(from_addr, to_addrs, msg.as_string())
        server.quit()
        print("邮件发送成功！")
    except Exception as e:
        print(f"邮件发送失败: {str(e)}")

if __name__ == "__main__":
    # 定义股票池和名称映射
    stock_names = {
        '002351.SZ': '漫步者',
        '000625.SZ': '长安汽车',
        '000550.SZ': '江铃汽车',
        '600737.SH': '中粮糖业',
        '002561.SZ': '徐家汇',
        '001979.SZ': '招商蛇口',
        '003000.SZ': '劲仔食品',
        '603728.SH': '鸣志电器',
        '603583.SH': '捷昌驱动',
        '002270.SZ': '华明装备',
        '300762.SZ': '上海瀚讯',
        '600919.SH': '江苏银行',
        '605111.SH': '新洁能',  
        "002690.SZ":"美亚光电",
        "300776.SZ":"帝尔激光",
        "000415.SZ":"渤海租赁",
        "688271.SH":"联影医疗",
        "000999.SZ":"华润三九",
        "688411.SH":"海博思创",
        "300001.SZ":"特锐德"
    }
    
    stock_pool = list(stock_names.keys())
    
    # 动态计算一年的回测区间
    start_date, end_date = get_date_range(365)
    print(f"回测区间: {start_date} 至 {end_date}")
    
    print("开始分析股票池...\n")
    results = analyze_stock_pool(stock_pool, start_date, end_date, min_return=40, profit_loss_ratio=1.5)
    
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


