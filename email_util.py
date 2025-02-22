import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header

def generate_html_table(results, stock_names):
    """生成HTML格式的结果表格"""
    # 将结果分为有信号和无信号两组
    signal_stocks = []
    no_signal_stocks = []
    
    # 分类逻辑
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
            .buy { color: #FF0000; font-weight: bold; }
            .sell { color: #008000; font-weight: bold; }
            .negative { color: #008000; }
            .positive { color: #FF0000; }
        </style>
    </head>
    <body>
        <h2>买卖策略</h2>
        <table>
            <tr>
                <th>股票代码</th>
                <th>股票名称</th>
                <th>明日操作</th>
                <th>最后一个交易日</th>
                <th>操作原因</th>
                <th>总收益率</th>
                <th>年化收益率</th>
                <th>交易次数</th>
                <th>盈利/亏损</th>
                <th>盈亏比</th>
                <th>单次最大收益</th>
                <th>单次最小收益</th>
                <th>整体最大回撤</th>
                <th>平均收益</th>
            </tr>
    """
    
    for result in sorted_results:
        stock_code = result['stock_code']
        stock_name = stock_names[stock_code]
        pred = result['prediction']
        stats = result['stats']
        
        # 根据操作类型设置颜色类名
        signal_class = 'buy' if pred['signal'] == '买入' else 'sell' if pred['signal'] == '卖出' else ''
        
        # 设置最大回撤和最大收益的颜色样式
        max_return_class = 'positive' if stats['max_return'] > 0 else ''
        min_return_class = 'negative' if stats['min_return'] < 0 else ''
        max_drawdown_class = 'negative' if stats['min_return'] < 0 else ''
        
        html += f"""
            <tr>
                <td>{stock_code}</td>
                <td>{stock_name}</td>
                <td class="{signal_class}">{pred['signal']}</td>
                <td>{pred['last_trade_date']}</td>
                <td>{pred['reason']}</td>
                <td>{result['total_return']:.2f}%</td>
                <td>{result['annual_return']*100:.2f}%</td>
                <td>{stats['total_trades']}</td>
                <td>{stats['profitable_trades']}/{stats['loss_trades']}</td>
                <td>{result['profit_loss_ratio']:.2f}</td>
                <td class="{max_return_class}">{stats['max_return']:.2f}%</td>
                <td class="{min_return_class}">{stats['min_return']:.2f}%</td>
                <td class="{max_drawdown_class}">{stats['max_drawdown']:.2f}%</td>
                <td>{stats['avg_return']:.2f}%</td>
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
    msg['Subject'] = Header('买卖策略回测结果', 'utf-8')
    
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
