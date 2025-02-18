import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import tushare as ts
import os
import pickle
import time
import smtplib
from email.mime.text import MIMEText
from email.header import Header

# 添加缓存目录配置
CACHE_DIR = 'cache/limit_down'
if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)

def save_to_cache(filename, data):
    """保存数据到缓存文件"""
    filepath = os.path.join(CACHE_DIR, filename)
    with open(filepath, 'wb') as f:
        pickle.dump(data, f)

def load_from_cache(filename):
    """从缓存文件加载数据"""
    filepath = os.path.join(CACHE_DIR, filename)
    if os.path.exists(filepath):
        with open(filepath, 'rb') as f:
            return pickle.load(f)
    return None

def analyze_limit_down_trend(start_date, end_date, pro):
    """分析跌停板趋势并获取基础数据"""
    try:
        # 获取上证指数数据
        df_sh = pro.fund_daily(ts_code='512100.SH', 
                              start_date=start_date,
                              end_date=end_date,
                              fields='trade_date,open,high,low,close')
        
        df_sh['amplitude'] = df_sh['open']
        df_sh['trade_date'] = pd.to_datetime(df_sh['trade_date'])
        df_sh = df_sh.set_index('trade_date')
        
        # 获取交易日历
        trade_cal = pro.trade_cal(start_date=start_date, end_date=end_date)
        trade_dates = trade_cal[trade_cal['is_open'] == 1]['cal_date'].sort_values().tolist()
        
        # 存储每日跌停数据
        daily_stats = []
        
        for trade_date in trade_dates:
            today = datetime.now().strftime('%Y%m%d')
            cache_filename = f"limit_down_{trade_date}.pkl"
            
            # 如果是当天的数据，直接获取不缓存
            if trade_date == today:
                try:
                    df_down = pro.kpl_list(trade_date=trade_date, tag='跌停', 
                                         fields='ts_code,name,trade_date,tag,status')
                    if df_down is None or len(df_down) == 0:
                        print(f"警告: {trade_date} 的跌停数据尚未更新")
                        continue
                    
                    data = {
                        'trade_date': trade_date,
                        'limit_down_count': len(df_down)
                    }
                    daily_stats.append(data)
                    print(f"获取 {trade_date} 的实时数据")
                    time.sleep(1)
                except Exception as e:
                    print(f"获取 {trade_date} 数据失败: {str(e)}")
                    continue
            else:
                # 历史数据使用缓存
                cached_data = load_from_cache(cache_filename)
                if cached_data is not None:
                    daily_stats.append(cached_data)
                else:
                    try:
                        df_down = pro.kpl_list(trade_date=trade_date, tag='跌停', 
                                             fields='ts_code,name,trade_date,tag,status')
                        time.sleep(1)
                        
                        data = {
                            'trade_date': trade_date,
                            'limit_down_count': len(df_down)
                        }
                        save_to_cache(cache_filename, data)
                        daily_stats.append(data)
                        print(f"获取并缓存 {trade_date} 的数据")
                    except Exception as e:
                        print(f"获取 {trade_date} 数据失败: {str(e)}")
                        continue
        
        # 转换为DataFrame
        df = pd.DataFrame(daily_stats)
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        df = df.set_index('trade_date')
        
        # 计算移动平均
        df['ma5'] = df['limit_down_count'].rolling(window=5).mean()
        df['ma10'] = df['limit_down_count'].rolling(window=10).mean()
        df['trend'] = df['ma5'] - df['ma10']
        
        # 合并上证指数数据
        df = pd.merge(df, df_sh[['amplitude']], left_index=True, right_index=True, how='left')
        
        return {'data': df}
        
    except Exception as e:
        print(f"分析过程出错: {str(e)}")
        return None

def check_trading_signals(df, i, position):
    """检查买卖信号
    Args:
        df: DataFrame, 包含交易数据
        i: int, 当前索引
        position: bool, 当前持仓状态
    Returns:
        dict: 包含信号类型和相关信息
    """
    current_count = df['limit_down_count'].iloc[i]
    prev_count = df['limit_down_count'].iloc[i-1]
    prev_prev_count = df['limit_down_count'].iloc[i-2]
    ma5 = df['limit_down_count'].rolling(window=5).mean().iloc[i]
    ma10 = df['limit_down_count'].rolling(window=10).mean().iloc[i]
    trend = ma5 - ma10
    recent_max = df['limit_down_count'].rolling(window=15).max().iloc[i]
    
    # 计算连续变化天数
    if current_count < prev_count and prev_count < prev_prev_count:
        consecutive_decrease = 2
    elif current_count < prev_count:
        consecutive_decrease = 1
    else:
        consecutive_decrease = 0
        
    if current_count > prev_count and prev_count > prev_prev_count:
        consecutive_increase = 2
    elif current_count > prev_count:
        consecutive_increase = 1
    else:
        consecutive_increase = 0
    
    # 买入信号判断
    if (not position and 
        current_count >= recent_max * 0.7 and  # 接近近期高点
        consecutive_decrease >= 1  and  # 连续下降
        trend > 0 and 
        ma5 > ma10
        ):
        return {
            'signal': 'buy',
            'current_count': current_count,
            'prev_count': prev_count,
            'prev_prev_count': prev_prev_count
        }
    
    # 卖出信号判断
    elif (position and 
          consecutive_increase >= 2 and  # 连续上升
          trend < 0 and 
          ma5 < ma10):
        return {
            'signal': 'sell',
            'current_count': current_count,
            'prev_count': prev_count,
            'prev_prev_count': prev_prev_count
        }
    
    return {'signal': None}

def calculate_returns(df):
    """计算策略收益"""
    initial_capital = 1000000
    current_capital = initial_capital
    position = False
    trades = []
    daily_values = []
    buy_signal = False
    sell_signal = False
    entry_price = 0
    consecutive_decrease = 0  # 记录连续下降的天数
    consecutive_increase = 0  # 记录连续上升的天数
    
    # 计算每日信号
    for i in range(30, len(df)):
        date = df.index[i]
        current_count = df['limit_down_count'].iloc[i]
        current_price = df['amplitude'].iloc[i]
        
        # 如果持仓，根据指数变化更新资金
        if position and i > 0:
            prev_price = df['amplitude'].iloc[i-1]
            daily_return = (current_price - prev_price) / (prev_price)
            current_capital = current_capital * (1 + daily_return)
        
        signal = check_trading_signals(df, i, position)
        
        # 检测买入信号
        if signal['signal'] == 'buy':
            buy_signal = True
            signal_count = signal['current_count']
            signal_prev_count = signal['prev_count']
            signal_prev_prev_count = signal['prev_prev_count']
            continue
            
        # 执行买入操作
        if buy_signal and not position:
            position = True
            entry_price = df['amplitude'].iloc[i]
            buy_signal = False
            trades.append({
                'date': date,
                'action': '买入',
                'price': entry_price,
                'capital': current_capital,
                'reason': f'跌停数量开始下降: {signal_prev_count}->{signal_count}'
            })
        
        # 检测卖出信号：移除对近期低点的判断，只关注连续上升趋势
        elif signal['signal'] == 'sell':
            sell_signal = True
            signal_count = signal['current_count']
            signal_prev_count = signal['prev_count']
            signal_prev_prev_count = signal['prev_prev_count']
            continue
            
        # 执行卖出操作
        elif sell_signal and position:
            position = False
            exit_price = df['amplitude'].iloc[i]
            # 不需要在这里计算returns，因为资金已经在每日更新了
            sell_signal = False
            trades.append({
                'date': date,
                'action': '卖出',
                'price': exit_price,
                'capital': current_capital,
                'reason': f'跌停数量连续上升: {signal_prev_prev_count}->{signal_prev_count}->{signal_count}'
            })
        
        # 每日净值记录
        daily_values.append({
            'date': date,
            'capital': current_capital,
            'position': '持仓' if position else '空仓',
            'price': current_price
        })
    
    # 计算策略指标
    returns = pd.DataFrame(daily_values)
    returns = returns.set_index('date')
    returns['returns'] = returns['capital'].pct_change()
    
    # 计算年化收益率
    total_days = (returns.index[-1] - returns.index[0]).days
    annual_return = (returns['capital'].iloc[-1] / initial_capital) ** (365 / total_days) - 1
    
    # 计算最大回撤
    returns['max_capital'] = returns['capital'].expanding().max()
    returns['drawdown'] = (returns['max_capital'] - returns['capital']) / returns['max_capital']
    max_drawdown = returns['drawdown'].max()
    print(trades)
    return {
        'trades': pd.DataFrame(trades),
        'returns': returns,
        'annual_return': annual_return,
        'max_drawdown': max_drawdown,
        'final_capital': current_capital,
        'total_return': (current_capital - initial_capital)/initial_capital
    }

def check_tomorrow_signal(df):
    """分析明天的买卖信号"""
    i = len(df) - 1  # 最新一天的索引
    position = False  # 假设当前无持仓
    
    # 获取信号
    signal = check_trading_signals(df, i, position)
    
    print("\n=== 明日交易信号分析 ===")
    print(f"当前日期: {df.index[-1].strftime('%Y-%m-%d')}")
    print(f"\n最近三天跌停数量变化:")
    print(f"{df['limit_down_count'].iloc[-3]:.0f} -> {df['limit_down_count'].iloc[-2]:.0f} -> {df['limit_down_count'].iloc[-1]:.0f}")
    
    # 打印技术指标
    print(f"\n技术指标:")
    print(f"MA5: {df['ma5'].iloc[-1]:.2f}")
    print(f"MA10: {df['ma10'].iloc[-1]:.2f}")
    print(f"趋势(MA5-MA10): {df['trend'].iloc[-1]:.2f}")
    print(f"15日最高跌停数: {df['limit_down_count'].rolling(window=15).max().iloc[-1]:.0f}")
    
    # 输出信号
    if signal['signal'] == 'buy':
        print("\n🟢 明日买入信号")
        print(f"原因: 跌停数量从{signal['prev_count']}降至{signal['current_count']}")
    elif signal['signal'] == 'sell':
        print("\n🔴 明日卖出信号")
        print(f"原因: 跌停数量连续上升 {signal['prev_prev_count']}->{signal['prev_count']}->{signal['current_count']}")
    else:
        print("\n⚪️ 明日无交易信号")

def format_signal_report(df, signal, backtest_result):
    """格式化信号报告"""
    report = []
    report.append("=== 市场信号分析报告 ===")
    report.append(f"分析日期: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    report.append(f"最新交易日: {df.index[-1].strftime('%Y-%m-%d')}")
    
    # 添加跌停数据分析 
    report.append("\n【跌停数据分析】")
    report.append(f"最近三天跌停数量变化: {df['limit_down_count'].iloc[-3]:.0f} -> {df['limit_down_count'].iloc[-2]:.0f} -> {df['limit_down_count'].iloc[-1]:.0f}")
    report.append(f"5日均线: {df['ma5'].iloc[-1]:.2f}")
    report.append(f"10日均线: {df['ma10'].iloc[-1]:.2f}")
    report.append(f"趋势指标(MA5-MA10): {df['trend'].iloc[-1]:.2f}")
    report.append(f"15日最高跌停数: {df['limit_down_count'].rolling(window=15).max().iloc[-1]:.0f}")
    
    # 添加策略表现
    report.append("\n【策略表现】")
    report.append(f"总收益率: {backtest_result['total_return'] / 1000000:.2%}")
    report.append(f"年化收益率: {backtest_result['annual_return']:.2%}")
    report.append(f"最大回撤: {backtest_result['max_drawdown']:.2%}")
    
    # 添加交易信号
    report.append("\n【明日交易信号】")
    if None == signal:
        report.append("⚪️ 无明确交易信号")
    elif signal['signal'] == 'buy':
        report.append("🟢 买入信号")
        report.append(f"理由:")
        report.append(f"1. 跌停数量从{signal['prev_count']}降至{signal['current_count']}")
        report.append(f"2. MA5 > MA10，趋势向上")
        report.append(f"3. 跌停数量接近近期高点")
    elif signal['signal'] == 'sell':
        report.append("🔴 卖出信号")
        report.append(f"理由:")
        report.append(f"1. 跌停数量连续上升: {signal['prev_prev_count']}->{signal['prev_count']}->{signal['current_count']}")
        report.append(f"2. MA5 < MA10，趋势向下")
    else:
        report.append("⚪️ 无明确交易信号")
    
    return "\n".join(report)

def send_email_signal(report):
    """发送邮件"""
    try:
        sender = '652433935@qq.com'
        receiver = '652433935@qq.com'
        smtp_server = 'smtp.qq.com'
        smtp_port = 587
        username = '652433935@qq.com'
        password = 'toepnllhqbfbbffc'  # QQ邮箱授权码
        
        message = MIMEText(report, 'plain', 'utf-8')
        message['From'] = Header(sender)
        message['To'] = Header(receiver)
        subject = f'市场信号分析报告 - {datetime.now().strftime("%Y-%m-%d")}'
        message['Subject'] = Header(subject)
        
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(username, password)
        server.sendmail(sender, [receiver], message.as_string())
        server.quit()
        print("邮件发送成功")
    except Exception as e:
        print(f"邮件发送失败: {str(e)}")

def main():
    # 设置tushare token
    ts.set_token('c0f992e8369579bfec7bf8481dc0bcc304ac66ab5b1dd12c1d154325')
    pro = ts.pro_api()
    
    # 设置时间范围（近3个月数据）
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
    
    # 分析跌停板趋势
    result = analyze_limit_down_trend(start_date, end_date, pro)
    if result and result['data'] is not None:
        data = result['data']
        # 回测
        backtest_result = calculate_returns(data)
        print(backtest_result)
        # 检查是否有今天的数据
        today = datetime.now().strftime('%Y-%m-%d')
        report = []
        if data.index[-1].strftime('%Y-%m-%d') != today:
            report.append(f"\n⚠️ 警告: 今日 ({today}) 的数据尚未更新,暂不提供交易信号，请稍后重试")
            report = "\n".join(report)
            print(f"\n⚠️ 警告: 今日 ({today}) 的数据尚未更新")
            print("暂不提供交易信号，请稍后重试")
           # send_email_signal(report)
        return
        signal = check_tomorrow_signal(data)
        report = format_signal_report(data, signal, backtest_result)
        print(report)
        
        # 发送邮件
        # send_email_signal(report)

if __name__ == "__main__":
    main()
