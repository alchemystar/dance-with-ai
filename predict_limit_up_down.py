import tushare as ts
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from datetime import datetime, timedelta
import os
import time
import pickle
import smtplib
from email.mime.text import MIMEText
from email.header import Header

# 设置 tushare token
ts.set_token('c0f992e8369579bfec7bf8481dc0bcc304ac66ab5b1dd12c1d154325')
pro = ts.pro_api()

CACHE_DIR = 'cache'
if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)

def save_to_cache(filename, data):
    with open(os.path.join(CACHE_DIR, filename), 'wb') as f:
        pickle.dump(data, f)

def load_from_cache(filename):
    filepath = os.path.join(CACHE_DIR, filename)
    if os.path.exists(filepath):
        with open(filepath, 'rb') as f:
            return pickle.load(f)
    return None

def get_daily_limit_data(trade_date, fields):
    """获取单日涨跌停数据"""
    return pro.limit_list_d(trade_date=trade_date, fields=fields)

def get_limit_ths_data(start_date, end_date, fields):
    """获取涨跌停数据"""
    return pro.limit_list_ths(
        start_date=start_date,
        end_date=end_date,
        fields=','.join(fields)
    )

def get_market_data(start_date, end_date):
    """获取市场数据，包括中小盘指数和涨跌停炸板数据"""
    try:
        # 获取中小盘指数数据（中证500指数）
        df_index = pro.index_daily(ts_code='000852.SH', 
                                 start_date=start_date, 
                                 end_date=end_date)
        
        # 获取交易日历
        trade_cal = pro.trade_cal(start_date=start_date, end_date=end_date)
        trade_dates = trade_cal[trade_cal['is_open'] == 1]['cal_date'].tolist()
        
        all_limit_data = []
        for trade_date in trade_dates:
            cache_filename = f"limit_data_{trade_date}.pkl"
            daily_stat = load_from_cache(cache_filename)
            if daily_stat is None:
                # 获取涨停、炸板和跌停数据
                df_up = pro.kpl_list(trade_date=trade_date, tag='涨停', 
                                   fields='ts_code,name,trade_date,tag,status')
                time.sleep(1)
                df_broken = pro.kpl_list(trade_date=trade_date, tag='炸板', 
                                       fields='ts_code,name,trade_date,tag,status')
                time.sleep(1)
                df_down = pro.kpl_list(trade_date=trade_date, tag='跌停', 
                                     fields='ts_code,name,trade_date,tag,status')
                time.sleep(1)
                
                # 统计当日数据
                daily_stat = {
                    'trade_date': trade_date,
                    'up_limit': len(df_up),             # 涨停家数
                    'down_limit': len(df_down),         # 跌停家数
                    'broken_limit': len(df_broken),     # 炸板家数
                    'pure_limit': len(df_up[df_up['status'].str.contains('首板', na=False)]),  # 首板家数
                    'avg_break_times': len(df_broken) / len(df_up) if len(df_up) > 0 else 0  # 炸板率
                }
                save_to_cache(cache_filename, daily_stat)
                print(f"已获取 {trade_date} 的涨跌停数据并保存到缓存")
            else:
                print(f"从缓存中加载 {trade_date} 的涨跌停数据")

            # 打印每天的涨跌停和炸板数据
            print(f"\n{trade_date} 的数据:")
            print(f"涨停家数: {daily_stat['up_limit']}")
            print(f"跌停家数: {daily_stat['down_limit']}")
            print(f"炸板家数: {daily_stat['broken_limit']}")
            print(f"首板家数: {daily_stat['pure_limit']}")
            print(f"炸板率: {daily_stat['avg_break_times']:.2%}")
            
            all_limit_data.append(daily_stat)
        
        # 转换为DataFrame
        daily_stats = pd.DataFrame(all_limit_data)
        daily_stats = daily_stats.set_index('trade_date')
        
        print(f"成功获取 {start_date} 至 {end_date} 的市场数据")
        print(f"共处理 {len(daily_stats)} 个交易日")
        
        return df_index, daily_stats
        
    except Exception as e:
        print(f"获取数据失败: {str(e)}")
        return None

def prepare_features(df_index, daily_stats, window=7):
    # 确保日期格式正确
    df_index['trade_date'] = pd.to_datetime(df_index['trade_date']).dt.strftime('%Y%m%d')
    df_index = df_index.set_index('trade_date')
    
    # 确保daily_stats的索引也是正确的日期格式
    daily_stats.index = pd.to_datetime(daily_stats.index).strftime('%Y%m%d')
    
    # 合并指数数据和涨跌停数据
    df = df_index.merge(daily_stats, left_index=True, right_index=True)
    
    # 计算指数涨跌幅
    df['index_return'] = df['close'].pct_change()
    
    # 添加更多特征
    df['up_limit_ratio'] = df['up_limit'] / (df['up_limit'] + df['down_limit'])
    df['down_limit_ratio'] = df['down_limit'] / (df['up_limit'] + df['down_limit'])
    
    # 创建特征
    feature_cols = []
    for i in range(window):
        df[f'up_limit_t{i+1}'] = df['up_limit'].shift(i+1)
        df[f'down_limit_t{i+1}'] = df['down_limit'].shift(i+1)
        df[f'broken_limit_t{i+1}'] = df['broken_limit'].shift(i+1)
        df[f'pure_limit_t{i+1}'] = df['pure_limit'].shift(i+1)
        df[f'avg_break_times_t{i+1}'] = df['avg_break_times'].shift(i+1)
        
        feature_cols.extend([
            f'up_limit_t{i+1}', 
            f'down_limit_t{i+1}',
            f'broken_limit_t{i+1}',
            f'pure_limit_t{i+1}',
            f'avg_break_times_t{i+1}'
        ])
    
    # 创建目标变量
    df['target'] = (df['index_return'].shift(-1) > 0).astype(int)
    
    return df.dropna(), feature_cols

def train_model(df, feature_cols):
    X = df[feature_cols]
    y = df['target']
    
    # 分割训练集和测试集
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # 训练随机森林模型
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    
    # 评估模型
    train_score = model.score(X_train, y_train)
    test_score = model.score(X_test, y_test)
    
    return model, train_score, test_score

def send_email(report):
    # 邮件发送配置
    sender = '652433935@qq.com'  # 发件人邮箱
    receiver = '652433935@qq.com'  # 收件人邮箱
    smtp_server = 'smtp.qq.com'  # SMTP服务器
    smtp_port = 587  # SMTP端口
    username = '652433935@qq.com'  # 登录用户名
    password = 'toepnllhqbfbbffc'  # 登录密码或授权码
    
    try:
        # 创建邮件内容
        message = MIMEText(report, 'plain', 'utf-8')
        message['From'] = Header(sender)
        message['To'] = Header(receiver)
        message['Subject'] = Header('股票预测结果')
        
        # 发送邮件
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()  # 使用TLS加密
        server.login(username, password)
        server.sendmail(sender, [receiver], message.as_string())
        server.quit()
        print("邮件发送成功")
    except Exception as e:
        print(f"邮件发送失败: {str(e)}")

def main():
    # 设置时间范围
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=40)).strftime('%Y%m%d')
    
    # 获取数据
    df_index, daily_stats = get_market_data(start_date, end_date)
    
    # 准备特征
    df, feature_cols = prepare_features(df_index, daily_stats)
    
    # 训练模型
    model, train_score, test_score = train_model(df, feature_cols)
    
    print(f"训练集准确率: {train_score:.4f}")
    print(f"测试集准确率: {test_score:.4f}")
    
    # 预测未来两天的市场走势
    latest_data = df[feature_cols].iloc[-1:]
    latest_date = df.index[-1]
    
    # 确保日期格式正确
    if isinstance(latest_date, (np.int64, np.int32, int)):
        latest_date = str(latest_date)
        if len(latest_date) != 8:  # 如果不是YYYYMMDD格式
            print("错误：日期格式不正确")
            return
    
    report = ""
    
    try:
        # 明天的预测
        tomorrow = datetime.strptime(latest_date, '%Y%m%d') + timedelta(days=1)
        tomorrow_date = tomorrow.strftime('%Y%m%d')
        prediction_tomorrow = model.predict(latest_data)
        probability_tomorrow = model.predict_proba(latest_data)
        
        # 后天的预测
        day_after = tomorrow + timedelta(days=1)
        day_after_date = day_after.strftime('%Y%m%d')
        prediction_day_after = model.predict(latest_data)
        probability_day_after = model.predict_proba(latest_data)
        
        report += "\n=== 中小盘股市场预测结果 ===\n"
        report += f"\n预测基准日期: {latest_date}\n"
        report += f"测试集准确率: {test_score:.4f}\n"
        
        report += f"\n明天 ({tomorrow_date}) 预测:\n"
        report += "上涨\n" if prediction_tomorrow[0] == 1 else "下跌\n"
        report += f"上涨概率: {probability_tomorrow[0][1]:.2%}\n"
        report += f"下跌概率: {probability_tomorrow[0][0]:.2%}\n"
        
        report += f"\n后天 ({day_after_date}) 预测:\n"
        report += "上涨\n" if prediction_day_after[0] == 1 else "下跌\n"
        report += f"上涨概率: {probability_day_after[0][1]:.2%}\n"
        report += f"下跌概率: {probability_day_after[0][0]:.2%}\n"
        
        # 买入建议
        report += "\n=== 买入建议 ===\n"
        if (prediction_tomorrow[0] == 1 and probability_tomorrow[0][1] >= 0.6 and
            prediction_day_after[0] == 1 and probability_day_after[0][1] >= 0.55):
            report += "建议买入 ✅\n"
            report += "理由：连续两天看涨，且明天上涨概率较高\n"
        elif prediction_tomorrow[0] == 1 and probability_tomorrow[0][1] >= 0.7:
            report += "可以考虑买入 🤔\n"
            report += "理由：明天强势上涨概率高\n"
        else:
            report += "暂不建议买入 ❌\n"
            report += "理由：上涨概率不确定或趋势不明显\n"
        
        report += "\n风险提示：模型预测仅供参考，请结合其他因素综合判断\n"
        
    except ValueError as e:
        report += f"日期格式错误: {e}\n"
        report += f"当前日期值: {latest_date}\n"
        report += "请确保日期格式为'YYYYMMDD'\n"
    except Exception as e:
        report += f"发生未知错误: {e}\n"
    
    # 发送邮件
    send_email(report)

if __name__ == "__main__":
    main()
