import tushare as ts
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from datetime import datetime, timedelta
import os
import time
import pickle
import smtplib
from email.mime.text import MIMEText
from email.header import Header
from sktime.classification.interval_based import TimeSeriesForestClassifier
from sklearn.ensemble import GradientBoostingClassifier  # 添加到文件顶部的导入部分

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
        # 获取交易日历并按日期正序排列
        trade_cal = pro.trade_cal(start_date=start_date, end_date=end_date)
        trade_dates = trade_cal[trade_cal['is_open'] == 1]['cal_date'].sort_values(ascending=False).tolist()
        
        # 打印日期范围信息用于调试
        print(f"获取数据的日期范围: {start_date} 至 {end_date}")
        print(f"最新交易日: {trade_dates[0]}")
        print(f"最早交易日: {trade_dates[-1]}")
        
        # 获取中小盘指数数据（中证500指数）
        df_index = pro.index_daily(ts_code='000852.SH', 
                                 start_date=start_date, 
                                 end_date=end_date)
        
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
            # else:
            #     print(f"从缓存中加载 {trade_date} 的涨跌停数据")
            
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
    # 按照时间升序排序指数数据
    df_index['trade_date'] = pd.to_datetime(df_index['trade_date'])
    df_index = df_index.sort_values('trade_date')
    df_index['trade_date'] = df_index['trade_date'].dt.strftime('%Y%m%d')
    df_index = df_index.set_index('trade_date')
    
    # 按照时间升序排序涨跌停数据
    daily_stats.index = pd.to_datetime(daily_stats.index)
    daily_stats = daily_stats.sort_index()
    daily_stats.index = daily_stats.index.strftime('%Y%m%d')
    
    # 合并数据
    df = df_index.merge(daily_stats, left_index=True, right_index=True)
    
    # 打印调试信息
    print(f"数据时间范围: {df.index[0]} 至 {df.index[-1]}")
    print(f"数据条数: {len(df)}")
    
    # 计算指数涨跌幅，并将NaN填充为0
    df['index_return'] = df['close'].pct_change().fillna(0)
    
    # 添加更多特征，处理除零情况
    total_limits = df['up_limit'] + df['down_limit']
    df['up_limit_ratio'] = (df['up_limit'] / total_limits).fillna(0)
    df['down_limit_ratio'] = (df['down_limit'] / total_limits).fillna(0)
    
    # 创建特征
    feature_cols = []
    for i in range(window):
        # 使用shift创建特征并填充NaN为0
        df[f'up_limit_t{i+1}'] = df['up_limit'].shift(i+1).fillna(0)
        df[f'down_limit_t{i+1}'] = df['down_limit'].shift(i+1).fillna(0)
        df[f'broken_limit_t{i+1}'] = df['broken_limit'].shift(i+1).fillna(0)
        df[f'pure_limit_t{i+1}'] = df['pure_limit'].shift(i+1).fillna(0)
        df[f'avg_break_times_t{i+1}'] = df['avg_break_times'].shift(i+1).fillna(0)
        
        feature_cols.extend([
            f'up_limit_t{i+1}', 
            f'down_limit_t{i+1}',
            f'broken_limit_t{i+1}',
            f'pure_limit_t{i+1}',
            f'avg_break_times_t{i+1}'
        ])
    
    # 创建目标变量，将NaN填充为0
    df['target'] = (df['index_return'].shift(-1) > 0).astype(int).fillna(0)
    
    return df, feature_cols

def train_model(df, feature_cols):
    """使用TimeSeriesForest模型进行时间序列预测"""
    X = df[feature_cols]
    y = df['target']
    
    # 按时间顺序分割训练集和测试集
    train_size = int(len(df) * 0.8)
    X_train = X[:train_size]
    X_test = X[train_size:]
    y_train = y[:train_size]
    y_test = y[train_size:]
    
    # 特征标准化
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    # 构建TimeSeriesForest模型
    model = TimeSeriesForestClassifier(
        n_estimators=100,
        random_state=42,
        n_jobs=1  # 单线程运行
    )
    
    # 训练模型
    print("开始训练模型...")
    model.fit(X_train_scaled, y_train)
    print("模型训练完成")
    
    # 评估模型
    train_score = model.score(X_train_scaled, y_train)
    test_score = model.score(X_test_scaled, y_test)
    
    return model, scaler, train_score, test_score

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

def find_optimal_days():
    """寻找最优的历史数据天数"""
    best_score = 0
    best_days = 30
    best_model = None
    best_scaler = None
    results = []
    
    # 测试不同的天数
    for days in range(90, 180, 5):  # 从30天到90天，每次增加3天
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y%m%d')
        
        print(f"\n测试 {days} 天的数据:")
        # 获取数据
        df_index, daily_stats = get_market_data(start_date, end_date)
        if df_index is None or daily_stats is None:
            continue
            
        # 准备特征
        df, feature_cols = prepare_features(df_index, daily_stats)
        
        # 训练模型
        model, scaler, train_score, test_score = train_model(df, feature_cols)
        
        print(f"天数: {days}")
        print(f"训练集准确率: {train_score:.4f}")
        print(f"测试集准确率: {test_score:.4f}")
        
        results.append({
            'days': days,
            'train_score': train_score,
            'test_score': test_score
        })
        
        # 更新最佳模型
        if test_score > best_score:
            best_score = test_score
            best_days = days
            best_model = model
            best_scaler = scaler
    
    # 打印所有结果
    results_df = pd.DataFrame(results)
    print("\n全部测试结果:")
    print(results_df.sort_values('test_score', ascending=False))
    
    print(f"\n最优天数: {best_days}")
    print(f"最优测试集准确率: {best_score:.4f}")
    
    return best_days, best_model, best_scaler, best_score

def main():
    # 寻找最优天数
    best_days, model, scaler, best_score = find_optimal_days()
    
    # 使用最优天数重新获取数据进行预测
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=best_days)).strftime('%Y%m%d')
    
    # 获取数据
    df_index, daily_stats = get_market_data(start_date, end_date)
    df, feature_cols = prepare_features(df_index, daily_stats)
    
    # 使用最优模型进行预测
    try:
        # 获取最新数据
        latest_data = df[feature_cols].iloc[-1:].values
        latest_data_scaled = scaler.transform(latest_data)
        
        # 确保日期格式正确
        latest_date = df.index[-1]
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
            prediction_tomorrow = model.predict(latest_data_scaled)
            probability_tomorrow = model.predict_proba(latest_data_scaled)
            
            # 后天的预测
            day_after = tomorrow + timedelta(days=1)
            day_after_date = day_after.strftime('%Y%m%d')
            prediction_day_after = model.predict(latest_data_scaled)
            probability_day_after = model.predict_proba(latest_data_scaled)
            
            report += "\n=== 中小盘股市场预测结果 ===\n"
            report += f"\n预测基准日期: {latest_date}\n"
            report += f"测试集准确率: {best_score:.4f}\n"
            
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
            print(e)
            report += f"日期格式错误: {e}\n"
            report += f"当前日期值: {latest_date}\n"
            report += "请确保日期格式为'YYYYMMDD'\n"
        except Exception as e:
            report += f"发生未知错误: {e}\n"
        print(report)
        # 发送邮件
        # send_email(report)
    
    except Exception as e:
        print(f"发生错误: {e}")

if __name__ == "__main__":
    main()
