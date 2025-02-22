import tushare as ts
import pandas as pd
from predict_next_signal import predict_next_signal_class

# Initialize Tushare with your token
ts.set_token('c0f992e8369579bfec7bf8481dc0bcc304ac66ab5b1dd12c1d154325')
pro = ts.pro_api()

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

def calculate_rsi(df, period=14):
    """计算RSI指标"""
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    df['rsi'] = 100 - (100 / (1 + rs))
    return df

class macd_with_deepdown(predict_next_signal_class):

    def trading_strategy(self,df):
        """修改后的交易策略，使用买入时20日低点作为止损"""
        df = calculate_macd(df)
        df = calculate_rsi(df)
        df['signal'] = 0

        # 参数设置
        high_open_threshold = 0.02  # 高开阈值
        dif_dea_gap_threshold = -0.2  # DIF低于DEA的阈值
        dif_dea_high_threshold = 0.2  # DIF高于DEA的阈值
        lookback_period = 5  # 回看周期
        ma_period = 20  # 20日低点参数
        holding_period = 20  # 持仓观察期

        # 计算20日低点
        df['lowest_20d'] = df['low'].rolling(window=ma_period).min()
        last_buy_index = None  # 记录上次买入位置
        buy_reference_low = None  # 记录买入时的20日低点

        for i in range(26, len(df)-1):
            # 计算开盘涨幅
            open_change = (df['open'].iloc[i] - df['close'].iloc[i-1]) / df['close'].iloc[i-1]

            # 检查前期是否存在DIF显著低于DEA的情况
            previous_gaps = [df['dif'].iloc[j] - df['dea'].iloc[j] 
                            for j in range(i-lookback_period, i)]
            min_gap = min(previous_gaps)

            # 买入条件：金叉且之前DIF大幅低于DEA，同时不是大幅高开
            if (df['dif'].iloc[i-1] < df['dea'].iloc[i-1] and 
                df['dif'].iloc[i] > df['dea'].iloc[i] and
                min_gap < dif_dea_gap_threshold and 
                open_change <= high_open_threshold):

                df.loc[i+1, 'signal'] = 1
                df.loc[i+1, 'reason'] = "金叉且之前DIF大幅低于DEA"
                last_buy_index = i + 1
                buy_reference_low = df['lowest_20d'].iloc[i]  # 记录买入时的20日低点

            # 卖出条件：
            # 1. 如果在观察期内（20天），只有跌破买入时的20日低点才卖出
            # 2. 如果超过观察期，则使用当前的20日低点作为参考
            # 3. DIF大幅高于DEA时可以获利了结
            elif last_buy_index is not None:
                days_since_buy = i - last_buy_index
                current_low = df['low'].iloc[i]

                if ((days_since_buy <= holding_period and current_low < buy_reference_low) or
                    (days_since_buy > holding_period and current_low < df['lowest_20d'].iloc[i]) or
                    (df['dif'].iloc[i] - df['dea'].iloc[i] > dif_dea_high_threshold)):

                    sell_type = ('跌破买入时20日低点' if days_since_buy <= holding_period 
                               else '跌破当前20日低点' if current_low < df['lowest_20d'].iloc[i]
                               else 'DIF大幅高于DEA')

                    df.loc[i+1, 'signal'] = -1
                    df.loc[i+1, 'reason'] = sell_type
                    last_buy_index = None  # 重置买入记录
            else:
                # 这边是为了处理金叉之后的观望，如果不增加下一天的信号，则predict_next_signal会不准确
                df.loc[i+1, 'signal'] = 0
                df.loc[i+1, 'reason'] = "观望"

        return df
  
