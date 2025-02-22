import tushare as ts
import pandas as pd

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

class macd_with_optimize_sell_strategy:
    def __init__(self, take_profit_pct=0.10, stop_loss_pct=0.05):
        """
        初始化策略参数
        
        Parameters:
        - take_profit_pct: 止盈百分比，默认10%
        - stop_loss_pct: 止损百分比，默认5%
        """
        self.take_profit_pct = take_profit_pct
        self.stop_loss_pct = stop_loss_pct

    def trading_strategy(self, df):
        """
        修正未来函数的交易策略，增加止盈止损功能
        """
        df = calculate_macd(df)
        df = calculate_rsi(df)
        df['signal'] = 0
        df['reason'] = ""
        
        death_cross_threshold = 0.02
        in_position = False
        entry_price = None

        # 从第二个数据开始，为下一个交易日生成信号
        for i in range(26, len(df)):

            
            if(df['trade_date'].iloc[i] == '20241113'):
                print('here')
            # 如果持仓中，检查止盈止损
            if in_position:
                # 收盘价做计算
                current_price = df['close'].iloc[i]
                if entry_price is not None and current_price is not None:
                    price_change = (current_price - entry_price) / entry_price
                    # 修正一下涨停版
                    if price_change >= 0.098:
                        price_change = 0.1
                    
                    # 触发止损
                    if price_change <= -self.stop_loss_pct:
                        df.loc[i+1, 'signal'] = -1
                        df.loc[i+1, 'reason'] = f"触发止损信号，亏损达到{self.stop_loss_pct*100}%，建议开盘卖出"
                        in_position = False
                        entry_price = None
                        continue
                    
                    # 触发止盈
                    if price_change >= self.take_profit_pct:
                        df.loc[i+1, 'signal'] = -1
                        df.loc[i+1, 'reason'] = f"触发止盈信号，收益达到{self.take_profit_pct*100}%，建议明日开盘卖出"
                        in_position = False
                        entry_price = None
                        continue

            # 计算60日内的高点
            high_60 = df['close'].iloc[max(0, i-60):i+1].max()

            # MACD金叉：当天形成金叉，第二天发出买入信号
            if (df['dif'].iloc[i-1] < df['dea'].iloc[i-1] and 
                df['dif'].iloc[i] > df['dea'].iloc[i]):  # 当前价格低于60日高点的95%
                df.loc[i+1, 'signal'] = 1
                entry_price = df['open'].iloc[i+1]  # 记录买入价格
                df.loc[i+1, 'reason'] = "今日形成MACD金叉，建议明日开盘买入"
                in_position = True

            # 提前卖出信号
            elif (df['dif'].iloc[i] > df['dea'].iloc[i] and 
                  (df['dif'].iloc[i] - df['dea'].iloc[i]) < death_cross_threshold and 
                  df['dif'].iloc[i] < df['dif'].iloc[i-1] and
                  in_position):
                df.loc[i+1, 'signal'] = -1
                df.loc[i+1, 'reason'] = "提前卖出信号，建议明日开盘卖出"
                in_position = False
                entry_price = None

            # MACD死叉
            elif (df['dif'].iloc[i-1] > df['dea'].iloc[i-1] and 
                  df['dif'].iloc[i] < df['dea'].iloc[i] and
                  in_position):
                df.loc[i+1, 'signal'] = -1
                df.loc[i+1, 'reason'] = "今日形成MACD死叉，建议明日开盘卖出"
                in_position = False
                entry_price = None
            else:
                # 这边是为了处理金叉之后的观望，如果不增加下一天的信号，则predict_next_signal会不准确
                df.loc[i+1, 'signal'] = 0
                df.loc[i+1, 'reason'] = "观望"

        return df

    def predict_next_signal(self, df):
        # 获取最后一个信号和原因
        last_signal = df['signal'].iloc[-1]
        last_reason = df['reason'].iloc[-1]
        last_dif = df['dif'].iloc[-2]
        last_dea = df['dea'].iloc[-2]

        # 获取上一个交易日
        last_trade_date = df['trade_date'].iloc[-2]
        
        # 确定信号和建议
        if last_signal == 1:
            signal = "买入"
        elif last_signal == -1:
            signal = "卖出"
        else:
            signal = "观望"
            if last_dif > last_dea:
                last_reason = "MACD处于多头区间，DIF在DEA上方，建议持股待涨"
            else:
                last_reason = "MACD处于空头区间，DIF在DEA下方，建议观望等待"
        
        return {
            'signal': signal,
            'reason': last_reason,
            'dif': last_dif,
            'dea': last_dea,
            'macd': (last_dif - last_dea) * 2,
            'last_trade_date': last_trade_date
        }