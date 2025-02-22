

class predict_next_signal_class:

    def predict_next_signal(self,df):
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