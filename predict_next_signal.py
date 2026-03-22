
import pandas as pd


class predict_next_signal_class:

    def predict_next_signal(self,df):
        trade_df = df[df["trade_date"].notna()].copy().reset_index(drop=True)
        if len(trade_df) < 2:
            raise ValueError("数据不足，无法预测下一交易日信号")

        next_row = {col: pd.NA for col in trade_df.columns}
        next_row["trade_date"] = f"{trade_df['trade_date'].iloc[-1]}_NEXT"
        next_row["signal"] = 0
        next_row["reason"] = "观望"
        preview_df = pd.concat([trade_df, pd.DataFrame([next_row])], ignore_index=True)
        preview_df = self.trading_strategy(preview_df)

        next_signal = preview_df["signal"].iloc[-1]
        next_reason = preview_df["reason"].iloc[-1]
        last_dif = preview_df["dif"].iloc[-2]
        last_dea = preview_df["dea"].iloc[-2]
        last_trade_date = preview_df["trade_date"].iloc[-2]
        
        # 确定信号和建议
        if next_signal == 1:
            signal = "买入"
        elif next_signal == -1:
            signal = "卖出"
        else:
            signal = "观望"
            if last_dif > last_dea:
                next_reason = "MACD处于多头区间，DIF在DEA上方，建议持股待涨"
            else:
                next_reason = "MACD处于空头区间，DIF在DEA下方，建议观望等待"
        
        return {
            'signal': signal,
            'reason': next_reason,
            'dif': last_dif,
            'dea': last_dea,
            'macd': (last_dif - last_dea) * 2,
            'last_trade_date': last_trade_date
        }
