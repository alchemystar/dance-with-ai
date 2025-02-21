import tushare as ts
import pandas as pd

def get_all_stocks_info():
    # 设置tushare的token
    ts.set_token('c0f992e8369579bfec7bf8481dc0bcc304ac66ab5b1dd12c1d154325')
    pro = ts.pro_api()

    # 获取所有A股股票信息
    all_stocks = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,market,list_date')

    # 将数据写入本地文件进行缓存
    all_stocks.to_csv('all_stocks_info.csv', index=False)
    print("All A-shares stock information has been written to all_stocks_info.csv")

if __name__ == "__main__":
    get_all_stocks_info()