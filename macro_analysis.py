import tushare as ts
import pandas as pd

def get_macro_data():
    # 设置tushare的token
    ts.set_token('c0f992e8369579bfec7bf8481dc0bcc304ac66ab5b1dd12c1d154325')
    pro = ts.pro_api()

    # 获取宏观经济数据，例如GDP
    gdp_data = pro.cn_gdp()

    # 获取其他宏观经济数据，例如CPI
    cpi_data = pro.cn_cpi()

    # 获取PMI数据
    pmi_data = pro.cn_pmi()

    # 你可以根据需要获取更多的宏观经济数据
    # ...

    return gdp_data, cpi_data, pmi_data

def analyze_macro_data(gdp_data, cpi_data, pmi_data):
    # 简单分析宏观数据，判断利好哪些行业
    # 这里只是一个示例，你可以根据实际需求进行更复杂的分析

    latest_gdp_growth = gdp_data['gdp_yoy'].iloc[-1]
    latest_cpi_growth = cpi_data['nt_yoy'].iloc[-1]
    latest_pmi = pmi_data['PMI010000'].iloc[-1]

    if latest_gdp_growth > 5:
        print("GDP增长较快，利好消费、科技等行业")
    else:
        print("GDP增长较慢，利好防御性行业如公用事业")

    if latest_cpi_growth < 2:
        print("CPI较低，利好消费品行业")
    else:
        print("CPI较高，利好原材料行业")

    if latest_pmi > 50:
        print("PMI高于50，表明制造业扩张，利好工业和制造业")
    else:
        print("PMI低于50，表明制造业收缩，利好服务业")

if __name__ == "__main__":
    gdp_data, cpi_data, pmi_data = get_macro_data()
    analyze_macro_data(gdp_data, cpi_data, pmi_data)