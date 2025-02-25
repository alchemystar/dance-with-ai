import tushare as ts
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

class BankValuationModel:
    def __init__(self, token):
        """
        初始化模型，设置Tushare API令牌
        """
        ts.set_token(token)
        self.pro = ts.pro_api()
        self.today = datetime.now().strftime('%Y%m%d')
        print("银行股估值模型初始化完成")
    
    def get_bank_stocks(self):
        """
        获取所有银行股的列表
        """
        # 获取所有股票列表
        stocks = self.pro.stock_basic(exchange='', list_status='L')
        # 筛选出银行股（行业为'银行'）
        banks = stocks[stocks['industry'] == '银行']
        print(f"共找到{len(banks)}只银行股")
        return banks
    
    def get_financial_data(self, bank_list):
        """
        获取银行的财务数据
        """
        # 获取最新季度的财务指标数据
        bank_codes = bank_list['ts_code'].tolist()
        
        # 获取最新财报日期
        latest_period = self.pro.income_vip(ts_code=bank_codes[0], fields='ann_date,end_date').iloc[0]['end_date']
        latest_period = latest_period.replace('-', '')
        
        # 基础财务指标
        fin_indicator = pd.DataFrame()
        for code in bank_codes:
            try:
                # 获取财务指标数据
                df = self.pro.fina_indicator(ts_code=code, period=latest_period)
                if not df.empty:
                    fin_indicator = pd.concat([fin_indicator, df])
            except Exception as e:
                print(f"获取{code}财务指标错误: {e}")
                continue
        
        # 资产负债表数据
        balance_sheet = pd.DataFrame()
        for code in bank_codes:
            try:
                # 获取资产负债表
                df = self.pro.balancesheet(ts_code=code, period=latest_period)
                if not df.empty:
                    balance_sheet = pd.concat([balance_sheet, df])
            except Exception as e:
                print(f"获取{code}资产负债表错误: {e}")
                continue
        
        # 利润表数据
        income = pd.DataFrame()
        for code in bank_codes:
            try:
                # 获取利润表
                df = self.pro.income(ts_code=code, period=latest_period)
                if not df.empty:
                    income = pd.concat([income, df])
            except Exception as e:
                print(f"获取{code}利润表错误: {e}")
                continue
                
        # 获取当前股价和市值数据
        daily = pd.DataFrame()
        for code in bank_codes:
            try:
                # 获取最新交易日数据
                df = self.pro.daily_basic(ts_code=code, trade_date=self.today)
                if df.empty:
                    # 如果当天没有数据，获取最近一个交易日的数据
                    trade_cal = self.pro.trade_cal(exchange='SSE', is_open='1', 
                                                 start_date='20230101', end_date=self.today)
                    latest_trade_date = trade_cal.sort_values('cal_date', ascending=False).iloc[0]['cal_date']
                    df = self.pro.daily_basic(ts_code=code, trade_date=latest_trade_date)
                
                if not df.empty:
                    daily = pd.concat([daily, df])
            except Exception as e:
                print(f"获取{code}日线数据错误: {e}")
                continue
        
        return fin_indicator, balance_sheet, income, daily
    
    def calculate_valuation_metrics(self, bank_list, fin_indicator, balance_sheet, income, daily):
        """
        计算银行股估值指标
        """
        # 合并数据
        bank_valuation = bank_list[['ts_code', 'name']].copy()
        
        # 合并财务指标 - 使用实际存在的列
        if not fin_indicator.empty:
            # 检查哪些列实际存在于数据中
            available_cols = ['ts_code']
            
            # 检查并添加可用的财务指标列
            for col in ['roe', 'roa', 'profit_to_gr', 'op_of_gr']:
                if col in fin_indicator.columns:
                    available_cols.append(col)
            
            # 只合并存在的列
            if len(available_cols) > 1:  # 确保至少有ts_code和一个指标
                bank_valuation = pd.merge(bank_valuation, fin_indicator[available_cols], on='ts_code', how='left')
        
        # 合并每日指标数据
        if not daily.empty:
            daily_cols = ['ts_code', 'close', 'pe', 'pb', 'total_mv']
            # 确保只使用实际存在的列
            available_daily_cols = ['ts_code'] + [col for col in daily_cols[1:] if col in daily.columns]
            bank_valuation = pd.merge(bank_valuation, daily[available_daily_cols], on='ts_code', how='left')
        
        # 计算PB、PE分位数
        # 获取历史PB、PE数据（过去3年）
        three_years_ago = (datetime.now().year - 3) * 10000 + 101  # 3年前的1月1日
        
        bank_valuation['pb_percentile'] = 0
        bank_valuation['pe_percentile'] = 0
        
        for idx, row in bank_valuation.iterrows():
            try:
                # 获取历史PB、PE数据
                hist_data = self.pro.daily_basic(ts_code=row['ts_code'], 
                                               start_date=str(three_years_ago), 
                                               end_date=self.today,
                                               fields='trade_date,pb,pe')
                
                if not hist_data.empty:
                    # 计算PB分位数
                    pb_current = row['pb']
                    pb_history = hist_data['pb'].dropna()
                    if len(pb_history) > 0 and not np.isnan(pb_current):
                        pb_percentile = sum(pb_history > pb_current) / len(pb_history) * 100
                        bank_valuation.at[idx, 'pb_percentile'] = pb_percentile
                    
                    # 计算PE分位数
                    pe_current = row['pe']
                    pe_history = hist_data['pe'].dropna()
                    if len(pe_history) > 0 and not np.isnan(pe_current):
                        pe_percentile = sum(pe_history > pe_current) / len(pe_history) * 100
                        bank_valuation.at[idx, 'pe_percentile'] = pe_percentile
            except Exception as e:
                print(f"计算{row['ts_code']}历史分位数错误: {e}")
                continue
        
        # 计算综合估值分数 (简单加权模型)
        # ROE权重0.3，PB分位数权重0.4，PE分位数权重0.3
        bank_valuation['valuation_score'] = (
            bank_valuation['roe'] * 0.3 + 
            bank_valuation['pb_percentile'] * 0.4 + 
            bank_valuation['pe_percentile'] * 0.3
        )
        
        return bank_valuation
    
    def estimate_fair_price(self, results):
        """估计银行股的合理价格"""
        results['fair_pb'] = 0.0
        results['fair_pe'] = 0.0
        results['price_target'] = 0.0
        
        for idx, row in results.iterrows():
            try:
                # 获取历史PB、PE数据
                hist_data = self.pro.daily_basic(ts_code=row['ts_code'], 
                                               start_date=str((datetime.now().year - 3) * 10000 + 101), 
                                               end_date=self.today,
                                               fields='trade_date,pb,pe')
                
                if not hist_data.empty:
                    # 计算历史中位数PB和PE
                    pb_median = hist_data['pb'].dropna().median()
                    pe_median = hist_data['pe'].dropna().median()
                    
                    # 根据ROE调整合理PB
                    industry_avg_roe = results['roe'].median()
                    roe_ratio = row['roe'] / industry_avg_roe if industry_avg_roe > 0 else 1
                    
                    # 计算合理PB和PE
                    fair_pb = pb_median * roe_ratio
                    fair_pe = pe_median
                    
                    # 记录合理估值
                    results.at[idx, 'fair_pb'] = fair_pb
                    results.at[idx, 'fair_pe'] = fair_pe
                    
                    # 计算目标价格 (取PB估值和PE估值的平均)
                    if 'total_mv' in row and 'close' in row and row['pb'] > 0 and row['pe'] > 0:
                        price_by_pb = row['close'] * (fair_pb / row['pb'])
                        price_by_pe = row['close'] * (fair_pe / row['pe'])
                        price_target = (price_by_pb * 0.6 + price_by_pe * 0.4)  # 权重可调整
                        results.at[idx, 'price_target'] = round(price_target, 2)
                        
                        # 计算上涨空间
                        results.at[idx, 'upside'] = round((price_target / row['close'] - 1) * 100, 2)
            except Exception as e:
                print(f"计算{row['ts_code']}合理价格错误: {e}")
        
        return results
    
    def run_valuation(self):
        """
        运行估值模型并返回结果
        """
        # 获取银行股列表
        banks = self.get_bank_stocks()
        if banks.empty:
            print("未找到银行股，请检查数据源")
            return None
        
        # 获取财务数据
        print("正在获取财务数据...")
        fin_indicator, balance_sheet, income, daily = self.get_financial_data(banks)
        
        # 计算估值指标
        print("计算估值指标...")
        valuation_results = self.calculate_valuation_metrics(banks, fin_indicator, balance_sheet, income, daily)
        
        # 估计合理价格
        print("估计合理价格...")
        valuation_results = self.estimate_fair_price(valuation_results)
        
        # 排序并显示结果
        if not valuation_results.empty:
            valuation_results = valuation_results.sort_values('valuation_score', ascending=False)
            print("\n银行股估值结果:")
            print("=" * 80)
            print(valuation_results[['name', 'close', 'pb', 'pe', 'roe', 'pb_percentile', 'pe_percentile', 'valuation_score', 'price_target', 'upside']]
                  .rename(columns={
                      'name': '名称', 
                      'close': '收盘价', 
                      'pb': 'PB', 
                      'pe': 'PE', 
                      'roe': 'ROE', 
                      'pb_percentile': 'PB分位数', 
                      'pe_percentile': 'PE分位数',
                      'valuation_score': '估值分数',
                      'price_target': '目标价格',
                      'upside': '上涨空间'
                  }))
            
            # 可视化估值分数
            #self.visualize_results(valuation_results)
            
            return valuation_results
        else:
            print("估值计算失败，请检查数据")
            return None
    
    def visualize_results(self, results):
        """
        可视化估值结果
        """
        try:
            # 使用更可能在Mac上可用的字体
            plt.rcParams['font.sans-serif'] = ['Arial Unicode MS', 'PingFang SC', 'Heiti TC', 
                                             'Hiragino Sans GB', 'STHeiti', 'Microsoft YaHei', 'SimHei']  
            plt.rcParams['axes.unicode_minus'] = False
            
            # 检查是否有中文字体可用
            from matplotlib.font_manager import FontManager
            fm = FontManager()
            font_names = set([f.name for f in fm.ttflist])
            chinese_fonts = [f for f in plt.rcParams['font.sans-serif'] if f in font_names]
            
            if not chinese_fonts:
                print("警告: 未找到支持中文的字体，图表中的中文可能无法正确显示")
                # 使用英文标签作为备选方案
                use_english = True
            else:
                print(f"使用字体: {chinese_fonts[0]}")
                plt.rcParams['font.sans-serif'] = chinese_fonts
                use_english = False
            
            plt.figure(figsize=(12, 8))
            
            # 绘制估值分数条形图
            top_banks = results.head(10)  # 取估值分数最高的10只银行股
            plt.bar(top_banks['name'], top_banks['valuation_score'])
            plt.xlabel('Bank Stocks' if use_english else '银行股')
            plt.ylabel('Valuation Score' if use_english else '估值分数')
            plt.title('Bank Valuation Score Ranking (Top 10)' if use_english else '银行股估值分数排名 (前10名)')
            plt.xticks(rotation=45)
            plt.tight_layout()
            
            # 保存图表
            plt.savefig('bank_valuation_results.png')
            print("估值结果图表已保存至 bank_valuation_results.png")
            
            # PB-ROE散点图
            plt.figure(figsize=(12, 8))
            plt.scatter(results['pb'], results['roe'], s=results['total_mv']/1e10, alpha=0.6)
            
            # 添加银行名称标签
            for i, txt in enumerate(results['name']):
                plt.annotate(txt, (results['pb'].iloc[i], results['roe'].iloc[i]))
                
            plt.xlabel('PB Ratio' if use_english else '市净率(PB)')
            plt.ylabel('ROE' if use_english else '净资产收益率(ROE)')
            plt.title('Bank PB-ROE Comparison (Bubble Size = Market Cap)' if use_english else '银行股 PB-ROE 对比图 (气泡大小表示市值)')
            plt.grid(True)
            plt.tight_layout()
            
            # 保存图表
            plt.savefig('bank_pb_roe_comparison.png')
            print("PB-ROE对比图已保存至 bank_pb_roe_comparison.png")
            
        except Exception as e:
            print(f"图表绘制失败: {e}")
            # 尝试不使用中文的方式保存图表
            try:
                plt.rcParams['font.sans-serif'] = ['Arial']
                
                plt.figure(figsize=(12, 8))
                top_banks = results.head(10)
                plt.bar(top_banks['name'], top_banks['valuation_score'])
                plt.xlabel('Bank Stocks')
                plt.ylabel('Valuation Score')
                plt.title('Bank Valuation Score Ranking (Top 10)')
                plt.xticks(rotation=45)
                plt.tight_layout()
                plt.savefig('bank_valuation_results.png')
                
                plt.figure(figsize=(12, 8))
                plt.scatter(results['pb'], results['roe'], s=results['total_mv']/1e10, alpha=0.6)
                for i, txt in enumerate(results['name']):
                    plt.annotate(txt, (results['pb'].iloc[i], results['roe'].iloc[i]))
                plt.xlabel('PB Ratio')
                plt.ylabel('ROE')
                plt.title('Bank PB-ROE Comparison (Bubble Size = Market Cap)')
                plt.grid(True)
                plt.tight_layout()
                plt.savefig('bank_pb_roe_comparison.png')
                
                print("使用英文标签成功保存图表")
            except Exception as ex:
                print(f"使用英文标签保存图表也失败: {ex}")

if __name__ == "__main__":
    # 请在这里替换为您的Tushare API令牌
    tushare_token = "c0f992e8369579bfec7bf8481dc0bcc304ac66ab5b1dd12c1d154325"
    
    # 初始化并运行模型
    model = BankValuationModel(tushare_token)
    valuation_results = model.run_valuation()
    
    # 输出结果可以保存到CSV文件
    #if valuation_results is not None:
    #    valuation_results.to_csv('bank_valuation_results.csv', index=False, encoding='utf-8-sig')
    #    print("估值结果已保存至 bank_valuation_results.csv")