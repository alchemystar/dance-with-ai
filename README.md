# dance-with-ai

与AI共舞。

当前仓库已经支持两套回测方式：

- `legacy`：保留原来的 MACD 规则，方便和老结果对比
- `regime`：新增趋势过滤版 MACD，加入 EMA 趋势过滤、RSI 过滤、ATR 止损/移动止盈
- `cycle`：周期股波段策略，结合行业资金流、趋势突破、回踩再走强和 ATR 风控
- `value_quality`：低估值 + 财报健康 + 同行业财报对比的长期持有策略，适合先选股再耐心等待价值回归
- `dividend_hold`：高股息 + 分红稳定 + 财报健康的低频持有策略，适合拿着等分红
- `bank_dividend_core`：银行股高股息核心组合策略，会先筛出更适合长期拿分红的银行，再用超低频方式持有
- `core_long_term_pool`：核心长期持有池筛选器，会把价值核心、红利核心和银行红利核心合并成一个最终跟踪池
- `bank_t`：银行股专用底仓做T策略，支持分批加仓/减仓
- `bank_t_state_owned_income`：国有大行收益优先版，使用更重底仓和更严格红利/财报阈值，适合单独观察国有大行
- `state_owned_dividend`：国有大行长持红利版，重点看低PB、高股息、财报稳定和低频买点
  默认股票池现在会自动使用本地股票清单里的发达地区银行
  会进一步拆成 `国有大行 / 股份行 / 区域/农商行` 三个子池分别使用不同参数
  如需回到全国银行范围，可加 `--bank-scope all`

常用命令：

```bash
pip install -r requirements.txt
```

```bash
python backtest.py --mode regime --days 365
```

```bash
python backtest.py --mode cycle --days 365
```

```bash
python backtest.py --mode cycle --days 365 --cycle-scope all
```

```bash
python backtest.py --mode bank_t --days 365
python backtest.py --mode bank_t_state_owned_income --days 365
python backtest.py --mode state_owned_dividend --days 365
python backtest.py --mode bank_dividend_core --days 720
```

```bash
python backtest.py --mode bank_t --days 365 --bank-scope all
```

```bash
python backtest.py --mode legacy --days 365
```

```bash
python strategy_optimizer.py --days 720 --top 10
```

```bash
python strategy_optimizer.py --mode cycle --days 720 --top 10
```

```bash
python strategy_optimizer.py --mode bank_t --days 720 --top 10
```

```bash
python strategy_optimizer.py --mode bank_t --bank-segment state_owned --days 720 --top 10
```

```bash
OPENAI_API_KEY=your_key python ai_stock_advisor.py --stock 300762.SZ --mode auto
```

```bash
python financial_quality_screener.py --industry 银行 --period 20250930 --top 20
```

```bash
python theme_stock_screener.py --days 120 --scope all --top 15
```

```bash
python backtest.py --mode theme --days 120 --theme-scope screened --theme-top 10
```

```bash
python value_stock_screener.py --days 720 --scope sample --top 20
```

```bash
python core_long_term_pool.py --days 720 --top 20
```

```bash
python hk_holdings_tracker.py --days 720
```

```bash
python backtest.py --mode value_quality --days 720 --value-scope screened --value-top 12
```

```bash
python backtest.py --mode dividend_hold --days 720 --value-top 10 --value-limit 50
```

说明：

- `backtest.py` 现在会把手续费、滑点、最大回撤、Sharpe、超额收益一起算出来
- `backtest.py` 现在支持目标仓位回测，因此可以回测“底仓 + 做T”这种分批加减仓策略
- `backtest.py` 会优先从 `Tushare` 缓存读取行情，并额外缓存上证/沪深300指数、`daily_info` 市场活跃度、`moneyflow_mkt_dc` 大盘资金流和 `moneyflow_ind_dc` 行业资金流
- `backtest.py` 现在会给所有 A 股策略统一叠加一层“财报健康度 + 同行业财报分位”过滤，只允许财报健康且同行对比不差的标的发出买入/加仓信号；这层过滤使用按公告日生效的财报时间线和同交易日横向排名，不使用未来数据
- `cycle` 策略默认先跑一组代表性周期龙头，重点看行业资金流、相对强弱、趋势突破、回踩再走强，以及商品价格指数和对应期货主力连续的景气代理；如果你要扩大到更多周期股，可用 `--cycle-scope all`
- 周期相关的商品景气代理会缓存到 `.cache/commodities/`，包括大宗商品价格指数和行业映射到的主力连续合约
- 周期策略现在还会叠加最新财报摘要过滤，优先要求利润、营收、现金流、ROE、负债率这几项不要明显走坏；财报缓存复用 `.cache/fundamentals/akshare/`
- 周期策略现在也会结合 `daily_basic` 的 `PE/PB` 三年分位做“低估买入、高估卖出”，只在景气和财报没坏、估值偏低且回踩低点时买入，估值回到高位且动能转弱时卖出
- 周期策略会按同一交易日的涨幅强度、成交额放大、接近前高和行业资金流给股票打“龙头强度分”，这个分数只使用当日及历史数据，次日才执行信号，不使用未来数据
- `cycle` 模式输出现在会额外按 `有色链 / 化工链 / 黑色链` 分别汇总平均收益、超额收益、回撤和交易次数，方便直接比较三条链的效果
- 周期股现在支持两种专用模式：`cycle_leader_hold` 只做有色龙头持有，`cycle_swing` 只做黑色/化工波段，可分别回测对比
- `theme` 模式现在会把题材情绪、连板梯队、首板/二板/炸板回封和板块热度一起缓存到 `.cache/tushare/`，回测时优先看梯队位置和热度变化，再用 MACD 做辅助过滤
- `theme_stock_screener.py` 用来先筛选题材股池，会综合最近强度、成交额、交易量放大、首板/二板/回封状态、题材热度、财报健康度和同行财报分位，先把更像题材核心的票挑出来，再决定后面的择时
- `backtest.py --mode theme` 现在支持 `--theme-scope screened`，会先用筛选器挑出当前观察池，再在这个池子里跑题材择时；这个模式更适合当前选股和后续跟踪，不适合拿来做严格无偏历史回测
- `value_stock_screener.py` 会先从样本池或全市场里筛出“低估值但财报健康”的股票，综合 `PE/PB` 三年分位、股息率、财报健康度和同行财报分位来排序
- `value_stock_screener.py` 现在还会额外做一层长期持有过滤：优先家电、食品、医药、银行、公用事业这类更适合长期拿的行业，同时主动压低地产、黑色链、强周期和 ST 风险股，尽量少掉进“看起来便宜其实是价值陷阱”的标的
- `core_long_term_pool.py` 会把价值核心、红利核心和银行红利核心三条线合并去重，输出一个更适合长期跟踪的最终股票池；如果同一只股票同时在价值和红利两条线靠前，会被优先提升到更高层级
- `core_long_term_pool.py` 现在还会额外给出 `buy_timing_score` 和 `buy_hint`，用来提示“低点可买 / 接近低点 / 等待回踩 / 先观察”
- `core_long_term_pool.py` 也会给 `suggested_position`，把长期状态和当前买点直接映射成一个更直观的建议仓位区间
- `hk_holdings_tracker.py` 用来单独跟踪港股持仓，当前支持 `吉利汽车 / 安踏体育 / 腾讯 / 阿里 / 美团 / 小米`，会输出当前动作、核心逻辑、加仓规则和减仓规则
- `backtest.py --mode value_quality` 适合做“先选股、再长期持有”的回测；它会在估值处在历史低位且财报健康时逐步建仓，在明显高估或财报恶化时减仓/清仓
- `backtest.py --mode dividend_hold` 会优先从更大的候选池里筛出高股息、分红稳定、财报健康的股票，再用低频方式建立红利底仓，适合“拿着等分红 + 等慢慢修复”
- `backtest.py --mode bank_dividend_core` 会先在银行池里按股息率、分红稳定性、PB 分位、财报健康度和同行财报分位筛出“核心红利银行”，再用更低频的方式长期持有；这个模式更适合“先挑能拿的银行，再耐心等分红和估值修复”
- 银行 `bank_t` 策略现在会额外参考 `daily_basic` 的 `PB`、`dv_ttm` 和 `dividend` 分红记录，综合 PB 分位、股息率和分红稳定性来决定底仓和做T力度
- 银行 `bank_t` 也会按财报公告时间线叠加 `利润增速 / 营收增速 / ROE / EPS / 每股净资产 / 财务健康分`，财报明显转弱时会主动降到底仓或防守仓位，避免未来函数
- 银行 `bank_t` 的新买点不再只看 MACD，必须先通过“财报低估值分数 + 价格低点时机分数”，也就是先确认低估，再等回踩低点，才允许建仓或加仓
- `state_owned_dividend` 更偏“拿住国有大行底仓”，买点主要要求 PB 回到较低分位、股息率达标、财报没有明显恶化，同时价格处于回踩或趋势修复阶段
- 银行 `bank_t` 默认聚焦北京、上海、江苏、浙江、深圳、福建这些发达地区银行，更适合银行做T的低波和资产质量假设；如果你要看全市场银行，可用 `--bank-scope all`
- 银行 `bank_t` 策略也会参考上证、沪深300、市场活跃度和大盘主力资金，把大盘环境折成仓位上限；大盘弱时会自动降低底仓和做T力度
- `strategy_optimizer.py` 现在支持 `regime`、`cycle` 和 `bank_t` 三套策略的参数扫描，帮助找到更适合当前行情的参数组合
- `ai_stock_advisor.py` 会用 `GPT-5.4` + `web_search` 搜最新新闻、财报、公告和舆情，再结合技术面给出是否买入的判断
- `financial_quality_screener.py` 会通过 `AkShare` 拉财务摘要、三大报表和同行比较，并把结果缓存到 `.cache/fundamentals/akshare/`
- 历史行情会自动缓存到 `.cache/tushare/`，重复回测默认直接复用本地数据
- `regime` 策略现在除了个股趋势外，还会参考大盘指数趋势、市场成交活跃度、大盘主力资金流和所属行业板块资金流；如果你的 `Tushare` 账号暂时没开通某个接口，会自动降级，不会中断回测
- 如果你想强制刷新数据，可以加 `--refresh-cache`
- `TUSHARE_TOKEN` 支持通过环境变量覆盖
