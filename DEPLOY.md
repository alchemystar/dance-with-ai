# 部署说明

这份文档按“部署到一台普通 Linux 云服务器，每天晚上 21:00 自动发邮件”来写。

## 1. 服务器准备

建议环境：

- Ubuntu 22.04 / Debian 12
- Python `3.10+`
- `git`
- 时区设置为 `Asia/Shanghai`

安装基础依赖：

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip git tzdata
sudo timedatectl set-timezone Asia/Shanghai
```

## 2. 拉代码

```bash
cd /opt
sudo git clone <你的仓库地址> dance-with-ai
sudo chown -R $USER:$USER /opt/dance-with-ai
cd /opt/dance-with-ai
```

如果你不是放在 `/opt/dance-with-ai`，后面的路径一起替换掉。

## 3. 创建虚拟环境

```bash
cd /opt/dance-with-ai
python3 -m venv .venv
.venv/bin/pip install -U pip
.venv/bin/pip install -r requirements.txt
```

如果你希望一步到位，也可以直接执行：

```bash
cd /opt/dance-with-ai
bash deploy.sh
```

## 4. 环境变量

推荐用环境变量，不要在云服务器上依赖硬编码。

先建一个环境文件：

```bash
cat > /opt/dance-with-ai/.env <<'EOF'
TUSHARE_TOKEN=你的tushare_token
OPENAI_API_KEY=你的openai_key
MAIL_FROM=你的发件邮箱
MAIL_PASSWORD=你的QQ邮箱授权码
MAIL_TO=你的收件邮箱
EOF
```

然后加载：

```bash
set -a
source /opt/dance-with-ai/.env
set +a
```

## 5. 缓存和日志目录

```bash
mkdir -p /opt/dance-with-ai/.cache/tushare
mkdir -p /opt/dance-with-ai/.cache/commodities
mkdir -p /opt/dance-with-ai/logs
```

## 6. 手动测试

先测试核心长期池：

```bash
cd /opt/dance-with-ai
set -a && source .env && set +a
HOME=/opt/dance-with-ai .venv/bin/python core_long_term_pool.py --days 720 --top 15
```

测试港股跟踪：

```bash
cd /opt/dance-with-ai
set -a && source .env && set +a
HOME=/opt/dance-with-ai .venv/bin/python hk_holdings_tracker.py --days 720
```

测试发邮件：

```bash
cd /opt/dance-with-ai
bash run_portfolio_report.sh
```

## 7. 每天 21:00 自动发邮件

先确认日志目录存在：

```bash
mkdir -p /opt/dance-with-ai/logs
```

编辑 `crontab`：

```bash
crontab -e
```

加入这一行：

```bash
0 21 * * * cd /opt/dance-with-ai && bash run_portfolio_report.sh >> /opt/dance-with-ai/logs/portfolio_mail.log 2>&1
```

含义：

- 每天 `21:00`
- 进入项目目录
- 执行统一邮件脚本
- 把输出写进日志

## 8. 可选：用 systemd 管理

如果你希望更稳一点，可以加一个 `systemd` 的一次性服务。

创建服务文件：

```bash
sudo tee /etc/systemd/system/dance-with-ai-mail.service > /dev/null <<'EOF'
[Unit]
Description=Dance With AI Portfolio Mail
After=network.target

[Service]
Type=oneshot
WorkingDirectory=/opt/dance-with-ai
Environment=HOME=/opt/dance-with-ai
ExecStart=/bin/bash -lc 'set -a && source /opt/dance-with-ai/.env && set +a && /opt/dance-with-ai/.venv/bin/python send_portfolio_report.py'
ExecStart=/bin/bash -lc 'cd /opt/dance-with-ai && bash run_portfolio_report.sh'
EOF
```

手动执行测试：

```bash
sudo systemctl daemon-reload
sudo systemctl start dance-with-ai-mail.service
sudo systemctl status dance-with-ai-mail.service
```

如果只是每天 21:00 发送，其实 `cron` 已经够用。

## 9. 服务器上最建议改的地方

当前仓库在“能跑”这件事上没问题，但如果要长期放在云服务器，最建议注意下面几点：

### 9.1 邮件账号和授权码

现在代码里还有 QQ 邮箱相关硬编码。

云服务器上建议统一改成环境变量，至少包括：

- `MAIL_FROM`
- `MAIL_PASSWORD`
- `MAIL_TO`

### 9.2 Tushare Token

`backtest.py` 现在支持 `TUSHARE_TOKEN` 环境变量覆盖，但仓库里仍保留了默认 token。

建议部署时始终在 `.env` 里显式设置：

```bash
TUSHARE_TOKEN=你的token
```

### 9.3 OpenAI Key

如果你后面会用：

- `ai_stock_advisor.py`

就要保证环境里有：

```bash
OPENAI_API_KEY=你的key
```

### 9.4 首次跑会慢

原因：

- 要补本地缓存
- 港股接口还有主动等待

所以第一次发送邮件比较慢是正常的，后面会快很多。

## 10. 常用运维命令

看邮件日志：

```bash
tail -f /opt/dance-with-ai/logs/portfolio_mail.log
```

手动发一次：

```bash
cd /opt/dance-with-ai
bash run_portfolio_report.sh
```

更新代码：

```bash
cd /opt/dance-with-ai
git pull
.venv/bin/pip install -r requirements.txt
```

## 11. 建议的部署顺序

推荐你按这个顺序走：

1. 拉代码
2. 建 `.venv`
3. 配 `.env`
4. 手动跑 `core_long_term_pool.py`
5. 手动跑 `send_portfolio_report.py`
6. 确认邮件正常
7. 最后再加 `crontab`

这样最稳。
