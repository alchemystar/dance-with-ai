#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
VENV_DIR="${ROOT_DIR}/.venv"
PIP_BIN="${VENV_DIR}/bin/pip"
PYTHON_VENV_BIN="${VENV_DIR}/bin/python"

echo "[1/7] 检查 Python..."
if ! command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
  echo "未找到 ${PYTHON_BIN}，请先安装 Python 3.10+"
  exit 1
fi

echo "[2/7] 创建目录..."
mkdir -p "${ROOT_DIR}/.cache/tushare"
mkdir -p "${ROOT_DIR}/.cache/commodities"
mkdir -p "${ROOT_DIR}/logs"

echo "[3/7] 创建虚拟环境..."
if [ ! -d "${VENV_DIR}" ]; then
  "${PYTHON_BIN}" -m venv "${VENV_DIR}"
fi

echo "[4/7] 安装依赖..."
"${PIP_BIN}" install -U pip
"${PIP_BIN}" install -r "${ROOT_DIR}/requirements.txt"

echo "[5/7] 检查环境文件..."
if [ ! -f "${ROOT_DIR}/.env" ]; then
  if [ -f "${ROOT_DIR}/.env.example" ]; then
    cp "${ROOT_DIR}/.env.example" "${ROOT_DIR}/.env"
    echo "已从 .env.example 生成 .env，请先补全里面的环境变量。"
  else
    cat > "${ROOT_DIR}/.env" <<'EOF'
TUSHARE_TOKEN=
OPENAI_API_KEY=
MAIL_FROM=
MAIL_PASSWORD=
MAIL_TO=
EOF
    echo "已生成空白 .env，请先补全里面的环境变量。"
  fi
fi

echo "[6/7] 做基础校验..."
"${PYTHON_VENV_BIN}" -m py_compile \
  "${ROOT_DIR}/backtest.py" \
  "${ROOT_DIR}/core_long_term_pool.py" \
  "${ROOT_DIR}/hk_holdings_tracker.py" \
  "${ROOT_DIR}/send_portfolio_report.py"

echo "[7/7] 部署完成。"
echo
echo "下一步建议："
echo "1. 编辑环境变量: ${ROOT_DIR}/.env"
echo "2. 手动测试核心池:"
echo "   cd ${ROOT_DIR} && set -a && source .env && set +a && HOME=${ROOT_DIR} ${PYTHON_VENV_BIN} core_long_term_pool.py --days 720 --top 15"
echo "3. 手动测试邮件:"
echo "   cd ${ROOT_DIR} && set -a && source .env && set +a && HOME=${ROOT_DIR} ${PYTHON_VENV_BIN} send_portfolio_report.py"
echo "4. 配置 crontab:"
echo "   0 21 * * * cd ${ROOT_DIR} && set -a && . ${ROOT_DIR}/.env && set +a && HOME=${ROOT_DIR} ${PYTHON_VENV_BIN} send_portfolio_report.py >> ${ROOT_DIR}/logs/portfolio_mail.log 2>&1"
