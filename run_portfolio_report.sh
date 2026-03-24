#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${ROOT_DIR}"

if [[ -f "${ROOT_DIR}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${ROOT_DIR}/.env"
  set +a
fi

mkdir -p "${ROOT_DIR}/logs"

export HOME="${ROOT_DIR}"

exec "${ROOT_DIR}/.venv/bin/python" "${ROOT_DIR}/send_portfolio_report.py" "$@"
