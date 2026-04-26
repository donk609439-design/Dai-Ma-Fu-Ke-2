#!/bin/bash
set -e

echo "[build] 安装 Python 依赖..."
SITE_PKG_DIR=".pythonlibs/lib/python3.11/site-packages"
mkdir -p "$SITE_PKG_DIR"

if command -v uv >/dev/null 2>&1; then
  uv pip install -r jetbrainsai2api/requirements.txt --target "$SITE_PKG_DIR" --quiet
elif command -v pip3 >/dev/null 2>&1; then
  pip3 install -r jetbrainsai2api/requirements.txt --target "$SITE_PKG_DIR" --quiet
elif command -v pip >/dev/null 2>&1; then
  pip install -r jetbrainsai2api/requirements.txt --target "$SITE_PKG_DIR" --quiet
else
  echo "[build] 警告: 未找到 pip/uv，跳过 Python 依赖安装"
fi
echo "[build] Python 依赖安装完成"

echo "[build] 构建 API Server..."
pnpm --filter @workspace/api-server run build
echo "[build] 构建完成"
