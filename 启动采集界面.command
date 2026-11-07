#!/bin/zsh
# 双击启动兼容界面；主入口推荐「启动一体化工作台.command」。
print '这是旧版兼容界面（外部 Chrome）；推荐使用「启动一体化工作台.command」。'
cd -- "$(dirname -- "$0")" || exit 1
if [[ ! -x .venv/bin/python ]]; then
  print '尚未安装运行环境。请在此目录执行：uv sync --extra collection --extra mcp'
  read '?按回车关闭…'
  exit 1
fi
.venv/bin/python main.py
result=$?
if (( result != 0 )); then
  print '界面启动失败。请保留上方错误信息，或执行 uv sync --extra collection --extra mcp 安装依赖。'
  read '?按回车关闭…'
fi
exit $result
