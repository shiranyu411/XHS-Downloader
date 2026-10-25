#!/bin/zsh
# 一体化入口；旧版仍可使用「启动采集界面.command」。
cd -- "$(dirname -- "$0")" || exit 1
if [[ ! -x .venv/bin/python ]]; then
  print '请先执行：uv sync --extra desktop --extra collection --extra mcp'
  read '?按回车关闭…'
  exit 1
fi
.venv/bin/python main.py WORKBENCH
result=$?
if (( result != 0 )); then
  print '启动失败。请检查上方错误；依赖安装命令：uv sync --extra desktop --extra collection --extra mcp'
  read '?按回车关闭…'
fi
exit $result
