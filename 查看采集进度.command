#!/bin/zsh
# 双击查看小红书采集进度：只读数据库，不联网、不改动任何东西。
cd -- "$(dirname -- "$0")" || exit 1
# 批次目录：环境变量 XHS_BATCH，或 collections/ 下最近修改的批次
batch="${XHS_BATCH:-$(ls -td "$PWD"/collections/*/ 2>/dev/null | head -1)}"
batch="${batch%/}"
[[ -d "$batch" ]] || { print '没有找到批次目录；请设置 XHS_BATCH=批次路径'; read '?按回车关闭…'; exit 1 }
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python scripts/batch_status.py --batch "$batch"
if ! launchctl print "gui/$(id -u)/local.xhs.collection.supervise" >/dev/null 2>&1; then
  print '\n⚠ 看守程序没有在运行（比如电脑重启过）。双击「继续采集.command」可以重新启动它。'
fi
read '?按回车关闭…'
