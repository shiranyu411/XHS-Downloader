#!/bin/zsh
# 双击暂停小红书采集：当前帖子保存完后停下，之后不会自动重启；已采的数据都保留。
cd -- "$(dirname -- "$0")" || exit 1
# 批次目录：环境变量 XHS_BATCH，或 collections/ 下最近修改的批次
batch="${XHS_BATCH:-$(ls -td "$PWD"/collections/*/ 2>/dev/null | head -1)}"
batch="${batch%/}"
[[ -d "$batch" ]] || { print '没有找到批次目录；请设置 XHS_BATCH=批次路径'; read '?按回车关闭…'; exit 1 }
running() { pgrep -f "main.py COLLECT resume --batch $batch" >/dev/null }
print -n pause > "$batch/.pause-requested"
print '已请求暂停，等待采集器在安全位置停下…'
for i in {1..60}; do
  running || break
  sleep 2
done
if running; then
  print '采集器还在收尾，稍后会自己停下；暂停文件已放好，不会再自动重启。'
else
  print '已暂停。要继续，双击「继续采集.command」。'
fi
read '?按回车关闭…'
