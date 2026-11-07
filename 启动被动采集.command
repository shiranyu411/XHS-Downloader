#!/bin/zsh
# 双击启动被动录制：自动开专用 Chrome、等你登录、按时间建批次、开始存档。
# 全程不注入脚本、不主动请求，只记录你在 Chrome 里手动浏览产生的响应。
cd -- "$(dirname -- "$0")" || exit 1
if [[ ! -x .venv/bin/python ]]; then
  print '尚未安装运行环境。请先执行：uv sync --extra collection --extra mcp'
  read '?按回车关闭…'
  exit 1
fi

cdp='http://127.0.0.1:9223'
ready() { curl -s --max-time 1 "$cdp/json/version" | grep -q webSocketDebuggerUrl }

if ready; then
  print '检测到专用 Chrome 已在运行，直接复用。'
else
  print '正在启动专用 Chrome…'
  .venv/bin/python main.py collect browser || { print 'Chrome 启动失败'; read '?按回车关闭…'; exit 1; }
  for i in {1..60}; do
    ready && break
    sleep 1
  done
  if ! ready; then
    print 'Chrome 调试端口 60 秒内未就绪。请手动启动后重试，或改用命令行。'
    read '?按回车关闭…'
    exit 1
  fi
fi

print ''
print '请在刚打开的 Chrome 窗口里登录小红书，并把搜索页翻到你要采集的关键词。'
read '?准备好后按回车开始录制…'

batch="collections/manual-$(date +%Y%m%d-%H%M%S)"
print ''
print "录制中：$batch"
print '看到「正在监听 N 个标签页」后，就可以正常打开笔记、展开评论。'
print '结束时按 Ctrl+C，报告会写入 passive_report.json。'
print ''
.venv/bin/python main.py collect record --batch "$batch" --cdp "$cdp"
result=$?
print ''
print "结果目录：$batch"
read '?按回车关闭窗口…'
exit $result
