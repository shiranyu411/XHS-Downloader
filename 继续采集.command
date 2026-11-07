#!/bin/zsh
# 双击继续小红书采集。先显示当前状态；验证码、登录、被限制这几种情况会先说明，等你确认。
cd -- "$(dirname -- "$0")" || exit 1
# 批次目录：环境变量 XHS_BATCH，或 collections/ 下最近修改的批次
batch="${XHS_BATCH:-$(ls -td "$PWD"/collections/*/ 2>/dev/null | head -1)}"
batch="${batch%/}"
[[ -d "$batch" ]] || { print '没有找到批次目录；请设置 XHS_BATCH=批次路径'; read '?按回车关闭…'; exit 1 }
py() { PYTHONDONTWRITEBYTECODE=1 .venv/bin/python "$@" }
py scripts/batch_status.py --batch "$batch"
print
if ! curl -s --max-time 2 'http://127.0.0.1:9223/json/version' | grep -q webSocketDebuggerUrl; then
  print '专用 Chrome（端口 9223）没有打开，正在打开…'
  py main.py collect browser || { print 'Chrome 启动失败'; read '?按回车关闭…'; exit 1 }
  print '请在打开的 Chrome 里确认小红书还是登录状态。'
  read '?确认后按回车继续…'
fi
blocked=$(py -c 'import sqlite3, sys, pathlib
db = sqlite3.connect((pathlib.Path(sys.argv[1]) / "state.sqlite").as_uri() + "?mode=ro", uri=True)
row = db.execute("SELECT value FROM controls WHERE key=?", ("workspace_blocked",)).fetchone()
print(row[0] if row else "")' "$batch")
case "$blocked" in
  login_or_verification_required)
    print '平台要求验证码或重新登录。请先在专用 Chrome 里手动完成，再回到这里。'
    read 'ans?已经手动完成了吗？输入 y 继续：'
    [[ $ans == [yY]* ]] || exit 0
    py main.py collect ack-restriction --batch "$batch" --confirmed || { read '?按回车关闭…'; exit 1 } ;;
  access_restricted)
    print '平台最近限制过访问。前两次看守程序会自己等完冷却（默认 6 小时、18 小时）再慢速继续；'
    print '48 小时内第 3 次被限制时它会停下，等你决定。现在继续有让账号受更多限制的风险。'
    read 'ans?仍要让它在冷却结束后继续吗？输入 y 确认：'
    [[ $ans == [yY]* ]] || exit 0
    py main.py collect ack-restriction --batch "$batch" --confirmed || { read '?按回车关闭…'; exit 1 } ;;
esac
touch "$batch/.supervisor-reset"
rm -f "$batch/.pause-requested"
if ! launchctl print "gui/$(id -u)/local.xhs.collection.supervise" >/dev/null 2>&1; then
  launchctl bootstrap "gui/$(id -u)" "$batch/supervise.plist" && print '已重新启动看守程序。'
fi
print '已继续：看守程序会在 30 秒内启动采集（如果还在冷却，会等冷却结束）。'
read '?按回车关闭…'
