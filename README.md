# XHS-Downloader · 关键词采集扩展

基于 [JoeanAmier/XHS-Downloader](https://github.com/JoeanAmier/XHS-Downloader)（GPL-3.0）的 Fork，在原有的作品下载之外增加了一套**低速、可恢复、留存原始证据**的小红书公开内容采集流程：

- 关键词搜索 → 翻页发现帖子 → 正文、图片/视频、一级评论与二级回复
- 每一次网络响应原样存档（`state.sqlite` + gzip 文件），可以离线回放、审计和重建导出
- 中断、暂停后从原断点继续，已保存的文件和评论分页不重复请求
- 被动录制模式：不注入脚本、不主动请求，只存档你手动浏览时产生的响应
- 可选的主题筛选（标题初筛、正文判定）和关键词轮流抽样
- 离线导出 CSV / 可读网页

原项目的下载器、TUI、API、MCP 和用户脚本入口都保留，用法见上游文档。

## 安装

Python 3.12+，需要本机安装 Google Chrome 或 Playwright Chromium。

```bash
uv sync --extra collection            # 采集
uv sync --extra desktop --extra collection --extra mcp   # 再加 Qt 一体化工作台和 MCP
```

## 快速开始

```bash
# 1. 打开一个独立配置目录的专用 Chrome（不会读取你日常浏览器的会话），在里面手动登录
.venv/bin/python main.py COLLECT browser

# 2. 检查 CDP 连接和网页模块（不请求评论）
.venv/bin/python main.py COLLECT doctor

# 3. 按关键词采集；批次目录默认在 collections/ 下
.venv/bin/python main.py COLLECT keyword 露营装备 咖啡手冲 --batch collections/示例批次

# 暂停（当前帖子保存完再停）/ 继续 / 导出
.venv/bin/python main.py COLLECT pause  --batch collections/示例批次
.venv/bin/python main.py COLLECT resume --batch collections/示例批次
.venv/bin/python main.py COLLECT export --batch collections/示例批次
```

其他子命令见 `python main.py COLLECT --help`。macOS 下也可以双击根目录的 `.command` 文件；它们默认操作 `collections/` 里最近修改的批次，或用环境变量 `XHS_BATCH` 指定。

辅助脚本（`scripts/`）：

| 脚本 | 作用 |
| --- | --- |
| `batch_status.py --batch <B>` | 中文进度表（只读） |
| `watch_batch.py --batch <B>` | 只读事件流：限制、状态变化、定时进度行 |
| `supervise_batch.py --batch <B>` | 看守进程：冷却结束后续采；验证码、登录、多次限制时通知并等人处理 |
| `prioritize_batch.py <B>` | 对已发现的帖子按 `topic-profile.json` 重新排序 |

## 速度与限制

默认配置刻意放得很慢，接近人工浏览：单帖串行、每分钟最多 4 次评论请求、打开帖子之间有固定间隔。平台一旦拒绝（HTTP 461、"访问频繁"、验证码、登录失效），整个批次立即停下：

- 普通限制：默认按 6 小时 → 18 小时 → 72 小时冷却（`restriction_cooldown` × 1/3/12），冷却后以更慢的速度继续；48 小时内第 3 次被限制就停止自动续采，等人决定。
- 验证码 / 登录：始终等人在浏览器里手动处理后，用 `ack-restriction --confirmed` 确认。
- 不做也不会加入：多账号轮换、换 IP / 代理切换、自动过验证码。

## 可选：主题筛选

关键词经常有同名的其他意思。在批次目录放一个 `topic-profile.json`，就会启用标题初筛和正文判定。示例见 [docs/topic-profile.example.json](docs/topic-profile.example.json)。没有这个文件时不做任何筛选。

- 标题初筛只丢弃明显撞词的帖子，看不准的一律打开正文再判断。
- 正文判定需要在 `run.json` 里设 `"topic_gate": true`；`core_keywords` 搜到的帖子永远不按正文筛。

## 可选：直接 API 通道（默认关闭）

`source/collection/http_runner.py` 不打开网页，直接调用网页端接口。签名由第三方库 [xhshow](https://github.com/Cloxl/xhshow) 生成，请求头和 Cookie 取自你自己登录的专用 Chrome（`browser_session.py`，保存在批次目录 `.credentials/`，权限 0600，不进日志，`.gitignore` 已排除）。

它**只在手动把批次 `run.json` 的 `engine` 改成 `"http_api"` 时启用**。速度由 `run.json` 的 `api_options` 控制（`interval` 请求间隔秒数，默认 1；`concurrency` 并行帖子数，默认 3；`comment_interval` 评论请求间隔），代码本身不设下限。遇到平台拒绝会停下，看守进程不会自动重试，需要人确认后再继续。

使用这个通道比走网页更容易触发风控，也可能违反平台用户协议，请自行评估。

## 速度建议与风控

代码不限制速度和并行数，浏览器通道的 `post_concurrency` 最高可设 6，API 通道没有上限。**并行和频率开得越高，越快触发平台风控**：轻则 HTTP 461 / 业务码 300013（"访问频繁"），重则要求验证码、重新登录，甚至限制账号。下面是开发时在同一个账号上实际观察到的情况，只能作为参考，平台策略随时会变：

| 速度 | 结果 |
| --- | --- |
| 浏览器通道，每小时约 10 次页面访问（打开帖子 + 搜索翻页），每分钟约 2 次评论请求 | 连续约 11 小时没有被拒绝 |
| 浏览器通道，每小时 20–30 次页面访问 | 连续数小时正常 |
| 浏览器通道，每小时约 55 次页面访问 | 数小时内被拒绝 |
| 6 篇帖子并行、请求间隔 0.27 秒 | 开始后约 20 秒被拒绝 |
| 二级回复请求每分钟约 200 次 | 被拒绝（461） |
| 4 篇帖子并行、帖子间隔 2 秒、翻页间隔 1 秒 | 几分钟内两次被拒绝 |
| API 通道，默认速度（间隔 1 秒、3 并行） | 约 75 秒、75 个请求后被拒绝 |

建议：

- 帖子并行保持 1；页面访问控制在每小时 30 次以内。API 通道的 `interval` 建议不低于数秒，`concurrency` 设为 1。
- 被拒绝后先冷却再继续，不要立刻重试。触发拒绝的主要是短时间的请求突发，不是脚本本身。
- 一个账号一套速度。不要用多账号、换 IP 或自动过验证码来绕过限制，本项目也不提供这些功能。

## 使用须知

- 本项目仅供学习与研究，只用于采集公开内容；请遵守《小红书用户服务协议》、robots 规则和所在地法律法规（包括个人信息保护相关规定）。
- 采集到的评论包含他人的昵称、ID 和言论，属于个人信息。请只在研究所需范围内保存，发布前做匿名化，不要二次分发原始数据。
- 不得用于商业用途、大规模爬取或任何影响平台正常运行的行为。由使用者自行承担使用后果。

## 测试

全部使用本地夹具，不访问平台：

```bash
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python scripts/run_isolated_tests.py
node --test tests/*.cjs
```

## 许可证

[GPL-3.0](LICENSE)，与上游一致。第三方来源（xhshow、xiaohongshu-cli 等）见 [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md)。
