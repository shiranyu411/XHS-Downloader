# 采集扩展的第三方来源

## 可选 Qt 桌面运行时

一体化入口通过 `desktop` extra 使用 PySide6 / Qt WebEngine / Qt WebChannel，以及 Playwright。当前验证组合为 PySide6 6.10.3 与 Playwright 1.64.1。Qt WebChannel 的 `qwebchannel.js` 从 Qt 自带的 `qrc` 资源读取，未复制进本仓库；`qt-bridge.js` 为本项目的 Promise 适配层。

发行桌面安装包时须保留 PySide6、Qt 和 Chromium 发行物附带的许可证与第三方声明，并核查实际采用模块、编解码器的分发条件，不能把 Chromium 全部第三方组件概括为单一许可证。当前只提供依赖安装和源码启动，未分发独立打包运行时。

参考：[Qt for Python 许可](https://doc.qt.io/qtforpython-6/licenses.html)、[Qt WebEngine 许可](https://doc.qt.io/qt-6/qtwebengine-licensing.html)。

## XHS-Downloader

本工作区基于 JoeanAmier/XHS-Downloader。原项目 GPL-3.0 许可保留在根目录 `LICENSE`。原脚本滚动和发现逻辑的基线为 `2ab2620b0dd35b04530e70f1e9c7ab3f78c5d6c5`。

## Yht20927/xiaohongshu-cli

- 仓库：https://github.com/Yht20927/xiaohongshu-cli
- 固定提交：`5c0159768147b4151d4912c083863a23bbbebb7a`
- 来源文件：`scripts/xiaohongshu.user.js`，webpack 请求客户端发现及两级评论 GET 参数组织。
- 本地改编：`static/collection-request.js`、`source/collection/request.py`。
- Copyright (c) 2026 Yht20927，MIT License。完整许可见 [third_party/xiaohongshu-cli-LICENSE](third_party/xiaohongshu-cli-LICENSE)。
- 上游该提交的相关声明另存于 [third_party/xiaohongshu-cli-DISCLAIMER.md](third_party/xiaohongshu-cli-DISCLAIMER.md)，作为来源记录；本地没有引入其 Node 服务、LLM 功能或写入/互动 API。

本地修改：只保留能力探测和两类只读分页；去掉固定模块 ID 回退、全局响应拦截器修改、返回值包装、任意表达式入口及轮询服务器；增加 namespace/default 别名去重、已配置网页请求包装器选择、歧义拒绝、操作状态和路径/笔记归属校验。Python 以 CDP 持久化的原始响应作为成功与分页依据。

`xpzouying/xiaohongshu-mcp` 等项目目前仅用于方案和交互规则参考，本轮未直接引入其实现代码。其他核查项目不是当前运行依赖。
