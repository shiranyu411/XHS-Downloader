"""启动 GUI 及其 PyWebView 下载器桥接。"""

import sys
from pathlib import Path

from ..module import ROOT

# GUI 静态资源与 Python 桥接代码分离，统一从项目 static 目录加载。
INDEX_PATH = ROOT.joinpath("static", "GUI", "collector.html")


def get_index_path() -> Path:
    """返回 GUI 入口页面的绝对路径，并在文件缺失时尽早报错。"""

    if not INDEX_PATH.is_file():
        raise FileNotFoundError(f"GUI 入口文件不存在：{INDEX_PATH}")
    return INDEX_PATH


def get_icon_path(platform_name: str | None = None) -> Path:
    """根据桌面平台选择对应的窗口图标格式。"""

    platform_name = platform_name or sys.platform
    suffix = {
        "win32": ".ico",
        "darwin": ".icns",
    }.get(platform_name, ".png")
    icon_path = ROOT.joinpath("static", f"XHS-Downloader{suffix}")
    if not icon_path.is_file():
        raise FileNotFoundError(f"GUI 图标文件不存在：{icon_path}")
    return icon_path


def launch() -> None:
    """Open the complete-collection UI; legacy Python download APIs stay available."""
    from .collector import launch as launch_collection
    launch_collection()


def main() -> None:
    """命令行入口。"""

    launch()


if __name__ == "__main__":
    main()
