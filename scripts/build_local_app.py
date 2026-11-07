"""Build a local macOS launcher, not a standalone/distributable application.

Run: .venv/bin/python scripts/build_local_app.py
The launcher uses this project's existing interpreter and never installs packages.
"""
import json
from pathlib import Path
import plistlib
import shlex
import shutil


ROOT = Path(__file__).resolve().parents[1]


def build(root=ROOT, output=None):
    root = Path(root).resolve()
    if not (root/'main.py').is_file() or not (root/'.venv/bin/python').exists():
        raise ValueError('缺少 main.py 或 .venv/bin/python；请先安装项目环境。')
    bundle = Path(output or root/'dist').resolve()/'XHS 资料工作台.app'
    marker = bundle/'Contents/Resources/local-launcher.json'
    if bundle.exists() and not marker.is_file():
        raise FileExistsError('目标不是本项目生成的启动器，拒绝覆盖：' + str(bundle))
    executable = bundle/'Contents/MacOS/launch-workbench'
    executable.parent.mkdir(parents=True,exist_ok=True)
    marker.parent.mkdir(parents=True,exist_ok=True)
    icon = root/'static/XHS-Downloader.icns'
    info = {'CFBundleName':'XHS 资料工作台', 'CFBundleDisplayName':'XHS 资料工作台',
            'CFBundleIdentifier':'local.xhs.archive.workbench', 'CFBundleVersion':'1',
            'CFBundleShortVersionString':'1.0', 'CFBundlePackageType':'APPL',
            'CFBundleExecutable':executable.name, 'NSHighResolutionCapable':True}
    if icon.is_file():
        shutil.copy2(icon, marker.parent/'workbench.icns')
        info['CFBundleIconFile']='workbench.icns'
    with (bundle/'Contents/Info.plist').open('wb') as file:
        plistlib.dump(info,file)
    script = '#!/bin/zsh\n' + 'ROOT=' + shlex.quote(str(root)) + '\n' + '''umask 077
if [[ ! -x "$ROOT/.venv/bin/python" || ! -f "$ROOT/main.py" ]]; then
    /usr/bin/osascript -e 'display alert "XHS 工作台无法启动" message "项目已移动或运行环境不存在。请在原项目目录重新安装环境并生成本地启动器。"'
    exit 1
fi
cd -- "$ROOT" || exit 1
mkdir -p Volume || exit 1
"$ROOT/.venv/bin/python" "$ROOT/main.py" WORKBENCH >> Volume/gui-launch.log 2>&1
status_code=$?
if (( status_code != 0 )); then
    /usr/bin/osascript -e 'display alert "XHS 工作台启动异常" message "请查看项目目录中的 Volume/gui-launch.log。此启动器不会自动安装依赖或开始采集。"'
fi
exit $status_code
'''
    executable.write_text(script)
    executable.chmod(0o755)
    marker.write_text(json.dumps({'project':str(root),'standalone':False},ensure_ascii=False))
    return bundle


if __name__ == '__main__':
    print(build())
