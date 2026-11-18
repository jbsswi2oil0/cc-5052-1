"""一键启动器：以真实多进程方式拉起 proxy / server / client UI。

用法：
    python3 -m netmove.launch              # 启动完整联机窗口
    python3 -m netmove.launch --headless   # 无头冒烟（SDL dummy 驱动，CI 可用）

关闭窗口后本脚本会终止 proxy 与 server 子进程并回收。
也可以分三个终端手动启动（顺序任意，client 会自动重连控制通道）：
    python3 -m netmove.proxy
    python3 -m netmove.server
    python3 -m netmove.client_ui
"""

import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MODULES = [
    ("proxy", "netmove.proxy"),
    ("server", "netmove.server"),
]


def main():
    headless = "--headless" in sys.argv
    procs = []
    try:
        for name, mod in MODULES:
            p = subprocess.Popen(
                [sys.executable, "-m", mod],
                cwd=ROOT,
            )
            procs.append((name, p))
            time.sleep(0.35)  # 给绑定端口留时间，client 自身也有重连兜底

        env_extra = {}
        if headless:
            env_extra["SDL_VIDEODRIVER"] = "dummy"
            env_extra["SDL_AUDIODRIVER"] = "dummy"
        client = subprocess.Popen(
            [sys.executable, "-m", "netmove.client_ui"]
            + (["--smoke"] if headless else []),
            cwd=ROOT,
            env={**dict(os.environ), **env_extra},
        )
        rc = client.wait()
        print("[launch] client 退出码 %d，正在收尾子进程…" % rc)
    finally:
        for name, p in procs:
            if p.poll() is None:
                p.terminate()
        for name, p in procs:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()
            print("[launch] %s 已停止" % name)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
