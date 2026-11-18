"""真实多进程端到端冒烟（无 GUI，走真实 UDP socket 与独立进程）。

拓扑与正式运行完全相同：
  headless 预测内核(UDP) -> proxy 进程 -> server 进程
通过 proxy 的 TCP 控制通道真实注入延迟、乱序和突发丢包。
输入由本测试按墙上时钟逐 tick 产生，不预读未来输入。

运行：python3 -m netmove.run_smoke
"""

import json
import math
import socket
import subprocess
import sys
import time
from pathlib import Path

from . import sim
from .client_kernel import PredictionKernel, UdpTransport

ROOT = Path(__file__).resolve().parent.parent


def wait_ctrl(timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            s = socket.create_connection(("127.0.0.1", 7070), timeout=0.5)
            s.close()
            return True
        except OSError:
            time.sleep(0.1)
    return False


class CtrlClient:
    def __init__(self):
        self.sock = socket.create_connection(("127.0.0.1", 7070), timeout=5)
        self.sock.setblocking(False)
        self.buf = b""
        self.snap = {}

    def send(self, obj):
        self.sock.sendall((json.dumps(obj) + "\n").encode())

    def pump(self):
        try:
            data = self.sock.recv(65535)
            if data:
                self.buf += data
        except BlockingIOError:
            pass
        while b"\n" in self.buf:
            line, self.buf = self.buf.split(b"\n", 1)
            self.snap = json.loads(line)


def main():
    procs = []
    try:
        for mod in ("netmove.proxy", "netmove.server"):
            procs.append(subprocess.Popen(
                [sys.executable, "-m", mod], cwd=ROOT,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True))
        # 等待代理控制通道就绪（两个进程均已绑定端口）
        if not wait_ctrl():
            raise RuntimeError("等待 proxy 控制通道超时")
        time.sleep(0.3)

        ctrl = CtrlClient()
        ctrl.send({"a": "set", "delay_ms": 150, "jitter_ms": 60,
                   "loss_p": 0.1, "reorder_p": 0.15})
        ctrl.pump()

        kernel = PredictionKernel(UdpTransport())
        # 阶段 1：向右移动 30 tick，期间触发一次手动突发丢包
        for i in range(30):
            kernel.produce_input(1, 0)
            kernel.pump_acks()
            ctrl.pump()
            if i == 10:
                ctrl.send({"a": "burst", "n": 5})
            time.sleep(sim.TICK_DT)
        # 阶段 2：斜向移动，逼近墙体，验证碰撞在两端一致
        for i in range(40):
            kernel.produce_input(1, 1)
            kernel.pump_acks()
            ctrl.pump()
            time.sleep(sim.TICK_DT)
        # 阶段 3：停止操作；把随机丢包降为 0（保留延迟/乱序），
        # 使缺失输入一定能靠重传补齐 —— 收敛条件可解释、可复现。
        ctrl.send({"a": "set", "loss_p": 0.0})
        for i in range(60):
            kernel.produce_input(0, 0)
            kernel.pump_acks()
            ctrl.pump()
            time.sleep(sim.TICK_DT)

        # 阶段 4：不再产生任何新输入，只重传未确认部分并接收 ack，
        # 排空在途报文，直到 pending 清空（带超时保护）。
        deadline = time.monotonic() + 8.0
        while time.monotonic() < deadline:
            kernel.idle_tick()
            ctrl.pump()
            if not kernel.pending:
                break
            time.sleep(sim.TICK_DT)

        snap = kernel.snapshot()
        stats = ctrl.snap.get("stats", {})
        cfg = ctrl.snap.get("cfg", {})

        print("\n===== 冒烟结果 =====")
        print("代理配置:", json.dumps(cfg, ensure_ascii=False))
        print("代理统计:", json.dumps(stats, ensure_ascii=False))
        print("客户端:", sim.state_hash(snap["state"]),
              "ack_seq=%s pending=%d" % (snap["ack_seq"], len(snap["pending"])))
        if snap["authoritative"]:
            print("服务端权威:", sim.state_hash(snap["authoritative"]))
        print("服务端 gap=%s 缺口起点=%s 已补齐缺失序号=%s"
              % (snap["server_gap"], snap.get("server_gaps"),
                 snap["server_drops"]))
        print("历史最大预测误差: %.2fpx" % snap["max_error"])
        print("ack 到达序(后10):", snap["ack_order"][-10:])

        failures = []
        if len(snap["pending"]) != 0:
            failures.append("pending 未清空(%d)" % len(snap["pending"]))
        auth = snap["authoritative"]
        if auth is None:
            failures.append("从未收到权威 ack")
        else:
            d = math.hypot(snap["state"]["x"] - auth["x"],
                           snap["state"]["y"] - auth["y"])
            if d > 1e-6:
                failures.append("停止后未收敛到权威位置(偏差%.3f)" % d)
        if stats.get("up_dropped", 0) == 0:
            failures.append("代理未观察到任何丢包，损伤注入未生效")
        if stats.get("reorders", 0) == 0:
            failures.append("未观察到乱序事件")
        if not (snap["server_drops"] or snap.get("server_gaps")):
            failures.append("服务端未记录任何缺口/补偿事件")
        # 碰撞：终点不得嵌墙
        st = snap["state"]
        for (wx, wy, ww, wh) in sim.WALLS:
            if (st["x"] + sim.RADIUS > wx and st["x"] - sim.RADIUS < wx + ww
                    and st["y"] + sim.RADIUS > wy
                    and st["y"] - sim.RADIUS < wy + wh):
                failures.append("终点嵌入墙体 %s" % str((wx, wy, ww, wh)))

        if failures:
            print("SMOKE FAIL:", "; ".join(failures))
            return 1
        print("SMOKE PASS: 停止操作后收敛到服务器认可位置，"
              "丢包/乱序补偿与碰撞均符合预期。")
        return 0
    finally:
        for p in procs:
            p.terminate()
        for p in procs:
            try:
                p.wait(timeout=5)
                out = p.stdout.read() if p.stdout else ""
            except subprocess.TimeoutExpired:
                p.kill()
                out, _ = p.communicate()
            print("\n--- %s 日志（尾部）---" % p.args[-1])
            if out:
                lines = out.strip().splitlines()
                print("\n".join(lines[-15:]))


if __name__ == "__main__":
    raise SystemExit(main())
