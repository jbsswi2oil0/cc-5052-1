"""本地网络损伤代理进程（真实 UDP 转发，非模拟回调）。

client 与 server 只信任自己看到的 proxy 地址，所有报文都从这里过，
因此延迟、抖动、乱序、丢包对两端都是真实的 socket 行为。

损伤模型（上下行独立统计）：
  delay_ms / jitter_ms : 基础延迟 + 均匀抖动
  reorder_p            : 命中的报文额外延后一个 delay 窗口，制造乱序
  loss_p               : 按概率随机丢包
  burst N              : 手动丢弃接下来 N 个上行输入报文（短时丢包演示）

另提供 TCP 控制通道（行分隔 JSON）：UI 下发参数，proxy 周期回传统计。

启动：python3 -m netmove.proxy
"""

import heapq
import json
import random
import socket
import time

from .protocol import (
    CLIENT_PROXY_PORT, PROXY_SERVER_PORT,
    PROXY_CLIENT_PORT, SERVER_PROXY_PORT,
    PROXY_CTRL_PORT, LOOPBACK, decode,
)


class Proxy:
    def __init__(self, seed=20261118):
        self.rng = random.Random(seed)
        # 双 socket：一个收 client 上行(7051)，一个收 server 下行(7062)，
        # 端口与 client 的下行监听口 7061 严格错开，避免抢占绑定。
        self.up_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.up_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.up_sock.bind((LOOPBACK, CLIENT_PROXY_PORT))
        self.up_sock.setblocking(False)
        self.down_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.down_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.down_sock.bind((LOOPBACK, SERVER_PROXY_PORT))
        self.down_sock.setblocking(False)
        self.sockets = [self.up_sock, self.down_sock]

        self.ctrl = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.ctrl.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.ctrl.bind((LOOPBACK, PROXY_CTRL_PORT))
        self.ctrl.listen(4)
        self.ctrl.setblocking(False)
        self.ctrl_clients = []

        # 可调节损伤参数
        self.cfg = {
            "delay_ms": 120.0,
            "jitter_ms": 40.0,
            "loss_p": 0.0,
            "reorder_p": 0.0,
            "burst_left": 0,
        }
        self.heap = []  # (due_monotonic, raw_bytes, dst_addr, tag)
        self.stats = {
            "up_recv": 0, "up_sent": 0, "up_dropped": 0,
            "down_recv": 0, "down_sent": 0, "down_dropped": 0,
            "reorders": 0,
        }
        self.events = []          # 环形事件日志，UI 可见
        self._next_id = 0
        self._last_stats = 0.0

    # ---------- 事件日志 ----------
    def log(self, msg):
        line = "%s %s" % (time.strftime("%H:%M:%S"), msg)
        self.events.append(line)
        del self.events[:-40]

    # ---------- 损伤注入 ----------
    def _schedule(self, raw, dst, tag, direction):
        pkt = decode(raw)
        seq = pkt.get("seq", pkt.get("ack", "-"))

        if direction == "up":
            self.stats["up_recv"] += 1
            # 手动突发丢包优先
            if self.cfg["burst_left"] > 0:
                self.cfg["burst_left"] -= 1
                self.stats["up_dropped"] += 1
                self.log("BURST 丢弃上行 seq=%s（剩余 %d）"
                         % (seq, self.cfg["burst_left"]))
                return
            if self.rng.random() < self.cfg["loss_p"]:
                self.stats["up_dropped"] += 1
                self.log("随机丢弃上行 seq=%s" % seq)
                return
            sent_key = "up_sent"
        else:
            self.stats["down_recv"] += 1
            if self.rng.random() < self.cfg["loss_p"]:
                self.stats["down_dropped"] += 1
                self.log("随机丢弃下行 ack=%s" % seq)
                return
            sent_key = "down_sent"

        delay = self.cfg["delay_ms"] / 1000.0
        jitter = self.rng.uniform(-self.cfg["jitter_ms"],
                                  self.cfg["jitter_ms"]) / 1000.0
        due = time.monotonic() + max(0.0, delay + jitter)

        reordered = (self.rng.random() < self.cfg["reorder_p"]
                     and not pkt.get("rt"))
        if reordered:
            # 额外延后半个延迟窗口，使后发报文可能先到，
            # 又不至于让缺口长时间无法闭合（重传副本走快路径，见 rt 标记）
            due += max(0.02, self.cfg["delay_ms"] / 1000.0 * 0.5)
            self.stats["reorders"] += 1
            self.log("乱序注入 %s seq=%s 延后投递" % (tag, seq))

        self._next_id += 1
        # 报文从哪个 socket 进来就从哪个 socket 转发出去
        out_sock = self.up_sock if direction == "up" else self.down_sock
        heapq.heappush(self.heap,
                       (due, self._next_id, raw, dst, sent_key, out_sock))

    def _pump(self):
        now = time.monotonic()
        while self.heap and self.heap[0][0] <= now:
            _, _, raw, dst, sent_key, out_sock = heapq.heappop(self.heap)
            out_sock.sendto(raw, dst)
            self.stats[sent_key] += 1

    # ---------- 控制通道 ----------
    def _handle_ctrl(self):
        try:
            c, _ = self.ctrl.accept()
            c.setblocking(False)
            self.ctrl_clients.append(c)
            self.log("控制通道接入")
        except BlockingIOError:
            pass

        alive = []
        for c in self.ctrl_clients:
            try:
                data = c.recv(65535)
            except BlockingIOError:
                alive.append(c)
                continue
            except ConnectionError:
                c.close()
                continue
            if not data:
                c.close()
                continue
            for line in data.splitlines():
                if not line.strip():
                    continue
                try:
                    cmd = json.loads(line)
                except json.JSONDecodeError:
                    continue
                self._apply_cmd(cmd, c)
            alive.append(c)
        self.ctrl_clients = alive

    def _apply_cmd(self, cmd, sock):
        action = cmd.get("a")
        if action == "set":
            for k in ("delay_ms", "jitter_ms", "loss_p", "reorder_p"):
                if k in cmd:
                    self.cfg[k] = float(cmd[k])
            self.log("参数更新: " + json.dumps(self.cfg, ensure_ascii=False))
        elif action == "burst":
            n = max(0, int(cmd.get("n", 3)))
            self.cfg["burst_left"] += n
            self.log("手动突发丢包: 接下来 %d 个上行报文" % n)
        elif action == "reset_stats":
            for k in self.stats:
                self.stats[k] = 0
            self.events.clear()
            self.log("统计已清零")
        elif action == "get":
            pass
        self._push_snapshot(sock)

    def _push_snapshot(self, sock=None):
        snap = {"a": "stats", "cfg": self.cfg, "stats": self.stats,
                "events": self.events[-12:]}
        payload = (json.dumps(snap, ensure_ascii=False) + "\n").encode()
        targets = [sock] if sock else list(self.ctrl_clients)
        alive = []
        for c in targets:
            if c is None:
                continue
            try:
                c.sendall(payload)
                alive.append(c)
            except ConnectionError:
                c.close()
        if sock is None:
            keep = set(id(c) for c in alive)
            self.ctrl_clients = [c for c in self.ctrl_clients if id(c) in keep]

    # ---------- 主循环 ----------
    def run(self):
        print("[proxy] 损伤代理 上行 udp/%d 下行 udp/%d 控制 tcp/%d"
              % (CLIENT_PROXY_PORT, SERVER_PROXY_PORT, PROXY_CTRL_PORT),
              flush=True)
        while True:
            for sock in self.sockets:
                while True:
                    try:
                        raw, addr = sock.recvfrom(65535)
                    except BlockingIOError:
                        break
                    try:
                        pkt = decode(raw)
                    except Exception:
                        continue
                    if pkt.get("dir") == "u":
                        self._schedule(raw, (LOOPBACK, PROXY_SERVER_PORT),
                                       "上行", "up")
                    elif pkt.get("dir") == "d":
                        self._schedule(raw, (LOOPBACK, PROXY_CLIENT_PORT),
                                       "下行", "down")

            self._pump()
            self._handle_ctrl()

            now = time.monotonic()
            if now - self._last_stats >= 0.25:
                self._push_snapshot()
                self._last_stats = now
            time.sleep(0.002)


if __name__ == "__main__":
    Proxy().run()
