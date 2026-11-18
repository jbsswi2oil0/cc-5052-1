"""权威服务端进程。

职责：
1. 以自己的权威状态逐条处理带序号输入，同 seq 重放只生效一次（去重）。
2. 维护“期望序号”，乱序先入缓冲；缺口可观察：缺号期间不猜测用户输入，
   冻结模拟推进并在 ack 中置 gap=True，客户端重传补齐后严格按序补执行。
   （补偿政策：等待 + 去重 + 按序重放，全程写日志，界面可见 gap/drops。）
3. 每 tick 发回完整权威状态（位置 + 已确认序号 + 缺口标志）。

启动：python3 -m netmove.server
"""

import socket
import time

from . import sim
from .protocol import (
    PROXY_SERVER_PORT, LOOPBACK, encode, decode, ack_packet,
    SERVER_PROXY_PORT,
)


class AuthoritativeServer:
    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((LOOPBACK, PROXY_SERVER_PORT))
        self.sock.setblocking(False)
        self.proxy_addr = (LOOPBACK, SERVER_PROXY_PORT)

        self.state = sim.initial_state()
        self.next_seq = 0            # 期望的下一个连续输入序号
        self.buffer = {}             # seq -> 输入载荷（乱序/提前到达暂存）
        self.applied = set()         # 已生效过的 seq，重发绝不二走
        self.drops_seen = []         # 曾缺失、后来补上的序号（补偿可观察）
        self.gaps_open = []          # 观察到的缺口事件（首个缺失序号）
        self._gap_announced = None   # 已通告过的缺口起点，避免重复记日志
        self.last_applied = -1
        self.gap = False

    def _drain_buffer(self, log):
        """按序号顺序消费缓冲；遇到缺口立即停下并保持 gap 标志。"""
        while self.next_seq in self.buffer:
            seq = self.next_seq
            payload = self.buffer.pop(seq)
            if seq in self.applied:
                self.next_seq += 1
                continue
            # 两种情况都算“缺失后补齐”，补偿可观察：
            #  a) 序号跨过上一条已应用输入（中间缺过）；
            #  b) 该序号之前已被通告为缺口起点。
            if seq > self.last_applied + 1 or seq in self.gaps_open:
                self.drops_seen.append(seq)
                log.append("server: 补偿缺失输入 seq=%d，按序补执行 -> %s"
                           % (seq, sim.state_hash(self.state)))
            self.state = sim.step(self.state, payload)
            self.applied.add(seq)
            self.last_applied = seq
            self.next_seq += 1
        # 见过更远的序号、但期望序号还没到 => 正处于缺口等待
        self.gap = bool(self.buffer) and min(self.buffer.keys()) > self.next_seq
        if self.gap:
            if self._gap_announced != self.next_seq:
                self._gap_announced = self.next_seq
                self.gaps_open.append(self.next_seq)
                log.append("server: 检测到缺口，等待缺失输入 seq=%d（冻结推进）"
                           % self.next_seq)
        else:
            self._gap_announced = None

    def handle_packet(self, raw, log):
        try:
            pkt = decode(raw)
        except Exception:
            return
        if pkt.get("t") != "in":
            return
        seq = int(pkt["seq"])

        if seq in self.applied or seq < self.next_seq:
            # 同一输入重发：服务器不会多走一次，仅靠 ack 再次确认
            log.append("server: 重复 seq=%d 去重，未再执行模拟步" % seq)
            return
        if seq in self.buffer:
            return

        self.buffer[seq] = {"dx": pkt["dx"], "dy": pkt["dy"]}
        if seq != self.next_seq:
            log.append("server: seq=%d 提前/乱序到达（期望 %d），入缓冲等待"
                       % (seq, self.next_seq))
        self._drain_buffer(log)

    def send_ack(self):
        # 不变量：服务端模拟步 == 已应用输入条数 == last_applied + 1
        pkt = ack_packet(
            ack_seq=self.last_applied,
            state=self.state,
            gap=self.gap,
            applied_count=len(self.applied),
            drop_seen=list(self.drops_seen[-8:]),
            gaps_open=list(self.gaps_open[-8:]),
        )
        self.sock.sendto(encode(pkt), self.proxy_addr)

    def run(self):
        print("[server] 权威服务端监听 udp/%d, 心跳 %dHz"
              % (PROXY_SERVER_PORT, sim.TICK_HZ), flush=True)
        log = []
        next_tick = time.monotonic()
        while True:
            now = time.monotonic()
            while True:
                try:
                    raw, _ = self.sock.recvfrom(65535)
                except BlockingIOError:
                    break
                self.handle_packet(raw, log)

            if now >= next_tick:
                # 缺口期间不推进模拟，只重复广播 gap 状态（补偿等待可观察）
                self.send_ack()
                next_tick += sim.TICK_DT
                if next_tick < now:
                    next_tick = now + sim.TICK_DT

            for line in log:
                print(line, flush=True)
            log.clear()
            time.sleep(0.005)


if __name__ == "__main__":
    AuthoritativeServer().run()
