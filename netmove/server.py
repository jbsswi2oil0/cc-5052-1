"""权威服务端进程。

职责：
1. 版本协商：hello/welcome 选定 v1（单个连续序号确认）或
   v2（带缺口的确认区间 + 显式纪元），见 migrate.py。
2. v1 旧行为：维护“期望序号”，乱序先入缓冲；缺口期间冻结推进并置
   gap=True，客户端重传补齐后严格按序补执行。
   v2 新行为：到达即结算（每条输入恰好生效一次），已结算集合用
   带缺口的区间随 ack 通告；缺口稍后被补齐时记入 drops 可观察。
3. 每 tick 发回完整权威状态（位置 + 确认视图 + 缺口标志）。
4. 应答 fsreq：旧端在 migrate.RESYNC_RULES 情形下请求完整状态时，
   回以无歧义的权威快照 + 当前确认区间。

SessionCore 是纯逻辑（无 socket），selftest 与回放直接复用，
AuthoritativeServer 只负责收发与心跳。

启动：python3 -m netmove.server
环境变量：NETMOVE_SERVER_VERSIONS=1   只支持旧版（混版演示）
          NETMOVE_SEQ_MOD=16        自定义序号回绕模数
"""

import os
import socket
import time

from . import sim, migrate
from .protocol import (
    PROXY_SERVER_PORT, LOOPBACK, encode, decode, ack_packet,
    SERVER_PROXY_PORT,
    PROTO_LEGACY, PROTO_RANGES, SUPPORTED_VERSIONS, SUPPORTED_CAPS,
    DEFAULT_SEQ_MOD, welcome_packet, full_state_packet, resolve_short,
)


class SessionCore:
    """一条会话的权威逻辑：协商、输入结算、确认视图导出。"""

    def __init__(self, server_versions=SUPPORTED_VERSIONS,
                 server_caps=SUPPORTED_CAPS, seq_mod=DEFAULT_SEQ_MOD):
        self.server_versions = tuple(server_versions)
        self.server_caps = tuple(server_caps)
        self.server_seq_mod = int(seq_mod)

        # 协商结果（收到 hello 前按 v1 旧行为，兼容未协商就发输入的旧端）
        self.version = PROTO_LEGACY
        self.caps = set()
        self.seq_mod = self.server_seq_mod
        self.session = 0

        self.state = sim.initial_state()
        self.prefix = -1           # 已连续结算到的完整序号
        self.extras = set()        # v2：前缀之上已结算的散点（缺口桥）
        self.buffer = {}           # v1：乱序/提前到达暂存 full_seq -> 载荷
        self.applied_count = 0
        self.last_applied = -1
        self.max_applied = -1
        self.drops_seen = []       # 曾缺失、后来补上的序号（补偿可观察）
        self.gaps_open = []        # 观察到的缺口事件（首个缺失序号）
        self._gap_announced = None
        self.gap = False

    # ------------------------------------------------------------------
    def negotiate(self, hello, log):
        result = migrate.negotiate(hello, self.server_versions,
                                   self.server_caps, self.server_seq_mod)
        if result["version"] is None:
            result["version"] = PROTO_LEGACY
            log.append("server: 无共同版本，按 v1 旧行为兜底")
        self.version = result["version"]
        self.caps = set(result["caps"])
        self.seq_mod = result["seq_mod"]
        self.session += 1
        log.append("server: 协商完成 v%d caps=%s seq_mod=%d (session=%d)"
                   % (self.version, sorted(self.caps), self.seq_mod,
                      self.session))
        return welcome_packet(self.version, self.caps, self.seq_mod,
                              self.session)

    # ------------------------------------------------------------------
    def handle_packet_json(self, pkt, log):
        """处理一条上行报文，返回需要立即下发的报文列表。"""
        t = pkt.get("t")
        if t == "hello":
            return [self.negotiate(pkt, log)]
        if t == "fsreq":
            log.append("server: 收到完整状态请求 reason=%s，回以权威快照"
                       % pkt.get("reason"))
            return [self.make_full()]
        if t != "in":
            return []
        short = int(pkt["seq"])
        payload = {"dx": pkt["dx"], "dy": pkt["dy"]}
        if self.version == PROTO_RANGES:
            self._handle_input_v2(pkt, short, payload, log)
        else:
            self._handle_input_v1(short, payload, log)
        return []

    # ---- v1：冻结式按序结算 ------------------------------------------
    def _handle_input_v1(self, short, payload, log):
        # 消歧参考用“已连续结算的前缀”：缺口补齐包（如乱序迟到的 seq 0）
        # 仍落在前缀前向半窗内，能被正确收下；只有已应用的才算重复。
        full, kind = resolve_short(short, self.prefix, self.seq_mod)
        if kind == "ambiguous":
            log.append("server: seq=%d 落在回绕边界，丢弃等待重传" % short)
            return
        if kind != "new":
            log.append("server: 重复 seq=%d 去重，未再执行模拟步" % short)
            return
        if full in self.buffer:
            return                      # 已缓冲的乱序副本
        self.buffer[full] = payload
        if full != self.prefix + 1:
            log.append("server: seq=%d 提前/乱序到达（期望 %d），入缓冲等待"
                       % (full, self.prefix + 1))
        self._drain_buffer_v1(log)

    def _drain_buffer_v1(self, log):
        """按序号顺序消费缓冲；遇到缺口立即停下并保持 gap 标志。"""
        while self.prefix + 1 in self.buffer:
            seq = self.prefix + 1
            payload = self.buffer.pop(seq)
            if seq in self.gaps_open:
                self.drops_seen.append(seq)
                log.append("server: 补偿缺失输入 seq=%d，按序补执行 -> %s"
                           % (seq, sim.state_hash(self.state)))
            self.state = sim.step(self.state, payload)
            self.last_applied = seq
            self.max_applied = seq
            self.prefix = seq
            self.applied_count += 1
        self.gap = bool(self.buffer) and min(self.buffer) > self.prefix + 1
        if self.gap:
            missing = self.prefix + 1
            if self._gap_announced != missing:
                self._gap_announced = missing
                self.gaps_open.append(missing)
                log.append("server: 检测到缺口，等待缺失输入 seq=%d（冻结推进）"
                           % missing)
        else:
            self._gap_announced = None

    # ---- v2：到达即结算，区间确认 --------------------------------------
    def _handle_input_v2(self, pkt, short, payload, log):
        if "ep" in pkt:
            # 显式纪元：完整序号无歧义，这是 v2 消除回绕问题的根本手段
            full = int(pkt["ep"]) * self.seq_mod + short
        else:
            full, kind = resolve_short(short, self.prefix, self.seq_mod)
            if kind != "new":
                log.append("server: 重复/越界 seq=%d 去重" % short)
                return

        if full <= self.prefix or full in self.extras:
            log.append("server: 重复 seq=%d 去重，未再执行模拟步" % full)
            return

        # 越过缺口先结算后续输入：记录缺口起点（首个缺失序号）
        missing = self.prefix + 1
        if full > missing and self._gap_announced != missing:
            self._gap_announced = missing
            self.gaps_open.append(missing)
            log.append("server: 检测到缺口 seq=%d，先结算后续输入 seq=%d"
                       % (missing, full))
        if full < self.max_applied:
            # 补到的是此前缺口的迟到输入
            self.drops_seen.append(full)
            log.append("server: 补偿缺失输入 seq=%d，迟到结算 -> %s"
                       % (full, sim.state_hash(self.state)))

        self.state = sim.step(self.state, payload)
        self.extras.add(full)
        self.applied_count += 1
        self.last_applied = full
        self.max_applied = max(self.max_applied, full)
        # 缺口被补齐时把散点卷入前缀
        while self.prefix + 1 in self.extras:
            self.prefix += 1
            self.extras.discard(self.prefix)
        self.gap = bool(self.extras)
        if not self.gap:
            self._gap_announced = None

    # ------------------------------------------------------------------
    def _ack_view(self):
        return migrate.AckView(self.prefix, self.extras)

    def make_ack(self):
        """当前确认报文。v1 只发连续前缀短序号；v2 加纪元与确认区间。"""
        view = self._ack_view()
        ep = None
        ranges = None
        if self.version == PROTO_RANGES:
            ep = view.prefix // self.seq_mod if view.prefix >= 0 else 0
            ranges = view.as_ranges(0)[-8:]
        return ack_packet(
            ack_seq=view.prefix % self.seq_mod,
            state=self.state,
            gap=self.gap,
            applied_count=self.applied_count,
            drop_seen=[s % self.seq_mod for s in self.drops_seen[-8:]],
            gaps_open=[s % self.seq_mod for s in self.gaps_open[-8:]],
            ep=ep, ranges=ranges,
        )

    def make_full(self):
        """完整状态应答：权威快照 + 当前确认区间（无歧义）。"""
        view = self._ack_view()
        return full_state_packet(
            state=self.state,
            ack_seq=view.prefix % self.seq_mod,
            ep=view.prefix // self.seq_mod if view.prefix >= 0 else 0,
            ranges=view.as_ranges(0),
            gap=self.gap,
        )


class AuthoritativeServer:
    def __init__(self, core=None):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((LOOPBACK, PROXY_SERVER_PORT))
        self.sock.setblocking(False)
        self.proxy_addr = (LOOPBACK, SERVER_PROXY_PORT)
        self.core = core or SessionCore()

    def handle_packet(self, raw, log):
        try:
            pkt = decode(raw)
        except Exception:
            return
        for resp in self.core.handle_packet_json(pkt, log):
            self.sock.sendto(encode(resp), self.proxy_addr)

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
                # 缺口期间 v1 冻结推进、v2 已结算部分保持，
                # 两种模式都重复广播当前确认视图（补偿等待可观察）
                self.sock.sendto(encode(self.core.make_ack()), self.proxy_addr)
                next_tick += sim.TICK_DT
                if next_tick < now:
                    next_tick = now + sim.TICK_DT

            for line in log:
                print(line, flush=True)
            log.clear()
            time.sleep(0.005)


def _env_versions():
    raw = os.environ.get("NETMOVE_SERVER_VERSIONS")
    if not raw:
        return SUPPORTED_VERSIONS
    return tuple(int(x) for x in raw.split(",") if x.strip())


if __name__ == "__main__":
    core = SessionCore(server_versions=_env_versions(),
                       seq_mod=int(os.environ.get("NETMOVE_SEQ_MOD",
                                                  DEFAULT_SEQ_MOD)))
    AuthoritativeServer(core).run()
