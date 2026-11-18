"""客户端预测-校正内核（纯逻辑，无 GUI 依赖）。

设计约束（对应任务要求）：
* 本地输入在产生的同一模拟步立即模拟，不等网络 —— 客户端预测。
* 每条输入带单调 seq，全量进入 pending 队列；收到权威 ack 后移除已确认部分，
  把模拟状态（含碰撞体坐标）整体重置为权威快照，再重放剩余未确认输入。
  显示与碰撞共用 sim.state 的同一坐标，校正不会只拉显示模型。
* 重发策略：每个 tick 发送最新输入；pending 中较老的输入周期性重发，
  服务端按 seq 去重，所以重发不会让服务器多走模拟步。
* 内核不做画面插值：render 平滑是 UI 层的事，sim 坐标永远是模拟真相。
"""

import math
import time

from . import sim


class KernelTransport:
    """真实传输由 UdpTransport 实现；测试可注入内存假对象。"""

    def send_input(self, seq, dx, dy, retrans=False):
        raise NotImplementedError

    def recv_ack(self):
        """非阻塞取一条 ack dict，没有则返回 None。"""
        raise NotImplementedError


class PredictionKernel:
    RESEND_EVERY = 3       # 每 N tick 全量重发一次 pending（丢包补偿）
    LINK_TIMEOUT = 1.0     # 超过此时长没收到任何 ack => 链路告警

    def __init__(self, transport):
        self.tr = transport
        self.state = sim.initial_state()      # 本地模拟状态（= 碰撞体坐标）
        self.pending = []                     # [{seq, inp, sent, retrans}]
        self.next_seq = 0
        self.tick_count = 0

        # 最新权威信息
        self.ack_seq = -1
        self.authoritative = None             # 最近一次服务端状态快照
        self.server_gap = False
        self.server_drops = []
        self.server_gaps = []

        # 可观察指标
        self.last_correction = None           # {t, dx, dy, dist, from, to}
        self.pred_error = None                # 重放前预测位置与权威位置的偏差
        self.max_error = 0.0
        self.last_ack_time = time.monotonic()
        self.ack_arrival_order = []           # 实际收到 ack 的到达序（可乱序）
        self._last_ack = None
        self.events = []

    def _log(self, msg):
        self.events.append("%s %s" % (time.strftime("%H:%M:%S"), msg))
        del self.events[:-30]

    # ------------------------------------------------------------------
    def produce_input(self, dx, dy):
        """UI 在每个固定模拟步调一次：产生输入并立即本地预测。"""
        seq = self.next_seq
        self.next_seq += 1
        inp = {"dx": dx, "dy": dy}

        # 1) 立即本地模拟（客户端预测，零本地延迟）
        self.state = sim.step(self.state, inp)

        # 2) 入未确认队列并立刻首发
        entry = {"seq": seq, "inp": inp, "sent": 0, "retrans": 0}
        self.pending.append(entry)
        self.tr.send_input(seq, dx, dy)
        entry["sent"] += 1
        self.tick_count += 1

        # 3) 周期性全量重发未确认部分（服务端去重，幂等）
        if self.tick_count % self.RESEND_EVERY == 0:
            self._retransmit()

        # 4) 顺手处理到达的 ack（真实使用时 UI 也会每帧泵一次）
        self.pump_acks()
        return seq

    def idle_tick(self):
        """不产生新输入的一拍：只做周期重传与 ack 泵取。

        用于“停止操作后排空在途报文”，保证收敛过程不引入新序号。
        """
        self.tick_count += 1
        if self.tick_count % self.RESEND_EVERY == 0:
            self._retransmit()
        self.pump_acks()

    def _retransmit(self):
        if not self.pending:
            return
        for entry in self.pending:
            inp = entry["inp"]
            self.tr.send_input(entry["seq"], inp["dx"], inp["dy"],
                               retrans=True)
            entry["sent"] += 1
            entry["retrans"] += 1
        self._log("重发未确认输入 seq=%d..%d（共 %d 条，服务端按 seq 去重）"
                  % (self.pending[0]["seq"], self.pending[-1]["seq"],
                     len(self.pending)))

    # ------------------------------------------------------------------
    def pump_acks(self):
        """收尽传输层当前缓存的 ack，逐条处理（乱序到达也安全）。"""
        while True:
            pkt = self.tr.recv_ack()
            if pkt is None:
                break
            self._handle_ack(pkt)

    def _handle_ack(self, pkt):
        ack = int(pkt["ack"])
        now = time.monotonic()
        self.last_ack_time = now
        self.server_gap = bool(pkt.get("gap"))
        self.server_drops = list(pkt.get("drops", []))
        self.server_gaps = list(pkt.get("gaps", []))
        self.ack_arrival_order.append(ack)
        del self.ack_arrival_order[:-16]

        # 乱序/重复到达的旧 ack 不能回滚确认点
        if ack <= self.ack_seq:
            self._log("ack=%d 迟到/重复，确认点保持 %d（到达序≠输入序）"
                      % (ack, self.ack_seq))
            # 确认点没推进但服务端报 gap：有输入没到，立即补一次重发
            if self.server_gap and self.pending:
                self._retransmit()
            return
        self.ack_seq = ack
        self._last_ack = pkt

        before = dict(self.state)
        # 权威快照：显示与碰撞体一起回到服务端认可的位置
        auth = {"x": float(pkt["x"]), "y": float(pkt["y"]),
                "step": int(pkt["sstep"])}
        self.authoritative = auth

        # 预测误差：重置前，本地预测位置与权威位置的差距
        err = math.hypot(before["x"] - auth["x"], before["y"] - auth["y"])
        self.pred_error = err
        self.max_error = max(self.max_error, err)

        # 移除已确认输入，保留未确认部分
        kept = [e for e in self.pending if e["seq"] > ack]
        confirmed = len(self.pending) - len(kept)
        self.pending = kept

        # 校正：权威状态 + 重新执行尚未确认的输入（确定性重放）
        self.state = dict(auth)
        for entry in self.pending:
            self.state = sim.step(self.state, entry["inp"])

        corr_dx = self.state["x"] - before["x"]
        corr_dy = self.state["y"] - before["y"]
        corr = math.hypot(corr_dx, corr_dy)
        self.last_correction = {
            "t": now, "dx": corr_dx, "dy": corr_dy, "dist": corr,
            "from": (before["x"], before["y"]),
            "to": (self.state["x"], self.state["y"]),
            "confirmed": confirmed, "replayed": len(self.pending),
        }
        if err > 1e-6 or corr > 1e-6:
            self._log("校正 ack=%d: 预测误差 %.2fpx，重放 %d 条，移动 %.2fpx"
                      % (ack, err, len(self.pending), corr))
        else:
            self._log("ack=%d 确认 %d 条，预测与权威一致，无需可见校正"
                      % (ack, confirmed))

        # 服务端仍在等缺口：立刻重发剩余未确认输入，加速收敛
        if self.server_gap and self.pending:
            self._retransmit()

    # ------------------------------------------------------------------
    def link_degraded(self):
        return (time.monotonic() - self.last_ack_time) > self.LINK_TIMEOUT

    def snapshot(self):
        """UI 每帧读取的只读视图。"""
        return {
            "state": dict(self.state),
            "pending": [dict(e) for e in self.pending],
            "next_seq": self.next_seq,
            "tick": self.tick_count,
            "ack_seq": self.ack_seq,
            "authoritative": dict(self.authoritative) if self.authoritative else None,
            "server_gap": self.server_gap,
            "server_drops": list(self.server_drops),
            "server_gaps": list(self.server_gaps),
            "pred_error": self.pred_error,
            "max_error": self.max_error,
            "last_correction": dict(self.last_correction) if self.last_correction else None,
            "degraded": self.link_degraded(),
            "ack_order": list(self.ack_arrival_order),
            "events": list(self.events[-8:]),
        }


class UdpTransport(KernelTransport):
    """真实 UDP 传输：经 proxy 上行、从 proxy 收下行。"""

    def __init__(self, proxy_host="127.0.0.1", up_port=7051, down_port=7061):
        import socket as _socket
        from .protocol import input_packet, encode, decode, LOOPBACK
        proxy_host = proxy_host or LOOPBACK
        self._encode_input = input_packet
        self._encode = encode
        self._decode = decode
        self.sock = _socket.socket(_socket.AF_INET, _socket.SOCK_DGRAM)
        self.sock.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEADDR, 1)
        self.sock.bind((proxy_host, down_port))
        self.sock.setblocking(False)
        self.proxy = (proxy_host, up_port)

    def send_input(self, seq, dx, dy, retrans=False):
        pkt = self._encode_input(seq, dx, dy, retrans=retrans)
        self.sock.sendto(self._encode(pkt), self.proxy)

    def recv_ack(self):
        try:
            raw, _ = self.sock.recvfrom(65535)
        except BlockingIOError:
            return None
        try:
            pkt = self._decode(raw)
        except Exception:
            return None
        return pkt if pkt.get("t") == "ack" else None
