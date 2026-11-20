"""客户端预测-校正内核（纯逻辑，无 GUI 依赖）。

设计约束（对应任务要求）：
* 本地输入在产生的同一模拟步立即模拟，不等网络 —— 客户端预测。
* 每条输入带单调 seq，全量进入 pending 队列；收到权威确认后，按
  migrate.AckView（连续前缀 + 缺口散点）移除“已结算”的部分 ——
  缺口里的输入必须留在队列里继续重放，绝不能把确认区间末端当成
  连续前缀（否则斜坡中间一拍延迟时，那一拍移动会在校正中被丢掉）。
  随后把模拟状态整体重置为权威快照，再重放剩余未确认输入。
* 重发策略：每个 tick 发送最新输入；pending 中较老的输入周期性重发，
  服务端按 seq 去重，所以重发不会让服务器多走模拟步。
* 版本协商：启动时发送 hello，welcome 到达前按 v1 旧端行为工作；
  v2 会话里输入带显式纪元 ep，确认带 ranges；v1 会话只有短序号，
  跨回绕时用 RFC1982 窗口消歧。
* 旧端在 migrate.RESYNC_RULES 三种情形下必须请求完整状态（fsreq）。
* 内核不做画面插值：render 平滑是 UI 层的事，sim 坐标永远是模拟真相。
"""

import math
import time

from . import sim, migrate
from .protocol import (
    PROTO_LEGACY, PROTO_RANGES, CAP_RANGES, CAP_WRAP,
    DEFAULT_SEQ_MOD,
)


class KernelTransport:
    """真实传输由 UdpTransport 实现；测试可注入内存假对象。"""

    def send_hello(self, hello):
        raise NotImplementedError

    def send_input(self, seq, dx, dy, retrans=False, ep=None):
        raise NotImplementedError

    def send_fsreq(self, reason):
        raise NotImplementedError

    def recv_ack(self):
        """非阻塞取下一条下行报文（ack/welcome/full），没有则返回 None。"""
        raise NotImplementedError


class PredictionKernel:
    RESEND_EVERY = 3       # 每 N tick 全量重发一次 pending（丢包补偿）
    LINK_TIMEOUT = 1.0     # 超过此时长没收到任何 ack => 链路告警

    def __init__(self, transport, offered_versions=(PROTO_LEGACY, PROTO_RANGES),
                 offered_caps=(CAP_RANGES, CAP_WRAP),
                 seq_mod=DEFAULT_SEQ_MOD, resume=None):
        self.tr = transport

        # 协商状态（welcome 之前按 v1 旧端行为，报文两种格式都能收）
        self.version = PROTO_LEGACY
        self.caps = set()
        self.seq_mod = seq_mod
        self.session = None
        self._negotiated = False

        self.state = sim.initial_state()      # 本地模拟状态（= 碰撞体坐标）
        self.pending = []                     # [{seq_full, inp, owner, sent, retrans}]
        self.next_seq_full = 0
        self.tick_count = 0

        # 最新权威信息：统一确认视图（前缀 + 缺口散点）
        self.ack_view = migrate.AckView(-1)
        self.authoritative = None             # 最近一次服务端状态快照
        self._auth_step = -1
        self.server_gap = False
        self.server_drops = []
        self.server_gaps = []

        # 可观察指标
        self.last_correction = None           # {t, dx, dy, dist, from, to, holes}
        self.pred_error = None                # 重放前预测位置与权威位置的偏差
        self.max_error = 0.0
        self.last_ack_time = time.monotonic()
        self._rx_was_degraded = False
        self.ack_arrival_order = []           # 实际收到 ack 的到达序（可乱序）
        self.events = []
        self.rx_ack_pkts = []                 # 最近收到的原始确认（调试注入用）

        # 重同步（旧端必须请求完整状态的情形，见 migrate.RESYNC_RULES）
        self.fsreq_count = 0
        self.last_fsreq = None

        if resume is not None:
            self._restore(resume)
        else:
            self.tr.send_hello({
                "vers": list(offered_versions),
                "caps": list(offered_caps),
                "seq_mod": seq_mod,
            })

    def _log(self, msg):
        self.events.append("%s %s" % (time.strftime("%H:%M:%S"), msg))
        del self.events[:-30]

    # ------------------------------------------------------------------
    def produce_input(self, dx, dy):
        """UI 在每个固定模拟步调一次：产生输入并立即本地预测。"""
        seq_full = self.next_seq_full
        self.next_seq_full += 1
        inp = {"dx": dx, "dy": dy}

        # 1) 立即本地模拟（客户端预测，零本地延迟）
        self.state = sim.step(self.state, inp)

        # 2) 入未确认队列并立刻首发（owner=local：本端拥有的方向键输入）
        entry = {"seq_full": seq_full, "inp": inp, "owner": "local",
                 "sent": 0, "retrans": 0}
        self.pending.append(entry)
        self._send_entry(entry, retrans=False)
        self.tick_count += 1

        # 3) 周期性全量重发未确认部分（服务端去重，幂等）
        if self.tick_count % self.RESEND_EVERY == 0:
            self._retransmit()

        # 4) 顺手处理到达的下行报文（真实使用时 UI 也会每帧泵一次）
        self.pump_acks()
        return seq_full

    def idle_tick(self):
        """不产生新输入的一拍：只做周期重传与 ack 泵取。

        用于“停止操作后排空在途报文”，保证收敛过程不引入新序号。
        """
        self.tick_count += 1
        if self.tick_count % self.RESEND_EVERY == 0:
            self._retransmit()
        self.pump_acks()

    def _send_entry(self, entry, retrans):
        seq_full = entry["seq_full"]
        short = seq_full % self.seq_mod
        ep = seq_full // self.seq_mod if self.version == PROTO_RANGES else None
        inp = entry["inp"]
        self.tr.send_input(short, inp["dx"], inp["dy"],
                           retrans=retrans, ep=ep)
        entry["sent"] += 1
        if retrans:
            entry["retrans"] += 1

    def _retransmit(self):
        if not self.pending:
            return
        # v1 短序号在回绕后不可区分：超出确认点前向半窗的重传，对
        # 领先的服务端会被误当成下一轮的新输入（有害），对落后的
        # 服务端又必然落在去重窗口里（无用）。因此 v1 只重传半窗内
        # 的序号；v2 有显式纪元，无此限制。
        limit = None
        if self.version == PROTO_LEGACY:
            limit = self.ack_view.prefix + self.seq_mod // 2
        sent = 0
        for entry in self.pending:
            if limit is not None and entry["seq_full"] > limit:
                continue
            self._send_entry(entry, retrans=True)
            sent += 1
        if sent:
            self._log("重发未确认输入 seq=%d..%d（共 %d 条，服务端按 seq 去重）"
                      % (self.pending[0]["seq_full"],
                         self.pending[-1]["seq_full"], sent))

    # ------------------------------------------------------------------
    def pump_acks(self):
        """收尽传输层当前缓存的下行报文，逐条处理（乱序到达也安全）。"""
        while True:
            pkt = self.tr.recv_ack()
            if pkt is None:
                break
            self._handle_downstream(pkt)

    def _handle_downstream(self, pkt):
        t = pkt.get("t")
        if t in ("ack", "welcome", "full"):
            # 先记录“本条到达前链路是否曾退化”，再刷新计时，
            # 供 R3（超时恢复后的第一条确认）判定使用
            self._rx_was_degraded = self.link_degraded()
            self.last_ack_time = time.monotonic()
        if t == "welcome":
            self._handle_welcome(pkt)
        elif t == "ack":
            self._handle_ack(pkt)
        elif t == "full":
            self._handle_full(pkt)

    def _handle_welcome(self, pkt):
        self.version = int(pkt.get("v", PROTO_LEGACY))
        self.caps = set(pkt.get("caps", []))
        self.seq_mod = int(pkt.get("seq_mod", self.seq_mod))
        self.session = pkt.get("session")
        self._negotiated = True
        self._log("协商完成: v%d caps=%s seq_mod=%d session=%s"
                  % (self.version, sorted(self.caps), self.seq_mod,
                     self.session))

    def _handle_ack(self, pkt):
        # R3：链路超时后恢复的第一条确认 —— 会话纪元不再可信，
        # 必须请求完整状态（增量确认仍按单调规则照常处理）
        if self.ack_view.prefix >= 0 and self._rx_was_degraded:
            self._request_full_state(migrate.RESYNC_RECONNECT)

        self.server_gap = bool(pkt.get("gap"))
        self.server_drops = list(pkt.get("drops", []))
        self.server_gaps = list(pkt.get("gaps", []))
        self.ack_arrival_order.append(int(pkt["ack"]))
        del self.ack_arrival_order[:-16]
        self.rx_ack_pkts.append(dict(pkt))
        del self.rx_ack_pkts[:-8]

        view, kind = migrate.normalize_ack(pkt, self.ack_view.prefix,
                                           self.seq_mod)
        if kind == "ambiguous":
            # R1：短序号落在回绕边界，无法判定属于哪一轮
            self._request_full_state(migrate.RESYNC_EPOCH_UNKNOWN)
            return

        merged = self.ack_view.merge(view)

        # R2 在 stale 判定之前：缺口通告可能随不推进的重复确认反复到达。
        # 服务端报告的缺口里有本地不拥有的序号 —— 无法重传补齐
        if self.server_gap:
            owned = {e["seq_full"] for e in self.pending}
            if migrate.gap_not_owned(merged, pkt.get("gaps", []),
                                     owned, self.seq_mod):
                self._request_full_state(migrate.RESYNC_GAP_NOT_OWNED)

        if kind == "stale":
            self.ack_view = merged
            self._log("ack=%d 迟到/重复，确认点保持 %d（到达序≠输入序）"
                      % (int(pkt["ack"]), self.ack_view.prefix))
            if self.server_gap and self.pending:
                self._retransmit()
            return
        sstep = int(pkt.get("sstep", -1))
        if sstep < self._auth_step:
            self.ack_view = merged
            self._log("ack=%d 权威快照不新(sstep=%d)，仅合并确认区间"
                      % (int(pkt["ack"]), sstep))
            return
        self._auth_step = sstep
        self.ack_view = merged

        auth = {"x": float(pkt["x"]), "y": float(pkt["y"]),
                "step": sstep, "on_slope": int(pkt.get("oslp", 0))}
        self._reconcile(auth, confirmed_tag=int(pkt["ack"]))

    def _handle_full(self, pkt):
        """完整状态应答：无歧义地重建确认视图与权威快照。"""
        view, _ = migrate.normalize_ack(
            {"ack": pkt["ack"], "ep": pkt.get("ep", 0),
             "ranges": pkt.get("ranges", [])},
            self.ack_view.prefix, self.seq_mod)
        self.ack_view = self.ack_view.merge(view)
        self.server_gap = bool(pkt.get("gap"))
        sstep = int(pkt["sstep"])
        self._auth_step = sstep
        auth = {"x": float(pkt["x"]), "y": float(pkt["y"]),
                "step": sstep, "on_slope": int(pkt.get("oslp", 0))}
        kept_before = len(self.pending)
        self._reconcile(auth, confirmed_tag="full")
        self._log("完整状态恢复： 确认前缀=%d，丢弃旧确认 %d 条，保留待重放 %d 条"
                  % (self.ack_view.prefix,
                     kept_before - len(self.pending), len(self.pending)))

    def _reconcile(self, auth, confirmed_tag):
        """权威快照 + 未确认重放。确认按集合成员判定，缺口输入保留。"""
        before = dict(self.state)
        self.authoritative = auth

        err = math.hypot(before["x"] - auth["x"], before["y"] - auth["y"])
        self.pred_error = err
        self.max_error = max(self.max_error, err)

        kept = [e for e in self.pending
                if not self.ack_view.contains(e["seq_full"])]
        confirmed = len(self.pending) - len(kept)
        self.pending = kept

        # 缺口里尚未结算的输入（其上方已有被确认的散点）必须保留重放
        max_acked = self.ack_view.prefix
        if self.ack_view.extras:
            max_acked = max(max_acked, max(self.ack_view.extras))
        holes = [e["seq_full"] for e in self.pending
                 if e["seq_full"] < max_acked]

        self.state = dict(auth)
        for entry in self.pending:
            self.state = sim.step(self.state, entry["inp"])

        corr_dx = self.state["x"] - before["x"]
        corr_dy = self.state["y"] - before["y"]
        corr = math.hypot(corr_dx, corr_dy)
        self.last_correction = {
            "t": time.monotonic(), "dx": corr_dx, "dy": corr_dy, "dist": corr,
            "from": (before["x"], before["y"]),
            "to": (self.state["x"], self.state["y"]),
            "confirmed": confirmed, "replayed": len(self.pending),
            "holes": holes,
        }
        if holes:
            self._log("确认=%s 保留缺口输入 seq=%s（上方已有确认散点，继续重放）"
                      % (confirmed_tag, holes))
        if err > 1e-6 or corr > 1e-6:
            self._log("校正 ack=%s: 预测误差 %.2fpx，重放 %d 条，移动 %.2fpx"
                      % (confirmed_tag, err, len(self.pending), corr))
        else:
            self._log("ack=%s 确认 %d 条，预测与权威一致，无需可见校正"
                      % (confirmed_tag, confirmed))

        # 服务端仍在等缺口：立刻重发剩余未确认输入，加速收敛
        if self.server_gap and self.pending:
            self._retransmit()

    # ------------------------------------------------------------------
    def _request_full_state(self, reason):
        self.fsreq_count += 1
        self.last_fsreq = reason
        self.tr.send_fsreq(reason)
        self._log("请求完整状态(%s)：旧端无法安全增量，等待权威快照" % reason)

    # ---- 调试注入（游戏视图“交换一份迟到的旧包”） ----------------------
    def inject_ack(self, pkt):
        """把一条手工构造的旧确认直接注入处理流程（暂停调试时用）。"""
        self._log("注入迟到旧包: ack=%s" % pkt.get("ack"))
        self._handle_downstream(dict(pkt))

    def craft_late_legacy_ack(self):
        """用保留的最早一条确认构造一份旧格式（v1）迟到报文。"""
        if not self.rx_ack_pkts:
            return None
        old = self.rx_ack_pkts[0]
        return {"t": "ack", "ack": int(old["ack"]),
                "x": old["x"], "y": old["y"], "sstep": old["sstep"],
                "oslp": old.get("oslp", 0),
                "gap": old.get("gap", False),
                "drops": old.get("drops", []), "gaps": old.get("gaps", [])}

    # ------------------------------------------------------------------
    def link_degraded(self):
        return (time.monotonic() - self.last_ack_time) > self.LINK_TIMEOUT

    # ---- 检查点：保存协商后的能力与输入所有权 --------------------------
    def save_checkpoint(self):
        """序列化会话与预测状态。pending 里每条输入都带 owner 标记，
        恢复时据此分清“旧确认”（已在 ack_view 中，不再重放）与
        “当前仍需重放的方向键”（owner=local 且未被确认）。"""
        return {
            "fmt": 1,
            "version": self.version,
            "caps": sorted(self.caps),
            "seq_mod": self.seq_mod,
            "session": self.session,
            "negotiated": self._negotiated,
            "next_seq_full": self.next_seq_full,
            "tick_count": self.tick_count,
            "ack_prefix": self.ack_view.prefix,
            "ack_extras": sorted(self.ack_view.extras),
            "auth_step": self._auth_step,
            "state": dict(self.state),
            "authoritative": (dict(self.authoritative)
                              if self.authoritative else None),
            "pending": [
                {"seq_full": e["seq_full"], "dx": e["inp"]["dx"],
                 "dy": e["inp"]["dy"], "owner": e["owner"],
                 "sent": e["sent"], "retrans": e["retrans"]}
                for e in self.pending
            ],
            "server_gap": self.server_gap,
            "saved_at": time.time(),
        }

    @classmethod
    def from_checkpoint(cls, transport, data):
        """从检查点恢复：协商能力直接沿用，不再重新 hello。"""
        return cls(transport, resume=data)

    def _restore(self, data):
        self.version = int(data["version"])
        self.caps = set(data.get("caps", []))
        self.seq_mod = int(data["seq_mod"])
        self.session = data.get("session")
        self._negotiated = bool(data.get("negotiated", True))
        self.next_seq_full = int(data["next_seq_full"])
        self.tick_count = int(data.get("tick_count", 0))
        self.ack_view = migrate.AckView(int(data["ack_prefix"]),
                                        data.get("ack_extras", []))
        self._auth_step = int(data.get("auth_step", -1))
        self.state = dict(data["state"])
        self.authoritative = (dict(data["authoritative"])
                              if data.get("authoritative") else None)
        self.server_gap = bool(data.get("server_gap", False))

        skipped_old = 0
        for e in data.get("pending", []):
            if e.get("owner") != "local":
                continue                      # 不拥有，不重放
            if self.ack_view.contains(int(e["seq_full"])):
                skipped_old += 1              # 旧确认覆盖，无需重放
                continue
            self.pending.append({
                "seq_full": int(e["seq_full"]),
                "inp": {"dx": e["dx"], "dy": e["dy"]},
                "owner": "local",
                "sent": int(e.get("sent", 0)),
                "retrans": int(e.get("retrans", 0)),
            })
        self._log("从检查点恢复: v%d caps=%s 确认前缀=%d，"
                  "待重放方向键 %d 条，跳过旧确认 %d 条"
                  % (self.version, sorted(self.caps), self.ack_view.prefix,
                     len(self.pending), skipped_old))

    # ------------------------------------------------------------------
    def snapshot(self):
        """UI 每帧读取的只读视图。"""
        max_acked = self.ack_view.prefix
        if self.ack_view.extras:
            max_acked = max(max_acked, max(self.ack_view.extras))
        pending = []
        for e in self.pending:
            seq_full = e["seq_full"]
            pending.append({
                "seq": seq_full % self.seq_mod,
                "seq_full": seq_full,
                "ep": seq_full // self.seq_mod,
                "inp": dict(e["inp"]),
                "owner": e["owner"],
                "sent": e["sent"],
                "retrans": e["retrans"],
                "hole": seq_full < max_acked,
            })
        return {
            "state": dict(self.state),
            "pending": pending,
            "next_seq": self.next_seq_full,
            "tick": self.tick_count,
            "ack_seq": self.ack_view.prefix,
            "ack_extras": sorted(self.ack_view.extras),
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
            "version": self.version,
            "caps": sorted(self.caps),
            "seq_mod": self.seq_mod,
            "epoch": self.next_seq_full // self.seq_mod,
            "negotiated": self._negotiated,
            "fsreq_count": self.fsreq_count,
            "last_fsreq": self.last_fsreq,
        }


class UdpTransport(KernelTransport):
    """真实 UDP 传输：经 proxy 上行、从 proxy 收下行。"""

    def __init__(self, proxy_host="127.0.0.1", up_port=7051, down_port=7061):
        import socket as _socket
        from . import protocol as _proto
        self._proto = _proto
        proxy_host = proxy_host or _proto.LOOPBACK
        self.sock = _socket.socket(_socket.AF_INET, _socket.SOCK_DGRAM)
        self.sock.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEADDR, 1)
        self.sock.bind((proxy_host, down_port))
        self.sock.setblocking(False)
        self.proxy = (proxy_host, up_port)
        self._hello_seq = 0

    def send_hello(self, hello):
        self._hello_seq += 1
        pkt = self._proto.hello_packet(
            "ui-%d" % self._hello_seq, hello["vers"], hello["caps"],
            hello["seq_mod"])
        self.sock.sendto(self._proto.encode(pkt), self.proxy)

    def send_input(self, seq, dx, dy, retrans=False, ep=None):
        pkt = self._proto.input_packet(seq, dx, dy, retrans=retrans, ep=ep)
        self.sock.sendto(self._proto.encode(pkt), self.proxy)

    def send_fsreq(self, reason):
        pkt = self._proto.full_state_request_packet(reason)
        self.sock.sendto(self._proto.encode(pkt), self.proxy)

    def recv_ack(self):
        try:
            raw, _ = self.sock.recvfrom(65535)
        except BlockingIOError:
            return None
        try:
            pkt = self._proto.decode(raw)
        except Exception:
            return None
        return pkt if pkt.get("t") in ("ack", "welcome", "full") else None
