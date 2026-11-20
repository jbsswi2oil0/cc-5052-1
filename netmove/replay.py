"""会话录制/重演与混版回绕小样本（无 GUI、无 socket，纯逻辑）。

* record_session / replay_session / verify_recording：
  旧端对旧端（v1）会话的确定性录制与重演 —— 同样的输入序列、同样的
  丢包/乱序脚本，必须得到同样的确认流与终点状态，证明历史操作可重演。
* wraparound_sample：混版会话（新客户端 + 旧线路，小序号模数）跨回绕
  小样本 —— 上一轮的重复确认晚于新一轮的相同短序号抵达，内核必须凭
  RFC1982 窗口把它识别为重复，既不推进也不回滚确认点。

运行：python3 -m netmove.replay   （打印两个样本的摘要）
"""

import json

from . import sim, migrate
from .client_kernel import PredictionKernel, KernelTransport
from .server import SessionCore
from .protocol import (
    PROTO_LEGACY, PROTO_RANGES, DEFAULT_SEQ_MOD,
)


class QueueTransport(KernelTransport):
    """完全脚本化的内存传输。

    上行：输入立即交给 SessionCore；每条上行报文触发服务端产一条确认，
    进入 wire 队列。下行：只有被 release 到 ready 的报文内核才能泵到，
    测试因此可以精确控制投递顺序（包括“按住一条稍后再发”）。
    可选 loss_seqs（首发即丢）、reorder_window（窗口内反转）与
    hold_seqs（指定序号的上行报文被按住，直到 release_seq 放行，
    用于“中间一拍被网络延迟”的场景）。
    """

    def __init__(self, core, loss_seqs=(), reorder_window=0):
        self.core = core
        self.loss_seqs = set(loss_seqs)
        self.reorder_window = reorder_window
        self.hold_seqs = set()
        self.held_up = {}            # seq -> [被按住的上行报文]
        self.wire = []             # 已产出、尚未放行给内核的下行报文
        self.ready = []            # 已放行、等待内核泵取的下行报文
        self.rx_log = []           # 内核实际收到的确认（录制流）
        self.hellos = []
        self.fsreqs = []
        self.up_log = []           # 实际上送服务端的输入短序号
        self._in_flight = []

    # ---- 上行 ----
    def send_hello(self, hello):
        self.hellos.append(dict(hello))
        pkt = {"t": "hello", "vers": hello["vers"], "caps": hello["caps"],
               "seq_mod": hello["seq_mod"], "cid": "rec"}
        self.wire.extend(self.core.handle_packet_json(pkt, []))

    def send_fsreq(self, reason):
        self.fsreqs.append(reason)
        pkt = {"t": "fsreq", "reason": reason}
        self.wire.extend(self.core.handle_packet_json(pkt, []))

    def send_input(self, seq, dx, dy, retrans=False, ep=None):
        pkt = {"t": "in", "seq": seq, "dx": dx, "dy": dy,
               "rt": 1 if retrans else 0}
        if ep is not None:
            pkt["ep"] = ep
        if seq in self.hold_seqs:
            self.held_up.setdefault(seq, []).append(pkt)
            return
        if seq in self.loss_seqs and not retrans:
            self.loss_seqs.discard(seq)     # 首发丢失；重发副本放行
            return
        self.up_log.append(seq)
        if self.reorder_window:
            self._in_flight.append(pkt)
            if len(self._in_flight) >= self.reorder_window:
                for p in reversed(self._in_flight):
                    self._deliver(p)
                self._in_flight = []
        else:
            self._deliver(pkt)

    def release_seq(self, seq):
        """放行被按住的指定序号上行报文（按到达顺序补投）。"""
        for pkt in self.held_up.pop(seq, []):
            self.up_log.append(seq)
            self._deliver(pkt)

    def _deliver(self, pkt):
        self.core.handle_packet_json(pkt, [])
        self.wire.append(self.core.make_ack())

    def flush_in_flight(self):
        for p in self._in_flight:
            self._deliver(p)
        self._in_flight = []

    # ---- 下行 ----
    def release(self, n=1):
        for _ in range(n):
            if self.wire:
                self.ready.append(self.wire.pop(0))

    def release_all(self):
        self.release(len(self.wire))

    def recv_ack(self):
        if not self.ready:
            return None
        pkt = self.ready.pop(0)
        if pkt.get("t") == "ack":
            self.rx_log.append(pkt)
        return pkt


def _ack_fingerprint(pkt):
    """确认流比对用的稳定指纹（JSON 原生结构，落盘往返后仍可比）。"""
    return [pkt.get("t"), pkt.get("ack"), pkt.get("ep"),
            pkt.get("sstep"),
            [list(r) for r in pkt.get("ranges", ())],
            round(float(pkt.get("x", 0.0)), 6),
            round(float(pkt.get("y", 0.0)), 6)]


def record_session(moves, loss_seqs=(), reorder_window=0,
                   seq_mod=DEFAULT_SEQ_MOD,
                   offered_versions=(PROTO_LEGACY,),
                   server_versions=(PROTO_LEGACY,)):
    """录制一条会话：默认旧端对旧端（v1），可参数化。"""
    core = SessionCore(server_versions=server_versions, seq_mod=seq_mod)
    tr = QueueTransport(core, loss_seqs=loss_seqs,
                        reorder_window=reorder_window)
    kernel = PredictionKernel(tr, offered_versions=offered_versions,
                              seq_mod=seq_mod)
    tr.release_all()
    kernel.pump_acks()                      # 吸收 welcome

    for mv in moves:
        kernel.produce_input(*mv)
        tr.release_all()
        kernel.pump_acks()
    tr.flush_in_flight()
    tr.release_all()
    kernel.pump_acks()

    return {
        "fmt": 1,
        "seq_mod": seq_mod,
        "offered_versions": list(offered_versions),
        "server_versions": list(server_versions),
        "loss_seqs": sorted(loss_seqs),
        "reorder_window": reorder_window,
        "inputs": [{"dx": dx, "dy": dy} for (dx, dy) in moves],
        "acks": [_ack_fingerprint(p) for p in tr.rx_log],
        "final": dict(kernel.state),
        "ack_prefix": kernel.ack_view.prefix,
        "state_hash": sim.state_hash(kernel.state),
    }


def replay_session(recording):
    """按录制参数重放同一段历史操作，返回重演结果。"""
    moves = [(i["dx"], i["dy"]) for i in recording["inputs"]]
    core = SessionCore(server_versions=recording["server_versions"],
                       seq_mod=recording["seq_mod"])
    tr = QueueTransport(core, loss_seqs=recording["loss_seqs"],
                        reorder_window=recording["reorder_window"])
    kernel = PredictionKernel(
        tr, offered_versions=recording["offered_versions"],
        seq_mod=recording["seq_mod"])
    tr.release_all()
    kernel.pump_acks()
    for mv in moves:
        kernel.produce_input(*mv)
        tr.release_all()
        kernel.pump_acks()
    tr.flush_in_flight()
    tr.release_all()
    kernel.pump_acks()
    return {
        "acks": [_ack_fingerprint(p) for p in tr.rx_log],
        "final": dict(kernel.state),
        "ack_prefix": kernel.ack_view.prefix,
        "state_hash": sim.state_hash(kernel.state),
    }


def verify_recording(recording):
    """重演并与录制比对：确认流与终点状态必须完全一致。"""
    rep = replay_session(recording)
    return (rep["acks"] == recording["acks"]
            and rep["final"] == recording["final"]
            and rep["ack_prefix"] == recording["ack_prefix"]
            and rep["state_hash"] == recording["state_hash"])


def save_recording(recording, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(recording, f, ensure_ascii=False, indent=1)


def load_recording(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def wraparound_sample(seq_mod=16, total=40, dup_short=5):
    """混版会话跨序号回绕小样本。

    新客户端（offer v1+v2）对上只懂 v1 的旧服务端：线上只有短序号。
    第 dup_short 号确认（第 0 轮）被网络复制了一份；副本被按住，
    直到第 1 轮相同短序号的确认投递之后才姗姗来迟。内核必须凭
    RFC1982 窗口把它识别为“上一轮迟到的重复”，确认点不动、
    待重放队列不受影响。随后再注入一个落在回绕边界的伪造短序号，
    验证旧端按 R1 规则请求完整状态。
    """
    core = SessionCore(server_versions=(PROTO_LEGACY,), seq_mod=seq_mod)
    tr = QueueTransport(core)
    kernel = PredictionKernel(
        tr, offered_versions=(PROTO_LEGACY, PROTO_RANGES), seq_mod=seq_mod)
    # 本样本聚焦“确认路径”的回绕消歧。输入重传另行说明：v1 小模数下
    # 超过半窗的在途输入与下一轮短序号线上不可区分（4bit 序号的固有
    # 极限），这正是 v2 显式纪元要消除的缺陷，故样本关闭输入重传。
    kernel.RESEND_EVERY = 10 ** 9
    tr.release_all()
    kernel.pump_acks()                      # welcome -> v1 混版会话

    stashed = None                          # 被网络复制并按住的第 0 轮确认
    dup_round1_short = None                 # 第 1 轮相同短序号
    held = []                               # 第 1 轮之后按住的确认（制造在途）
    trace = []
    for i in range(total):
        kernel.produce_input(1, 0)
        # 每个 tick 放行一条确认（重传产生的额外副本视同网络重复，丢弃）
        tr.release_all()
        pkt = tr.ready.pop(0) if tr.ready else None
        tr.ready.clear()
        if pkt is None:
            continue
        if i == dup_short:
            stashed = dict(pkt)             # 网络复制：副本稍后投递
        if i == dup_short + seq_mod:
            dup_round1_short = int(pkt["ack"])
        if i <= dup_short + seq_mod:
            kernel.inject_ack(pkt)          # 正常投递到第 1 轮相同短序号
        else:
            held.append(pkt)                # 后续确认按住，pending 开始在途
        trace.append((i, int(pkt["ack"]), kernel.ack_view.prefix))

    assert stashed is not None and dup_round1_short == dup_short
    assert held, "样本需要第 1 轮之后的在途确认"

    prefix_before = kernel.ack_view.prefix
    pending_before = [e["seq_full"] for e in kernel.pending]
    corr_t_before = (kernel.last_correction or {}).get("t")

    # 上一轮的重复确认晚于新一轮相同短序号抵达
    kernel.inject_ack(stashed)
    prefix_after_dup = kernel.ack_view.prefix
    pending_after_dup = [e["seq_full"] for e in kernel.pending]
    dup_ignored = (prefix_after_dup == prefix_before
                   and pending_after_dup == pending_before
                   and (kernel.last_correction or {}).get("t") == corr_t_before)

    # 回绕边界上的伪造短序号 -> 必须触发 R1（请求完整状态）
    boundary_short = ((kernel.ack_view.prefix % seq_mod) + seq_mod // 2) \
        % seq_mod
    kernel.inject_ack({"t": "ack", "ack": boundary_short,
                       "x": 0, "y": 0, "sstep": 0, "gap": False,
                       "drops": [], "gaps": []})
    # 完整状态应答到达 -> 旧端无歧义恢复
    tr.release_all()
    kernel.pump_acks()
    recovered = kernel.ack_view.prefix == core.prefix

    # 被按住的确认此时已成旧包，逐条注入只会被识别为重复
    for pkt in held:
        kernel.inject_ack(pkt)

    return {
        "seq_mod": seq_mod,
        "total": total,
        "dup_short": dup_short,
        "stashed_wire": dict(stashed),
        "prefix_before_dup": prefix_before,
        "prefix_after_dup": prefix_after_dup,
        "dup_ignored": dup_ignored,
        "pending_before": list(pending_before),
        "pending_after_dup": list(pending_after_dup),
        "server_prefix": core.prefix,
        "server_applied": core.applied_count,
        "r1_fsreq": migrate.RESYNC_EPOCH_UNKNOWN in tr.fsreqs,
        "recovered_after_full": recovered,
        "converged": (not kernel.pending
                      and kernel.state == core.state),
        "kernel_version": kernel.version,
        "trace_tail": trace[-4:],
    }


def main():
    moves = ([(1, 0)] * 6 + [(0, 1)] * 4 + [(1, 1)] * 4
             + [(-1, 0)] * 3 + [(0, 0)] * 6)
    rec = record_session(moves, loss_seqs={3, 11}, reorder_window=3)
    print("旧端对旧端录制: %s acks=%d 重演一致=%s"
          % (rec["state_hash"], len(rec["acks"]), verify_recording(rec)))

    sample = wraparound_sample()
    print("混版回绕样本: mod=%d 第0轮重复确认 ack=%d 迟到后 前缀 %d->%d "
          "pending %d->%d R1请求完整状态=%s 收敛=%s"
          % (sample["seq_mod"], sample["stashed_wire"]["ack"],
             sample["prefix_before_dup"], sample["prefix_after_dup"],
             len(sample["pending_before"]), len(sample["pending_after_dup"]),
             sample["r1_fsreq"], sample["converged"]))


if __name__ == "__main__":
    main()
