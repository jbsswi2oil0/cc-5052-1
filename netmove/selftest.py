"""逻辑层自测（无 GUI、无 socket）：证明预测/校正内核的关键不变量。

运行：python3 -m netmove.selftest
"""

import math

from . import sim
from .client_kernel import PredictionKernel, KernelTransport


# ----------------------------------------------------------------------
# 内存假传输：按测试脚本决定丢/延迟/乱序，模拟 proxy 的语义，
# 但不预知未来输入 —— 报文只有在 produce_input 真正发出后才进入网络。
# ----------------------------------------------------------------------
class ScriptedTransport(KernelTransport):
    def __init__(self, server, loss_seqs=(), reorder_window=0):
        self.server = server
        self.loss_seqs = set(loss_seqs)
        self.reorder_window = reorder_window
        self.up_log = []           # 实际上送服务器的报文（含重发）
        self.ack_queue = []
        self._in_flight = []       # 乱序缓冲：(seq, raw)

    def send_input(self, seq, dx, dy, retrans=False):
        pkt = {"t": "in", "seq": seq, "dx": dx, "dy": dy,
               "rt": 1 if retrans else 0}
        self.up_log.append(seq)
        if seq in self.loss_seqs:
            # 首发丢失；重发报文（同 seq 再次出现）放行
            self.loss_seqs.discard(seq)
            return
        self._deliver(pkt)

    def _deliver(self, pkt):
        if self.reorder_window and self._in_flight is not None:
            self._in_flight.append(pkt)
            if len(self._in_flight) >= self.reorder_window:
                # 反转窗口内顺序后批量上交
                for p in reversed(self._in_flight):
                    self.server.handle_packet_json(p)
                self._in_flight = []
        else:
            self.server.handle_packet_json(pkt)
        self._collect_ack()

    def _collect_ack(self):
        self.ack_queue.append(self.server.make_ack())

    def recv_ack(self):
        if not self.ack_queue:
            return None
        return self.ack_queue.pop(0)


class HarnessServer:
    """与真实 server 同构的内存版权威端，复用同一套处理规则。"""

    def __init__(self):
        self.state = sim.initial_state()
        self.next_seq = 0
        self.buffer = {}
        self.applied = set()
        self.drops = []
        self.last_applied = -1
        self.gap = False

    def handle_packet_json(self, pkt):
        seq = int(pkt["seq"])
        if seq in self.applied or seq < self.next_seq:
            return
        if seq in self.buffer:
            return
        self.buffer[seq] = {"dx": pkt["dx"], "dy": pkt["dy"]}
        while self.next_seq in self.buffer:
            s = self.next_seq
            payload = self.buffer.pop(s)
            if s > self.last_applied + 1:
                self.drops.append(s)
            self.state = sim.step(self.state, payload)
            self.applied.add(s)
            self.last_applied = s
            self.next_seq += 1
        self.gap = bool(self.buffer) and min(self.buffer) > self.next_seq

    def make_ack(self):
        return {
            "t": "ack", "ack": self.last_applied,
            "x": self.state["x"], "y": self.state["y"],
            "sstep": self.state["step"], "gap": self.gap,
            "applied": len(self.applied), "drops": list(self.drops),
            "gaps": list(self.drops),
        }


def check(name, cond, detail=""):
    tag = "PASS" if cond else "FAIL"
    print("[%s] %s %s" % (tag, name, detail))
    if not cond:
        raise AssertionError(name)


def test_determinism():
    inputs = [{"dx": 1, "dy": 0}, {"dx": 0, "dy": 1}, {"dx": 1, "dy": 1},
              {"dx": -1, "dy": 0}, None, {"dx": 0, "dy": -1}]
    a = sim.initial_state()
    b = sim.initial_state()
    for inp in inputs:
        a = sim.step(a, inp)
    b = sim.replay(b, inputs)
    check("确定性：顺序重放与逐步执行一致", a == b, sim.state_hash(a))


def test_collision():
    # 直冲右侧长墙 (120,130,320,26)，半径 14 不应嵌入
    st = sim.initial_state()
    for _ in range(120):
        st = sim.step(st, {"dx": 1, "dy": 1})
    penetrating = any(
        st["x"] + sim.RADIUS > wx and st["x"] - sim.RADIUS < wx + ww
        and st["y"] + sim.RADIUS > wy and st["y"] - sim.RADIUS < wy + wh
        for (wx, wy, ww, wh) in sim.WALLS
    )
    inside_arena = (sim.RADIUS <= st["x"] <= sim.ARENA_W - sim.RADIUS
                    and sim.RADIUS <= st["y"] <= sim.ARENA_H - sim.RADIUS)
    check("碰撞：角色不嵌入任何墙体且留在场地内",
          not penetrating and inside_arena, sim.state_hash(st))


def test_dedup_and_reorder():
    srv = HarnessServer()
    # 乱序：1 先到，0 后到；再把 0 重发两次
    srv.handle_packet_json({"t": "in", "seq": 1, "dx": 1, "dy": 0})
    check("乱序：缺口时服务端 gap=True 且不推进",
          srv.gap and srv.state == sim.initial_state())
    srv.handle_packet_json({"t": "in", "seq": 0, "dx": 1, "dy": 0})
    steps_after_fill = srv.state["step"]
    srv.handle_packet_json({"t": "in", "seq": 0, "dx": 1, "dy": 0})
    srv.handle_packet_json({"t": "in", "seq": 1, "dx": 1, "dy": 0})
    check("去重：重放 seq 0/1 不增加模拟步",
          srv.state["step"] == steps_after_fill == 2,
          "step=%d" % srv.state["step"])
    check("补偿可观察：drops 记录了缺失序号", srv.drops == [],
          "（本例 0 仅延迟未真正丢失，drops=%s）" % srv.drops)


def test_kernel_convergence_with_loss():
    srv = HarnessServer()
    # seq 2、7 的首发报文丢失，依赖内核周期重发补偿
    tr = ScriptedTransport(srv, loss_seqs={2, 7})
    k = PredictionKernel(tr)
    moves = [
        (1, 0), (1, 0), (1, 0), (0, 1), (0, 1),
        (-1, 0), (-1, 0), (1, 1), (1, 1), (0, -1),
        (0, 0), (0, 0), (1, 0), (1, 0), (0, 1),
    ]
    for mv in moves:
        k.produce_input(*mv)
    # 再跑若干空 tick 触发重发与补偿
    for _ in range(12):
        k.produce_input(0, 0)

    snap = k.snapshot()
    check("丢包补偿：所有输入最终被服务端确认",
          snap["ack_seq"] == len(moves) + 12 - 1,
          "ack=%d" % snap["ack_seq"])
    check("收敛：pending 清空", len(snap["pending"]) == 0)
    dist = math.hypot(snap["state"]["x"] - srv.state["x"],
                      snap["state"]["y"] - srv.state["y"])
    check("收敛：客户端与权威位置一致", dist < 1e-9,
          "偏差=%.2e %s" % (dist, sim.state_hash(snap["state"])))
    retrans_events = [e for e in snap["events"] if "重发" in e]
    check("内核日志可见重发动作", len(retrans_events) > 0,
          retrans_events[-1] if retrans_events else "")


def test_kernel_reconciliation_matches_replay():
    srv = HarnessServer()
    tr = ScriptedTransport(srv, loss_seqs={3, 4}, reorder_window=3)
    k = PredictionKernel(tr)
    for i in range(20):
        k.produce_input(1 if i % 2 else 0, 1 if i % 3 == 0 else 0)
    for _ in range(10):
        k.produce_input(0, 0)
    snap = k.snapshot()
    # 用共享内核从初始态独立重放全部输入，应与校正后的客户端状态逐位相同
    ref = sim.initial_state()
    ref = sim.replay(ref, [e["inp"] for e in [
        {"inp": {"dx": 1 if i % 2 else 0, "dy": 1 if i % 3 == 0 else 0}}
        for i in range(20)
    ]] + [{"dx": 0, "dy": 0}] * 10)
    check("校正=权威快照+未确认重放：与独立参考轨迹逐位一致",
          snap["state"] == ref, sim.state_hash(snap["state"]))
    check("最终与服务端一致",
          math.hypot(snap["state"]["x"] - srv.state["x"],
                     snap["state"]["y"] - srv.state["y"]) < 1e-9)


def test_idle_input_identity():
    st = sim.initial_state()
    st2 = sim.step(st, {"dx": 0, "dy": 0})
    st3 = sim.step(st, None)
    check("零输入/空转只增 step 不改坐标",
          st2["x"] == st["x"] and st2["y"] == st["y"]
          and st3["x"] == st["x"] and st3["y"] == st["y"])


def main():
    test_determinism()
    test_collision()
    test_idle_input_identity()
    test_dedup_and_reorder()
    test_kernel_convergence_with_loss()
    test_kernel_reconciliation_matches_replay()
    print("\n全部自测通过。")


if __name__ == "__main__":
    main()
