"""逻辑层自测（无 GUI、无 socket）：证明预测/校正内核与协议迁移的不变量。

运行：python3 -m netmove.selftest
"""

import math
import os
import tempfile
import time

from . import sim, migrate, replay
from .client_kernel import PredictionKernel
from .server import SessionCore
from .protocol import (
    PROTO_LEGACY, PROTO_RANGES, CAP_RANGES, CAP_WRAP,
    DEFAULT_SEQ_MOD, resolve_short,
)


def check(name, cond, detail=""):
    tag = "PASS" if cond else "FAIL"
    print("[%s] %s %s" % (tag, name, detail))
    if not cond:
        raise AssertionError(name)


# ----------------------------------------------------------------------
# 既有内核行为（v1 旧端语义保持不变）
# ----------------------------------------------------------------------
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


def test_idle_input_identity():
    st = sim.initial_state()
    st2 = sim.step(st, {"dx": 0, "dy": 0})
    st3 = sim.step(st, None)
    check("零输入/空转只增 step 不改坐标",
          st2["x"] == st["x"] and st2["y"] == st["y"]
          and st3["x"] == st["x"] and st3["y"] == st["y"])


# 冲坡路径：从出生点右行到开阔车道，下到底，左回到坡脚前，再右行冲坡。
# （x=60 正下方有左竖墙 (60,280,26,240)，直线走会被挡住。）
SLOPE_PATH = ([(1, 0)] * 82 + [(0, 1)] * 97 + [(-1, 0)] * 75
              + [(1, 0)] * 50)
SLOPE_PHASE_D_START = 82 + 97 + 75      # 开始右行冲坡的拍号


def test_slope_sim():
    """斜坡本身：贴地右行会爬上坡面，离坡后落点高于地面。"""
    st = sim.initial_state()
    for mv in SLOPE_PATH[:82 + 97]:
        st = sim.step(st, {"dx": mv[0], "dy": mv[1]})
    check("坡前落地： y 到地面", abs(st["y"] - 606.0) < 1e-9,
          "y=%.2f" % st["y"])
    saw_contact = False
    for mv in SLOPE_PATH[82 + 97:]:
        st = sim.step(st, {"dx": mv[0], "dy": mv[1]})
        saw_contact = saw_contact or bool(st["on_slope"])
    check("坡面接触： 跨越过程中 on_slope 置位", saw_contact)
    check("落点： 离坡后停在坡顶高度而非地面",
          st["y"] < 550.0 and st["x"] > 330.0,
          "落点=(%.2f,%.2f)" % (st["x"], st["y"]))


def test_dedup_and_reorder():
    srv = SessionCore(server_versions=(PROTO_LEGACY,))
    # 乱序：1 先到，0 后到；再把 0 重发两次
    srv.handle_packet_json({"t": "in", "seq": 1, "dx": 1, "dy": 0}, [])
    check("乱序：缺口时服务端 gap=True 且不推进",
          srv.gap and srv.state == sim.initial_state())
    srv.handle_packet_json({"t": "in", "seq": 0, "dx": 1, "dy": 0}, [])
    steps_after_fill = srv.state["step"]
    srv.handle_packet_json({"t": "in", "seq": 0, "dx": 1, "dy": 0}, [])
    srv.handle_packet_json({"t": "in", "seq": 1, "dx": 1, "dy": 0}, [])
    check("去重：重放 seq 0/1 不增加模拟步",
          srv.state["step"] == steps_after_fill == 2,
          "step=%d" % srv.state["step"])
    check("补偿可观察：缺口起点 0 曾被通告、补齐后入 drops",
          srv.gaps_open == [0] and srv.drops_seen == [0],
          "gaps=%s drops=%s" % (srv.gaps_open, srv.drops_seen))


def _make_session(offered=(PROTO_LEGACY,), server_versions=(PROTO_LEGACY,),
                  seq_mod=DEFAULT_SEQ_MOD, loss_seqs=(), reorder_window=0):
    core = SessionCore(server_versions=server_versions, seq_mod=seq_mod)
    tr = replay.QueueTransport(core, loss_seqs=loss_seqs,
                               reorder_window=reorder_window)
    k = PredictionKernel(tr, offered_versions=offered, seq_mod=seq_mod)
    tr.release_all()
    k.pump_acks()                       # 吸收 welcome
    return core, tr, k


def test_kernel_convergence_with_loss():
    core, tr, k = _make_session(loss_seqs={2, 7})
    moves = [
        (1, 0), (1, 0), (1, 0), (0, 1), (0, 1),
        (-1, 0), (-1, 0), (1, 1), (1, 1), (0, -1),
        (0, 0), (0, 0), (1, 0), (1, 0), (0, 1),
    ]
    for mv in moves:
        k.produce_input(*mv)
        tr.release_all()
    for _ in range(12):
        k.produce_input(0, 0)
        tr.release_all()
    tr.flush_in_flight()
    tr.release_all()
    k.pump_acks()

    snap = k.snapshot()
    check("丢包补偿：所有输入最终被服务端确认",
          snap["ack_seq"] == len(moves) + 12 - 1,
          "ack=%d" % snap["ack_seq"])
    check("收敛：pending 清空", len(snap["pending"]) == 0)
    dist = math.hypot(snap["state"]["x"] - core.state["x"],
                      snap["state"]["y"] - core.state["y"])
    check("收敛：客户端与权威位置一致", dist < 1e-9,
          "偏差=%.2e %s" % (dist, sim.state_hash(snap["state"])))
    retrans_events = [e for e in snap["events"] if "重发" in e]
    check("内核日志可见重发动作", len(retrans_events) > 0,
          retrans_events[-1] if retrans_events else "")


def test_kernel_reconciliation_matches_replay():
    core, tr, k = _make_session(loss_seqs={3, 4}, reorder_window=3)
    moves = [(1 if i % 2 else 0, 1 if i % 3 == 0 else 0) for i in range(20)]
    for mv in moves:
        k.produce_input(*mv)
        tr.release_all()
    for _ in range(10):
        k.produce_input(0, 0)
        tr.release_all()
    tr.flush_in_flight()
    tr.release_all()
    k.pump_acks()
    snap = k.snapshot()
    # 用共享内核从初始态独立重放全部输入，应与校正后的客户端状态逐位相同
    ref = sim.initial_state()
    ref = sim.replay(ref, [{"dx": dx, "dy": dy} for (dx, dy) in moves]
                     + [{"dx": 0, "dy": 0}] * 10)
    check("校正=权威快照+未确认重放：与独立参考轨迹逐位一致",
          snap["state"] == ref, sim.state_hash(snap["state"]))
    check("最终与服务端一致",
          math.hypot(snap["state"]["x"] - core.state["x"],
                     snap["state"]["y"] - core.state["y"]) < 1e-9)


# ----------------------------------------------------------------------
# 协议迁移：协商 / 翻译 / 保留缺口的重放
# ----------------------------------------------------------------------
def test_version_negotiation():
    both = migrate.negotiate(
        {"vers": [1, 2], "caps": [CAP_RANGES, CAP_WRAP], "seq_mod": 65536})
    check("协商：双端都支持 v2 -> ranges+wrap",
          both["version"] == PROTO_RANGES
          and both["caps"] == {CAP_RANGES, CAP_WRAP}
          and both["seq_mod"] == 65536)
    legacy_only = migrate.negotiate({"vers": [1], "caps": []})
    check("协商：旧端只会 v1 -> 回落 v1 且无区间能力",
          legacy_only["version"] == PROTO_LEGACY
          and legacy_only["caps"] == set())
    small_mod = migrate.negotiate(
        {"vers": [1, 2], "caps": [CAP_RANGES, CAP_WRAP], "seq_mod": 16})
    check("协商：序号模数取双方较小值（混版小样本用 16）",
          small_mod["seq_mod"] == 16)
    no_common = migrate.negotiate({"vers": [9], "caps": []})
    check("协商：无共同版本 -> version=None（调用方兜底）",
          no_common["version"] is None)


def test_ack_translation():
    pkt_v2 = {"t": "ack", "ack": 5, "ep": 0, "sstep": 10,
              "ranges": [[0, 5], [7, 9]]}
    view, kind = migrate.normalize_ack(pkt_v2, -1, DEFAULT_SEQ_MOD)
    check("翻译：v2 区间 -> 前缀 5 + 散点 {7,8,9}",
          view.prefix == 5 and view.extras == {7, 8, 9} and kind == "new")
    check("翻译：给旧端看的连续前缀绝不取区间末端",
          migrate.legacy_prefix(view) == 5)
    pkt_v1 = {"t": "ack", "ack": 9}
    view1, kind1 = migrate.normalize_ack(pkt_v1, -1, DEFAULT_SEQ_MOD)
    check("翻译：v1 单个连续序号 -> 无前缀散点",
          view1.prefix == 9 and not view1.extras and kind1 == "new")
    merged = view1.merge(view)
    check("翻译：合并单调且前缀不回退",
          merged.prefix == 9 and merged.extras == set())
    # 回绕：ref=21(mod16 短序号5)，短序号 5 是重复、6 是新的、13 越界
    check("回绕：上一轮同短序号=重复", resolve_short(5, 21, 16)[1] == "equal")
    check("回绕：下一短序号=new", resolve_short(6, 21, 16) == (22, "new"))
    check("回绕：半窗边界=ambiguous", resolve_short(13, 21, 16)[1] == "ambiguous")


def test_slope_gap_preserving_replay():
    """核心场景：角色冲过斜坡，中间一拍（seq=K）被网络延迟，
    v2 服务端先确认后续输入（带缺口的确认区间）。内核必须：
    保留缺口里的输入继续重放（落点正确），而不是把区间末端
    误当连续前缀丢掉那一拍。"""
    core, tr, k = _make_session(offered=(PROTO_RANGES,),
                                server_versions=(PROTO_LEGACY, PROTO_RANGES))
    check("前置：协商到 v2", k.version == PROTO_RANGES)

    K = SLOPE_PHASE_D_START + 19        # 中间一拍：正在坡面上 (x≈213)
    tr.hold_seqs = {K}                  # 网络按住这一拍（首发与重传都延迟）
    saw_slope = False
    gap_checks = []

    for i, mv in enumerate(SLOPE_PATH):
        k.produce_input(*mv)
        tr.release_all()
        k.pump_acks()
        saw_slope = saw_slope or bool(k.state["on_slope"])
        if i in (K + 1, K + 2):
            gap_checks.append(dict(
                pending=[e["seq_full"] for e in k.pending],
                inps=[(e["seq_full"], dict(e["inp"])) for e in k.pending],
                prefix=k.ack_view.prefix,
                extras=set(k.ack_view.extras),
                holes=list((k.last_correction or {}).get("holes", [])),
                auth=dict(k.authoritative),
                state=dict(k.state),
            ))

    g = gap_checks[0]
    check("缺口期间：服务端先确认后续输入（前缀停在 K-1，散点在 K 上方）",
          g["prefix"] == K - 1 and K + 1 in g["extras"],
          "prefix=%d extras=%s" % (g["prefix"], sorted(g["extras"])))
    check("保留缺口：被延迟的一拍仍在待重放队列",
          K in g["pending"], "pending 含 seq=%d" % K)
    check("保留缺口：校正记录标出缺口输入",
          K in g["holes"], "holes=%s" % g["holes"])
    check("翻译不越界：连续前缀不是区间末端",
          migrate.legacy_prefix(k.ack_view) == K - 1)

    # 反事实：若把区间末端误当连续前缀，K 会被丢掉，落点立即不同
    correct = sim.replay(dict(g["auth"]), [inp for (_, inp) in g["inps"]])
    buggy = sim.replay(dict(g["auth"]),
                       [inp for (s, inp) in g["inps"] if s != K])
    check("落点：保留缺口重放的位置 == 内核当前模拟位置",
          g["state"] == correct, sim.state_hash(g["state"]))
    check("落点：丢掉缺口一拍会得到不同的（错误）落点",
          correct != buggy,
          "差 %.2fpx" % math.hypot(correct["x"] - buggy["x"],
                                   correct["y"] - buggy["y"]))

    check("坡面接触：跨越过程中内核状态出现 on_slope", saw_slope)

    # 网络放行被按住的那一拍：服务端补偿结算，两端收敛
    tr.release_seq(K)
    for _ in range(6):
        k.idle_tick()
        tr.release_all()
        k.pump_acks()
    check("补偿可观察：服务端记录迟到补齐的 seq",
          K in core.drops_seen, "drops=%s" % core.drops_seen)
    check("收敛：pending 清空且两端状态一致",
          not k.pending and k.state == core.state,
          "%s vs %s" % (sim.state_hash(k.state), sim.state_hash(core.state)))
    check("落点：最终停在坡顶高度（爬过了斜坡）",
          k.state["y"] < 550.0 and k.state["x"] > 330.0,
          "落点=(%.2f,%.2f)" % (k.state["x"], k.state["y"]))


# ----------------------------------------------------------------------
# 混版会话：跨序号回绕小样本
# ----------------------------------------------------------------------
def test_wraparound_mixed_session():
    s = replay.wraparound_sample(seq_mod=16, total=40, dup_short=5)
    check("混版：新客户端与旧线路协商到 v1", s["kernel_version"] == PROTO_LEGACY)
    check("回绕：服务端 40 条输入全部按序结算（短序号回绕无歧义）",
          s["server_applied"] == 40 and s["server_prefix"] == 39)
    check("回绕：第 0 轮重复确认晚到，确认点不动",
          s["prefix_before_dup"] == 21 and s["prefix_after_dup"] == 21)
    check("回绕：待重放队列不受迟到重复影响",
          s["dup_ignored"] and s["pending_before"] == s["pending_after_dup"]
          and s["pending_before"] == list(range(22, 40)))
    check("回绕：边界短序号触发 R1 请求完整状态", s["r1_fsreq"])
    check("回绕：完整状态应答后无歧义恢复", s["recovered_after_full"])
    check("回绕：最终收敛（pending 清空，两端一致）", s["converged"])


# ----------------------------------------------------------------------
# 旧端对旧端：录制可重演
# ----------------------------------------------------------------------
def test_legacy_recording_replay():
    moves = ([(1, 0)] * 6 + [(0, 1)] * 4 + [(1, 1)] * 4
             + [(-1, 0)] * 3 + [(0, 0)] * 6)
    rec = replay.record_session(moves, loss_seqs={3, 11}, reorder_window=3)
    check("录制：旧端对旧端（两端都是 v1）",
          rec["offered_versions"] == [PROTO_LEGACY]
          and rec["server_versions"] == [PROTO_LEGACY])
    check("录制：确认流非空且最终全部确认",
          len(rec["acks"]) > 0 and rec["ack_prefix"] == len(moves) - 1,
          "acks=%d prefix=%d" % (len(rec["acks"]), rec["ack_prefix"]))
    check("重演：同一录制的历史操作逐位重演",
          replay.verify_recording(rec), rec["state_hash"])
    with tempfile.TemporaryDirectory() as td:
        path = os.path.join(td, "legacy_session.json")
        replay.save_recording(rec, path)
        loaded = replay.load_recording(path)
        check("重演：录制落盘后再加载仍可重演",
              replay.verify_recording(loaded))


# ----------------------------------------------------------------------
# 检查点：协商能力与输入所有权的保存/恢复
# ----------------------------------------------------------------------
def test_checkpoint_restore():
    core, tr, k = _make_session(offered=(PROTO_LEGACY, PROTO_RANGES),
                                server_versions=(PROTO_LEGACY, PROTO_RANGES))
    check("前置：协商到 v2", k.version == PROTO_RANGES)
    k.RESEND_EVERY = 10000                # 每条输入恰好一条确认，放行计数精确
    moves = [(1 if i % 2 else 0, 1 if i % 3 == 0 else 0) for i in range(10)]
    for mv in moves:
        k.produce_input(*mv)
    tr.release(7)                         # 只放行前 7 条确认（0..6）
    k.pump_acks()
    check("前置：确认前缀=6，待重放 7..9",
          k.ack_view.prefix == 6
          and [e["seq_full"] for e in k.pending] == [7, 8, 9])

    cp = k.save_checkpoint()
    check("检查点：保存协商后的能力",
          cp["version"] == PROTO_RANGES and cp["negotiated"]
          and CAP_RANGES in cp["caps"] and cp["seq_mod"] == DEFAULT_SEQ_MOD)
    check("检查点：待重放输入都带所有权标记",
          all(e["owner"] == "local" for e in cp["pending"])
          and [e["seq_full"] for e in cp["pending"]] == [7, 8, 9])

    tr.release_all()                      # 原会话继续走到全部确认
    k.pump_acks()
    assert not k.pending and k.ack_view.prefix == 9

    tr2 = replay.QueueTransport(core)     # 恢复用全新传输（如换端重启）
    k2 = PredictionKernel.from_checkpoint(tr2, cp)
    check("恢复：协商能力沿用，不再重新 hello",
          tr2.hellos == [] and k2.version == PROTO_RANGES
          and k2._negotiated)
    check("恢复：当前仍需重放的方向键回到队列",
          [e["seq_full"] for e in k2.pending] == [7, 8, 9]
          and all(e["owner"] == "local" for e in k2.pending))

    # 旧确认（检查点之前已确认的序号）到达 -> 识别为重复，不触发校正
    k2.inject_ack({"t": "ack", "ack": 5, "ep": 0, "ranges": [[0, 5]],
                   "x": 0, "y": 0, "sstep": 6, "gap": False,
                   "drops": [], "gaps": []})
    check("恢复：旧确认被识别为重复（分清旧确认）",
          k2.ack_view.prefix == 6 and k2.last_correction is None)

    # 恢复端重传仍待确认的方向键，服务端去重，最新确认到达后收敛
    k2._retransmit()
    tr2.release_all()
    k2.pump_acks()
    check("恢复：重放方向键后收敛到权威状态",
          not k2.pending and k2.ack_view.prefix == 9
          and k2.state == core.state,
          sim.state_hash(k2.state))
    check("恢复：服务端没有重复结算（重传被去重）",
          core.applied_count == 10, "applied=%d" % core.applied_count)


# ----------------------------------------------------------------------
# 旧端必须请求完整状态的情形（migrate.RESYNC_RULES）
# ----------------------------------------------------------------------
def test_full_state_request_rules():
    core, tr, k = _make_session()         # 旧端对旧端
    for _ in range(5):
        k.produce_input(1, 0)
    tr.release_all()
    k.pump_acks()
    assert k.ack_view.prefix == 4 and not k.pending

    # R2：服务端报告缺口 seq=5，但本地 pending 已没有该序号（不拥有）
    k.inject_ack({"t": "ack", "ack": 4, "x": 0, "y": 0, "sstep": 5,
                  "gap": True, "gaps": [5], "drops": []})
    check("R2：缺口序号不在本地 -> 必须请求完整状态",
          migrate.RESYNC_GAP_NOT_OWNED in tr.fsreqs)
    tr.release_all()
    k.pump_acks()
    check("R2：完整状态应答后恢复（前缀与权威一致）",
          k.ack_view.prefix == core.prefix)

    # R3：链路超时后恢复的第一条确认
    k.last_ack_time = time.monotonic() - 2.0
    k.produce_input(1, 0)                 # seq=5 发出，服务端应用
    tr.release_all()
    k.pump_acks()                         # 收到恢复后的第一条确认
    check("R3：超时恢复后必须请求完整状态",
          migrate.RESYNC_RECONNECT in tr.fsreqs)
    tr.release_all()
    k.pump_acks()

    # 正常路径不触发：缺口序号就在本地 pending 时只靠重传
    core2, tr2, k2 = _make_session(loss_seqs={2})
    for _ in range(5):
        k2.produce_input(1, 0)
        tr2.release_all()
        k2.pump_acks()
    for _ in range(6):
        k2.idle_tick()
        tr2.release_all()
        k2.pump_acks()
    check("正常路径：缺口序号本地拥有 -> 重传补齐，不请求完整状态",
          tr2.fsreqs == [] and not k2.pending
          and k2.ack_view.prefix == 4)


def main():
    test_determinism()
    test_collision()
    test_idle_input_identity()
    test_slope_sim()
    test_dedup_and_reorder()
    test_kernel_convergence_with_loss()
    test_kernel_reconciliation_matches_replay()
    test_version_negotiation()
    test_ack_translation()
    test_slope_gap_preserving_replay()
    test_wraparound_mixed_session()
    test_legacy_recording_replay()
    test_checkpoint_restore()
    test_full_state_request_rules()
    print("\n全部自测通过。")


if __name__ == "__main__":
    main()
