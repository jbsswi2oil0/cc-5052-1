"""协议迁移层：版本协商、确认翻译与重同步规则。

本模块是纯函数集合，client_kernel 与 server 共用，不持有会话状态。

一、版本协商（negotiate）
    客户端 hello 声明支持的版本/能力/序号模数，服务端取双方交集：
    都支持 v2 则用带缺口的确认区间 + 显式纪元；否则回落 v1 旧行为
    （单个连续序号确认、缺口期间冻结推进）。序号模数取双方较小值，
    并向下钳到 2 的幂、不低于 MIN_SEQ_MOD。

二、确认翻译（AckView / normalize_ack / legacy_prefix）
    内核内部统一用 AckView（连续前缀 prefix + 前缀之上的散点确认
    extras）表示“服务端已结算哪些输入”：
      * 旧端报文（单个连续序号）翻译为 extras 为空的 AckView；
      * 新端报文（ranges 区间）翻译为 prefix + extras，缺口被保留；
      * 需要向旧端展示时（legacy_prefix）只取连续前缀 —— 绝不能把
        区间末端当作连续前缀，否则校正时会把缺口里尚未结算的移动
        从待重放队列中丢掉（斜坡中间一拍延迟场景即此 bug）。

三、旧端必须请求完整状态的情形（RESYNC_RULES）
    R1 epoch_unknown : 收到的短序号落在回绕窗口边界（ambiguous），
                       无法判定属于哪一轮；
    R2 gap_not_owned : 服务端报告缺口，但缺口序号不在本地待重放队列
                       中（不拥有该输入，无法重传补齐）；
    R3 reconnect     : 链路超时后恢复，会话纪元不再可信。
    命中任一规则时旧端必须发 fsreq，用完整状态重建，禁止增量猜测。
"""

from .protocol import (
    PROTO_LEGACY, PROTO_RANGES, CAP_RANGES, CAP_WRAP,
    DEFAULT_SEQ_MOD, MIN_SEQ_MOD, resolve_short,
)

# 旧端必须请求完整状态的三类情形（原因串会写进 fsreq 报文与日志）
RESYNC_EPOCH_UNKNOWN = "epoch_unknown"
RESYNC_GAP_NOT_OWNED = "gap_not_owned"
RESYNC_RECONNECT = "reconnect"
RESYNC_RULES = (RESYNC_EPOCH_UNKNOWN, RESYNC_GAP_NOT_OWNED,
                RESYNC_RECONNECT)


def _floor_pow2(n):
    p = 1
    while p * 2 <= n:
        p *= 2
    return p


def negotiate(client_hello, server_versions=(PROTO_LEGACY, PROTO_RANGES),
              server_caps=(CAP_RANGES, CAP_WRAP),
              server_seq_mod=DEFAULT_SEQ_MOD):
    """根据客户端 hello 与服务端能力，选定会话参数。

    返回 {"version", "caps", "seq_mod"}；version 为 None 表示没有共同
    版本，调用方应拒绝会话或按 v1 兜底。
    """
    common = [v for v in client_hello.get("vers", [])
              if v in server_versions]
    version = max(common) if common else None

    caps = set()
    if version == PROTO_RANGES:
        caps = set(client_hello.get("caps", [])) & set(server_caps)

    try:
        client_mod = int(client_hello.get("seq_mod", DEFAULT_SEQ_MOD))
    except (TypeError, ValueError):
        client_mod = DEFAULT_SEQ_MOD
    seq_mod = min(client_mod, server_seq_mod)
    seq_mod = max(MIN_SEQ_MOD, _floor_pow2(seq_mod))

    return {"version": version, "caps": caps, "seq_mod": seq_mod}


class AckView:
    """确认集合的统一视图：连续前缀 + 前缀之上的散点（缺口桥）。

    prefix  已连续确认到的完整序号（-1 表示尚无）；
    extras  大于 prefix、但已被确认的散点完整序号（区间缺口被保留）。
    """

    __slots__ = ("prefix", "extras")

    def __init__(self, prefix=-1, extras=()):
        self.prefix = int(prefix)
        self.extras = frozenset(int(s) for s in extras)

    def contains(self, seq_full):
        return seq_full <= self.prefix or seq_full in self.extras

    def merge(self, other):
        """两个视图取并集，并把能接上的散点卷入前缀（单调不减）。"""
        extras = set(self.extras) | set(other.extras)
        prefix = max(self.prefix, other.prefix)
        while prefix + 1 in extras:
            prefix += 1
            extras.discard(prefix)
        extras = {s for s in extras if s > prefix}
        return AckView(prefix, extras)

    def as_ranges(self, start=0):
        """导出为区间列表（完整序号空间），前缀区间在前。"""
        ranges = []
        if self.prefix >= start:
            ranges.append([start, self.prefix])
        for lo, hi in _compress(sorted(self.extras)):
            ranges.append([lo, hi])
        return ranges

    def __repr__(self):
        return "AckView(prefix=%d, extras=%s)" % (
            self.prefix, sorted(self.extras))


def _compress(sorted_seqs):
    """把有序序号列表压缩成 [lo, hi] 区间列表。"""
    ranges = []
    for s in sorted_seqs:
        if ranges and s == ranges[-1][1] + 1:
            ranges[-1][1] = s
        else:
            ranges.append([s, s])
    return ranges


def normalize_ack(pkt, ref_prefix, seq_mod):
    """把线上确认报文（v1 或 v2）翻译成 AckView。

    返回 (view, kind)：kind 为 stale 表示整条报文已过时（迟到/重复），
    为 ambiguous 表示短序号落在回绕边界（调用方应触发 R1 重同步）。
    v2 报文走显式纪元与完整序号区间，天然无歧义；
    v1 报文只有短序号，用 RFC1982 窗口对照 ref_prefix 消歧。
    """
    if "ranges" in pkt:  # v2：显式纪元 + 完整序号区间
        prefix = int(pkt.get("ep", 0)) * seq_mod + int(pkt["ack"])
        extras = set()
        for lo, hi in pkt["ranges"]:
            for s in range(int(lo), int(hi) + 1):
                if s > prefix:
                    extras.add(s)
        view = AckView(prefix, extras)
        # 前缀回退即整包过时；前缀相同但散点/权威步更新的情形
        # 由调用方按 sstep 进一步判定（视图合并本身单调无害）。
        kind = "stale" if view.prefix < ref_prefix else "new"
        return view, kind

    # v1：单个连续短序号
    full, kind = resolve_short(int(pkt["ack"]), ref_prefix, seq_mod)
    if kind == "ambiguous":
        return None, "ambiguous"
    if kind in ("equal", "old"):
        return AckView(full), "stale"
    return AckView(full), "new"


def legacy_prefix(view):
    """向旧端展示用的连续前缀：区间缺口之前的部分，绝不取区间末端。"""
    return view.prefix


def gap_not_owned(view, gap_starts, owned_seqs, seq_mod):
    """R2 判定：服务端报告的缺口起点里，是否有本地不拥有的序号。

    gap_starts 是线上短序号列表，先按 ref=view.prefix 消歧；
    落在 prefix 之前（含）的缺口已被确认覆盖，无需处理。
    """
    for short in gap_starts:
        full, kind = resolve_short(int(short), view.prefix, seq_mod)
        if kind == "ambiguous":
            return True     # 连缺口属于哪一轮都说不清，必须重同步
        if kind == "new" and full not in owned_seqs:
            return True
    return False
