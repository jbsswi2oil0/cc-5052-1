"""进程间通信约定：UDP 端口与 JSON 报文编解码。

链路拓扑（全部本机回环，真实 UDP socket、真实多进程）：

  client UI  --UDP-->  proxy(代理)  --UDP-->  server(权威)
            <--UDP--             <--UDP--

proxy 可注入延迟/抖动/乱序/丢包；client 与 server 互不知道对方真实地址，
只和 proxy 对话，因此所有网络损伤都经过同一条可观察路径。

协议迁移（本次新增）：
  协议有两个版本，靠 hello/welcome 握手协商（见 migrate.py）：
    v1 旧端：确认字段是单个连续序号 ack（短序号，会回绕）；
    v2 新端：确认字段是带缺口的确认区间 ranges（完整序号空间）+ 显式纪元 ep。
  线上的输入/确认序号都是“短序号”（full % seq_mod），回绕后的歧义
  由 RFC1982 式串行比较（serial.py 风格的 resolve_short）消除；
  v2 报文额外携带显式纪元 ep，从根本上避免歧义。
"""

import json

CLIENT_PROXY_PORT = 7051     # client -> proxy（proxy 上行监听口）
PROXY_SERVER_PORT = 7052     # proxy -> server（server 监听口）
SERVER_PROXY_PORT = 7062     # server -> proxy（proxy 下行监听口）
PROXY_CLIENT_PORT = 7061     # proxy -> client（client 监听口）
PROXY_CTRL_PORT = 7070       # TCP 控制通道（UI 调节损伤参数）

LOOPBACK = "127.0.0.1"

# 报文方向标记，proxy 据此决定转发路径
DIR_UP = "u"     # client -> server
DIR_DOWN = "d"   # server -> client

# ---- 协议版本与能力 -------------------------------------------------
PROTO_LEGACY = 1           # 旧端：单个连续序号确认
PROTO_RANGES = 2           # 新端：带缺口的确认区间
SUPPORTED_VERSIONS = (PROTO_LEGACY, PROTO_RANGES)

CAP_RANGES = "ranges"      # 支持带缺口的确认区间
CAP_WRAP = "wrap"          # 支持显式纪元（跨序号回绕无歧义）
SUPPORTED_CAPS = (CAP_RANGES, CAP_WRAP)

DEFAULT_SEQ_MOD = 1 << 16  # 序号回绕模数（短序号位宽 16bit）
MIN_SEQ_MOD = 16           # 混版小样本用的小模数下限


def encode(packet):
    return json.dumps(packet, separators=(",", ":")).encode("utf-8")


def decode(raw):
    return json.loads(raw.decode("utf-8"))


# ---- 序号回绕（RFC1982 式串行比较） ----------------------------------
def resolve_short(short, ref_full, mod):
    """把线上的短序号映射到 ref_full 附近的完整序号。

    返回 (full, kind)，kind ∈ new / equal / old / ambiguous：
      new   在 ref 之后半个模数窗口内 —— 是更新的序号；
      equal 与 ref 相同 —— 重复；
      old   在 ref 之前半个模数窗口内 —— 上一轮迟到的重复；
      ambiguous 恰好落在半个模数边界上 —— 无法判定，调用方必须
                走“请求完整状态”流程（见 migrate.RESYNC_RULES）。
    """
    delta = (int(short) - (int(ref_full) % mod)) % mod
    if delta == 0:
        return ref_full, "equal"
    half = mod // 2
    if delta < half:
        return ref_full + delta, "new"
    if delta > half:
        return ref_full - (mod - delta), "old"
    return None, "ambiguous"


# ---- 报文构造 -------------------------------------------------------
def hello_packet(client_id, versions, caps, seq_mod):
    """客户端协商报文：声明自己支持的协议版本、能力与期望的序号模数。"""
    return {"t": "hello", "dir": DIR_UP, "cid": client_id,
            "vers": list(versions), "caps": list(caps), "seq_mod": seq_mod}


def welcome_packet(version, caps, seq_mod, session):
    """服务端协商应答：最终选定的版本/能力/序号模数与会话号。"""
    return {"t": "welcome", "dir": DIR_DOWN, "v": version,
            "caps": sorted(caps), "seq_mod": seq_mod, "session": session}


def input_packet(seq, dx, dy, retrans=False, ep=None):
    """客户端输入报文。同一 seq 重发内容完全一致，服务端按 seq 去重。

    retrans=True 表示这是重传副本：代理对其不再追加乱序延迟（快路径），
    基础延迟/抖动/丢包依旧生效，因此不会制造零延迟效果。
    ep 仅在 v2 会话中携带：显式纪元，跨回绕时服务端无需猜测。
    """
    pkt = {"t": "in", "seq": seq, "dx": dx, "dy": dy, "dir": DIR_UP,
           "rt": 1 if retrans else 0}
    if ep is not None:
        pkt["ep"] = ep
    return pkt


def ack_packet(ack_seq, state, gap, applied_count, drop_seen, gaps_open,
               ep=None, ranges=None):
    """服务端确认报文，携带完整权威状态而不只是一个序号。

    v1（旧端）：只有 ack 短序号（连续前缀）。
    v2（新端）：额外携带 ep（前缀纪元）与 ranges（带缺口的确认区间，
    完整序号空间，含前缀区间），旧字段 ack 仍然保留用于兼容显示。
    """
    pkt = {
        "t": "ack",
        "dir": DIR_DOWN,
        "ack": ack_seq,          # 已连续处理到的输入序号（短序号）
        "x": state["x"],
        "y": state["y"],
        "sstep": state["step"],
        "oslp": state.get("on_slope", 0),   # 权威快照的坡面接触标志
        "gap": gap,              # 服务端是否观察到序号缺口（等待/补偿中）
        "applied": applied_count,
        "drops": drop_seen,      # 服务端视角曾缺失、后来补到的序号
        "gaps": gaps_open,       # 观察到的缺口起点（含尚未补齐的）
    }
    if ep is not None:
        pkt["ep"] = ep
    if ranges is not None:
        pkt["ranges"] = [list(r) for r in ranges]
    return pkt


def full_state_request_packet(reason):
    """旧端请求完整状态（触发条件见 migrate.RESYNC_RULES）。"""
    return {"t": "fsreq", "dir": DIR_UP, "reason": reason}


def full_state_packet(state, ack_seq, ep, ranges, gap):
    """完整状态应答：权威快照 + 当前确认区间，供旧端无歧义地重建。"""
    return {"t": "full", "dir": DIR_DOWN,
            "x": state["x"], "y": state["y"], "sstep": state["step"],
            "oslp": state.get("on_slope", 0),
            "ack": ack_seq, "ep": ep,
            "ranges": [list(r) for r in ranges], "gap": gap}
