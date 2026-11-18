"""进程间通信约定：UDP 端口与 JSON 报文编解码。

链路拓扑（全部本机回环，真实 UDP socket、真实多进程）：

  client UI  --UDP-->  proxy(代理)  --UDP-->  server(权威)
            <--UDP--             <--UDP--

proxy 可注入延迟/抖动/乱序/丢包；client 与 server 互不知道对方真实地址，
只和 proxy 对话，因此所有网络损伤都经过同一条可观察路径。
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


def encode(packet):
    return json.dumps(packet, separators=(",", ":")).encode("utf-8")


def decode(raw):
    return json.loads(raw.decode("utf-8"))


def input_packet(seq, dx, dy, retrans=False):
    """客户端输入报文。同一 seq 重发内容完全一致，服务端按 seq 去重。

    retrans=True 表示这是重传副本：代理对其不再追加乱序延迟（快路径），
    基础延迟/抖动/丢包依旧生效，因此不会制造零延迟效果。
    """
    return {"t": "in", "seq": seq, "dx": dx, "dy": dy, "dir": DIR_UP,
            "rt": 1 if retrans else 0}


def ack_packet(ack_seq, state, gap, applied_count, drop_seen, gaps_open):
    """服务端确认报文，携带完整权威状态而不只是一个序号。"""
    return {
        "t": "ack",
        "dir": DIR_DOWN,
        "ack": ack_seq,          # 服务端已连续处理到的输入序号
        "x": state["x"],
        "y": state["y"],
        "sstep": state["step"],
        "gap": gap,              # 服务端是否观察到序号缺口（等待/补偿中）
        "applied": applied_count,
        "drops": drop_seen,      # 服务端视角曾缺失、后来补到的序号
        "gaps": gaps_open,       # 观察到的缺口起点（含尚未补齐的）
    }
