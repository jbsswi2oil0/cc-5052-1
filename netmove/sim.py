"""共享确定性模拟内核。

客户端与服务端都 import 这一个文件，保证移动参数与碰撞解释完全一致。
所有函数都是纯函数式的固定步长更新：同样的状态 + 同样的输入序列
必然得到同样的结果，这是预测/校正能够收敛的基础。

三个容易混淆的编号概念（在界面上分别显示）：
  seq   输入序号：每个模拟步产生一条输入，序号单调递增
  step  模拟步：  内核固定 30Hz 推进一次，step == 已应用输入条数
  arrive 到达序：  网络层实际收到报文的先后，可与 seq 不同（乱序）
"""

import math

TICK_HZ = 30
TICK_DT = 1.0 / TICK_HZ

# 双方一致的移动参数
SPEED = 170.0          # 像素/秒
RADIUS = 14.0          # 角色半径，碰撞体与显示体共用同一个位置
ARENA_W = 560
ARENA_H = 620

SPAWN = (60.0, 60.0)

# 静态墙体矩形 (x, y, w, h)
WALLS = [
    (120, 130, 320, 26),
    (60, 280, 26, 240),
    (200, 430, 300, 26),
    (420, 200, 26, 170),
]

# 斜坡线段 (x0, y0) -> (x1, y1)：从地面 (120,606) 升到 (300,500)。
# 角色圆心进入坡面一个半径内时被吸附到坡面（确定性，无弹性），
# 该拍状态里 on_slope=1。坡顶与墙体 (200,430,300,26) 底边相距 44px，
# 大于吸附半径，两处碰撞互不影响。
SLOPE = (120.0, 606.0, 300.0, 500.0)


def slope_surface_y(x):
    """x 处坡面高度；不在坡段水平范围内返回 None。"""
    x0, y0, x1, y1 = SLOPE
    if x < min(x0, x1) or x > max(x0, x1):
        return None
    t = (x - x0) / (x1 - x0)
    return y0 + t * (y1 - y0)


def initial_state():
    """权威初始状态。显示位置与碰撞体位置是同一个 x/y，不做分离。"""
    return {
        "x": SPAWN[0],
        "y": SPAWN[1],
        "step": 0,
        "on_slope": 0,
    }


def normalize_input(inp):
    """把原始按键方向归一化为单位长度向量（斜向不加速）。"""
    dx = float(inp.get("dx", 0))
    dy = float(inp.get("dy", 0))
    mag = math.hypot(dx, dy)
    if mag > 0.0:
        dx /= mag
        dy /= mag
    return dx, dy


def _circle_vs_walls(x, y):
    """圆角色 vs 轴对齐墙体：逐轴推出，保证碰撞体离开墙体。"""
    r = RADIUS
    for (wx, wy, ww, wh) in WALLS:
        # 找矩形上离圆心最近的点
        nx = min(max(x, wx), wx + ww)
        ny = min(max(y, wy), wy + wh)
        ddx = x - nx
        ddy = y - ny
        d2 = ddx * ddx + ddy * ddy
        if d2 >= r * r:
            continue
        if d2 > 1e-12:
            # 圆心在矩形外：沿最近点方向推到表面
            dist = math.sqrt(d2)
            push = r - dist
            x += ddx / dist * push
            y += ddy / dist * push
        else:
            # 圆心嵌入矩形内部：沿穿透最浅的轴推出
            left = x - wx
            right = (wx + ww) - x
            top = y - wy
            bottom = (wy + wh) - y
            m = min(left, right, top, bottom)
            if m == left:
                x = wx - r
            elif m == right:
                x = wx + ww + r
            elif m == top:
                y = wy - r
            else:
                y = wy + wh + r
    return x, y


def step(state, inp):
    """一个固定模拟步。inp 可能为 None（缺口补偿：零向量空转一步）。"""
    if inp is None:
        dx, dy = 0.0, 0.0
    else:
        dx, dy = normalize_input(inp)

    nx = state["x"] + dx * SPEED * TICK_DT
    ny = state["y"] + dy * SPEED * TICK_DT

    # 场地边界
    nx = min(max(nx, RADIUS), ARENA_W - RADIUS)
    ny = min(max(ny, RADIUS), ARENA_H - RADIUS)

    # 墙体碰撞（碰撞体与显示体同一坐标，校正时一起移动）
    nx, ny = _circle_vs_walls(nx, ny)

    # 坡面吸附：圆心距坡面一个半径内时贴到坡面上（纯位置约束）
    on_slope = 0
    sy = slope_surface_y(nx)
    if sy is not None and abs(ny - sy) <= RADIUS:
        ny = sy
        on_slope = 1

    return {
        "x": nx,
        "y": ny,
        "step": state["step"] + 1,
        "on_slope": on_slope,
    }


def replay(state, inputs):
    """从某状态确定性重放一串输入，用于校正后重新执行未确认部分。"""
    for inp in inputs:
        state = step(state, inp)
    return state


def state_hash(state):
    """跨进程比对状态用的紧凑指纹。"""
    return "s%d@(%.3f,%.3f)%s" % (
        state["step"], state["x"], state["y"],
        "S" if state.get("on_slope") else "-")
