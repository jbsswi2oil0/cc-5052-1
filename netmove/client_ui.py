"""联机场景窗口（pygame），由 PredictionKernel 驱动。

分层原则（任务硬性要求）：
  模拟状态  = kernel.state，含碰撞体坐标，校正时整体重置+重放；
  渲染平滑  = 本文件里的 render_pos，只在两个已确认的模拟状态之间插值，
              从不参与碰撞，也不会改变 kernel 的任何坐标。

启动（见文件末尾说明，也可用 python3 -m netmove.launch 一键拉起三进程）：
  python3 -m netmove.proxy
  python3 -m netmove.server
  python3 -m netmove.client_ui
"""

import json
import socket
import sys
import time
from pathlib import Path

import pygame

from . import sim
from .client_kernel import PredictionKernel, UdpTransport
from .protocol import PROXY_CTRL_PORT, LOOPBACK

WIN_W, WIN_H = 1180, 820
SCENE_PAD = 12
PANEL_X = SCENE_PAD + sim.ARENA_W + 16
PANEL_W = WIN_W - PANEL_X - 10


class ProxyControl:
    """通过 TCP 控制通道实时调节代理损伤，周期拉取统计快照。"""

    def __init__(self):
        self.sock = None
        self.connected = False
        self.buf = b""
        self.snapshot = {"cfg": {}, "stats": {}, "events": []}
        self._last_pull = 0.0
        self._last_retry = 0.0
        self._connect()

    def _connect(self):
        # 回环控制面用阻塞短超时即可，避免非阻塞 connect 的 EINPROGRESS
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(0.25)
        try:
            s.connect((LOOPBACK, PROXY_CTRL_PORT))
        except OSError:
            s.close()
            self.connected = False
            self.sock = None
            return
        s.setblocking(False)
        self.sock = s
        self.connected = True
        self.buf = b""

    def _send(self, obj):
        if not self.connected or self.sock is None:
            return
        try:
            self.sock.sendall((json.dumps(obj) + "\n").encode())
        except OSError:
            self.connected = False

    def set(self, key, value):
        self._send({"a": "set", key: value})

    def burst(self, n=3):
        self._send({"a": "burst", "n": n})

    def reset_stats(self):
        self._send({"a": "reset_stats"})

    def pump(self):
        if not self.connected:
            # 断线/未连上时限速重连，启动顺序因此不敏感
            now = time.monotonic()
            if now - self._last_retry > 0.5:
                self._last_retry = now
                self._connect()
            return
        now = time.monotonic()
        if now - self._last_pull > 0.2:
            self._send({"a": "get"})
            self._last_pull = now
        try:
            data = self.sock.recv(65535)
            if not data:
                self.connected = False
                return
            self.buf += data
        except BlockingIOError:
            pass
        except OSError:
            self.connected = False
            return
        while b"\n" in self.buf:
            line, self.buf = self.buf.split(b"\n", 1)
            try:
                self.snapshot = json.loads(line)
            except json.JSONDecodeError:
                pass


FONT_PATH = str(Path(__file__).resolve().parent / "assets"
                / "SourceHanSansSC.otf")


def make_font(size, bold=False):
    # 优先用随项目分发的中文字体，避免环境缺 CJK 字体时显示方块
    try:
        font = pygame.font.Font(FONT_PATH, size)
        font.set_bold(bold)
        return font
    except OSError:
        names = ["notosanscjk", "wenquanyimicrohei", "wenquanyizenhei",
                 "droidsansfallback", "dejavusans"]
        return pygame.font.SysFont(",".join(names), size, bold=bold)


class Button:
    def __init__(self, rect, label):
        self.rect = pygame.Rect(rect)
        self.label = label

    def hit(self, pos):
        return self.rect.collidepoint(pos)


class ClientUI:
    def __init__(self, smoke=False):
        pygame.init()
        pygame.display.set_caption("预测移动联机原型 · 客户端预测 / 服务端校正")
        self.screen = pygame.display.set_mode((WIN_W, WIN_H))
        self.clock = pygame.time.Clock()
        self.f_sm = make_font(13)
        self.f_md = make_font(15, bold=True)
        self.f_lg = make_font(19, bold=True)
        self.f_tiny = make_font(12)

        self.kernel = PredictionKernel(UdpTransport())
        self.ctrl = ProxyControl()
        self.smoke = smoke
        self.smoke_frames = 0

        # 渲染层坐标（仅画面平滑用，不参与模拟/碰撞）
        self.render_pos = (sim.initial_state()["x"], sim.initial_state()["y"])
        self._prev_sim = dict(sim.initial_state())
        self._acc = 0.0
        self._last = time.monotonic()
        self.flash_until = 0.0
        self.ui_log = []

        cfg0 = self.ctrl.snapshot.get("cfg", {})
        # 仅存范围与初值，实际矩形在绘制时按流式 y 坐标生成
        self.slider_specs = {
            "delay_ms": {"min": 0, "max": 500,
                         "val": cfg0.get("delay_ms", 120)},
            "jitter_ms": {"min": 0, "max": 150,
                          "val": cfg0.get("jitter_ms", 40)},
            "loss_p": {"min": 0, "max": 0.6,
                       "val": cfg0.get("loss_p", 0.0)},
            "reorder_p": {"min": 0, "max": 0.8,
                          "val": cfg0.get("reorder_p", 0.0)},
        }
        self.sliders = {}
        self.drag = None
        self.buttons = {
            "burst3": Button((0, 0, 135, 28), "突发丢包 x3"),
            "burst8": Button((0, 0, 135, 28), "突发丢包 x8"),
            "reset": Button((0, 0, 135, 28), "清零代理统计"),
        }

    # ------------------------------------------------------------------
    def _read_movement(self):
        if self.smoke:
            # 无头自测用的脚本移动：逐 tick 实时产生，不预读未来输入，
            # 与真人按键走完全相同的 produce_input 路径。
            t = self.kernel.tick_count
            pattern = [(1, 0), (1, 0), (0, 1), (1, 1), (-1, 0), (0, -1)]
            return pattern[t % len(pattern)]
        keys = pygame.key.get_pressed()
        dx = (1 if keys[pygame.K_RIGHT] or keys[pygame.K_d] else 0) - \
             (1 if keys[pygame.K_LEFT] or keys[pygame.K_a] else 0)
        dy = (1 if keys[pygame.K_DOWN] or keys[pygame.K_s] else 0) - \
             (1 if keys[pygame.K_UP] or keys[pygame.K_w] else 0)
        return dx, dy

    def _sim_tick(self):
        dx, dy = self._read_movement()
        self.kernel.produce_input(dx, dy)
        snap = self.kernel.snapshot()
        corr = snap["last_correction"]
        if corr and corr["t"] >= self._last - sim.TICK_DT * 2 and corr["dist"] > 0.5:
            self.flash_until = time.monotonic() + 0.6
            self.ui_log.insert(0, "校正 %.1fpx (重放%d条)"
                               % (corr["dist"], corr["replayed"]))
            del self.ui_log[6:]
            # 校正跨越了非连续状态，渲染基准直接锚定新模拟状态，
            # 避免在“被否定的旧预测”与权威状态之间做错误插值
            self._prev_sim = dict(self.kernel.state)
            self._acc = 0.0

    # ------------------------------------------------------------------
    def _draw_scene(self, snap, alpha):
        sx0, sy0 = SCENE_PAD, SCENE_PAD
        pygame.draw.rect(self.screen, (24, 26, 32),
                         (sx0, sy0, sim.ARENA_W, sim.ARENA_H))
        # 墙体
        for (wx, wy, ww, wh) in sim.WALLS:
            pygame.draw.rect(self.screen, (110, 120, 140),
                             (sx0 + wx, sy0 + wy, ww, wh), border_radius=3)
        # 权威幽灵（服务端认可位置）
        auth = snap["authoritative"]
        if auth:
            pygame.draw.circle(self.screen, (70, 150, 90),
                               (int(sx0 + auth["x"]), int(sy0 + auth["y"])),
                               int(sim.RADIUS), 2)
        # 渲染位置：在 prev 与当前模拟状态之间插值，仅视觉平滑
        cur = snap["state"]
        rx = self._prev_sim["x"] + (cur["x"] - self._prev_sim["x"]) * alpha
        ry = self._prev_sim["y"] + (cur["y"] - self._prev_sim["y"]) * alpha
        self.render_pos = (rx, ry)

        # 校正闪现：从旧位置到新位置画一条校正向量
        corr = snap["last_correction"]
        if corr and time.monotonic() < self.flash_until:
            fx, fy = corr["from"]
            tx, ty = corr["to"]
            pygame.draw.line(self.screen, (230, 90, 90),
                             (sx0 + fx, sy0 + fy),
                             (sx0 + tx, sy0 + ty), 2)

        col = (235, 170, 60) if not snap["degraded"] else (230, 90, 90)
        pygame.draw.circle(self.screen, col,
                           (int(sx0 + rx), int(sy0 + ry)), int(sim.RADIUS))
        pygame.draw.circle(self.screen, (255, 255, 255),
                           (int(sx0 + rx), int(sy0 + ry)),
                           int(sim.RADIUS), 1)

        head = self.f_md.render("运动场景（圆=本地预测/碰撞体，绿圈=服务端权威）",
                                True, (220, 220, 220))
        self.screen.blit(head, (sx0, sy0 + sim.ARENA_H + 6))
        tip = self.f_sm.render("WASD/方向键移动 · 圆与碰撞体同坐标，校正时一起移动",
                               True, (150, 150, 160))
        self.screen.blit(tip, (sx0, sy0 + sim.ARENA_H + 28))

    def _draw_panel(self, snap):
        x = PANEL_X
        pygame.draw.line(self.screen, (70, 72, 80),
                         (x - 8, 10), (x - 8, WIN_H - 10), 1)
        y = 8

        def line(text, color=(210, 210, 215), font=None):
            nonlocal y
            font = font or self.f_sm
            self.screen.blit(font.render(text, True, color), (x, y))
            y += font.get_height() + (1 if font is self.f_tiny else 3)

        def gap(h=6):
            nonlocal y
            y += h

        line("客户端（预测内核）", (240, 200, 110), self.f_lg)
        st = snap["state"]
        line("模拟步 step=%d   下一条 seq=%d" % (st["step"], snap["next_seq"]))
        line("模拟/碰撞坐标 (%.1f, %.1f)" % (st["x"], st["y"]))
        line("渲染坐标(仅平滑) (%.1f, %.1f)" % self.render_pos, (150, 150, 160))
        line("ack 到达序(最近): %s" % (snap["ack_order"][-6:] or "尚无"))
        if snap["degraded"]:
            line("链路告警: 是（%.0fms 无确认，预测继续）"
                 % ((time.monotonic() - self.kernel.last_ack_time) * 1000),
                 (230, 110, 110))
        else:
            line("链路告警: 否", (150, 200, 150))

        gap()
        line("服务端（权威）", (120, 200, 140), self.f_lg)
        auth = snap["authoritative"]
        if auth:
            line("权威坐标 (%.1f, %.1f) step=%d"
                 % (auth["x"], auth["y"], auth["step"]))
        else:
            line("权威坐标: 等待首个 ack…", (160, 160, 160))
        line("已确认 ack_seq=%s   服务端 gap=%s"
             % (snap["ack_seq"], snap["server_gap"]),
             (230, 150, 90) if snap["server_gap"] else (210, 210, 215))
        line("缺口起点: %s" % (snap.get("server_gaps") or "无"),
             font=self.f_tiny)
        line("已补齐缺失序号: %s" % (snap["server_drops"] or "无"),
             font=self.f_tiny)

        gap()
        err = snap["pred_error"]
        line("预测误差（上次校正）: %s"
             % ("%.2f px" % err if err is not None else "—"), (230, 150, 90))
        line("历史最大误差: %.2f px" % snap["max_error"])

        gap()
        line("未确认队列 pending（%d 条，最多显示 5）"
             % len(snap["pending"]), (140, 180, 240), self.f_md)
        for e in snap["pending"][:5]:
            line("  seq=%d 向量(%d,%d) 已发%d次 重传%d"
                 % (e["seq"], e["inp"]["dx"], e["inp"]["dy"],
                    e["sent"], e["retrans"]), (180, 190, 210), self.f_tiny)
        if len(snap["pending"]) > 5:
            line("  …另有 %d 条" % (len(snap["pending"]) - 5), font=self.f_tiny)

        gap(8)
        line("本地代理损伤调节（TCP 控制通道）", (200, 160, 220), self.f_md)
        cfg = self.ctrl.snapshot.get("cfg", {})
        stats = self.ctrl.snapshot.get("stats", {})
        labels = {"delay_ms": "延迟 ms", "jitter_ms": "抖动 ms",
                  "loss_p": "丢包率", "reorder_p": "乱序率"}
        self.sliders = {}
        track_x = x + 95
        track_w = PANEL_W - 105
        for key, spec in self.slider_specs.items():
            val = cfg.get(key, spec["val"])
            spec["val"] = val
            shown = ("%.0f" % val) if key.endswith("ms") else ("%.2f" % val)
            line("%s %s" % (labels[key], shown), font=self.f_tiny)
            rect = pygame.Rect(track_x, y - 2, track_w, 8)
            pygame.draw.rect(self.screen, (90, 92, 100), rect,
                             border_radius=3)
            t = (val - spec["min"]) / max(1e-9, spec["max"] - spec["min"])
            knob_x = rect.x + int(rect.w * t)
            pygame.draw.circle(self.screen, (200, 200, 210),
                               (knob_x, rect.centery), 7)
            self.sliders[key] = {"r": rect, "min": spec["min"],
                                 "max": spec["max"], "val": val}
            y += 16

        gap(4)
        bx = x
        for key, btn in self.buttons.items():
            btn.rect.update(bx, y, 135, 28)
            pygame.draw.rect(self.screen, (70, 74, 88), btn.rect,
                             border_radius=5)
            pygame.draw.rect(self.screen, (140, 145, 160), btn.rect, 1,
                             border_radius=5)
            tw = self.f_sm.size(btn.label)[0]
            self.screen.blit(
                self.f_sm.render(btn.label, True, (235, 235, 240)),
                (btn.rect.centerx - tw // 2, btn.rect.y + 6))
            bx += 145
        y += 36

        line("代理统计 上行 收%d/发%d/丢%d 乱序%d"
             % (stats.get("up_recv", 0), stats.get("up_sent", 0),
                stats.get("up_dropped", 0), stats.get("reorders", 0)),
             font=self.f_tiny)
        line("          下行 收%d/发%d/丢%d"
             % (stats.get("down_recv", 0), stats.get("down_sent", 0),
                stats.get("down_dropped", 0)), font=self.f_tiny)
        if not self.ctrl.connected:
            line("控制通道未连接（等待 proxy 启动…）", (230, 110, 110),
                 self.f_tiny)

        gap(6)
        line("事件（内核/校正）", (240, 200, 110), self.f_md)
        for ev in snap["events"][:4]:
            line(ev, (185, 190, 200), self.f_tiny)
        for ev in self.ui_log[:2]:
            line(ev, (230, 150, 120), self.f_tiny)
        for ev in self.ctrl.snapshot.get("events", [])[:2]:
            line("proxy: " + ev, (190, 160, 210), self.f_tiny)

    # ------------------------------------------------------------------
    def _handle_click(self, pos):
        for key, btn in self.buttons.items():
            if btn.hit(pos):
                if key == "burst3":
                    self.ctrl.burst(3)
                elif key == "burst8":
                    self.ctrl.burst(8)
                else:
                    self.ctrl.reset_stats()
                return
        for key, sl in self.sliders.items():
            r = sl["r"].inflate(0, 14)
            if r.collidepoint(pos):
                self.drag = key
                self._update_slider(key, pos[0])
                return

    def _update_slider(self, key, mouse_x):
        sl = self.sliders[key]
        t = min(1.0, max(0.0, (mouse_x - sl["r"].x) / sl["r"].w))
        val = sl["min"] + t * (sl["max"] - sl["min"])
        if key.endswith("_p"):
            val = round(val, 2)
        else:
            val = round(val)
        self.ctrl.set(key, val)

    def run(self):
        max_frames = 60 * 8 if self.smoke else None
        smoke_configured = not self.smoke
        running = True
        while running:
            now = time.monotonic()
            frame_dt = min(0.1, now - self._last)
            self._last = now

            if self.smoke and not smoke_configured and self.ctrl.connected:
                # 无头演示：真实注入延迟/乱序/丢包，验证校正路径被走过
                self.ctrl.set("delay_ms", 150)
                self.ctrl.set("jitter_ms", 50)
                self.ctrl.set("loss_p", 0.1)
                self.ctrl.set("reorder_p", 0.15)
                self.ctrl.burst(5)
                smoke_configured = True

            for event in pygame.event.get():
                if event.type == pygame.QUIT:
                    running = False
                elif event.type == pygame.KEYDOWN and event.key == pygame.K_ESCAPE:
                    running = False
                elif event.type == pygame.MOUSEBUTTONDOWN and event.button == 1:
                    self._handle_click(event.pos)
                elif event.type == pygame.MOUSEBUTTONUP and event.button == 1:
                    self.drag = None
            if self.drag:
                self._update_slider(self.drag, pygame.mouse.get_pos()[0])

            # 固定步长推进模拟；渲染帧率独立
            self._acc += frame_dt
            steps = 0
            while self._acc >= sim.TICK_DT and steps < 5:
                prev_state = dict(self.kernel.state)
                self._sim_tick()
                self._prev_sim = prev_state
                self._acc -= sim.TICK_DT
                steps += 1

            self.ctrl.pump()
            self.kernel.pump_acks()
            snap = self.kernel.snapshot()

            self.screen.fill((18, 19, 24))
            self._draw_scene(snap, min(1.0, self._acc / sim.TICK_DT))
            self._draw_panel(snap)
            pygame.display.flip()
            self.clock.tick(60)

            if self.smoke:
                self.smoke_frames += 1
                if self.smoke_frames == 300:
                    pygame.image.save(
                        self.screen,
                        str(Path(__file__).resolve().parent / "assets"
                            / "smoke_screenshot.png"))
                if max_frames and self.smoke_frames >= max_frames:
                    running = False

        final = self.kernel.snapshot()
        pygame.quit()
        return final


def main():
    smoke = "--smoke" in sys.argv
    ui = ClientUI(smoke=smoke)
    final = ui.run()
    if smoke and final is not None:
        print("[client_ui] smoke 结束: step=%d 位置=(%.2f,%.2f) pending=%d"
              % (final["state"]["step"], final["state"]["x"],
                 final["state"]["y"], len(final["pending"])))


if __name__ == "__main__":
    main()
