"""
core/waveform_engine.py — Phone-style audio waveform overlay.

Design goals:
  • Sharp, vivid bars — flat full-brightness colours (no luma-weighting)
  • Clear gap between bars (gap_half = bar_px // 5)
  • Rounded 4 corners on the whole waveform area (geq alpha mask)
  • Soft glow via two-pass overlay (glow behind + sharp bars on top)
  • Bright green #00FF50 at tips, pale mint #B4FFD4 at baseline
  • 9-frame tmix → slow, natural movement
"""

import subprocess

_W = 1920
_H = 1080
_MARGIN_BOTTOM = 10

_N_BARS       = 48
_TMIX_FRAMES  = 9
_TMIX_WEIGHTS = "1 2 3 4 5 4 3 2 1"

# Vivid colour palette (flat — no luma weighting)
_TIP  = (  0, 255,  80)   # bright green   #00FF50  (bar tip  / loud)
_BASE = (180, 255, 210)   # pale mint       #B4FFD2  (baseline / quiet)

# Rounded corners on the waveform overlay
_CORNER_R = 5

# Glow params (applied behind sharp bars)
_GLOW_SIGMA = 4
_GLOW_STEPS = 1

# Colorkey threshold — removes background noise left by tmix partial pixels
_CK_SIMILARITY = 0.25


class WaveformEngine:

    def _color_expr(self, tip_ch: int, base_ch: int,
                    bar_px: int, gap_half: int) -> str:
        """Per-channel geq expression — flat vivid colour, no luma weighting.

        Y=0 = bar tip (loud)      → tip_ch  (bright green end)
        Y=H = flat baseline       → base_ch (pale mint)
        Pixels in gap zones → 0 (transparent after colorkey).
        Pixels below threshold (tmix partial) → 0.
        All clearly-lit pixels → full flat colour (vivid, sharp).
        """
        delta = base_ch - tip_ch
        if delta == 0:
            color = str(tip_ch)
        elif delta > 0:
            color = f"{tip_ch}+{delta}*Y/H"
        else:
            color = f"{tip_ch}-{abs(delta)}*Y/H"

        # Gap zones: left and right edge columns of each bar
        in_gap = (f"(lt(mod(X,{bar_px}),{gap_half})"
                  f"+gte(mod(X,{bar_px}),{bar_px - gap_half}))")

        # Threshold 60: only clearly lit pixels (> 60/255) get colour.
        # Removes dim tmix-transition pixels → sharp bar edges.
        return f"if({in_gap}+lt(p(X,Y),60),0,{color})"

    def subtitle_margin_v(self, wf_height: int, preset_margin_v: int = 45) -> int:
        needed = wf_height + _MARGIN_BOTTOM + 10
        return max(preset_margin_v, needed)

    def build_filter(
        self,
        wf_display_w: int,
        wf_h:         int,
        y_pos:        int,
        x_off:        int,
        fps:          int,
        a_label: str = "0:a",
        v_label: str = "0:v",
        out:     str = "out",
    ) -> str:
        """Complete filter_complex fragment for sharp waveform with rounded corners.

        Chain:
          showwaves → tmix → crop → vflip → scale →
          geq (flat vivid colour, wider gap) →
          colorkey (hard cutoff, similarity=0.25) →
          rounded-corner alpha mask (geq on small RGBA) →
          split → gblur (glow behind) + sharp copy →
          two-pass overlay on video
        """
        wf_h2    = wf_h * 2
        bar_px   = max(1, wf_display_w // _N_BARS)
        gap_half = max(2, bar_px // 5)   # wider gap for clearer bar separation

        r_e = self._color_expr(_TIP[0], _BASE[0], bar_px, gap_half)
        g_e = self._color_expr(_TIP[1], _BASE[1], bar_px, gap_half)
        b_e = self._color_expr(_TIP[2], _BASE[2], bar_px, gap_half)

        # Rounded corner mask — applied to the small RGBA waveform image
        cr = _CORNER_R
        # A pixel is outside the rounded rectangle if it's in a corner zone
        # outside the inscribed circle of radius cr at that corner.
        in_tl = f"(lt(X,{cr})*lt(Y,{cr})*gt(hypot(X-{cr},Y-{cr}),{cr}))"
        in_tr = f"(gt(X,W-{cr})*lt(Y,{cr})*gt(hypot(X-(W-{cr}),Y-{cr}),{cr}))"
        in_bl = f"(lt(X,{cr})*gt(Y,H-{cr})*gt(hypot(X-{cr},Y-(H-{cr})),{cr}))"
        in_br = f"(gt(X,W-{cr})*gt(Y,H-{cr})*gt(hypot(X-(W-{cr}),Y-(H-{cr})),{cr}))"
        outside_corners = f"({in_tl}+{in_tr}+{in_bl}+{in_br})"
        rounded_mask = (
            f"geq=r='r(X,Y)':g='g(X,Y)':b='b(X,Y)'"
            f":a='if({outside_corners},0,a(X,Y))'"   # a() not alpha()
        )

        return (
            # 1 — sparse bars at 2× height (cline symmetric)
            f"[{a_label}]showwaves=s={_N_BARS}x{wf_h2}"
            f":mode=cline:colors=white:scale=sqrt:draw=full:r={fps}[_wraw];"

            # 2 — 9-frame smooth temporal averaging
            f"[_wraw]tmix=frames={_TMIX_FRAMES}:weights='{_TMIX_WEIGHTS}'[_wsmooth];"

            # 3 — crop bottom half: bars grow down from centre in cline mode
            f"[_wsmooth]crop={_N_BARS}:{wf_h}:0:{wf_h}[_wcrop];"

            # 4 — flip: flat bottom, bars grow upward
            f"[_wcrop]vflip[_wflip];"

            # 5 — scale to display width with neighbor → pixel-sharp blocks
            f"[_wflip]scale={wf_display_w}:{wf_h}:flags=neighbor[_wscaled];"

            # 6 — flat vivid colour (no luma weight) + wide gap mask
            f"[_wscaled]geq=r='{r_e}':g='{g_e}':b='{b_e}'[_wcol];"

            # 7 — hard colorkey (similarity=0.25 removes tmix partial pixels)
            f"[_wcol]colorkey=0x000000:similarity={_CK_SIMILARITY}:blend=0"
            f",format=rgba[_wsharp_raw];"

            # 8 — rounded corners: zero alpha in corner zones
            f"[_wsharp_raw]{rounded_mask}[_wsharp];"

            # 9 — split: glow copy (blurred RGBA) + sharp copy (crisp RGBA)
            f"[_wsharp]split=2[_ws1][_ws2];"
            f"[_ws2]gblur=sigma={_GLOW_SIGMA}:steps={_GLOW_STEPS}[_wglow];"

            # 10 — two-pass overlay: glow first (halo behind), sharp bars on top
            f"[{v_label}][_wglow]overlay={x_off}:{y_pos}:format=auto[_vg];"
            f"[_vg][_ws1]overlay={x_off}:{y_pos}:format=auto[{out}]"
        )

    def apply_waveform(
        self,
        input_path:     str,
        output_path:    str,
        fps:            int   = 60,
        wf_width_ratio: float = 0.30,
        wf_height:      int   = 52,
        video_codec:    str   = "libx264",
        quality_preset: str   = "balanced",
    ) -> dict:
        _PRESET = {"fast": "ultrafast", "balanced": "medium", "quality": "slow"}
        _CRF    = {"fast": 28,          "balanced": 23,        "quality": 18}

        wf_display_w = int(_W * wf_width_ratio)
        if wf_display_w % 2:
            wf_display_w -= 1
        wf_h  = wf_height if wf_height % 2 == 0 else wf_height + 1
        y_pos = _H - wf_h - _MARGIN_BOTTOM
        x_off = (_W - wf_display_w) // 2

        filt = self.build_filter(wf_display_w, wf_h, y_pos, x_off, fps)
        cmd  = [
            "ffmpeg", "-y",
            "-i", input_path,
            "-filter_complex", filt,
            "-map", "[out]", "-map", "0:a",
            "-c:v", video_codec,
            "-preset", _PRESET.get(quality_preset, "medium"),
            "-crf",    str(_CRF.get(quality_preset, 23)),
            "-c:a", "copy",
            "-pix_fmt", "yuv420p",
            "-movflags", "+faststart",
            output_path,
        ]
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
            if proc.returncode == 0:
                return {"ok": True}
            return {"ok": False, "error": proc.stderr[-1500:]}
        except subprocess.TimeoutExpired:
            return {"ok": False, "error": "Waveform encode timed out"}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}
