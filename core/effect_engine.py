"""
core/effect_engine.py — Fast animated Ken Burns effects for still images.

Design (performance-first):
  • Background: cover-scale + gblur on 1-fps input stream (cost = 1 op/sec).
  • Foreground: contain-scale overlaid centred on blurred background.
  • zoompan animates the composite at target fps.
  • zoompan's z/x/y expressions use the built-in frame counter `on`.

Output: 1920×1080, yuv420p, map label [out].
"""

import math
import os
import random
import subprocess

from config import (
    DEFAULT_AUDIO_BITRATE,
    DEFAULT_AUDIO_CODEC,
    DEFAULT_FPS,
    DEFAULT_VIDEO_CODEC,
    EFFECT_SPEEDS,
)

_W   = 1920
_H   = 1080
_PI  = math.pi


class EffectEngine:

    EFFECTS = ["zoom_pulse", "pan_horizontal", "pan_vertical", "pan_diagonal", "tilt_wave"]

    # ── internal helpers ──────────────────────────────────────────────────────

    def _cycle_frames(self, speed: str, fps: int = DEFAULT_FPS) -> int:
        return EFFECT_SPEEDS.get(speed, EFFECT_SPEEDS["normal"]) * fps

    def _total_frames(self, duration: float, fps: int = DEFAULT_FPS) -> int:
        # Small extra buffer; -shortest trims the output at audio length.
        return int(duration * fps) + fps

    def _zoompan(
        self,
        z_expr:  str,
        x_expr:  str,
        y_expr:  str,
        total_frames: int,
        fps:   int   = DEFAULT_FPS,
        scale: float = 1.0,
    ) -> str:
        """Blur-fill letterbox + Ken Burns animation.

        ① Split 1-fps input → bg copy + fg copy
        ② bg:  cover-scale → center-crop → strong blur  (fills the frame)
        ③ fg:  contain-scale at (scale × output size); scale<1.0 exposes the blur border
        ④ Overlay fg centred on blurred bg
        ⑤ zoompan animates the composite
        """
        scale  = max(0.1, min(1.0, scale))
        z_sub  = f"({z_expr})"
        safe_x = x_expr.replace("/z", f"/{z_sub}")
        safe_y = y_expr.replace("/z", f"/{z_sub}")

        if scale >= 0.999:
            # ── scale = 100 %: original 2-split pipeline ──────────────────────
            return (
                f"[0:v]split=2[_bg][_fg];"
                f"[_bg]scale={_W}:{_H}:force_original_aspect_ratio=increase:flags=lanczos,"
                f"crop={_W}:{_H},gblur=sigma=30[_blurbg];"
                f"[_fg]scale={_W}:{_H}:force_original_aspect_ratio=decrease:flags=lanczos[_fgfit];"
                f"[_blurbg][_fgfit]overlay=(W-w)/2:(H-h)/2[_composite];"
                f"[_composite]zoompan=z='{z_expr}':x='{safe_x}':y='{safe_y}':"
                f"d={total_frames}:s={_W}x{_H}:fps={fps},format=yuv420p[out]"
            )

        # ── scale < 100 %: Ken Burns on full image → scale down → outer blur ──
        # This guarantees the blur border is always visible regardless of zoom level.
        fg_w = int(_W * scale)
        fg_h = int(_H * scale)
        if fg_w % 2 != 0: fg_w -= 1
        if fg_h % 2 != 0: fg_h -= 1
        return (
            # three copies: outer blur bg, inner blur bg, foreground
            f"[0:v]split=3[_bg1][_bg2][_fg];"
            # outer blur: always-visible background layer
            f"[_bg1]scale={_W}:{_H}:force_original_aspect_ratio=increase:flags=lanczos,"
            f"crop={_W}:{_H},gblur=sigma=30[_outblur];"
            # inner: blur + contain-scaled fg → composite for Ken Burns
            f"[_bg2]scale={_W}:{_H}:force_original_aspect_ratio=increase:flags=lanczos,"
            f"crop={_W}:{_H},gblur=sigma=30[_blurbg];"
            f"[_fg]scale={_W}:{_H}:force_original_aspect_ratio=decrease:flags=lanczos[_fgfit];"
            f"[_blurbg][_fgfit]overlay=(W-w)/2:(H-h)/2[_composite];"
            # Ken Burns on the composite
            f"[_composite]zoompan=z='{z_expr}':x='{safe_x}':y='{safe_y}':"
            f"d={total_frames}:s={_W}x{_H}:fps={fps}[_kbout];"
            # shrink the KB result to scale × frame size
            f"[_kbout]scale={fg_w}:{fg_h}:flags=lanczos[_kbscaled];"
            # centre the scaled KB result on the outer blur
            f"[_outblur][_kbscaled]overlay=(W-w)/2:(H-h)/2,format=yuv420p[out]"
        )

    # ── effects ───────────────────────────────────────────────────────────────

    def zoom_pulse(self, duration: float, speed: str = "normal", scale: float = 1.0) -> str:
        """
        Breathe effect: image content zooms in (60 % of pixels visible) then
        out (100 % visible) repeatedly.

        In zoompan coordinates:
          z=1.0  → full image visible (100 %)
          z=1.667 → only 60 % of pixels visible (magnified / zoomed in)
        """
        cf = self._cycle_frames(speed)
        tf = self._total_frames(duration)
        # FFmpeg 8.x: use 'on' (output frame number) — 'n' was removed from x/y context
        z  = f"1+0.667*abs(sin(PI*on/{cf}))"
        x  = "(iw-iw/z)/2"
        y  = "(ih-ih/z)/2"
        return self._zoompan(z, x, y, tf, scale=scale)

    def pan_horizontal(self, duration: float, speed: str = "normal", scale: float = 1.0) -> str:
        """Pan left ↔ right at 1.4× zoom (30 % crop = room to move)."""
        cf = self._cycle_frames(speed)
        tf = self._total_frames(duration)
        z  = "1.4"
        x  = f"(iw-iw/z)/2*(1-cos(2*PI*on/{cf}))"
        y  = "(ih-ih/z)/2"
        return self._zoompan(z, x, y, tf, scale=scale)

    def pan_vertical(self, duration: float, speed: str = "normal", scale: float = 1.0) -> str:
        """Pan top ↔ bottom at 1.4× zoom."""
        cf = self._cycle_frames(speed)
        tf = self._total_frames(duration)
        z  = "1.4"
        x  = "(iw-iw/z)/2"
        y  = f"(ih-ih/z)/2*(1-cos(2*PI*on/{cf}))"
        return self._zoompan(z, x, y, tf, scale=scale)

    def pan_diagonal(self, duration: float, speed: str = "normal", scale: float = 1.0) -> str:
        """Pan top-left ↔ bottom-right diagonally at 1.4× zoom."""
        cf = self._cycle_frames(speed)
        tf = self._total_frames(duration)
        z  = "1.4"
        osc = f"(1-cos(2*PI*on/{cf}))/2"
        x  = f"(iw-iw/z)*({osc})"
        y  = f"(ih-ih/z)*({osc})"
        return self._zoompan(z, x, y, tf, scale=scale)

    def tilt_wave(self, duration: float, speed: str = "normal", scale: float = 1.0) -> str:
        """Gentle diagonal sway that mimics a camera tilt / handheld feel."""
        cf = self._cycle_frames(speed)
        tf = self._total_frames(duration)
        z  = "1.3"
        x  = f"(iw-iw/z)/2 + (iw-iw/z)/3*sin(2*PI*on/{cf})"
        y  = f"(ih-ih/z)/2 - (ih-ih/z)/3*sin(2*PI*on/{cf})"
        return self._zoompan(z, x, y, tf, scale=scale)

    # ── public API ────────────────────────────────────────────────────────────

    def get_effect(self, name: str, duration: float, speed: str = "normal",
                   scale: float = 1.0) -> str:
        dispatch = {
            "zoom_pulse":     self.zoom_pulse,
            "pan_horizontal": self.pan_horizontal,
            "pan_vertical":   self.pan_vertical,
            "pan_diagonal":   self.pan_diagonal,
            "tilt_wave":      self.tilt_wave,
        }
        fn = dispatch.get(name)
        if fn is None:
            raise ValueError(f"Unknown effect '{name}'. Available: {list(dispatch)}")
        return fn(duration, speed, scale)

    def get_random_effect(self, duration: float, speed: str = "normal",
                          scale: float = 1.0) -> tuple[str, str]:
        name = random.choice(self.EFFECTS)
        return name, self.get_effect(name, duration, speed, scale)


# ── module-level utilities ────────────────────────────────────────────────────

def _probe_duration(audio_path: str) -> float:
    try:
        proc = subprocess.run(
            ["ffprobe", "-v", "quiet", "-show_entries", "format=duration",
             "-of", "csv=p=0", audio_path],
            capture_output=True, text=True, timeout=10,
        )
        return float(proc.stdout.strip())
    except Exception:
        return 5.0