"""
core/effect_engine.py — Fast animated Ken Burns effects for still images.

Design philosophy (performance-first):
  • Every effect = ONE zoompan filter on letterboxed input → [out].
  • No split, no overlay, no gblur, no scale=eval=frame.
  • zoompan's z/x/y expressions use the built-in frame counter `n`.
  • Expected render speed: 3-10× real-time per segment (vs. 0.02× before).

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
        fps: int = DEFAULT_FPS,
    ) -> str:
        """
        Single-pass filter chain:
          scale+pad (letterbox to 1920×1080)
          → zoompan (Ken Burns animation via n-based expressions)
          → format=yuv420p
          → [out]

        FFmpeg 8.x broke the `z` variable inside x/y expressions (it evaluates
        to 0 on frame 0, causing division-by-zero and a silent crash).  Fix:
        substitute the zoom expression literally wherever x/y reference `/z`.
        """
        z_sub = f"({z_expr})"
        safe_x = x_expr.replace("/z", f"/{z_sub}")
        safe_y = y_expr.replace("/z", f"/{z_sub}")
        return (
            f"[0:v]"
            f"scale={_W}:{_H}:force_original_aspect_ratio=decrease:flags=lanczos,"
            f"pad={_W}:{_H}:(ow-iw)/2:(oh-ih)/2:color=black,"
            f"zoompan="
            f"z='{z_expr}':x='{safe_x}':y='{safe_y}':"
            f"d={total_frames}:s={_W}x{_H}:fps={fps},"
            f"format=yuv420p"
            f"[out]"
        )

    # ── effects ───────────────────────────────────────────────────────────────

    def zoom_pulse(self, duration: float, speed: str = "normal") -> str:
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
        return self._zoompan(z, x, y, tf)

    def pan_horizontal(self, duration: float, speed: str = "normal") -> str:
        """Pan left ↔ right at 1.4× zoom (30 % crop = room to move)."""
        cf = self._cycle_frames(speed)
        tf = self._total_frames(duration)
        z  = "1.4"
        x  = f"(iw-iw/z)/2*(1-cos(2*PI*on/{cf}))"
        y  = "(ih-ih/z)/2"
        return self._zoompan(z, x, y, tf)

    def pan_vertical(self, duration: float, speed: str = "normal") -> str:
        """Pan top ↔ bottom at 1.4× zoom."""
        cf = self._cycle_frames(speed)
        tf = self._total_frames(duration)
        z  = "1.4"
        x  = "(iw-iw/z)/2"
        y  = f"(ih-ih/z)/2*(1-cos(2*PI*on/{cf}))"
        return self._zoompan(z, x, y, tf)

    def pan_diagonal(self, duration: float, speed: str = "normal") -> str:
        """Pan top-left ↔ bottom-right diagonally at 1.4× zoom."""
        cf = self._cycle_frames(speed)
        tf = self._total_frames(duration)
        z  = "1.4"
        osc = f"(1-cos(2*PI*on/{cf}))/2"
        x  = f"(iw-iw/z)*({osc})"
        y  = f"(ih-ih/z)*({osc})"
        return self._zoompan(z, x, y, tf)

    def tilt_wave(self, duration: float, speed: str = "normal") -> str:
        """
        Gentle diagonal sway that mimics a camera tilt / handheld feel.
        Zoom fixed at 1.3×; x and y oscillate in opposing directions.
        """
        cf = self._cycle_frames(speed)
        tf = self._total_frames(duration)
        z  = "1.3"
        x  = f"(iw-iw/z)/2 + (iw-iw/z)/3*sin(2*PI*on/{cf})"
        y  = f"(ih-ih/z)/2 - (ih-ih/z)/3*sin(2*PI*on/{cf})"
        return self._zoompan(z, x, y, tf)

    # ── public API ────────────────────────────────────────────────────────────

    def get_effect(self, name: str, duration: float, speed: str = "normal") -> str:
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
        return fn(duration, speed)

    def get_random_effect(self, duration: float, speed: str = "normal") -> tuple[str, str]:
        name = random.choice(self.EFFECTS)
        return name, self.get_effect(name, duration, speed)


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