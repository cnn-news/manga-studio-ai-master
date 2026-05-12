"""
core/effect_engine.py — Fast animated Ken Burns effects for still images.

Design (performance-first):
  • Background: cover-scale + gblur on 1-fps input stream (cost = 1 op/sec).
  • Foreground: contain-scale overlaid centred on blurred background.
  • zoompan animates the foreground at target fps.
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

    EFFECTS = [
        "zoom_pulse", "pan_horizontal", "pan_vertical", "pan_diagonal", "tilt_wave",
        "kb_in", "kb_out",
        "pan_left", "pan_right", "pan_up", "pan_down",
        "shake", "wobble",
    ]

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

        scale=1.0  → image fills the full 1920×1080 frame.
        scale<1.0  → image occupies (scale × frame) and is centred on a
                     blurred background.  The Ken Burns effect is applied only
                     to the image, not to the blur layer.
        """
        scale  = max(0.1, min(1.0, scale))
        z_sub  = f"({z_expr})"
        safe_x = x_expr.replace("/z", f"/{z_sub}")
        safe_y = y_expr.replace("/z", f"/{z_sub}")

        if scale >= 0.999:
            # ── scale = 100 %: image fills the whole frame ────────────────────
            return (
                f"[0:v]split=2[_bg][_fg];"
                f"[_bg]scale={_W}:{_H}:force_original_aspect_ratio=increase:flags=lanczos,"
                f"crop={_W}:{_H},gblur=sigma=40:steps=6[_blurbg];"
                f"[_fg]scale={_W}:{_H}:force_original_aspect_ratio=decrease:flags=lanczos[_fgfit];"
                f"[_blurbg][_fgfit]overlay=(W-w)/2:(H-h)/2[_composite];"
                f"[_composite]zoompan=z='{z_expr}':x='{safe_x}':y='{safe_y}':"
                f"d={total_frames}:s={_W}x{_H}:fps={fps},format=yuv420p[out]"
            )

        # ── scale < 100 %: Ken Burns on the IMAGE only, blur fills the border ─
        # The image is cover-scaled to fg_w × fg_h and animated there.
        # The blur background stays static at full 1920×1080.
        fg_w = int(_W * scale)
        fg_h = int(_H * scale)
        if fg_w % 2 != 0: fg_w -= 1
        if fg_h % 2 != 0: fg_h -= 1
        return (
            f"[0:v]split=2[_bg][_fg];"
            # static blurred background: scale up 2× before blur so downsample gives sharp edges,
            # then scale to output — eliminates the blocky square artifacts from single-pass gblur
            f"[_bg]scale={_W*2}:{_H*2}:force_original_aspect_ratio=increase:flags=lanczos,"
            f"crop={_W*2}:{_H*2},gblur=sigma=60:steps=6,"
            f"scale={_W}:{_H}:flags=lanczos[_outblur];"
            # image cover-scaled & cropped to the allocated fg area
            f"[_fg]scale={fg_w}:{fg_h}:force_original_aspect_ratio=increase:flags=lanczos,"
            f"crop={fg_w}:{fg_h}[_fgfit];"
            # Ken Burns animation on the image only, within fg_w × fg_h
            f"[_fgfit]zoompan=z='{z_expr}':x='{safe_x}':y='{safe_y}':"
            f"d={total_frames}:s={fg_w}x{fg_h}:fps={fps}[_kbout];"
            # upsample static blur so overlay is driven by the animated layer
            f"[_outblur]fps={fps}[_ob{fps}];"
            f"[_ob{fps}][_kbout]overlay=(W-w)/2:(H-h)/2,format=yuv420p[out]"
        )

    # ── effects ───────────────────────────────────────────────────────────────

    def zoom_pulse(self, duration: float, speed: str = "normal", scale: float = 1.0) -> str:
        """Breathe: image zooms in then out repeatedly."""
        cf = self._cycle_frames(speed)
        tf = self._total_frames(duration)
        z  = f"1+0.667*abs(sin(PI*on/{cf}))"
        x  = "(iw-iw/z)/2"
        y  = "(ih-ih/z)/2"
        return self._zoompan(z, x, y, tf, scale=scale)

    def pan_horizontal(self, duration: float, speed: str = "normal", scale: float = 1.0) -> str:
        """Oscillate left ↔ right at 1.4× zoom."""
        cf = self._cycle_frames(speed)
        tf = self._total_frames(duration)
        z  = "1.4"
        x  = f"(iw-iw/z)/2*(1-cos(2*PI*on/{cf}))"
        y  = "(ih-ih/z)/2"
        return self._zoompan(z, x, y, tf, scale=scale)

    def pan_vertical(self, duration: float, speed: str = "normal", scale: float = 1.0) -> str:
        """Oscillate top ↔ bottom at 1.4× zoom."""
        cf = self._cycle_frames(speed)
        tf = self._total_frames(duration)
        z  = "1.4"
        x  = "(iw-iw/z)/2"
        y  = f"(ih-ih/z)/2*(1-cos(2*PI*on/{cf}))"
        return self._zoompan(z, x, y, tf, scale=scale)

    def pan_diagonal(self, duration: float, speed: str = "normal", scale: float = 1.0) -> str:
        """Diagonal oscillation at 1.4× zoom."""
        cf = self._cycle_frames(speed)
        tf = self._total_frames(duration)
        z  = "1.4"
        osc = f"(1-cos(2*PI*on/{cf}))/2"
        x  = f"(iw-iw/z)*({osc})"
        y  = f"(ih-ih/z)*({osc})"
        return self._zoompan(z, x, y, tf, scale=scale)

    def tilt_wave(self, duration: float, speed: str = "normal", scale: float = 1.0) -> str:
        """Gentle diagonal sway mimicking a camera tilt."""
        cf = self._cycle_frames(speed)
        tf = self._total_frames(duration)
        z  = "1.3"
        x  = f"(iw-iw/z)/2 + (iw-iw/z)/3*sin(2*PI*on/{cf})"
        y  = f"(ih-ih/z)/2 - (ih-ih/z)/3*sin(2*PI*on/{cf})"
        return self._zoompan(z, x, y, tf, scale=scale)

    # ── new effects ───────────────────────────────────────────────────────────

    def kb_in(self, duration: float, speed: str = "normal", scale: float = 1.0) -> str:
        """Ken Burns In: smooth zoom-in/out cycle, starts wide then zooms in."""
        cf = self._cycle_frames(speed)
        tf = self._total_frames(duration)
        z  = f"1.25-0.25*cos(2*PI*on/{cf})"   # 1.0 → 1.5 → 1.0 smoothly
        x  = "(iw-iw/z)/2"
        y  = "(ih-ih/z)/2"
        return self._zoompan(z, x, y, tf, scale=scale)

    def kb_out(self, duration: float, speed: str = "normal", scale: float = 1.0) -> str:
        """Ken Burns Out: smooth zoom-out/in cycle, starts zoomed in then pulls back."""
        cf = self._cycle_frames(speed)
        tf = self._total_frames(duration)
        z  = f"1.25+0.25*cos(2*PI*on/{cf})"   # 1.5 → 1.0 → 1.5 smoothly
        x  = "(iw-iw/z)/2"
        y  = "(ih-ih/z)/2"
        return self._zoompan(z, x, y, tf, scale=scale)

    def pan_left(self, duration: float, speed: str = "normal", scale: float = 1.0) -> str:
        """Continuous horizontal oscillation, starts at right then sweeps left."""
        cf = self._cycle_frames(speed)
        tf = self._total_frames(duration)
        z  = "1.4"
        x  = f"(iw-iw/z)/2*(1+cos(2*PI*on/{cf}))"   # on=0: right, on=cf/2: left
        y  = "(ih-ih/z)/2"
        return self._zoompan(z, x, y, tf, scale=scale)

    def pan_right(self, duration: float, speed: str = "normal", scale: float = 1.0) -> str:
        """Continuous horizontal oscillation, starts at left then sweeps right."""
        cf = self._cycle_frames(speed)
        tf = self._total_frames(duration)
        z  = "1.4"
        x  = f"(iw-iw/z)/2*(1-cos(2*PI*on/{cf}))"   # on=0: left, on=cf/2: right
        y  = "(ih-ih/z)/2"
        return self._zoompan(z, x, y, tf, scale=scale)

    def pan_up(self, duration: float, speed: str = "normal", scale: float = 1.0) -> str:
        """Continuous vertical oscillation, starts at bottom then sweeps up."""
        cf = self._cycle_frames(speed)
        tf = self._total_frames(duration)
        z  = "1.4"
        x  = "(iw-iw/z)/2"
        y  = f"(ih-ih/z)/2*(1+cos(2*PI*on/{cf}))"   # on=0: bottom, on=cf/2: top
        return self._zoompan(z, x, y, tf, scale=scale)

    def pan_down(self, duration: float, speed: str = "normal", scale: float = 1.0) -> str:
        """Continuous vertical oscillation, starts at top then sweeps down."""
        cf = self._cycle_frames(speed)
        tf = self._total_frames(duration)
        z  = "1.4"
        x  = "(iw-iw/z)/2"
        y  = f"(ih-ih/z)/2*(1-cos(2*PI*on/{cf}))"   # on=0: top, on=cf/2: bottom
        return self._zoompan(z, x, y, tf, scale=scale)

    def shake(self, duration: float, speed: str = "normal", scale: float = 1.0) -> str:
        """Camera shake: handheld jitter at ~2 Hz, continuous and irregular."""
        cf = self._cycle_frames(speed)
        tf = self._total_frames(duration)
        z  = "1.15"
        # Primary oscillation at cf period; secondary at 0.77× for irregular feel.
        x  = f"(iw-iw/z)/2 + (iw-iw/z)*0.55*sin(2*PI*on/{cf})"
        y  = f"(ih-ih/z)/2 + (ih-ih/z)*0.55*cos(2*PI*on/({cf}*0.77))"
        return self._zoompan(z, x, y, tf, scale=scale)

    def wobble(self, duration: float, speed: str = "normal", scale: float = 1.0) -> str:
        """Gentle wobble: slow rhythmic sway with mild zoom pulse."""
        cf = self._cycle_frames(speed) * 2   # half the speed of normal oscillation
        tf = self._total_frames(duration)
        z  = f"1.08+0.07*sin(2*PI*on/{cf})"
        x  = f"(iw-iw/z)/2 + (iw-iw/z)*0.35*sin(2*PI*on/{cf})"
        y  = "(ih-ih/z)/2"
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
            "kb_in":          self.kb_in,
            "kb_out":         self.kb_out,
            "pan_left":       self.pan_left,
            "pan_right":      self.pan_right,
            "pan_up":         self.pan_up,
            "pan_down":       self.pan_down,
            "shake":          self.shake,
            "wobble":         self.wobble,
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
