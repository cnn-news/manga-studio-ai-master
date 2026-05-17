import os
import subprocess

from config import (
    DEFAULT_AUDIO_BITRATE,
    DEFAULT_AUDIO_CODEC,
    DEFAULT_VIDEO_CODEC,
)

# Maps our transition names to FFmpeg xfade transition identifiers
_XFADE_MAP = {
    # originals
    "fade_black":     "fadeblack",
    "fade_white":     "fadewhite",
    "cross_dissolve": "fade",
    "slide_left":     "slideleft",
    "slide_right":    "slideright",
    "zoom_transition": "zoomin",
    # new
    "slide_up":       "slideup",
    "slide_down":     "slidedown",
    "cover_left":     "coverleft",
    "cover_right":    "coverright",
    "cover_up":       "coverup",
    "cover_down":     "coverdown",
    "reveal_left":    "revealleft",
    "reveal_right":   "revealright",
    "iris_open":      "circleopen",
    "iris_close":     "circleclose",
    "radial_wipe":    "radial",
    "pixelize":       "pixelize",
    "dissolve":       "dissolve",
    "wipe_left":      "wipeleft",
    "wipe_right":     "wiperight",
    "smooth_left":    "smoothleft",
    "smooth_right":   "smoothright",
    # smart transitions
    "blur_dissolve":  "pixelize",   # pixelize ≈ blur-dissolve aesthetic
    "zoom_through":   "zoomin",     # zoom punch through
}

# Auto mode cycles through these 4 transitions between slides
_AUTO_CYCLE = ["fade_black", "cross_dissolve", "zoom_through", "blur_dissolve"]

_XFADE_THRESHOLD = 20  # segments above this use concat demuxer


class TransitionEngine:

    TRANSITIONS = {
        # originals
        "fade_black":     "Fade to Black",
        "fade_white":     "Fade to White",
        "cross_dissolve": "Cross Dissolve",
        "slide_left":     "Slide Left",
        "slide_right":    "Slide Right",
        "zoom_transition": "Zoom In",
        # new
        "slide_up":       "Slide Up",
        "slide_down":     "Slide Down",
        "cover_left":     "Cover Left",
        "cover_right":    "Cover Right",
        "cover_up":       "Cover Up",
        "cover_down":     "Cover Down",
        "reveal_left":    "Reveal Left",
        "reveal_right":   "Reveal Right",
        "iris_open":      "Iris Open",
        "iris_close":     "Iris Close",
        "radial_wipe":    "Radial Wipe",
        "pixelize":       "Pixelize",
        "dissolve":       "Dissolve",
        "wipe_left":      "Wipe Left",
        "wipe_right":     "Wipe Right",
        "smooth_left":    "Smooth Left",
        "smooth_right":   "Smooth Right",
        # smart transitions
        "blur_dissolve":  "Blur Dissolve",
        "zoom_through":   "Zoom Through",
        "auto":           "Tự Động (4 kiểu)",
    }

    def get_xfade_filter(
        self, transition: str, duration: float, offset: float
    ) -> str:
        """Return the bare xfade filter string (no labels) for embedding in a
        custom filter_complex.

        offset = start of transition in seconds from the beginning of the combined
                 output stream produced by the preceding xfade (or segment 0).
        """
        name = _XFADE_MAP.get(transition, "fadeblack")
        return f"xfade=transition={name}:duration={duration}:offset={offset}"

    # ── xfade-based concat ────────────────────────────────────────────────

    def build_concat_command(
        self,
        segment_files: list[str],
        output_path: str,
        transition: str = "fade_black",
        transition_duration: float = 0.5,
        video_only: bool = False,
    ) -> list:
        """Build an FFmpeg command that joins all segments with xfade transitions.

        video_only=True is used when segments have no audio track (single-audio
        mode). In that case only the video xfade filter is applied; audio is
        injected separately by the caller after merging.

        For each pair of adjacent segments the offset is calculated as:
            offset_i = sum(D_0..D_i) - (i+1) * transition_duration
        so the transition begins exactly transition_duration seconds before the
        end of the current accumulated output.
        """
        n = len(segment_files)

        cmd = ["ffmpeg", "-y"]
        for f in segment_files:
            cmd += ["-i", f]

        if video_only:
            encode_opts = [
                "-c:v", DEFAULT_VIDEO_CODEC,
                "-preset", "fast",
                "-crf", "18",
                "-bf", "0",
                "-an",
                "-pix_fmt", "yuv420p",
                "-movflags", "+faststart",
                output_path,
            ]
        else:
            encode_opts = [
                "-c:v", DEFAULT_VIDEO_CODEC,
                "-preset", "fast",
                "-crf", "18",
                "-bf", "0",
                "-c:a", DEFAULT_AUDIO_CODEC,
                "-b:a", DEFAULT_AUDIO_BITRATE,
                "-pix_fmt", "yuv420p",
                "-movflags", "+faststart",
                output_path,
            ]

        if n == 1:
            maps = ["-map", "0:v"] + ([] if video_only else ["-map", "0:a"])
            cmd += maps + encode_opts
            return cmd

        # "auto" mode: cycle through 4 transitions, one per slide boundary
        is_auto = (transition == "auto")
        default_xfade = _XFADE_MAP.get(transition, "fadeblack")
        durations = [_probe_duration(f) for f in segment_files]

        vf: list[str] = []
        af: list[str] = []
        cumulative = 0.0

        for i in range(n - 1):
            cumulative += durations[i]
            offset = max(0.0, round(cumulative - (i + 1) * transition_duration, 3))

            # Pick transition name: cycle through 4 if auto mode
            if is_auto:
                t_name = _AUTO_CYCLE[i % len(_AUTO_CYCLE)]
                xfade_name = _XFADE_MAP.get(t_name, "fadeblack")
            else:
                xfade_name = default_xfade

            in_v  = f"[xv{i - 1}]" if i > 0 else f"[{i}:v]"
            out_v = "[vout]" if i == n - 2 else f"[xv{i}]"

            vf.append(
                f"{in_v}[{i + 1}:v]"
                f"xfade=transition={xfade_name}:duration={transition_duration}:offset={offset}"
                f"{out_v}"
            )

            if not video_only:
                in_a  = f"[xa{i - 1}]" if i > 0 else f"[{i}:a]"
                out_a = "[aout]" if i == n - 2 else f"[xa{i}]"
                af.append(
                    f"{in_a}[{i + 1}:a]"
                    f"acrossfade=d={transition_duration}:c1=tri:c2=tri"
                    f"{out_a}"
                )

        filter_complex = ";".join(vf + af)
        cmd += ["-filter_complex", filter_complex, "-map", "[vout]"]
        if not video_only:
            cmd += ["-map", "[aout]"]
        cmd += encode_opts
        return cmd

    # ── concat-demuxer-based concat ───────────────────────────────────────

    def build_concat_file(
        self, segment_files: list[str], concat_file_path: str
    ) -> str:
        """Write an FFmpeg concat demuxer list file and return its path.

        File paths are stored as absolute to make the list portable regardless
        of the working directory when FFmpeg is invoked.
        """
        lines: list[str] = []
        for f in segment_files:
            # Forward slashes work on all platforms inside FFmpeg.
            abs_path = os.path.abspath(f).replace("\\", "/")
            # Escape any single quotes that appear in the path itself.
            escaped = abs_path.replace("'", "'\\''")
            lines.append(f"file '{escaped}'")

        content = "\n".join(lines) + "\n"
        os.makedirs(os.path.dirname(os.path.abspath(concat_file_path)), exist_ok=True)
        with open(concat_file_path, "w", encoding="utf-8") as fh:
            fh.write(content)
        return concat_file_path

    def concat_with_file(self, concat_file_path: str, output_path: str) -> list:
        """Build an FFmpeg command that uses the concat demuxer (stream copy, no
        re-encode).  Suitable for > 20 segments where the xfade filter_complex
        would become unwieldy.
        """
        return [
            "ffmpeg", "-y",
            "-f", "concat",
            "-safe", "0",
            "-i", concat_file_path,
            "-c", "copy",
            "-movflags", "+faststart",
            output_path,
        ]

    # ── strategy selector ─────────────────────────────────────────────────

    def choose_method(self, segment_count: int) -> str:
        """Return 'xfade' for <= 20 segments, 'concat_file' for larger batches."""
        return "xfade" if segment_count <= _XFADE_THRESHOLD else "concat_file"


# ── module-level helper ───────────────────────────────────────────────────────

def _probe_duration(file_path: str) -> float:
    """Return media file duration in seconds via ffprobe. Falls back to 5.0."""
    try:
        proc = subprocess.run(
            [
                "ffprobe", "-v", "quiet",
                "-show_entries", "format=duration",
                "-of", "csv=p=0",
                file_path,
            ],
            capture_output=True, text=True, timeout=10,
        )
        return float(proc.stdout.strip())
    except Exception:
        return 5.0
