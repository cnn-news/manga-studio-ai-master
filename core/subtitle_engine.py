import os
import re

from config import DEFAULT_VIDEO_CODEC

# ── presets ───────────────────────────────────────────────────────────────────

SUBTITLE_PRESETS = {
    "youtube_classic": {
        "fontsize": 24, "fontcolor": "white", "bg_color": "black@0.5",
        "borderw": 2, "shadow": 0, "position": "bottom",
        "fontname": "Arial",
    },
    "netflix_style": {
        "fontsize": 32, "fontcolor": "white", "bg_color": "none",
        "borderw": 4, "shadow": 0, "position": "bottom",
        "fontname": "Arial Bold",
    },
    "minimal": {
        "fontsize": 20, "fontcolor": "white", "bg_color": "none",
        "borderw": 1, "shadow": 1, "position": "bottom",
        "fontname": "Arial",
    },
    "social_media": {
        "fontsize": 40, "fontcolor": "yellow", "bg_color": "none",
        "borderw": 3, "shadow": 2, "position": "center",
        "fontname": "Arial Bold",
    },
    "karaoke": {
        "fontsize": 36, "fontcolor": "yellow", "bg_color": "black@0.6",
        "borderw": 2, "shadow": 0, "position": "bottom",
        "fontname": "Arial Bold",
    },
    "anime": {
        "fontsize": 44, "fontcolor": "white", "bg_color": "none",
        "borderw": 8, "shadow": 0, "position": "bottom",
        "fontname": "Arial Black",
    },
    "cinematic": {
        "fontsize": 28, "fontcolor": "white", "bg_color": "black@0.45",
        "borderw": 0, "shadow": 0, "position": "top",
        "fontname": "Georgia",
    },
    "pop": {
        "fontsize": 38, "fontcolor": "white", "bg_color": "none",
        "borderw": 5, "shadow": 3, "position": "center",
        "fontname": "Arial Bold",
    },
    "none": None,
}

# ── color helpers ─────────────────────────────────────────────────────────────

# Named colors in RGB
_COLOR_RGB: dict[str, tuple[int, int, int]] = {
    "white":  (255, 255, 255),
    "yellow": (255, 255, 0),
    "black":  (0, 0, 0),
    "red":    (255, 0, 0),
    "blue":   (0, 0, 255),
    "green":  (0, 255, 0),
    "gray":   (128, 128, 128),
}


def _color_to_ass(color_str: str) -> str:
    """Convert 'colorname' or 'colorname@opacity' to ASS &HAABBGGRR hex.

    ASS channel order is AABBGGRR where AA=00 means fully opaque.
    opacity 1.0 = fully visible → alpha 0x00; 0.0 = invisible → alpha 0xFF.
    """
    alpha = 0
    name = color_str.strip().lower()

    if "@" in name:
        name, alpha_str = name.split("@", 1)
        try:
            opacity = float(alpha_str)
            alpha = max(0, min(255, int((1.0 - opacity) * 255)))
        except ValueError:
            pass

    r, g, b = _COLOR_RGB.get(name.strip(), (255, 255, 255))
    return f"&H{alpha:02X}{b:02X}{g:02X}{r:02X}"


def _escape_filter_path(path: str) -> str:
    """Normalize and escape a path for embedding in an FFmpeg filter string.

    On Windows, drive-letter colons (e.g. C:/) must be escaped as C\\:/.
    Spaces are safe inside single-quoted filter arguments.
    """
    p = os.path.abspath(path).replace("\\", "/")
    if len(p) > 1 and p[1] == ":":
        p = p[0] + "\\:" + p[2:]
    return p


# ── timestamp helper ──────────────────────────────────────────────────────────

_TS_RE = re.compile(
    r"^\d{2}:\d{2}:\d{2},\d{3}\s*-->\s*\d{2}:\d{2}:\d{2},\d{3}"
)


def _seconds_to_srt(seconds: float) -> str:
    """Convert a float second value to SRT timestamp HH:MM:SS,mmm."""
    ms = int(round((seconds % 1) * 1000))
    total_s = int(seconds)
    s = total_s % 60
    m = (total_s // 60) % 60
    h = total_s // 3600
    return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"


# ── engine ────────────────────────────────────────────────────────────────────

class SubtitleEngine:

    def validate_srt(self, srt_path: str) -> dict:
        """Parse an SRT file and report validity.

        Returns {"ok": bool, "entry_count": int, "errors": list[str]}.
        Checks: file readable, non-empty, each block has an integer index,
        a valid HH:MM:SS,mmm --> HH:MM:SS,mmm timestamp, and non-empty text.
        """
        result: dict = {"ok": False, "entry_count": 0, "errors": []}
        try:
            with open(srt_path, encoding="utf-8-sig") as fh:
                content = fh.read()
        except FileNotFoundError:
            result["errors"].append(f"File not found: {srt_path}")
            return result
        except Exception as exc:
            result["errors"].append(str(exc))
            return result

        # Split on blank lines into blocks
        blocks = [b.strip() for b in re.split(r"\n\s*\n", content) if b.strip()]
        if not blocks:
            result["errors"].append("File is empty or contains no subtitle blocks")
            return result

        for n, block in enumerate(blocks, 1):
            lines = block.splitlines()
            if len(lines) < 3:
                result["errors"].append(
                    f"Block {n}: expected at least 3 lines, got {len(lines)}"
                )
                continue
            if not lines[0].strip().isdigit():
                result["errors"].append(
                    f"Block {n}: first line is not a sequence number: '{lines[0].strip()}'"
                )
            if not _TS_RE.match(lines[1].strip()):
                result["errors"].append(
                    f"Block {n}: invalid timestamp line: '{lines[1].strip()}'"
                )
            if not "\n".join(lines[2:]).strip():
                result["errors"].append(f"Block {n}: subtitle text is empty")

        result["entry_count"] = len(blocks)
        result["ok"] = len(result["errors"]) == 0 and len(blocks) > 0
        return result

    def build_subtitle_filter(
        self,
        srt_path: str,
        preset: str,
        custom: dict = None,
    ) -> str:
        """Return an FFmpeg subtitles filter string with force_style applied.

        Settings are resolved in order: preset defaults → custom overrides.
        Position 'bottom' → Alignment=2, MarginV=80.
        Position 'center' → Alignment=5 (middle-center), MarginV=0.
        preset 'none' returns an empty string (no subtitles).
        """
        base = SUBTITLE_PRESETS.get(preset)
        if base is None:  # covers preset "none" and unknown keys
            return ""

        settings = dict(base)
        if custom:
            settings.update(custom)

        fontsize   = settings.get("fontsize", 24)
        fontcolor  = settings.get("fontcolor", "white")
        bg_color   = settings.get("bg_color", "none")
        borderw    = settings.get("borderw", 2)
        shadow     = settings.get("shadow", 0)
        position   = settings.get("position", "bottom")
        fontname   = settings.get("fontname", "Arial")

        primary    = _color_to_ass(fontcolor)
        outline_c  = _color_to_ass("black")

        if position == "center":
            alignment, margin_v = 5, 0
        else:
            alignment, margin_v = 2, 80

        if bg_color and bg_color != "none":
            back_color   = _color_to_ass(bg_color)
            border_style = 4   # filled box background
            style_extra  = f",BackColour={back_color},BorderStyle={border_style}"
        else:
            style_extra  = ",BorderStyle=1"  # outline + shadow, no box

        force_style = (
            f"FontName={fontname},"
            f"FontSize={fontsize},"
            f"PrimaryColour={primary},"
            f"OutlineColour={outline_c},"
            f"Outline={borderw},"
            f"Shadow={shadow},"
            f"Alignment={alignment},"
            f"MarginV={margin_v}"
            + style_extra
        )

        escaped_path = _escape_filter_path(srt_path)
        return f"subtitles='{escaped_path}':force_style='{force_style}'"

    def burn_subtitles_command(
        self,
        input_video: str,
        srt_path: str,
        output_video: str,
        preset: str = "youtube_classic",
        custom: dict = None,
    ) -> list:
        """Return an FFmpeg command that burns subtitles into a rendered video."""
        subtitle_filter = self.build_subtitle_filter(srt_path, preset, custom)

        if subtitle_filter:
            video_opts = [
                "-vf", subtitle_filter,
                "-c:v", DEFAULT_VIDEO_CODEC,
                "-preset", "fast",
                "-crf", "18",
                "-pix_fmt", "yuv420p",
            ]
        else:
            video_opts = ["-c:v", "copy"]

        return [
            "ffmpeg", "-y",
            "-i", input_video,
            *video_opts,
            "-c:a", "copy",
            "-movflags", "+faststart",
            output_video,
        ]

    def get_preset_preview(self, preset: str) -> dict:
        """Return a copy of the preset settings dict for UI display.

        Returns {} for preset 'none'.
        Raises ValueError for unknown preset names.
        """
        if preset not in SUBTITLE_PRESETS:
            raise ValueError(
                f"Unknown preset '{preset}'. "
                f"Available: {list(SUBTITLE_PRESETS.keys())}"
            )
        settings = SUBTITLE_PRESETS[preset]
        return dict(settings) if settings else {}

    def create_dummy_srt(self, audio_durations: dict, output_path: str) -> str:
        """Generate a placeholder SRT file from {filename: duration_seconds}.

        Entries are sorted by filename so the order matches the image sequence.
        Each entry contains editable placeholder text for the user.
        Returns the path to the written file.
        """
        entries = sorted(audio_durations.items())

        blocks: list[str] = []
        current = 0.0

        for i, (filename, duration) in enumerate(entries, 1):
            stem = os.path.splitext(filename)[0]
            start = _seconds_to_srt(current)
            end   = _seconds_to_srt(current + max(duration, 0.0))
            blocks.append(
                f"{i}\n"
                f"{start} --> {end}\n"
                f"Slide {stem}: edit subtitle text here"
            )
            current += duration

        content = "\n\n".join(blocks) + "\n"
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as fh:
            fh.write(content)
        return output_path
