import os
import re

from config import DEFAULT_VIDEO_CODEC

# ── presets ───────────────────────────────────────────────────────────────────

# fontsize calibrated for FFmpeg SRT→ASS default PlayResY=288
# (scale: fontsize / 288 * video_height = rendered pixel height)
# Target ~50-65px at 1080p → fontsize ≈ 13–18
SUBTITLE_PRESETS = {
    "youtube_classic": {
        "fontsize": 14, "fontcolor": "white",   "bg_color": "black@0.72",
        "borderw": 0, "shadow": 0, "bold": False, "italic": False,
        "position": "bottom", "margin_v": 20, "fontname": "Arial",
    },
    "netflix_style": {
        "fontsize": 16, "fontcolor": "white",   "bg_color": "none",
        "borderw": 3, "shadow": 1, "bold": True,  "italic": False,
        "position": "bottom", "margin_v": 20, "fontname": "Arial",
    },
    "minimal": {
        "fontsize": 13, "fontcolor": "#eeeeee", "bg_color": "none",
        "borderw": 1, "shadow": 0, "bold": False, "italic": False,
        "position": "bottom", "margin_v": 20, "fontname": "Arial",
    },
    "social_media": {
        "fontsize": 17, "fontcolor": "#FFE500", "bg_color": "none",
        "borderw": 2, "shadow": 2, "bold": True,  "italic": False,
        "position": "bottom", "margin_v": 20, "fontname": "Arial",
    },
    "karaoke": {
        "fontsize": 16, "fontcolor": "#FFE500", "bg_color": "black@0.75",
        "borderw": 0, "shadow": 0, "bold": True,  "italic": False,
        "position": "bottom", "margin_v": 20, "fontname": "Arial",
    },
    "anime": {
        "fontsize": 18, "fontcolor": "white",   "bg_color": "none",
        "borderw": 6, "shadow": 0, "bold": True,  "italic": False,
        "position": "bottom", "margin_v": 20, "fontname": "Arial",
    },
    "cinematic": {
        "fontsize": 13, "fontcolor": "#f5f5dc", "bg_color": "black@0.45",
        "borderw": 0, "shadow": 0, "bold": False, "italic": True,
        "position": "bottom", "margin_v": 20, "fontname": "Georgia",
    },
    "pop": {
        "fontsize": 17, "fontcolor": "#FF6B9D", "bg_color": "none",
        "borderw": 3, "shadow": 2, "bold": True,  "italic": False,
        "position": "bottom", "margin_v": 20, "fontname": "Arial",
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
    """Convert color string to ASS &HAABBGGRR hex.

    Accepts:
      - Named colors: 'white', 'yellow', etc.
      - Hex colors:   '#RRGGBB'
      - With opacity: 'white@0.7' or '#FF6B9D@0.8'
    ASS channel order is AABBGGRR; AA=00 = fully opaque.
    """
    alpha = 0
    raw = color_str.strip()

    if "@" in raw:
        raw, alpha_str = raw.rsplit("@", 1)
        try:
            opacity = float(alpha_str)
            alpha = max(0, min(255, int((1.0 - opacity) * 255)))
        except ValueError:
            pass

    raw = raw.strip()

    # Hex color #RRGGBB
    if raw.startswith("#") and len(raw) == 7:
        try:
            r = int(raw[1:3], 16)
            g = int(raw[3:5], 16)
            b = int(raw[5:7], 16)
            return f"&H{alpha:02X}{b:02X}{g:02X}{r:02X}"
        except ValueError:
            pass

    r, g, b = _COLOR_RGB.get(raw.lower(), (255, 255, 255))
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

        All presets default to Alignment=2 (center-bottom).
        MarginV is in ASS units relative to PlayResY=288 (FFmpeg SRT default).
        At 1080p: MarginV=20 ≈ 75px from bottom edge.
        """
        base = SUBTITLE_PRESETS.get(preset)
        if base is None:
            return ""

        settings = dict(base)
        if custom:
            settings.update(custom)

        fontsize  = settings.get("fontsize", 14)
        fontcolor = settings.get("fontcolor", "white")
        bg_color  = settings.get("bg_color", "none")
        borderw   = settings.get("borderw", 1)
        shadow    = settings.get("shadow", 0)
        position  = settings.get("position", "bottom")
        fontname  = settings.get("fontname", "Arial")
        bold      = 1 if settings.get("bold", False) else 0
        italic    = 1 if settings.get("italic", False) else 0
        margin_v  = settings.get("margin_v", 20)

        primary   = _color_to_ass(fontcolor)
        outline_c = _color_to_ass("black")

        # Alignment: 2 = center-bottom (default), 5 = center-middle, 8 = center-top
        if position == "top":
            alignment, mv = 8, margin_v
        else:
            # All other positions → center-bottom
            alignment, mv = 2, margin_v

        if bg_color and bg_color != "none":
            back_color   = _color_to_ass(bg_color)
            border_style = 4          # opaque box behind text
            style_extra  = f",BackColour={back_color},BorderStyle={border_style}"
        else:
            border_style = 1          # outline-only
            style_extra  = f",BorderStyle={border_style}"

        force_style = (
            f"FontName={fontname},"
            f"FontSize={fontsize},"
            f"Bold={bold},"
            f"Italic={italic},"
            f"PrimaryColour={primary},"
            f"OutlineColour={outline_c},"
            f"Outline={borderw},"
            f"Shadow={shadow},"
            f"Alignment={alignment},"
            f"MarginV={mv}"
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
