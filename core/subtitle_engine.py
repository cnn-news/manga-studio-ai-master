import os
import re

from config import DEFAULT_VIDEO_CODEC


def _has_cjk(text: str) -> bool:
    """Return True if text contains any CJK / Japanese characters."""
    for ch in text:
        cp = ord(ch)
        if (
            0x3040 <= cp <= 0x309F   # Hiragana
            or 0x30A0 <= cp <= 0x30FF  # Katakana
            or 0x4E00 <= cp <= 0x9FFF  # CJK Unified Ideographs
            or 0xFF00 <= cp <= 0xFFEF  # Fullwidth forms
            or 0x3000 <= cp <= 0x303F  # CJK Symbols and Punctuation
        ):
            return True
    return False


def _find_japanese_font() -> str:
    """Return path to a Japanese-capable font file, or empty string if none found."""
    candidates = [
        # Windows 10/11 — high-quality Japanese fonts
        r"C:\Windows\Fonts\YuGothM.ttc",      # Yu Gothic Medium
        r"C:\Windows\Fonts\YuGothR.ttc",      # Yu Gothic Regular
        r"C:\Windows\Fonts\YuGothB.ttc",      # Yu Gothic Bold
        r"C:\Windows\Fonts\meiryo.ttc",       # Meiryo
        r"C:\Windows\Fonts\meiryob.ttc",      # Meiryo Bold
        r"C:\Windows\Fonts\msgothic.ttc",     # MS Gothic
        r"C:\Windows\Fonts\msmincho.ttc",     # MS Mincho
        # Linux — Noto CJK fonts
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/truetype/noto/NotoSansCJKjp-Regular.otf",
        "/usr/share/fonts/noto-cjk/NotoSansCJK-Regular.ttc",
        # macOS
        "/System/Library/Fonts/ヒラギノ角ゴシック W3.ttc",
        "/Library/Fonts/Osaka.ttf",
    ]
    for path in candidates:
        if os.path.isfile(path):
            return path
    return ""

# ── presets  (fontsize is in pixels at PlayResY=1080) ─────────────────────────

SUBTITLE_PRESETS = {
    "youtube_classic": {
        "fontsize": 40, "fontcolor": "white",   "bg_color": "black@0.72",
        "borderw": 0, "shadow": 0, "bold": False, "italic": False,
        "position": "bottom", "margin_v": 45, "fontname": "Arial",
        "anim": "fade",          # \fad fade-in/out
    },
    "netflix_style": {
        "fontsize": 44, "fontcolor": "white",   "bg_color": "none",
        "borderw": 3, "shadow": 1, "bold": True,  "italic": False,
        "position": "bottom", "margin_v": 45, "fontname": "Arial",
        "anim": "fade",
    },
    "minimal": {
        "fontsize": 36, "fontcolor": "#eeeeee", "bg_color": "none",
        "borderw": 1, "shadow": 0, "bold": False, "italic": False,
        "position": "bottom", "margin_v": 45, "fontname": "Arial",
        "anim": "fade_slow",     # longer, subtler fade
    },
    "social_media": {
        "fontsize": 46, "fontcolor": "#FFE500", "bg_color": "none",
        "borderw": 2, "shadow": 2, "bold": True,  "italic": False,
        "position": "bottom", "margin_v": 45, "fontname": "Arial",
        "anim": "pop",           # quick pop-in
    },
    "karaoke": {
        "fontsize": 42, "fontcolor": "#FFE500", "bg_color": "black@0.75",
        "borderw": 0, "shadow": 0, "bold": True,  "italic": False,
        "position": "bottom", "margin_v": 45, "fontname": "Arial",
        "anim": "karaoke",       # \kf word-by-word sweep
        "secondary": "white",    # unread-text color for \kf
    },
    "anime": {
        "fontsize": 52, "fontcolor": "white",   "bg_color": "none",
        "borderw": 6, "shadow": 0, "bold": True,  "italic": False,
        "position": "bottom", "margin_v": 45, "fontname": "Arial",
        "anim": "flash",         # instant appear
    },
    "cinematic": {
        "fontsize": 38, "fontcolor": "#f5f5dc", "bg_color": "black@0.45",
        "borderw": 0, "shadow": 0, "bold": False, "italic": True,
        "position": "bottom", "margin_v": 45, "fontname": "Georgia",
        "anim": "fade_slow",
    },
    "pop": {
        "fontsize": 46, "fontcolor": "#FF6B9D", "bg_color": "none",
        "borderw": 3, "shadow": 2, "bold": True,  "italic": False,
        "position": "bottom", "margin_v": 45, "fontname": "Arial",
        "anim": "pop",
    },
    "none": None,
}

# ── color helpers ─────────────────────────────────────────────────────────────

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
    """Convert 'colorname[@opacity]' or '#RRGGBB[@opacity]' → ASS &HAABBGGRR.

    ASS channel order: AA=00 fully opaque, BB GG RR.
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


def _color_transparent() -> str:
    return "&H00000000"


def _escape_filter_path(path: str) -> str:
    """Return an FFmpeg filter-safe path for the `filename='...'` option.

    On Windows the drive-letter colon must be escaped as \\: even inside
    single-quoted strings when using explicit option syntax (filename='...').
    Forward slashes are used throughout for cross-platform compatibility.

    Tested working form: ass=filename='C\\:/path/with spaces/file.ass'
    """
    p = os.path.abspath(path).replace("\\", "/")
    # Escape the Windows drive-letter colon so FFmpeg doesn't treat it as an
    # option separator inside the filter string (e.g. C:/ → C\:/)
    if len(p) >= 2 and p[1] == ":":
        p = p[0] + "\\:" + p[2:]
    return p


# ── timestamp helpers ──────────────────────────────────────────────────────────

_TS_RE = re.compile(
    r"^\d{2}:\d{2}:\d{2},\d{3}\s*-->\s*\d{2}:\d{2}:\d{2},\d{3}"
)


def _srt_ts_to_ms(ts: str) -> int:
    """'HH:MM:SS,mmm' → milliseconds."""
    ts = ts.strip().replace(",", ".")
    h, m, rest = ts.split(":")
    s, ms = rest.split(".")
    return int(h) * 3_600_000 + int(m) * 60_000 + int(s) * 1_000 + int(ms[:3])


def _ms_to_ass_time(ms: int) -> str:
    """milliseconds → ASS 'H:MM:SS.cc' (centisecond precision)."""
    cs   = ms // 10
    secs = cs // 100; cs %= 100
    mins = secs // 60; secs %= 60
    hrs  = mins // 60; mins %= 60
    return f"{hrs}:{mins:02d}:{secs:02d}.{cs:02d}"


def _seconds_to_srt(seconds: float) -> str:
    ms = int(round((seconds % 1) * 1000))
    t  = int(seconds)
    s  = t % 60; m = (t // 60) % 60; h = t // 3600
    return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"


# ── engine ────────────────────────────────────────────────────────────────────

class SubtitleEngine:

    # ── SRT validation ────────────────────────────────────────────────────

    def validate_srt(self, srt_path: str) -> dict:
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

        blocks = [b.strip() for b in re.split(r"\n\s*\n", content) if b.strip()]
        if not blocks:
            result["errors"].append("File is empty or contains no subtitle blocks")
            return result

        for n, block in enumerate(blocks, 1):
            lines = block.splitlines()
            if len(lines) < 3:
                result["errors"].append(f"Block {n}: too few lines ({len(lines)})")
                continue
            if not lines[0].strip().isdigit():
                result["errors"].append(f"Block {n}: non-numeric sequence number")
            if not _TS_RE.match(lines[1].strip()):
                result["errors"].append(f"Block {n}: invalid timestamp")
            if not "\n".join(lines[2:]).strip():
                result["errors"].append(f"Block {n}: empty text")

        result["entry_count"] = len(blocks)
        result["ok"] = len(result["errors"]) == 0 and len(blocks) > 0
        return result

    # ── SRT parser ────────────────────────────────────────────────────────

    def _parse_srt(self, srt_path: str) -> list[dict]:
        """Return list of {start_ms, end_ms, text}."""
        with open(srt_path, encoding="utf-8-sig") as fh:
            content = fh.read()

        entries = []
        blocks  = [b.strip() for b in re.split(r"\n\s*\n", content) if b.strip()]
        arrow_re = re.compile(
            r"(\d{2}:\d{2}:\d{2},\d{3})\s*-->\s*(\d{2}:\d{2}:\d{2},\d{3})"
        )
        for block in blocks:
            lines = block.splitlines()
            if len(lines) < 3:
                continue
            m = arrow_re.search(lines[1])
            if not m:
                continue
            text = " ".join(l.strip() for l in lines[2:] if l.strip())
            entries.append({
                "start_ms": _srt_ts_to_ms(m.group(1)),
                "end_ms":   _srt_ts_to_ms(m.group(2)),
                "text":     text,
            })
        return entries

    # ── ASS generator ─────────────────────────────────────────────────────

    def generate_ass(self, srt_path: str, preset: str, output_path: str,
                     margin_v_override: int | None = None) -> str:
        """Convert SRT → styled ASS file for the given preset.

        Uses PlayResX=1920, PlayResY=1080.  Font sizes in presets are in px.
        Returns the output_path written.
        """
        entries  = self._parse_srt(srt_path)
        settings = dict(SUBTITLE_PRESETS.get(preset) or SUBTITLE_PRESETS["youtube_classic"])

        fontname  = settings["fontname"]

        # Auto-detect Japanese/CJK and switch to a compatible font
        all_text = " ".join(e["text"] for e in entries)
        if _has_cjk(all_text):
            jp_font = _find_japanese_font()
            if jp_font:
                ttc_name_map = {
                    "YuGothM.ttc": "Yu Gothic",
                    "YuGothR.ttc": "Yu Gothic",
                    "YuGothB.ttc": "Yu Gothic",
                    "meiryo.ttc":  "Meiryo",
                    "meiryob.ttc": "Meiryo",
                    "msgothic.ttc": "MS Gothic",
                    "msmincho.ttc": "MS Mincho",
                }
                ttc_base = os.path.basename(jp_font)
                fontname = ttc_name_map.get(ttc_base, "Yu Gothic")

        fontsize  = settings["fontsize"]
        bold      = 1 if settings.get("bold")   else 0
        italic    = 1 if settings.get("italic")  else 0
        borderw   = settings.get("borderw", 1)
        shadow    = settings.get("shadow", 0)
        margin_v  = margin_v_override if margin_v_override is not None else settings.get("margin_v", 45)
        bg_color  = settings.get("bg_color", "none")
        anim      = settings.get("anim", "fade")

        primary_c   = _color_to_ass(settings["fontcolor"])
        # SecondaryColour: used for karaoke unread-text color
        secondary_c = _color_to_ass(settings.get("secondary", "white"))
        outline_c   = _color_to_ass("black")

        if bg_color and bg_color != "none":
            back_c       = _color_to_ass(bg_color)
            border_style = 4   # filled box
            # Outline > 0 on a filled box (BorderStyle=4):
            #   • extends the box creating natural text padding
            #   • libass renders outline corners as rounded → rounded box corners
            # Use max(existing, 10) so presets with explicit borderw keep their intent.
            borderw = max(borderw, 10)
        else:
            back_c       = "&H00000000"
            border_style = 1   # outline only

        style_line = (
            f"Style: Default,{fontname},{fontsize},"
            f"{primary_c},{secondary_c},{outline_c},{back_c},"
            f"{bold},{italic},0,0,"
            f"100,100,0,0,"
            f"{border_style},{borderw},{shadow},"
            f"2,10,10,{margin_v},1"
        )

        lines = [
            "[Script Info]",
            "ScriptType: v4.00+",
            "PlayResX: 1920",
            "PlayResY: 1080",
            "ScaledBorderAndShadow: yes",
            "WrapStyle: 0",
            "",
            "[V4+ Styles]",
            "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour,"
            " OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut,"
            " ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow,"
            " Alignment, MarginL, MarginR, MarginV, Encoding",
            style_line,
            "",
            "[Events]",
            "Format: Layer, Start, End, Style, Name,"
            " MarginL, MarginR, MarginV, Effect, Text",
        ]

        for entry in entries:
            start = _ms_to_ass_time(entry["start_ms"])
            end   = _ms_to_ass_time(entry["end_ms"])
            text  = self._apply_anim_tags(entry, anim)
            lines.append(f"Dialogue: 0,{start},{end},Default,,0,0,0,,{text}")

        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        with open(output_path, "w", encoding="utf-8-sig") as fh:
            fh.write("\n".join(lines) + "\n")

        return output_path

    def _apply_anim_tags(self, entry: dict, anim: str) -> str:
        """Prepend ASS override tags to the subtitle text based on anim type."""
        text     = entry["text"]
        dur_ms   = max(1, entry["end_ms"] - entry["start_ms"])

        if anim == "karaoke":
            # Distribute duration evenly across words with \kf (fill sweep)
            words = text.split()
            if not words:
                return text
            ms_per_word = dur_ms / len(words)
            cs_per_word = max(1, round(ms_per_word / 10))   # centiseconds
            return " ".join(f"{{\\kf{cs_per_word}}}{w}" for w in words)

        if anim == "fade":
            fi = min(300, dur_ms // 4)
            fo = min(200, dur_ms // 5)
            return f"{{\\fad({fi},{fo})}}{text}"

        if anim == "fade_slow":
            fi = min(600, dur_ms // 3)
            fo = min(400, dur_ms // 4)
            return f"{{\\fad({fi},{fo})}}{text}"

        if anim == "pop":
            # Quick fade-in, normal fade-out
            fi = min(80, dur_ms // 6)
            fo = min(150, dur_ms // 4)
            return f"{{\\fad({fi},{fo})}}{text}"

        if anim == "flash":
            # Near-instant appearance
            fi = min(30, dur_ms // 8)
            fo = min(50, dur_ms // 6)
            return f"{{\\fad({fi},{fo})}}{text}"

        # default: no tag
        return text

    # ── filter / command builders ─────────────────────────────────────────

    def burn_subtitles_command(
        self,
        input_video: str,
        srt_path: str,
        output_video: str,
        preset: str = "youtube_classic",
        custom: dict = None,
        ass_output_path: str = None,
        margin_v_override: int | None = None,
    ) -> list:
        """Return FFmpeg command that burns styled+animated subtitles.

        Generates an ASS file (at ass_output_path if given, otherwise next to
        the SRT), then uses the `ass` filter so that karaoke \\kf and fade
        \\fad tags render correctly.
        """
        if ass_output_path is None:
            ass_output_path = os.path.splitext(srt_path)[0] + f"_{preset}.ass"
        try:
            self.generate_ass(srt_path, preset, ass_output_path,
                              margin_v_override=margin_v_override)
            # Use explicit filename= option; on Windows the drive colon must be
            # escaped as \: inside single-quoted filter values.
            vf = f"ass=filename='{_escape_filter_path(ass_output_path)}'"
        except Exception:
            # Fallback: static subtitles filter
            vf = self.build_subtitle_filter(srt_path, preset, custom)

        if vf:
            video_opts = [
                "-vf", vf,
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

    def build_subtitle_filter(
        self,
        srt_path: str,
        preset: str,
        custom: dict = None,
    ) -> str:
        """Fallback: static subtitles filter with force_style (no animation)."""
        base = SUBTITLE_PRESETS.get(preset)
        if base is None:
            return ""

        settings = dict(base)
        if custom:
            settings.update(custom)

        fontsize  = settings.get("fontsize", 40)
        fontcolor = settings.get("fontcolor", "white")
        bg_color  = settings.get("bg_color", "none")
        borderw   = settings.get("borderw", 1)
        shadow    = settings.get("shadow", 0)
        fontname  = settings.get("fontname", "Arial")
        bold      = 1 if settings.get("bold")   else 0
        italic    = 1 if settings.get("italic")  else 0
        margin_v  = settings.get("margin_v", 45)

        # Detect CJK in SRT and override font if needed
        try:
            with open(srt_path, encoding="utf-8-sig") as _fh:
                _raw = _fh.read()
            if _has_cjk(_raw):
                _jp = _find_japanese_font()
                if _jp:
                    _ttc_map = {
                        "YuGothM.ttc": "Yu Gothic", "YuGothR.ttc": "Yu Gothic",
                        "YuGothB.ttc": "Yu Gothic", "meiryo.ttc": "Meiryo",
                        "meiryob.ttc": "Meiryo", "msgothic.ttc": "MS Gothic",
                        "msmincho.ttc": "MS Mincho",
                    }
                    fontname = _ttc_map.get(os.path.basename(_jp), "Yu Gothic")
        except Exception:
            pass

        primary   = _color_to_ass(fontcolor)
        outline_c = _color_to_ass("black")

        if bg_color and bg_color != "none":
            back_color   = _color_to_ass(bg_color)
            style_extra  = f",BackColour={back_color},BorderStyle=4"
        else:
            style_extra  = ",BorderStyle=1"

        force_style = (
            f"FontName={fontname},FontSize={fontsize},"
            f"Bold={bold},Italic={italic},"
            f"PrimaryColour={primary},OutlineColour={outline_c},"
            f"Outline={borderw},Shadow={shadow},"
            f"Alignment=2,MarginV={margin_v}"
            + style_extra
        )
        escaped_path = _escape_filter_path(srt_path)
        return f"subtitles=filename='{escaped_path}':force_style='{force_style}'"

    # ── misc utilities ────────────────────────────────────────────────────

    def get_preset_preview(self, preset: str) -> dict:
        if preset not in SUBTITLE_PRESETS:
            raise ValueError(f"Unknown preset '{preset}'.")
        settings = SUBTITLE_PRESETS[preset]
        return dict(settings) if settings else {}

    def create_dummy_srt(self, audio_durations: dict, output_path: str) -> str:
        entries = sorted(audio_durations.items())
        blocks:  list[str] = []
        current = 0.0
        for i, (filename, duration) in enumerate(entries, 1):
            stem  = os.path.splitext(filename)[0]
            start = _seconds_to_srt(current)
            end   = _seconds_to_srt(current + max(duration, 0.0))
            blocks.append(f"{i}\n{start} --> {end}\nSlide {stem}: edit subtitle here")
            current += duration
        content = "\n\n".join(blocks) + "\n"
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as fh:
            fh.write(content)
        return output_path