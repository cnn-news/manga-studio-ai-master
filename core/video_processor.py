import os
import psutil
import random
import shutil
import subprocess
import tempfile
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime

from config import (
    DEFAULT_AUDIO_BITRATE,
    DEFAULT_AUDIO_CODEC,
    DEFAULT_FPS,
    DEFAULT_RESOLUTION,
    DEFAULT_VIDEO_BITRATE,
    DEFAULT_VIDEO_CODEC,
    SUPPORTED_AUDIO_FORMATS,
    SUPPORTED_IMAGE_FORMATS,
)
from core.audio_processor import AudioProcessor
from core.effect_engine import EffectEngine
from core.logger import RenderLogger
from core.subtitle_engine import SubtitleEngine
from core.transition_engine import TransitionEngine
from core.validator import SystemValidator
from core.waveform_engine import WaveformEngine

# ── constants ─────────────────────────────────────────────────────────────────

# quality_preset → x264/x265 preset name
_QUALITY_PRESET = {"fast": "ultrafast", "balanced": "medium", "quality": "slow"}

# quality_preset → CRF value (lower = better quality, larger file)
_QUALITY_CRF = {"fast": 28, "balanced": 23, "quality": 18}

_WATERMARK_POS = {
    "top_left":     "10:10",
    "top_right":    "W-w-10:10",
    "bottom_left":  "10:H-h-10",
    "bottom_right": "W-w-10:H-h-10",
}

# hw_encoder value → preferred video codec for that accelerator
_HW_CODEC = {
    "nvenc":        "h264_nvenc",
    "videotoolbox": "h264_videotoolbox",
    "vaapi":        "h264_vaapi",
}

# Minimum free disk space required before each segment encode (MB)
_MIN_DISK_MB = 500

# Audio clips shorter than this (seconds) are skipped with a warning
_MIN_AUDIO_DURATION = 1.0


# ── dataclasses ───────────────────────────────────────────────────────────────

@dataclass
class RenderConfig:
    """All settings for a single render pipeline run."""
    image_folder:        str
    output_folder:       str
    audio_folder:        str   = ""        # folder with one audio file per image (stem-matched)
    single_audio_file:   str | None = None # OR one audio file for the entire slideshow
    project_name:        str   = "output"
    resolution:          str   = DEFAULT_RESOLUTION
    fps:                 int   = DEFAULT_FPS
    video_codec:         str   = DEFAULT_VIDEO_CODEC
    audio_codec:         str   = DEFAULT_AUDIO_CODEC
    video_bitrate:       str   = DEFAULT_VIDEO_BITRATE
    audio_bitrate:       str   = DEFAULT_AUDIO_BITRATE
    quality_preset:      str   = "balanced"      # fast / balanced / quality
    effect_mode:         str   = "random"        # random / sequential / fixed
    fixed_effect:        str   = "zoom_pulse"
    effect_speed:        str   = "normal"
    transition:          str   = "fade_black"
    transition_duration: float = 0.5
    subtitle_preset:     str   = "none"
    subtitle_srt_path:   str | None = None
    normalize_audio:     bool  = True
    audio_fade:          float = 0.3
    bgm_path:            str | None = None
    bgm_volume:          float = 0.15
    bgm_ducking:         bool  = True   # auto-duck BGM when voice is detected
    watermark_text:       str        = "Manhwa Recap Hub"
    watermark_path:       str | None = None
    watermark_position:   str        = "bottom_right"
    watermark_opacity:    float      = 0.9   # text / image opacity
    watermark_scale:      float      = 0.15
    watermark_color:      str        = "#ff6b9d"  # text color (#RRGGBB or named)
    watermark_bg_color:   str        = "#000000"  # box background color
    watermark_bg_opacity: float      = 0.7   # box background opacity (0 = none)
    intro_path:          str | None = None
    outro_path:          str | None = None
    image_scale:         float = 0.8          # 0.1–1.0; <1.0 shows blur background border
    render_parts:        list = field(default_factory=list)  # [{image_folder, audio_file}, ...]
    max_workers:         int | None = None   # None → auto (cpu_count // 2)
    scroll_mode:         bool = False        # stack images vertically, pan top→bottom
    # ── waveform overlay ─────────────────────────────────────────────────
    waveform_enabled:     bool  = False
    waveform_height:      int   = 52      # bar area height in pixels
    waveform_width_ratio: float = 0.30    # bar area width as fraction of video width
    render_progress_bar:  bool  = False   # burn time-progress bar into video


@dataclass
class RenderProgress:
    """Mutable state object passed to the progress callback each update."""
    total_segments:     int   = 0
    completed_segments: int   = 0
    current_phase:      str   = ""   # preparing / rendering / merging / finalizing
    phase_progress:     float = 0.0
    overall_progress:   float = 0.0
    elapsed_seconds:    float = 0.0
    logs:               list  = field(default_factory=list)
    status:             str   = "idle"


# ── helpers ───────────────────────────────────────────────────────────────────

def _probe_duration(path: str) -> float:
    try:
        proc = subprocess.run(
            ["ffprobe", "-v", "quiet", "-show_entries", "format=duration",
             "-of", "csv=p=0", path],
            capture_output=True, text=True, timeout=10,
        )
        return float(proc.stdout.strip())
    except Exception:
        return 0.0


def _parse_bitrate_mbps(bitrate_str: str) -> float:
    s = bitrate_str.strip().upper()
    if s.endswith("M"):
        return float(s[:-1])
    if s.endswith("K"):
        return float(s[:-1]) / 1000
    return float(s) / 1_000_000


def _free_mb(path: str) -> float:
    """Return free disk space in MB for the drive containing path."""
    try:
        check = path
        while check and not os.path.exists(check):
            check = os.path.dirname(check)
        return psutil.disk_usage(check or os.getcwd()).free / (1024 * 1024)
    except Exception:
        return float("inf")


def _sanitize_filename(name: str) -> str:
    """Strip characters that are invalid in Windows filenames: \\ / : * ? \" < > |"""
    import re
    return re.sub(r'[\\/:*?"<>|]', "", name).strip()


# ── main class ────────────────────────────────────────────────────────────────

class VideoProcessor:

    def __init__(
        self,
        config: RenderConfig,
        progress_callback=None,
        log_callback=None,
    ):
        self.config            = config
        self.progress_callback = progress_callback
        self.log_callback      = log_callback

        self.progress   = RenderProgress()
        self._start_time: float = 0.0
        self.temp_dir:  str = ""

        # Sub-engines
        self.effect_engine     = EffectEngine()
        self.transition_engine = TransitionEngine()
        self.audio_processor   = AudioProcessor()
        self.subtitle_engine   = SubtitleEngine()
        self.waveform_engine   = WaveformEngine()

        # File logger (started lazily in run())
        self._logger = RenderLogger()

        # Hardware encoder detected once and cached
        self._hw_encoder: str = ""

        # Threading controls
        self._cancel_event  = threading.Event()
        self._resume_event  = threading.Event()
        self._resume_event.set()
        self._progress_lock = threading.Lock()

    # ── hardware detection ────────────────────────────────────────────────

    def _detect_hw(self) -> str:
        """Return the detected hardware encoder string (cached after first call)."""
        if not self._hw_encoder:
            result = SystemValidator().check_ffmpeg()
            self._hw_encoder = result.get("hw_encoder", "cpu")
        return self._hw_encoder

    # ── encode option builders ────────────────────────────────────────────

    def _sw_encode_opts(self, copy_audio: bool = False) -> list:
        """Software encode options used for segment rendering (filter_complex safe)."""
        preset = _QUALITY_PRESET.get(self.config.quality_preset, "medium")
        crf    = _QUALITY_CRF.get(self.config.quality_preset, 23)
        audio  = (["-c:a", "copy"] if copy_audio
                  else ["-c:a", self.config.audio_codec, "-b:a", self.config.audio_bitrate])
        return [
            "-c:v", self.config.video_codec,
            "-preset", preset,
            "-crf", str(crf),
            *audio,
            "-pix_fmt", "yuv420p",
            "-movflags", "+faststart",
        ]

    def _final_encode_opts(self) -> list:
        """Software encode options for the finalize pass.

        FFmpeg 8.x NVENC in vbr/cq mode auto-inserts a pixel-format conversion
        filter (vf#0:0) that fails with -40 (Function not implemented) when the
        input comes from a CPU-encoded concat.  Since the finalize pass is a
        single sequential encode (not the bottleneck), libx264 is simpler,
        universally compatible, and produces identical quality to the segments.
        VideoToolbox (macOS) is still used as it works correctly without filter
        insertion.
        """
        hw     = self._detect_hw()
        audio  = ["-c:a", self.config.audio_codec, "-b:a", self.config.audio_bitrate]
        common = ["-pix_fmt", "yuv420p", "-movflags", "+faststart"]

        if hw == "videotoolbox":
            self._log("Finalize: using Apple VideoToolbox encoder")
            return [
                "-c:v", "h264_videotoolbox",
                "-q:v", "65",
                "-b:v", self.config.video_bitrate,
                *audio, *common,
            ]

        # Software (libx264) for all other cases — NVENC, VAAPI, CPU
        preset = _QUALITY_PRESET.get(self.config.quality_preset, "medium")
        crf    = _QUALITY_CRF.get(self.config.quality_preset, 23)
        self._log(f"Finalize: using libx264 (software)")
        return [
            "-c:v", self.config.video_codec,
            "-preset", preset,
            "-crf", str(crf),
            "-b:v", self.config.video_bitrate,
            *audio, *common,
        ]

    # Previously named _ffmpeg_encode_opts — kept for watermark/subtitle phases
    def _ffmpeg_encode_opts(self, copy_audio: bool = False) -> list:
        return self._sw_encode_opts(copy_audio)

    # ── internal utilities ────────────────────────────────────────────────

    def _log(self, message: str, level: str = "info") -> None:
        ts    = datetime.now().strftime("%H:%M:%S")
        entry = {"time": ts, "message": message, "level": level}
        self.progress.logs.append(entry)
        if self.log_callback:
            self.log_callback(message, level)
        self._logger.write(message, level)

    def _notify_progress(self) -> None:
        with self._progress_lock:
            self.progress.elapsed_seconds = round(time.time() - self._start_time, 1)
        if self.progress_callback:
            self.progress_callback(self.progress)

    def _run_ffmpeg(self, cmd: list, timeout: int = 600) -> dict:
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
            if proc.returncode == 0:
                return {"ok": True, "output": proc.stderr}
            return {"ok": False, "error": proc.stderr[-800:], "returncode": proc.returncode}
        except subprocess.TimeoutExpired:
            return {"ok": False, "error": f"FFmpeg timed out after {timeout}s"}
        except FileNotFoundError:
            return {"ok": False, "error": "ffmpeg not found in PATH"}
        except Exception as exc:
            return {"ok": False, "error": str(exc)}

    def _build_effect_list(self, count: int) -> list[str]:
        mode = self.config.effect_mode
        if mode == "fixed":
            return [self.config.fixed_effect] * count
        if mode == "sequential":
            effects = self.effect_engine.EFFECTS
            return [effects[i % len(effects)] for i in range(count)]
        if mode == "smart_cycle":
            # Cycle through 4 smart Ken Burns patterns (ease-in-out, no oscillation)
            cycle = self.effect_engine.SMART_CYCLE
            return [cycle[i % len(cycle)] for i in range(count)]
        return [random.choice(self.effect_engine.EFFECTS) for _ in range(count)]

    # ── Phase 0: prepare ─────────────────────────────────────────────────

    def prepare(self) -> dict:
        if self.config.single_audio_file:
            return self._prepare_single_audio()
        return self._prepare_folder_audio()

    def _prepare_folder_audio(self) -> dict:
        """Original prepare path: one audio file per image, matched by stem."""
        self._log("Phase 0: Validating inputs…")
        val = SystemValidator().run_all(
            self.config.image_folder,
            self.config.audio_folder,
            self.config.output_folder,
        )
        if not val["passed"]:
            failed = {k: v for k, v in val.items() if isinstance(v, dict) and not v.get("ok")}
            return {"ok": False, "error": f"Validation failed: {failed}"}

        img_map = {
            os.path.splitext(f)[0]: f
            for f in sorted(os.listdir(self.config.image_folder))
            if os.path.splitext(f)[1].lower() in SUPPORTED_IMAGE_FORMATS
        }
        aud_map = {
            os.path.splitext(f)[0]: f
            for f in sorted(os.listdir(self.config.audio_folder))
            if os.path.splitext(f)[1].lower() in SUPPORTED_AUDIO_FORMATS
        }
        stems = sorted(img_map.keys() & aud_map.keys())
        if not stems:
            return {"ok": False, "error": "No matching image-audio pairs found"}

        self._log(f"Found {len(stems)} matched pairs")

        audio_files = [aud_map[s] for s in stems]
        durations   = self.audio_processor.get_batch_durations(self.config.audio_folder, audio_files)
        total_dur   = self.audio_processor.estimate_total_duration(durations)

        self.temp_dir = os.path.join(self.config.output_folder, f"_temp_{uuid.uuid4().hex[:8]}")
        os.makedirs(self.temp_dir, exist_ok=True)
        os.makedirs(self.config.output_folder, exist_ok=True)

        effect_list = self._build_effect_list(len(stems))
        segments = [
            {
                "index":    i,
                "image":    os.path.join(self.config.image_folder, img_map[s]),
                "audio":    os.path.join(self.config.audio_folder, aud_map[s]),
                "output":   os.path.join(self.temp_dir, f"seg_{i:03d}.mp4"),
                "effect":   effect_list[i],
                "duration": durations.get(aud_map[s], 0.0),
            }
            for i, s in enumerate(stems)
        ]

        bitrate_mbps  = _parse_bitrate_mbps(self.config.video_bitrate)
        estimated_mb  = round((bitrate_mbps * total_dur) / 8 * 1.1, 2)

        with self._progress_lock:
            self.progress.total_segments  = len(segments)
            self.progress.overall_progress = 0.05

        self._log(f"Prepared {len(segments)} segments, total={total_dur:.1f}s, est={estimated_mb}MB")
        self._notify_progress()

        return {
            "ok":              True,
            "segments":        segments,
            "segment_count":   len(segments),
            "total_duration":  round(total_dur, 2),
            "estimated_size_mb": estimated_mb,
            "durations":       durations,
            "single_audio_mode": False,
        }

    def _prepare_single_audio(self) -> dict:
        """Single audio file mode — all images share one audio track, divided equally."""
        self._log("Phase 0: Single-audio mode — validating inputs…")

        val_ff = SystemValidator().check_ffmpeg()
        if not val_ff["ok"]:
            return {"ok": False, "error": f"FFmpeg: {val_ff['error']}"}

        # Single-audio mode: accept any image filenames (no numbered naming required)
        val_img = SystemValidator().check_folder_images_any(self.config.image_folder)
        if not val_img["ok"]:
            errs = val_img.get("errors") or ["unknown"]
            return {"ok": False, "error": f"Images: {errs[0]}"}

        audio_path = self.config.single_audio_file
        if not audio_path or not os.path.isfile(audio_path):
            return {"ok": False, "error": f"Audio file not found: {audio_path}"}

        ext = os.path.splitext(audio_path)[1].lower()
        if ext not in SUPPORTED_AUDIO_FORMATS:
            return {"ok": False, "error": f"Unsupported audio format: {ext}"}

        try:
            audio_duration = self.audio_processor.get_audio_duration(audio_path)
        except Exception as exc:
            return {"ok": False, "error": f"Cannot probe audio duration: {exc}"}

        if audio_duration <= 0:
            return {"ok": False, "error": "Audio duration is 0 or invalid"}

        images = sorted(
            f for f in os.listdir(self.config.image_folder)
            if os.path.splitext(f)[1].lower() in SUPPORTED_IMAGE_FORMATS
        )
        N = len(images)
        if N == 0:
            return {"ok": False, "error": "No images found in image folder"}

        # Divide audio duration equally across all images.
        # concat demuxer is used (no transitions) so total = N * per_seg = audio_duration exactly.
        per_seg_duration = audio_duration / N

        self.temp_dir = os.path.join(
            self.config.output_folder, f"_temp_{uuid.uuid4().hex[:8]}"
        )
        os.makedirs(self.temp_dir, exist_ok=True)
        os.makedirs(self.config.output_folder, exist_ok=True)

        effect_list = self._build_effect_list(N)
        segments = [
            {
                "index":    i,
                "image":    os.path.join(self.config.image_folder, img),
                "audio":    None,           # no per-segment audio
                "output":   os.path.join(self.temp_dir, f"seg_{i:03d}.mp4"),
                "effect":   effect_list[i],
                "duration": per_seg_duration,
            }
            for i, img in enumerate(images)
        ]

        bitrate_mbps = _parse_bitrate_mbps(self.config.video_bitrate)
        estimated_mb = round((bitrate_mbps * audio_duration) / 8 * 1.1, 2)

        with self._progress_lock:
            self.progress.total_segments  = N
            self.progress.overall_progress = 0.05

        self._log(
            f"Single-audio: {N} images, audio={audio_duration:.3f}s, "
            f"per_seg={per_seg_duration:.6f}s, est={estimated_mb}MB"
        )
        self._notify_progress()

        return {
            "ok":              True,
            "segments":        segments,
            "segment_count":   N,
            "total_duration":  round(audio_duration, 2),
            "estimated_size_mb": estimated_mb,
            "durations":       {os.path.basename(audio_path): audio_duration},
            "single_audio_mode": True,
        }

    # ── Phase 1a: single segment ─────────────────────────────────────────

    def render_segment_image_only(
        self,
        segment_index: int,
        image_path: str,
        duration: float,
        output_path: str,
        effect_name: str,
    ) -> dict:
        """Render an image-only segment (no audio) for single-audio mode.
        Duration is enforced exactly via -t; the caller guarantees
        sum(durations) == audio_duration so the final mux is sample-accurate.
        """
        video_filter = self.effect_engine.get_effect(
            effect_name, duration, self.config.effect_speed, self.config.image_scale
        )
        preset = _QUALITY_PRESET.get(self.config.quality_preset, "medium")
        crf    = _QUALITY_CRF.get(self.config.quality_preset, 23)

        cmd = [
            "ffmpeg", "-y",
            "-threads", "1",
            "-loop", "1",
            "-r", "1",
            "-i", image_path,
            "-filter_complex", video_filter,
            "-map", "[out]",
            "-c:v", self.config.video_codec,
            "-preset", preset,
            "-crf", str(crf),
            "-bf", "0",          # disable B-frames → monotonic PTS → smooth xfade
            "-pix_fmt", "yuv420p",
            "-t", f"{duration:.6f}",
            "-an",
            "-movflags", "+faststart",
            output_path,
        ]

        result = self._run_ffmpeg(cmd, timeout=300)
        if result["ok"]:
            return {"ok": True, "path": output_path, "duration": duration, "error": ""}
        return {"ok": False, "path": "", "duration": 0.0, "error": result["error"]}

    def render_segment(
        self,
        segment_index: int,
        image_path: str,
        audio_path: str,
        output_path: str,
        effect_name: str,
    ) -> dict:
        # Probe audio duration
        try:
            duration = self.audio_processor.get_audio_duration(audio_path)
        except Exception as exc:
            return {"ok": False, "path": "", "duration": 0.0, "error": str(exc)}

        # Guard: skip audio clips that are too short for a meaningful video
        if duration < _MIN_AUDIO_DURATION:
            self._log(
                f"Segment {segment_index}: audio too short ({duration:.2f}s < {_MIN_AUDIO_DURATION}s) — skipping",
                "warning",
            )
            return {
                "ok":      False,
                "path":    "",
                "duration": duration,
                "error":   "audio_too_short",
                "skipped": True,
            }

        video_filter = self.effect_engine.get_effect(
            effect_name, duration, self.config.effect_speed, self.config.image_scale
        )

        # Audio processing chain
        af_parts: list[str] = []
        if self.config.normalize_audio:
            af_parts.append(self.audio_processor.normalize_audio_filter())
        if self.config.audio_fade > 0:
            af_parts.append(
                self.audio_processor.fade_filter(duration, self.config.audio_fade, self.config.audio_fade)
            )

        if af_parts:
            audio_chain = ",".join(af_parts)
            full_filter = f"{video_filter};[1:a]{audio_chain}[aout]"
            audio_map   = "[aout]"
        else:
            full_filter = video_filter
            audio_map   = "1:a"

        preset = _QUALITY_PRESET.get(self.config.quality_preset, "medium")
        crf    = _QUALITY_CRF.get(self.config.quality_preset, 23)

        cmd = [
            "ffmpeg", "-y",
            "-threads", "1",          # 1 thread per process; parallelism via workers
            "-loop", "1",
            "-r", "1",                # 1 fps input so zoompan gets d frames per image
            "-i", image_path,
            "-i", audio_path,
            "-filter_complex", full_filter,
            "-map", "[out]",
            "-map", audio_map,
            "-c:v", self.config.video_codec,
            "-preset", preset,
            "-crf", str(crf),
            "-bf", "0",               # disable B-frames → monotonic PTS → smooth xfade
            "-c:a", self.config.audio_codec,
            "-b:a", self.config.audio_bitrate,
            "-pix_fmt", "yuv420p",
            "-shortest",
            "-movflags", "+faststart",
            output_path,
        ]

        result = self._run_ffmpeg(cmd, timeout=300)
        if result["ok"]:
            return {"ok": True, "path": output_path, "duration": duration, "error": ""}
        return {"ok": False, "path": "", "duration": 0.0, "error": result["error"]}

    # ── Phase 1b: all segments in parallel ───────────────────────────────

    def render_all_segments_parallel(self, segments: list) -> list:
        total   = len(segments)
        results: list = [None] * total

        cpu = os.cpu_count() or 2
        # Cap workers: each FFmpeg uses 1 thread; running more than cpu_count
        # processes only adds overhead (context-switch contention).
        # Hard cap at 8 to keep memory in check on typical workstations.
        workers = self.config.max_workers or min(total, cpu, 8)
        workers = max(1, workers)
        self._log(f"Rendering {total} segments with {workers} parallel worker(s)")

        def render_one(seg: dict) -> dict:
            self._resume_event.wait()
            if self._cancel_event.is_set():
                return {"ok": False, "path": "", "duration": 0.0, "error": "cancelled"}

            idx = seg["index"]

            # Disk space guard before each FFmpeg call
            free_mb = _free_mb(self.config.output_folder)
            if free_mb < _MIN_DISK_MB:
                self._log(
                    f"Low disk space: {free_mb:.0f} MB free (need {_MIN_DISK_MB} MB) — pausing",
                    "warning",
                )
                self.pause()
                # Unblock eventually via resume(); notify caller through log_callback
                self._resume_event.wait()
                if self._cancel_event.is_set():
                    return {"ok": False, "path": "", "duration": 0.0, "error": "cancelled"}

            self._log(f"  [{idx+1}/{total}] {os.path.basename(seg['image'])} effect={seg['effect']}")

            # Dispatch to the appropriate render method
            if seg.get("audio") is None:
                # Single-audio mode: image-only segment
                r = self.render_segment_image_only(
                    idx, seg["image"], seg["duration"], seg["output"], seg["effect"]
                )
                if not r.get("ok") and not self._cancel_event.is_set():
                    self._log(f"  [{idx+1}/{total}] failed ({r.get('error', '')[:80]}), retrying…", "warning")
                    if os.path.exists(seg["output"]):
                        os.remove(seg["output"])
                    r = self.render_segment_image_only(
                        idx, seg["image"], seg["duration"], seg["output"], seg["effect"]
                    )
            else:
                # Folder-audio mode: image + matched audio
                r = self.render_segment(idx, seg["image"], seg["audio"], seg["output"], seg["effect"])
                # Retry once on non-skip failure
                if not r.get("ok") and not r.get("skipped") and not self._cancel_event.is_set():
                    self._log(f"  [{idx+1}/{total}] failed ({r.get('error', '')[:80]}), retrying…", "warning")
                    if os.path.exists(seg["output"]):
                        os.remove(seg["output"])
                    r = self.render_segment(idx, seg["image"], seg["audio"], seg["output"], seg["effect"])

            with self._progress_lock:
                self.progress.completed_segments += 1
                self.progress.overall_progress = (
                    0.05 + self.progress.completed_segments / total * 0.70
                )
            self._notify_progress()

            if r.get("ok"):
                self._log(f"  [{idx+1}/{total}] done ({r['duration']:.1f}s)")
            elif r.get("skipped"):
                self._log(f"  [{idx+1}/{total}] skipped (audio too short)", "warning")
            else:
                self._log(f"  [{idx+1}/{total}] FAILED: {r.get('error', '')}", "error")
            return r

        with ThreadPoolExecutor(max_workers=workers) as pool:
            future_map = {pool.submit(render_one, seg): seg["index"] for seg in segments}
            for future in as_completed(future_map):
                idx = future_map[future]
                try:
                    results[idx] = future.result()
                except Exception as exc:
                    results[idx] = {"ok": False, "path": "", "duration": 0.0, "error": str(exc)}

        return results

    # ── Scroll mode render ────────────────────────────────────────────────────

    def _concat_segment_audio(self, segments: list) -> str | None:
        """Concatenate per-segment audio files into one for scroll mode."""
        audio_files = [seg["audio"] for seg in segments if seg.get("audio")]
        if not audio_files:
            return None
        if len(audio_files) == 1:
            return audio_files[0]

        concat_list = os.path.join(self.temp_dir, "scroll_audio_list.txt")
        with open(concat_list, "w", encoding="utf-8") as f:
            for af in audio_files:
                escaped = af.replace("'", "'\\''")
                f.write(f"file '{escaped}'\n")

        out_audio = os.path.join(self.temp_dir, "scroll_concat.aac")
        cmd = [
            "ffmpeg", "-y",
            "-f", "concat", "-safe", "0",
            "-i", concat_list,
            "-c:a", self.config.audio_codec,
            "-b:a", self.config.audio_bitrate,
            out_audio,
        ]
        result = self._run_ffmpeg(cmd, timeout=600)
        if result["ok"]:
            return out_audio
        self._log(f"Audio concat warning: {result['error'][:120]}", "warning")
        return None

    def _probe_scaled_height(self, img_path: str, w_out: int, h_fallback: int) -> int:
        """Return image height after scaling to w_out, keeping aspect ratio (even number)."""
        try:
            proc = subprocess.run(
                ["ffprobe", "-v", "quiet", "-select_streams", "v:0",
                 "-show_entries", "stream=width,height",
                 "-of", "csv=p=0", img_path],
                capture_output=True, text=True, timeout=10,
            )
            parts = proc.stdout.strip().split(",")
            iw, ih = int(parts[0]), int(parts[1])
            new_h = max(2, int(ih * w_out / iw))
        except Exception:
            new_h = h_fallback
        return new_h if new_h % 2 == 0 else new_h + 1

    def _vstack_run(self, image_paths: list, heights: list, w_out: int,
                    output_path: str, image_scale: float = 1.0) -> bool:
        """Stack images vertically.

        image_scale < 1.0: each row becomes a (w_out × heights[i]) blur panel
        with the image centred at (w_out*scale) width.  heights[i] must already
        be calculated at fg_w = int(w_out*scale) width by the caller.
        """
        n     = len(image_paths)
        scale = max(0.1, min(1.0, image_scale))
        cmd   = ["ffmpeg", "-y"]
        for p in image_paths:
            cmd += ["-i", p]

        if scale >= 0.999:
            # ── simple: each image scales to w_out × heights[i] ──────────────
            if n == 1:
                fc = f"[0:v]scale={w_out}:{heights[0]}:flags=lanczos[stacked]"
            else:
                parts   = [f"[{i}:v]scale={w_out}:{heights[i]}:flags=lanczos[s{i}]"
                           for i in range(n)]
                vstk_in = "".join(f"[s{i}]" for i in range(n))
                fc      = ";".join(parts) + f";{vstk_in}vstack=inputs={n}[stacked]"
        else:
            # ── blur panel: fg at fg_w centred on blurred bg at w_out ─────────
            fg_w = int(w_out * scale)
            if fg_w % 2 != 0:
                fg_w -= 1
            panel_parts = []
            for i in range(n):
                h = heights[i]
                panel_parts.append(
                    f"[{i}:v]split=2[_pb{i}][_pf{i}];"
                    # blur bg: scale 2× before blur then back down — eliminates gblur square artifacts
                    f"[_pb{i}]scale={w_out*2}:{h*2}:force_original_aspect_ratio=increase:flags=lanczos,"
                    f"crop={w_out*2}:{h*2},gblur=sigma=60:steps=6,"
                    f"scale={w_out}:{h}:flags=lanczos[_pbg{i}];"
                    # fg: scale to exactly fg_w × h (heights[i] was computed at fg_w)
                    f"[_pf{i}]scale={fg_w}:{h}:flags=lanczos[_pfg{i}];"
                    # overlay centred
                    f"[_pbg{i}][_pfg{i}]overlay=(W-w)/2:(H-h)/2[_panel{i}]"
                )
            if n == 1:
                fc = ";".join(panel_parts) + ";[_panel0]copy[stacked]"
            else:
                vstk_in = "".join(f"[_panel{i}]" for i in range(n))
                fc      = ";".join(panel_parts) + f";{vstk_in}vstack=inputs={n}[stacked]"

        cmd += ["-filter_complex", fc, "-map", "[stacked]", "-frames:v", "1", output_path]
        result = self._run_ffmpeg(cmd, timeout=600)
        if not result["ok"]:
            self._log(f"vstack failed: {result['error'][:200]}", "error")
        return result["ok"]

    def _render_scroll_mode(self, meta: dict) -> dict:
        """Render scroll using chunked approach to stay within FFmpeg's ~16384px frame limit.

        The virtual strip is split into overlapping chunks of ≤ 14 000 px each.
        Each chunk is rendered as a short clip; all clips are then concatenated.
        The math guarantees frame-perfect continuity at every chunk boundary.
        """
        segments = meta["segments"]
        if not segments:
            return {"ok": False, "error": "No segments for scroll mode"}

        try:
            w_out, h_out = (int(x) for x in self.config.resolution.split("x"))
        except Exception:
            w_out, h_out = 1920, 1080

        image_paths  = [seg["image"] for seg in segments]
        n            = len(image_paths)
        image_scale  = max(0.1, min(1.0, self.config.image_scale))

        # When scale < 1: each panel row is the image at fg_w wide (blur fills the rest)
        if image_scale < 0.999:
            fg_w = int(w_out * image_scale)
            if fg_w % 2 != 0:
                fg_w -= 1
            probe_w = fg_w
        else:
            probe_w = w_out

        self._log(f"Scroll: probing {n} images (scale={image_scale:.0%}, probe_w={probe_w})...")
        heights = [self._probe_scaled_height(p, probe_w, h_out) for p in image_paths]

        # Cumulative y-offsets in the virtual strip
        y_starts: list[int] = []
        y = 0
        for h in heights:
            y_starts.append(y)
            y += h
        total_height = y
        scroll_range = max(0, total_height - h_out)

        total_dur = meta["total_duration"]
        if total_dur <= 0:
            total_dur = max(n * 5.0, 10.0)

        fps    = self.config.fps
        preset = _QUALITY_PRESET.get(self.config.quality_preset, "medium")
        crf    = _QUALITY_CRF.get(self.config.quality_preset, 23)

        self._log(f"Scroll: strip {w_out}x{total_height}px, range={scroll_range}px, dur={total_dur:.1f}s")

        # ── static (no scrolling) ──────────────────────────────────────────
        if scroll_range == 0:
            stacked = os.path.join(self.temp_dir, "scroll_stack.png")
            if not self._vstack_run(image_paths, heights, w_out, stacked):
                return {"ok": False, "error": "Image stacking failed"}
            scroll_raw = os.path.join(self.temp_dir, "scroll_raw.mp4")
            cmd = [
                "ffmpeg", "-y", "-loop", "1", "-r", str(fps), "-i", stacked,
                "-vf", f"crop={w_out}:{h_out}:0:0,format=yuv420p",
                "-c:v", self.config.video_codec, "-preset", preset, "-crf", str(crf),
                "-pix_fmt", "yuv420p", "-t", f"{total_dur:.6f}",
                "-an", "-movflags", "+faststart", scroll_raw,
            ]
            res = self._run_ffmpeg(cmd, timeout=3600)
            if not res["ok"]:
                return {"ok": False, "error": f"Static encode failed: {res['error']}"}

        # ── scrolling ─────────────────────────────────────────────────────
        else:
            pps = scroll_range / total_dur

            # Keep each chunk PNG height ≤ 14 000 px.
            # Worst-case chunk height = max_chunk_scroll + h_out + max_single_image_h.
            max_img_h       = max(heights) if heights else h_out
            max_chunk_scroll = max(h_out, 14000 - h_out - max_img_h)
            self._log(f"Scroll: pps={pps:.2f}px/s, chunk_scroll={max_chunk_scroll}px")

            clip_paths: list[str] = []
            y_curr = 0
            ci     = 0
            while y_curr < scroll_range:
                y_end    = min(y_curr + max_chunk_scroll, scroll_range)
                clip_out = os.path.join(self.temp_dir, f"scroll_clip_{ci:04d}.mp4")
                result   = self._render_scroll_chunk(
                    image_paths, heights, y_starts, n,
                    y_curr, y_end, w_out, h_out, pps, fps, preset, crf, clip_out,
                    image_scale,
                )
                if result is None:
                    return {"ok": False, "error": f"Scroll chunk {ci} failed"}
                clip_paths.append(result)

                with self._progress_lock:
                    self.progress.overall_progress = 0.10 + (y_end / scroll_range) * 0.60
                self._notify_progress()
                self._log(f"Scroll: chunk {ci+1} done  y=[{y_curr}→{y_end}]")

                y_curr = y_end
                ci    += 1

            scroll_raw = os.path.join(self.temp_dir, "scroll_raw.mp4")
            if len(clip_paths) == 1:
                shutil.copy2(clip_paths[0], scroll_raw)
            else:
                self._log(f"Scroll: joining {len(clip_paths)} chunks...")
                concat_txt = os.path.join(self.temp_dir, "scroll_clips.txt")
                with open(concat_txt, "w", encoding="utf-8") as f:
                    for cp in clip_paths:
                        f.write(f"file '{cp.replace(chr(92), '/')}'\n")
                cmd_cat = [
                    "ffmpeg", "-y", "-f", "concat", "-safe", "0",
                    "-i", concat_txt, "-c", "copy", scroll_raw,
                ]
                cat_res = self._run_ffmpeg(cmd_cat, timeout=1800)
                if not cat_res["ok"]:
                    return {"ok": False, "error": f"Chunk concat failed: {cat_res['error']}"}

        with self._progress_lock:
            self.progress.overall_progress = 0.70
        self._notify_progress()

        # ── mux audio ─────────────────────────────────────────────────────
        if meta.get("single_audio_mode"):
            audio_src = self.config.single_audio_file
        else:
            audio_src = self._concat_segment_audio(segments)
            if not audio_src:
                return {"ok": False, "error": "Failed to concatenate audio files"}

        scroll_out = os.path.join(self.temp_dir, "scroll_muxed.mp4")
        cmd_mux = [
            "ffmpeg", "-y",
            "-i", scroll_raw, "-i", audio_src,
            "-map", "0:v", "-map", "1:a",
            "-c:v", "copy", "-c:a", self.config.audio_codec, "-b:a", self.config.audio_bitrate,
            "-shortest", scroll_out,
        ]
        self._log("Scroll: muxing audio...")
        mux_res = self._run_ffmpeg(cmd_mux, timeout=1800)
        if not mux_res["ok"]:
            return {"ok": False, "error": f"Audio mux failed: {mux_res['error']}"}

        with self._progress_lock:
            self.progress.overall_progress = 0.80
        self._notify_progress()
        self._log("Scroll render complete")
        return {"ok": True, "path": scroll_out, "segment_count": n}

    def _render_scroll_chunk(
        self,
        image_paths: list, heights: list, y_starts: list, n: int,
        y_scroll_start: float, y_scroll_end: float,
        w_out: int, h_out: int, pps: float,
        fps: int, preset: str, crf: int,
        out_path: str,
        image_scale: float = 1.0,
    ) -> str | None:
        """Render one scroll clip covering y_scroll ∈ [y_scroll_start, y_scroll_end].

        The chunk PNG contains every image that is (partially) visible anywhere
        in this range.  The crop y-expression maps global scroll positions to
        the correct row inside the chunk PNG, guaranteeing seamless joins.
        """
        vis_end    = y_scroll_end + h_out
        chunk_idxs = [
            i for i in range(n)
            if y_starts[i] + heights[i] > y_scroll_start and y_starts[i] < vis_end
        ]
        if not chunk_idxs:
            return None

        first_i   = chunk_idxs[0]
        chunk_png = out_path.replace(".mp4", "_stack.png")
        ok = self._vstack_run(
            [image_paths[i] for i in chunk_idxs],
            [heights[i]     for i in chunk_idxs],
            w_out, chunk_png,
            image_scale,
        )
        if not ok:
            return None

        # y-offset inside the chunk PNG corresponding to y_scroll_start
        y_crop_0        = y_scroll_start - y_starts[first_i]
        scroll_in_chunk = y_scroll_end - y_scroll_start
        dur             = scroll_in_chunk / pps

        y_expr = f"min({y_crop_0:.2f}+t*{pps:.4f},{y_crop_0:.2f}+{scroll_in_chunk:.2f})"
        vf     = f"crop={w_out}:{h_out}:0:'{y_expr}',format=yuv420p"

        cmd = [
            "ffmpeg", "-y",
            "-loop", "1", "-r", str(fps),
            "-i", chunk_png,
            "-vf", vf,
            "-c:v", "libx264",          # consistent codec across all clips for -c copy concat
            "-preset", preset, "-crf", str(crf),
            "-pix_fmt", "yuv420p",
            "-t", f"{dur:.6f}",
            "-an", "-movflags", "+faststart",
            out_path,
        ]
        res = self._run_ffmpeg(cmd, timeout=600)
        if not res["ok"]:
            self._log(f"Scroll chunk failed: {res['error'][:150]}", "error")
            return None
        return out_path

    # ── Phase 2: merge ────────────────────────────────────────────────────

    def merge_segments(
        self,
        segment_files: list,
        output_path: str,
        force_concat: bool = False,
        video_only: bool = False,
    ) -> dict:
        n = len(segment_files)
        self._log(f"Merging {n} segment(s) → {os.path.basename(output_path)}")

        if n == 1:
            shutil.copy2(segment_files[0], output_path)
            return {"ok": True, "path": output_path}

        method = "concat_file" if force_concat else self.transition_engine.choose_method(n)
        if method == "xfade":
            cmd = self.transition_engine.build_concat_command(
                segment_files, output_path,
                transition=self.config.transition,
                transition_duration=self.config.transition_duration,
                video_only=video_only,
            )
        else:
            concat_file = os.path.join(self.temp_dir, "concat_list.txt")
            self.transition_engine.build_concat_file(segment_files, concat_file)
            cmd = self.transition_engine.concat_with_file(concat_file, output_path)

        result = self._run_ffmpeg(cmd, timeout=1800)
        if result["ok"]:
            self._log(f"Merge complete ({method})")
        else:
            self._log(f"Merge failed: {result['error']}", "error")
        result["path"] = output_path if result["ok"] else ""
        return result

    # ── Phase 2b: inject single audio ────────────────────────────────────

    def _inject_single_audio(self, video_path: str, output_path: str) -> dict:
        """Mux a video-only file with the configured single audio file.
        Uses -shortest so the output is trimmed to whichever stream ends first
        (they should be identical in duration given the exact per-segment split).
        """
        audio_path = self.config.single_audio_file
        self._log(f"Injecting audio: {os.path.basename(audio_path)}")
        cmd = [
            "ffmpeg", "-y",
            "-i", video_path,
            "-i", audio_path,
            "-map", "0:v",
            "-map", "1:a",
            "-c:v", "copy",
            "-c:a", self.config.audio_codec,
            "-b:a", self.config.audio_bitrate,
            "-shortest",
            output_path,
        ]
        result = self._run_ffmpeg(cmd, timeout=1800)
        if result["ok"]:
            self._log("Audio injection complete")
        else:
            self._log(f"Audio injection failed: {result['error']}", "error")
        result["path"] = output_path if result["ok"] else ""
        return result

    # ── Phase 3a: watermark ───────────────────────────────────────────────

    @staticmethod
    def _text_has_cjk(text: str) -> bool:
        """Return True if text contains any CJK / Japanese characters."""
        for ch in text:
            cp = ord(ch)
            if (
                0x3040 <= cp <= 0x309F
                or 0x30A0 <= cp <= 0x30FF
                or 0x4E00 <= cp <= 0x9FFF
                or 0xFF00 <= cp <= 0xFFEF
                or 0x3000 <= cp <= 0x303F
            ):
                return True
        return False

    @staticmethod
    def _find_system_font(cjk: bool = False) -> str:
        """Return the path to an available TTF/OTF/TTC font file on this system.

        When cjk=True, prefer Japanese-capable fonts first.
        """
        import glob as _glob
        japanese_candidates = [
            r"C:\Windows\Fonts\YuGothM.ttc",
            r"C:\Windows\Fonts\YuGothR.ttc",
            r"C:\Windows\Fonts\meiryo.ttc",
            r"C:\Windows\Fonts\msgothic.ttc",
            "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
            "/usr/share/fonts/truetype/noto/NotoSansCJKjp-Regular.otf",
        ]
        latin_candidates = [
            r"C:\Windows\Fonts\arial.ttf",
            r"C:\Windows\Fonts\calibri.ttf",
            r"C:\Windows\Fonts\segoeui.ttf",
            r"C:\Windows\Fonts\tahoma.ttf",
            r"C:\Windows\Fonts\verdana.ttf",
            "/Library/Fonts/Arial.ttf",
            "/System/Library/Fonts/Supplemental/Arial.ttf",
            "/System/Library/Fonts/Helvetica.ttc",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
            "/usr/share/fonts/truetype/ubuntu/Ubuntu-R.ttf",
        ]
        ordered = (japanese_candidates + latin_candidates) if cjk else (latin_candidates + japanese_candidates)
        for path in ordered:
            if os.path.isfile(path):
                return path
        for pattern in [r"C:\Windows\Fonts\*.ttf", r"C:\Windows\Fonts\*.ttc", "/usr/share/fonts/**/*.ttf"]:
            matches = _glob.glob(pattern, recursive=True)
            if matches:
                return matches[0]
        return ""

    def _create_badge_png(
        self,
        logo_path: str,
        text: str,
        fontsize: int,
        font_path: str,
        text_color: str,
        bg_color: str,
        bg_opacity: float,
        temp_dir: str,
    ) -> tuple:
        """Create a badge PNG: [logo | text] on a rounded-corner background.

        Returns (path_to_png, True) on success, ("", False) on failure.
        Uses Pillow; falls back gracefully if PIL is unavailable.
        """
        try:
            from PIL import Image, ImageDraw, ImageFont

            pad    = 12   # padding around content
            gap    = 10   # gap between logo and text

            # ── Font ──────────────────────────────────────────────────────
            try:
                pil_font = ImageFont.truetype(font_path, fontsize) if font_path else ImageFont.load_default()
            except Exception:
                pil_font = ImageFont.load_default()

            # ── Measure text ──────────────────────────────────────────────
            dummy = Image.new("RGBA", (1, 1))
            bbox  = ImageDraw.Draw(dummy).textbbox((0, 0), text, font=pil_font)
            tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
            text_y_off = -bbox[1]   # vertical offset to align baseline

            # ── Logo ──────────────────────────────────────────────────────
            logo_h = max(th + 4, fontsize)
            logo_h = logo_h if logo_h % 2 == 0 else logo_h + 1
            logo_w = logo_h
            if logo_path and os.path.isfile(logo_path):
                logo_img = Image.open(logo_path).convert("RGBA")
                logo_img = logo_img.resize((logo_w, logo_h), Image.LANCZOS)
            else:
                logo_img = None
                logo_w   = 0

            # ── Badge dimensions ──────────────────────────────────────────
            content_h = max(logo_h if logo_img else 0, th)
            badge_h   = content_h + pad * 2
            badge_w   = pad + (logo_w + gap if logo_img else 0) + tw + pad
            if badge_w % 2: badge_w += 1
            if badge_h % 2: badge_h += 1

            # Corner radius: subtle rounding similar to drawtext boxborderw look
            # Scales with badge height but stays small (6-10 px) for a gentle effect
            corner = max(6, min(10, badge_h // 6))

            # ── Draw badge ────────────────────────────────────────────────
            badge = Image.new("RGBA", (badge_w, badge_h), (0, 0, 0, 0))
            draw  = ImageDraw.Draw(badge)

            # Rounded background
            def _hex(h):
                h = h.lstrip("#")
                return tuple(int(h[i:i+2], 16) for i in (0, 2, 4))

            bg_r, bg_g, bg_b = _hex(bg_color) if bg_color.startswith("#") else (0, 0, 0)
            bg_a = int(bg_opacity * 255)
            draw.rounded_rectangle(
                [(0, 0), (badge_w - 1, badge_h - 1)],
                radius=corner,
                fill=(bg_r, bg_g, bg_b, bg_a),
            )

            # Paste logo (transparent background — no box behind logo)
            x_cursor = pad
            if logo_img:
                logo_y = (badge_h - logo_h) // 2
                badge.paste(logo_img, (x_cursor, logo_y), logo_img)
                x_cursor += logo_w + gap

            # Draw text
            tc_r, tc_g, tc_b = _hex(text_color) if text_color.startswith("#") else (255, 255, 255)
            text_y = (badge_h - th) // 2 + text_y_off
            draw.text((x_cursor, text_y), text, font=pil_font, fill=(tc_r, tc_g, tc_b, 255))

            # Pre-bake watermark_opacity into the alpha channel so FFmpeg
            # can do a simple alpha-composite without colorchannelmixer.
            # badge_op is passed as the 'temp_dir' caller's badge_op variable;
            # here we receive it embedded in bg_opacity already — but we also
            # need to scale by the watermark overall opacity (handled by caller).
            # Leave that to the overlay; just ensure corners are correct.

            # Save with full alpha information
            out_path = os.path.join(temp_dir, f"wm_badge_{uuid.uuid4().hex[:8]}.png")
            badge.save(out_path, "PNG")
            return out_path, True

        except ImportError:
            self._log("Pillow not available — install Pillow for logo+text badge", "warning")
            return "", False
        except Exception as exc:
            self._log(f"Badge PNG error: {exc}", "warning")
            return "", False

    @staticmethod
    def _rounded_alpha_expr(r: int) -> str:
        """geq alpha expression for rounded rectangle with corner radius r.
        W, H refer to the logo frame size inside geq context.
        """
        tl = f"if(lt(X,{r})*lt(Y,{r}),lte(hypot(X-{r},Y-{r}),{r}),1)"
        tr = f"if(gt(X,W-{r})*lt(Y,{r}),lte(hypot(X-(W-{r}),Y-{r}),{r}),1)"
        bl = f"if(lt(X,{r})*gt(Y,H-{r}),lte(hypot(X-{r},Y-(H-{r})),{r}),1)"
        br = f"if(gt(X,W-{r})*gt(Y,H-{r}),lte(hypot(X-(W-{r}),Y-(H-{r})),{r}),1)"
        return f"255*{tl}*{tr}*{bl}*{br}"

    def _build_drawtext_parts(self, fontsize: int, text_escaped: str,
                               font_part: str, pos_x: str, pos_y: str) -> str:
        """Build drawtext filter string from components."""
        alpha     = self.config.watermark_opacity
        raw_color = (self.config.watermark_color or "white").strip()
        ffmpeg_color = ("0x" + raw_color[1:]) if (raw_color.startswith("#") and len(raw_color) == 7) else raw_color
        bg_opacity = max(0.0, min(1.0, self.config.watermark_bg_opacity))

        parts = [f"text='{text_escaped}'"]
        if font_part:
            parts.append(font_part)
        parts += [
            f"fontsize={fontsize}",
            f"fontcolor={ffmpeg_color}@{alpha}",
            f"shadowcolor=black@{alpha}:shadowx=2:shadowy=2",
        ]
        if bg_opacity > 0.01:
            raw_bg = (self.config.watermark_bg_color or "#000000").strip()
            ffmpeg_bg = ("0x" + raw_bg[1:]) if (raw_bg.startswith("#") and len(raw_bg) == 7) else raw_bg
            parts.append(f"box=1:boxcolor={ffmpeg_bg}@{bg_opacity:.2f}:boxborderw=6")
        parts += [f"x='{pos_x}'", f"y='{pos_y}'"]
        return "drawtext=" + ":".join(parts)

    def apply_watermark(self, input_path: str, output_path: str) -> dict:
        has_text  = bool(self.config.watermark_text and self.config.watermark_text.strip())
        has_logo  = bool(self.config.watermark_path and os.path.isfile(self.config.watermark_path))
        if not has_text and not has_logo:
            return {"ok": True, "path": input_path, "skipped": True}

        self._log("Applying watermark…")

        margin   = 18
        fontsize = max(18, int(self.config.watermark_scale * 120))
        pos_key  = self.config.watermark_position

        # ── font setup ────────────────────────────────────────────────────────
        font_path = self._find_system_font(cjk=self._text_has_cjk(self.config.watermark_text or ""))
        if font_path:
            fp        = font_path.replace("\\", "/").replace(":", "\\:")
            font_part = f"fontfile='{fp}'"
        else:
            font_part = ""
            self._log("No system font — drawtext may fail on Windows", "warning")

        if has_text:
            text_escaped = (self.config.watermark_text.strip()
                            .replace("\\", "\\\\").replace("'", "\\'")
                            .replace(":", "\\:").replace(",", "\\,"))

        if has_logo and has_text:
            # ── Combined: PIL badge (logo + text on rounded bg) ───────────────
            badge_path, badge_ok = self._create_badge_png(
                logo_path=self.config.watermark_path,
                text=self.config.watermark_text.strip(),
                fontsize=fontsize,
                font_path=self._find_system_font(
                    cjk=self._text_has_cjk(self.config.watermark_text or "")),
                text_color=self.config.watermark_color or "#ffffff",
                bg_color=self.config.watermark_bg_color or "#000000",
                bg_opacity=max(0.0, min(1.0, self.config.watermark_bg_opacity)),
                temp_dir=self.temp_dir,
            )
            if badge_ok and badge_path:
                pos = _WATERMARK_POS.get(pos_key, "W-w-10:H-h-10")
                # format=rgba on the badge input preserves rounded-corner alpha.
                # No colorchannelmixer — it can silently corrupt partial alpha values
                # produced by PIL's anti-aliased rounded_rectangle corners.
                fc = (
                    f"[1:v]format=rgba[_badge];"
                    f"[0:v][_badge]overlay={pos}:format=auto[vout]"
                )
                cmd = [
                    "ffmpeg", "-y",
                    "-i", input_path,   # input 0: video
                    "-i", badge_path,   # input 1: badge PNG (still image)
                    "-filter_complex", fc,
                    "-map", "[vout]", "-map", "0:a",
                    *self._sw_encode_opts(copy_audio=True),
                    output_path,
                ]
            else:
                # Badge creation failed → fall back to text-only
                self._log("Badge PNG failed — falling back to text watermark", "warning")
                _POS_TEXT = {
                    "top_left":     ("20", "20"),
                    "top_right":    ("w-text_w-20", "20"),
                    "bottom_left":  ("20", "h-text_h-20"),
                    "bottom_right": ("w-text_w-20", "h-text_h-20"),
                }
                px, py = _POS_TEXT.get(pos_key, ("w-text_w-20", "h-text_h-20"))
                vf = self._build_drawtext_parts(fontsize, text_escaped, font_part, px, py)
                cmd = [
                    "ffmpeg", "-y",
                    "-i", input_path,
                    "-vf", vf,
                    "-map", "0:v", "-map", "0:a",
                    *self._sw_encode_opts(copy_audio=True),
                    output_path,
                ]

        elif has_logo:
            # ── Logo only with rounded corners ────────────────────────────────
            logo_w_px = max(40, int(self.config.watermark_scale * 200))
            logo_w_px = logo_w_px if logo_w_px % 2 == 0 else logo_w_px + 1
            r         = max(4, logo_w_px // 5)
            pos       = _WATERMARK_POS.get(pos_key, "W-w-10:H-h-10")
            alpha_exp = self._rounded_alpha_expr(r)
            fc        = (
                f"[1:v]scale={logo_w_px}:-2,format=rgba,"
                f"geq=r='p(X,Y)':g='p(X,Y)':b='p(X,Y)':a='{alpha_exp}'"
                f",colorchannelmixer=aa={self.config.watermark_opacity}[_logo_r];"
                f"[0:v][_logo_r]overlay={pos}:format=auto[vout]"
            )
            cmd = [
                "ffmpeg", "-y",
                "-i", input_path,
                "-i", self.config.watermark_path,
                "-filter_complex", fc,
                "-map", "[vout]", "-map", "0:a",
                *self._sw_encode_opts(copy_audio=True),
                output_path,
            ]

        else:
            # ── Text only (original behavior) ─────────────────────────────────
            _POS_TEXT = {
                "top_left":     ("20", "20"),
                "top_right":    ("w-text_w-20", "20"),
                "bottom_left":  ("20", "h-text_h-20"),
                "bottom_right": ("w-text_w-20", "h-text_h-20"),
            }
            px, py = _POS_TEXT.get(pos_key, ("w-text_w-20", "h-text_h-20"))
            vf = self._build_drawtext_parts(fontsize, text_escaped, font_part, px, py)
            cmd = [
                "ffmpeg", "-y",
                "-i", input_path,
                "-vf", vf,
                "-map", "0:v", "-map", "0:a",
                *self._sw_encode_opts(copy_audio=True),
                output_path,
            ]

        result = self._run_ffmpeg(cmd, timeout=1800)
        result["path"] = output_path if result["ok"] else ""
        if not result["ok"]:
            self._log(f"Watermark failed: {result['error']}", "error")
        return result

    # ── Phase 3b: progress bar burned into video ─────────────────────────

    def apply_progress_bar(self, input_path: str, output_path: str) -> dict:
        """Burn a time-based glowing progress bar at the top of the video.

        The bar grows from left (00:00) to right (end of video) in sync with
        the video's own duration — letting viewers see playback position.
        Three overlapping drawbox layers create a blue glow effect.
        """
        if not self.config.render_progress_bar:
            return {"ok": True, "path": input_path, "skipped": True}

        dur = _probe_duration(input_path)
        if dur <= 0:
            self._log("Progress bar: could not probe duration — skipping", "warning")
            return {"ok": True, "path": input_path, "skipped": True}

        self._log(f"Burning progress bar (duration={dur:.2f}s)…")

        # Progress bar — positioned below waveform, same width as waveform.
        #
        # Strategy: crop ONLY the bar strip (7px) from the video, apply a simple
        # geq expression, then overlay back at the exact position.
        # Processing 1920×7 px (not full frame) makes geq extremely fast.
        #
        # geq expressions use ONLY 2-arg comparison functions (no min/clip/nested-if)
        # so the FFmpeg parser never hits nesting limits.  All commas are inside
        # single-quoted option values and are therefore protected.

        # ── Compute bar position from waveform config ─────────────────────────
        wf_ratio = max(0.10, min(1.0, self.config.waveform_width_ratio))
        wf_w     = int(1920 * wf_ratio) & ~1   # make even
        wf_h     = (self.config.waveform_height
                    if self.config.waveform_height % 2 == 0
                    else self.config.waveform_height + 1)
        wf_y     = _H - wf_h - 10              # waveform top-y  (_MARGIN_BOTTOM=10)
        x_off    = (1920 - wf_w) // 2          # same x as waveform

        bar_h   = 5                             # progress bar height (px)
        bar_top = wf_y + wf_h + 4              # 4 px gap below waveform
        # Clamp strip so it never exceeds video bounds (1080 px height)
        max_strip = _H - bar_top               # rows available from bar_top to bottom
        strip_h   = min(bar_h + 2, max_strip)  # never overflow video height

        if strip_h <= 0:
            self._log("Progress bar: bar_top out of video bounds — skipping", "warning")
            return {"ok": True, "path": input_path, "skipped": True}

        dur_s = f"{dur:.4f}"

        # In-bar condition (in strip coordinates — Y=0 is bar_top in the original video)
        in_bar = (
            f"gte(X,{x_off})"
            f"*lte(X,{x_off}+{wf_w}*t/{dur_s})"
            f"*lte(X,{x_off+wf_w})"   # hard clamp: bar max width = wf_w
            f"*gte(Y,0)"
            f"*lte(Y,{bar_h})"
        )
        is_top = f"gte(Y,0)*lte(Y,1)"  # top 2 rows = white highlight

        # geq operates on yuv420p: r/g/b expressions control Y / Cb / Cr planes.
        # White → Y=235, Cb=128, Cr=128 (broadcast-range YUV)
        # Vivid green → Y=145, Cb=54, Cr=34   (approximate)
        # Arithmetic blend (no if/clip): video + in_bar*(bar_colour - video)
        y_top   = "235"                              # Y for white
        y_green = "145"                              # Y for green
        cb_top  = "128"                              # Cb for white (neutral)
        cb_green = "54"                              # Cb for green
        cr_top  = "128"                              # Cr for white
        cr_green = "34"                              # Cr for green

        y_col  = f"({y_top}*{is_top}+{y_green}*(1-({is_top})))"
        cb_col = f"({cb_top}*{is_top}+{cb_green}*(1-({is_top})))"
        cr_col = f"({cr_top}*{is_top}+{cr_green}*(1-({is_top})))"

        r_e = f"p(X,Y)+{in_bar}*({y_col}-p(X,Y))"    # Y  plane (mapped via r=)
        g_e = f"p(X,Y)+{in_bar}*({cb_col}-p(X,Y))"   # Cb plane (mapped via g=)
        b_e = f"p(X,Y)+{in_bar}*({cr_col}-p(X,Y))"   # Cr plane (mapped via b=)

        filter_complex = (
            f"[0:v]split=2[_pv][_ptmp];"
            # Crop the exact bar strip (clamped to video bounds)
            f"[_ptmp]crop=W:{strip_h}:0:{bar_top}[_ps];"
            # Colour the bar pixels using YUV plane expressions
            f"[_ps]geq=r='{r_e}':g='{g_e}':b='{b_e}'[_pbar];"
            # Overlay the coloured strip back at the exact vertical position
            f"[_pv][_pbar]overlay=0:{bar_top}[out]"
        )

        preset = _QUALITY_PRESET.get(self.config.quality_preset, "medium")
        crf    = _QUALITY_CRF.get(self.config.quality_preset, 23)
        cmd = [
            "ffmpeg", "-y",
            "-i", input_path,
            "-filter_complex", filter_complex,
            "-map", "[out]", "-map", "0:a",
            "-c:v", self.config.video_codec,
            "-preset", preset, "-crf", str(crf),
            "-c:a", "copy",
            "-pix_fmt", "yuv420p",
            "-movflags", "+faststart",
            output_path,
        ]
        result = self._run_ffmpeg(cmd, timeout=1800)
        result["path"] = output_path if result["ok"] else ""
        if not result["ok"]:
            self._log(f"Progress bar failed: {result['error']}", "error")
        return result

    # ── Phase 3c: waveform overlay ────────────────────────────────────────

    def apply_waveform_overlay(self, input_path: str, output_path: str) -> dict:
        """Overlay the orange→green audio waveform at the bottom of the video.

        Skipped when waveform_enabled=False or when the input has no audio
        stream (image-only segments before audio injection).
        """
        if not self.config.waveform_enabled:
            return {"ok": True, "path": input_path, "skipped": True}

        self._log("Applying waveform overlay…")
        result = self.waveform_engine.apply_waveform(
            input_path,
            output_path,
            fps=self.config.fps,
            wf_width_ratio=self.config.waveform_width_ratio,
            wf_height=self.config.waveform_height,
            video_codec=self.config.video_codec,
            quality_preset=self.config.quality_preset,
        )
        result["path"] = output_path if result["ok"] else ""
        if not result["ok"]:
            self._log(f"Waveform overlay failed: {result['error']}", "error")
        return result

    def _waveform_subtitle_margin(self) -> int | None:
        """Return the subtitle margin_v override needed when waveform is on."""
        if not self.config.waveform_enabled:
            return None
        return self.waveform_engine.subtitle_margin_v(self.config.waveform_height)

    # ── Phase 3c: subtitles ───────────────────────────────────────────────

    def apply_subtitles(self, input_path: str, output_path: str) -> dict:
        if not self.config.subtitle_srt_path or self.config.subtitle_preset == "none":
            return {"ok": True, "path": input_path, "skipped": True}

        srt = self.config.subtitle_srt_path
        if not os.path.isfile(srt):
            err = f"SRT file not found: {srt}"
            self._log(err, "error")
            return {"ok": False, "path": "", "error": err}

        preset = self.config.subtitle_preset
        self._log(f"Burning subtitles (preset={preset})…")

        # Use the system temp directory for both ASS and a SRT copy.
        # This guarantees a path without spaces on most Windows systems —
        # the output_folder path often contains "My Projects" or similar
        # directory names with spaces that break the FFmpeg `ass` / `subtitles`
        # filter path parser on Windows.
        uid       = uuid.uuid4().hex[:8]
        sys_tmp   = tempfile.gettempdir()
        safe_ass  = os.path.join(sys_tmp, f"manga_sub_{uid}_{preset}.ass")
        safe_srt  = os.path.join(sys_tmp, f"manga_srt_{uid}.srt")

        # Copy SRT to a guaranteed-safe path for the fallback subtitles filter.
        try:
            shutil.copy2(srt, safe_srt)
        except Exception as exc:
            self._log(f"Could not copy SRT to temp: {exc}", "warning")
            safe_srt = srt  # fall back to original path

        result: dict = {"ok": False, "error": "not started"}
        margin_v_ovr = self._waveform_subtitle_margin()
        try:
            # Primary: ASS filter (supports karaoke/fade animations)
            cmd = self.subtitle_engine.burn_subtitles_command(
                input_path,
                safe_srt,
                output_path,
                preset=preset,
                ass_output_path=safe_ass,
                margin_v_override=margin_v_ovr,
            )
            result = self._run_ffmpeg(cmd, timeout=1800)

            # Fallback: plain subtitles filter (simpler, same libass dependency)
            if not result["ok"]:
                self._log(
                    f"ASS filter failed ({result['error'][:120]}); "
                    "retrying with subtitles filter…",
                    "warning",
                )
                fallback_vf = self.subtitle_engine.build_subtitle_filter(safe_srt, preset)
                if fallback_vf:
                    fallback_cmd = [
                        "ffmpeg", "-y",
                        "-i", input_path,
                        "-vf", fallback_vf,
                        "-c:v", DEFAULT_VIDEO_CODEC, "-preset", "fast", "-crf", "18",
                        "-pix_fmt", "yuv420p",
                        "-c:a", "copy",
                        "-movflags", "+faststart",
                        output_path,
                    ]
                    result = self._run_ffmpeg(fallback_cmd, timeout=1800)
        finally:
            for f in (safe_ass, safe_srt if safe_srt != srt else None):
                if f and os.path.isfile(f):
                    try:
                        os.remove(f)
                    except Exception:
                        pass

        result["path"] = output_path if result["ok"] else ""
        if not result["ok"]:
            self._log(
                "Subtitle burn failed — video rendered WITHOUT subtitles.\n"
                "If this keeps happening, verify FFmpeg was compiled with libass "
                f"(run: ffmpeg -filters | findstr ass).\n"
                f"FFmpeg error: {result['error'][-600:]}",
                "error",
            )
        return result

    # ── Phase 3c: intro / outro ───────────────────────────────────────────

    def add_intro_outro(self, input_path: str, output_path: str) -> dict:
        clips = []
        if self.config.intro_path:
            clips.append(self.config.intro_path)
        clips.append(input_path)
        if self.config.outro_path:
            clips.append(self.config.outro_path)

        if len(clips) == 1:
            return {"ok": True, "path": input_path, "skipped": True}

        self._log(f"Adding intro/outro ({len(clips)} clips)…")
        n   = len(clips)
        cmd = ["ffmpeg", "-y"]
        for c in clips:
            cmd += ["-i", c]

        concat_parts   = "".join(f"[{i}:v][{i}:a]" for i in range(n))
        filter_complex = f"{concat_parts}concat=n={n}:v=1:a=1[vout][aout]"
        cmd += [
            "-filter_complex", filter_complex,
            "-map", "[vout]",
            "-map", "[aout]",
            *self._sw_encode_opts(),
            output_path,
        ]
        result = self._run_ffmpeg(cmd, timeout=1800)
        result["path"] = output_path if result["ok"] else ""
        if not result["ok"]:
            self._log(f"Intro/outro failed: {result['error']}", "error")
        return result

    # ── Phase 4: finalize ─────────────────────────────────────────────────

    def finalize(self, input_path: str, final_output_path: str) -> dict:
        self._log(f"Finalizing → {os.path.basename(final_output_path)}")

        # Check disk space before the final (potentially large) encode
        free_mb = _free_mb(self.config.output_folder)
        if free_mb < _MIN_DISK_MB:
            return {
                "ok":    False,
                "error": f"Insufficient disk space for finalize: {free_mb:.0f} MB free",
            }

        start  = time.time()
        source = input_path

        # Optional BGM mix
        if self.config.bgm_path:
            vol    = self.config.bgm_volume
            duck   = self.config.bgm_ducking
            mode   = "ducking" if duck else "simple mix"
            self._log(f"Mixing BGM (volume={vol}, {mode})…")
            bgm_mixed = os.path.join(self.temp_dir, "bgm_mixed.mp4")

            if duck:
                # Audio ducking: sidechaincompress uses voice as sidechain to
                # automatically lower BGM during speech, raise it during silence.
                # attack=300ms, release=700ms → smooth 0.3 s fade each way.
                filter_complex = (
                    f"[0:a]asplit=2[_voice_out][_voice_sc];"
                    f"[1:a]volume={vol}[_bgm_raw];"
                    f"[_bgm_raw][_voice_sc]sidechaincompress="
                    f"threshold=0.05:ratio=4:attack=300:release=700:level_sc=1[_bgm_ducked];"
                    f"[_voice_out][_bgm_ducked]amix=inputs=2:duration=first:dropout_transition=2[aout]"
                )
            else:
                filter_complex = (
                    f"[0:a]volume=1.0[_voice];"
                    f"[1:a]volume={vol}[_bgm];"
                    f"[_voice][_bgm]amix=inputs=2:duration=first:dropout_transition=2[aout]"
                )

            cmd_bgm = [
                "ffmpeg", "-y",
                "-i", source,
                "-i", self.config.bgm_path,
                "-filter_complex", filter_complex,
                "-map", "0:v",
                "-map", "[aout]",
                "-c:v", "copy",
                "-c:a", self.config.audio_codec,
                "-b:a", self.config.audio_bitrate,
                "-shortest",
                bgm_mixed,
            ]
            bgm_res = self._run_ffmpeg(cmd_bgm, timeout=1800)
            if bgm_res["ok"]:
                source = bgm_mixed
                self._log("BGM mixed successfully")
            else:
                self._log(f"BGM mix failed (continuing without): {bgm_res['error']}", "warning")

        cmd = [
            "ffmpeg", "-y",
            "-i", source,
            *self._final_encode_opts(),
            final_output_path,
        ]
        result      = self._run_ffmpeg(cmd, timeout=3600)
        render_time = round(time.time() - start, 2)

        if not result["ok"]:
            self._log(f"Finalize failed: {result['error']}", "error")
            return {"ok": False, "error": result["error"], "render_time": render_time}

        file_size_mb = round(os.path.getsize(final_output_path) / (1024 * 1024), 2)
        duration     = _probe_duration(final_output_path)
        self._log(f"Final output: {file_size_mb} MB, {duration:.1f}s, encoded in {render_time}s")
        self.cleanup_temp()
        return {
            "ok":           True,
            "output_path":  final_output_path,
            "file_size_mb": file_size_mb,
            "duration":     duration,
            "render_time":  render_time,
        }

    # ── multi-part pipeline ───────────────────────────────────────────────

    def _run_multi_part(self) -> dict:
        """Render N (image_folder, audio_file) pairs and concatenate into one video.

        Sub-renders share all quality/effect/watermark settings.
        Subtitle is disabled per-part (timing mismatch across different audios).
        """
        import copy as _copy

        all_parts = [
            {"image_folder": self.config.image_folder,
             "audio_file":   self.config.single_audio_file or ""},
        ] + list(self.config.render_parts)
        n = len(all_parts)
        self._log(f"Multi-part: {n} video(s) — project={self.config.project_name}")
        self._notify_progress()

        os.makedirs(self.config.output_folder, exist_ok=True)
        self.temp_dir = os.path.join(
            self.config.output_folder, f"_temp_{uuid.uuid4().hex[:8]}"
        )
        os.makedirs(self.temp_dir, exist_ok=True)

        part_videos: list[str] = []

        for idx, part in enumerate(all_parts):
            if self._cancel_event.is_set():
                return self._cancel()

            img = part.get("image_folder", "")
            aud = part.get("audio_file",   "")
            self._log(f"=== Video {idx+1}/{n}: {os.path.basename(img)} ===")

            # Sub-config: shared settings + specific inputs; no subtitle/bgm/intro/outro
            sub_cfg = _copy.copy(self.config)
            sub_cfg.image_folder      = img
            sub_cfg.audio_folder      = ""
            sub_cfg.single_audio_file = aud
            sub_cfg.output_folder     = self.temp_dir
            sub_cfg.project_name      = f"part_{idx:02d}"
            sub_cfg.render_parts      = []     # no recursion
            sub_cfg.subtitle_srt_path = None   # subtitle timing differs per part
            sub_cfg.subtitle_preset   = "none"
            sub_cfg.bgm_path          = None   # each part has its own audio
            sub_cfg.intro_path        = None
            sub_cfg.outro_path        = None
            sub_cfg.waveform_enabled    = False  # applied once on merged video below
            sub_cfg.render_progress_bar = False  # applied once on merged video below

            # Scale sub-progress into [idx/n .. (idx+1)/n] of overall
            base  = idx / n
            scale = 1.0 / n

            def _make_cb(_base=base, _scale=scale):
                def cb(prog):
                    with self._progress_lock:
                        self.progress.current_phase      = prog.current_phase
                        self.progress.total_segments     = prog.total_segments
                        self.progress.completed_segments = prog.completed_segments
                        self.progress.overall_progress   = _base + prog.overall_progress * _scale
                    self._notify_progress()
                return cb

            sub = VideoProcessor(
                sub_cfg,
                progress_callback=_make_cb(),
                log_callback=self.log_callback,
            )
            sub._cancel_event = self._cancel_event
            sub._resume_event = self._resume_event

            res = sub.run()
            if not res["ok"]:
                self.cleanup_temp()
                return self._fail(f"Video {idx+1} thất bại: {res.get('error', 'unknown')}")

            part_videos.append(res["output_path"])
            self._log(f"Video {idx+1} done: {os.path.basename(res['output_path'])}")

        # ── concatenate all part videos ───────────────────────────────────
        self._log(f"Ghép {n} video lại…")
        self.progress.current_phase    = "merging"
        self.progress.overall_progress = 0.97
        self._notify_progress()

        output_file = os.path.join(
            self.config.output_folder,
            f"{_sanitize_filename(self.config.project_name)}.mp4",
        )

        if n == 1:
            shutil.copy2(part_videos[0], output_file)
        else:
            concat_txt = os.path.join(self.temp_dir, "parts_list.txt")
            with open(concat_txt, "w", encoding="utf-8") as f:
                for pv in part_videos:
                    f.write(f"file '{pv.replace(chr(92), '/')}'\n")
            cat_res = self._run_ffmpeg(
                ["ffmpeg", "-y", "-f", "concat", "-safe", "0",
                 "-i", concat_txt, "-c", "copy", output_file],
                timeout=3600,
            )
            if not cat_res["ok"]:
                return self._fail(f"Ghép video thất bại: {cat_res['error']}")

        # Delete individual part files (they are already in temp_dir, but be explicit)
        for pv in part_videos:
            try:
                if os.path.isfile(pv):
                    os.remove(pv)
            except Exception:
                pass

        # Phase 3 — subtitle / intro-outro on the merged video
        # (watermark already applied per-part to preserve quality per segment)
        current = output_file

        if self.config.waveform_enabled:
            self.progress.overall_progress = 0.975
            self._notify_progress()
            wf_path = os.path.join(self.temp_dir, "waveform.mp4")
            wf = self.apply_waveform_overlay(current, wf_path)
            if wf["ok"] and not wf.get("skipped"):
                current = wf_path

        if self.config.subtitle_srt_path and self.config.subtitle_preset != "none":
            self.progress.current_phase    = "finalizing"
            self.progress.overall_progress = 0.98
            self._notify_progress()
            self._log("Đốt phụ đề vào video ghép…")
            sub_path = os.path.join(self.temp_dir, "subtitled.mp4")
            sub_res = self.apply_subtitles(current, sub_path)
            if sub_res["ok"] and not sub_res.get("skipped"):
                current = sub_path
                self._log("Phụ đề đã được đốt thành công.")
            elif sub_res.get("skipped"):
                pass  # subtitle_preset = "none" handled upstream
            else:
                err_detail = sub_res.get("error", "unknown")[:200]
                self._log(f"PHỤĐỀ THẤT BẠI — video không có phụ đề: {err_detail}", "error")

        if self.config.intro_path or self.config.outro_path:
            io_path = os.path.join(self.temp_dir, "with_io.mp4")
            io = self.add_intro_outro(current, io_path)
            if io["ok"] and not io.get("skipped"):
                current = io_path

        if self.config.render_progress_bar:
            pb_path = os.path.join(self.temp_dir, "progress_bar.mp4")
            pb = self.apply_progress_bar(current, pb_path)
            if pb["ok"] and not pb.get("skipped"):
                current = pb_path

        if current != output_file:
            try:
                # os.replace is atomic on same drive and overwrites existing dst on Windows
                os.replace(current, output_file)
            except OSError:
                # Cross-drive or other error: fall back to copy + delete
                shutil.copy2(current, output_file)
                try:
                    os.remove(current)
                except Exception:
                    pass

        file_size_mb = round(os.path.getsize(output_file) / (1024 * 1024), 2)
        duration     = _probe_duration(output_file)
        total_time   = round(time.time() - self._start_time, 2)

        self.cleanup_temp()
        self.progress.status           = "completed"
        self.progress.overall_progress = 1.0
        self._notify_progress()
        self._log(f"Multi-part hoàn thành → {output_file}")
        self._cleanup_subtitle_files()
        self._logger.close()

        return {
            "ok":              True,
            "status":          "completed",
            "output_path":     output_file,
            "file_size_mb":    file_size_mb,
            "duration":        duration,
            "segment_count":   n,
            "failed_segments": 0,
            "render_time":     total_time,
        }

    # ── full pipeline ─────────────────────────────────────────────────────

    def run(self) -> dict:
        self._start_time   = time.time()
        self.progress.status = "running"

        # Start file logger and detect hardware upfront
        self._logger.start_session()
        hw = self._detect_hw()
        self._log(f"Pipeline started — project: {self.config.project_name}, hw_encoder: {hw}")
        self._notify_progress()

        # Multi-part mode: delegate to separate pipeline
        if self.config.render_parts:
            return self._run_multi_part()

        # Phase 0
        self.progress.current_phase = "preparing"
        meta = self.prepare()
        if not meta["ok"]:
            return self._fail(meta.get("error", "Prepare failed"))

        segments = meta["segments"]
        if self._cancel_event.is_set():
            return self._cancel()

        if self.config.scroll_mode:
            # Scroll mode: bypass per-segment render — build one vertical image strip + pan
            self.progress.current_phase = "rendering"
            self._log("Scroll mode: building continuous scroll video")
            scroll_result = self._render_scroll_mode(meta)
            if not scroll_result["ok"]:
                return self._fail(scroll_result.get("error", "Scroll render failed"))
            current = scroll_result["path"]
            failed_count = 0
        else:
            # Phase 1 — render each segment individually
            self.progress.current_phase = "rendering"
            render_results = self.render_all_segments_parallel(segments)
            if self._cancel_event.is_set():
                return self._cancel()

            ok_segments   = [r["path"] for r in render_results if r and r.get("ok")]
            failed_count  = len(segments) - len(ok_segments)
            skipped_count = sum(1 for r in render_results if r and r.get("skipped"))
            if skipped_count:
                self._log(f"{skipped_count} segment(s) skipped (audio too short)", "warning")
            if failed_count - skipped_count > 0:
                self._log(f"{failed_count - skipped_count} segment(s) failed after retry", "warning")
            if not ok_segments:
                return self._fail("All segments failed to render")

            # Phase 2
            self.progress.current_phase  = "merging"
            self.progress.overall_progress = 0.75
            self._notify_progress()

            single_audio = bool(self.config.single_audio_file)
            merged_path  = os.path.join(self.temp_dir, "merged.mp4")
            # In single-audio mode segments are video-only (no audio track).
            # Pass video_only=True so xfade is applied without acrossfade;
            # audio is injected separately in Phase 2b.
            merge_result = self.merge_segments(
                ok_segments, merged_path, video_only=single_audio
            )
            if not merge_result["ok"]:
                return self._fail(f"Merge failed: {merge_result.get('error')}")

            current = merged_path

            # Phase 2b: inject single audio track
            if single_audio:
                self.progress.overall_progress = 0.78
                self._notify_progress()
                injected_path = os.path.join(self.temp_dir, "audio_injected.mp4")
                inject_result = self._inject_single_audio(current, injected_path)
                if not inject_result["ok"]:
                    return self._fail(f"Audio injection failed: {inject_result.get('error')}")
                current = injected_path

        if self._cancel_event.is_set():
            return self._cancel()

        # Phase 3
        self.progress.current_phase  = "finalizing"
        self.progress.overall_progress = 0.85
        self._notify_progress()

        if self.config.watermark_text.strip() or self.config.watermark_path:
            wm_path = os.path.join(self.temp_dir, "watermarked.mp4")
            wm = self.apply_watermark(current, wm_path)
            if wm["ok"] and not wm.get("skipped"):
                current = wm_path

        if self.config.waveform_enabled:
            wf_path = os.path.join(self.temp_dir, "waveform.mp4")
            wf = self.apply_waveform_overlay(current, wf_path)
            if wf["ok"] and not wf.get("skipped"):
                current = wf_path

        if self.config.subtitle_srt_path and self.config.subtitle_preset != "none":
            sub_path = os.path.join(self.temp_dir, "subtitled.mp4")
            sub = self.apply_subtitles(current, sub_path)
            if sub["ok"] and not sub.get("skipped"):
                current = sub_path

        if self.config.intro_path or self.config.outro_path:
            io_path = os.path.join(self.temp_dir, "with_io.mp4")
            io = self.add_intro_outro(current, io_path)
            if io["ok"] and not io.get("skipped"):
                current = io_path

        if self.config.render_progress_bar:
            pb_path = os.path.join(self.temp_dir, "progress_bar.mp4")
            pb = self.apply_progress_bar(current, pb_path)
            if pb["ok"] and not pb.get("skipped"):
                current = pb_path

        if self._cancel_event.is_set():
            return self._cancel()

        # Phase 4
        self.progress.overall_progress = 0.90
        self._notify_progress()

        output_file = os.path.join(
            self.config.output_folder,
            f"{_sanitize_filename(self.config.project_name)}.mp4",
        )
        final = self.finalize(current, output_file)
        if not final["ok"]:
            return self._fail(final.get("error", "Finalize failed"))

        self.progress.status           = "completed"
        self.progress.overall_progress = 1.0
        self._notify_progress()
        self._log(f"Pipeline complete → {output_file}")
        self._cleanup_subtitle_files()
        self._logger.close()

        return {
            **final,
            "ok":              True,
            "status":          "completed",
            "segment_count":   len(segments),
            "failed_segments": failed_count,
            "render_time":     round(time.time() - self._start_time, 2),  # total pipeline time
        }

    def _cleanup_subtitle_files(self) -> None:
        """Delete auto-generated SRT after a successful render.

        The ASS file is now written to temp_dir (cleaned by cleanup_temp), so
        only the source SRT needs explicit deletion here — and only when it was
        auto-generated by Whisper (i.e. it lives inside the output folder).
        User-selected SRT files stored elsewhere are intentionally left alone.
        """
        srt = self.config.subtitle_srt_path
        if not srt:
            return

        # Only delete the SRT when it was auto-generated (lives in output folder)
        try:
            out_abs = os.path.abspath(self.config.output_folder)
            srt_abs = os.path.abspath(srt)
            if srt_abs.startswith(out_abs + os.sep) or srt_abs.startswith(out_abs + "/"):
                if os.path.isfile(srt_abs):
                    os.remove(srt_abs)
                    self._log(f"Deleted auto-generated SRT: {os.path.basename(srt_abs)}")
        except Exception as exc:
            self._log(f"Could not delete SRT {os.path.basename(srt)}: {exc}", "warning")

    # ── control ───────────────────────────────────────────────────────────

    def cancel(self) -> None:
        self._cancel_event.set()
        self._resume_event.set()
        self.progress.status = "cancelled"
        self._log("Cancel requested")

    def pause(self) -> None:
        self._resume_event.clear()
        self.progress.status = "paused"
        self._log("Paused")
        self._notify_progress()

    def resume(self) -> None:
        self._resume_event.set()
        self.progress.status = "running"
        self._log("Resumed")
        self._notify_progress()

    def cleanup_temp(self) -> None:
        if self.temp_dir and os.path.isdir(self.temp_dir):
            try:
                shutil.rmtree(self.temp_dir)
                self._log(f"Cleaned temp: {self.temp_dir}")
            except Exception as exc:
                self._log(f"Cleanup warning: {exc}", "warning")
            finally:
                self.temp_dir = ""

    # ── private pipeline helpers ──────────────────────────────────────────

    def _fail(self, error: str) -> dict:
        self.progress.status = "failed"
        self._log(f"Pipeline failed: {error}", "error")
        self._notify_progress()
        self.cleanup_temp()
        self._logger.close()
        return {"ok": False, "status": "failed", "error": error}

    def _cancel(self) -> dict:
        self.progress.status = "cancelled"
        self._notify_progress()
        self.cleanup_temp()
        self._logger.close()
        return {"ok": False, "status": "cancelled", "error": "Cancelled by user"}