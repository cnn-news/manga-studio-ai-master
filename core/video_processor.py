import os
import psutil
import random
import shutil
import subprocess
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
    watermark_text:      str        = ""
    watermark_path:      str | None = None
    watermark_position:  str        = "bottom_right"
    watermark_opacity:   float      = 0.7
    watermark_scale:     float      = 0.15
    intro_path:          str | None = None
    outro_path:          str | None = None
    max_workers:         int | None = None   # None → auto (cpu_count // 2)


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
        video_filter = self.effect_engine.get_effect(effect_name, duration, self.config.effect_speed)
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

        # Effect engine now handles letterbox internally — no pre-processing needed
        video_filter = self.effect_engine.get_effect(effect_name, duration, self.config.effect_speed)

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

    # ── Phase 2: merge ────────────────────────────────────────────────────

    def merge_segments(self, segment_files: list, output_path: str, force_concat: bool = False) -> dict:
        n = len(segment_files)
        self._log(f"Merging {n} segment(s) → {os.path.basename(output_path)}")

        if n == 1:
            shutil.copy2(segment_files[0], output_path)
            return {"ok": True, "path": output_path}

        # force_concat=True is used by single-audio mode (no transitions, exact timing)
        method = "concat_file" if force_concat else self.transition_engine.choose_method(n)
        if method == "xfade":
            cmd = self.transition_engine.build_concat_command(
                segment_files, output_path,
                transition=self.config.transition,
                transition_duration=self.config.transition_duration,
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

    def apply_watermark(self, input_path: str, output_path: str) -> dict:
        has_text  = bool(self.config.watermark_text and self.config.watermark_text.strip())
        has_image = bool(self.config.watermark_path)
        if not has_text and not has_image:
            return {"ok": True, "path": input_path, "skipped": True}

        self._log("Applying watermark…")

        if has_text:
            # Text watermark via FFmpeg drawtext
            _POS_TEXT = {
                "top_left":     "x=20:y=20",
                "top_right":    "x=w-text_w-20:y=20",
                "bottom_left":  "x=20:y=h-text_h-20",
                "bottom_right": "x=w-text_w-20:y=h-text_h-20",
            }
            pos      = _POS_TEXT.get(self.config.watermark_position, "x=w-text_w-20:y=h-text_h-20")
            alpha    = self.config.watermark_opacity
            fontsize = max(18, int(self.config.watermark_scale * 120))
            # Escape special chars for drawtext
            text = (self.config.watermark_text.strip()
                    .replace("\\", "\\\\").replace("'", "\\'")
                    .replace(":", "\\:").replace(",", "\\,"))
            vf = (
                f"drawtext=text='{text}':fontsize={fontsize}:"
                f"fontcolor=white@{alpha}:"
                f"shadowcolor=black@{alpha}:shadowx=2:shadowy=2:"
                f"{pos}"
            )
            cmd = [
                "ffmpeg", "-y",
                "-i", input_path,
                "-vf", vf,
                "-map", "0:v", "-map", "0:a",
                *self._sw_encode_opts(copy_audio=True),
                output_path,
            ]
        else:
            # Image watermark
            pos            = _WATERMARK_POS.get(self.config.watermark_position, "W-w-10:H-h-10")
            filter_complex = (
                f"[1:v]scale=iw*{self.config.watermark_scale}:-1,"
                f"format=rgba,"
                f"colorchannelmixer=aa={self.config.watermark_opacity}[wm];"
                f"[0:v][wm]overlay={pos}[vout]"
            )
            cmd = [
                "ffmpeg", "-y",
                "-i", input_path,
                "-i", self.config.watermark_path,
                "-filter_complex", filter_complex,
                "-map", "[vout]", "-map", "0:a",
                *self._sw_encode_opts(copy_audio=True),
                output_path,
            ]

        result = self._run_ffmpeg(cmd, timeout=1800)
        result["path"] = output_path if result["ok"] else ""
        if not result["ok"]:
            self._log(f"Watermark failed: {result['error']}", "error")
        return result

    # ── Phase 3b: subtitles ───────────────────────────────────────────────

    def apply_subtitles(self, input_path: str, output_path: str) -> dict:
        if not self.config.subtitle_srt_path or self.config.subtitle_preset == "none":
            return {"ok": True, "path": input_path, "skipped": True}

        self._log(f"Burning subtitles (preset={self.config.subtitle_preset})…")
        cmd = self.subtitle_engine.burn_subtitles_command(
            input_path,
            self.config.subtitle_srt_path,
            output_path,
            preset=self.config.subtitle_preset,
        )
        result = self._run_ffmpeg(cmd, timeout=1800)
        result["path"] = output_path if result["ok"] else ""
        if not result["ok"]:
            self._log(f"Subtitle burn failed: {result['error']}", "error")
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
            self._log(f"Mixing BGM (volume={self.config.bgm_volume})…")
            bgm_mixed      = os.path.join(self.temp_dir, "bgm_mixed.mp4")
            filter_complex = (
                f"[0:a]volume=1.0[voice];"
                f"[1:a]volume={self.config.bgm_volume}[bgm];"
                f"[voice][bgm]amix=inputs=2:duration=first:dropout_transition=2[aout]"
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

    # ── full pipeline ─────────────────────────────────────────────────────

    def run(self) -> dict:
        self._start_time   = time.time()
        self.progress.status = "running"

        # Start file logger and detect hardware upfront
        self._logger.start_session()
        hw = self._detect_hw()
        self._log(f"Pipeline started — project: {self.config.project_name}, hw_encoder: {hw}")
        self._notify_progress()

        # Phase 0
        self.progress.current_phase = "preparing"
        meta = self.prepare()
        if not meta["ok"]:
            return self._fail(meta.get("error", "Prepare failed"))

        segments = meta["segments"]
        if self._cancel_event.is_set():
            return self._cancel()

        # Phase 1
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
        # Single-audio segments are video-only; force concat (no transitions) so
        # total duration equals exactly the audio duration.
        merge_result = self.merge_segments(ok_segments, merged_path, force_concat=single_audio)
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

        if self.config.watermark_path:
            wm_path = os.path.join(self.temp_dir, "watermarked.mp4")
            wm = self.apply_watermark(current, wm_path)
            if wm["ok"] and not wm.get("skipped"):
                current = wm_path

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

        if self._cancel_event.is_set():
            return self._cancel()

        # Phase 4
        self.progress.overall_progress = 0.90
        self._notify_progress()

        output_file = os.path.join(
            self.config.output_folder, f"{self.config.project_name}.mp4"
        )
        final = self.finalize(current, output_file)
        if not final["ok"]:
            return self._fail(final.get("error", "Finalize failed"))

        self.progress.status           = "completed"
        self.progress.overall_progress = 1.0
        self._notify_progress()
        self._log(f"Pipeline complete → {output_file}")
        self._logger.close()

        return {
            **final,
            "ok":              True,
            "status":          "completed",
            "segment_count":   len(segments),
            "failed_segments": failed_count,
        }

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