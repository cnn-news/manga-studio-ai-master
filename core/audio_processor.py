import json
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed


class AudioProcessor:

    # ── probing ───────────────────────────────────────────────────────────

    def get_audio_duration(self, audio_path: str) -> float:
        """Return audio duration in seconds via ffprobe.

        Raises RuntimeError if ffprobe fails or the file is unreadable.
        """
        proc = subprocess.run(
            [
                "ffprobe", "-v", "quiet",
                "-show_entries", "format=duration",
                "-of", "csv=p=0",
                audio_path,
            ],
            capture_output=True, text=True, timeout=10,
        )
        if proc.returncode != 0 or not proc.stdout.strip():
            raise RuntimeError(
                f"ffprobe could not read '{audio_path}': {proc.stderr.strip()}"
            )
        return float(proc.stdout.strip())

    def get_audio_info(self, audio_path: str) -> dict:
        """Return a dict with duration, sample_rate, channels, bitrate.

        Uses ffprobe -print_format json -show_streams.
        bitrate is in bits/sec (int); missing fields default to 0.
        """
        proc = subprocess.run(
            [
                "ffprobe", "-v", "quiet",
                "-print_format", "json",
                "-show_streams",
                "-show_format",
                audio_path,
            ],
            capture_output=True, text=True, timeout=10,
        )
        if proc.returncode != 0:
            raise RuntimeError(
                f"ffprobe failed for '{audio_path}': {proc.stderr.strip()}"
            )
        data = json.loads(proc.stdout)

        # Prefer audio stream; fall back to format-level values
        audio_stream = next(
            (s for s in data.get("streams", []) if s.get("codec_type") == "audio"),
            {},
        )
        fmt = data.get("format", {})

        def _int(d, *keys):
            for k in keys:
                v = d.get(k)
                if v is not None:
                    try:
                        return int(float(v))
                    except (ValueError, TypeError):
                        pass
            return 0

        def _float(d, *keys):
            for k in keys:
                v = d.get(k)
                if v is not None:
                    try:
                        return float(v)
                    except (ValueError, TypeError):
                        pass
            return 0.0

        return {
            "duration":    _float(audio_stream, "duration") or _float(fmt, "duration"),
            "sample_rate": _int(audio_stream, "sample_rate"),
            "channels":    _int(audio_stream, "channels"),
            "bitrate":     _int(audio_stream, "bit_rate") or _int(fmt, "bit_rate"),
        }

    # ── filter string builders ────────────────────────────────────────────

    def normalize_audio_filter(self, target_lufs: float = -14.0) -> str:
        """Return a loudnorm filter string targeting the given LUFS level.

        Defaults match YouTube's loudness normalisation spec:
          I  = -14 LUFS (integrated loudness)
          TP = -1.5 dBTP (true peak ceiling)
          LRA = 11 LU  (loudness range)
        """
        return f"loudnorm=I={target_lufs}:TP=-1.5:LRA=11"

    def fade_filter(
        self,
        duration: float,
        fade_in: float = 0.3,
        fade_out: float = 0.3,
    ) -> str:
        """Return an afade filter string that applies fade-in at the start and
        fade-out at the end of a clip with the given total duration.
        """
        fade_out_start = max(0.0, duration - fade_out)
        return (
            f"afade=t=in:st=0:d={fade_in},"
            f"afade=t=out:st={fade_out_start:.3f}:d={fade_out}"
        )

    # ── command builders ──────────────────────────────────────────────────

    def build_audio_mix_command(
        self,
        voice_path: str,
        bgm_path: str,
        output_path: str,
        bgm_volume: float = 0.15,
        voice_volume: float = 1.0,
    ) -> list:
        """Return an FFmpeg command that mixes voice + BGM with simple ducking.

        BGM is held at bgm_volume throughout so it stays under the voice.
        amix duration=first keeps the output length equal to the voice track.
        dropout_transition=2 fades BGM out smoothly at the end.
        """
        filter_complex = (
            f"[0:a]volume={voice_volume}[voice];"
            f"[1:a]volume={bgm_volume}[bgm];"
            f"[voice][bgm]amix=inputs=2:duration=first:dropout_transition=2[out]"
        )
        return [
            "ffmpeg", "-y",
            "-i", voice_path,
            "-i", bgm_path,
            "-filter_complex", filter_complex,
            "-map", "[out]",
            "-c:a", "aac",
            "-b:a", "192k",
            output_path,
        ]

    # ── batch helpers ─────────────────────────────────────────────────────

    def get_batch_durations(
        self, audio_folder: str, audio_files: list[str]
    ) -> dict:
        """Return {filename: duration_seconds} for every file in audio_files.

        Files are probed concurrently via ThreadPoolExecutor.
        Files that fail to probe get duration 0.0.
        """
        result: dict[str, float] = {}

        def _probe(filename: str) -> tuple[str, float]:
            path = os.path.join(audio_folder, filename)
            try:
                return filename, self.get_audio_duration(path)
            except Exception:
                return filename, 0.0

        # Cap workers at 8; probing is I/O-bound but spawning too many
        # threads for a large batch wastes overhead.
        max_workers = min(8, len(audio_files)) if audio_files else 1
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(_probe, f): f for f in audio_files}
            for future in as_completed(futures):
                filename, duration = future.result()
                result[filename] = duration

        return result

    def estimate_total_duration(self, durations: dict) -> float:
        """Return the sum of all duration values in seconds."""
        return sum(durations.values())
