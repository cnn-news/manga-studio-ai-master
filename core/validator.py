import os
import re
import subprocess

import psutil

from config import SUPPORTED_IMAGE_FORMATS, SUPPORTED_AUDIO_FORMATS

_STEM_RE = re.compile(r"^\d{3}$")


class SystemValidator:

    def check_ffmpeg(self) -> dict:
        result = {"ok": False, "version": "", "hw_encoder": "cpu", "error": ""}
        try:
            proc = subprocess.run(
                ["ffmpeg", "-version"],
                capture_output=True, text=True, timeout=10
            )
            first_line = proc.stdout.splitlines()[0] if proc.stdout else ""
            version = first_line.split("version ")[1].split(" ")[0] if "version " in first_line else first_line

            # Detect available hardware encoders
            enc_proc = subprocess.run(
                ["ffmpeg", "-hide_banner", "-encoders"],
                capture_output=True, text=True, timeout=10
            )
            enc_output = enc_proc.stdout + enc_proc.stderr
            if "h264_nvenc" in enc_output:
                hw_encoder = "nvenc"
            elif "h264_videotoolbox" in enc_output:
                hw_encoder = "videotoolbox"
            elif "h264_vaapi" in enc_output:
                hw_encoder = "vaapi"
            else:
                hw_encoder = "cpu"

            result.update({"ok": True, "version": version, "hw_encoder": hw_encoder})
        except FileNotFoundError:
            result["error"] = "ffmpeg not found in PATH"
        except subprocess.TimeoutExpired:
            result["error"] = "ffmpeg check timed out"
        except Exception as e:
            result["error"] = str(e)
        return result

    def check_folder_images(self, folder_path: str) -> dict:
        return self._check_folder(folder_path, SUPPORTED_IMAGE_FORMATS)

    def check_folder_images_any(self, folder_path: str) -> dict:
        """Like check_folder_images but accepts any filename (no 001/002 requirement).
        Used for single-audio mode where naming convention is not needed.
        """
        result = {"ok": False, "files": [], "count": 0, "errors": []}
        try:
            if not os.path.isdir(folder_path):
                result["errors"].append(f"Folder not found: {folder_path}")
                return result
            files = sorted(
                f for f in os.listdir(folder_path)
                if os.path.splitext(f)[1].lower() in SUPPORTED_IMAGE_FORMATS
            )
            result["files"] = files
            result["count"] = len(files)
            result["ok"] = len(files) > 0
            if len(files) == 0:
                result["errors"].append("Không tìm thấy ảnh (.jpg .jpeg .png .webp)")
        except PermissionError as e:
            result["errors"].append(f"Permission denied: {e}")
        except Exception as e:
            result["errors"].append(str(e))
        return result

    def check_folder_audio(self, folder_path: str) -> dict:
        return self._check_folder(folder_path, SUPPORTED_AUDIO_FORMATS)

    def _check_folder(self, folder_path: str, extensions: list) -> dict:
        result = {"ok": False, "files": [], "count": 0, "errors": []}
        try:
            if not os.path.isdir(folder_path):
                result["errors"].append(f"Folder not found: {folder_path}")
                return result

            files = sorted(
                f for f in os.listdir(folder_path)
                if os.path.splitext(f)[1].lower() in extensions
            )

            bad_names = [
                f for f in files
                if not _STEM_RE.match(os.path.splitext(f)[0])
            ]
            if bad_names:
                result["errors"].append(
                    f"Non-sequential naming (expected 001, 002, ...): {bad_names}"
                )

            result["files"] = files
            result["count"] = len(files)
            result["ok"] = len(files) > 0 and not bad_names
        except PermissionError as e:
            result["errors"].append(f"Permission denied: {e}")
        except Exception as e:
            result["errors"].append(str(e))
        return result

    def check_matching(self, image_folder: str, audio_folder: str) -> dict:
        result = {
            "ok": False,
            "matched": [],
            "unmatched_images": [],
            "unmatched_audio": [],
        }
        try:
            img_check = self.check_folder_images(image_folder)
            aud_check = self.check_folder_audio(audio_folder)

            img_stems = {os.path.splitext(f)[0] for f in img_check["files"]}
            aud_stems = {os.path.splitext(f)[0] for f in aud_check["files"]}

            result["matched"] = sorted(img_stems & aud_stems)
            result["unmatched_images"] = sorted(img_stems - aud_stems)
            result["unmatched_audio"] = sorted(aud_stems - img_stems)
            result["ok"] = (
                bool(result["matched"])
                and not result["unmatched_images"]
                and not result["unmatched_audio"]
            )
        except Exception as e:
            result["unmatched_images"] = [str(e)]
        return result

    def check_single_audio_file(self, audio_path: str) -> dict:
        """Validate a single audio file: exists, supported format, non-zero duration."""
        result = {"ok": False, "duration": 0.0, "errors": []}
        try:
            if not os.path.isfile(audio_path):
                result["errors"].append(f"File not found: {audio_path}")
                return result

            ext = os.path.splitext(audio_path)[1].lower()
            if ext not in SUPPORTED_AUDIO_FORMATS:
                result["errors"].append(
                    f"Unsupported format: {ext}. Supported: {list(SUPPORTED_AUDIO_FORMATS)}"
                )
                return result

            proc = subprocess.run(
                ["ffprobe", "-v", "quiet", "-show_entries", "format=duration",
                 "-of", "csv=p=0", audio_path],
                capture_output=True, text=True, timeout=10,
            )
            dur = float(proc.stdout.strip())
            if dur <= 0:
                result["errors"].append("Audio duration is 0 or invalid")
                return result
            result["duration"] = round(dur, 3)
            result["ok"] = True
        except ValueError:
            result["errors"].append("Cannot parse audio duration from ffprobe output")
        except FileNotFoundError:
            result["errors"].append("ffprobe not found — FFmpeg must be installed")
        except Exception as exc:
            result["errors"].append(str(exc))
        return result

    def run_all_single_audio(
        self, image_folder: str, single_audio_file: str, output_folder: str
    ) -> dict:
        """Validate inputs for single-audio-file mode."""
        ffmpeg = self.check_ffmpeg()
        # Single-audio mode accepts any image filename (no 001/002 naming required)
        images = self.check_folder_images_any(image_folder)
        audio  = self.check_single_audio_file(single_audio_file)

        img_count = images.get("count", 0)
        estimated_mb = self.estimate_output_size(
            image_count=img_count,
            avg_audio_duration=audio["duration"] / max(img_count, 1),
            bitrate_mbps=8.0,
        )
        disk = self.check_disk_space(output_folder, estimated_mb)

        passed = all([ffmpeg["ok"], images["ok"], audio["ok"], disk["ok"]])
        return {
            "passed": passed,
            "ffmpeg": ffmpeg,
            "images": images,
            "audio":  audio,
            "disk":   disk,
            "estimated_output_mb": estimated_mb,
            "mode": "single_audio",
        }

    def check_disk_space(self, output_folder: str, estimated_mb: float) -> dict:
        result = {"ok": False, "available_mb": 0.0, "required_mb": estimated_mb}
        try:
            # Use parent directory if output folder doesn't exist yet
            check_path = output_folder
            while check_path and not os.path.exists(check_path):
                check_path = os.path.dirname(check_path)
            if not check_path:
                check_path = os.getcwd()

            usage = psutil.disk_usage(check_path)
            available_mb = usage.free / (1024 * 1024)
            result["available_mb"] = round(available_mb, 2)
            result["ok"] = available_mb >= estimated_mb
        except Exception as e:
            result["error"] = str(e)
        return result

    def estimate_output_size(
        self,
        image_count: int,
        avg_audio_duration: float,
        bitrate_mbps: float,
    ) -> float:
        total_seconds = image_count * avg_audio_duration
        size_mb = (bitrate_mbps * total_seconds) / 8 * 1.1
        return round(size_mb, 2)

    def run_all(
        self, image_folder: str, audio_folder: str, output_folder: str
    ) -> dict:
        ffmpeg = self.check_ffmpeg()
        images = self.check_folder_images(image_folder)
        audio = self.check_folder_audio(audio_folder)
        matching = self.check_matching(image_folder, audio_folder)

        # Rough size estimate: assume 5s per slide, DEFAULT_VIDEO_BITRATE "8M" = 8 Mbps
        estimated_mb = self.estimate_output_size(
            image_count=images["count"],
            avg_audio_duration=5.0,
            bitrate_mbps=8.0,
        )
        disk = self.check_disk_space(output_folder, estimated_mb)

        passed = all([
            ffmpeg["ok"],
            images["ok"],
            audio["ok"],
            matching["ok"],
            disk["ok"],
        ])

        return {
            "passed": passed,
            "ffmpeg": ffmpeg,
            "images": images,
            "audio": audio,
            "matching": matching,
            "disk": disk,
            "estimated_output_mb": estimated_mb,
        }