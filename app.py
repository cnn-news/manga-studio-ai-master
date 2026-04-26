import os
import sys
import tempfile
import time
import uuid
from dataclasses import asdict, fields
from datetime import datetime

import psutil
from flask import Flask, jsonify, request, send_file, send_from_directory
from flask_socketio import SocketIO, emit, join_room

from config import APP_NAME, APP_VERSION, DEBUG, HOST, PORT
from core.project_manager import ProjectManager
from core.validator import SystemValidator
from core.video_processor import RenderConfig, VideoProcessor

# ── app / socketio setup ──────────────────────────────────────────────────────

app = Flask(__name__, static_folder="static", template_folder="templates")
app.config["SECRET_KEY"] = "manga-studio-ai-local-secret"

_CORS_ORIGINS = [f"http://localhost:{PORT}", f"http://127.0.0.1:{PORT}"]
# threading mode: Flask-SocketIO uses real OS threads — no monkey_patch needed,
# subprocess/FFmpeg calls are safe, works reliably on Windows.
socketio = SocketIO(
    app,
    async_mode="threading",
    cors_allowed_origins=_CORS_ORIGINS,
    logger=False,
    engineio_logger=False,
)

# ── singletons ────────────────────────────────────────────────────────────────

pm = ProjectManager()
_validator = SystemValidator()

# job_id → {"processor": VideoProcessor, "config": dict}
jobs: dict = {}

_CONFIG_FIELDS = {f.name for f in fields(RenderConfig)}

# ── helpers ───────────────────────────────────────────────────────────────────

def _api_error(message: str, detail: str = "", status: int = 400):
    print(f"[ERROR] {message}: {detail}", file=sys.stderr)
    return jsonify({"error": message, "detail": detail}), status


def _dict_to_config(data: dict) -> RenderConfig:
    """Convert a JSON dict to RenderConfig, ignoring unknown keys."""
    filtered = {k: v for k, v in data.items() if k in _CONFIG_FIELDS and v is not None}
    for req in ("image_folder", "audio_folder", "output_folder"):
        if req not in filtered:
            raise ValueError(f"Missing required field: {req}")
    return RenderConfig(**filtered)


def _progress_to_dict(progress) -> dict:
    d = asdict(progress)
    # Keep only the last 200 log entries to avoid huge payloads
    d["logs"] = d["logs"][-200:]
    return d


# ── CORS headers for REST endpoints ──────────────────────────────────────────

@app.after_request
def _add_cors(response):
    origin = request.headers.get("Origin", "")
    if any(origin.startswith(o) for o in [f"http://localhost", f"http://127.0.0.1"]):
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, DELETE, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return response


@app.before_request
def _handle_options():
    if request.method == "OPTIONS":
        return app.make_default_options_response()


# ── static / index ────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory("templates", "index.html")


@app.route("/static/<path:filename>")
def static_files(filename):
    return send_from_directory("static", filename)


# ── /api/validate ─────────────────────────────────────────────────────────────

@app.route("/api/validate", methods=["POST"])
def api_validate():
    try:
        data = request.json or {}
        image_folder  = data.get("image_folder", "")
        audio_folder  = data.get("audio_folder", "")
        output_folder = data.get("output_folder", "")

        if not all([image_folder, audio_folder, output_folder]):
            return _api_error("image_folder, audio_folder, output_folder are required")

        result = _validator.run_all(image_folder, audio_folder, output_folder)
        return jsonify(result)
    except Exception as exc:
        return _api_error("Validation failed", str(exc), 500)


# ── /api/prepare ──────────────────────────────────────────────────────────────

@app.route("/api/prepare", methods=["POST"])
def api_prepare():
    try:
        config = _dict_to_config(request.json or {})
    except (ValueError, TypeError) as exc:
        return _api_error("Invalid config", str(exc))

    try:
        vp = VideoProcessor(config)
        meta = vp.prepare()
        if not meta["ok"]:
            return _api_error("Prepare failed", meta.get("error", ""), 400)
        vp.cleanup_temp()
        return jsonify({
            "segment_count":     meta["segment_count"],
            "total_duration":    meta["total_duration"],
            "estimated_size_mb": meta["estimated_size_mb"],
            "audio_durations":   meta["durations"],
        })
    except Exception as exc:
        return _api_error("Prepare error", str(exc), 500)


# ── /api/render ───────────────────────────────────────────────────────────────

@app.route("/api/render/start", methods=["POST"])
def render_start():
    try:
        config = _dict_to_config(request.json or {})
    except (ValueError, TypeError) as exc:
        return _api_error("Invalid config", str(exc))

    job_id = uuid.uuid4().hex
    config_dict = request.json

    # Callbacks wired to SocketIO — captured by closure over job_id
    def on_progress(progress):
        socketio.emit("render_progress", _progress_to_dict(progress),
                      room=f"job_{job_id}")

    def on_log(message, level):
        socketio.emit("render_log", {
            "message":   message,
            "level":     level,
            "timestamp": datetime.now().strftime("%H:%M:%S"),
        }, room=f"job_{job_id}")

    processor = VideoProcessor(config, progress_callback=on_progress,
                               log_callback=on_log)
    jobs[job_id] = {"processor": processor, "config": config_dict}

    def _run():
        try:
            result = processor.run()
            jobs[job_id]["result"] = result

            if result.get("ok"):
                socketio.emit("render_complete", {"result": result},
                              room=f"job_{job_id}")
                pm.add_render_history({
                    "project_name":        config_dict.get("project_name", ""),
                    "image_folder":        config_dict.get("image_folder", ""),
                    "audio_folder":        config_dict.get("audio_folder", ""),
                    "output_folder":       config_dict.get("output_folder", ""),
                    "output_file":         result.get("output_path", ""),
                    "segment_count":       result.get("segment_count", 0),
                    "total_duration":      result.get("duration", 0),
                    "render_time_seconds": result.get("render_time", 0),
                    "file_size_mb":        result.get("file_size_mb", 0),
                    "resolution":          config_dict.get("resolution", "1920x1080"),
                    "fps":                 config_dict.get("fps", 60),
                    "status":              "completed",
                })
            else:
                error = result.get("error", "Unknown error")
                socketio.emit("render_error", {"error": error},
                              room=f"job_{job_id}")
                pm.add_render_history({
                    "project_name":  config_dict.get("project_name", ""),
                    "status":        result.get("status", "failed"),
                    "error_message": error,
                })
        except Exception as exc:
            err = str(exc)
            jobs[job_id]["result"] = {"ok": False, "error": err}
            socketio.emit("render_error", {"error": err}, room=f"job_{job_id}")
            print(f"[ERROR] Job {job_id} crashed: {exc}", file=sys.stderr)

    socketio.start_background_task(_run)
    return jsonify({"job_id": job_id, "status": "started"})


@app.route("/api/render/cancel", methods=["POST"])
def render_cancel():
    try:
        job_id = (request.json or {}).get("job_id", "")
        if job_id not in jobs:
            return _api_error("Job not found", status=404)
        jobs[job_id]["processor"].cancel()
        return jsonify({"status": "cancelled"})
    except Exception as exc:
        return _api_error("Cancel failed", str(exc), 500)


@app.route("/api/render/pause", methods=["POST"])
def render_pause():
    try:
        job_id = (request.json or {}).get("job_id", "")
        if job_id not in jobs:
            return _api_error("Job not found", status=404)
        jobs[job_id]["processor"].pause()
        return jsonify({"status": "paused"})
    except Exception as exc:
        return _api_error("Pause failed", str(exc), 500)


@app.route("/api/render/resume", methods=["POST"])
def render_resume():
    try:
        job_id = (request.json or {}).get("job_id", "")
        if job_id not in jobs:
            return _api_error("Job not found", status=404)
        jobs[job_id]["processor"].resume()
        return jsonify({"status": "running"})
    except Exception as exc:
        return _api_error("Resume failed", str(exc), 500)


@app.route("/api/render/status/<job_id>")
def render_status(job_id):
    if job_id not in jobs:
        return _api_error("Job not found", status=404)
    try:
        processor = jobs[job_id]["processor"]
        return jsonify(_progress_to_dict(processor.progress))
    except Exception as exc:
        return _api_error("Status error", str(exc), 500)


# ── /api/history & /api/stats ─────────────────────────────────────────────────

@app.route("/api/history")
def api_history():
    try:
        limit = int(request.args.get("limit", 20))
        return jsonify(pm.get_render_history(limit=limit))
    except Exception as exc:
        return _api_error("History error", str(exc), 500)


@app.route("/api/stats")
def api_stats():
    try:
        return jsonify(pm.get_stats())
    except Exception as exc:
        return _api_error("Stats error", str(exc), 500)


# ── /api/project ──────────────────────────────────────────────────────────────

@app.route("/api/project/save", methods=["POST"])
def project_save():
    try:
        data = request.json or {}
        name     = data.get("name", "")
        settings = data.get("settings", {})
        if not name:
            return _api_error("name is required")
        ok = pm.save_project(name, settings)
        return jsonify({"ok": ok})
    except Exception as exc:
        return _api_error("Save failed", str(exc), 500)


@app.route("/api/project/list")
def project_list():
    try:
        return jsonify(pm.list_projects())
    except Exception as exc:
        return _api_error("List failed", str(exc), 500)


@app.route("/api/project/load", methods=["POST"])
def project_load():
    try:
        name = (request.json or {}).get("name", "")
        if not name:
            return _api_error("name is required")
        project = pm.load_project(name)
        if project is None:
            return _api_error(f"Project '{name}' not found", status=404)
        return jsonify(project)
    except Exception as exc:
        return _api_error("Load failed", str(exc), 500)


@app.route("/api/project/<name>", methods=["DELETE"])
def project_delete(name):
    try:
        ok = pm.delete_project(name)
        if not ok:
            return _api_error(f"Project '{name}' not found", status=404)
        return jsonify({"ok": True})
    except Exception as exc:
        return _api_error("Delete failed", str(exc), 500)


@app.route("/api/project/export", methods=["POST"])
def project_export():
    try:
        name = (request.json or {}).get("name", "")
        if not name:
            return _api_error("name is required")

        tmp = os.path.join(tempfile.gettempdir(),
                           f"manga_export_{uuid.uuid4().hex[:8]}.json")
        if not pm.export_settings(name, tmp):
            return _api_error(f"Project '{name}' not found", status=404)

        response = send_file(tmp, as_attachment=True,
                             download_name=f"{name}.json",
                             mimetype="application/json")

        @response.call_on_close
        def _cleanup():
            try:
                os.remove(tmp)
            except OSError:
                pass

        return response
    except Exception as exc:
        return _api_error("Export failed", str(exc), 500)


@app.route("/api/project/import", methods=["POST"])
def project_import():
    try:
        if "file" not in request.files:
            return _api_error("No file in request")

        f = request.files["file"]
        if not f.filename.endswith(".json"):
            return _api_error("Only .json files are accepted")

        tmp = os.path.join(tempfile.gettempdir(),
                           f"manga_import_{uuid.uuid4().hex[:8]}.json")
        f.save(tmp)
        data = pm.import_settings(tmp)
        try:
            os.remove(tmp)
        except OSError:
            pass

        if data is None:
            return _api_error("Invalid or unreadable JSON file")

        return jsonify(data)
    except Exception as exc:
        return _api_error("Import failed", str(exc), 500)


# ── /api/ping · /api/heartbeat ───────────────────────────────────────────────

@app.route("/api/ping")
def api_ping():
    return jsonify({"ok": True})


@app.route("/api/heartbeat", methods=["POST"])
def api_heartbeat():
    """Browser keep-alive. No-op here; lifecycle is tracked via WebSocket."""
    return jsonify({"ok": True})


# ── /api/audio/analyze ───────────────────────────────────────────────────────

@app.route("/api/audio/analyze", methods=["POST"])
def audio_analyze():
    """Return count + real per-file durations for an audio folder (ffprobe)."""
    from core.audio_processor import AudioProcessor
    try:
        path = (request.json or {}).get("path", "").strip()
        if not path or not os.path.isdir(path):
            return _api_error("path không hợp lệ")

        result = _validator.check_folder_audio(path)
        if not result.get("files"):
            return jsonify({**result, "total_duration": 0, "avg_duration": 0,
                            "segment_count": 0})

        ap        = AudioProcessor()
        durations = ap.get_batch_durations(path, result["files"])
        total     = round(sum(durations.values()), 1)
        count     = len(result["files"])

        return jsonify({
            **result,
            "segment_count":  count,
            "total_duration": total,
            "avg_duration":   round(total / count, 1) if count else 0,
        })
    except Exception as exc:
        return _api_error("Audio analysis failed", str(exc), 500)


# ── /api/validate/folder (single-folder lightweight check) ───────────────────

@app.route("/api/validate/folder", methods=["POST"])
def validate_single_folder():
    """Validate one folder for images or audio — no cross-folder matching needed."""
    try:
        data = request.json or {}
        path = (data.get("path") or "").strip()
        kind = data.get("type", "image")   # "image" | "audio"

        if not path:
            return _api_error("path is required")

        if kind == "image":
            result = _validator.check_folder_images(path)
        elif kind == "audio":
            result = _validator.check_folder_audio(path)
        else:
            return _api_error(f"Unknown type '{kind}'. Use 'image' or 'audio'.")

        return jsonify(result)
    except Exception as exc:
        return _api_error("Folder validation failed", str(exc), 500)


# ── /api/system/metrics (realtime CPU/RAM/Disk) ───────────────────────────────

@app.route("/api/system/metrics")
def system_metrics():
    """Lightweight realtime resource metrics — polled every few seconds by UI."""
    try:
        cpu_p  = psutil.cpu_percent(interval=None)
        vm     = psutil.virtual_memory()
        disk   = psutil.disk_usage(os.getcwd())
        return jsonify({
            "cpu_percent":   round(cpu_p, 1),
            "ram_percent":   round(vm.percent, 1),
            "ram_used_gb":   round(vm.used  / 1024 ** 3, 1),
            "ram_total_gb":  round(vm.total / 1024 ** 3, 1),
            "disk_free_gb":  round(disk.free  / 1024 ** 3, 1),
            "disk_total_gb": round(disk.total / 1024 ** 3, 1),
        })
    except Exception as exc:
        return _api_error("Metrics error", str(exc), 500)


# ── /api/system/check ─────────────────────────────────────────────────────────

@app.route("/api/system/check")
def system_check():
    try:
        ffmpeg = _validator.check_ffmpeg()
        vm     = psutil.virtual_memory()
        disk   = psutil.disk_usage(os.getcwd())

        return jsonify({
            "app_name":          APP_NAME,
            "app_version":       APP_VERSION,
            "ffmpeg_ok":         ffmpeg["ok"],
            "ffmpeg_version":    ffmpeg.get("version", ""),
            "hw_encoder":        ffmpeg.get("hw_encoder", "cpu"),
            "cpu_cores":         os.cpu_count() or 1,
            "ram_total_gb":      round(vm.total    / 1024**3, 1),
            "ram_available_gb":  round(vm.available / 1024**3, 1),
            "disk_free_gb":      round(disk.free    / 1024**3, 1),
            "python_version":    sys.version.split()[0],
        })
    except Exception as exc:
        return _api_error("System check failed", str(exc), 500)


# ── /api/browse/folder ────────────────────────────────────────────────────────

@app.route("/api/browse/folder", methods=["POST"])
def browse_folder():
    import string as _string

    def _list_drives():
        """Return all accessible drive roots on Windows, or ['/'] on Unix."""
        if os.name == "nt":
            drives = []
            for letter in _string.ascii_uppercase:
                root = f"{letter}:\\"
                if os.path.exists(root):
                    drives.append({
                        "name":   f"{letter}:",
                        "path":   f"{letter}:/",
                        "is_dir": True,
                    })
            return drives
        return [{"name": "/", "path": "/", "is_dir": True}]

    try:
        data = request.json or {}
        raw  = (data.get("path") or "").strip()

        # Empty path or sentinel "root" → show all drives
        if not raw or raw == "root":
            return jsonify({
                "current":  "root",
                "parent":   None,
                "contents": _list_drives(),
                "is_root":  True,
            })

        path = os.path.abspath(raw)

        if not os.path.isdir(path):
            return _api_error(f"Not a directory: {path}")

        include_files  = bool(data.get("include_files", False))
        file_exts      = [x.lower() for x in (data.get("file_extensions") or [])]

        entries = []
        try:
            for entry in sorted(os.scandir(path),
                                 key=lambda e: (not e.is_dir(), e.name.lower())):
                if entry.is_dir():
                    entries.append({
                        "name":   entry.name,
                        "path":   entry.path.replace("\\", "/"),
                        "is_dir": True,
                    })
                elif include_files:
                    ext = os.path.splitext(entry.name)[1].lower()
                    if not file_exts or ext in file_exts:
                        entries.append({
                            "name":   entry.name,
                            "path":   entry.path.replace("\\", "/"),
                            "is_dir": False,
                        })
        except PermissionError:
            return _api_error(f"Permission denied: {path}", status=403)

        norm   = path.replace("\\", "/")
        parent = os.path.dirname(path).replace("\\", "/")

        # At a drive root (C:/) the dirname equals itself → go back to drive list
        if parent == norm or (os.name == "nt" and len(norm.rstrip("/")) <= 2):
            parent = "root"
        elif parent == norm:
            parent = None  # Unix filesystem root

        return jsonify({
            "current":  norm,
            "parent":   parent,
            "contents": entries,
            "is_root":  False,
        })
    except Exception as exc:
        return _api_error("Browse failed", str(exc), 500)


# ── /api/subtitle/generate (Whisper) ─────────────────────────────────────────

@app.route("/api/subtitle/generate", methods=["POST"])
def subtitle_generate():
    """Auto-generate SRT from audio files using OpenAI Whisper."""

    def _ts(s: float) -> str:
        ms = int((s % 1) * 1000)
        t  = int(s)
        return f"{t // 3600:02d}:{(t % 3600) // 60:02d}:{t % 60:02d},{ms:03d}"

    try:
        try:
            import whisper as _whisper          # openai-whisper
        except ImportError:
            return _api_error(
                "Whisper chưa được cài đặt",
                "Chạy lệnh: pip install openai-whisper",
                status=500,
            )

        data          = request.json or {}
        audio_folder  = (data.get("audio_folder")  or "").strip()
        output_folder = (data.get("output_folder") or "").strip()
        language      = data.get("language") or None   # None = auto-detect
        model_size    = data.get("model_size", "base")

        if not audio_folder or not os.path.isdir(audio_folder):
            return _api_error("audio_folder không hợp lệ")
        if not output_folder:
            return _api_error("output_folder là bắt buộc")
        os.makedirs(output_folder, exist_ok=True)

        from config import SUPPORTED_AUDIO_FORMATS
        from core.audio_processor import AudioProcessor

        audio_files = sorted(
            f for f in os.listdir(audio_folder)
            if os.path.splitext(f)[1].lower() in SUPPORTED_AUDIO_FORMATS
        )
        if not audio_files:
            return _api_error("Không tìm thấy file audio trong thư mục")

        model = _whisper.load_model(model_size)
        ap    = AudioProcessor()

        blocks: list[str] = []
        idx    = 1
        offset = 0.0

        for fname in audio_files:
            fpath = os.path.join(audio_folder, fname)
            try:
                dur = ap.get_audio_duration(fpath)
            except Exception:
                dur = 5.0

            result = model.transcribe(fpath, language=language, task="transcribe")
            for seg in result.get("segments", []):
                text = seg["text"].strip()
                if not text:
                    continue
                start = offset + float(seg["start"])
                end   = offset + float(seg["end"])
                blocks.append(f"{idx}\n{_ts(start)} --> {_ts(end)}\n{text}")
                idx += 1
            offset += dur

        srt_content = "\n\n".join(blocks) + "\n"
        fname_out   = f"subtitles_{int(time.time())}.srt"
        srt_path    = os.path.join(output_folder, fname_out)
        with open(srt_path, "w", encoding="utf-8") as fh:
            fh.write(srt_content)

        return jsonify({
            "ok":          True,
            "srt_path":    srt_path.replace("\\", "/"),
            "entry_count": idx - 1,
        })
    except Exception as exc:
        return _api_error("Subtitle generation failed", str(exc), 500)


# ── WebSocket events ──────────────────────────────────────────────────────────

# ── Client lifecycle & auto-shutdown ─────────────────────────────────────────

import atexit as _atexit
import signal as _signal

_browser_sids: set  = set()   # currently connected browser clients
_ever_connected     = False    # True once the first browser has connected
_shutdown_emitted   = False

def _emit_shutdown(reason: str = "server_stopped") -> None:
    """Emit server_shutdown to all clients; idempotent."""
    global _shutdown_emitted
    if _shutdown_emitted:
        return
    _shutdown_emitted = True
    try:
        socketio.emit("server_shutdown", {"reason": reason})
        time.sleep(1.2)          # brief grace period for clients to receive event
    except Exception:
        pass


def _monitor_browsers() -> None:
    """Shut down when no browser has been connected for IDLE_LIMIT seconds.

    Guards:
    - Only activates after first browser connects.
    - Resets the idle timer while any render job is running (even if browser
      temporarily disconnects due to load — common with many FFmpeg workers).
    - Uses a generous 120 s timeout so page-refresh / brief drops don't
      trigger a false shutdown.
    """
    idle_start = None
    idle_limit = 120  # seconds — generous to survive heavy-render disconnects

    while True:
        time.sleep(5)

        # Never auto-shutdown while a render job is actively running
        has_active_render = any(
            getattr(v.get("processor"), "progress", None) is not None
            and v["processor"].progress.status == "running"
            for v in jobs.values()
        )
        if has_active_render:
            idle_start = None
            continue

        if _ever_connected and len(_browser_sids) == 0:
            if idle_start is None:
                idle_start = time.time()
            elif time.time() - idle_start > idle_limit:
                print("\n[INFO] Không còn browser kết nối — tự động tắt server.")
                _emit_shutdown("no_clients")
                os._exit(0)
        else:
            idle_start = None


# Register graceful shutdown for Ctrl+C path
def _on_exit():
    _emit_shutdown("server_stopped")

_atexit.register(_on_exit)

# Start browser-presence monitor as a background greenlet
socketio.start_background_task(_monitor_browsers)


@socketio.on("connect")
def on_connect():
    global _ever_connected
    _ever_connected = True
    _browser_sids.add(request.sid)
    print(f"[WS] Client connected: {request.sid}  (total: {len(_browser_sids)})")


@socketio.on("disconnect")
def on_disconnect():
    _browser_sids.discard(request.sid)
    print(f"[WS] Client disconnected: {request.sid}  (remaining: {len(_browser_sids)})")


@socketio.on("subscribe_job")
def on_subscribe_job(data):
    """Client calls this after receiving a job_id to start receiving progress events."""
    job_id = (data or {}).get("job_id", "")
    if not job_id:
        emit("error", {"message": "job_id required"})
        return
    room = f"job_{job_id}"
    join_room(room)
    # Immediately emit current progress if job already started
    if job_id in jobs:
        processor = jobs[job_id]["processor"]
        emit("render_progress", _progress_to_dict(processor.progress))


# ── entrypoint ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    url = f"http://{HOST}:{PORT}"
    print(f"Starting {APP_NAME} v{APP_VERSION}")
    print(f"  URL: {url}")
    print("  Khuyến nghị: chạy qua 'py run.py' để có đầy đủ tính năng.\n")

    def _auto_open():
        time.sleep(2.5)
        try:
            if sys.platform == "win32":
                os.startfile(url)
            else:
                import webbrowser
                webbrowser.open(url)
            print(f"[OK] Đã mở browser: {url}")
        except Exception as e:
            print(f"[INFO] Mở browser thủ công tại: {url}  ({e})")

    socketio.start_background_task(_auto_open)
    socketio.run(app, host=HOST, port=PORT, debug=DEBUG)
