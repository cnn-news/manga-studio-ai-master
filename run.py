"""
run.py — Manga Studio AI launcher
"""

import argparse
import os
import shutil
import socket
import subprocess
import sys
import threading
import time
import webbrowser
from pathlib import Path

# ─── CLI args ─────────────────────────────────────────────────────────────────

parser = argparse.ArgumentParser(description="Manga Studio AI launcher")
parser.add_argument(
    "--test-mode",
    action="store_true",
    help="Start server only, do not open browser (for automated tests)",
)
args = parser.parse_args()


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _print_banner():
    banner = r"""
╔══════════════════════════════════╗
║     MANGA STUDIO AI v1.0.0      ║
║   Professional Video Renderer   ║
╚══════════════════════════════════╝
"""
    print(banner)


def _check_ffmpeg() -> bool:
    """Return True if ffmpeg is reachable on PATH."""
    return shutil.which("ffmpeg") is not None


def _print_ffmpeg_install_guide():
    print("[ERROR] FFmpeg không tìm thấy trên hệ thống.\n")
    print("Hướng dẫn cài đặt FFmpeg:")
    print("  Windows : https://ffmpeg.org/download.html  (hoặc: winget install ffmpeg)")
    print("  macOS   : brew install ffmpeg")
    print("  Ubuntu  : sudo apt install ffmpeg")
    print("  Fedora  : sudo dnf install ffmpeg")
    print("\nSau khi cài xong, hãy khởi động lại terminal và chạy lại run.py.\n")


def _is_port_free(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.3)
        return s.connect_ex((host, port)) != 0


def _pick_port(host: str, start: int, max_tries: int = 10) -> int:
    for port in range(start, start + max_tries):
        if _is_port_free(host, port):
            return port
    raise RuntimeError(
        f"Không tìm thấy port trống trong dải {start}–{start + max_tries - 1}."
    )


def _ensure_dirs():
    dirs = [
        Path("database"),
        Path("tests") / "output",
    ]
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)


def _open_browser(url: str):
    """Open the default browser."""
    try:
        if sys.platform == "win32":
            os.startfile(url)          # ShellExecuteW — not affected by eventlet
        elif sys.platform == "darwin":
            subprocess.Popen(["open", url])
        else:
            if not webbrowser.open(url):
                subprocess.Popen(["xdg-open", url])
        print(f"[OK] Đã mở browser: {url}")
    except Exception as e:
        print(f"[INFO] Mở browser thủ công tại: {url}  ({e})")


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    _print_banner()

    # 1. FFmpeg check
    if not _check_ffmpeg():
        _print_ffmpeg_install_guide()
        sys.exit(1)
    print("[OK] FFmpeg đã được cài đặt.")

    # 2. Port selection
    HOST = "127.0.0.1"
    START_PORT = 5000
    port = _pick_port(HOST, START_PORT)
    if port != START_PORT:
        print(f"[INFO] Port {START_PORT} đang bị chiếm — dùng port {port}.")
    else:
        print(f"[OK] Sử dụng port {port}.")

    # 3. Required directories
    _ensure_dirs()
    print("[OK] Thư mục database/ và tests/output/ đã sẵn sàng.")

    # 4. Import app after env is verified (avoids import-time side effects)
    try:
        from app import app, socketio
    except ImportError as e:
        print(f"[ERROR] Không thể import app: {e}")
        sys.exit(1)

    url = f"http://{HOST}:{port}"
    print(f"\n[INFO] Manga Studio AI đang chạy tại {url}\n")
    print("       Nhấn Ctrl+C để dừng.\n")

    # 5. Open browser after a short delay to ensure the server has bound the port.
    if not args.test_mode:
        def _open_browser_task():
            time.sleep(2.5)
            _open_browser(url)

        socketio.start_background_task(_open_browser_task)

    # 6. Start Flask-SocketIO server
    try:
        socketio.run(
            app,
            host=HOST,
            port=port,
            debug=False,
            use_reloader=False,
        )
    except KeyboardInterrupt:
        pass
    finally:
        print("\nĐang tắt hệ thống...")
        sys.exit(0)


if __name__ == "__main__":
    main()