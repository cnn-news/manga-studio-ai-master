"""
core/logger.py — Per-session render log with auto-cleanup.

Log format:  [HH:MM:SS] [LEVEL  ] message
Files:       logs/render_YYYYMMDD_HHMMSS.txt
Retention:   7 days (older files deleted on each new session start)
"""

import glob
import os
import time
from datetime import datetime


class RenderLogger:
    LOG_DIR   = "logs"
    KEEP_DAYS = 7

    def __init__(self, log_dir: str = LOG_DIR):
        self.log_dir  = log_dir
        self.log_path = ""
        self._cleanup()

    # ── public API ────────────────────────────────────────────────────────

    def start_session(self, job_id: str = "") -> str:
        """Create a new log file for this render session.

        Returns the absolute path of the created file.
        """
        os.makedirs(self.log_dir, exist_ok=True)
        ts     = datetime.now().strftime("%Y%m%d_%H%M%S")
        suffix = f"_{job_id[:8]}" if job_id else ""
        self.log_path = os.path.join(self.log_dir, f"render_{ts}{suffix}.txt")
        header = (
            f"=== Manga Studio AI — render session {ts} ===\n"
            f"Job: {job_id or 'N/A'}\n"
            f"{'─' * 48}\n"
        )
        self._append(header)
        return self.log_path

    def write(self, message: str, level: str = "INFO") -> None:
        """Append one log line.  No-op if start_session() was never called."""
        if not self.log_path:
            return
        ts   = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] [{level.upper():<7}] {message}\n"
        self._append(line)

    def close(self) -> None:
        """Write a footer and clear the active path."""
        if not self.log_path:
            return
        self._append(f"{'─' * 48}\n=== Session ended ===\n")
        self.log_path = ""

    # ── internal ──────────────────────────────────────────────────────────

    def _append(self, text: str) -> None:
        try:
            with open(self.log_path, "a", encoding="utf-8") as fh:
                fh.write(text)
        except OSError:
            pass  # never let logging crash the render

    def _cleanup(self) -> None:
        """Delete log files older than KEEP_DAYS days."""
        if not os.path.isdir(self.log_dir):
            return
        cutoff = time.time() - self.KEEP_DAYS * 86_400
        for path in glob.glob(os.path.join(self.log_dir, "render_*.txt")):
            try:
                if os.path.getmtime(path) < cutoff:
                    os.remove(path)
            except OSError:
                pass