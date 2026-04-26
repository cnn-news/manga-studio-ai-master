import json
import os
import sqlite3
from datetime import datetime

_SCHEMA = """
CREATE TABLE IF NOT EXISTS render_history (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    project_name        TEXT,
    image_folder        TEXT,
    audio_folder        TEXT,
    output_folder       TEXT,
    output_file         TEXT,
    segment_count       INTEGER,
    total_duration      REAL,
    render_time_seconds REAL,
    file_size_mb        REAL,
    resolution          TEXT,
    fps                 INTEGER,
    status              TEXT,
    error_message       TEXT,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS saved_projects (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    name         TEXT UNIQUE,
    settings_json TEXT,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

# Columns accepted by add_render_history; any extra keys in record are ignored.
_HISTORY_FIELDS = [
    "project_name", "image_folder", "audio_folder", "output_folder",
    "output_file", "segment_count", "total_duration", "render_time_seconds",
    "file_size_mb", "resolution", "fps", "status", "error_message",
]


class ProjectManager:

    def __init__(self, db_path: str = "database/history.db"):
        """Create the database file and tables if they do not already exist."""
        self.db_path = db_path
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        with self._connect() as conn:
            conn.executescript(_SCHEMA)

    # ── connection helper ─────────────────────────────────────────────────

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    # ── saved projects ────────────────────────────────────────────────────

    def save_project(self, name: str, settings: dict) -> bool:
        """Insert or update a project's settings (upsert on name).

        created_at is preserved on update; updated_at is refreshed.
        """
        try:
            settings_json = json.dumps(settings, ensure_ascii=False)
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO saved_projects (name, settings_json, created_at, updated_at)
                    VALUES (?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    ON CONFLICT(name) DO UPDATE SET
                        settings_json = excluded.settings_json,
                        updated_at    = CURRENT_TIMESTAMP
                    """,
                    (name, settings_json),
                )
            return True
        except Exception:
            return False

    def load_project(self, name: str) -> dict | None:
        """Return the project record with 'settings' as a parsed dict.

        Returns None if the project does not exist.
        """
        try:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT * FROM saved_projects WHERE name = ?", (name,)
                ).fetchone()
            if row is None:
                return None
            data = dict(row)
            data["settings"] = json.loads(data.pop("settings_json") or "{}")
            return data
        except Exception:
            return None

    def list_projects(self) -> list[dict]:
        """Return all saved projects (without settings_json) newest-updated first."""
        try:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT id, name, created_at, updated_at
                    FROM saved_projects
                    ORDER BY updated_at DESC
                    """
                ).fetchall()
            return [dict(r) for r in rows]
        except Exception:
            return []

    def delete_project(self, name: str) -> bool:
        """Delete a project by name. Returns True if a row was actually removed."""
        try:
            with self._connect() as conn:
                cursor = conn.execute(
                    "DELETE FROM saved_projects WHERE name = ?", (name,)
                )
                return cursor.rowcount > 0
        except Exception:
            return False

    # ── render history ────────────────────────────────────────────────────

    def add_render_history(self, record: dict) -> int:
        """Insert a render record. Unknown keys in record are ignored.

        Returns the new row id, or -1 on failure.
        """
        cols = [f for f in _HISTORY_FIELDS if f in record]
        vals = [record[f] for f in cols]
        placeholders = ", ".join("?" * len(cols))
        col_names = ", ".join(cols)
        try:
            with self._connect() as conn:
                cursor = conn.execute(
                    f"INSERT INTO render_history ({col_names}) VALUES ({placeholders})",
                    vals,
                )
                return cursor.lastrowid or -1
        except Exception:
            return -1

    def get_render_history(self, limit: int = 20) -> list[dict]:
        """Return the most recent render records, newest first."""
        try:
            with self._connect() as conn:
                rows = conn.execute(
                    "SELECT * FROM render_history ORDER BY created_at DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            return [dict(r) for r in rows]
        except Exception:
            return []

    # ── statistics ────────────────────────────────────────────────────────

    def get_stats(self) -> dict:
        """Return aggregate statistics across all render history entries."""
        try:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT
                        COUNT(*) AS total_renders,
                        COALESCE(SUM(total_duration),      0) AS total_duration_seconds,
                        COALESCE(SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END), 0)
                            AS completed,
                        COALESCE(AVG(render_time_seconds), 0) AS avg_render_time
                    FROM render_history
                    """
                ).fetchone()
            total     = row["total_renders"]
            completed = row["completed"]
            return {
                "total_renders":        total,
                "total_duration_hours": round(row["total_duration_seconds"] / 3600, 2),
                "success_rate":         round(completed / total * 100, 1) if total > 0 else 0.0,
                "avg_render_time":      round(row["avg_render_time"], 2),
            }
        except Exception:
            return {
                "total_renders":        0,
                "total_duration_hours": 0.0,
                "success_rate":         0.0,
                "avg_render_time":      0.0,
            }

    # ── import / export ───────────────────────────────────────────────────

    def export_settings(self, name: str, output_path: str) -> bool:
        """Export a saved project's settings to a JSON file.

        The exported object includes the project name and an exported_at timestamp
        so the file is self-describing when shared or archived.
        """
        try:
            project = self.load_project(name)
            if project is None:
                return False
            export_data = {
                "name":        name,
                "settings":    project.get("settings", {}),
                "exported_at": datetime.now().isoformat(),
            }
            parent = os.path.dirname(os.path.abspath(output_path))
            os.makedirs(parent, exist_ok=True)
            with open(output_path, "w", encoding="utf-8") as fh:
                json.dump(export_data, fh, ensure_ascii=False, indent=2)
            return True
        except Exception:
            return False

    def import_settings(self, json_path: str) -> dict | None:
        """Load settings from a JSON file.

        Accepts both the full export envelope {"name":..., "settings":{...}}
        and a bare settings dict.  Returns None on any read or parse error.
        """
        try:
            with open(json_path, encoding="utf-8") as fh:
                data = json.load(fh)
            if "settings" in data:
                return data   # already in envelope format
            return {"name": "", "settings": data}
        except Exception:
            return None
