"""
tests/test_integration.py — End-to-end integration test suite for Manga Studio AI.

Run:
    python tests/test_integration.py          # custom report
    python -m pytest tests/test_integration.py -v
"""

import os
import sys
import json
import shutil
import time
import threading
import unittest

# ── path setup (allow running from repo root or tests/ dir) ───────────────────
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from app import app, jobs  # noqa: E402
from core.video_processor import RenderConfig, VideoProcessor  # noqa: E402
from core.project_manager import ProjectManager  # noqa: E402

# ── paths ─────────────────────────────────────────────────────────────────────
TESTS_DIR  = os.path.dirname(os.path.abspath(__file__))
SAMPLE_DIR = os.path.join(TESTS_DIR, "sample_data")
IMG_DIR    = os.path.join(SAMPLE_DIR, "images")
AUD_DIR    = os.path.join(SAMPLE_DIR, "audio")
OUT_DIR    = os.path.join(TESTS_DIR, "output")
SRT_PATH   = os.path.join(OUT_DIR, "integration_test.srt")


# ── helpers ───────────────────────────────────────────────────────────────────

def _ensure_dirs():
    os.makedirs(OUT_DIR, exist_ok=True)


def _write_test_srt():
    """Write a minimal SRT covering the 3-segment test audio."""
    content = (
        "1\n00:00:00,000 --> 00:00:01,500\nSlide 001: integration test\n\n"
        "2\n00:00:01,500 --> 00:00:03,000\nSlide 002: integration test\n\n"
        "3\n00:00:03,000 --> 00:00:04,500\nSlide 003: integration test\n"
    )
    with open(SRT_PATH, "w", encoding="utf-8") as fh:
        fh.write(content)


def _base_config(**overrides) -> RenderConfig:
    """Return a fast, small-resolution RenderConfig pointing at sample data."""
    defaults = dict(
        image_folder=IMG_DIR,
        audio_folder=AUD_DIR,
        output_folder=OUT_DIR,
        project_name="integration",
        resolution="640x360",
        fps=30,
        quality_preset="fast",
        effect_mode="fixed",
        fixed_effect="zoom_pulse",
        transition="fade_black",
        transition_duration=0.3,
        normalize_audio=False,
        audio_fade=0.0,
        max_workers=1,
    )
    defaults.update(overrides)
    return RenderConfig(**defaults)


# ── TC-01: Full render pipeline ───────────────────────────────────────────────

class TestRenderPipeline(unittest.TestCase):
    """TC-01 — Full render pipeline: 3 images + 3 audio → .mp4"""

    def setUp(self):
        self.out_file = os.path.join(OUT_DIR, "tc01_full_pipeline.mp4")
        if os.path.exists(self.out_file):
            os.remove(self.out_file)

    def test_output_file_created(self):
        config = _base_config(project_name="tc01_full_pipeline")
        processor = VideoProcessor(config)
        result = processor.run()

        self.assertTrue(
            result.get("ok"),
            f"Pipeline returned ok=False — error: {result.get('error')}",
        )
        self.assertTrue(
            os.path.exists(self.out_file),
            "Output .mp4 was not created",
        )

    def test_output_file_non_empty(self):
        config = _base_config(project_name="tc01_full_pipeline")
        processor = VideoProcessor(config)
        processor.run()

        size = os.path.getsize(self.out_file) if os.path.exists(self.out_file) else 0
        self.assertGreater(size, 0, "Output file size is 0 bytes")

    def test_output_duration_positive(self):
        config = _base_config(project_name="tc01_full_pipeline")
        processor = VideoProcessor(config)
        result = processor.run()

        self.assertGreater(
            result.get("duration", 0),
            0,
            "Reported duration is not > 0",
        )


# ── TC-02: Render with subtitle ───────────────────────────────────────────────

class TestSubtitleRender(unittest.TestCase):
    """TC-02 — Render pipeline with subtitle burning."""

    def setUp(self):
        _write_test_srt()
        self.out_file = os.path.join(OUT_DIR, "tc02_subtitle.mp4")
        if os.path.exists(self.out_file):
            os.remove(self.out_file)

    def test_subtitle_render_succeeds(self):
        config = _base_config(
            project_name="tc02_subtitle",
            subtitle_preset="youtube_classic",
            subtitle_srt_path=SRT_PATH,
        )
        processor = VideoProcessor(config)
        result = processor.run()

        self.assertTrue(
            result.get("ok"),
            f"Subtitle render returned ok=False — error: {result.get('error')}",
        )

    def test_subtitle_output_exists_and_non_empty(self):
        config = _base_config(
            project_name="tc02_subtitle",
            subtitle_preset="youtube_classic",
            subtitle_srt_path=SRT_PATH,
        )
        processor = VideoProcessor(config)
        processor.run()

        self.assertTrue(
            os.path.exists(self.out_file),
            "Subtitle output file not created",
        )
        size = os.path.getsize(self.out_file) if os.path.exists(self.out_file) else 0
        self.assertGreater(size, 0, "Subtitle output file is empty")

    def test_srt_file_valid(self):
        """Sanity-check that the test SRT itself is correctly formed."""
        from core.subtitle_engine import SubtitleEngine
        result = SubtitleEngine().validate_srt(SRT_PATH)
        self.assertTrue(result["ok"], f"SRT validation errors: {result['errors']}")
        self.assertEqual(result["entry_count"], 3)


# ── TC-03: Cancel render ──────────────────────────────────────────────────────

class TestCancelRender(unittest.TestCase):
    """TC-03 — Start a render then cancel it after 0.5 seconds."""

    def test_cancel_returns_cancelled_status(self):
        config = _base_config(project_name="tc03_cancel", max_workers=1)
        processor = VideoProcessor(config)
        result_holder: dict = {}

        def _run():
            result_holder["result"] = processor.run()

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        time.sleep(0.5)       # cancel early — before all 3 FFmpeg encodes finish
        processor.cancel()
        t.join(timeout=60)    # generous join; FFmpeg in-flight still needs to finish

        result = result_holder.get("result", {})
        self.assertEqual(
            result.get("status"),
            "cancelled",
            f"Expected status='cancelled', got: {result.get('status')!r}. "
            f"Full result: {result}",
        )

    def test_cancel_cleans_up_temp_dir(self):
        config = _base_config(project_name="tc03_cancel_cleanup", max_workers=1)
        processor = VideoProcessor(config)

        def _run():
            processor.run()

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        time.sleep(0.5)
        processor.cancel()
        t.join(timeout=60)

        # After cancel, temp_dir must be empty string or a non-existent path
        temp_still_exists = bool(
            processor.temp_dir and os.path.isdir(processor.temp_dir)
        )
        self.assertFalse(
            temp_still_exists,
            f"Temp directory was NOT cleaned up: {processor.temp_dir}",
        )


# ── TC-04: API endpoint tests ─────────────────────────────────────────────────

class TestAPIEndpoints(unittest.TestCase):
    """TC-04 — HTTP API correctness tests using Flask test client."""

    @classmethod
    def setUpClass(cls):
        app.config["TESTING"] = True
        cls.client = app.test_client()

    def _get(self, url, **qs):
        return self.client.get(url, query_string=qs)

    def _post(self, url, payload):
        return self.client.post(url, json=payload,
                                content_type="application/json")

    # /api/system/check
    def test_system_check_shape(self):
        r = self._get("/api/system/check")
        self.assertEqual(r.status_code, 200)
        d = r.get_json()
        for key in ("ffmpeg_ok", "app_version", "cpu_cores", "ram_total_gb",
                    "disk_free_gb", "python_version"):
            self.assertIn(key, d, f"Missing key '{key}' in /api/system/check")

    def test_system_check_ffmpeg_true(self):
        d = self._get("/api/system/check").get_json()
        self.assertTrue(d["ffmpeg_ok"], "ffmpeg_ok is False — FFmpeg not found?")

    # /api/validate
    def test_validate_missing_body_returns_400(self):
        r = self._post("/api/validate", {})
        self.assertEqual(r.status_code, 400)
        self.assertIn("error", r.get_json())

    def test_validate_valid_folders(self):
        r = self._post("/api/validate", {
            "image_folder":  IMG_DIR,
            "audio_folder":  AUD_DIR,
            "output_folder": OUT_DIR,
        })
        self.assertEqual(r.status_code, 200)
        d = r.get_json()
        self.assertIn("passed", d)
        self.assertIn("images", d)
        self.assertIn("audio",  d)

    def test_validate_bad_folder_returns_failure(self):
        r = self._post("/api/validate", {
            "image_folder":  "/nonexistent/images",
            "audio_folder":  "/nonexistent/audio",
            "output_folder": OUT_DIR,
        })
        self.assertEqual(r.status_code, 200)
        d = r.get_json()
        self.assertFalse(d["passed"])

    # /api/render/start
    def test_render_start_missing_config_returns_400(self):
        r = self._post("/api/render/start", {})
        self.assertEqual(r.status_code, 400)
        self.assertIn("error", r.get_json())

    def test_render_start_returns_job_id(self):
        r = self._post("/api/render/start", {
            "image_folder":  IMG_DIR,
            "audio_folder":  AUD_DIR,
            "output_folder": OUT_DIR,
            "project_name":  "tc04_api_start",
            "resolution":    "640x360",
            "fps":           30,
            "quality_preset": "fast",
        })
        self.assertEqual(r.status_code, 200)
        d = r.get_json()
        self.assertIn("job_id", d)
        self.assertEqual(d.get("status"), "started")

        # Clean up: cancel immediately so we don't leave a background render
        job_id = d["job_id"]
        time.sleep(0.2)
        self._post("/api/render/cancel", {"job_id": job_id})

    # /api/render/status & cancel
    def test_render_status_nonexistent_returns_404(self):
        r = self._get("/api/render/status/no-such-job-id-xyz")
        self.assertEqual(r.status_code, 404)

    def test_render_cancel_nonexistent_returns_404(self):
        r = self._post("/api/render/cancel", {"job_id": "no-such-job"})
        self.assertEqual(r.status_code, 404)

    # /api/history & /api/stats
    def test_history_returns_list(self):
        r = self._get("/api/history")
        self.assertEqual(r.status_code, 200)
        self.assertIsInstance(r.get_json(), list)

    def test_stats_shape(self):
        r = self._get("/api/stats")
        self.assertEqual(r.status_code, 200)
        d = r.get_json()
        for key in ("total_renders", "success_rate", "avg_render_time"):
            self.assertIn(key, d, f"Missing key '{key}' in /api/stats")

    # /api/browse/folder
    def test_browse_valid_folder(self):
        r = self._post("/api/browse/folder", {"path": OUT_DIR})
        self.assertEqual(r.status_code, 200)
        d = r.get_json()
        self.assertIn("current",  d)
        self.assertIn("contents", d)
        self.assertIsInstance(d["contents"], list)

    def test_browse_invalid_folder_returns_400(self):
        r = self._post("/api/browse/folder", {"path": "/nonexistent/path/xyz/abc"})
        self.assertEqual(r.status_code, 400)

    # /api/project
    def test_project_save_missing_name_returns_400(self):
        r = self._post("/api/project/save", {"settings": {"fps": 30}})
        self.assertEqual(r.status_code, 400)

    def test_project_list_returns_list(self):
        r = self._get("/api/project/list")
        self.assertEqual(r.status_code, 200)
        self.assertIsInstance(r.get_json(), list)

    def test_project_load_nonexistent_returns_404(self):
        r = self._post("/api/project/load", {"name": "nonexistent_xyz_project"})
        self.assertEqual(r.status_code, 404)

    def test_project_save_and_load_via_api(self):
        name = f"tc04_api_project_{int(time.time())}"
        settings = {"fps": 30, "resolution": "640x360", "quality_preset": "fast"}

        # Save
        r = self._post("/api/project/save", {"name": name, "settings": settings})
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.get_json().get("ok"))

        # Load
        r2 = self._post("/api/project/load", {"name": name})
        self.assertEqual(r2.status_code, 200)
        d = r2.get_json()
        self.assertEqual(d["settings"]["fps"], 30)

        # Delete
        r3 = self.client.delete(f"/api/project/{name}")
        self.assertEqual(r3.status_code, 200)


# ── TC-05: Project save/load cycle ────────────────────────────────────────────

class TestProjectCycle(unittest.TestCase):
    """TC-05 — ProjectManager save → load → compare settings (direct DB test)."""

    DB_PATH = os.path.join(OUT_DIR, "tc05_projects.db")

    @classmethod
    def setUpClass(cls):
        cls.pm = ProjectManager(db_path=cls.DB_PATH)
        cls.project_name = f"tc05_cycle_{int(time.time())}"
        cls.settings = {
            "image_folder":   IMG_DIR,
            "audio_folder":   AUD_DIR,
            "output_folder":  OUT_DIR,
            "resolution":     "1280x720",
            "fps":            30,
            "quality_preset": "fast",
            "effect_mode":    "fixed",
            "fixed_effect":   "zoom_pulse",
            "transition":     "fade_black",
            "normalize_audio": True,
            "bgm_volume":     0.15,
        }

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(cls.DB_PATH):
            os.remove(cls.DB_PATH)

    def test_save_returns_true(self):
        ok = self.pm.save_project(self.project_name, self.settings)
        self.assertTrue(ok, "save_project() returned False")

    def test_load_returns_record(self):
        self.pm.save_project(self.project_name, self.settings)
        loaded = self.pm.load_project(self.project_name)
        self.assertIsNotNone(loaded, "load_project() returned None")
        self.assertEqual(loaded["name"], self.project_name)

    def test_settings_roundtrip_exact(self):
        self.pm.save_project(self.project_name, self.settings)
        loaded = self.pm.load_project(self.project_name)
        loaded_settings = loaded["settings"]

        for key, expected in self.settings.items():
            self.assertEqual(
                loaded_settings.get(key),
                expected,
                f"Mismatch for '{key}': expected {expected!r}, "
                f"got {loaded_settings.get(key)!r}",
            )

    def test_list_contains_saved_project(self):
        self.pm.save_project(self.project_name, self.settings)
        names = [p["name"] for p in self.pm.list_projects()]
        self.assertIn(self.project_name, names)

    def test_upsert_updates_settings(self):
        self.pm.save_project(self.project_name, self.settings)
        updated = {**self.settings, "fps": 60, "resolution": "1920x1080"}
        self.pm.save_project(self.project_name, updated)
        loaded = self.pm.load_project(self.project_name)
        self.assertEqual(loaded["settings"]["fps"], 60)
        self.assertEqual(loaded["settings"]["resolution"], "1920x1080")

    def test_delete_removes_project(self):
        name = f"{self.project_name}_del"
        self.pm.save_project(name, self.settings)
        ok = self.pm.delete_project(name)
        self.assertTrue(ok, "delete_project() returned False")
        self.assertIsNone(
            self.pm.load_project(name),
            "Project still loadable after delete",
        )

    def test_load_nonexistent_returns_none(self):
        self.assertIsNone(self.pm.load_project("definitely_not_saved_xyz"))


# ── Report runner ─────────────────────────────────────────────────────────────

_TC_ORDER = [
    ("TestRenderPipeline", "TC-01: Full render pipeline"),
    ("TestSubtitleRender", "TC-02: Render với subtitle"),
    ("TestCancelRender",   "TC-03: Cancel render"),
    ("TestAPIEndpoints",   "TC-04: API endpoint tests"),
    ("TestProjectCycle",   "TC-05: Project save/load cycle"),
]

_TC_CLASSES = {
    "TestRenderPipeline": TestRenderPipeline,
    "TestSubtitleRender": TestSubtitleRender,
    "TestCancelRender":   TestCancelRender,
    "TestAPIEndpoints":   TestAPIEndpoints,
    "TestProjectCycle":   TestProjectCycle,
}


class _TrackingResult(unittest.TestResult):
    """Collects pass/fail counts per test-class name."""

    def __init__(self):
        super().__init__()
        self._groups: dict[str, dict] = {}

    def _g(self, test):
        name = type(test).__name__
        if name not in self._groups:
            self._groups[name] = {"passed": 0, "failed": 0, "messages": []}
        return self._groups[name]

    def addSuccess(self, test):
        self._g(test)["passed"] += 1

    def addFailure(self, test, err):
        super().addFailure(test, err)
        g = self._g(test)
        g["failed"] += 1
        g["messages"].append(f"    FAIL {test._testMethodName}: {err[1]}")

    def addError(self, test, err):
        super().addError(test, err)
        g = self._g(test)
        g["failed"] += 1
        g["messages"].append(f"    ERROR {test._testMethodName}: {err[1]}")

    def addSkip(self, test, reason):
        super().addSkip(test, reason)

    def tc_passed(self, cls_name: str) -> bool:
        g = self._groups.get(cls_name)
        return g is not None and g["failed"] == 0 and g["passed"] > 0


def _run_report():
    loader = unittest.TestLoader()
    suite  = unittest.TestSuite()
    for cls_name, _ in _TC_ORDER:
        suite.addTests(loader.loadTestsFromTestCase(_TC_CLASSES[cls_name]))

    result = _TrackingResult()
    t0     = time.time()
    suite.run(result)
    elapsed = round(time.time() - t0, 1)

    W = 54
    print("\n" + "═" * W)
    print("  MANGA STUDIO AI — Integration Test Report")
    print("═" * W)

    passed_count = 0
    for cls_name, label in _TC_ORDER:
        ok  = result.tc_passed(cls_name)
        tag = "PASS ✓" if ok else "FAIL ✗"
        print(f"  [{tag}]  {label}")
        for msg in result._groups.get(cls_name, {}).get("messages", []):
            print(msg)
        if ok:
            passed_count += 1

    print("─" * W)
    print(f"  Result  : {passed_count}/5 tests passed   ({elapsed}s total)")
    print("═" * W + "\n")

    return passed_count == 5


if __name__ == "__main__":
    _ensure_dirs()
    _write_test_srt()

    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--pytest", action="store_true",
                   help="Hand off to pytest instead of custom runner")
    args = p.parse_args()

    if args.pytest:
        import pytest
        sys.exit(pytest.main([__file__, "-v"]))
    else:
        success = _run_report()
        sys.exit(0 if success else 1)