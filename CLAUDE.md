# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Running the app

```bash
# Preferred launcher — checks FFmpeg, auto-selects a free port, opens browser
py run.py

# Direct (no port-selection, no browser auto-open)
py app.py

# Test mode (server only, no browser)
py run.py --test-mode
```

The app runs at `http://127.0.0.1:5000` (or the next free port). It auto-shuts down 120 s after the last browser disconnects (skipped while a render job is running).

## Install dependencies

```bash
pip install -r requirements.txt
```

**External dependency**: FFmpeg must be on `PATH`. On Windows: `winget install ffmpeg`.

## Architecture

### Entry points

- `run.py` — production launcher with FFmpeg check and port selection
- `app.py` — Flask + Flask-SocketIO server; all REST routes and WebSocket events live here
- `config.py` — global constants (codecs, bitrates, FPS, supported formats)

### Render pipeline (`core/video_processor.py`)

The main class is `VideoProcessor(config: RenderConfig)`. Render jobs are started in a background thread via `socketio.start_background_task`. Progress is pushed in real-time to the client via a SocketIO room named `job_{job_id}`.

Pipeline phases:
| Phase | Method | What it does |
|-------|--------|-------------|
| 0 | `prepare()` | Validate inputs, pair images+audio, build segment list |
| 1 | `render_all_segments_parallel()` | Encode each segment (image + audio → MP4) in parallel workers |
| 2 | `merge_segments()` | Join segments with xfade transitions or concat demuxer |
| 2b | `_inject_single_audio()` | Single-audio mode only: mux the shared audio track |
| 3 | `apply_watermark()`, `apply_subtitles()`, `add_intro_outro()` | Optional post-processing |
| 4 | `finalize()` | Optional BGM mix, final re-encode |

**Two audio modes** controlled by `RenderConfig`:
- **Folder-audio mode** (`audio_folder` set): each image is paired to an audio file with the same stem. Files **must** be named `001.jpg`/`001.mp3`, `002.jpg`/`002.mp3`, etc.
- **Single-audio mode** (`single_audio_file` set): one audio file shared across all images (any filename). Audio duration is divided equally; segments are video-only until Phase 2b.

**Scroll mode** (`scroll_mode=True`): instead of per-segment Ken Burns, images are stacked vertically into a PNG strip and panned top-to-bottom. Large strips are split into ≤14 000 px chunks to stay within FFmpeg's frame-size limit.

### Effects (`core/effect_engine.py`)

`EffectEngine.get_effect(name, duration, speed, scale)` returns a raw FFmpeg `filter_complex` string ending in `[out]`. The string is passed directly to `-filter_complex` in the segment encode command.

All effects use `zoompan` internally via `_zoompan(z_expr, x_expr, y_expr, ...)`.

- `scale=1.0` — image fills the full 1920×1080 frame with a blurred background behind letterbox bars
- `scale<1.0` — Ken Burns animates only the image; the blurred background fills the border at full frame

**Blur quality**: background uses `scale 2× → gblur sigma=60:steps=6 → scale back to 1920×1080`. The 2× upscale before blur is intentional — it eliminates the blocky square artifacts that appear with a single-pass box blur at output resolution.

### Transitions (`core/transition_engine.py`)

`TransitionEngine.choose_method(n)` returns `"xfade"` for ≤20 segments or `"concat_file"` for larger batches. xfade uses FFmpeg's `xfade` + `acrossfade` filter_complex; concat_file uses the demuxer (`-c copy`, no re-encode).

### Subtitles (`core/subtitle_engine.py`)

Converts SRT → styled ASS then burns via `ass=filename=...` filter. Falls back to `subtitles=` filter if ASS fails. Japanese/CJK text is auto-detected (`_has_cjk()`); when found, the font is overridden to Yu Gothic → Meiryo → MS Gothic (whichever is installed).

### Persistence (`core/project_manager.py`)

SQLite database at `database/history.db`. Two tables:
- `render_history` — timestamped log of every render attempt
- `saved_projects` — named project configs stored as JSON blobs (upsert on name)

### Validation (`core/validator.py`)

`SystemValidator` checks FFmpeg availability, folder contents, image/audio naming, file-stem matching, and disk space before any render starts. Folder-audio mode requires sequential `001`/`002`/... stems; single-audio mode accepts any filename.

## Key constants (`config.py`)

| Constant | Value |
|----------|-------|
| `DEFAULT_FPS` | 60 |
| `DEFAULT_RESOLUTION` | 1920×1080 |
| `DEFAULT_VIDEO_CODEC` | libx264 |
| `DEFAULT_VIDEO_BITRATE` | 8M |
| `EFFECT_SPEEDS` | slow=8s, normal=5s, fast=3s cycle |

## FFmpeg filter patterns

When editing filter strings, note:
- All `filter_complex` strings must end with `[out]` to be consumed by `-map [out]`
- `zoompan` `d=` parameter is total frames, not seconds
- Segment encodes use `-r 1` input (1 fps still image); zoompan's `on` counter increments at output FPS
- Windows: drive-letter colons must be escaped as `\:` inside FFmpeg filter option values (e.g. `C\:/path/file.ass`)
- Hardware encoder (nvenc/vaapi) is used for segment encodes; the finalize pass always uses libx264 to avoid nvenc pixel-format insertion errors
