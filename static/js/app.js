/* app.js — Manga Studio AI main logic
   Implements window.App which index.html calls directly.
   Inline scripts in index.html handle: clock, heatmap, theme, splash reveal.
*/

'use strict';

// ─── Helpers ─────────────────────────────────────────────────────────────────

function $(id)        { return document.getElementById(id); }
function setText(id, v){ const e = $(id); if (e) e.textContent = v ?? '—'; }
function show(id)     { const e = $(id); if (e) e.classList.remove('hidden'); }
function hide(id)     { const e = $(id); if (e) e.classList.add('hidden'); }

function showState(name) {
    document.querySelectorAll('.panel-state').forEach(el => el.classList.remove('active'));
    const el = $('state-' + name);
    if (el) el.classList.add('active');
}

function toast(msg, type = 'info') {
    const c = $('toast-container');
    if (!c) return;
    const t = document.createElement('div');
    t.className = `toast ${type}`;
    const icons = { success: '✅', error: '❌', warning: '⚠️', info: 'ℹ️' };
    t.innerHTML = `<span class="toast-icon">${icons[type] || 'ℹ️'}</span>
                   <div class="toast-body"><div class="toast-msg">${msg}</div></div>`;
    c.appendChild(t);
    setTimeout(() => t.remove(), 4000);
}

function fmtDur(secs) {
    if (!secs || secs <= 0) return '—';
    const m = Math.floor(secs / 60), s = Math.round(secs % 60);
    return m > 0 ? `${m}m ${s}s` : `${s}s`;
}

// Short duration formatter used in folder-change handlers (same logic, separate name)
function _fmtDur(secs) {
    if (!secs || secs <= 0) return '—';
    const h = Math.floor(secs / 3600);
    const m = Math.floor((secs % 3600) / 60);
    const s = Math.round(secs % 60);
    if (h > 0) return `${h}h ${m}m`;
    if (m > 0) return `${m}m ${s}s`;
    return `${s}s`;
}

// ─── Server lifecycle (browser ↔ CMD sync) ───────────────────────────────────

let _liveSocket       = null;   // persistent connection (separate from render socket)
let _disconnectedAt   = null;
let _shutdownShown    = false;

function _showShutdownOverlay(reason) {
    if (_shutdownShown) return;
    _shutdownShown = true;

    // Stop heartbeat / polling
    document.title = '⚠️ Server đã dừng';

    const el = document.createElement('div');
    el.style.cssText = [
        'position:fixed', 'inset:0', 'z-index:9999999',
        'background:rgba(5,5,15,.97)', 'backdrop-filter:blur(8px)',
        'display:flex', 'flex-direction:column',
        'align-items:center', 'justify-content:center', 'gap:20px',
        'color:#e0e0f8', 'font-family:system-ui,sans-serif',
    ].join(';');

    el.innerHTML = `
      <div style="font-size:3rem;line-height:1">⚠️</div>
      <div style="font-size:1.15rem;font-weight:700;color:#fff">Server Manga Studio AI đã dừng</div>
      <div style="font-size:.85rem;color:#8b8ba7;text-align:center;line-height:1.8">
        Tab sẽ tự đóng trong <strong id="_cd">3</strong> giây…<br>
        <span style="font-size:.75rem;color:#6b6b87">Khởi động lại: <code style="background:#1e1e30;padding:2px 6px;border-radius:4px">py run.py</code></span>
      </div>`;
    document.body.appendChild(el);

    let n = 3;
    const iv = setInterval(() => {
        const c = el.querySelector('#_cd');
        if (c) c.textContent = --n;
        if (n <= 0) {
            clearInterval(iv);
            window.close();
            // If browser blocks window.close() (tab not script-opened):
            setTimeout(() => {
                const c2 = el.querySelector('strong');
                if (c2) c2.closest('div').innerHTML = 'Vui lòng <strong>đóng tab này</strong> thủ công.';
            }, 500);
        }
    }, 1000);
}

function _initServerLifecycle() {
    if (typeof io === 'undefined') return;

    // Create a dedicated persistent socket for server events
    _liveSocket = io({
        transports: ['websocket'],
        reconnectionAttempts: 12,
        reconnectionDelay: 1500,
    });

    // Server sent explicit shutdown event (graceful Ctrl+C)
    _liveSocket.on('server_shutdown', d => {
        _showShutdownOverlay(d?.reason || '');
    });

    // Track connection state
    _liveSocket.on('connect',    () => { _disconnectedAt = null; });
    _liveSocket.on('disconnect', () => { if (!_disconnectedAt) _disconnectedAt = Date.now(); });

    // All reconnect attempts failed (server definitively dead)
    _liveSocket.on('reconnect_failed', () => _showShutdownOverlay('connection_failed'));

    // Check: if still disconnected after 45 s → server likely force-killed.
    // Don't trigger during rendering (temporary drops happen under heavy load).
    setInterval(() => {
        if (!_disconnectedAt) return;
        if (Date.now() - _disconnectedAt < 45_000) return;
        const isRendering = !!document.getElementById('state-rendering')?.classList.contains('active');
        if (isRendering) { _disconnectedAt = Date.now(); return; }  // reset clock
        _showShutdownOverlay('connection_lost');
    }, 5_000);

    // Send heartbeat every 8 s (backup mechanism)
    setInterval(() => {
        fetch('/api/heartbeat', { method: 'POST', keepalive: true }).catch(() => {});
    }, 8_000);
}

// ─── JS Tooltip (position:fixed — not clipped by sidebar overflow) ───────────

function _initTooltip() {
    const tip = document.createElement('div');
    tip.id = 'js-tooltip';
    document.body.appendChild(tip);

    function show(el) {
        tip.textContent = el.dataset.tooltip || '';
        if (!tip.textContent) return;
        tip.classList.add('visible');
        const r   = el.getBoundingClientRect();
        const tw  = 240;
        const th  = tip.offsetHeight || 60;
        let   lft = r.left + r.width / 2 - tw / 2;
        let   top = r.top  - th - 10;
        if (top < 8)                        top = r.bottom + 8;
        if (lft < 8)                        lft = 8;
        if (lft + tw > window.innerWidth - 8) lft = window.innerWidth - tw - 8;
        tip.style.left  = lft + 'px';
        tip.style.width = tw  + 'px';
        tip.style.top   = top + 'px';
    }
    function hide() { tip.classList.remove('visible'); }

    // Attach to all current + future info-icons via delegation
    document.addEventListener('mouseover', e => {
        const el = e.target.closest('.info-icon[data-tooltip]');
        if (el) show(el);
    });
    document.addEventListener('mouseout', e => {
        if (e.target.closest('.info-icon[data-tooltip]')) hide();
    });
    // Also handle [data-tooltip] on non-info-icon elements (if any)
    document.addEventListener('mouseover', e => {
        const el = e.target.closest('[data-tooltip]:not(.info-icon)');
        if (el) show(el);
    });
    document.addEventListener('mouseout', e => {
        if (e.target.closest('[data-tooltip]:not(.info-icon)')) hide();
    });
}

// ─── Realtime metrics polling ─────────────────────────────────────────────────

async function pollMetrics() {
    try {
        const d = await fetch('/api/system/metrics').then(r => r.json());
        if (d.error) return;

        const color = (val, warnAt, errAt) =>
            val >= errAt  ? 'var(--error)'   :
            val >= warnAt ? 'var(--warning)' : 'var(--success)';

        const cpu = $('sys-cpu');
        if (cpu) {
            cpu.textContent = d.cpu_percent + '%';
            cpu.style.color = color(d.cpu_percent, 70, 90);
        }
        const ram = $('sys-ram');
        if (ram) {
            ram.textContent = d.ram_used_gb + '/' + d.ram_total_gb + 'GB';
            ram.style.color = color(d.ram_percent, 75, 90);
        }
        const disk = $('sys-disk');
        if (disk) {
            disk.textContent = d.disk_free_gb.toFixed(1) + 'GB free';
            disk.style.color = d.disk_free_gb < 5 ? 'var(--error)' :
                               d.disk_free_gb < 20 ? 'var(--warning)' : 'var(--success)';
        }
    } catch (_) {}
}

// ─── System info ──────────────────────────────────────────────────────────────

async function loadSystemInfo() {
    try {
        const d = await fetch('/api/system/check').then(r => r.json());
        // footer status pills
        const dotF = $('dot-ffmpeg'), lblF = $('label-ffmpeg');
        if (dotF) dotF.className = 'status-dot ' + (d.ffmpeg_ok ? 'ok' : 'error');
        if (lblF) lblF.textContent = d.ffmpeg_ok ? 'FFmpeg OK' : 'FFmpeg ✗';

        const dotG = $('dot-gpu'), lblG = $('label-gpu');
        const hw = d.hw_encoder || 'cpu';
        if (dotG) dotG.className = 'status-dot ' + (hw !== 'cpu' ? 'ok' : '');
        if (lblG) lblG.textContent = hw === 'cpu' ? 'CPU only' : hw.toUpperCase();

        const lblPy = $('label-python');
        if (lblPy) lblPy.textContent = 'Python ' + (d.python_version || '');

        // sys-bar (right-bottom main panel)
        setText('sys-ffmpeg-ver', d.ffmpeg_version || '—');
        setText('sys-encoder',    hw.toUpperCase());
        setText('sys-cpu',        (d.cpu_cores || '—') + ' cores');
        setText('sys-ram',        d.ram_total_gb ? d.ram_total_gb + ' GB' : '—');
        setText('sys-disk',       d.disk_free_gb  ? d.disk_free_gb  + ' GB free' : '—');
        setText('sys-python',     d.python_version || '—');
    } catch (e) {
        console.warn('system/check failed:', e.message);
    }
}

async function loadStats() {
    try {
        const d = await fetch('/api/stats').then(r => r.json());
        setText('card-segments', d.total_renders ?? '0');
        setText('card-duration',  d.total_duration_hours != null ? d.total_duration_hours + 'h' : '—');
        setText('card-size',      '—');
    } catch (_) {}
}

// ─── File Picker (for intro/outro/srt) ───────────────────────────────────────

const FilePicker = (() => {
    let _targetField = null;
    let _allowedExts  = [];
    let _currentDir   = '';

    const ICON_FOLDER = `<svg viewBox="0 0 24 24" style="width:15px;height:15px;fill:var(--accent-light);flex-shrink:0"><path d="M10 4H4c-1.1 0-2 .9-2 2v12c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V8c0-1.1-.9-2-2-2h-8l-2-2z"/></svg>`;
    const ICON_FILE   = `<svg viewBox="0 0 24 24" style="width:15px;height:15px;fill:var(--text-secondary);flex-shrink:0"><path d="M14 2H6c-1.1 0-2 .9-2 2v16c0 1.1.9 2 2 2h12c1.1 0 2-.9 2-2V8l-6-6zm2 16H8v-2h8v2zm0-4H8v-2h8v2zm-3-5V3.5L18.5 9H13z"/></svg>`;
    const ICON_UP     = `<svg viewBox="0 0 24 24" style="width:15px;height:15px;fill:var(--text-muted);flex-shrink:0"><path d="M20 11H7.83l5.59-5.59L12 4l-8 8 8 8 1.41-1.41L7.83 13H20v-2z"/></svg>`;

    async function _load(path) {
        const list = $('file-list');
        if (list) list.innerHTML = '<div style="padding:16px;text-align:center;color:var(--text-muted);font-size:.8rem">Đang tải…</div>';
        try {
            const d = await fetch('/api/browse/folder', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ path: path ?? '', include_files: true, file_extensions: _allowedExts }),
            }).then(r => r.json());

            if (d.error) { toast(d.error, 'error'); return; }
            _currentDir = d.is_root ? '' : (d.current || '');
            _render(d);
        } catch (e) {
            toast('Lỗi duyệt file: ' + e.message, 'error');
        }
    }

    function _render(d) {
        const isRoot = !!d.is_root;
        const crumb  = $('file-modal-breadcrumb');
        if (crumb) crumb.textContent = isRoot ? '💻 Chọn ổ đĩa' : (_currentDir || '/');

        const list = $('file-list');
        if (!list) return;
        list.innerHTML = '';

        // Manual path input
        const bar = document.createElement('div');
        bar.style.cssText = 'display:flex;gap:6px;padding:4px 4px 8px;border-bottom:1px solid var(--border);margin-bottom:6px;';
        const inp = document.createElement('input');
        inp.type = 'text'; inp.className = 'field-input';
        inp.placeholder = 'Nhập đường dẫn thủ công…';
        inp.value = isRoot ? '' : (_currentDir || '');
        inp.style.cssText = 'font-size:.8rem;height:30px;padding:4px 8px;';
        const goBtn = document.createElement('div');
        goBtn.className = 'btn-sm'; goBtn.textContent = '→'; goBtn.style.flexShrink = '0';
        goBtn.onclick = () => { if (inp.value.trim()) _load(inp.value.trim()); };
        inp.addEventListener('keydown', e => { if (e.key === 'Enter') goBtn.onclick(); });
        bar.appendChild(inp); bar.appendChild(goBtn);
        list.appendChild(bar);

        // Up button
        if (d.parent != null) {
            const up = document.createElement('div');
            up.className = 'folder-item folder-item-up';
            up.innerHTML = `${ICON_UP} ${d.parent === 'root' ? '← Danh sách ổ đĩa' : '← Lên trên'}`;
            up.onclick = () => _load(d.parent);
            list.appendChild(up);
        }

        // Contents
        (d.contents || []).forEach(item => {
            const el = document.createElement('div');
            if (item.is_dir) {
                el.className = 'folder-item';
                el.innerHTML = `${ICON_FOLDER} ${item.name}`;
                el.onclick   = () => _load(item.path);
            } else {
                el.className = 'folder-item';
                el.style.cssText = 'color:var(--text-primary);';
                el.innerHTML = `${ICON_FILE} <strong>${item.name}</strong>`;
                el.onclick = () => {
                    // Select file
                    const inp2 = $(_targetField);
                    if (inp2) inp2.value = item.path;
                    const label = $('file-selected-path');
                    if (label) label.textContent = item.name;
                    _close();
                };
            }
            list.appendChild(el);
        });
    }

    function _close() {
        const m = $('file-modal-backdrop');
        if (m) m.classList.add('hidden');
    }

    return {
        open(fieldId, exts) {
            _targetField  = fieldId;
            _allowedExts  = exts || [];
            const existing = $(fieldId)?.value?.trim() || '';
            const startDir  = existing.includes('/') || existing.includes('\\')
                ? existing.substring(0, Math.max(existing.lastIndexOf('/'), existing.lastIndexOf('\\')))
                : '';
            _load(startDir || '');
            const title = $('file-modal-title');
            if (title) title.textContent = exts?.includes('.srt') ? 'Chọn file SRT' : 'Chọn file video';
            const m = $('file-modal-backdrop');
            if (m) m.classList.remove('hidden');
        },
        close: _close,
    };
})();

// ─── Folder picker ────────────────────────────────────────────────────────────

const FolderPicker = (() => {
    let _targetField = null;
    let _currentPath = '';   // '' means "root / drive list"

    // ── SVG icons ──
    const ICON_DRIVE  = `<svg viewBox="0 0 24 24" style="width:16px;height:16px;fill:var(--accent-light);flex-shrink:0"><path d="M6 2h12l3 6v10a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8l3-6zm0 6h12M5 12h.01M8 12h8"/><rect x="3" y="8" width="18" height="12" rx="1" fill="none" stroke="currentColor" stroke-width="1.5"/><circle cx="7" cy="14" r="1" fill="var(--accent-light)"/></svg>`;
    const ICON_FOLDER = `<svg viewBox="0 0 24 24" style="width:16px;height:16px;fill:var(--accent-light);flex-shrink:0"><path d="M10 4H4c-1.1 0-2 .9-2 2v12c0 1.1.9 2 2 2h16c1.1 0 2-.9 2-2V8c0-1.1-.9-2-2-2h-8l-2-2z"/></svg>`;
    const ICON_UP     = `<svg viewBox="0 0 24 24" style="width:16px;height:16px;fill:var(--text-muted);flex-shrink:0"><path d="M20 11H7.83l5.59-5.59L12 4l-8 8 8 8 1.41-1.41L7.83 13H20v-2z"/></svg>`;

    async function _load(path) {
        const list = $('folder-list');
        if (list) list.innerHTML = '<div style="padding:16px;text-align:center;color:var(--text-muted);font-size:.8rem">Đang tải…</div>';
        try {
            const d = await fetch('/api/browse/folder', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ path: path ?? '' }),
            }).then(r => r.json());

            if (d.error) { toast(d.error, 'error'); return; }
            _currentPath = d.is_root ? '' : (d.current || '');
            _render(d);
        } catch (e) {
            toast('Lỗi duyệt thư mục: ' + e.message, 'error');
        }
    }

    function _render(d) {
        const isRoot = !!d.is_root;

        // Breadcrumb label
        const crumb = $('modal-breadcrumb');
        if (crumb) crumb.textContent = isRoot ? '💻  Chọn ổ đĩa' : (_currentPath || '/');

        // Footer path + confirm button label
        const footPath = $('modal-current-path');
        if (footPath) footPath.textContent = isRoot ? '(chưa chọn)' : (_currentPath || '/');

        const list = $('folder-list');
        if (!list) return;
        list.innerHTML = '';

        // ── Manual path input ──
        const bar = document.createElement('div');
        bar.style.cssText = 'display:flex;gap:6px;padding:4px 4px 8px;border-bottom:1px solid var(--border);margin-bottom:6px;';
        const inp = document.createElement('input');
        inp.type = 'text';
        inp.className = 'field-input';
        inp.placeholder = 'Nhập đường dẫn trực tiếp…';
        inp.value = isRoot ? '' : (_currentPath || '');
        inp.style.cssText = 'font-size:.8rem;height:30px;padding:4px 8px;';
        const goBtn = document.createElement('div');
        goBtn.className = 'btn-sm';
        goBtn.textContent = '→ Đi';
        goBtn.style.cssText = 'flex-shrink:0;height:30px;';
        goBtn.onclick = () => { const v = inp.value.trim(); if (v) _load(v); };
        inp.addEventListener('keydown', e => { if (e.key === 'Enter') goBtn.onclick(); });
        bar.appendChild(inp);
        bar.appendChild(goBtn);
        list.appendChild(bar);

        // ── Up / back button ──
        if (d.parent != null) {
            const up = document.createElement('div');
            up.className = 'folder-item folder-item-up';
            const label = d.parent === 'root' ? '← Danh sách ổ đĩa' : '← Thư mục trên';
            up.innerHTML = `${ICON_UP} ${label}`;
            up.onclick = () => _load(d.parent);
            list.appendChild(up);
        }

        // ── Drive / folder items ──
        if (!d.contents?.length) {
            const empty = document.createElement('div');
            empty.style.cssText = 'padding:20px;text-align:center;color:var(--text-muted);font-size:.8rem;';
            empty.textContent = isRoot ? 'Không tìm thấy ổ đĩa.' : 'Thư mục trống.';
            list.appendChild(empty);
            return;
        }

        d.contents.forEach(item => {
            const el = document.createElement('div');
            el.className = 'folder-item';
            const icon = isRoot ? ICON_DRIVE : ICON_FOLDER;
            // Show drive letter in larger text
            const label = isRoot
                ? `<span style="font-weight:600;font-size:.9rem">${item.name}</span>`
                : item.name;
            el.innerHTML = `${icon}${label}`;
            el.onclick = () => _load(item.path);
            list.appendChild(el);
        });
    }

    return {
        open(fieldId) {
            _targetField = fieldId;
            const existing = $(fieldId)?.value?.trim() || '';
            // Start at existing path if valid, otherwise show drive list
            _load(existing || '');
            const modal = $('folder-modal-backdrop');
            if (modal) modal.classList.remove('hidden');
        },

        confirm() {
            if (_targetField && _currentPath) {
                const inp = $(_targetField);
                if (inp) inp.value = _currentPath;
            }
            this.close();
            // Trigger the specific handler so validation + card updates run
            if      (_targetField === 'image-folder')  App.onImageFolderChange();
            else if (_targetField === 'audio-folder')  App.onAudioFolderChange();
            else                                       App.validateFolders();
        },

        close() {
            const modal = $('folder-modal-backdrop');
            if (modal) modal.classList.add('hidden');
        },
    };
})();

// ─── Validation ───────────────────────────────────────────────────────────────

let _valTimer = null;
async function _runValidate() {
    const img = $('image-folder')?.value || '';
    const aud = $('audio-folder')?.value  || '';
    const out = $('output-folder')?.value || '';
    if (!img || !aud || !out) return;
    try {
        const d = await fetch('/api/validate', {
            method: 'POST', headers: {'Content-Type':'application/json'},
            body: JSON.stringify({ image_folder: img, audio_folder: aud, output_folder: out }),
        }).then(r => r.json());
        const ok = d.passed ?? false;
        const btn = $('btn-render');
        if (btn) btn.classList.toggle('disabled', !ok);
    } catch (_) {}
}

// ─── Render / progress ────────────────────────────────────────────────────────

let _socket    = null;
let _jobId     = null;
let _startTime = null;
let _timerIv   = null;
let _autoscroll = true;

function _startTimer() {
    _startTime = Date.now();
    clearInterval(_timerIv);
    _timerIv = setInterval(() => {
        const e = Math.floor((Date.now() - _startTime) / 1000);
        const mm = String(Math.floor(e / 60)).padStart(2, '0');
        const ss = String(e % 60).padStart(2, '0');
        setText('render-elapsed', mm + ':' + ss);
    }, 1000);
}

function _stopTimer() { clearInterval(_timerIv); _timerIv = null; }

function addLog(msg, level = 'info') {
    const body = $('log-body');
    if (!body) return;
    const ts = new Date().toLocaleTimeString('en-GB', { hour12: false });
    const line = document.createElement('div');
    line.className = `log-line ${level}`;
    line.innerHTML = `<span class="log-ts">[${ts}]</span>${msg.replace(/</g,'&lt;')}`;
    body.appendChild(line);
    if (_autoscroll) body.scrollTop = body.scrollHeight;
}

function onProgress(data) {
    const progress = data.overall_progress || 0;
    const pct = Math.round(progress * 100);
    setText('progress-pct', pct + '%');
    const fill = $('progress-fill');
    if (fill) fill.style.width = pct + '%';
    if (data.current_phase) setText('phase-badge', _phaseLabel(data.current_phase));
    if (data.current_phase) setText('progress-phase-label', _phaseLabel(data.current_phase));

    // Segment counter
    const total = data.total_segments || 0;
    const done  = data.completed_segments || 0;
    if (total > 0) {
        setText('render-seg-total', total);
        setText('render-seg-done',  done);
        setText('render-segments-label', done + ' / ' + total + ' phân đoạn');
    }

    // ETA — only meaningful after some time has elapsed and work is progressing
    if (_startTime) {
        const elapsed = (Date.now() - _startTime) / 1000;
        let remaining = -1;
        if (data.current_phase === 'rendering' && total > 0 && done > 0 && elapsed > 3) {
            // throughput: segments per second → estimate time for remaining segments
            remaining = Math.round((total - done) / (done / elapsed));
        } else if (progress > 0.06 && elapsed > 3) {
            // fallback: linear extrapolation from overall_progress
            remaining = Math.round(elapsed * (1 - progress) / progress);
        }
        if (remaining > 0 && remaining < 86400) {
            const em = Math.floor(remaining / 60);
            const es = remaining % 60;
            setText('render-eta', String(em).padStart(2, '0') + ':' + String(es).padStart(2, '0'));
        }
    }

    document.title = `[${pct}%] Manga Studio AI`;
}

function _phaseLabel(phase) {
    return { preparing: 'Chuẩn bị', rendering: 'Đang render', merging: 'Ghép video', finalizing: 'Hoàn thiện' }[phase] || phase;
}

function onComplete(result) {
    _stopTimer();
    const fill = $('progress-fill');
    if (fill) { fill.style.width = '100%'; fill.classList.add('done'); }
    setText('progress-pct', '100%');
    setText('phase-badge', 'Hoàn tất');
    addLog('✅ Render hoàn tất!', 'success');
    document.title = 'Manga Studio AI ✓';
    setTimeout(() => { document.title = 'Manga Studio AI'; }, 8000);
    // Re-enable render button
    const rbtn = $('btn-render');
    if (rbtn) rbtn.classList.remove('disabled');

    // Notification
    if ('Notification' in window && Notification.permission === 'granted') {
        new Notification('Manga Studio AI', { body: 'Render hoàn tất! 🎉' });
    }

    // Show complete state
    const r = result.result || result;
    setText('out-duration',    fmtDur(r.duration));
    setText('out-size',        r.file_size_mb ? r.file_size_mb + ' MB' : '—');
    setText('out-segments',    r.segment_count ?? '—');
    setText('out-render-time', r.render_time   ? r.render_time + 's' : '—');
    setText('out-path',        r.output_path   || '—');
    setText('success-sub',     r.output_path   ? 'Đã lưu: ' + r.output_path : 'Video đã sẵn sàng.');

    showState('complete');
    loadStats();
}

function onError(data) {
    _stopTimer();
    addLog('❌ Lỗi: ' + (data.message || data.error || 'Unknown'), 'error');
    toast('Render thất bại: ' + (data.message || data.error || ''), 'error');
    document.title = 'Manga Studio AI';
    showState('idle');
    const rbtn = $('btn-render');
    if (rbtn) rbtn.classList.remove('disabled');
}

function _connectSocket(jobId) {
    if (typeof io === 'undefined') { console.warn('Socket.IO not loaded'); return; }
    // Reuse existing socket — never disconnect (would kill the lifecycle socket too).
    // On subsequent renders just remove stale handlers then re-subscribe.
    if (!_socket) {
        _socket = io({ transports: ['websocket'] });
    }
    _socket.off('render_progress');
    _socket.off('render_log');
    _socket.off('render_complete');
    _socket.off('render_error');
    _socket.emit('subscribe_job', { job_id: jobId });
    _socket.on('render_progress', onProgress);
    _socket.on('render_log',      d => addLog(d.message || '', d.level || 'info'));
    _socket.on('render_complete', onComplete);
    _socket.on('render_error',    onError);
}

// ─── App namespace (called by index.html) ─────────────────────────────────────

const App = {

    // ── Folder browsing ──────────────────────────────────────
    browseFolder(fieldId) { FolderPicker.open(fieldId); },
    closeFolderModal()    { FolderPicker.close(); },
    selectCurrentFolder() { FolderPicker.confirm(); },

    // ── File browsing (intro/outro/srt) ──────────────────────
    browseFile(fieldId, exts) { FilePicker.open(fieldId, exts || []); },
    closeFileModal()  { FilePicker.close(); },
    browseSRT()       { FilePicker.open('srt-path', ['.srt']); },
    browseVideo(fieldId) { FilePicker.open(fieldId, ['.mp4','.avi','.mov','.mkv']); },

    // ── Image folder change ───────────────────────────────────
    async onImageFolderChange() {
        this.validateFolders();
        const path = $('image-folder')?.value?.trim() || '';
        const msg  = $('img-folder-msg');
        const _set = (t, c) => { if (msg) { msg.innerHTML = t; msg.className = `folder-msg ${c}`; } };

        if (!path) { _set('', ''); return; }
        _set('⏳ Kiểm tra…', '');
        try {
            const d = await fetch('/api/validate/folder', {
                method: 'POST', headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ path, type: 'image' }),
            }).then(r => r.json());

            if (d.error) { _set(`❌ ${d.error}`, 'err'); return; }
            const cnt = d.count ?? 0;

            if (d.ok && cnt > 0) {
                _set(`✅ ${cnt} ảnh hợp lệ`, 'ok');
                setText('card-total-images', cnt);
                setText('card-segments', cnt);    // seed until audio matched
            } else if (cnt > 0) {
                _set(`⚠️ ${(d.errors||[])[0] || 'Tên file cần định dạng 001, 002…'}`, 'warn');
            } else {
                _set(`❌ ${(d.errors||[])[0] || 'Không tìm thấy ảnh (.jpg .png .webp)'}`, 'err');
            }

            // If audio already selected, re-run cross-check to update matched count
            if (d.ok && cnt > 0 && $('audio-folder')?.value?.trim()) {
                this.onAudioFolderChange();
            }
        } catch { _set('❌ Lỗi kết nối', 'err'); }
    },

    // ── Audio folder change ───────────────────────────────────
    async onAudioFolderChange() {
        this.validateFolders();
        const path    = $('audio-folder')?.value?.trim() || '';
        const imgPath = $('image-folder')?.value?.trim() || '';
        const outPath = $('output-folder')?.value?.trim() || 'D:/';
        const msg     = $('aud-folder-msg');
        const _set    = (t, c) => { if (msg) { msg.innerHTML = t; msg.className = `folder-msg ${c}`; } };

        if (!path) { _set('', ''); return; }
        _set('⏳ Đang phân tích audio…', '');
        try {
            // Get real durations via ffprobe
            const da = await fetch('/api/audio/analyze', {
                method: 'POST', headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ path }),
            }).then(r => r.json());

            if (da.error) { _set(`❌ ${da.error}`, 'err'); return; }
            const audCnt = da.count ?? 0;
            const totalS = da.total_duration ?? 0;

            if (!da.ok || audCnt === 0) {
                _set(`❌ ${(da.errors||[])[0] || 'Không tìm thấy audio (.mp3 .wav .m4a)'}`, 'err');
                return;
            }

            const durStr = _fmtDur(totalS);

            // Cross-check with image folder
            if (imgPath) {
                const dv = await fetch('/api/validate', {
                    method: 'POST', headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ image_folder: imgPath, audio_folder: path, output_folder: outPath }),
                }).then(r => r.json());

                const matched = (dv.matching?.matched || []).length;
                if (matched > 0) {
                    _set(`✅ ${audCnt} audio · <strong>${matched}</strong> phân đoạn · ⏱ ${durStr}`, 'ok');
                    setText('card-segments', matched);
                    setText('card-duration', durStr);
                    setText('card-size', dv.estimated_output_mb > 0 ? dv.estimated_output_mb + ' MB' : '—');
                } else {
                    _set(`⚠️ ${audCnt} audio (${durStr}) — tên không khớp với ảnh`, 'warn');
                    setText('card-duration', durStr);
                }
            } else {
                _set(`✅ ${audCnt} file audio · ⏱ ${durStr}`, 'ok');
                setText('card-duration', durStr);
                setText('card-segments', audCnt);
            }
        } catch { _set('❌ Lỗi kết nối', 'err'); }
    },

    // Validation
    validateFolders() {
        clearTimeout(_valTimer);
        _valTimer = setTimeout(_runValidate, 400);
    },

    // Render lifecycle
    async startRender() {
        // Effect mode
        const effectRandom  = $('effect-random')?.checked ?? true;
        const activeEffect  = document.querySelector('.fx-btn.active[data-effect]');
        // Transition mode
        const transRandom   = $('transition-random')?.checked ?? true;
        const activeTrans   = document.querySelector('.fx-btn.active[data-transition]');
        const transOptions  = ['fade_black','fade_white','cross_dissolve','slide_left','slide_right','zoom_transition'];
        // Subtitle
        const subEnabled    = $('subtitle-enabled')?.checked ?? false;
        const activeSubStyle = document.querySelector('.sub-style-btn.active');

        const cfg = {
            image_folder:       $('image-folder')?.value  || '',
            audio_folder:       $('audio-folder')?.value   || '',
            output_folder:      $('output-folder')?.value  || '',
            project_name:       $('project-name')?.value   || 'output',
            resolution:         $('resolution')?.value     || '1920x1080',
            fps:                parseInt($('fps')?.value)   || 60,
            quality_preset:     $('quality')?.value        || 'balanced',
            effect_mode:        effectRandom ? 'random' : 'fixed',
            fixed_effect:       activeEffect?.dataset.effect || 'zoom_pulse',
            effect_speed:       $('effect-speed')?.value   || 'normal',
            transition:         transRandom
                                    ? transOptions[Math.floor(Math.random() * transOptions.length)]
                                    : (activeTrans?.dataset.transition || 'fade_black'),
            transition_duration: parseFloat($('transition-duration')?.value) || 0.5,
            subtitle_preset:    subEnabled ? (activeSubStyle?.dataset.style || 'youtube_classic') : 'none',
            subtitle_srt_path:  subEnabled ? ($('srt-path')?.value || null) : null,
            normalize_audio:    $('normalize-audio')?.checked ?? true,
            audio_fade:         parseFloat($('audio-fade-in')?.value) || 0.3,
            intro_path:         $('intro-path')?.value     || null,
            outro_path:         $('outro-path')?.value     || null,
            video_bitrate:      $('video-bitrate')?.value  || '8M',
            audio_bitrate:      $('audio-bitrate')?.value  || '192k',
        };

        if (!cfg.image_folder || !cfg.audio_folder || !cfg.output_folder) {
            toast('Vui lòng chọn đủ 3 thư mục trước khi render.', 'warning');
            return;
        }

        try {
            const d = await fetch('/api/render/start', {
                method: 'POST', headers: {'Content-Type':'application/json'},
                body: JSON.stringify(cfg),
            }).then(r => r.json());

            if (!d.job_id) throw new Error(d.error || 'Không nhận được job_id');
            _jobId = d.job_id;

            // Disable render button while running
            const rbtn = $('btn-render');
            if (rbtn) rbtn.classList.add('disabled');

            // Reset UI
            const fill = $('progress-fill');
            if (fill) { fill.style.width = '0%'; fill.classList.remove('done'); }
            setText('progress-pct', '0%');
            setText('phase-badge', 'Chuẩn bị…');
            setText('render-elapsed', '00:00');
            setText('render-eta', '--:--');
            setText('render-seg-done',  '0');
            setText('render-seg-total', '—');
            setText('render-segments-label', '0 / — phân đoạn');
            setText('render-project-name', cfg.project_name);
            const lb = $('log-body'); if (lb) lb.innerHTML = '';

            showState('rendering');
            _startTimer();
            _connectSocket(_jobId);
            addLog('Render khởi động — job: ' + _jobId, 'info');
        } catch (e) {
            toast('Không thể bắt đầu render: ' + e.message, 'error');
        }
    },

    async pauseRender() {
        if (!_jobId) { toast('Không có render đang chạy.', 'warning'); return; }
        try {
            const r = await fetch('/api/render/pause', {
                method: 'POST', headers: {'Content-Type':'application/json'},
                body: JSON.stringify({ job_id: _jobId }),
            }).then(res => res.json());
            if (r.error) throw new Error(r.error);
            show('btn-resume'); hide('btn-pause');
            addLog('Render tạm dừng.', 'warning');
        } catch (e) {
            toast('Lỗi tạm dừng: ' + e.message, 'error');
        }
    },

    async resumeRender() {
        if (!_jobId) return;
        try {
            const r = await fetch('/api/render/resume', {
                method: 'POST', headers: {'Content-Type':'application/json'},
                body: JSON.stringify({ job_id: _jobId }),
            }).then(res => res.json());
            if (r.error) throw new Error(r.error);
            hide('btn-resume'); show('btn-pause');
            addLog('Render tiếp tục.', 'info');
        } catch (e) {
            toast('Lỗi tiếp tục render: ' + e.message, 'error');
        }
    },

    async cancelRender() {
        if (!_jobId) { toast('Không có render đang chạy.', 'warning'); return; }
        if (!confirm('Hủy render hiện tại?')) return;
        try {
            await fetch('/api/render/cancel', {
                method: 'POST', headers: {'Content-Type':'application/json'},
                body: JSON.stringify({ job_id: _jobId }),
            });
        } catch (e) {
            toast('Lỗi gửi lệnh hủy: ' + e.message, 'error');
        }
        _stopTimer();
        addLog('Render đã hủy.', 'warning');
        toast('Đã hủy render.', 'warning');
        document.title = 'Manga Studio AI';
        const rbtn = $('btn-render'); if (rbtn) rbtn.classList.remove('disabled');
        showState('idle');
        _jobId = null;
    },

    newRender() {
        _jobId = null;
        showState('idle');
        document.title = 'Manga Studio AI';
    },

    // Log
    clearLog() {
        const b = $('log-body'); if (b) b.innerHTML = '';
    },
    toggleAutoscroll() {
        _autoscroll = !_autoscroll;
        const btn = $('btn-autoscroll');
        if (btn) btn.textContent = _autoscroll ? 'Auto ↓' : 'Auto ○';
    },

    // Output actions
    copyOutputPath() {
        const v = $('out-path')?.textContent;
        if (v && v !== '—') {
            navigator.clipboard.writeText(v).then(() => toast('Đã sao chép đường dẫn.', 'success'));
        }
    },
    openOutputFolder() {
        toast('Mở thư mục không được hỗ trợ trong trình duyệt.', 'info');
    },

    // Project management
    async saveProject() {
        const name = prompt('Tên dự án:');
        if (!name?.trim()) return;
        const settings = {
            image_folder:  $('image-folder')?.value,
            audio_folder:  $('audio-folder')?.value,
            output_folder: $('output-folder')?.value,
            project_name:  $('project-name')?.value,
            resolution:    $('resolution')?.value,
            fps:           $('fps')?.value,
            quality:       $('quality')?.value,
        };
        try {
            const d = await fetch('/api/project/save', {
                method: 'POST', headers: {'Content-Type':'application/json'},
                body: JSON.stringify({ name: name.trim(), settings }),
            }).then(r => r.json());
            toast(d.ok ? `Đã lưu dự án "${name}"` : (d.error || 'Lỗi lưu'), d.ok ? 'success' : 'error');
        } catch (e) { toast('Lỗi: ' + e.message, 'error'); }
    },

    async showProjectList() {
        try {
            const d = await fetch('/api/project/list').then(r => r.json());
            const list = $('project-list');
            if (!list) return;
            list.innerHTML = '';
            if (!d.length) {
                list.innerHTML = '<div style="padding:20px;text-align:center;color:var(--text-muted)">Chưa có dự án nào</div>';
            } else {
                d.forEach(p => {
                    const item = document.createElement('div');
                    item.className = 'project-item';
                    item.innerHTML = `<span>${p.name}</span><span class="project-date">${(p.updated_at || '').slice(0, 10)}</span>`;
                    item.onclick = () => App._loadProject(p.name);
                    list.appendChild(item);
                });
            }
            const modal = $('project-modal-backdrop');
            if (modal) modal.classList.remove('hidden');
        } catch (e) { toast('Lỗi tải danh sách dự án: ' + e.message, 'error'); }
    },

    closeProjectModal() {
        const modal = $('project-modal-backdrop');
        if (modal) modal.classList.add('hidden');
    },

    async _loadProject(name) {
        try {
            const d = await fetch('/api/project/load', {
                method: 'POST', headers: {'Content-Type':'application/json'},
                body: JSON.stringify({ name }),
            }).then(r => r.json());
            const s = d.settings || {};
            const set = (id, v) => { const e = $(id); if (e && v != null) e.value = v; };
            set('image-folder',  s.image_folder);
            set('audio-folder',  s.audio_folder);
            set('output-folder', s.output_folder);
            set('project-name',  s.project_name || name);
            set('resolution',    s.resolution);
            set('fps',           s.fps);
            set('quality',       s.quality);
            this.closeProjectModal();
            this.validateFolders();
            toast(`Đã tải dự án "${name}"`, 'success');
        } catch (e) { toast('Lỗi tải dự án: ' + e.message, 'error'); }
    },

    // History
    async loadHistory() {
        try {
            const data = await fetch('/api/history?limit=50').then(r => r.json());
            const list  = $('history-list');
            const empty = $('history-empty');
            const count = $('history-count');
            if (!list) return;
            if (count) count.textContent = data.length + ' render';
            if (!data.length) {
                if (empty) empty.style.display = '';
                return;
            }
            if (empty) empty.style.display = 'none';
            // Remove old items but keep empty placeholder
            list.querySelectorAll('.history-item').forEach(e => e.remove());
            data.forEach(r => {
                const item = document.createElement('div');
                item.className = 'history-item';
                const badge = `<span class="history-badge ${r.status || 'failed'}">${r.status || 'failed'}</span>`;
                item.innerHTML = `
                    <div class="history-item-name">${r.project_name || '(không tên)'}</div>
                    <div class="history-item-meta">
                        ${badge}
                        <span>${r.total_duration ? fmtDur(r.total_duration) : ''}</span>
                        <span>${r.file_size_mb ? r.file_size_mb + ' MB' : ''}</span>
                        <span>${(r.created_at || '').slice(0, 16)}</span>
                    </div>`;
                list.insertBefore(item, list.firstChild);
            });
        } catch (e) { console.warn('loadHistory:', e.message); }
    },

    // ── Effect toggle ──────────────────────────────────────────
    toggleEffectRandom() {
        const on = document.getElementById('effect-random')?.checked;
        document.getElementById('effect-selector')?.classList.toggle('hidden', !!on);
    },

    selectEffect(el) {
        document.querySelectorAll('.fx-btn[data-effect]').forEach(b => b.classList.remove('active'));
        el.classList.add('active');
    },

    // ── Transition toggle ─────────────────────────────────────
    toggleTransitionRandom() {
        const on = document.getElementById('transition-random')?.checked;
        document.getElementById('transition-selector')?.classList.toggle('hidden', !!on);
    },

    selectTransition(el) {
        document.querySelectorAll('.fx-btn[data-transition]').forEach(b => b.classList.remove('active'));
        el.classList.add('active');
    },

    // ── Subtitle toggle & source ──────────────────────────────
    toggleSubtitle() {
        const on = document.getElementById('subtitle-enabled')?.checked;
        const panel = document.getElementById('subtitle-panel');
        if (panel) panel.style.display = on ? '' : 'none';
        if (on) this.renderSubPreview(document.querySelector('.sub-style-btn.active')?.dataset.style || 'youtube_classic');
    },

    toggleSubtitleSource(source) {
        document.querySelectorAll('.sub-source-btn').forEach(b => b.classList.toggle('active', b.dataset.source === source));
        const autoSec = document.getElementById('sub-auto-section');
        const srtSec  = document.getElementById('sub-srt-section');
        if (autoSec) autoSec.classList.toggle('hidden', source !== 'auto');
        if (srtSec)  srtSec.classList.toggle('hidden',  source !== 'srt');
    },

    selectSubStyle(el) {
        document.querySelectorAll('.sub-style-btn').forEach(b => b.classList.remove('active'));
        el.classList.add('active');
        this.renderSubPreview(el.dataset.style);
    },

    renderSubPreview(style) {
        const canvas = document.getElementById('sub-preview-canvas');
        if (!canvas) return;
        const ctx = canvas.getContext('2d');
        const W = canvas.width, H = canvas.height;

        const MAP = {
            youtube_classic: { color: '#fff',     bg: 'rgba(0,0,0,0.72)', size: 13, bold: false, outline: 0, shadow: false, pos: 'bottom' },
            netflix_style:   { color: '#fff',     bg: null,                size: 15, bold: true,  outline: 3, shadow: true,  pos: 'bottom' },
            minimal:         { color: '#eeeeee',  bg: null,                size: 11, bold: false, outline: 1, shadow: false, pos: 'bottom' },
            social_media:    { color: '#FFE500',  bg: null,                size: 15, bold: true,  outline: 2, shadow: true,  pos: 'center' },
            karaoke:         { color: '#FFE500',  bg: 'rgba(0,0,40,0.75)',size: 13, bold: true,  outline: 1, shadow: false, pos: 'bottom' },
            anime:           { color: '#ffffff',  bg: null,                size: 17, bold: true,  outline: 5, shadow: false, pos: 'bottom' },
            cinematic:       { color: '#f5f5dc',  bg: 'rgba(0,0,0,0.45)', size: 11, bold: false, outline: 0, shadow: false, pos: 'top'    },
            pop:             { color: '#FF6B9D',  bg: null,                size: 15, bold: true,  outline: 3, shadow: true,  pos: 'center' },
        };
        const s = MAP[style] || MAP.youtube_classic;
        const sample = 'Đây là phụ đề mẫu của bạn ✨';

        // Background
        ctx.clearRect(0, 0, W, H);
        ctx.fillStyle = '#0d0d1a';
        ctx.fillRect(0, 0, W, H);
        ctx.fillStyle = 'rgba(255,255,255,0.03)';
        ctx.fillRect(4, 4, W/2 - 8, H - 8);
        ctx.fillRect(W/2 + 4, 4, W/2 - 8, H - 8);

        ctx.font = `${s.bold ? 'bold ' : ''}${s.size}px Arial,sans-serif`;
        const tw = ctx.measureText(sample).width;
        const tx = Math.max(4, (W - tw) / 2);
        const ty = s.pos === 'top' ? s.size + 6 : s.pos === 'center' ? H / 2 + s.size / 2 : H - 6;

        if (s.bg) {
            ctx.fillStyle = s.bg;
            ctx.fillRect(tx - 7, ty - s.size - 2, Math.min(tw + 14, W - 8), s.size + 8);
        }
        if (s.outline > 0) {
            ctx.strokeStyle = '#000';
            ctx.lineWidth = s.outline * 2;
            ctx.lineJoin = 'round';
            ctx.strokeText(sample, tx, ty);
        }
        if (s.shadow) {
            ctx.shadowColor = 'rgba(0,0,0,0.9)';
            ctx.shadowBlur = 8;
            ctx.shadowOffsetX = 1;
            ctx.shadowOffsetY = 1;
        }
        ctx.fillStyle = s.color;
        ctx.fillText(sample, tx, ty);
        ctx.shadowColor = 'transparent';
        ctx.shadowBlur = 0;
    },

    // ── Whisper auto-generate ─────────────────────────────────
    async generateSubtitle() {
        const audioFolder  = $('audio-folder')?.value  || '';
        const outputFolder = $('output-folder')?.value || '';
        const language     = $('sub-language')?.value  || 'vi';
        if (!audioFolder || !outputFolder) { toast('Cần chọn thư mục audio và thư mục xuất.', 'warning'); return; }

        const btn    = $('btn-generate-sub');
        const status = $('sub-gen-status');
        if (btn) { btn.textContent = 'Đang phân tích…'; btn.classList.add('disabled'); }
        if (status) status.textContent = '⏳ Đang chạy Whisper…';

        try {
            const d = await fetch('/api/subtitle/generate', {
                method: 'POST', headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ audio_folder: audioFolder, output_folder: outputFolder, language }),
            }).then(r => r.json());

            if (d.error) {
                toast((d.error || '') + (d.detail ? ' — ' + d.detail : ''), 'error');
                if (status) status.textContent = '❌ Thất bại';
            } else {
                const inp = $('srt-path'); if (inp) inp.value = d.srt_path || '';
                if (status) status.textContent = `✅ ${d.entry_count} phụ đề`;
                toast(`Đã tạo ${d.entry_count} phụ đề tự động!`, 'success');
                this.toggleSubtitleSource('srt');
            }
        } catch (e) {
            toast('Lỗi: ' + e.message, 'error');
            if (status) status.textContent = '❌ Lỗi kết nối';
        } finally {
            if (btn) { btn.textContent = '🎙️ Phân tích giọng nói'; btn.classList.remove('disabled'); }
        }
    },

    // ── SRT utility (kept for compat) ────────────────────────
    async generateDummySRT() {
        const img = $('image-folder')?.value;
        const aud = $('audio-folder')?.value;
        const out = $('output-folder')?.value;
        if (!img || !aud || !out) { toast('Cần chọn đủ thư mục trước.', 'warning'); return; }
        try {
            const d = await fetch('/api/prepare', {
                method: 'POST', headers: {'Content-Type':'application/json'},
                body: JSON.stringify({ image_folder: img, audio_folder: aud, output_folder: out }),
            }).then(r => r.json());
            if (d.error) { toast(d.error, 'error'); return; }
            toast('File SRT mẫu đã được tạo trong thư mục xuất.', 'success');
        } catch (e) { toast('Lỗi: ' + e.message, 'error'); }
    },
};

// ─── Keyboard shortcuts ───────────────────────────────────────────────────────

document.addEventListener('keydown', e => {
    if (e.ctrlKey && e.key === 'Enter') {
        e.preventDefault();
        const btn = $('btn-render');
        if (btn && !btn.classList.contains('disabled')) App.startRender();
    } else if (e.key === 'Escape') {
        // Close modals
        $('folder-modal-backdrop')?.classList.add('hidden');
        $('project-modal-backdrop')?.classList.add('hidden');
    }
});

// ─── Notification permission ──────────────────────────────────────────────────

if ('Notification' in window && Notification.permission === 'default') {
    // Request on first user gesture instead of immediately
    document.addEventListener('click', () => Notification.requestPermission(), { once: true });
}

// ─── Boot ─────────────────────────────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', () => {
    // The inline splash in index.html already handles reveal timing.
    // We load data and call revealApp() when ready.
    Promise.all([loadSystemInfo(), loadStats()]).finally(() => {
        if (typeof window.revealApp === 'function') {
            window.revealApp();
        }
    });

    // Live validation when folder inputs change
    const imgFld = $('image-folder');
    if (imgFld) imgFld.addEventListener('change', () => App.onImageFolderChange());
    const audFld = $('audio-folder');
    if (audFld) audFld.addEventListener('change', () => App.onAudioFolderChange());
    const outFld = $('output-folder');
    if (outFld) outFld.addEventListener('change', () => App.validateFolders());

    // Init subtitle preview on first style
    App.renderSubPreview('youtube_classic');

    // JS tooltip system (position:fixed, not clipped by sidebar overflow)
    _initTooltip();

    // Server lifecycle: browser↔CMD sync
    _initServerLifecycle();

    // Realtime sys metrics
    pollMetrics();
    setInterval(pollMetrics, 4000);
});

// Expose globally so inline onclick="" handlers work
window.App = App;