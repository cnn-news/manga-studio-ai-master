class ProgressManager {
    constructor() {
        this.jobId = null;
        this.startTime = null;
        this.timerInterval = null;
        this.resourceInterval = null;
        this.isRunning = false;
    }

    start(jobId) {
        this.jobId = jobId;
        this.startTime = Date.now();
        this.isRunning = true;
        this._reset();
        this.startTimer();
        this.updateResources();
    }

    startTimer() {
        clearInterval(this.timerInterval);
        this.timerInterval = setInterval(() => {
            const elapsed = Math.floor((Date.now() - this.startTime) / 1000);
            const mm = String(Math.floor(elapsed / 60)).padStart(2, '0');
            const ss = String(elapsed % 60).padStart(2, '0');
            const el = document.getElementById('progress-timer');
            if (el) el.textContent = `${mm}:${ss}`;
        }, 1000);
    }

    stop() {
        clearInterval(this.timerInterval);
        clearInterval(this.resourceInterval);
        this.timerInterval = null;
        this.resourceInterval = null;
        this.isRunning = false;

        if (this.startTime) {
            const mins = ((Date.now() - this.startTime) / 60000).toFixed(1);
            this.addLog(`Completed in ${mins} minute(s).`, 'success');
        }
    }

    addLog(message, level = 'info') {
        const panel = document.getElementById('log-panel');
        if (!panel) return;

        const line = document.createElement('div');
        line.className = `log-line log-${level}`;

        const ts = new Date().toLocaleTimeString('en-GB', { hour12: false });
        line.innerHTML = `<span class="log-ts">[${ts}]</span> <span class="log-msg">${this._escapeHtml(message)}</span>`;
        panel.appendChild(line);
        panel.scrollTop = panel.scrollHeight;
    }

    clearLog() {
        const panel = document.getElementById('log-panel');
        if (panel) panel.innerHTML = '';
    }

    async copyLog() {
        const panel = document.getElementById('log-panel');
        if (!panel) return;
        const text = panel.innerText;
        try {
            await navigator.clipboard.writeText(text);
            this.addLog('Log copied to clipboard.', 'info');
        } catch {
            this.addLog('Failed to copy log.', 'error');
        }
    }

    updateProgress(data) {
        // overall_progress bar
        const bar = document.getElementById('progress-bar');
        const pct = document.getElementById('progress-pct');
        if (bar) bar.style.width = `${data.overall_progress ?? 0}%`;
        if (pct) pct.textContent = `${Math.round(data.overall_progress ?? 0)}%`;

        // phase badge
        const phase = document.getElementById('progress-phase');
        if (phase && data.phase) {
            phase.textContent = data.phase;
            phase.className = `phase-badge phase-${data.phase.toLowerCase().replace(/\s+/g, '-')}`;
        }

        // segment counter
        const seg = document.getElementById('progress-segments');
        if (seg && data.current_segment != null && data.total_segments != null) {
            seg.textContent = `${data.current_segment} / ${data.total_segments}`;
        }

        // status text
        const status = document.getElementById('progress-status');
        if (status && data.status) status.textContent = data.status;
    }

    async updateResources() {
        const poll = async () => {
            try {
                const res = await fetch('/api/system/check');
                if (!res.ok) return;
                const data = await res.json();

                const cpu = document.getElementById('resource-cpu');
                const ram = document.getElementById('resource-ram');
                if (cpu && data.cpu_percent != null) cpu.textContent = `CPU: ${data.cpu_percent.toFixed(1)}%`;
                if (ram && data.ram_percent != null) ram.textContent = `RAM: ${data.ram_percent.toFixed(1)}%`;
            } catch { /* ignore */ }
        };

        await poll();
        clearInterval(this.resourceInterval);
        this.resourceInterval = setInterval(poll, 2000);
    }

    reset() {
        this.stop();
        this.jobId = null;
        this.startTime = null;
        this._reset();
    }

    _reset() {
        const bar    = document.getElementById('progress-bar');
        const pct    = document.getElementById('progress-pct');
        const phase  = document.getElementById('progress-phase');
        const seg    = document.getElementById('progress-segments');
        const status = document.getElementById('progress-status');
        const timer  = document.getElementById('progress-timer');

        if (bar)    bar.style.width   = '0%';
        if (pct)    pct.textContent   = '0%';
        if (phase)  phase.textContent = 'Idle';
        if (seg)    seg.textContent   = '0 / 0';
        if (status) status.textContent = '';
        if (timer)  timer.textContent  = '00:00';
    }

    _escapeHtml(str) {
        return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    }
}

const progressManager = new ProgressManager();