// Engagement scores (0-10) indexed as [dayIndex][hour] where
// dayIndex: 0=Mon … 6=Sun, hour: 0-23 EST
const ENGAGEMENT_DATA = (() => {
    const base = [
        // Mon-Fri share the same weekday profile
        [1,1,0.5,0.5,0.5,1,2,3,4,5,5,5, 8,8,7,6,5,6,7,9,10,9,8,5],  // Mon
        [1,1,0.5,0.5,0.5,1,2,3,4,5,5,5, 8,8,7,6,5,6,7,9,10,9,8,5],  // Tue
        [1,1,0.5,0.5,0.5,1,2,3,4,5,5,5, 8,8,7,6,5,6,7,9,10,9,8,5],  // Wed
        [1,1,0.5,0.5,0.5,1,2,3,4,5,5,5, 8,8,7,6,5,6,7,9,10,9,8,5],  // Thu
        [1,1,0.5,0.5,0.5,1,2,3,4,5,5,5, 7,7,6,6,5,6,8,9,10,8,7,4],  // Fri
        // Weekend
        [1,1,0.5,0.5,0.5,1,2,4,6,9,10,8, 7,6,9,9,8,7,6,5,5,4,3,2],  // Sat
        [1,1,0.5,0.5,0.5,1,2,4,6,9,10,8, 7,6,8,8,7,6,5,5,4,3,2,1],  // Sun
    ];
    return base;
})();

const DAY_LABELS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];

class HeatmapRenderer {
    render(containerId) {
        const container = document.getElementById(containerId);
        if (!container) return;

        container.innerHTML = '';
        const table = document.createElement('table');
        table.className = 'heatmap-table';

        // Header row: hours
        const thead = document.createElement('thead');
        const hRow = document.createElement('tr');
        hRow.appendChild(this._th(''));
        for (let h = 0; h < 24; h++) {
            const th = this._th(h === 0 ? '12a' : h < 12 ? `${h}a` : h === 12 ? '12p' : `${h - 12}p`);
            th.className = 'heatmap-hour';
            hRow.appendChild(th);
        }
        thead.appendChild(hRow);
        table.appendChild(thead);

        // Body: days × hours
        const tbody = document.createElement('tbody');
        DAY_LABELS.forEach((day, di) => {
            const row = document.createElement('tr');
            row.appendChild(this._td(day, 'heatmap-day-label'));
            for (let h = 0; h < 24; h++) {
                const score = ENGAGEMENT_DATA[di][h];
                const cell = document.createElement('td');
                cell.className = 'heatmap-cell';
                cell.style.backgroundColor = this._scoreToColor(score);
                cell.title = `${day} ${h}:00 EST — Score: ${score}/10\nVN: ${this.getVietnamTime(h, di)}`;
                cell.dataset.score = score;
                cell.dataset.day = di;
                cell.dataset.hour = h;
                row.appendChild(cell);
            }
            tbody.appendChild(row);
        });
        table.appendChild(tbody);
        container.appendChild(table);
    }

    getTopSlots(n = 3) {
        const slots = [];
        DAY_LABELS.forEach((day, di) => {
            for (let h = 0; h < 24; h++) {
                const score = ENGAGEMENT_DATA[di][h];
                const reasons = this._getReason(di, h, score);
                if (reasons) {
                    slots.push({
                        day,
                        hour: h,
                        score,
                        estLabel: `${day} ${h}:00–${h + 1}:00 EST`,
                        vnLabel: this.getVietnamTime(h, di),
                        reason: reasons,
                    });
                }
            }
        });
        slots.sort((a, b) => b.score - a.score);
        return slots.slice(0, n);
    }

    // estHour: 0-23, dayOffset: 0=Mon…6=Sun
    getVietnamTime(estHour, dayOffset) {
        // EST = UTC-5, VN = UTC+7  →  VN = EST + 12
        let vnHour = (estHour + 12) % 24;
        const crossMidnight = (estHour + 12) >= 24;
        const days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
        const vnDay = days[(dayOffset + (crossMidnight ? 1 : 0)) % 7];
        const ampm = vnHour >= 12 ? 'PM' : 'AM';
        const disp = vnHour % 12 || 12;
        return `${vnDay} ${disp}:00 ${ampm} (VN)`;
    }

    _scoreToColor(score) {
        // 0 → dark blue-grey, 10 → vivid orange-red
        const r = Math.round(20  + (score / 10) * 210);
        const g = Math.round(30  + (score / 10) * 60);
        const b = Math.round(80  - (score / 10) * 60);
        return `rgb(${r},${g},${b})`;
    }

    _getReason(di, h, score) {
        if (score < 7) return null;
        const isWeekend = di >= 5;
        if (!isWeekend && h >= 19 && h <= 22) return 'Weekday prime time — users off work, browsing socials';
        if (!isWeekend && h >= 12 && h <= 13) return 'Weekday lunch break — high mobile usage';
        if (isWeekend && h >= 9  && h <= 10)  return 'Weekend morning — peak leisure browsing';
        if (isWeekend && h >= 14 && h <= 15)  return 'Weekend afternoon — social media peak';
        return 'High engagement window';
    }

    _th(text) {
        const el = document.createElement('th');
        el.textContent = text;
        return el;
    }

    _td(text, cls = '') {
        const el = document.createElement('td');
        el.textContent = text;
        if (cls) el.className = cls;
        return el;
    }
}

const heatmapRenderer = new HeatmapRenderer();