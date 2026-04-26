class ClockManager {
    constructor() {
        this.timezones = [
            { id: 'la',      city: 'Los Angeles', tz: 'America/Los_Angeles', flag: '🇺🇸' },
            { id: 'london',  city: 'London',      tz: 'Europe/London',       flag: '🇬🇧' },
            { id: 'sydney',  city: 'Sydney',      tz: 'Australia/Sydney',    flag: '🇦🇺' },
            { id: 'toronto', city: 'Toronto',     tz: 'America/Toronto',     flag: '🇨🇦' },
        ];
        this._interval = null;
    }

    start() {
        this._tick();
        this._interval = setInterval(() => this._tick(), 1000);
    }

    stop() {
        clearInterval(this._interval);
        this._interval = null;
    }

    formatTime(date, tz) {
        const timeFmt = new Intl.DateTimeFormat('en-GB', {
            timeZone: tz,
            hour: '2-digit', minute: '2-digit', second: '2-digit',
            hour12: false,
        });

        const dateFmt = new Intl.DateTimeFormat('en-GB', {
            timeZone: tz,
            weekday: 'short', day: '2-digit', month: 'short',
        });

        const offsetFmt = new Intl.DateTimeFormat('en-GB', {
            timeZone: tz,
            timeZoneName: 'shortOffset',
        });

        const timeStr = timeFmt.format(date);
        const dateStr = dateFmt.format(date);

        // Extract UTC offset from formatted string
        const parts = offsetFmt.formatToParts(date);
        const offsetPart = parts.find(p => p.type === 'timeZoneName');
        const offset = offsetPart ? offsetPart.value : '';

        return { time: timeStr, date: dateStr, offset };
    }

    _tick() {
        const now = new Date();
        this.timezones.forEach(({ id, tz }) => {
            const el = document.getElementById(`clock-${id}`);
            if (!el) return;
            const { time, date, offset } = this.formatTime(now, tz);
            const timeEl  = el.querySelector('.clock-time');
            const dateEl  = el.querySelector('.clock-date');
            const offsetEl = el.querySelector('.clock-offset');
            if (timeEl)   timeEl.textContent  = time;
            if (dateEl)   dateEl.textContent  = date;
            if (offsetEl) offsetEl.textContent = offset;
        });
    }
}

const clockManager = new ClockManager();