class ThemeManager {
    constructor() {
        this.storageKey = 'manga-studio-theme';
        this.lightClass = 'light';
    }

    load() {
        const saved = localStorage.getItem(this.storageKey);
        if (saved === 'light') {
            document.body.classList.add(this.lightClass);
        } else {
            document.body.classList.remove(this.lightClass);
        }
        this._syncIcon();
    }

    toggle() {
        const isLight = document.body.classList.toggle(this.lightClass);
        localStorage.setItem(this.storageKey, isLight ? 'light' : 'dark');
        this._animateIcon();
        this._syncIcon();
    }

    getTheme() {
        return document.body.classList.contains(this.lightClass) ? 'light' : 'dark';
    }

    _syncIcon() {
        const icon = document.getElementById('theme-icon');
        if (!icon) return;
        icon.textContent = this.getTheme() === 'light' ? '🌙' : '☀️';
    }

    _animateIcon() {
        const btn = document.getElementById('theme-toggle');
        if (!btn) return;
        btn.classList.add('theme-spin');
        btn.addEventListener('animationend', () => btn.classList.remove('theme-spin'), { once: true });
    }
}

const themeManager = new ThemeManager();

document.addEventListener('DOMContentLoaded', () => themeManager.load());