// Shared client helpers for the fleet UI. All API calls flow through fleetFetch
// which attaches the admin token from localStorage and bounces to /fleet/login
// when missing or invalid.

(function () {
  const TOKEN_KEY = 'fleet_token';

  function getToken() {
    return localStorage.getItem(TOKEN_KEY) || '';
  }

  function setToken(t) {
    if (t) localStorage.setItem(TOKEN_KEY, t);
    else localStorage.removeItem(TOKEN_KEY);
  }

  function ensureToken(redirect = true) {
    const t = getToken();
    if (!t && redirect && !location.pathname.endsWith('/fleet/login')) {
      location.href = '/fleet/login';
      return null;
    }
    return t;
  }

  async function fleetFetch(path, opts = {}) {
    const token = getToken();
    const headers = Object.assign({}, opts.headers || {}, {
      Authorization: `Bearer ${token}`,
    });
    if (opts.body && typeof opts.body !== 'string') {
      headers['Content-Type'] = 'application/json';
      opts = Object.assign({}, opts, { body: JSON.stringify(opts.body) });
    }
    const res = await fetch(path, Object.assign({}, opts, { headers }));
    if (res.status === 401 || res.status === 403) {
      setToken(null);
      location.href = '/fleet/login';
      throw new Error('unauthorized');
    }
    if (!res.ok) {
      let text = await res.text();
      try { text = JSON.parse(text).detail || text; } catch (_) {}
      throw new Error(`${res.status} ${text}`);
    }
    const ct = res.headers.get('content-type') || '';
    if (ct.includes('application/json')) return res.json();
    return res.text();
  }

  function fmtBytes(n) {
    if (n == null || isNaN(n)) return '—';
    const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
    let i = 0;
    n = Number(n);
    while (n >= 1024 && i < units.length - 1) { n /= 1024; i++; }
    return `${n.toFixed(n >= 10 || i === 0 ? 0 : 1)} ${units[i]}`;
  }

  function fmtRate(n) {
    if (n == null) return '—';
    return `${fmtBytes(n)}/s`;
  }

  function fmtUptime(seconds) {
    if (seconds == null) return '—';
    const d = Math.floor(seconds / 86400);
    const h = Math.floor((seconds % 86400) / 3600);
    const m = Math.floor((seconds % 3600) / 60);
    if (d) return `${d}d ${h}h`;
    if (h) return `${h}h ${m}m`;
    return `${m}m`;
  }

  function fmtAgo(ts) {
    if (!ts) return 'never';
    const s = Math.max(0, Math.floor(Date.now() / 1000) - ts);
    if (s < 60) return `${s}s ago`;
    if (s < 3600) return `${Math.floor(s / 60)}m ago`;
    if (s < 86400) return `${Math.floor(s / 3600)}h ago`;
    return `${Math.floor(s / 86400)}d ago`;
  }

  function fmtTemp(c) {
    if (c == null) return '—';
    return `${Number(c).toFixed(0)}°C`;
  }

  function el(tag, attrs = {}, ...children) {
    const n = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs || {})) {
      if (k === 'class') n.className = v;
      else if (k === 'html') n.innerHTML = v;
      else if (k.startsWith('on') && typeof v === 'function') n.addEventListener(k.slice(2).toLowerCase(), v);
      else if (v != null) n.setAttribute(k, v);
    }
    for (const c of children.flat()) {
      if (c == null || c === false) continue;
      n.appendChild(typeof c === 'string' ? document.createTextNode(c) : c);
    }
    return n;
  }

  function renderNav(active) {
    const links = [
      { id: 'hosts', label: 'Hosts', href: '/fleet' },
      { id: 'map', label: 'Map', href: '/fleet/map' },
      { id: 'alerts', label: 'Alerts', href: '/fleet/alerts' },
      { id: 'rules', label: 'Rules', href: '/fleet/rules' },
    ];
    const header = el('header', { class: 'fleet-nav' },
      el('div', { class: 'brand' }, 'Fleet'),
      el('nav', {}, ...links.map(l => el('a', {
        href: l.href, class: l.id === active ? 'active' : '',
      }, l.label))),
      el('div', { class: 'right' },
        el('span', { id: 'live-indicator' }),
        el('button', {
          class: 'signout',
          onclick: () => { setToken(null); location.href = '/fleet/login'; },
        }, 'Sign out'),
      ),
    );
    document.body.insertBefore(header, document.body.firstChild);
  }

  function setLive(text, ok = true) {
    const n = document.getElementById('live-indicator');
    if (n) {
      n.textContent = text;
      n.style.color = ok ? 'var(--fg-2)' : 'var(--crit)';
    }
  }

  function statusClass(host) {
    if (!host.last_seen) return 'pending';
    return host.up ? 'up' : 'down';
  }

  function severityFor(host, latest) {
    if (!host.up) return 'down';
    if (!latest) return 'up';
    const cpu = latest.cpu?.pct ?? 0;
    const mem = latest.mem?.pct ?? 0;
    const cpuTemp = latest.cpu?.temp ?? 0;
    if (cpu > 95 || mem > 95 || cpuTemp > 90) return 'warn';
    return 'up';
  }

  function pctBarClass(p) {
    if (p == null) return '';
    if (p >= 90) return 'crit';
    if (p >= 75) return 'warn';
    return '';
  }

  function pct(n, digits = 1) {
    if (n == null) return '—';
    return `${Number(n).toFixed(digits)}%`;
  }

  function num(n, digits = 1) {
    if (n == null) return '—';
    return Number(n).toFixed(digits);
  }

  window.Fleet = {
    fetch: fleetFetch,
    ensureToken,
    setToken,
    getToken,
    fmtBytes, fmtRate, fmtUptime, fmtAgo, fmtTemp,
    pct, num, el, renderNav, setLive, statusClass, severityFor, pctBarClass,
  };
})();
