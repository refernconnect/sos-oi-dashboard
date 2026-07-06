"""
SOS OI DASHBOARD — NSE Option Chain for NIFTY / BANKNIFTY
No API key needed. Fetches public NSE data.
Deploy to Railway Hobby plan.
"""

import os
import math
import time
import json
import threading
from datetime import datetime, timezone, timedelta
from flask import Flask, jsonify, render_template_string

import requests

app = Flask(__name__)

# ─── CONFIG ───
IST = timezone(timedelta(hours=5, minutes=30))
REFRESH_SECONDS = 180  # NSE refreshes ~3 min
STRIKES_AROUND_ATM = 10  # show 10 above + 10 below ATM

# ─── NSE SESSION HANDLER ───
class NSEFetcher:
    """Handles NSE cookie/session management and data fetch."""

    BASE = "https://www.nseindia.com"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.nseindia.com/option-chain",
    }

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
        self.cookies_set = False
        self.last_cookie_time = 0

    def _set_cookies(self):
        """Visit NSE homepage to get session cookies."""
        now = time.time()
        if self.cookies_set and (now - self.last_cookie_time) < 300:
            return True
        try:
            r = self.session.get(self.BASE, timeout=10)
            if r.status_code == 200:
                self.cookies_set = True
                self.last_cookie_time = now
                return True
        except Exception as e:
            print(f"Cookie fetch failed: {e}")
        return False

    def fetch_option_chain(self, symbol="NIFTY"):
        """Fetch full option chain for symbol."""
        if not self._set_cookies():
            return None

        if symbol in ("NIFTY", "BANKNIFTY", "NIFTY NEXT 50", "NIFTY FINANCIAL SERVICES"):
            url = f"{self.BASE}/api/option-chain-indices?symbol={symbol}"
        else:
            url = f"{self.BASE}/api/option-chain-equities?symbol={symbol}"

        try:
            r = self.session.get(url, timeout=15)
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 403:
                # Reset cookies and retry once
                self.cookies_set = False
                if self._set_cookies():
                    r = self.session.get(url, timeout=15)
                    if r.status_code == 200:
                        return r.json()
        except Exception as e:
            print(f"Fetch failed for {symbol}: {e}")
        return None


nse = NSEFetcher()

# ─── DATA CACHE ───
SYMBOLS = {
    "NIFTY":      {"type": "index", "step": 50},
    "BANKNIFTY":  {"type": "index", "step": 100},
    "FINNIFTY":   {"type": "index", "step": 50},
    "MIDCPNIFTY": {"type": "index", "step": 25},
    "SENSEX":     {"type": "index", "step": 100},
    "RELIANCE":   {"type": "equity", "step": 20},
    "HDFCBANK":   {"type": "equity", "step": 20},
    "INFY":       {"type": "equity", "step": 20},
    "TCS":        {"type": "equity", "step": 50},
    "ICICIBANK":  {"type": "equity", "step": 20},
    "SBIN":       {"type": "equity", "step": 10},
    "TATAMOTORS": {"type": "equity", "step": 10},
    "BAJFINANCE": {"type": "equity", "step": 100},
    "ITC":        {"type": "equity", "step": 10},
    "TATASTEEL":  {"type": "equity", "step": 5},
}
cache = {sym: {"data": None, "timestamp": None, "error": None} for sym in SYMBOLS}
cache_lock = threading.Lock()


def process_chain(raw, symbol):
    """Extract actionable OI data from raw NSE response."""
    if not raw or "records" not in raw:
        return None

    records = raw["records"]
    data = records.get("data", [])
    spot = records.get("underlyingValue", 0)
    expiry_dates = records.get("expiryDates", [])
    nearest_expiry = expiry_dates[0] if expiry_dates else ""

    # Filter to nearest expiry
    filtered = [d for d in data if d.get("expiryDate") == nearest_expiry]

    step = SYMBOLS.get(symbol, {}).get("step", 50)
    atm = round(spot / step) * step

    strikes = []
    total_ce_oi = 0
    total_pe_oi = 0
    total_ce_chg = 0
    total_pe_chg = 0
    max_ce_oi = 0
    max_pe_oi = 0
    max_ce_oi_strike = 0
    max_pe_oi_strike = 0

    for row in filtered:
        sp = row.get("strikePrice", 0)
        if abs(sp - atm) > step * STRIKES_AROUND_ATM:
            continue

        ce = row.get("CE", {})
        pe = row.get("PE", {})

        ce_oi = ce.get("openInterest", 0) or 0
        pe_oi = pe.get("openInterest", 0) or 0
        ce_chg_oi = ce.get("changeinOpenInterest", 0) or 0
        pe_chg_oi = pe.get("changeinOpenInterest", 0) or 0
        ce_iv = ce.get("impliedVolatility", 0) or 0
        pe_iv = pe.get("impliedVolatility", 0) or 0
        ce_ltp = ce.get("lastPrice", 0) or 0
        pe_ltp = pe.get("lastPrice", 0) or 0
        ce_vol = ce.get("totalTradedVolume", 0) or 0
        pe_vol = pe.get("totalTradedVolume", 0) or 0

        total_ce_oi += ce_oi
        total_pe_oi += pe_oi
        total_ce_chg += ce_chg_oi
        total_pe_chg += pe_chg_oi

        if ce_oi > max_ce_oi:
            max_ce_oi = ce_oi
            max_ce_oi_strike = sp
        if pe_oi > max_pe_oi:
            max_pe_oi = pe_oi
            max_pe_oi_strike = sp

        is_atm = sp == atm

        strikes.append({
            "strike": sp,
            "is_atm": is_atm,
            "ce_oi": ce_oi,
            "pe_oi": pe_oi,
            "ce_chg_oi": ce_chg_oi,
            "pe_chg_oi": pe_chg_oi,
            "ce_iv": round(ce_iv, 1),
            "pe_iv": round(pe_iv, 1),
            "ce_ltp": round(ce_ltp, 2),
            "pe_ltp": round(pe_ltp, 2),
            "ce_vol": ce_vol,
            "pe_vol": pe_vol,
        })

    strikes.sort(key=lambda x: x["strike"])

    # PCR
    pcr = round(total_pe_oi / total_ce_oi, 2) if total_ce_oi > 0 else 0

    # Max Pain calculation
    max_pain = compute_max_pain(filtered)

    # OI-based support/resistance
    # Highest PE OI = support (writers don't want price below)
    # Highest CE OI = resistance (writers don't want price above)

    # Directional signal from OI change
    # Fresh PE writing (positive chg, price stable/up) = bullish
    # Fresh CE writing (positive chg, price stable/down) = bearish
    # CE unwinding (negative chg) on up move = short covering = bullish continuation
    # PE unwinding (negative chg) on down move = short covering = bearish continuation

    signal = "NEUTRAL"
    if pcr > 1.2:
        signal = "BULLISH (heavy PE writing)"
    elif pcr < 0.7:
        signal = "BEARISH (heavy CE writing)"
    elif total_pe_chg > 0 and total_ce_chg < 0:
        signal = "BULLISH (PE build + CE unwind)"
    elif total_ce_chg > 0 and total_pe_chg < 0:
        signal = "BEARISH (CE build + PE unwind)"

    return {
        "symbol": symbol,
        "spot": spot,
        "atm": atm,
        "expiry": nearest_expiry,
        "pcr": pcr,
        "max_pain": max_pain,
        "signal": signal,
        "ce_wall": max_ce_oi_strike,
        "ce_wall_oi": max_ce_oi,
        "pe_wall": max_pe_oi_strike,
        "pe_wall_oi": max_pe_oi,
        "total_ce_oi": total_ce_oi,
        "total_pe_oi": total_pe_oi,
        "total_ce_chg": total_ce_chg,
        "total_pe_chg": total_pe_chg,
        "strikes": strikes,
    }


def compute_max_pain(data):
    """Max pain = strike where total payout to option buyers is minimized."""
    strike_prices = set()
    for row in data:
        strike_prices.add(row.get("strikePrice", 0))

    if not strike_prices:
        return 0

    min_pain = float("inf")
    max_pain_strike = 0

    for test_strike in sorted(strike_prices):
        total_pain = 0
        for row in data:
            sp = row.get("strikePrice", 0)
            ce_oi = row.get("CE", {}).get("openInterest", 0) or 0
            pe_oi = row.get("PE", {}).get("openInterest", 0) or 0

            if test_strike > sp:
                total_pain += (test_strike - sp) * ce_oi
            if test_strike < sp:
                total_pain += (sp - test_strike) * pe_oi

        if total_pain < min_pain:
            min_pain = total_pain
            max_pain_strike = test_strike

    return max_pain_strike


def refresh_data():
    """Background thread: fetch and cache data every 3 min."""
    while True:
        for sym in SYMBOLS:
            try:
                raw = nse.fetch_option_chain(sym)
                if raw:
                    processed = process_chain(raw, sym)
                    if processed:
                        with cache_lock:
                            cache[sym]["data"] = processed
                            cache[sym]["timestamp"] = datetime.now(IST).strftime("%H:%M:%S")
                            cache[sym]["error"] = None
                    else:
                        with cache_lock:
                            cache[sym]["error"] = "Parse failed"
                else:
                    with cache_lock:
                        cache[sym]["error"] = "NSE returned empty"
            except Exception as e:
                with cache_lock:
                    cache[sym]["error"] = str(e)
            time.sleep(2)  # small gap between symbols

        time.sleep(REFRESH_SECONDS)


# ─── ROUTES ───
@app.route("/")
def dashboard():
    return render_template_string(DASHBOARD_HTML)


@app.route("/api/data")
def api_data():
    with cache_lock:
        return jsonify(cache)


@app.route("/health")
def health():
    return "ok"


# ─── DASHBOARD HTML ───
DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SOS OI Dashboard</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
    background: #1A1816;
    color: #e8e2d8;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', monospace;
    font-size: 13px;
    padding: 8px;
}
.header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 8px 0;
    border-bottom: 1px solid #3a3630;
    margin-bottom: 8px;
}
.header h1 {
    font-size: 14px;
    color: #C4A882;
    font-weight: 600;
    letter-spacing: 1px;
}
.tabs {
    display: flex;
    gap: 4px;
    margin-bottom: 8px;
    flex-wrap: wrap;
}
.tab {
    padding: 6px 16px;
    background: #2a2622;
    border: 1px solid #3a3630;
    color: #9a9590;
    cursor: pointer;
    font-size: 12px;
    border-radius: 2px;
}
.tab.active {
    background: #3a3630;
    color: #C4A882;
    border-color: #C4A882;
}
.summary {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 6px;
    margin-bottom: 10px;
}
.stat {
    background: #2a2622;
    padding: 8px;
    border-radius: 2px;
    text-align: center;
}
.stat-label {
    font-size: 10px;
    color: #9a9590;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}
.stat-value {
    font-size: 16px;
    font-weight: 700;
    margin-top: 2px;
}
.signal-box {
    background: #2a2622;
    padding: 10px;
    margin-bottom: 10px;
    border-left: 3px solid #C4A882;
    border-radius: 2px;
}
.signal-label { font-size: 10px; color: #9a9590; }
.signal-value { font-size: 14px; font-weight: 700; margin-top: 2px; }
.walls {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 6px;
    margin-bottom: 10px;
}
.wall {
    padding: 8px;
    border-radius: 2px;
    text-align: center;
}
.wall-ce { background: #3d2020; }
.wall-pe { background: #1e3d20; }
.wall-label { font-size: 10px; color: #9a9590; }
.wall-strike { font-size: 18px; font-weight: 700; }
.wall-oi { font-size: 11px; color: #9a9590; }

table {
    width: 100%;
    border-collapse: collapse;
    font-size: 11px;
}
th {
    background: #2a2622;
    color: #C4A882;
    padding: 6px 4px;
    text-align: right;
    font-weight: 600;
    font-size: 10px;
    position: sticky;
    top: 0;
    z-index: 1;
}
th.strike-col { text-align: center; }
td {
    padding: 5px 4px;
    text-align: right;
    border-bottom: 1px solid #2a2622;
}
td.strike-col {
    text-align: center;
    font-weight: 700;
    color: #C4A882;
    background: #2a2622;
}
tr.atm-row { background: #2a2a1e; }
tr.atm-row td.strike-col { background: #C4A882; color: #1A1816; }
.oi-bar {
    display: inline-block;
    height: 8px;
    border-radius: 1px;
    vertical-align: middle;
}
.ce-bar { background: #d9534f; }
.pe-bar { background: #4cc46a; }
.chg-pos { color: #4cc46a; }
.chg-neg { color: #d9534f; }
.table-wrap {
    max-height: 50vh;
    overflow-y: auto;
    border: 1px solid #2a2622;
    border-radius: 2px;
}
.refresh-info {
    font-size: 10px;
    color: #666;
}
.bullish { color: #4cc46a; }
.bearish { color: #d9534f; }
.neutral { color: #C4A882; }
.loading {
    text-align: center;
    padding: 40px;
    color: #666;
}
</style>
</head>
<body>
<div class="header">
    <h1>SOS OI DASHBOARD</h1>
    <span class="refresh-info" id="ts">loading...</span>
</div>
<div class="tabs">
  <div class="tab active" onclick="switchTab('NIFTY')">NIFTY</div>
    <div class="tab" onclick="switchTab('BANKNIFTY')">BNIFTY</div>
    <div class="tab" onclick="switchTab('FINNIFTY')">FINNIFTY</div>
    <div class="tab" onclick="switchTab('RELIANCE')">RIL</div>
    <div class="tab" onclick="switchTab('HDFCBANK')">HDFC</div>
    <div class="tab" onclick="switchTab('INFY')">INFY</div>
    <div class="tab" onclick="switchTab('TCS')">TCS</div>
    <div class="tab" onclick="switchTab('ICICIBANK')">ICICI</div>
    <div class="tab" onclick="switchTab('SBIN')">SBIN</div>
    <div class="tab" onclick="switchTab('TATAMOTORS')">TATAMTR</div>
    <div class="tab" onclick="switchTab('BAJFINANCE')">BAJFIN</div>
    <div class="tab" onclick="switchTab('ITC')">ITC</div>
    <div class="tab" onclick="switchTab('TATASTEEL')">TSTEEL</div>
    <div class="tab" onclick="switchTab('MIDCPNIFTY')">MIDCP</div>
    <div class="tab" onclick="switchTab('SENSEX')">SENSEX</div>
</div>
<div id="content"><div class="loading">Fetching NSE data...</div></div>

<script>
let current = 'NIFTY';
let allData = {};

function switchTab(sym) {
    current = sym;
    document.querySelectorAll('.tab').forEach(t => {
        t.classList.toggle('active', t.textContent === sym);
    });
    renderSymbol();
}

function fmt(n) {
    if (n >= 10000000) return (n/10000000).toFixed(2) + ' Cr';
    if (n >= 100000) return (n/100000).toFixed(2) + ' L';
    if (n >= 1000) return (n/1000).toFixed(1) + 'K';
    return n;
}

function renderSymbol() {
    let c = allData[current];
    if (!c || !c.data) {
        document.getElementById('content').innerHTML =
            '<div class="loading">' + (c && c.error ? c.error : 'Waiting for data...') + '</div>';
        return;
    }
    let d = c.data;
    let maxOI = 0;
    d.strikes.forEach(s => { maxOI = Math.max(maxOI, s.ce_oi, s.pe_oi); });

    let sigClass = d.signal.includes('BULLISH') ? 'bullish' : d.signal.includes('BEARISH') ? 'bearish' : 'neutral';

    let html = `
    <div class="summary">
        <div class="stat"><div class="stat-label">Spot</div><div class="stat-value">${d.spot.toFixed(1)}</div></div>
        <div class="stat"><div class="stat-label">ATM</div><div class="stat-value">${d.atm}</div></div>
        <div class="stat"><div class="stat-label">PCR</div><div class="stat-value ${d.pcr > 1 ? 'bullish' : d.pcr < 0.8 ? 'bearish' : 'neutral'}">${d.pcr}</div></div>
        <div class="stat"><div class="stat-label">Max Pain</div><div class="stat-value">${d.max_pain}</div></div>
    </div>
    <div class="signal-box">
        <div class="signal-label">OI SIGNAL</div>
        <div class="signal-value ${sigClass}">${d.signal}</div>
    </div>
    <div class="walls">
        <div class="wall wall-ce">
            <div class="wall-label">CE WALL (resistance)</div>
            <div class="wall-strike">${d.ce_wall}</div>
            <div class="wall-oi">OI: ${fmt(d.ce_wall_oi)}</div>
        </div>
        <div class="wall wall-pe">
            <div class="wall-label">PE WALL (support)</div>
            <div class="wall-strike">${d.pe_wall}</div>
            <div class="wall-oi">OI: ${fmt(d.pe_wall_oi)}</div>
        </div>
    </div>
    <div class="walls">
        <div class="stat"><div class="stat-label">CE OI Chg</div><div class="stat-value ${d.total_ce_chg > 0 ? 'bearish' : 'bullish'}">${fmt(d.total_ce_chg)}</div></div>
        <div class="stat"><div class="stat-label">PE OI Chg</div><div class="stat-value ${d.total_pe_chg > 0 ? 'bullish' : 'bearish'}">${fmt(d.total_pe_chg)}</div></div>
    </div>
    <div class="table-wrap" style="margin-top:8px">
    <table>
        <tr>
            <th>OI</th><th>Chg</th><th>IV</th><th>LTP</th>
            <th class="strike-col">STRIKE</th>
            <th>LTP</th><th>IV</th><th>Chg</th><th>OI</th>
        </tr>`;

    d.strikes.forEach(s => {
        let ceBar = maxOI > 0 ? Math.round(s.ce_oi / maxOI * 40) : 0;
        let peBar = maxOI > 0 ? Math.round(s.pe_oi / maxOI * 40) : 0;
        let ceChgCls = s.ce_chg_oi > 0 ? 'chg-neg' : s.ce_chg_oi < 0 ? 'chg-pos' : '';
        let peChgCls = s.pe_chg_oi > 0 ? 'chg-pos' : s.pe_chg_oi < 0 ? 'chg-neg' : '';
        let rowCls = s.is_atm ? 'atm-row' : '';

        html += `<tr class="${rowCls}">
            <td>${fmt(s.ce_oi)} <span class="oi-bar ce-bar" style="width:${ceBar}px"></span></td>
            <td class="${ceChgCls}">${fmt(s.ce_chg_oi)}</td>
            <td>${s.ce_iv}</td>
            <td>${s.ce_ltp}</td>
            <td class="strike-col">${s.strike}</td>
            <td>${s.pe_ltp}</td>
            <td>${s.pe_iv}</td>
            <td class="${peChgCls}">${fmt(s.pe_chg_oi)}</td>
            <td><span class="oi-bar pe-bar" style="width:${peBar}px"></span> ${fmt(s.pe_oi)}</td>
        </tr>`;
    });

    html += '</table></div>';
    document.getElementById('content').innerHTML = html;
    document.getElementById('ts').textContent =
        d.expiry + ' · updated ' + c.timestamp;
}

async function fetchData() {
    try {
        let r = await fetch('/api/data');
        allData = await r.json();
        renderSymbol();
    } catch(e) {
        document.getElementById('content').innerHTML =
            '<div class="loading">Fetch error — retrying...</div>';
    }
}

fetchData();
setInterval(fetchData, 30000);  // poll server every 30s
</script>
</body>
</html>
"""

# ─── START ───
if __name__ == "__main__":
    t = threading.Thread(target=refresh_data, daemon=True)
    t.start()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
