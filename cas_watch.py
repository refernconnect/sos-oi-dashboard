#!/usr/bin/env python3
"""
cas_watch.py - NSE Closing Auction Session logger
Service: sos-oi-dashboard (Railway project: exciting-insight)
Writes to: sos-stock-radar Postgres via DATABASE_URL

LOG-AND-STORE ONLY. No alerts, no orders, no derived signals.
Everything (elasticity, synthetic index, divergence) is computed
later from order_book JSONB. Deliberately left out of this phase.

Env:
  DATABASE_URL       (required)  Postgres connection string
  CAS_POLL_SECONDS   (default 5)
  CAS_WINDOW_START   (default 15:14)  IST
  CAS_WINDOW_END     (default 15:41)  IST
  CAS_LOG_LEVEL      (default INFO)

Deps: requests, psycopg2-binary
"""

import os
import sys
import time
import logging
import threading
import datetime as dt
from email.utils import parsedate_to_datetime

import requests
import psycopg2
from psycopg2.extras import execute_values, Json

# ----------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------

IST = dt.timezone(dt.timedelta(hours=5, minutes=30))

NSE_HOME = "https://www.nseindia.com"
CAS_PAGE = "https://www.nseindia.com/market-data/closing-auction-session"
CAS_API = "https://www.nseindia.com/api/NextApi/apiClient/casApi?functionName=getCASData"

DATABASE_URL = os.getenv("DATABASE_URL")
POLL_SECONDS = float(os.getenv("CAS_POLL_SECONDS", "5"))
WINDOW_START = os.getenv("CAS_WINDOW_START", "15:14")
WINDOW_END = os.getenv("CAS_WINDOW_END", "15:41")

COOKIE_MAX_AGE = 600          # re-prime NSE session every 10 min
CLOCK_RESYNC_SECS = 3600      # re-sync clock offset hourly
IDLE_SLEEP_MAX = 300          # never sleep more than 5 min in one go

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",   # no 'br' - avoids brotli dependency
    "Connection": "keep-alive",
}

logging.basicConfig(
    level=os.getenv("CAS_LOG_LEVEL", "INFO"),
    format="%(asctime)s [cas_watch] %(levelname)s %(message)s",
)
log = logging.getLogger("cas_watch")


# ----------------------------------------------------------------------
# Clock - DO NOT trust the container clock.
# Railway containers have been observed 5.5h off. IST time is derived
# from NSE's own HTTP Date header and held as an offset against the
# monotonic clock. This is the July-CONVICTION class of bug, designed out.
# ----------------------------------------------------------------------

_clock_offset = 0.0
_clock_synced_at = 0.0


def sync_clock(session):
    """Set _clock_offset from NSE's HTTP Date header."""
    global _clock_offset, _clock_synced_at
    try:
        r = session.get(NSE_HOME, timeout=15)
        hdr = r.headers.get("Date")
        if not hdr:
            raise ValueError("no Date header")
        server_utc = parsedate_to_datetime(hdr)
        if server_utc.tzinfo is None:
            server_utc = server_utc.replace(tzinfo=dt.timezone.utc)
        _clock_offset = server_utc.timestamp() - time.time()
        _clock_synced_at = time.time()
        log.info(
            "clock synced: offset %.1fs -> IST now %s",
            _clock_offset,
            now_ist().strftime("%Y-%m-%d %H:%M:%S"),
        )
    except Exception as e:
        log.warning("clock sync failed (%s); keeping offset %.1fs", e, _clock_offset)
        _clock_synced_at = time.time()


def now_ist():
    return dt.datetime.fromtimestamp(time.time() + _clock_offset, IST)


def hhmm_today(now, hhmm):
    h, m = [int(x) for x in hhmm.split(":")]
    return now.replace(hour=h, minute=m, second=0, microsecond=0)


# ----------------------------------------------------------------------
# NSE session
# ----------------------------------------------------------------------

def new_session():
    """Cookie-primed requests session. NSE rejects cold API hits."""
    s = requests.Session()
    s.headers.update(BROWSER_HEADERS)
    s.get(NSE_HOME, timeout=15)
    s.get(CAS_PAGE, timeout=15)
    log.info("nse session primed (%d cookies)", len(s.cookies))
    return s


def fetch_cas(session):
    """Return parsed CAS payload, or None. Re-primes once on failure."""
    headers = {
        "Referer": CAS_PAGE,
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
        "X-Requested-With": "XMLHttpRequest",
    }
    for attempt in (1, 2):
        try:
            r = session.get(CAS_API, headers=headers, timeout=15)
            if r.status_code != 200:
                raise ValueError("http %s" % r.status_code)
            return r.json()
        except Exception as e:
            log.warning("cas fetch attempt %d failed: %s", attempt, e)
            if attempt == 1:
                try:
                    session.cookies.clear()
                    session.get(NSE_HOME, timeout=15)
                    session.get(CAS_PAGE, timeout=15)
                except Exception as e2:
                    log.warning("re-prime failed: %s", e2)
            else:
                return None
    return None


# ----------------------------------------------------------------------
# Database
# ----------------------------------------------------------------------

DDL = """
CREATE TABLE IF NOT EXISTS cas_snapshots (
    id                BIGSERIAL PRIMARY KEY,
    snap_ts           TIMESTAMPTZ      NOT NULL,
    session_date      DATE             NOT NULL,
    status_msg        TEXT,
    symbol            TEXT             NOT NULL,
    series            TEXT,
    reference_price   DOUBLE PRECISION,
    prev_close        DOUBLE PRECISION,
    upper_band        DOUBLE PRECISION,
    lower_band        DOUBLE PRECISION,
    iep               DOUBLE PRECISION,
    iep_change        DOUBLE PRECISION,
    iep_pct_change    DOUBLE PRECISION,
    iiq_at_ep         DOUBLE PRECISION,
    iiq_at_mo         DOUBLE PRECISION,
    ato_buy_qty       BIGINT,
    ato_sell_qty      BIGINT,
    total_buy_qty     BIGINT,
    total_sell_qty    BIGINT,
    best_bid_price    DOUBLE PRECISION,
    best_bid_qty      BIGINT,
    best_ask_price    DOUBLE PRECISION,
    best_ask_qty      BIGINT,
    last_traded_price DOUBLE PRECISION,
    avg_trd_price     DOUBLE PRECISION,
    open_price        DOUBLE PRECISION,
    high_price        DOUBLE PRECISION,
    low_price         DOUBLE PRECISION,
    final_price       DOUBLE PRECISION,
    final_qty         BIGINT,
    final_value       DOUBLE PRECISION,
    indicative_value  DOUBLE PRECISION,
    nse_update_time   TEXT,
    order_book        JSONB,
    CONSTRAINT cas_snapshots_uniq UNIQUE (session_date, snap_ts, symbol)
);

CREATE INDEX IF NOT EXISTS cas_snapshots_sym_idx
    ON cas_snapshots (session_date, symbol, snap_ts);

CREATE INDEX IF NOT EXISTS cas_snapshots_ts_idx
    ON cas_snapshots (snap_ts);

CREATE TABLE IF NOT EXISTS cas_session_totals (
    id             BIGSERIAL PRIMARY KEY,
    snap_ts        TIMESTAMPTZ NOT NULL,
    session_date   DATE        NOT NULL,
    status_msg     TEXT,
    total_value    DOUBLE PRECISION,
    total_quantity BIGINT,
    symbol_count   INTEGER,
    CONSTRAINT cas_session_totals_uniq UNIQUE (session_date, snap_ts)
);
"""

INSERT_SNAP = """
INSERT INTO cas_snapshots (
    snap_ts, session_date, status_msg, symbol, series,
    reference_price, prev_close, upper_band, lower_band,
    iep, iep_change, iep_pct_change, iiq_at_ep, iiq_at_mo,
    ato_buy_qty, ato_sell_qty, total_buy_qty, total_sell_qty,
    best_bid_price, best_bid_qty, best_ask_price, best_ask_qty,
    last_traded_price, avg_trd_price, open_price, high_price, low_price,
    final_price, final_qty, final_value, indicative_value,
    nse_update_time, order_book
) VALUES %s
ON CONFLICT ON CONSTRAINT cas_snapshots_uniq DO NOTHING
"""

INSERT_TOTALS = """
INSERT INTO cas_session_totals (
    snap_ts, session_date, status_msg, total_value, total_quantity, symbol_count
) VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT ON CONSTRAINT cas_session_totals_uniq DO NOTHING
"""


def connect_db():
    if not DATABASE_URL:
        log.error("DATABASE_URL not set - add it as a Railway reference variable")
        sys.exit(1)
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(DDL)
    log.info("db ready")
    return conn


def _f(d, k):
    """Float or None."""
    v = d.get(k)
    if v is None or v == "" or v == "-":
        return None
    try:
        return float(str(v).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _i(d, k):
    v = _f(d, k)
    return int(v) if v is not None else None


def persist(conn, payload, snap_ts):
    rows = payload.get("data") or []
    if not rows:
        return 0

    status = payload.get("statusMsg")
    sess_date = snap_ts.date()

    values = []
    for r in rows:
        sym = r.get("symbol")
        if not sym:
            continue
        values.append((
            snap_ts, sess_date, status, sym, r.get("series"),
            _f(r, "refrencePrice"),          # NSE's own typo - keep it
            _f(r, "prevClose"),
            _f(r, "upperBand"), _f(r, "lowerBand"),
            _f(r, "IEP"), _f(r, "change"), _f(r, "perChange"),
            _f(r, "iiqAtEP"), _f(r, "iiqAtMO"),
            _i(r, "atoBuyQuantity"), _i(r, "atoSellQuantity"),
            _i(r, "totalBuyQuantity"), _i(r, "totalSellQuantity"),
            _f(r, "bestBidPrice"), _i(r, "bestBidQty"),
            _f(r, "bestAskPrice"), _i(r, "bestAskQty"),
            _f(r, "lastTradedPrice"), _f(r, "avgTrdPrice"),
            _f(r, "openPrice"), _f(r, "highPrice"), _f(r, "lowPrice"),
            _f(r, "finalPrice"), _i(r, "finalQuantity"), _f(r, "finalValue"),
            _f(r, "indicativeValue"),
            r.get("lastUpdateTime"),
            Json(r.get("orderBook") or []),
        ))

    if not values:
        return 0

    with conn.cursor() as cur:
        execute_values(cur, INSERT_SNAP, values, page_size=500)
        cur.execute(
            INSERT_TOTALS,
            (snap_ts, sess_date, status,
             _f(payload, "totalValue"), _i(payload, "totalQuantity"), len(values)),
        )
    return len(values)


# ----------------------------------------------------------------------
# Main loop
# ----------------------------------------------------------------------

def run_window(conn, session, end_dt):
    """Poll tight through the CAS window."""
    log.info("=== CAS WINDOW OPEN until %s ===", end_dt.strftime("%H:%M"))
    snaps = 0
    primed_at = time.time()

    while now_ist() <= end_dt:
        cycle_start = time.time()

        if time.time() - primed_at > COOKIE_MAX_AGE:
            try:
                session = new_session()
                primed_at = time.time()
            except Exception as e:
                log.warning("re-prime failed: %s", e)

        payload = fetch_cas(session)
        if payload:
            snap_ts = now_ist()
            try:
                n = persist(conn, payload, snap_ts)
                snaps += 1
                if snaps % 12 == 1:
                    log.info(
                        "%s | %s | %d symbols | snap #%d",
                        snap_ts.strftime("%H:%M:%S"),
                        payload.get("statusMsg"),
                        n, snaps,
                    )
            except Exception as e:
                log.error("persist failed: %s", e)

        elapsed = time.time() - cycle_start
        time.sleep(max(0.0, POLL_SECONDS - elapsed))

    log.info("=== CAS WINDOW CLOSED - %d snapshots ===", snaps)
    return session


# ======================================================================
# OPTION-CHAIN RECORDER (v2) - runs as a daemon thread beside the CAS loop
# ----------------------------------------------------------------------
# Purpose: forward-record the Nifty option chain (LTP, bid/ask, IV, OI) plus
# India VIX at the two timestamps the overnight short-vol study needs
# (~09:20 and ~15:20), and densely through 15:09-15:41 so the post-CAS
# option repricing window is captured. Kite serves no history for expired
# contracts, so this is the only way this data ever exists.
#
# Fully isolated: own NSE session, own DB connection, every step wrapped.
# A failure here can never stop CAS logging.
#
# Env:
#   CHAIN_ENABLED     (default 1)
#   CHAIN_SYMBOLS     (default NIFTY)   comma list, e.g. NIFTY,BANKNIFTY
#   CHAIN_HOT_SECS    (default 20)      cadence inside hot windows
#   CHAIN_WARM_SECS   (default 300)     cadence 09:31-15:09
#   CHAIN_STRIKES     (default 20)      strikes each side of ATM
#   CHAIN_EXPIRIES    (default 3)       nearest expiries kept
# ======================================================================

CHAIN_ENABLED = os.getenv("CHAIN_ENABLED", "1") == "1"
CHAIN_SYMBOLS = [s.strip().upper() for s in os.getenv("CHAIN_SYMBOLS", "NIFTY").split(",") if s.strip()]
CHAIN_HOT_SECS = float(os.getenv("CHAIN_HOT_SECS", "20"))
CHAIN_WARM_SECS = float(os.getenv("CHAIN_WARM_SECS", "300"))
CHAIN_STRIKES = int(os.getenv("CHAIN_STRIKES", "20"))
CHAIN_EXPIRIES = int(os.getenv("CHAIN_EXPIRIES", "3"))
CHAIN_PAGE = "https://www.nseindia.com/option-chain"
CHAIN_API = "https://www.nseindia.com/api/option-chain-indices?symbol={sym}"
INDICES_API = "https://www.nseindia.com/api/allIndices"
STRIKE_STEP = {"NIFTY": 50, "BANKNIFTY": 100, "FINNIFTY": 50, "MIDCPNIFTY": 25}
# (start, end) IST windows polled at CHAIN_HOT_SECS; everything between the
# first start and last end is polled at CHAIN_WARM_SECS
CHAIN_HOT = [("09:14", "09:31"), ("15:09", "15:41")]

CHAIN_DDL = """
CREATE TABLE IF NOT EXISTS oc_meta (
    snap_ts      timestamptz NOT NULL,
    symbol       text        NOT NULL,
    underlying   numeric,
    atm_strike   numeric,
    india_vix    numeric,
    expiries     text[],
    rows_written integer,
    PRIMARY KEY (snap_ts, symbol)
);
CREATE TABLE IF NOT EXISTS oc_chain (
    snap_ts   timestamptz NOT NULL,
    symbol    text        NOT NULL,
    expiry    date        NOT NULL,
    strike    numeric     NOT NULL,
    ce_ltp numeric, ce_bid numeric, ce_ask numeric, ce_iv numeric,
    ce_oi bigint, ce_oi_chg bigint, ce_vol bigint,
    pe_ltp numeric, pe_bid numeric, pe_ask numeric, pe_iv numeric,
    pe_oi bigint, pe_oi_chg bigint, pe_vol bigint,
    PRIMARY KEY (snap_ts, symbol, expiry, strike)
);
CREATE INDEX IF NOT EXISTS oc_chain_sym_exp_ts ON oc_chain (symbol, expiry, snap_ts);
"""


def chain_session():
    s = requests.Session()
    s.headers.update(BROWSER_HEADERS)
    s.get(NSE_HOME, timeout=15)
    s.get(CHAIN_PAGE, timeout=15)
    log.info("[chain] nse session primed (%d cookies)", len(s.cookies))
    return s


def _num(x):
    try:
        if x is None or x == "" or x == "-":
            return None
        return float(x)
    except Exception:
        return None


def _int(x):
    v = _num(x)
    return int(v) if v is not None else None


def parse_expiry(s):
    for fmt in ("%d-%b-%Y", "%d-%b-%y", "%Y-%m-%d"):
        try:
            return dt.datetime.strptime(s, fmt).date()
        except Exception:
            pass
    return None


def fetch_vix(session):
    try:
        r = session.get(INDICES_API, headers={"Referer": CHAIN_PAGE,
                                              "X-Requested-With": "XMLHttpRequest"}, timeout=15)
        for row in (r.json().get("data") or []):
            if (row.get("index") or "").upper().replace(" ", "") == "INDIAVIX":
                return _num(row.get("last"))
    except Exception as e:
        log.debug("[chain] vix fetch failed: %s", e)
    return None


def fetch_chain(session, sym):
    r = session.get(CHAIN_API.format(sym=sym),
                    headers={"Referer": CHAIN_PAGE, "X-Requested-With": "XMLHttpRequest"},
                    timeout=20)
    if r.status_code != 200:
        raise RuntimeError("chain %s http %s" % (sym, r.status_code))
    j = r.json()
    rec = j.get("records") or {}
    data = rec.get("data") or []
    under = _num(rec.get("underlyingValue"))
    if not data or under is None:
        raise RuntimeError("chain %s empty" % sym)
    return under, data


def build_rows(sym, under, data, snap_ts):
    step = STRIKE_STEP.get(sym, 50)
    atm = round(under / step) * step
    lo, hi = atm - CHAIN_STRIKES * step, atm + CHAIN_STRIKES * step
    exps = sorted({parse_expiry(d.get("expiryDate")) for d in data if d.get("expiryDate")} - {None})
    keep = set(exps[:CHAIN_EXPIRIES])
    rows = []
    for d in data:
        e = parse_expiry(d.get("expiryDate") or "")
        k = _num(d.get("strikePrice"))
        if e not in keep or k is None or k < lo or k > hi:
            continue
        ce, pe = d.get("CE") or {}, d.get("PE") or {}
        rows.append((
            snap_ts, sym, e, k,
            _num(ce.get("lastPrice")), _num(ce.get("bidprice")), _num(ce.get("askPrice")),
            _num(ce.get("impliedVolatility")), _int(ce.get("openInterest")),
            _int(ce.get("changeinOpenInterest")), _int(ce.get("totalTradedVolume")),
            _num(pe.get("lastPrice")), _num(pe.get("bidprice")), _num(pe.get("askPrice")),
            _num(pe.get("impliedVolatility")), _int(pe.get("openInterest")),
            _int(pe.get("changeinOpenInterest")), _int(pe.get("totalTradedVolume")),
        ))
    return atm, [str(e) for e in sorted(keep)], rows


def persist_chain(conn, sym, under, atm, vix, exps, rows, snap_ts):
    with conn.cursor() as cur:
        if rows:
            execute_values(cur, """
                INSERT INTO oc_chain (snap_ts, symbol, expiry, strike,
                    ce_ltp, ce_bid, ce_ask, ce_iv, ce_oi, ce_oi_chg, ce_vol,
                    pe_ltp, pe_bid, pe_ask, pe_iv, pe_oi, pe_oi_chg, pe_vol)
                VALUES %s ON CONFLICT DO NOTHING""", rows, page_size=500)
        cur.execute("""
            INSERT INTO oc_meta (snap_ts, symbol, underlying, atm_strike, india_vix, expiries, rows_written)
            VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
            (snap_ts, sym, under, atm, vix, exps, len(rows)))


def chain_cadence(now):
    """Seconds to sleep, or None when outside the recording day."""
    hm = now.strftime("%H:%M")
    for a, b in CHAIN_HOT:
        if a <= hm <= b:
            return CHAIN_HOT_SECS
    if CHAIN_HOT[0][0] <= hm <= CHAIN_HOT[-1][1]:
        return CHAIN_WARM_SECS
    return None


def chain_loop():
    log.info("[chain] recorder starting: symbols=%s hot=%ss warm=%ss strikes=+-%d expiries=%d",
             CHAIN_SYMBOLS, CHAIN_HOT_SECS, CHAIN_WARM_SECS, CHAIN_STRIKES, CHAIN_EXPIRIES)
    conn = None
    session = None
    primed_at = 0.0
    while True:
        try:
            if conn is None:
                conn = psycopg2.connect(DATABASE_URL)
                conn.autocommit = True
                with conn.cursor() as cur:
                    cur.execute(CHAIN_DDL)
                log.info("[chain] db ready")
            now = now_ist()
            if now.weekday() >= 5:
                time.sleep(IDLE_SLEEP_MAX)
                continue
            cad = chain_cadence(now)
            if cad is None:
                start_dt = hhmm_today(now, CHAIN_HOT[0][0])
                if now < start_dt:
                    wait = (start_dt - now).total_seconds()
                else:
                    tomorrow = (now + dt.timedelta(days=1)).replace(hour=0, minute=1, second=0, microsecond=0)
                    wait = (tomorrow - now).total_seconds()
                time.sleep(max(5.0, min(wait, IDLE_SLEEP_MAX)))
                continue
            if session is None or time.time() - primed_at > COOKIE_MAX_AGE:
                session = chain_session()
                primed_at = time.time()
            snap_ts = now.replace(microsecond=0)
            vix = fetch_vix(session)
            for sym in CHAIN_SYMBOLS:
                try:
                    under, data = fetch_chain(session, sym)
                    atm, exps, rows = build_rows(sym, under, data, snap_ts)
                    persist_chain(conn, sym, under, atm, vix, exps, rows, snap_ts)
                    log.info("[chain] %s %s spot=%.1f atm=%d vix=%s rows=%d exps=%s",
                             snap_ts.strftime("%H:%M:%S"), sym, under, atm, vix, len(rows), exps)
                except Exception as e:
                    log.warning("[chain] %s snapshot failed: %s", sym, e)
                    session = None          # force re-prime next pass
            time.sleep(cad)
        except Exception as e:
            log.exception("[chain] loop error: %s", e)
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass
            conn = None
            session = None
            time.sleep(30)



def main():
    log.info("cas_watch starting (poll=%.1fs window=%s-%s IST)",
             POLL_SECONDS, WINDOW_START, WINDOW_END)

    conn = connect_db()
    session = new_session()
    sync_clock(session)
    if CHAIN_ENABLED:
        threading.Thread(target=chain_loop, name="chain", daemon=True).start()
    else:
        log.info("[chain] disabled via CHAIN_ENABLED=0")

    while True:
        try:
            if time.time() - _clock_synced_at > CLOCK_RESYNC_SECS:
                sync_clock(session)

            now = now_ist()

            # Sat/Sun - idle
            if now.weekday() >= 5:
                log.info("weekend (%s) - idling", now.strftime("%a"))
                time.sleep(IDLE_SLEEP_MAX)
                continue

            start_dt = hhmm_today(now, WINDOW_START)
            end_dt = hhmm_today(now, WINDOW_END)

            if now < start_dt:
                wait = (start_dt - now).total_seconds()
                log.info("waiting %.0f min for CAS window", wait / 60.0)
                time.sleep(min(wait, IDLE_SLEEP_MAX))

            elif now <= end_dt:
                try:
                    session = new_session()   # fresh cookies entering the window
                except Exception as e:
                    log.warning("pre-window prime failed: %s", e)
                session = run_window(conn, session, end_dt)

            else:
                tomorrow = (now + dt.timedelta(days=1)).replace(
                    hour=0, minute=1, second=0, microsecond=0)
                wait = (tomorrow - now).total_seconds()
                log.info("post-window - idling %.1f h", wait / 3600.0)
                time.sleep(min(wait, IDLE_SLEEP_MAX))

        except KeyboardInterrupt:
            log.info("interrupted - exiting")
            break
        except Exception as e:
            log.exception("loop error: %s", e)
            time.sleep(30)
            try:
                conn = connect_db()
                session = new_session()
                sync_clock(session)
            except Exception as e2:
                log.error("recovery failed: %s", e2)


if __name__ == "__main__":
    main()
