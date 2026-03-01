"""
Early PumpSwap Monitor — 4 Channel Alert Bot

Bot 1: Token < 24h old | MC >= $200K | Vol >= $100K | Has profile
Bot 2: Token < 24h old | MC >= $1M   | Vol >= $100K | Has profile
Bot 3: Token from Bot 1 that dumps 70% from its ATH
Bot 4: Token from Bot 3 that recovers 50% from its ATL (MC still >= $200K)

Setup:
  1. pip install requests websocket-client psycopg2-binary python-dotenv
  2. Fill in .env (see .env.example)
  3. python3 early_monitor.py
"""

import json
import os
import re
import threading
import time
from datetime import datetime
from pathlib import Path

import requests
import websocket
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

# ── Telegram tokens ───────────────────────────────────────────────────────────
BOT_TOKEN_1 = os.getenv("EARLY_BOT_TOKEN_1", "")  # < 24h, MC >= $200K
BOT_TOKEN_2 = os.getenv("EARLY_BOT_TOKEN_2", "")  # < 24h, MC >= $1M
BOT_TOKEN_3 = os.getenv("EARLY_BOT_TOKEN_3", "")  # dump 70% from ATH
BOT_TOKEN_4 = os.getenv("EARLY_BOT_TOKEN_4", "")  # recover 50% from ATL
CHAT_ID      = os.getenv("EARLY_CHAT_ID", "")
DATABASE_URL = os.getenv("EARLY_DATABASE_URL", "")

FILTER_POLL_SEC = int(os.getenv("EARLY_FILTER_POLL_SEC", 60))

# ── Filter config ─────────────────────────────────────────────────────────────
F1 = {
    "key":      "f1",
    "min_mcap": float(os.getenv("E_F1_MIN_MCAP", 200_000)),
    "max_mcap": float("inf"),
    "max_age_h": 24.0,
    "min_vol":  float(os.getenv("E_F1_MIN_VOL", 100_000)),
    "label":    "Early Gem — MC >$200K",
}
F2 = {
    "key":      "f2",
    "min_mcap": float(os.getenv("E_F2_MIN_MCAP", 1_000_000)),
    "max_mcap": float("inf"),
    "max_age_h": 24.0,
    "min_vol":  float(os.getenv("E_F2_MIN_VOL", 100_000)),
    "label":    "Early Moon — MC >$1M",
}

DUMP_THRESHOLD     = float(os.getenv("DUMP_THRESHOLD",     0.70))  # 70% drop from ATH
RECOVERY_THRESHOLD = float(os.getenv("RECOVERY_THRESHOLD", 0.50))  # 50% rise from ATL

PUMPPORTAL_WS   = "wss://pumpportal.fun/api/data"
DEXSCREENER_API = "https://api.dexscreener.com/latest/dex/tokens/{}"

TG_API_1 = f"https://api.telegram.org/bot{BOT_TOKEN_1}"
TG_API_2 = f"https://api.telegram.org/bot{BOT_TOKEN_2}"
TG_API_3 = f"https://api.telegram.org/bot{BOT_TOKEN_3}"
TG_API_4 = f"https://api.telegram.org/bot{BOT_TOKEN_4}"

lock             = threading.Lock()
seen_set: set    = set()
filter_state     = {}
update_offsets   = {"f1": 0, "f2": 0, "f3": 0, "f4": 0}

# In-memory ATH/ATL tracking (also persisted to DB)
# ath_tracking:      {address: {"ath": float, "alerted_dump": bool}}
# recovery_tracking: {address: {"atl": float, "alerted_recovery": bool}}
ath_tracking      = {}
recovery_tracking = {}


# ─── DATABASE ─────────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS early_seen (
                    address       TEXT PRIMARY KEY,
                    created_at_ms BIGINT DEFAULT 0,
                    added_at      TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS early_filter_state (
                    filter_key TEXT NOT NULL,
                    address    TEXT NOT NULL,
                    state      TEXT NOT NULL,
                    PRIMARY KEY (filter_key, address)
                )
            """)
            # Permanently alerted tokens per filter (never re-alert)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS early_alerted (
                    filter_key TEXT NOT NULL,
                    address    TEXT NOT NULL,
                    PRIMARY KEY (filter_key, address)
                )
            """)
            # ATH tracking — tokens from Bot 1
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ath_tracking (
                    address       TEXT PRIMARY KEY,
                    ath_price     DOUBLE PRECISION DEFAULT 0,
                    alerted_dump  BOOLEAN DEFAULT FALSE,
                    updated_at    TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            # Recovery tracking — tokens from Bot 3
            cur.execute("""
                CREATE TABLE IF NOT EXISTS recovery_tracking (
                    address            TEXT PRIMARY KEY,
                    atl_price          DOUBLE PRECISION DEFAULT 0,
                    alerted_recovery   BOOLEAN DEFAULT FALSE,
                    updated_at         TIMESTAMPTZ DEFAULT NOW()
                )
            """)
        conn.commit()
    print("  [DB] Tables ready.")


def db_load_seen() -> set:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT address FROM early_seen")
            return {r[0] for r in cur.fetchall()}


def db_add_seen(address: str, created_at_ms: int = 0):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO early_seen (address, created_at_ms)
                    VALUES (%s, %s) ON CONFLICT DO NOTHING
                """, (address, created_at_ms))
            conn.commit()
    except Exception as e:
        print(f"  [DB] db_add_seen error: {e}")


def db_load_filter_state(key: str, state_type: str) -> set:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT address FROM early_filter_state
                WHERE filter_key = %s AND state = %s
            """, (key, state_type))
            return {r[0] for r in cur.fetchall()}


def db_save_filter_state(key: str, state_type: str, addresses: set):
    if not addresses:
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM early_filter_state WHERE filter_key=%s AND state=%s",
                            (key, state_type))
                psycopg2.extras.execute_values(cur,
                    "INSERT INTO early_filter_state (filter_key, address, state) VALUES %s ON CONFLICT DO NOTHING",
                    [(key, a, state_type) for a in addresses])
            conn.commit()
    except Exception as e:
        print(f"  [DB] db_save_filter_state error: {e}")


def db_load_alerted(key: str) -> set:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT address FROM early_alerted WHERE filter_key = %s", (key,))
            return {r[0] for r in cur.fetchall()}


def db_add_alerted(key: str, address: str):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO early_alerted (filter_key, address)
                    VALUES (%s, %s) ON CONFLICT DO NOTHING
                """, (key, address))
            conn.commit()
    except Exception as e:
        print("  [DB] db_add_alerted error: " + str(e))


def db_load_ath_tracking() -> dict:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT address, ath_price, alerted_dump FROM ath_tracking")
            return {r[0]: {"ath": r[1], "alerted_dump": r[2]} for r in cur.fetchall()}


def db_upsert_ath(address: str, ath_price: float, alerted_dump: bool):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO ath_tracking (address, ath_price, alerted_dump, updated_at)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (address) DO UPDATE
                    SET ath_price = EXCLUDED.ath_price,
                        alerted_dump = EXCLUDED.alerted_dump,
                        updated_at = NOW()
                """, (address, ath_price, alerted_dump))
            conn.commit()
    except Exception as e:
        print(f"  [DB] db_upsert_ath error: {e}")


def db_load_recovery_tracking() -> dict:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT address, atl_price, alerted_recovery FROM recovery_tracking")
            return {r[0]: {"atl": r[1], "alerted_recovery": r[2]} for r in cur.fetchall()}


def db_upsert_recovery(address: str, atl_price: float, alerted_recovery: bool):
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO recovery_tracking (address, atl_price, alerted_recovery, updated_at)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (address) DO UPDATE
                    SET atl_price = EXCLUDED.atl_price,
                        alerted_recovery = EXCLUDED.alerted_recovery,
                        updated_at = NOW()
                """, (address, atl_price, alerted_recovery))
            conn.commit()
    except Exception as e:
        print(f"  [DB] db_upsert_recovery error: {e}")


def db_cleanup(max_age_hours: float):
    cutoff_ms = int(time.time() * 1000 - max_age_hours * 3600 * 1000)
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                # Remove from seen
                cur.execute("DELETE FROM early_seen WHERE created_at_ms > 0 AND created_at_ms < %s",
                            (cutoff_ms,))
                r1 = cur.rowcount
                # Remove from filter state
                cur.execute("""
                    DELETE FROM early_filter_state
                    WHERE address IN (
                        SELECT fs.address FROM early_filter_state fs
                        LEFT JOIN early_seen s ON fs.address = s.address
                        WHERE s.address IS NULL
                    )
                """)
                r2 = cur.rowcount
                # Remove from ath_tracking
                cur.execute("""
                    DELETE FROM ath_tracking
                    WHERE address IN (
                        SELECT at.address FROM ath_tracking at
                        LEFT JOIN early_seen s ON at.address = s.address
                        WHERE s.address IS NULL
                    )
                """)
                r3 = cur.rowcount
                # Remove from recovery_tracking
                cur.execute("""
                    DELETE FROM recovery_tracking
                    WHERE address IN (
                        SELECT rt.address FROM recovery_tracking rt
                        LEFT JOIN early_seen s ON rt.address = s.address
                        WHERE s.address IS NULL
                    )
                """)
                r4 = cur.rowcount
            conn.commit()
        total = r1 + r2 + r3 + r4
        if total:
            print("  [DB] Cleanup: " + str(r1) + " seen, " + str(r2) + " filter_state, " +
                  str(r3) + " ath, " + str(r4) + " recovery removed")
    except Exception as e:
        print("  [DB] cleanup error: " + str(e))


# ─── HELPERS ──────────────────────────────────────────────────────────────────

def esc(s): return s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
def fmt_usd(v): return "$" + "{:,.0f}".format(v) if v else "N/A"
def fmt_pct(v): return ("+" if v > 0 else "") + "{:.1f}%".format(v) if v else "N/A"

def time_ago(ms):
    if not ms: return "unknown"
    secs = int(time.time() - ms / 1000)
    if secs < 60:     return str(secs) + "s ago"
    elif secs < 3600: return str(secs // 60) + "m ago"
    elif secs < 86400:
        h = secs // 3600; m = (secs % 3600) // 60
        return (str(h) + "h " + str(m) + "m ago") if m else (str(h) + "h ago")
    else:
        d = secs // 86400; h = (secs % 86400) // 3600
        return (str(d) + "d " + str(h) + "h ago") if h else (str(d) + "d ago")

def age_hours(pair):
    ms = pair.get("pairCreatedAt", 0)
    if not ms: return 0
    return (time.time() * 1000 - ms) / 3_600_000

def get_price(pair) -> float:
    try: return float(pair.get("priceUsd") or 0)
    except: return 0.0

def fetch_token_data(addresses: list) -> list:
    all_pairs = []
    headers = {"User-Agent": "Mozilla/5.0"}
    for i in range(0, len(addresses), 30):
        batch = addresses[i:i+30]
        try:
            r = requests.get(DEXSCREENER_API.format(",".join(batch)),
                             headers=headers, timeout=15)
            if r.ok:
                pairs = r.json().get("pairs") or []
                all_pairs.extend(p for p in pairs
                                 if p.get("chainId") == "solana"
                                 and "pump" in p.get("dexId","").lower())
        except Exception as e:
            print(f"  [API] error: {e}")
        time.sleep(0.3)
    # If multiple pairs exist for same token, keep the one with highest liquidity
    best = {}
    for p in all_pairs:
        addr = (p.get("baseToken") or {}).get("address", "")
        if not addr: continue
        liq = (p.get("liquidity") or {}).get("usd", 0) or 0
        if addr not in best or liq > ((best[addr].get("liquidity") or {}).get("usd", 0) or 0):
            best[addr] = p
    return list(best.values())

def passes_filter(pair, flt) -> bool:
    mc  = pair.get("marketCap", 0) or 0
    vol = (pair.get("volume") or {}).get("h24", 0) or 0
    age = age_hours(pair)
    return (
        mc >= flt["min_mcap"] and
        mc <= flt["max_mcap"] and
        age <= flt["max_age_h"] and
        vol >= flt["min_vol"]
    )

def send_tg(api_url, text):
    try:
        r = requests.post(api_url + "/sendMessage", json={
            "chat_id": CHAT_ID, "text": text,
            "parse_mode": "HTML", "disable_web_page_preview": True,
        }, timeout=10)
        if not r.ok:
            requests.post(api_url + "/sendMessage", json={
                "chat_id": CHAT_ID,
                "text": re.sub(r"<[^>]+>", "", text),
                "disable_web_page_preview": True,
            }, timeout=10)
    except Exception as e:
        print(f"  [TG] error: {e}")

def social_url(platform, handle):
    if not handle: return None
    if "twitter"  in platform: return "https://twitter.com/" + handle
    if "telegram" in platform: return "https://t.me/" + handle
    return None


# ─── ALERT BUILDERS ───────────────────────────────────────────────────────────

def build_filter_alert(pair, label):
    base       = pair.get("baseToken", {})
    symbol     = esc(base.get("symbol", "?"))
    name       = esc(base.get("name", "?"))
    addr       = base.get("address", "?")
    pair_addr  = pair.get("pairAddress", "")
    price      = pair.get("priceUsd", "?")
    mc         = pair.get("marketCap", 0)
    vol        = (pair.get("volume") or {}).get("h24", 0)
    liq        = (pair.get("liquidity") or {}).get("usd", 0)
    chg        = pair.get("priceChange", {})
    txns_5m    = pair.get("txns", {}).get("m5", {})
    txns_1h    = pair.get("txns", {}).get("h1", {})
    created_ms = pair.get("pairCreatedAt", 0)
    created_str = datetime.fromtimestamp(created_ms/1000).strftime("%Y-%m-%d %H:%M:%S") if created_ms else "?"
    info    = pair.get("info") or {}
    sites   = info.get("websites") or []
    socials = info.get("socials") or []
    website = sites[0].get("url") if sites else None
    twitter = next((social_url(s.get("platform",""), s.get("handle",""))
                    for s in socials if "twitter"  in (s.get("platform") or "").lower()), None)
    tg_link = next((social_url(s.get("platform",""), s.get("handle",""))
                    for s in socials if "telegram" in (s.get("platform") or "").lower()), None)
    links = []
    if website: links.append("<a href='" + website + "'>Website</a>")
    if twitter: links.append("<a href='" + twitter + "'>Twitter</a>")
    if tg_link: links.append("<a href='" + tg_link + "'>Telegram</a>")

    return (
        "<b>" + label + "</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "<b>" + symbol + "</b>  |  " + name + "\n"
        "Created: <b>" + created_str + "</b>  (" + time_ago(created_ms) + ")\n\n"
        "Price:      <code>$" + str(price) + "</code>\n"
        "Market Cap: <code>" + fmt_usd(mc) + "</code>\n"
        "Liquidity:  <code>" + fmt_usd(liq) + "</code>\n"
        "Vol 24h:    <code>" + fmt_usd(vol) + "</code>\n\n"
        "Txns 5m: " + str(txns_5m.get("buys",0)) + "B / " + str(txns_5m.get("sells",0)) + "S\n"
        "Txns 1h: " + str(txns_1h.get("buys",0)) + "B / " + str(txns_1h.get("sells",0)) + "S\n\n"
        "Change: 5m " + fmt_pct(chg.get("m5")) + "  |  1h " + fmt_pct(chg.get("h1")) + "\n"
        "        6h " + fmt_pct(chg.get("h6")) + "  |  24h " + fmt_pct(chg.get("h24")) + "\n\n"
        "Contract:\n<code>" + addr + "</code>\n\n"
        "<a href='https://dexscreener.com/solana/" + pair_addr + "'>DexScreener</a>  |  "
        "<a href='https://pump.fun/coin/" + addr + "'>Pump.fun</a>\n" +
        ("  |  ".join(links) if links else "No socials")
    )


def build_dump_alert(pair, ath_price):
    base        = pair.get("baseToken", {})
    symbol      = esc(base.get("symbol", "?"))
    name        = esc(base.get("name", "?"))
    addr        = base.get("address", "?")
    pair_addr   = pair.get("pairAddress", "")
    cur_price   = get_price(pair)
    mc          = pair.get("marketCap", 0)
    liq         = (pair.get("liquidity") or {}).get("usd", 0)
    vol         = (pair.get("volume") or {}).get("h24", 0)
    chg         = pair.get("priceChange", {})
    txns_5m     = pair.get("txns", {}).get("m5", {})
    txns_1h     = pair.get("txns", {}).get("h1", {})
    created_ms  = pair.get("pairCreatedAt", 0)
    created_str = datetime.fromtimestamp(created_ms/1000).strftime("%Y-%m-%d %H:%M:%S") if created_ms else "?"
    created_ago = time_ago(created_ms) if created_ms else "unknown"
    drop_pct    = ((ath_price - cur_price) / ath_price * 100) if ath_price else 0

    return (
        "📉 <b>Dump Alert — " + symbol + " dumped " + "{:.1f}".format(drop_pct) + "% from ATH!</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "<b>" + symbol + "</b>  |  " + name + "\n"
        "Created: <b>" + created_str + "</b>  (" + created_ago + ")\n\n"
        "ATH Price:   <code>$" + "{:.8f}".format(ath_price) + "</code>\n"
        "Now Price:   <code>$" + "{:.8f}".format(cur_price) + "</code>\n"
        "Drop:        <b>-" + "{:.1f}".format(drop_pct) + "%</b>\n\n"
        "Market Cap:  <code>" + fmt_usd(mc) + "</code>\n"
        "Liquidity:   <code>" + fmt_usd(liq) + "</code>\n"
        "Vol 24h:     <code>" + fmt_usd(vol) + "</code>\n\n"
        "Txns 5m: " + str(txns_5m.get("buys",0)) + "B / " + str(txns_5m.get("sells",0)) + "S\n"
        "Txns 1h: " + str(txns_1h.get("buys",0)) + "B / " + str(txns_1h.get("sells",0)) + "S\n\n"
        "Change: 5m " + fmt_pct(chg.get("m5")) + "  |  1h " + fmt_pct(chg.get("h1")) + "\n"
        "        6h " + fmt_pct(chg.get("h6")) + "  |  24h " + fmt_pct(chg.get("h24")) + "\n\n"
        "Contract:\n<code>" + addr + "</code>\n\n"
        "<a href='https://dexscreener.com/solana/" + pair_addr + "'>DexScreener</a>  |  "
        "<a href='https://pump.fun/coin/" + addr + "'>Pump.fun</a>"
    )


def build_recovery_alert(pair, atl_price):
    base        = pair.get("baseToken", {})
    symbol      = esc(base.get("symbol", "?"))
    name        = esc(base.get("name", "?"))
    addr        = base.get("address", "?")
    pair_addr   = pair.get("pairAddress", "")
    cur_price   = get_price(pair)
    mc          = pair.get("marketCap", 0)
    liq         = (pair.get("liquidity") or {}).get("usd", 0)
    vol         = (pair.get("volume") or {}).get("h24", 0)
    chg         = pair.get("priceChange", {})
    txns_5m     = pair.get("txns", {}).get("m5", {})
    txns_1h     = pair.get("txns", {}).get("h1", {})
    created_ms  = pair.get("pairCreatedAt", 0)
    created_str = datetime.fromtimestamp(created_ms/1000).strftime("%Y-%m-%d %H:%M:%S") if created_ms else "?"
    created_ago = time_ago(created_ms) if created_ms else "unknown"
    rise_pct    = ((cur_price - atl_price) / atl_price * 100) if atl_price else 0

    return (
        "🔄 <b>Recovery Alert — " + symbol + " up " + "{:.1f}".format(rise_pct) + "% from ATL!</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "<b>" + symbol + "</b>  |  " + name + "\n"
        "Created: <b>" + created_str + "</b>  (" + created_ago + ")\n\n"
        "ATL Price:   <code>$" + "{:.8f}".format(atl_price) + "</code>\n"
        "Now Price:   <code>$" + "{:.8f}".format(cur_price) + "</code>\n"
        "Recovery:    <b>+" + "{:.1f}".format(rise_pct) + "%</b>\n\n"
        "Market Cap:  <code>" + fmt_usd(mc) + "</code>\n"
        "Liquidity:   <code>" + fmt_usd(liq) + "</code>\n"
        "Vol 24h:     <code>" + fmt_usd(vol) + "</code>\n\n"
        "Txns 5m: " + str(txns_5m.get("buys",0)) + "B / " + str(txns_5m.get("sells",0)) + "S\n"
        "Txns 1h: " + str(txns_1h.get("buys",0)) + "B / " + str(txns_1h.get("sells",0)) + "S\n\n"
        "Change: 5m " + fmt_pct(chg.get("m5")) + "  |  1h " + fmt_pct(chg.get("h1")) + "\n"
        "        6h " + fmt_pct(chg.get("h6")) + "  |  24h " + fmt_pct(chg.get("h24")) + "\n\n"
        "Contract:\n<code>" + addr + "</code>\n\n"
        "<a href='https://dexscreener.com/solana/" + pair_addr + "'>DexScreener</a>  |  "
        "<a href='https://pump.fun/coin/" + addr + "'>Pump.fun</a>"
    )


# ─── WEBSOCKET ────────────────────────────────────────────────────────────────

def fetch_token_meta(mint, retries=6, delay=5.0):
    for attempt in range(retries):
        try:
            r = requests.get("https://api.dexscreener.com/latest/dex/tokens/" + mint,
                             headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
            if r.ok:
                pairs = r.json().get("pairs") or []
                for pair in sorted(pairs, key=lambda p: p.get("dexId","") != "pumpswap"):
                    base = pair.get("baseToken", {})
                    if base.get("symbol") and base.get("name"):
                        return {"symbol": base["symbol"], "name": base["name"],
                                "created_ms": pair.get("pairCreatedAt", 0)}
        except Exception:
            pass
        if attempt < retries - 1:
            print("  [Meta] not indexed yet, retrying in " + str(delay) + "s (" + str(attempt+1) + "/" + str(retries) + ")")
            time.sleep(delay)
    return {"symbol": "?", "name": "?", "created_ms": 0}


def on_ws_open(ws):
    print("  [WS] Connected to PumpPortal")
    ws.send(json.dumps({"method": "subscribeMigration"}))

def on_ws_message(ws, message):
    try:
        data = json.loads(message)
        mint = data.get("mint") or data.get("token") or data.get("address")
        if not mint: return
        with lock:
            if mint in seen_set: return
        meta = fetch_token_meta(mint)
        data["mint"]       = mint
        data["symbol"]     = meta["symbol"]
        data["name"]       = meta["name"]
        data["created_ms"] = meta["created_ms"]
        db_add_seen(mint, meta["created_ms"])
        with lock:
            seen_set.add(mint)
        print("  [GRADUATED] " + data["symbol"] + " | " + mint)
    except Exception as e:
        print("  [WS] error: " + str(e))

def on_ws_error(ws, error):   print("  [WS] Error: " + str(error))
def on_ws_close(ws, *args):
    print("  [WS] Closed. Reconnecting in 5s...")
    time.sleep(5); start_websocket()

def start_websocket():
    ws = websocket.WebSocketApp(PUMPPORTAL_WS,
        on_open=on_ws_open, on_message=on_ws_message,
        on_error=on_ws_error, on_close=on_ws_close)
    threading.Thread(target=ws.run_forever, daemon=True).start()


# ─── INITIAL SCAN ─────────────────────────────────────────────────────────────

def initial_scan(key, flt):
    state = filter_state[key]
    with lock:
        tokens = list(seen_set)
    if not tokens: return
    print("  [" + key.upper() + "] Initial scan of " + str(len(tokens)) + " tokens...")
    pairs = fetch_token_data(tokens)
    for pair in pairs:
        addr = (pair.get("baseToken") or {}).get("address")
        if addr and passes_filter(pair, flt):
            state["currently"].add(addr)
    db_save_filter_state(key, "currently", state["currently"])
    print("  [" + key.upper() + "] " + str(len(state["currently"])) + " currently passing, " + str(len(state["alerted"])) + " already alerted")


# ─── FILTER LOOP (Bot 1 & 2) ──────────────────────────────────────────────────

def filter_loop(key, api_url, flt, initial_delay=0):
    state = filter_state[key]
    if initial_delay:
        time.sleep(initial_delay)
    print("  [" + key.upper() + "] Filter loop started — " + flt["label"])

    while True:
        time.sleep(FILTER_POLL_SEC)
        try:
            with lock:
                tokens = list(seen_set)
            tokens = [t for t in tokens if t not in state["expired"]]
            pairs  = fetch_token_data(tokens) if tokens else []

            new_passing = set()
            for pair in pairs:
                addr  = (pair.get("baseToken") or {}).get("address", "")
                age_h = age_hours(pair)
                if age_h > flt["max_age_h"]:
                    state["expired"].add(addr); continue
                if passes_filter(pair, flt):
                    new_passing.add(addr)

            # Exclude already alerted tokens — one alert per token ever
            just_entered = new_passing - state["currently"] - state["alerted"]

            for addr in just_entered:
                pair = next((p for p in pairs
                             if (p.get("baseToken") or {}).get("address") == addr), None)
                if not pair: continue
                symbol = (pair.get("baseToken") or {}).get("symbol", "?")
                print("  [" + key.upper() + "] ALERT: " + symbol + " | " + addr)
                send_tg(api_url, build_filter_alert(pair, flt["label"]))

                # Permanently mark as alerted
                state["alerted"].add(addr)
                db_add_alerted(key, addr)

                # Bot 1 entries → start ATH tracking
                if key == "f1":
                    price = get_price(pair)
                    if price > 0:
                        with lock:
                            ath_tracking[addr] = {"ath": price, "alerted_dump": False}
                        db_upsert_ath(addr, price, False)

            state["currently"] = new_passing
            db_save_filter_state(key, "currently", state["currently"])
            db_save_filter_state(key, "expired",   state["expired"])

            # Cleanup all tables for tokens older than 24h + 2h buffer
            db_cleanup(26.0)
            with lock:
                seen_set.clear()
                seen_set.update(db_load_seen())
                # Sync ath/recovery in-memory to match DB after cleanup
                expired_addrs = set(ath_tracking.keys()) - seen_set
                for addr in expired_addrs:
                    ath_tracking.pop(addr, None)
                    recovery_tracking.pop(addr, None)

        except Exception as e:
            print("  [" + key.upper() + "] Error: " + str(e))


# ─── DUMP MONITOR LOOP (Bot 3) ────────────────────────────────────────────────

def dump_monitor_loop():
    print("  [DUMP] Dump monitor started (every 20s)")
    while True:
        time.sleep(20)  # faster than filter loop for tighter ATH/dump detection
        try:
            with lock:
                tracked = dict(ath_tracking)

            to_check = [addr for addr, data in tracked.items() if not data["alerted_dump"]]
            if not to_check:
                continue

            pairs = fetch_token_data(to_check)
            for pair in pairs:
                addr      = (pair.get("baseToken") or {}).get("address", "")
                if not addr or addr not in tracked: continue

                cur_price = get_price(pair)
                if cur_price <= 0: continue

                data = tracked[addr]

                # Update ATH if price is higher — do NOT continue, still check dump
                # (price could have been higher mid-cycle and now falling)
                if cur_price > data["ath"]:
                    with lock:
                        ath_tracking[addr]["ath"] = cur_price
                    db_upsert_ath(addr, cur_price, False)
                    tracked[addr]["ath"] = cur_price
                    continue  # price still rising, no dump yet

                # Check dump threshold against latest ATH
                drop_pct = (data["ath"] - cur_price) / data["ath"]
                if drop_pct >= DUMP_THRESHOLD:
                    symbol = (pair.get("baseToken") or {}).get("symbol", "?")
                    print("  [DUMP] ALERT: " + symbol + " dumped " + "{:.1f}".format(drop_pct*100) + "% | " + addr)
                    send_tg(TG_API_3, build_dump_alert(pair, data["ath"]))

                    with lock:
                        ath_tracking[addr]["alerted_dump"] = True
                        recovery_tracking[addr] = {"atl": cur_price, "alerted_recovery": False}
                    db_upsert_ath(addr, data["ath"], True)
                    db_upsert_recovery(addr, cur_price, False)

        except Exception as e:
            print("  [DUMP] Error: " + str(e))


# ─── RECOVERY MONITOR LOOP (Bot 4) ───────────────────────────────────────────

def recovery_monitor_loop():
    print("  [RECOVERY] Recovery monitor started (every 20s)")
    while True:
        time.sleep(20)  # faster for tighter ATL/recovery detection
        try:
            with lock:
                tracked = dict(recovery_tracking)

            to_check = [addr for addr, data in tracked.items() if not data["alerted_recovery"]]
            if not to_check:
                continue

            pairs = fetch_token_data(to_check)
            for pair in pairs:
                addr      = (pair.get("baseToken") or {}).get("address", "")
                if not addr or addr not in tracked: continue

                cur_price = get_price(pair)
                if cur_price <= 0: continue

                data = tracked[addr]

                # Update ATL if price goes lower — keep tracking the real bottom
                if cur_price < data["atl"]:
                    with lock:
                        recovery_tracking[addr]["atl"] = cur_price
                    db_upsert_recovery(addr, cur_price, False)
                    tracked[addr]["atl"] = cur_price
                    continue  # still falling, no recovery yet

                # Check recovery from lowest point
                rise_pct = (cur_price - data["atl"]) / data["atl"]
                if rise_pct >= RECOVERY_THRESHOLD:
                    symbol = (pair.get("baseToken") or {}).get("symbol", "?")
                    print("  [RECOVERY] ALERT: " + symbol + " recovered " + "{:.1f}".format(rise_pct*100) + "% | " + addr)
                    send_tg(TG_API_4, build_recovery_alert(pair, data["atl"]))

                    with lock:
                        recovery_tracking[addr]["alerted_recovery"] = True
                    db_upsert_recovery(addr, data["atl"], True)

        except Exception as e:
            print("  [RECOVERY] Error: " + str(e))


# ─── COMMANDS ─────────────────────────────────────────────────────────────────

def check_commands():
    bots = [
        ("f1", TG_API_1, F1),
        ("f2", TG_API_2, F2),
        ("f3", TG_API_3, None),
        ("f4", TG_API_4, None),
    ]
    for key, api_url, flt in bots:
        try:
            r = requests.get(api_url + "/getUpdates",
                             params={"offset": update_offsets.get(key, 0) + 1, "timeout": 0},
                             timeout=10)
            if not r.ok: continue
            for update in r.json().get("result", []):
                update_offsets[key] = update["update_id"]
                text = (update.get("message", {}).get("text") or "").strip().lower()
                if text.startswith("/status") and flt:
                    handle_status(api_url, flt)
                elif text.startswith("/tracking"):
                    handle_tracking(api_url)
        except Exception as e:
            print("  [CMD] error (" + key + "): " + str(e))


def handle_status(api_url, flt):
    with lock:
        tokens = list(seen_set)
    if not tokens:
        send_tg(api_url, "<b>No tokens tracked yet.</b>"); return
    send_tg(api_url, "Checking " + str(len(tokens)) + " tokens...")
    pairs = fetch_token_data(tokens)
    passing = [p for p in pairs if passes_filter(p, flt)]
    if not passing:
        send_tg(api_url, "<b>No tokens currently passing filter.</b>"); return
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = ["<b>Passing: " + str(len(passing)) + "</b>  |  <i>" + now + "</i>"]
    for p in sorted(passing, key=lambda x: x.get("marketCap", 0), reverse=True):
        b = p.get("baseToken", {})
        mc = p.get("marketCap", 0)
        vol = (p.get("volume") or {}).get("h24", 0)
        lines.append(
            "<a href='https://dexscreener.com/solana/" + p.get("pairAddress","") + "'>"
            "<b>" + esc(b.get("symbol","?")) + "</b></a>  "
            "MC " + fmt_usd(mc) + "  Vol " + fmt_usd(vol) + "  Age " + "{:.1f}".format(age_hours(p)) + "h"
        )
    chunk = ""
    for line in lines:
        if len(chunk) + len(line) + 1 > 3800:
            send_tg(api_url, chunk); chunk = line; time.sleep(0.3)
        else:
            chunk = chunk + "\n" + line if chunk else line
    if chunk: send_tg(api_url, chunk)


def handle_tracking(api_url):
    with lock:
        ath = dict(ath_tracking)
        rec = dict(recovery_tracking)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = ("<b>ATH Tracking: " + str(len(ath)) + " tokens</b>\n"
           "<b>Recovery Tracking: " + str(len(rec)) + " tokens</b>\n"
           "<i>" + now + "</i>")
    send_tg(api_url, msg)


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    global seen_set, filter_state, ath_tracking, recovery_tracking

    print("=" * 60)
    print("  Early PumpSwap Monitor — 4 Channel")
    print("=" * 60)

    for var, name in [
        (BOT_TOKEN_1,  "EARLY_BOT_TOKEN_1"),
        (BOT_TOKEN_2,  "EARLY_BOT_TOKEN_2"),
        (BOT_TOKEN_3,  "EARLY_BOT_TOKEN_3"),
        (BOT_TOKEN_4,  "EARLY_BOT_TOKEN_4"),
        (CHAT_ID,      "EARLY_CHAT_ID"),
        (DATABASE_URL, "EARLY_DATABASE_URL"),
    ]:
        if not var:
            print("  ERROR: " + name + " missing in .env"); return

    init_db()

    seen_set         = db_load_seen()
    ath_tracking     = db_load_ath_tracking()
    recovery_tracking = db_load_recovery_tracking()
    print("  Loaded " + str(len(seen_set)) + " seen tokens")
    print("  Loaded " + str(len(ath_tracking)) + " ATH tracked tokens")
    print("  Loaded " + str(len(recovery_tracking)) + " recovery tracked tokens")

    filter_state["f1"] = {
        "currently": db_load_filter_state("f1", "currently"),
        "expired":   db_load_filter_state("f1", "expired"),
        "alerted":   db_load_alerted("f1"),
    }
    filter_state["f2"] = {
        "currently": db_load_filter_state("f2", "currently"),
        "expired":   db_load_filter_state("f2", "expired"),
        "alerted":   db_load_alerted("f2"),
    }

    if not filter_state["f1"]["currently"] and not filter_state["f1"]["expired"]:
        initial_scan("f1", F1)
    if not filter_state["f2"]["currently"] and not filter_state["f2"]["expired"]:
        initial_scan("f2", F2)

    print("\n  Connecting to PumpPortal WebSocket...")
    start_websocket()

    # F1 starts immediately, F2 starts after half interval (staggered)
    threading.Thread(target=filter_loop, args=("f1", TG_API_1, F1), kwargs={"initial_delay": 0}, daemon=True).start()
    threading.Thread(target=filter_loop, args=("f2", TG_API_2, F2), kwargs={"initial_delay": FILTER_POLL_SEC / 2}, daemon=True).start()
    threading.Thread(target=dump_monitor_loop,     daemon=True).start()
    threading.Thread(target=recovery_monitor_loop, daemon=True).start()

    def flt_msg(flt):
        return ("Started: <b>" + flt["label"] + "</b>\n"
                "MC >= " + fmt_usd(flt["min_mcap"]) + " | "
                "Age <= " + str(int(flt["max_age_h"])) + "h | "
                "Vol >= " + fmt_usd(flt["min_vol"]) + " | Has profile\n"
                "/status — see passing tokens")

    send_tg(TG_API_1, flt_msg(F1))
    send_tg(TG_API_2, flt_msg(F2))
    send_tg(TG_API_3, "Started: <b>Dump Alert</b>\nAlerts when Bot 1 token dumps " + "{:.0f}".format(DUMP_THRESHOLD*100) + "% from ATH\n/tracking — see tracked count")
    send_tg(TG_API_4, "Started: <b>Recovery Alert</b>\nAlerts when dumped token recovers " + "{:.0f}".format(RECOVERY_THRESHOLD*100) + "% from ATL\n/tracking — see tracked count")

    print("\n  Running. Press Ctrl+C to stop.\n")
    while True:
        try:
            check_commands()
            time.sleep(3)
        except KeyboardInterrupt:
            print("\n  Stopped.")
            break

if __name__ == "__main__":
    main()