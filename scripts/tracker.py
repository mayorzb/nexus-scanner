"""
Nexus Outcome Tracker v0.1  (The Four - 14-day experiment)

Every run (hourly via GitHub Actions):
  1. DISCOVER  new Solana tokens from DexScreener (latest profiles + boosts)
  2. SCORE     each new token once, at first sighting (transparent v0 rules)
  3. LOG       it to data/tracker/log.csv with the entry price
  4. FOLLOW UP prices at +1h, +24h, +7d for every logged token
  5. REPORT    writes data/tracker/REPORT.md (score tiers vs real outcomes)

Paper trading only. No wallet, no keys, no money. Standard library only.
"""

import csv
import json
import os
import statistics
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "tracker")
LOG_PATH = os.path.join(DATA_DIR, "log.csv")
REPORT_PATH = os.path.join(DATA_DIR, "REPORT.md")

DS = "https://api.dexscreener.com"
RUGCHECK = "https://api.rugcheck.xyz/v1/tokens/{}/report/summary"
CHAIN = "solana"
MAX_NEW_PER_RUN = 40          # cap new tokens per hour (keeps API use gentle)
CHECKPOINTS = [("p_1h", 1), ("p_24h", 24), ("p_7d", 168)]  # column, hours
GONE = "GONE"                 # token vanished -> counted as -100% (conservative)

FIELDS = [
    "first_seen_utc", "token", "symbol", "pair_address", "pair_url", "dex", "source",
    "age_h", "liq_usd", "vol24_usd", "buys_h1", "sells_h1", "mcap_usd",
    "rug_dangers", "rug_status", "score", "tier",
    "p_entry", "p_1h", "p_24h", "p_7d",
]


# ---------------------------------------------------------------- helpers
def now_utc():
    return datetime.now(timezone.utc)


def get_json(url, retries=3):
    req = urllib.request.Request(url, headers={"User-Agent": "nexus-tracker/0.1"})
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 429:                      # rate limited -> back off
                time.sleep(5 * (attempt + 1))
                continue
            print(f"  HTTP {e.code} for {url}")
            return None
        except Exception as e:                     # network hiccup -> retry
            print(f"  error {type(e).__name__} for {url}: {e}")
            time.sleep(2)
    return None


def num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def load_log():
    if not os.path.exists(LOG_PATH):
        return []
    with open(LOG_PATH, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def save_log(rows):
    os.makedirs(DATA_DIR, exist_ok=True)
    tmp = LOG_PATH + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    os.replace(tmp, LOG_PATH)


# ---------------------------------------------------------------- 1. discover
def discover():
    found = {}
    for path, source in [("/token-profiles/latest/v1", "profile"),
                         ("/token-boosts/latest/v1", "boost")]:
        data = get_json(DS + path) or []
        for item in data if isinstance(data, list) else []:
            if item.get("chainId") == CHAIN and item.get("tokenAddress"):
                found.setdefault(item["tokenAddress"], source)
    return found


def pair_rank(p):
    """Main pair = where the real trading is: 24h volume first, then liquidity."""
    vol = num((p.get("volume") or {}).get("h24")) or 0
    liq = num((p.get("liquidity") or {}).get("usd")) or 0
    return (vol, liq)


def fetch_pairs(addresses, pin=None):
    """Return {token_address: pair} via the batch endpoint (30 per call).
    pin = {token: pair_address} -> follow the SAME pair we entered on."""
    pin = pin or {}
    best = {}
    for i in range(0, len(addresses), 30):
        chunk = addresses[i:i + 30]
        data = get_json(f"{DS}/tokens/v1/{CHAIN}/{','.join(chunk)}") or []
        for p in data if isinstance(data, list) else []:
            addr = (p.get("baseToken") or {}).get("address")
            if addr not in chunk:
                continue
            want = pin.get(addr)
            if want:
                if p.get("pairAddress") == want:
                    best[addr] = p
                continue
            if addr not in best or pair_rank(p) > pair_rank(best[addr]):
                best[addr] = p
        time.sleep(0.3)
    return best


def rug_summary(token):
    data = get_json(RUGCHECK.format(token), retries=2)
    if not isinstance(data, dict):
        return None, "unavailable"
    risks = data.get("risks") or []
    dangers = sum(1 for r in risks if str(r.get("level", "")).lower() == "danger")
    return dangers, "ok"


# ---------------------------------------------------------------- 2. score
def score_token(age_h, liq, vol24, buys, sells, dangers):
    """v0 rules = a HYPOTHESIS. The 14-day log tests whether it predicts anything."""
    s = 0
    if liq is not None:
        s += 25 if liq >= 100_000 else 20 if liq >= 20_000 else 10 if liq >= 5_000 else 0
        if vol24 is not None and liq > 0:
            ratio = vol24 / liq
            s += 15 if 0.5 <= ratio <= 10 else 5 if ratio > 10 else 0
    if buys is not None and sells is not None:
        bs = buys / max(sells, 1)
        s += 15 if 1.0 <= bs <= 3.0 else 5 if bs > 3.0 else 0
    if age_h is not None:
        s += 15 if 1 <= age_h <= 72 else 10 if age_h > 72 else 5
    if dangers is not None:
        s += 30 if dangers == 0 else 10 if dangers == 1 else 0
    return s


def tier_of(score, liq):
    if liq is None:
        return "unknown-liq"
    return "high" if score >= 70 else "mid" if score >= 40 else "low"


# ---------------------------------------------------------------- 3+4. log & follow up
def log_new(rows, found, t):
    seen = {r["token"] for r in rows}
    new = [a for a in found if a not in seen][:MAX_NEW_PER_RUN]
    if not new:
        return 0
    pairs = fetch_pairs(new)
    added = 0
    for addr in new:
        p = pairs.get(addr)
        if not p or num(p.get("priceUsd")) is None:
            continue                                # no tradable pair yet
        created = num(p.get("pairCreatedAt"))
        age_h = round((t.timestamp() * 1000 - created) / 3_600_000, 2) if created else None
        liq = num((p.get("liquidity") or {}).get("usd"))
        vol24 = num((p.get("volume") or {}).get("h24"))
        h1 = (p.get("txns") or {}).get("h1") or {}
        buys, sells = num(h1.get("buys")), num(h1.get("sells"))
        dangers, rstatus = rug_summary(addr)
        time.sleep(0.5)
        sc = score_token(age_h, liq, vol24, buys, sells, dangers)
        rows.append({
            "first_seen_utc": t.isoformat(timespec="seconds"),
            "token": addr,
            "symbol": (p.get("baseToken") or {}).get("symbol", ""),
            "pair_address": p.get("pairAddress", ""),
            "pair_url": p.get("url", ""),
            "dex": p.get("dexId", ""),
            "source": found[addr],
            "age_h": age_h if age_h is not None else "",
            "liq_usd": liq if liq is not None else "",
            "vol24_usd": vol24 if vol24 is not None else "",
            "buys_h1": buys if buys is not None else "",
            "sells_h1": sells if sells is not None else "",
            "mcap_usd": num(p.get("marketCap")) or "",
            "rug_dangers": dangers if dangers is not None else "",
            "rug_status": rstatus,
            "score": sc,
            "tier": tier_of(sc, liq),
            "p_entry": p.get("priceUsd"),
            "p_1h": "", "p_24h": "", "p_7d": "",
        })
        added += 1
    return added


def follow_up(rows, t):
    due = {}  # token -> [columns due]
    for r in rows:
        age = (t - datetime.fromisoformat(r["first_seen_utc"])).total_seconds() / 3600
        for col, hrs in CHECKPOINTS:
            if not r.get(col) and age >= hrs:
                due.setdefault(r["token"], []).append(col)
    if not due:
        return 0
    pin = {r["token"]: r.get("pair_address") for r in rows if r["token"] in due}
    pairs = fetch_pairs(list(due), pin)
    filled = 0
    for r in rows:
        for col in due.get(r["token"], []):
            p = pairs.get(r["token"])
            price = num(p.get("priceUsd")) if p else None
            r[col] = price if price is not None else GONE
            filled += 1
    return filled


# ---------------------------------------------------------------- 5. report
def ret(row, col):
    entry = num(row.get("p_entry"))
    v = row.get(col)
    if not v or not entry:
        return None
    if v == GONE:
        return -100.0
    return (float(v) / entry - 1) * 100


def summarise(rows, col):
    rs = [x for x in (ret(r, col) for r in rows) if x is not None]
    if not rs:
        return "â", "â", "â", 0
    med = statistics.median(rs)
    win = sum(1 for x in rs if x >= 100) / len(rs) * 100      # hit 2x
    dead = sum(1 for x in rs if x <= -90) / len(rs) * 100     # -90% or gone
    return f"{med:+.1f}%", f"{win:.0f}%", f"{dead:.0f}%", len(rs)


def write_report(rows, t):
    tiers = ["high", "mid", "low", "unknown-liq", "ALL"]
    lines = [
        "# Nexus Outcome Tracker â Report",
        f"Updated: {t.strftime('%Y-%m-%d %H:%M')} UTC Â· Tokens logged: **{len(rows)}**",
        "",
        "Paper trading only. GONE tokens count as -100%. "
        "2x = return â¥ +100%. Dead = return â¤ -90%.",
        "",
    ]
    for col, label in [("p_1h", "+1 hour"), ("p_24h", "+24 hours"), ("p_7d", "+7 days")]:
        lines += [f"## Outcome at {label}", "",
                  "| Tier | n | Median return | Hit 2x | Dead |",
                  "|---|---|---|---|---|"]
        for tier in tiers:
            subset = rows if tier == "ALL" else [r for r in rows if r["tier"] == tier]
            med, win, dead, n = summarise(subset, col)
            lines.append(f"| {tier} | {n} | {med} | {win} | {dead} |")
        lines.append("")

    hi = [x for x in (ret(r, "p_24h") for r in rows if r["tier"] == "high") if x is not None]
    lo = [x for x in (ret(r, "p_24h") for r in rows if r["tier"] in ("mid", "low")) if x is not None]
    lines += ["## Day-14 verdict (auto)", ""]
    if len(hi) < 50 or len(lo) < 50:
        lines.append(f"**Not enough data yet** â need 50+ HIGH and 50+ REST at +24h "
                     f"(high: {len(hi)}, mid+low: {len(lo)}).")
    else:
        mh, ml = statistics.median(hi), statistics.median(lo)
        dead_hi = sum(1 for x in hi if x <= -90) / len(hi)
        dead_lo = sum(1 for x in lo if x <= -90) / len(lo)
        edge = (mh - ml) >= 10 and dead_hi < dead_lo
        lines.append(f"High median {mh:+.1f}% vs mid+low {ml:+.1f}% Â· "
                     f"dead rate {dead_hi:.0%} vs {dead_lo:.0%}")
        lines.append("")
        lines.append("**â Scorer shows an edge â consider tiny capped live test.**" if edge
                     else "**â No edge â do not trade on this scorer. Revise or shut down.**")
    lines += ["", "_A positive median in ALL tiers can just mean the market went up. "
              "The test is HIGH beating MID+LOW._"]
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


# ---------------------------------------------------------------- main
def main():
    t = now_utc()
    rows = load_log()
    print(f"[{t.isoformat(timespec='seconds')}] loaded {len(rows)} rows")
    found = discover()
    print(f"  discovered {len(found)} Solana tokens")
    added = log_new(rows, found, t)
    filled = follow_up(rows, t)
    save_log(rows)
    write_report(rows, t)
    print(f"  added {added} new Â· filled {filled} follow-ups Â· total {len(rows)}")
    if not found and not rows:
        print("  WARNING: nothing discovered â check API access in the Actions log.")
        sys.exit(1)


if __name__ == "__main__":
    main()
