"""
calm_dashboard.py - runs the v1.4 scan and writes a calm, auto-refreshing dashboard.html

Keep this file in the SAME folder as concept_scorer_v1_4.py.
    python calm_dashboard.py --once --recheck   also re-prices paper trades + shadow log first

    python calm_dashboard.py --once            one scan, writes dashboard.html, then stops
    python calm_dashboard.py --watch 30        scan every 30 minutes, quiet unless something changes
    python calm_dashboard.py --demo --once     fake data, no internet needed
    python calm_dashboard.py --demo --once --weather=RED

The page refreshes itself every 15 minutes when opened in a browser.
Research and paper-trading tool only. Not financial advice.
"""
import csv
import html
import os
import sys
import time
from datetime import datetime, timezone

import concept_scorer_v1_4 as cs

OUT = "dashboard.html"


def run_scan(demo=False, demo_regime="NEUTRAL"):
    data = cs.run_scan(demo, demo_regime)      # one shared engine: same logic as the command line
    if data is None:
        raise SystemExit("No pairs found this time. Try again in a few minutes.")
    return data


def paper_rows(demo=False):
    rows = cs.load_csv(cs.fp(cs.PAPER_FILE))[-15:]
    if not rows:
        return []
    prices = {}
    if rows and not demo:
        try:
            prices = cs.fetch_prices([r["address"] for r in rows])
        except Exception:
            pass
    out = []
    for r in reversed(rows):
        entry = cs.to_float(r.get("entry_price_usd"))
        cur = cs.exit_price(prices[r["address"]]) if r["address"] in prices else None
        chg = 100 * (cur - entry) / entry if entry and cur is not None else None
        out.append((r["symbol"], r["logged_at"][:16].replace("T", " "), chg))
    return out


CSS = """
:root{--bg:#f4f6f8;--card:#fff;--text:#1d2630;--mute:#66727f;--line:#e2e7ec;--acc:#8aa4c8}
@media(prefers-color-scheme:dark){:root{--bg:#0f1720;--card:#17212b;--text:#e6edf3;--mute:#93a1ae;--line:#25323f}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);
font:16px/1.5 -apple-system,system-ui,Segoe UI,Roboto,sans-serif;padding:16px;max-width:1280px;margin:auto}
h1{font-size:20px;margin:4px 0 2px}.mute{color:var(--mute);font-size:13px}
.card{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:14px;margin:12px 0}
.weather{border-left:6px solid var(--acc)}.big{display:flex;gap:10px}
.big div{flex:1;background:var(--card);border:1px solid var(--line);border-radius:14px;padding:12px;text-align:center}
.big b{display:block;font-size:30px}.sym{font-weight:700}.pill{font-size:12px;padding:2px 8px;border-radius:99px;
background:var(--line)}.table-wrap{overflow-x:auto}table{width:100%;border-collapse:collapse}
th,td{padding:8px;border-top:1px solid var(--line);font-size:13px;text-align:left;vertical-align:top}
th{position:sticky;top:0;background:var(--card);color:var(--mute);white-space:nowrap}
.coin-table{min-width:1180px}.coin-name{min-width:150px}.address{font-family:ui-monospace,monospace;overflow-wrap:anywhere;max-width:210px}
.details-cell{white-space:normal;min-width:240px;max-width:360px}.up{color:#3fa66b}.down{color:#d06a62}
details summary{cursor:pointer;color:var(--mute)}
.calm{text-align:center;font-style:italic;padding:14px;color:var(--mute)}
"""
ACCENT = {"GREEN": "#5fbf8f", "NEUTRAL": "#8aa4c8", "RED": "#e0a15a", "DEEP_RED": "#d9776f", "UNKNOWN": "#9aa0a6"}


def esc(x):
    return html.escape(str(x))


def write_html(data, demo=False):
    w, results = data["weather"], data["results"]
    regime = w["regime"]
    avoided = [r for r in results if r["verdict"] in ("SKIP", "REJECT")]
    watch = [r for r in results if r["verdict"] in ("WATCH", "WATCH_LOGGED", "BENCH")]
    moves = ", ".join(f"{k} {v:+.1f}%" for k, v in w["changes"].items() if v is not None) or "no data"

    def value(x, prefix="", suffix="", precision=2):
        number = cs.to_float(x)
        return "n/a" if number is None else f"{prefix}{number:,.{precision}f}{suffix}"

    def change(x):
        number = cs.to_float(x)
        if number is None:
            return "n/a", ""
        return f"{number:+.1f}%", "up" if number >= 0 else "down"

    parts = [f"<!doctype html><html><head><meta charset='utf-8'>"
             f"<meta name='viewport' content='width=device-width,initial-scale=1'>"
             f"<meta http-equiv='refresh' content='900'><title>Calm dashboard</title><style>{CSS}</style></head><body>",
             "<h1>Timmy's Calm Dashboard</h1>",
             f"<div class='mute'>Updated {data['updated'].strftime('%d %b %Y, %H:%M UTC')}"
             f" &middot; refreshes itself every 15 min{' &middot; DEMO DATA' if demo else ''}</div>",
             f"<div class='card weather' style='border-left-color:{ACCENT[regime]}'>"
             f"<b>Market weather: {esc(regime.replace('_', ' '))}</b><br>{esc(cs.WEATHER_TEXT[regime])}"
             f"<div class='mute'>{esc(moves)}</div>"
             f"<div class='mute'>Today's rules: liquidity floor ${data['floor']:,.0f}, score bar "
             f"{data['rules']['min_score']}, max {data['rules']['max_watch']} paper trade(s).</div></div>",
             f"<div class='big'><div><b>{len(avoided)}</b>traps you didn't walk into</div>"
             f"<div><b>{data['watch']}</b>worth a calm look</div></div>"]

    if regime == "DEEP_RED":
        parts.append("<div class='card'>Stand-down day. Nothing here needs chasing. Look, learn, log.</div>")
    elif not watch:
        parts.append("<div class='card'>Nothing earned a place today. That's a good result.</div>")

    parts.append(f"<div class='card'><b>Coin scan results ({len(results)})</b>"
                 "<div class='mute'>One row per coin, including skipped coins. Full token addresses identify the exact mint.</div>"
                 "<div class='table-wrap'><table class='coin-table'><thead><tr>"
                 "<th>Name / symbol</th><th>Token address</th><th>Price</th><th>Liquidity</th>"
                 "<th>Market cap</th><th>24h volume</th><th>Age</th><th>1h change</th><th>24h change</th>"
                 "<th>Buys / sells (1h)</th><th>Score</th><th>Verdict</th><th>Scan details</th>"
                 "</tr></thead><tbody>")
    for r in results:
        p = r["p"]
        base = p.get("baseToken") or {}
        name = base.get("name") or base.get("symbol") or "Unknown"
        symbol = base.get("symbol") or "n/a"
        age = cs.age_hours(p)
        age_text = "n/a" if age is None else (f"{age:.1f}h" if age < 48 else f"{age / 24:.1f}d")
        price = cs.to_float(p.get("priceUsd"))
        price_text = "n/a" if price is None else f"${price:.8g}"
        liquidity = cs.liquidity_of(p)
        liquidity_text = value(liquidity, "$", precision=0)
        liq_source = p.get("_liq_src") or "source unknown"
        mcap_text = value(p.get("marketCap") or p.get("fdv"), "$", precision=0)
        vol_text = value((p.get("volume") or {}).get("h24"), "$", precision=0)
        price_change = p.get("priceChange") or {}
        h1_text, h1_class = change(price_change.get("h1"))
        h24_text, h24_class = change(price_change.get("h24"))
        txns = (p.get("txns") or {}).get("h1") or {}
        buys, sells = txns.get("buys"), txns.get("sells")
        details = r.get("rec") or "; ".join(r.get("fails") or r.get("warns") or []) or "No additional details"
        parts.append(
            f"<tr><td class='coin-name'><b>{esc(name)}</b><br><span class='mute'>{esc(symbol)}</span></td>"
            f"<td class='address'>{esc(r['mint'])}</td><td>{esc(price_text)}</td>"
            f"<td>{esc(liquidity_text)}<br><span class='mute'>{esc(liq_source)}</span></td>"
            f"<td>{esc(mcap_text)}</td><td>{esc(vol_text)}</td><td>{esc(age_text)}</td>"
            f"<td class='{h1_class}'>{esc(h1_text)}</td><td class='{h24_class}'>{esc(h24_text)}</td>"
            f"<td>{esc(buys if buys is not None else 'n/a')} / {esc(sells if sells is not None else 'n/a')}</td>"
            f"<td>{esc(r['total'])}</td><td><span class='pill'>{esc(r['verdict'])}</span></td>"
            f"<td class='details-cell'>{esc(details)}</td></tr>")
    parts.append("</tbody></table></div></div>")

    ls = data["liq_stats"]
    parts.append(f"<div class='card'><b>Visibility</b><div class='mute'>Liquidity unknown in the main feed: "
                 f"{ls['unknown_before']} of {ls['total']}. Still unknown after fallbacks: {ls['unknown_after']}."
                 f"</div></div>")
    try:
        a = cs.analyse_shadow(cs.load_csv(cs.fp(cs.SHADOW_FILE)))
        if a["base"] and a["n_outcomes"] >= 20:
            blk = a["classes"].get("blocked by the gate")
            sur = a["classes"].get("survivors (WATCH/BENCH)")
            lines = [f"{a['n_outcomes']} coins have outcomes so far"
                     f"{' (small sample, a hint only)' if a['n_outcomes'] < 100 else ''}."]
            if blk:
                lines.append(f"Blocked by the gate: {blk['rugged']:.0f}% lost 70%+, {blk['doubled']:.0f}% doubled.")
            if sur:
                lines.append(f"Survivors: {sur['rugged']:.0f}% lost 70%+, {sur['doubled']:.0f}% doubled.")
            parts.append("<div class='card'><b>Does the gate work?</b>"
                         + "".join(f"<div class='mute'>{esc(x)}</div>" for x in lines) + "</div>")
    except Exception:
        pass

    rows = paper_rows(demo)
    if rows:
        parts.append("<div class='card'><b>Paper trades (latest first)</b><table>")
        for sym, when, chg in rows:
            cls = "" if chg is None else ("up" if chg >= 0 else "down")
            txt = "n/a" if chg is None else f"{chg:+.1f}%"
            parts.append(f"<tr><td class='sym'>{esc(sym)}</td><td class='mute'>{esc(when)}</td>"
                         f"<td class='{cls}' style='text-align:right'>{txt}</td></tr>")
        parts.append("</table><div class='mute'>Unrealised and paper only. Most of these will fade.</div></div>")

    line = cs.CALM_LINES[datetime.now().toordinal() % len(cs.CALM_LINES)]
    parts.append(f"<div class='calm'>{esc(line)}</div>"
                 "<div class='mute'>WATCH is not SAFE. Paper trade only. Not financial advice.</div></body></html>")
    with open(OUT, "w", encoding="utf-8") as f:
        f.write("".join(parts))


def once(demo, regime):
    if "--recheck" in sys.argv and not demo:
        cs.recheck()
    data = run_scan(demo, regime)
    write_html(data, demo)
    return data


def main():
    demo = "--demo" in sys.argv
    regime = "NEUTRAL"
    for a in sys.argv:
        if a.startswith("--weather="):
            regime = a.split("=", 1)[1].upper()
    if "--watch" in sys.argv:
        minutes = int(sys.argv[sys.argv.index("--watch") + 1])
        last = set()
        while True:
            data = once(demo, regime)
            now = {r["p"]["baseToken"]["symbol"] for r in data["results"] if r["verdict"] == "WATCH"}
            stamp = datetime.now().strftime("%H:%M")
            if now - last:
                print(f"{stamp}  weather {data['weather']['regime']}  new calm look: {', '.join(sorted(now - last))}")
            else:
                print(f"{stamp}  weather {data['weather']['regime']}  all quiet. Nothing to do.")
            last = now
            time.sleep(minutes * 60)
    else:
        data = once(demo, regime)
        print(f"Wrote {OUT}: weather {data['weather']['regime']}, {data['watch']} worth a calm look.")


if __name__ == "__main__":
    main()
