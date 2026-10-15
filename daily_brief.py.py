"""
daily_brief.py - Money Copilot, Stage 1: the daily brief (no AI, no API key, no cost).

Reads the logs your scanners already write and turns them into one short, plain note:
  - Is the scanner healthy?  (running, blind rate, missed snapshots)
  - How far along is the v1.4 test?  (outcomes, what the gate has done so far)
  - How are the paper trades doing?
  - Polymarket lab progress (if you use it)
  - Risk flags, and ONE sensible next step

Rules baked in (they are code, not promises):
  * Never recommends buying anything. It reports, flags risk and says "not enough data" when true.
  * Every claim comes from a number it counted in your CSV files. Nothing is invented.
  * Read-only: it never touches an account, a key, or the internet.

Run:
    python daily_brief.py              writes brief_latest.md and briefs/brief_YYYY-MM-DD.md, prints it
    python daily_brief.py --selftest   offline tests

Keep this file in the SAME folder as concept_scorer_v1_4.py.
"""
import os
import sys
from datetime import datetime, timedelta, timezone

try:
    import concept_scorer_v1_4 as cs
except ImportError:
    cs = None

PM_MARKETS = "pm_markets.csv"
PM_FORECASTS = "pm_forecasts.csv"
OUTCOME_TARGET = 100


def hours_ago(iso, now):
    t = cs.parse_dt(iso) if hasattr(cs, "parse_dt") else None
    if t is None:
        try:
            t = datetime.fromisoformat(iso)
        except Exception:
            return None
    if t.tzinfo is None:
        t = t.replace(tzinfo=timezone.utc)
    return (now - t).total_seconds() / 3600


def pct(n, d):
    return 100 * n / d if d else 0.0


def build_brief(now=None, base_dir="."):
    now = now or datetime.now(timezone.utc)
    cwd = os.getcwd()
    os.chdir(base_dir)
    try:
        return _build(now)
    finally:
        os.chdir(cwd)


def _build(now):
    cs.STATE["demo"] = False
    shadow = cs.load_csv(cs.SHADOW_FILE)
    paper = cs.load_csv(cs.PAPER_FILE)
    pm_m, pm_f = cs.load_csv(PM_MARKETS), cs.load_csv(PM_FORECASTS)
    flags, lines = [], []

    def L(s=""):
        lines.append(s)

    L(f"# Money Copilot brief - {now.strftime('%a %d %b %Y, %H:%M')} UTC")
    L()

    if not shadow:
        L("**No scanner data yet.** shadow_log.csv is empty or missing.")
        L()
        L("Next step: run `python concept_scorer_v1_4.py` once (or trigger the GitHub workflow), then rerun this brief.")
        L()
        L("_Research and paper trading only. Not financial advice._")
        return "\n".join(lines)

    # ---------- scanner health ----------
    n = len(shadow)
    firsts = [hours_ago(r["first_seen"], now) for r in shadow if r.get("first_seen")]
    lasts = [hours_ago(r["last_seen"], now) for r in shadow if r.get("last_seen")]
    firsts = [h for h in firsts if h is not None]
    lasts = [h for h in lasts if h is not None]
    since_last = min(lasts) if lasts else None
    new24 = sum(1 for h in firsts if h <= 24)
    span_days = (max(firsts) - min(firsts)) / 24 if firsts else 0
    blind_all = sum(1 for r in shadow if "liquidity unknown" in (r.get("categories") or ""))
    recent = [r for r in shadow if (hours_ago(r.get("first_seen", ""), now) or 999) <= 24]
    blind_recent = sum(1 for r in recent if "liquidity unknown" in (r.get("categories") or ""))
    old = [r for r in shadow if (hours_ago(r.get("first_seen", ""), now) or 0) > 36]
    na_old = sum(1 for r in old if r.get("p24h") == "NA")

    if since_last is None:
        flags.append(("watch", "Can't tell when the scanner last ran (no last-seen times)."))
    elif since_last >= 12:
        flags.append(("fail", f"Scanner has been quiet for {since_last:.0f}h. Check the GitHub Actions tab."))
    elif since_last >= 3:
        flags.append(("watch", f"Scanner last ran {since_last:.1f}h ago (hourly is normal)."))
    if len(recent) >= 10:
        b = pct(blind_recent, len(recent))
        if b >= 35:
            flags.append(("fail", f"{b:.0f}% of today's coins had unreadable liquidity. A data feed is probably failing."))
        elif b >= 15:
            flags.append(("watch", f"{b:.0f}% of today's coins had unreadable liquidity (target: under 15%)."))
    if len(old) >= 10:
        na = pct(na_old, len(old))
        if na >= 40:
            flags.append(("fail", f"{na:.0f}% of 24h price snapshots were missed. Rechecks are running too rarely."))
        elif na >= 20:
            flags.append(("watch", f"{na:.0f}% of 24h price snapshots were missed. Run rechecks more often."))

    L("## Scanner")
    L(f"- Coins logged: **{n}** ({new24} new in the last 24h), across {span_days:.1f} days")
    L(f"- Last scan: {'unknown' if since_last is None else f'{since_last:.1f}h ago'}")
    if recent:
        L(f"- Liquidity unreadable today: {blind_recent} of {len(recent)} ({pct(blind_recent, len(recent)):.0f}%)")
    L(f"- Liquidity unreadable overall: {blind_all} of {n} ({pct(blind_all, n):.0f}%)")
    latest = max(shadow, key=lambda r: r.get("first_seen") or "")
    w = latest.get("weather") or "UNKNOWN"
    L(f"- Market weather at the latest scan: **{w.replace('_', ' ')}**")
    if w in ("RED", "DEEP_RED"):
        flags.append(("watch", f"Market weather is {w.replace('_', ' ').lower()}: rules are stricter, expect fewer survivors."))
    survivors_today = [r.get("symbol") for r in recent if r.get("verdict") == "WATCH"]
    if survivors_today:
        L(f"- Passed every check in the last 24h (paper only, NOT safe): {', '.join(s for s in survivors_today if s)}")
    else:
        L("- Nothing passed every check in the last 24h. That is a normal, good outcome.")
    L()

    # ---------- test progress ----------
    a = cs.analyse_shadow(shadow)
    outcomes = sum(1 for r in shadow if cs.snap_value(r.get("p24h")) is not None)
    L("## v1.4 test progress")
    L(f"- Coins with a 24h outcome: **{outcomes} of {OUTCOME_TARGET}**")
    if outcomes < OUTCOME_TARGET:
        L("- Status: **too early to judge.** Keep the rules frozen and let the data arrive.")
    surv = a["classes"].get("survivors (WATCH/BENCH)")
    blocked = a["classes"].get("blocked by the gate")
    if a["base"] and outcomes >= 20:
        if blocked:
            L(f"- Blocked by the gate: {blocked['rugged']:.0f}% lost 70%+, {blocked['doubled']:.0f}% doubled (n={blocked['n']})")
        if surv:
            L(f"- Survivors: {surv['rugged']:.0f}% lost 70%+, {surv['doubled']:.0f}% doubled (n={surv['n']})")
        if outcomes < OUTCOME_TARGET:
            L("- These are hints, not results, at this sample size.")
    L()

    # ---------- paper trades ----------
    L("## Paper trades")
    if not paper:
        L("- None logged yet.")
    else:
        nets = [cs.to_float(r.get("net_change_pct")) for r in paper]
        nets = [x for x in nets if x is not None]
        openn = sum(1 for r in paper if (r.get("status") or "open") == "open")
        L(f"- Logged: **{len(paper)}** ({openn} still open), with a result so far: {len(nets)}")
        if nets:
            med = cs.median(nets)
            L(f"- Median result after costs: {med:+.1f}%   losers: {pct(sum(1 for x in nets if x < 0), len(nets)):.0f}%")
            worst = min(paper, key=lambda r: cs.to_float(r.get("net_change_pct")) if cs.to_float(r.get("net_change_pct")) is not None else 0)
            wv = cs.to_float(worst.get("net_change_pct"))
            if wv is not None and wv <= -50:
                L(f"- Worst so far: {worst.get('symbol')} at {wv:+.0f}%")
            if len(nets) >= 10 and med < -20:
                flags.append(("watch", f"Paper trades have a median result of {med:+.0f}% after costs."))
    L()

    # ---------- polymarket ----------
    if pm_m or pm_f:
        res_m = sum(1 for r in pm_m if r.get("status") == "resolved")
        res_f = 0
        ids = {r["id"] for r in pm_m if r.get("status") == "resolved"}
        res_f = sum(1 for r in pm_f if r.get("id") in ids)
        L("## Polymarket lab")
        L(f"- Markets logged: {len(pm_m)} ({res_m} resolved)   your forecasts: {len(pm_f)} ({res_f} resolved)")
        if res_f < 30:
            L(f"- Not enough resolved forecasts to say anything yet ({res_f} of 30).")
        else:
            L("- Enough resolved forecasts for a first look: run `python polymarket_lab_v0_1.py --report`.")
        L()

    # ---------- flags + next step ----------
    L("## Risk flags")
    if not flags:
        L("- None. Everything this brief can check looks normal.")
    for sev, msg in sorted(flags, key=lambda f: 0 if f[0] == "fail" else 1):
        L(f"- {'FAIL' if sev == 'fail' else 'WATCH'}: {msg}")
    L()
    L("## One next step")
    if any(s == "fail" for s, _ in flags):
        L("Fix the failing check above before trusting any result.")
    elif outcomes < OUTCOME_TARGET:
        L(f"Do nothing to the rules. About {OUTCOME_TARGET - outcomes} more 24h outcomes are needed before judging the gate.")
    else:
        L("Run `python concept_scorer_v1_4.py --report` and review the rules one at a time. Log any change and restart the count.")
    L()
    L("_Research and paper trading only. Not financial advice. This brief reports counts from your own logs; "
      "it never recommends a trade._")
    return "\n".join(lines)


def write_brief(text, now=None, base_dir="."):
    now = now or datetime.now(timezone.utc)
    os.makedirs(os.path.join(base_dir, "briefs"), exist_ok=True)
    with open(os.path.join(base_dir, "brief_latest.md"), "w") as f:
        f.write(text + "\n")
    with open(os.path.join(base_dir, "briefs", f"brief_{now.strftime('%Y-%m-%d')}.md"), "w") as f:
        f.write(text + "\n")


# ======================= self-test =======================
def selftest():
    import tempfile
    ok = True

    def check(name, cond):
        nonlocal ok
        print(("PASS  " if cond else "FAIL  ") + name)
        ok = ok and bool(cond)

    now = datetime.now(timezone.utc)

    def ts(h):
        return (now - timedelta(hours=h)).isoformat(timespec="minutes")

    def row(i, hrs, cat="", verdict="SKIP", p24="", weather="NEUTRAL", sym=None, seen_h=0.5):
        return {"mint": f"M{i}", "symbol": sym or f"S{i}", "first_seen": ts(hrs), "last_seen": ts(seen_h),
                "weather": weather, "verdict": verdict, "categories": cat, "entry_price": "1", "est_cost_pct": "2",
                "p1h": "", "p6h": "", "p24h": p24, "narrative": 1, "meme": 1, "community": 1, "timing": 1, "value": 1,
                "total": 50}

    with tempfile.TemporaryDirectory() as d:
        # 1. empty
        t = build_brief(now, d)
        check("empty folder: friendly message, no crash", "No scanner data yet" in t)

        # 2. healthy-ish data
        rows = [row(i, 2 + i * 0.1, "thin liquidity" if i % 2 else "") for i in range(40)]
        rows[0]["verdict"] = "WATCH"
        rows[0]["symbol"] = "OWLX"
        cs.save_csv(os.path.join(d, cs.SHADOW_FILE), cs.SHADOW_COLS, rows)
        t = build_brief(now, d)
        check("healthy: no FAIL lines", "FAIL:" not in t)
        check("healthy: says too early to judge", "too early to judge" in t)
        check("healthy: survivor listed with NOT safe warning", "OWLX" in t and "NOT safe" in t)
        check("healthy: shows progress count", "0 of 100" in t)

        # 3. blind feed
        rows = [row(i, 3, "liquidity unknown") for i in range(30)]
        cs.save_csv(os.path.join(d, cs.SHADOW_FILE), cs.SHADOW_COLS, rows)
        t = build_brief(now, d)
        check("blind feed: FAIL flag raised", "FAIL:" in t and "unreadable liquidity" in t)
        check("blind feed: next step says fix failing check", "Fix the failing check" in t)

        # 4. stale scanner
        rows = [row(i, 30, seen_h=20) for i in range(15)]
        cs.save_csv(os.path.join(d, cs.SHADOW_FILE), cs.SHADOW_COLS, rows)
        t = build_brief(now, d)
        check("stale scanner: flagged", "quiet for" in t)

        # 5. missed snapshots
        rows = [row(i, 80, p24="NA") for i in range(15)] + [row(99, 1)]
        cs.save_csv(os.path.join(d, cs.SHADOW_FILE), cs.SHADOW_COLS, rows)
        t = build_brief(now, d)
        check("missed 24h snapshots: flagged", "snapshots were missed" in t)

        # 6. outcomes + paper + polymarket sections
        rows = []
        for i in range(60):
            rows.append(row(i, 50 + i * 0.1, "thin liquidity" if i % 2 else "", "SKIP" if i % 2 else "WATCH",
                            p24="DEAD" if i % 2 else "1.5"))
        cs.save_csv(os.path.join(d, cs.SHADOW_FILE), cs.SHADOW_COLS, rows)
        paper = [{"logged_at": ts(40), "symbol": f"P{i}", "address": f"A{i}", "net_change_pct": "-60" if i < 8 else "10",
                  "status": "closed-ish"} for i in range(12)]
        cs.save_csv(os.path.join(d, cs.PAPER_FILE), cs.PAPER_COLS, paper)
        cs.save_csv(os.path.join(d, PM_MARKETS), ["id", "status"], [{"id": "1", "status": "resolved"}, {"id": "2", "status": "open"}])
        cs.save_csv(os.path.join(d, PM_FORECASTS), ["id"], [{"id": "1"}])
        t = build_brief(now, d)
        check("outcomes counted", "60 of 100" in t)
        check("gate stats shown", "Blocked by the gate" in t and "Survivors" in t)
        check("paper trades summarised", "Logged: **12**" in t and "Worst so far" in t)
        check("paper median flag", "median result of" in t)
        check("polymarket section present", "Polymarket lab" in t and "Not enough resolved forecasts" in t)

        # 7. guardrails + determinism + files
        low = t.lower()
        check("never recommends buying", all(p not in low for p in ("you should buy", "buy now", "recommend buying", "go long")))
        check("disclaimer present", "never recommends a trade" in t)
        check("deterministic", build_brief(now, d) == t)
        write_brief(t, now, d)
        check("files written", os.path.exists(os.path.join(d, "brief_latest.md"))
              and os.path.exists(os.path.join(d, "briefs", f"brief_{now.strftime('%Y-%m-%d')}.md")))
    print("\nALL PASS" if ok else "\nSOME TESTS FAILED")
    return ok


def main():
    if cs is None:
        print("Put daily_brief.py in the same folder as concept_scorer_v1_4.py.")
        sys.exit(1)
    if "--selftest" in sys.argv:
        sys.exit(0 if selftest() else 1)
    text = build_brief()
    write_brief(text)
    print(text)


if __name__ == "__main__":
    main()
