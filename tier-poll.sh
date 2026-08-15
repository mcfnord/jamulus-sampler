#!/bin/bash
# D4c tier trace: poll the sampler /stats and append one row. Cron: every 60s.
#
# Purpose: capture active_pool / active_tier / thresholds over time so D4c
# engagement during peak hours is readable. The sampler computes the idle-cap
# tier on-demand and never logs it, so a single /stats snapshot can't show
# whether the tier transitioned during a spike. Observer-side only — this does
# NOT touch the sampler (no restart, does not disturb today_peak/yesterday_peak).
#
# Reversible: remove the crontab line (`crontab -e`), then delete this script
# and tier-trace.csv.
#
# Install-location independent: OUT defaults to tier-trace.csv beside this script,
# so a checkout anywhere works without editing. Both settings can be overridden
# from the environment or the crontab line, e.g.
#   * * * * * OUT=/var/log/tier-trace.csv /path/to/tier-poll.sh 2>>/path/to/tier-poll.err
OUT="${OUT:-$(cd "$(dirname "$0")" && pwd)/tier-trace.csv}"
STATS_URL="${STATS_URL:-http://127.0.0.1:5001/stats}"
if [ ! -f "$OUT" ]; then
  echo "epoch,iso_utc,active_pool,active_tier,pool_normal_thresh,pool_busy_thresh,active_pool_age_s" > "$OUT"
fi
curl -s --max-time 10 "$STATS_URL" | python3 -c '
import sys, json, time, datetime
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(0)          # curl failed or bad JSON -> write no row (no garbage)
s = d.get("servers", {})
iso = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
row = [int(time.time()), iso,
       s.get("active_pool"), s.get("active_tier"),
       s.get("pool_normal_thresh"), s.get("pool_busy_thresh"),
       s.get("active_pool_age_s")]
print(",".join(str(x) for x in row))
' >> "$OUT"
