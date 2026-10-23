# CLAUDE.md — jamulus-sampler

Persistent Python daemon on `137.184.43.255` replacing `servers.php`. Probes Jamulus servers in the background; HTTP handler returns instantly from in-memory state.

## Service

```
systemctl status jamulus-sampler.service   # check health
systemctl restart jamulus-sampler.service  # restart
journalctl -u jamulus-sampler.service -n 100  # recent logs
```

Runs on port **5001**. Nothing listens on 80/443 here (apache2 and php-fpm are installed but inactive); `servers.php` is no longer a rollback path on this host.

## Endpoints

| Endpoint | Description |
|---|---|
| `GET /servers?central=<host>:<port>` | Server list for one directory (also `?directory=`). Same JSON as servers.php + `first_seen`/`last_absent` per client. |
| `GET /servers/all` | Merged list from all 7 directories; each server tagged with `central` field. |
| `GET /stats` | Probe health: `probes.per_second`, `directories[]` (with `sweep_count`, `last_sweep_ago_s`), `unreachable_servers[]` (with `probe_attempts`, `probe_successes`), `activity` tier histogram, `active_pool`. |
| `POST /ping-density` | jamfan26 pushes `{"active_pool":N,"ts":unix}` — count of unique ALLOWED client IPs seen in the last 180s. Tightens the global idle-probe cap (`_global_idle_cap_locked`). |
| `GET /debug/stuck` | Servers currently marked `_probe_inflight` — use when throughput drops or the watchdog logs `ALL n/n stuck`. |

## Key Constants (`sampler.py`)

```python
# Probe intervals are TIERED, not flat. Old PROBE_ACTIVE/STABLE/IDLE/PROBE_DORMANT
# constants were removed — do not reintroduce them.
_TIER_RATES = {              # (probe_active, probe_stable, probe_idle) seconds
    "quiet":  (4.0, 10.0, 20.0),
    "normal": (4.0,  8.0, 15.0),
    "busy":   (4.0,  6.0, 10.0),
}
DIRECTORY_SWEEP   =  90.0   # s — re-query directory (NAT TTL ≥120s measured)
CLIENT_PORT_RANGE =  15     # 15 pool ports 22135–22149; ONE probe each
SWEEP_PORT        =  22134  # separate socket, all directory sweeps share it
GLOBAL_NORMAL_POOL = 8      # unique browser IPs (last 180s) → normal global idle tier
GLOBAL_BUSY_POOL   = 15     # unique browser IPs (last 180s) → busy global idle tier
```

**How the tier is chosen (two independent signals):**
- **Per-server** (`_server_tier`): rank the current clock hour against the server's own `hourly_events[24]` bins. `current ≤ p33` → quiet, `≥ p67` → busy, else normal. Defaults to normal until the server has logged ≥1 event. This is what makes probing follow each server's *own* daily rhythm.
- **Global idle cap** (`_global_idle_cap_locked`): `_active_pool` (browser IPs, pushed via `POST /ping-density`) caps the idle interval for servers that aren't historically quiet. Historically-quiet servers (idle ≥ 20s) are exempt.

## CRITICAL: GUID Lookup Tables

GUIDs = `MD5(name + country_string + instrument_string)`. The `COUNTRIES` and `INSTRUMENTS` dicts in `sampler.py` were extracted **verbatim** from `/var/www/html/servers.php` on this server. Do not replace them with any other source.

**`/tmp/probe1014.py`'s tables are wrong** — they diverge at instrument index 21 (PHP: `Cello`, probe1014.py: `Viola`) and at country index 4 (PHP: `American Samoa`, probe1014.py omits it). Using those tables produces GUIDs that don't match any historical data. If you need to verify, diff against the PHP arrays at `/var/www/html/servers.php` lines ~184–561.

**Verified 2026-07-08:** `COUNTRIES` (262 entries, 0–261) and `INSTRUMENTS` (50 entries, 0–49) match `servers.php` **byte-for-byte at every index (0 divergences)**. **Rule — verify by element, not by length:** a mid-table insertion/shift keeps the count identical while corrupting every GUID from that index on (that is exactly how probe1014.py fails). After any table edit on either side, diff each `idx → value`, not just `len()`.

## Architecture

- Min-heap scheduler (`_heap`, protected by `_heap_lock`) drives all probes via a `ThreadPoolExecutor` (`max_workers=CLIENT_PORT_RANGE`).
- Each server gets its own `SERVER_STATE` entry. Probe interval is set by `_do_srv_task` based on client activity.
- Directory sweeps (`_do_dir_task`) run every `DIRECTORY_SWEEP` seconds per directory, repopulating `SERVER_STATE` with newly registered servers.
- `probe_server()` always returns a dict (never `None` in normal operation). Success = `result.get('ping', -1) >= 0`.
- Port pool (`_port_pool`, semaphore-guarded): 15 UDP ports for concurrent probes. `_ports_in_use` and `_ports_peak` track utilization; `/stats` exposes headroom.

## Per-client timestamps

Two fields per client in the JSON response:
- `first_seen` — Unix float: when the sampler **first observed** this client on this server.
- `last_absent` — Unix float: when the sampler **last probed and did not see** this client (lower bound on join time).

Together they define a bounded join window `(last_absent, first_seen]` used by Layer 2 (Plan B) to narrow ping candidate lookup.

## Validation (run from jamfan26 / 134.199.209.51)

```bash
# Quick sanity check
curl -s 'http://137.184.43.255:5001/servers?central=anygenre1.jamulus.io:22124' | python3 -m json.tool | head -40
curl -s 'http://137.184.43.255:5001/stats' | python3 -m json.tool

# Long-running comparison vs servers.php
python3 /root/compare-samplers.py --interval 5
```

## Deployment context

- **M3 live (2026-06-10)**: active Layer 1 data source for `gather-server-data.py` on jamfan26. `BASE_URL` already points to `http://137.184.43.255:5001/servers?central=`.
- Rollback if needed: change `BASE_URL` back to `http://137.184.43.255/servers.php?central=`, then `systemctl restart call-servers-php.service` on jamfan26. **⚠️ 2026-07-08: this is NOT instant — nginx + php-fpm on 137.184.43.255 are currently stopped, so `servers.php` returns nothing until you `systemctl start nginx php*-fpm` here first.**
- GitHub: `https://github.com/mcfnord/jamulus-sampler`
- **⚠️ jamfan26 IP is recorded inconsistently (tucked away for next time):** this file cites `134.199.209.51` (Validation section) *and* `24.199.107.192` (here). Both are live SSH hosts. 137.184.43.255 cannot SSH to either (trust is one-way: jamfan26 → here). Reconcile the canonical IP later; do not guess.

## TODO / open decisions (inline, per project convention)

- **Push or hold?** Local tree is 4 commits ahead of `origin/master` (D2/D3 ping-density). Repo is public — review before `git push`.
- **Port pool saturates 15/15 in steady state** (not just cold start; observed 2026-07-08). **Widening it is code-only** — measured 2026-08-16, this host has *no* firewall rule for UDP 22134–22149 (`ufw status` allows only lo, 22/tcp, 80/tcp from two IPs, 5001/tcp; `iptables -S | grep -c '221[0-9][0-9]'` = 0). The pool rides `ufw-before-input -m conntrack --ctstate RELATED,ESTABLISHED` because the sampler always initiates, so ports above :22149 behave identically. *(This bullet previously claimed ufw had to be opened first; that was wrong and was quoted downstream as fact.)* Note the pool also sets `max_workers` for the shared `ThreadPoolExecutor`, so widening it is what buys headroom for concurrent directory sweeps — see `FLEET-SAMPLING-GAP.md` on central command for why sweeps currently run at 220 s against a 90 s `DIRECTORY_SWEEP`. Only pursue for join-window width if jamfan26 evidence shows they are too wide (see `JAMFAN26-AGENT-BRIEF.md`).
- **POSTPONED tiny change toward IP↔GUID:** emit per-client `probe_interval` so Layer 2's documented `first_seen − probe_interval` fallback is computable. Gate: only if the correlator often sees clients lacking `last_absent`. Ask the jamfan26 agent to measure that first.
