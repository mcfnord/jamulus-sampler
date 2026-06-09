# CLAUDE.md — jamulus-sampler

Persistent Python daemon on `137.184.43.255` replacing `servers.php`. Probes Jamulus servers in the background; HTTP handler returns instantly from in-memory state.

## Service

```
systemctl status jamulus-sampler.service   # check health
systemctl restart jamulus-sampler.service  # restart
journalctl -u jamulus-sampler.service -n 100  # recent logs
```

Runs on port **5001**. Do not change the port — `servers.php` owns port 80 and stays untouched until M3 cutover is validated.

## Endpoints

| Endpoint | Description |
|---|---|
| `GET /servers?central=<host>:<port>` | Server list for one directory (also `?directory=`). Same JSON as servers.php + `first_seen`/`last_absent` per client. |
| `GET /servers/all` | Merged list from all 7 directories; each server tagged with `central` field. |
| `GET /stats` | Probe health: `probes.per_second`, `directories[]` (with `sweep_count`, `last_sweep_ago_s`), `unreachable_servers[]` (with `probe_attempts`, `probe_successes`). |

## Key Constants (`sampler.py`)

```python
PROBE_ACTIVE    =   3.0   # s — after any client-set change
PROBE_STABLE    =  15.0   # s — clients present, stable
PROBE_IDLE      =  60.0   # s — zero clients
DIRECTORY_SWEEP =  90.0   # s — re-query central directory (NAT TTL ≥120s measured)
CLIENT_PORT_RANGE = 15    # UDP ports 22135–22149; one per in-flight probe
```

## CRITICAL: GUID Lookup Tables

GUIDs = `MD5(name + country_string + instrument_string)`. The `COUNTRIES` and `INSTRUMENTS` dicts in `sampler.py` were extracted **verbatim** from `/var/www/html/servers.php` on this server. Do not replace them with any other source.

**`/tmp/probe1014.py`'s tables are wrong** — they diverge at instrument index 21 (PHP: `Cello`, probe1014.py: `Viola`) and at country index 4 (PHP: `American Samoa`, probe1014.py omits it). Using those tables produces GUIDs that don't match any historical data. If you need to verify, diff against the PHP arrays at `/var/www/html/servers.php` lines ~184–561.

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

- Replaces `/var/www/html/servers.php` as the Layer 1 data source for `gather-server-data.py` on jamfan26.
- Cutover (M3): edit `ips-from-joins/gather-server-data.py` line 9 on jamfan26, change `BASE_URL` from `http://137.184.43.255/servers.php?central=` to `http://137.184.43.255:5001/servers?central=`, then `systemctl restart call-servers-php.service`.
- Rollback: revert that one line + restart. ~30 seconds, no data loss.
- GitHub: `https://github.com/mcfnord/jamulus-sampler`
