# jamulus-sampler

A long-running daemon that keeps a live picture of every server on the seven public
[Jamulus](https://jamulus.io) directories (and any other directory a request names), who is
connected to each one and when each person first appeared, and serves it over HTTP as JSON in
the shape of softins' `servers.php`.

It replaces a `servers.php`-style page that queried the directory and every server on each
web request. The sampler does the UDP work in the background, so an HTTP request is answered
from memory without waiting on the network.

## What it does

- **Sweeps each directory** every 90 s (`DIRECTORY_SWEEP`). It asks for the server list
  (`CLM_REQ_SERVER_LIST`); the directory then tells every listed server to say hello to the
  sampler (`CLM_EMPTY_MESSAGE`), and the sampler answers each hello with a ping. That exchange is
  the NAT hole-punch: it is the only way a server behind NAT becomes reachable, and the port the
  hello came from is `servers.php`'s `port2`.
- **Probes each listed server** for ping time, version/OS, features, welcome message and its
  connected-client list (`CLM_PING_MS_WITHNUMCLIENTS`, then `CLM_REQ_VERSION_AND_OS`,
  `CLM_REQ_SERVER_FEATURES`, `CLM_REQ_WELCOME_MESSAGE` and, when occupied,
  `CLM_REQ_CONN_CLIENTS_LIST` — the same burst `servers.php` sends).
- **Adapts the probe rate.** A server whose client list just changed is probed again in 4 s.
  After that the interval stretches by 1 s per unchanged probe, up to a cap: 6–10 s for a
  server with clients, 10–20 s for an empty one. The cap depends on two signals:
  - *Per server:* the current hour is ranked against that server's own 24-hour history of
    join/leave events. Its quiet hours are probed less often and its busy hours more often.
  - *Global (optional):* a caller can push an activity count to `POST /ping-density`
    (see below). When it is high compared with yesterday's peaks, idle servers are probed
    more often.
- **Records arrival times.** Every client carries `first_seen` (when the sampler first saw
  them) and `last_absent` (the last probe that did *not* see them). A client therefore
  joined somewhere in `(last_absent, first_seen]`.
- **Forgets servers that leave.** A server that **no** directory has listed for 30 minutes
  (`REAP_AFTER`) is dropped. The rule is based on listing, not on whether the server
  answers. Some servers can only be reached through the directory's hole-punch and fail
  every direct probe, yet they stay listed. Reaping waits until every directory has swept
  since the server was last listed, so a directory outage stops reaping instead of
  triggering it.

## HTTP API (port 5001)

The sampler is a drop-in for softins' [`servers.php`](https://github.com/softins/jamulus-php): point a
`servers.php` consumer at `/servers.php` here and it gets the same JSON — same keys, key order, value
types and error bodies — with two additions per client, `first_seen` and `last_absent`. `tools/parity.py`
is the acceptance test (it compares every directory against `explorer.jamulus.io` and passes with 0
structural differences; its `--control` arm compares Explorer's two backends with each other to show
what timing noise alone produces).

| Request | Returns |
|---|---|
| `GET /servers.php?directory=<host>:<port>` (alias `/servers`, `?central=`) | One directory's servers in `servers.php`'s shape, plus `first_seen` / `last_absent` (Unix floats) per client. A bare host means port 22124. A directory not in `DIRECTORIES` is swept on demand and kept in the loop while requests keep coming (dropped after 30 min without one; at most `ADOPT_MAX` such directories). |
| `GET /servers.php?server=<host>:<port>[&name=…]` | One server as `servers.php` returns it: a one-element list, or `[]` if it never answered. |
| `GET /servers.php?query=<host>:<port>` | `servers.php`'s welcome-message query: a short audio connection that yields `client_id`, `rawaudio` and `welcome`. |
| `&pretty` | Pretty-printed JSON, like `servers.php`. |
| no parameter / malformed / unresolvable | `servers.php`'s bodies, HTTP 200: `{"error":"No directory or server specified"}`, `{"error":"Invalid directory or server specified"}`, `{"error":"Directory or server not found"}`. |
| `GET /servers/all` | Every directory merged; each server is tagged with its `central`. |
| `GET /stats` *(admin)* | Health: uptime, port-pool use, probe rate and queue depth, server counts (including `reaped_total`), per-directory sweep counts and age, unreachable servers, and the activity tiers. |
| `GET /debug/stuck` *(admin)* | Servers with a probe queued or in flight. |
| `POST /ping-density` *(admin)* | Body `{"active_pool": N, "ts": <unix>}`. Sets the global activity level used to tighten idle probing. |

*(admin)* routes answer only to `127.0.0.1` and the addresses in `SAMPLER_ADMIN_IPS`; everyone else
gets 403. Everything else is public and read-only, served from memory by a threaded server.

Environment knobs: `SAMPLER_HTTP_PORT` (5001), `SAMPLER_UDP_BASE` (22134: the sweep socket, with the
15 probe ports and 4 on-demand ports above it), `SAMPLER_ADMIN_IPS` (comma-separated; with none
set, the admin routes answer only to localhost). `SAMPLER_VERBOSE=1` logs every HTTP request and every
per-server sweep step (off by default: only failed requests and an hourly request count).

Example:

```sh
curl -s 'http://127.0.0.1:5001/servers.php?directory=anygenre1.jamulus.io:22124' | python3 -m json.tool | head
curl -s 'http://127.0.0.1:5001/servers.php?server=rock.jamulus.io:22424&name=Rock'
curl -s http://127.0.0.1:5001/stats | python3 -m json.tool      # from an admin address
```

The public routes are read-only and answer from memory, so the HTTP port can face the internet.
The server binds `0.0.0.0`; if you would rather not publish it, restrict the port with a firewall.

## Running it

It needs only the Python 3 standard library (it runs on 3.10).

```sh
python3 sampler.py
```

On startup it schedules a sweep of each directory in `DIRECTORIES` and logs to stderr:
`[dir]` for sweeps (including directories adopted on request), `[sweep]` for the exchanges
within one (a hello that revealed a `port2`, client-list requests), `[reap]` for retired
servers, `[watchdog]` when more than half of all servers are waiting for a probe, and `[http]`
for a request that raised. Log lines carry counts and addresses, never player names.

It sends UDP from a fixed block of local ports (`SAMPLER_UDP_BASE`, default 22134, and the 19
ports above it):

| Port(s) | Use |
|---|---|
| 22134 (`SWEEP_PORT`) | Directory sweeps. This port must stay fixed: the directory gives it to every server as the address to contact. |
| 22135–22149 (`CLIENT_PORT_START`, `CLIENT_PORT_RANGE`) | One port per scheduled probe in flight, so at most 15 probes run at once. |
| 22150–22153 (`ONDEMAND_PORTS`) | `?server=` and `?query=` requests, so they never queue behind the probe pool. |

A request for a directory that is not configured is swept on demand, like `servers.php` does on
every request, at most once a minute per directory and never two at once; inside that minute the
last sweep's rows are answered. A name that resolves to a configured directory's address
(`jazz.jamulus.app:22324` is `jazz.jamulus.io:22324`; upstream names the directories `.app`) is
served from that directory's state, not swept.

**The firewall must allow inbound UDP to the whole block (22134–22153 by default) from any
address and any port.** When the sampler asks a directory for its list, the directory tells every
registered server to say hello to the sampler (`CLM_EMPTY_MESSAGE`) — from the server's *real*
port, which is how a server behind NAT becomes reachable at all and how `port2` is learned. Those
hellos are not replies to anything the sampler sent, so a stateful rule alone drops them.
Measured 2026-10-23 on a host with only `RELATED,ESTABLISHED`: 1,014 dropped hellos in 24 hours and
every NAT-only server permanently "unreachable"; with the rule, 25,000 hellos answered in the first
hour and those servers reachable with real pings. (The same requirement is in jamulus-php's README.)

A systemd unit along these lines keeps it running:

```ini
[Unit]
Description=jamulus-sampler — persistent Jamulus server prober on :5001
After=network.target

[Service]
Type=simple
WorkingDirectory=/opt/jamulus-sampler
# Addresses allowed to read /stats and /debug/stuck and to POST /ping-density (localhost always is)
Environment=SAMPLER_ADMIN_IPS=203.0.113.10
ExecStart=/usr/bin/python3 /opt/jamulus-sampler/sampler.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

State lives only in memory. **A restart forgets every server, so every connected client gets
a fresh `first_seen` equal to the restart time.** If you consume arrival times, ignore the
first few minutes after a restart.

## Files

| File | Purpose |
|---|---|
| `sampler.py` | The daemon. |
| `tools/parity.py` | servers.php parity test against `explorer.jamulus.io`: `python3 tools/parity.py [--sampler http://127.0.0.1:5001] [--control]`. |
| `tier-poll.sh` | Optional observer: run from cron every minute, it appends one `/stats` row to `tier-trace.csv` so the activity tiers can be studied over time. It never touches the sampler. |

## Country and instrument tables

`COUNTRIES` and `INSTRUMENTS` in `sampler.py` turn the protocol's numeric IDs into names.
They match the tables in the original `servers.php` at every index. Anything that derives an
identity from a client's name, country and instrument depends on them, so after any edit,
compare the tables entry by entry, not just their lengths. One inserted row keeps the length
the same but shifts every entry after it.
