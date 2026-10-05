# jamulus-sampler

A long-running daemon that keeps a live picture of every server on the seven built-in
[Jamulus](https://jamulus.io) directories, who is connected to each one and when each person
first appeared, and serves it over HTTP as JSON.

It replaces a `servers.php`-style page that queried the directory and every server on each
web request. The sampler does the UDP work in the background, so an HTTP request is answered
from memory without waiting on the network.

## What it does

- **Sweeps each directory** every 90 s (`DIRECTORY_SWEEP`). It asks for the server list
  (`CLM_REQ_SERVER_LIST`) and handles the pings that servers send back through the
  directory's hole-punch.
- **Probes each listed server** for ping time, version/OS, and its connected-client list
  (`CLM_PING_MS_WITHNUMCLIENTS`, `CLM_REQ_VERSION_AND_OS`, `CLM_REQ_CONN_CLIENTS_LIST`).
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

| Request | Returns |
|---|---|
| `GET /servers?central=<host>:<port>` | One directory's servers, in the same JSON shape as `servers.php`, plus `first_seen` / `last_absent` per client. `?directory=` is accepted as an alias. A bare host means port 22124. |
| `GET /servers/all` | Every directory merged; each server is tagged with its `central`. |
| `GET /stats` | Health: uptime, port-pool use, probe rate and queue depth, server counts (including `reaped_total`), per-directory sweep counts and age, unreachable servers, and the activity tiers. |
| `GET /debug/stuck` | Servers with a probe queued or in flight. |
| `POST /ping-density` | Body `{"active_pool": N, "ts": <unix>}`. Sets the global activity level used to tighten idle probing. |

Example:

```sh
curl -s 'http://127.0.0.1:5001/servers?central=anygenre1.jamulus.io:22124' | python3 -m json.tool | head
curl -s http://127.0.0.1:5001/stats | python3 -m json.tool
```

**There is no authentication**, and `POST /ping-density` changes how hard the sampler probes.
The server binds `0.0.0.0:5001`, so restrict port 5001 to your own callers with a firewall.

## Running it

It needs only the Python 3 standard library (it runs on 3.10).

```sh
python3 sampler.py
```

On startup it schedules a sweep of each directory in `DIRECTORIES` and logs to stderr:
`[dir]` for sweeps, `[reap]` for retired servers, and `[watchdog]` when more than half of
all servers are waiting for a probe.

It sends UDP from fixed local ports:

| Port(s) | Use |
|---|---|
| 22134 (`SWEEP_PORT`) | Directory sweeps. This port must stay fixed: the directory gives it to every server as the address to ping back. |
| 22135–22149 (`CLIENT_PORT_START`, `CLIENT_PORT_RANGE`) | One port per probe in flight, so at most 15 probes run at once. |

Every exchange is started by the sampler, so a stateful firewall that allows replies
(`RELATED,ESTABLISHED`) is enough; no inbound UDP rule is needed.

A systemd unit along these lines keeps it running:

```ini
[Unit]
Description=jamulus-sampler — persistent Jamulus server prober on :5001
After=network.target

[Service]
Type=simple
WorkingDirectory=/opt/jamulus-sampler
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
| `tier-poll.sh` | Optional observer: run from cron every minute, it appends one `/stats` row to `tier-trace.csv` so the activity tiers can be studied over time. It never touches the sampler. |

## Country and instrument tables

`COUNTRIES` and `INSTRUMENTS` in `sampler.py` turn the protocol's numeric IDs into names.
They match the tables in the original `servers.php` at every index. Anything that derives an
identity from a client's name, country and instrument depends on them, so after any edit,
compare the tables entry by entry, not just their lengths. One inserted row keeps the length
the same but shifts every entry after it.
