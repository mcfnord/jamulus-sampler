# CLAUDE.md — jamulus-sampler

Guidance for an AI agent (or a new maintainer) working in this repository. The README is the
user-facing description; this file holds the invariants that are easy to break and the way the
code is meant to be changed.

## What it is

One Python 3 file, standard library only. A background scheduler probes every server on the
configured Jamulus directories over UDP and keeps the result in memory; a threaded HTTP server
answers from that memory. The HTTP surface is a drop-in for softins' `servers.php`
(same JSON, same request modes, same error bodies) plus `first_seen` / `last_absent` per client.

## Invariants — check these before and after any change

1. **servers.php parity.** `tools/parity.py` must exit 0 against `explorer.jamulus.io` (0 structural
   FAIL; coverage lines are WARN). Its `--control` arm compares Explorer's two backends with each
   other and must also exit 0 — any check the control arm fails is a wrong check, not a wrong sampler.
   Key order, value types, `numip` as `"4:<hex>"`, the no-reply shape (`ping: -1` and NO
   `nclients`/`clients`), the three error bodies, and `?server=` dropping a server that never
   answered are all servers.php behaviour read off its source; change them only with servers.php.
2. **A configured directory always answers a JSON list** — possibly `[]`, never an error object.
   Downstream consumers iterate the result without checking its type.
3. **The GUID lookup tables are protocol.** `COUNTRIES` and `INSTRUMENTS` match servers.php at every
   index. Anything that derives an identity from name + country + instrument depends on them; after
   any edit compare entry by entry, not by length — one inserted row shifts everything after it.
4. **Nothing in the UDP path blocks on the HTTP path and nothing in the HTTP path waits for the
   probe pool.** The 15 pool ports run saturated on a busy host; `?server=` and `?query=` use their
   own four on-demand ports (`ONDEMAND_PORTS`) and a request-driven directory sweep is rate-limited
   (`ONDEMAND_COOLDOWN`) and serialised (`_adopt_lock`) so a client cannot starve the scheduler or
   turn the daemon into a UDP scanner. **A directory is its resolved address, not its spelling:**
   `canonical_directory()` maps any name at a configured directory's ip:port (`.app` for `.io`) onto
   that directory's state, and inside the cooldown the held rows are answered, never `[]` for a
   directory that answered. 2026-10-24: with all `ADOPT_MAX` slots taken by duplicates, the `.app`
   names answered `[]` on every repeat inside a minute (softins' report).
5. **The hole-punch is answered.** A directory introduction arrives as `CLM_EMPTY_MESSAGE` (1009)
   from the server's real port; the sweep answers it with a ping. Without that, no server behind NAT
   is ever reached and `port2` can never be observed. The firewall must allow inbound UDP to the
   whole range (sweep port, pool, on-demand ports) from anywhere — see the README.
6. **Timestamps are stamped at send time.** A ping packet built once and reused makes the RTT read
   as time since the sweep began. `ping_packet()` exists for this reason; the payload-less requests
   (`REQ_*_PKT`) are constants because they carry no stamp.
7. **One template, one folder, one owner.** `no_reply()` is the shape of a server that has answered
   nothing — every probe result, state entry and ad-hoc row starts from it, so a new reply field is
   added in one place. `apply_reply_info()` folds a version/features/welcome reply for the sweep and
   the pool probe alike. `_merge_probe_fields()` is the only place reply-driven fields enter a
   server's state (os, version, features, welcome, port2, the features-miss counter). An empty
   welcome is an answer; `port2` is cleared when the server answers from its listed port.

## Deployment knobs (environment)

`SAMPLER_HTTP_PORT` (5001), `SAMPLER_UDP_BASE` (22134: the sweep socket; pool and on-demand ports
follow), `SAMPLER_ADMIN_IPS` (comma list allowed to read `/stats`, `/debug/stuck` and POST
`/ping-density`; `127.0.0.1` always). Two instances on one host need different bases. `SAMPLER_VERBOSE=1` turns on the per-server
`[sweep]` lines and one line per HTTP request; by default only failed requests (>= 400) are logged, plus one
`[http] <hour>Z requests N: 2xx=… 4xx=…` count line per UTC hour.

## How to work on it

- Run a second instance beside production for any change: another HTTP port and UDP base, then
  `tools/parity.py --sampler http://127.0.0.1:<port>`; warm-up is two sweeps of every directory
  (~3 minutes).
- Read the production vantage, not just a developer box: NAT servers, the pool at 15/15 and the
  hole-punch only show up on the host that the directories know.
- Log lines are counts and addresses, never player names. Do not add a per-packet or per-server log
  line outside `VERBOSE`: the sweep answers tens of thousands of hellos an hour, and on 2026-10-24 the
  per-server sweep lines plus one line per routine HTTP request were ~95% of a journal that then held
  less than a day.
- State is in memory only; a restart forgets every server, so every client gets a fresh
  `first_seen` at restart time.

## Open design note

Directories a request brings in ("adopted") are a special case grafted onto the scheduler
(`adopted` flag, `ADOPT_MAX`, a drop check in `_do_dir_task`). A uniform model — every directory
carries `last_request_time`, configured ones are pinned, and `_do_dir_task` is the only creator of
scheduler tokens — would remove the flag and the special cases. Worth doing before adding a third
kind of directory.
