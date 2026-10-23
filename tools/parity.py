#!/usr/bin/env python3
"""
parity.py — does the sampler answer like softins' servers.php?

Fetches the same directories from both, then checks the sampler's output against the
servers.php GRAMMAR (key sets, key order, value types, numip format, no-reply shape) and the
servers.php behaviour for the single-server, welcome-query, no-parameter, unknown-directory,
malformed-port and ?pretty requests. Set differences between the two server lists are WARN
only: directories change between two fetches.

  python3 parity.py                                  # sampler on 127.0.0.1:5001 vs explorer.jamulus.io
  python3 parity.py --sampler http://127.0.0.1:5002  # a side-by-side build
  python3 parity.py --control                        # Explorer lon1 vs lon2: what timing noise looks like

Exit 0 = every check PASS. Standard library only.
"""
import argparse
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

# The seven public directories. Kept as a copy of sampler.DIRECTORIES rather than imported:
# importing sampler.py binds its UDP sockets at module load, and this test must run anywhere.
DIRECTORIES = [
    'anygenre1.jamulus.io:22124', 'anygenre2.jamulus.io:22224', 'anygenre3.jamulus.io:22624',
    'rock.jamulus.io:22424', 'jazz.jamulus.io:22324', 'classical.jamulus.io:22524',
    'choral.jamulus.io:22724',
]
# A DNS alias of the first directory: resolves, is not configured, and so exercises the on-demand
# sweep-and-adopt path with a known answer (the same servers as anygenre1).
UNCONFIGURED_ALIAS = 'anygenre1.jamulus.app:22124'

# servers.php grammar, read off explorer.jamulus.io on 2026-10-23 (softins/jamulus-php b5462bf).
# The first 14 keys are set from the directory's list entry and the ping reply, in this order.
# nclients/clients/features/welcome/port2 are inserted as the matching UDP replies ARRIVE, so
# their relative order varies in servers.php itself (measured: lon1 vs lon2 disagree on 58 rows).
# index is appended last. The order check therefore covers the fixed prefix and index only.
SERVER_PREFIX = ['numip', 'port', 'countryid', 'maxclients', 'perm', 'country', 'name', 'ipaddrs',
                 'city', 'ip', 'ping', 'os', 'version', 'versionsort']
SERVER_MIDDLE = {'nclients', 'clients', 'features', 'welcome', 'port2'}
SERVER_ORDER = SERVER_PREFIX + sorted(SERVER_MIDDLE) + ['index']
SERVER_OPTIONAL = {'features', 'welcome', 'port2'}          # present only when the server sent them
CLIENT_ORDER = ['chanid', 'countryid', 'instrumentid', 'skillid', 'country', 'instrument', 'skill',
                'name', 'city']
CLIENT_EXTRA = ['first_seen', 'last_absent']                 # the sampler's additions, floats, at the end
SINGLE_ORDER = ['index', 'name', 'numip', 'ip', 'port', 'ping', 'os', 'version', 'versionsort', 'nclients']
QUERY_KEYS = {'index', 'name', 'numip', 'ip', 'port', 'ping', 'os', 'version', 'versionsort',
              'client_id', 'rawaudio', 'welcome'}
NUMIP_RE = re.compile(r'^[46]:[0-9a-f]+$')

FAILS, WARNS = [], []


def fail(msg):
    FAILS.append(msg); print('FAIL ' + msg)


def warn(msg):
    WARNS.append(msg); print('WARN ' + msg)


def ok(msg):
    print('ok   ' + msg)


def get(url, timeout=25):
    """Return (status, headers, body_bytes); status 0 on a dropped connection."""
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()
    except Exception as e:                                   # connection reset, timeout, ...
        return 0, {}, repr(e).encode()


def get_json(url, timeout=25):
    """get() plus a tolerant parse: (status, headers, body, parsed-or-None)."""
    st, h, body = get(url, timeout)
    try:
        return st, h, body, json.loads(body)
    except Exception:
        return st, h, body, None


def sampler_url(base, query):
    # Accepts either a servers.php-style base (ends in .php) or the sampler's /servers route.
    if base.endswith('.php') or base.endswith('/servers'):
        return f'{base}?{query}'
    return f'{base.rstrip("/")}/servers?{query}'


def type_map(rows):
    """key -> set of JSON type names seen, over a list of dicts."""
    t = {}
    for r in rows:
        for k, v in r.items():
            t.setdefault(k, set()).add(type(v).__name__)
    return t


def check_order(label, keys, order, allowed_extra=(), unordered=frozenset(), index_last=True):
    """Keys must appear in `order`, except members of `unordered`, whose relative position is free.
    In directory mode servers.php appends `index` last; in server=/query= mode it is set first."""
    base = [k for k in keys if k in order and k not in unordered]
    want = [k for k in order if k in keys and k not in unordered]
    if base != want:
        fail(f'{label}: key order {base} != servers.php {want}')
    stray = [k for k in keys if k not in order and k not in allowed_extra]
    if stray:
        fail(f'{label}: keys not in servers.php grammar: {stray}')
    if index_last and 'index' in keys and keys[-1] != 'index':
        fail(f'{label}: index is not the last key (servers.php appends it last)')


def check_directory(ctx, d, s_rows, e_rows):
    label = f'[{d}]'
    if not isinstance(s_rows, list):
        fail(f'{label} top level is {type(s_rows).__name__}, servers.php gives a list'); return
    ok(f'{label} list of {len(s_rows)} (explorer {len(e_rows)})')
    s_keys = {(r.get('ip'), r.get('port')) for r in s_rows}
    e_keys = {(r.get('ip'), r.get('port')) for r in e_rows}
    if s_keys != e_keys:
        warn(f'{label} server sets differ: only sampler {len(s_keys - e_keys)}, only explorer {len(e_keys - s_keys)}')
    if [r.get('index') for r in s_rows] != list(range(len(s_rows))):
        fail(f'{label} index != position')
    e_types, e_ctypes = ctx['e_types'], ctx['e_ctypes']      # built over ALL directories first
    bad_type, bad_numip, bad_noreply, bad_corder, bad_sorder = 0, 0, 0, 0, 0
    for r in s_rows:
        keys = list(r)
        before = len(FAILS)
        check_order(f'{label} {r.get("ip")}:{r.get("port")}', keys, SERVER_ORDER, unordered=SERVER_MIDDLE)
        bad_sorder += len(FAILS) - before
        for k, v in r.items():
            if k in e_types and type(v).__name__ not in e_types[k]:
                bad_type += 1
                if bad_type <= 3:
                    fail(f'{label} {k}={v!r} is {type(v).__name__}, servers.php emits {sorted(e_types[k])}')
        if not (isinstance(r.get('numip'), str) and NUMIP_RE.match(r['numip'])):
            bad_numip += 1
        # no-reply shape: ping -1 <=> nclients and clients absent
        noreply = r.get('ping', 0) < 0
        if noreply != ('nclients' not in r) or (noreply and 'clients' in r):
            bad_noreply += 1
        for c in r.get('clients', []):
            before = len(FAILS)
            check_order(f'{label} client {c.get("name")!r}', list(c), CLIENT_ORDER + CLIENT_EXTRA)
            bad_corder += len(FAILS) - before
            for k in CLIENT_EXTRA:
                if k in c and not isinstance(c[k], float):
                    fail(f'{label} client {k}={c[k]!r} is not a float')
            for k, v in c.items():
                if k in e_ctypes and type(v).__name__ not in e_ctypes[k]:
                    fail(f'{label} client {k}={v!r} is {type(v).__name__}, servers.php emits {sorted(e_ctypes[k])}')
    if bad_numip:
        fail(f'{label} {bad_numip}/{len(s_rows)} rows: numip is not servers.php\'s "4:<hex>"/"6:<hex>" string')
    if bad_noreply:
        fail(f'{label} {bad_noreply} rows: no-reply shape wrong (servers.php: ping -1 and NO nclients/clients)')
    if bad_type > 3:
        fail(f'{label} ... {bad_type} type mismatches in all')
    if not (bad_type or bad_numip or bad_noreply or bad_corder or bad_sorder):
        ok(f'{label} every row matches the servers.php grammar')
    # Feature/welcome/port2 COVERAGE — a WARN, never a FAIL. These fields are collected
    # per-server over successive probes and (for port2) only when a NAT-behind server happens
    # to answer from an alternate port, so the count at any instant is intermittent: Explorer's
    # own lon1 vs lon2 differ here too (the --control arm proves it). Structural correctness of
    # whatever IS carried is enforced above (key order, types); this only tracks how many rows
    # carry the optional field, and reports a shortfall for a human to watch, not a failure.
    for k in sorted(SERVER_OPTIONAL):
        s_n = sum(k in r for r in s_rows); e_n = sum(k in r for r in e_rows)
        msg = f'{label} rows carrying {k}: sampler {s_n}, explorer {e_n}'
        (ok if s_n >= e_n else warn)(msg)


def check_modes(args, a_server, pass1):
    """Behaviour for the non-directory requests, compared against servers.php's live answers.
    pass1: {directory: (status, headers, parsed rows)} from the directory pass, reused here."""
    S, E = args.sampler, args.explorer
    # no parameter
    st, _, body = get(sampler_url(S, ''))
    want = b'{"error":"No directory or server specified"}'
    (ok if st == 200 and body.strip() == want else fail)(f'[no param] sampler {st} {body[:60]!r}; servers.php 200 {want!r}')
    # unknown directory
    st, _, body = get(sampler_url(S, 'directory=nosuch.invalid:22124'))
    want = b'{"error":"Directory or server not found"}'
    (ok if st == 200 and body.strip() == want else fail)(f'[unknown dir] sampler {st} {body[:60]!r}; servers.php 200 {want!r}')
    # malformed (bad port, empty value): servers.php parse_host() fails → "Invalid …" (measured 2026-10-23)
    want = b'{"error":"Invalid directory or server specified"}'
    for q in ('directory=anygenre1.jamulus.io:abc', 'directory=', 'central='):
        st, _, body = get(sampler_url(S, q))
        (ok if st == 200 and body.strip() == want else fail)(f'[malformed ?{q}] sampler {st} {body[:60]!r}; servers.php 200 {want!r}')
    # IPv6 literal: servers.php resolves it, hears nothing, answers []
    st, _, body = get(sampler_url(S, 'directory=%5B2001:db8::1%5D:22124'))
    (ok if st == 200 and body.strip() == b'[]' else fail)(f'[ipv6 literal] sampler {st} {body[:60]!r}; servers.php 200 []')
    # single server that never answers → []
    st, _, body = get(sampler_url(S, 'server=127.0.0.1:1'))
    (ok if st == 200 and body.strip() == b'[]' else fail)(f'[server= no answer] sampler {st} {body[:60]!r}; servers.php 200 []')
    # a known directory must ALWAYS be a list (downstream consumers iterate it without checking)
    for d, (st, _, rows) in pass1.items():
        if st != 200 or not isinstance(rows, list):
            fail(f'[known dir {d}] sampler {st}, body not a list')
    if not any('[known dir' in f for f in FAILS):
        ok('[known dirs] all 7 answered 200 with a list')
    # on-demand sweep of an unconfigured directory (the alias), then a second request served from state
    if not args.control:
        t0 = time.time(); st, _, body, rows = get_json(sampler_url(S, f'directory={UNCONFIGURED_ALIAS}'), timeout=90); t1 = time.time() - t0
        if st != 200 or not isinstance(rows, list) or not rows:
            fail(f'[on-demand dir] sampler {st} {body[:60]!r} after {t1:.1f}s; servers.php: a list for any directory that answers')
        else:
            t0 = time.time(); st2, _, body2 = get(sampler_url(S, f'directory={UNCONFIGURED_ALIAS}')); t2 = time.time() - t0
            (ok if st2 == 200 and t2 < t1 else warn)(f'[on-demand dir] first request {len(rows)} rows in {t1:.1f}s, second in {t2:.2f}s (adopted: served from state)')
    # pretty
    st, _, body = get(sampler_url(S, f'directory={DIRECTORIES[0]}&pretty'))
    newlines = body.count(b'\n')
    (ok if st == 200 and newlines > 2 else fail)(f'[pretty] sampler {st}, {newlines} newlines (servers.php pretty-prints)')
    # content-type, from the directory pass
    h = next(iter(pass1.values()))[1] if pass1 else {}
    ct = {k.lower(): v for k, v in h.items()}.get('content-type', '')
    (ok if ct.startswith('application/json') else fail)(f'[content-type] sampler {ct!r}; servers.php application/json')
    if not a_server:
        warn('[server=/query=] no answering server found in the directory data, modes not checked'); return
    # single server
    st, _, body, rows = get_json(sampler_url(S, f'server={a_server}&name=Parity+Check'))
    if st != 200 or not isinstance(rows, list) or len(rows) != 1:
        fail(f'[server=] sampler {st} {body[:80]!r}; servers.php: 200, one-element list')
    else:
        r = rows[0]
        check_order('[server=] row', list(r), SINGLE_ORDER + sorted(SERVER_MIDDLE - {'nclients'}),
                    unordered=SERVER_MIDDLE, index_last=False)
        (ok if r.get('name') == 'Parity Check' else fail)(f'[server=] name={r.get("name")!r}; servers.php echoes &name= ("Parity Check")')
        (ok if isinstance(r.get('numip'), str) and NUMIP_RE.match(r['numip']) else fail)(f'[server=] numip={r.get("numip")!r}')
        if not any('[server=]' in f for f in FAILS):
            ok(f'[server=] one-element list, servers.php shape')
    # welcome query
    st, _, body, rows = get_json(sampler_url(S, f'query={a_server}'), timeout=40)
    if st != 200 or not isinstance(rows, list) or len(rows) != 1:
        fail(f'[query=] sampler {st} {body[:80]!r}; servers.php: 200, one-element list')
    else:
        # client_id / rawaudio / welcome appear only when the server sends them (old servers send no
        # CLIENT_ID at all), so the ground truth is servers.php's own answer for the same server.
        st_e, _, _, e_rows = get_json(f'{E}?query={a_server}', timeout=40)
        e_keys = set(e_rows[0]) if st_e == 200 and isinstance(e_rows, list) and e_rows else None
        base_missing = (QUERY_KEYS - {'client_id', 'rawaudio', 'welcome'}) - set(rows[0])
        if base_missing:
            fail(f'[query=] keys missing vs servers.php: {sorted(base_missing)}')
        elif e_keys is None:
            warn(f'[query=] shape ok; servers.php did not answer for {a_server}, optional keys not compared')
        else:
            diff = set(rows[0]) ^ e_keys
            (ok if not diff else warn)(f'[query=] keys {"match" if not diff else "differ: " + str(sorted(diff))} servers.php for {a_server} (optional keys depend on timing)')


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--sampler', default='http://127.0.0.1:5001', help='sampler base URL or a servers.php URL')
    ap.add_argument('--explorer', default='https://explorer.jamulus.io/servers.php')
    ap.add_argument('--control', action='store_true', help='compare Explorer lon1 vs lon2 instead (timing-noise baseline)')
    ap.add_argument('--no-modes', action='store_true', help='skip the server=/query=/error-shape checks')
    args = ap.parse_args()
    if args.control:
        args.sampler = 'https://explorer.jamulus.io/servers-lon2.php'
    ctx = {'e_types': {}, 'e_ctypes': {}}
    a_server = None
    pairs = []
    pass1 = {}      # directory → (sampler status, headers, rows) for check_modes
    # Pass 1: fetch everything and learn servers.php's value types over ALL directories, so a
    # type that only one directory happens to show (a null city, say) is not a false mismatch.
    for d in DIRECTORIES:
        st_e, _, body_e, e_rows = get_json(f'{args.explorer}?directory={d}')
        st_s, h_s, body_s, s_rows = get_json(sampler_url(args.sampler, f'directory={d}'))
        pass1[d] = (st_s, h_s, s_rows)
        if st_e != 200:
            warn(f'[{d}] explorer returned {st_e}; skipped'); continue
        if st_s != 200:
            fail(f'[{d}] sampler returned {st_s}: {body_s[:80]!r}'); continue
        if e_rows is None or s_rows is None:
            fail(f'[{d}] JSON parse failed (explorer ok={e_rows is not None}, sampler ok={s_rows is not None})'); continue
        if isinstance(e_rows, dict):
            warn(f'[{d}] explorer answered an error object {e_rows}; skipped'); continue
        for k, v in type_map(e_rows).items():
            ctx['e_types'].setdefault(k, set()).update(v)
        for k, v in type_map([c for r in e_rows for c in r.get('clients', [])]).items():
            ctx['e_ctypes'].setdefault(k, set()).update(v)
        if a_server is None:
            for r in e_rows:
                if r.get('ping', -1) >= 0 and r.get('nclients', 0) == 0 and r.get('ip'):
                    a_server = f"{r['ip']}:{r['port']}"; break
        pairs.append((d, s_rows, e_rows))
    # Pass 2: check.
    for d, s_rows, e_rows in pairs:
        check_directory(ctx, d, s_rows, e_rows)
    if not args.no_modes:
        check_modes(args, a_server, pass1)
    print(f'\n{len(FAILS)} FAIL, {len(WARNS)} WARN')
    sys.exit(1 if FAILS else 0)


if __name__ == '__main__':
    main()
