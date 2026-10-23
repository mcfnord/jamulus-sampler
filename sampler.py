#!/usr/bin/env python3
"""
jamulus-sampler daemon — persistent background prober replacing servers.php.
Background thread probes servers at adaptive rates; HTTP handler reads in-memory
state instantly without blocking on UDP.

A drop-in for softins' servers.php: GET /servers (alias /servers.php) with
?directory= (or ?central=), ?server=…&name=, ?query=, and &pretty answer in
servers.php's JSON shape — same keys, key order, value types, error bodies.
The only additions are first_seen / last_absent (Unix floats) per client, and the
sampler's own routes /servers/all and /stats. tools/parity.py is the acceptance test.

Environment: SAMPLER_HTTP_PORT (5001), SAMPLER_UDP_BASE (22134: sweep socket, then 15 pool
ports above it), SAMPLER_ADMIN_IPS (comma list allowed to POST /ping-density and read
/stats, /debug/stuck; 127.0.0.1 is always allowed).
"""

import heapq
import json
import os
import re
import socket
import struct
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

# ── Deployment knobs (env) ────────────────────────────────────────────────────

HTTP_PORT = int(os.environ.get('SAMPLER_HTTP_PORT', '5001'))
UDP_BASE  = int(os.environ.get('SAMPLER_UDP_BASE', '22134'))
ADMIN_IPS = {'127.0.0.1'} | {
    a.strip() for a in os.environ.get('SAMPLER_ADMIN_IPS', '24.199.107.192').split(',') if a.strip()
}
ADOPT_MAX = 8           # directories beyond DIRECTORIES that requests may make us sample

# ── Protocol constants ────────────────────────────────────────────────────────

CLIENT_PORT_START = UDP_BASE + 1
CLIENT_PORT_RANGE = 15
SWEEP_PORT        = UDP_BASE   # fixed sweep socket port — same as servers.php CLIENT_PORT
# TEST 2026-08-22: directories listed here get their OWN sweep socket on the given port
# instead of sharing SWEEP_PORT, so their sweeps run concurrently with the rest.
# Empty dict == original behaviour (all sweeps serialized on 22134).
SWEEP_PORT_OVERRIDES = {}   # per-directory sweep port. Deliberately EMPTY: giving choral its
                            # own socket on 22150 changed nothing (ratio 1.00x, 2026-08-22,
                            # re-confirmed over 3.6 more days). Sweep serialization is NOT the
                            # bottleneck — the dead-server probe backlog is. Do not re-run.
TIMEOUT_SEC       = 0.5
MAX_ATTEMPTS      = 3

CLM_START                   = 1000
CLM_PING_MS_WITHNUMCLIENTS  = 1002
CLM_SERVER_LIST             = 1006
CLM_REQ_SERVER_LIST         = 1007
CLM_DISCONNECTION           = 1010
CLM_VERSION_AND_OS          = 1011
CLM_REQ_VERSION_AND_OS      = 1012
CLM_CONN_CLIENTS_LIST       = 1013
CLM_REQ_CONN_CLIENTS_LIST   = 1014
CLM_SERVER_FEATURES         = 1019
CLM_REQ_SERVER_FEATURES     = 1020
CLM_WELCOME_MESSAGE         = 1021
CLM_REQ_WELCOME_MESSAGE     = 1022
# connected-mode messages seen during ?query= (a short audio connection, as servers.php does)
ACKN                        = 1
CHAT_TEXT                   = 18
CLIENT_ID                   = 32
RAWAUDIO_SUPPORTED          = 36

# CLM_SERVER_FEATURES bit names, verbatim from servers.php $features (bit index = position)
FEATURE_BITS = ['fast_update', 'multithreading', 'recorder_enabled', 'is_recording', 'delay_pan',
                'ipv6_available', 'raw_audio_available', 'discon_on_quit', 'has_welcome_message',
                'is_logging', 'has_licence', 'has_gui', 'rpc_enabled']

# servers.php error bodies, byte for byte
ERR_NO_TARGET  = {'error': 'No directory or server specified'}
ERR_INVALID    = {'error': 'Invalid directory or server specified'}
ERR_NOT_FOUND  = {'error': 'Directory or server not found'}

# (probe_active, probe_stable, probe_idle) per global activity tier
_TIER_RATES = {
    "quiet":  (4.0, 10.0, 20.0),
    "normal": (4.0,  8.0, 15.0),
    "busy":   (4.0,  6.0, 10.0),
}
GLOBAL_NORMAL_PERCENTILE = 90  # active_pool above this percentile of yesterday → normal idle cap
GLOBAL_BUSY_PERCENTILE   = 98  # active_pool above this percentile of yesterday → busy idle cap
WATCHDOG_QUEUE_RATIO = 0.5  # warn when >this share of SERVER_STATE is queued but not started.
WATCHDOG_WARN_EVERY  = 300.0  # s — throttle; the scheduler loop runs ~1x/s.
                            # The old gate was `_n_inf == _n_tot`, which never fired: silent
                            # at 96% queued (3,689/3,851, 2026-08-22) and 0 hits in 3.6 days.
DIRECTORY_SWEEP =  90.0   # s — re-query directory; NAT TTL measured ≥120s
REAP_AFTER      = 1800.0  # s — retire a server unlisted by EVERY directory this long.
                          # Membership-based on purpose: punch-required fleet servers
                          # fail every pool probe by design yet stay listed, so a
                          # reachability-based reaper would retire our own fleet.

# Directories pre-probed on startup (the 7 queried by gather-server-data.py)
DIRECTORIES = [
    'anygenre1.jamulus.io:22124',
    'anygenre2.jamulus.io:22224',
    'anygenre3.jamulus.io:22624',
    'rock.jamulus.io:22424',
    'jazz.jamulus.io:22324',
    'classical.jamulus.io:22524',
    'choral.jamulus.io:22724',
]

# ── Lookup tables — verbatim from /var/www/html/servers.php ──────────────────

COUNTRIES = {
    0: '-', 1: 'Afghanistan', 2: 'Albania', 3: 'Algeria', 4: 'American Samoa',
    5: 'Andorra', 6: 'Angola', 7: 'Anguilla', 8: 'Antarctica',
    9: 'Antigua And Barbuda', 10: 'Argentina', 11: 'Armenia', 12: 'Aruba',
    13: 'Australia', 14: 'Austria', 15: 'Azerbaijan', 16: 'Bahamas',
    17: 'Bahrain', 18: 'Bangladesh', 19: 'Barbados', 20: 'Belarus',
    21: 'Belgium', 22: 'Belize', 23: 'Benin', 24: 'Bermuda', 25: 'Bhutan',
    26: 'Bolivia', 27: 'Bosnia And Herzegowina', 28: 'Botswana',
    29: 'Bouvet Island', 30: 'Brazil', 31: 'British Indian Ocean Territory',
    32: 'Brunei', 33: 'Bulgaria', 34: 'Burkina Faso', 35: 'Burundi',
    36: 'Cambodia', 37: 'Cameroon', 38: 'Canada', 39: 'Cape Verde',
    40: 'Cayman Islands', 41: 'Central African Republic', 42: 'Chad',
    43: 'Chile', 44: 'China', 45: 'Christmas Island', 46: 'Cocos Islands',
    47: 'Colombia', 48: 'Comoros', 49: 'Congo Kinshasa',
    50: 'Congo Brazzaville', 51: 'Cook Islands', 52: 'Costa Rica',
    53: 'Ivory Coast', 54: 'Croatia', 55: 'Cuba', 56: 'Cyprus',
    57: 'Czech Republic', 58: 'Denmark', 59: 'Djibouti', 60: 'Dominica',
    61: 'Dominican Republic', 62: 'East Timor', 63: 'Ecuador', 64: 'Egypt',
    65: 'El Salvador', 66: 'Equatorial Guinea', 67: 'Eritrea', 68: 'Estonia',
    69: 'Ethiopia', 70: 'Falkland Islands', 71: 'Faroe Islands', 72: 'Fiji',
    73: 'Finland', 74: 'France', 75: 'Guernsey', 76: 'French Guiana',
    77: 'French Polynesia', 78: 'French Southern Territories', 79: 'Gabon',
    80: 'Gambia', 81: 'Georgia', 82: 'Germany', 83: 'Ghana',
    84: 'Gibraltar', 85: 'Greece', 86: 'Greenland', 87: 'Grenada',
    88: 'Guadeloupe', 89: 'Guam', 90: 'Guatemala', 91: 'Guinea',
    92: 'Guinea Bissau', 93: 'Guyana', 94: 'Haiti',
    95: 'Heard And McDonald Islands', 96: 'Honduras', 97: 'Hong Kong',
    98: 'Hungary', 99: 'Iceland', 100: 'India', 101: 'Indonesia',
    102: 'Iran', 103: 'Iraq', 104: 'Ireland', 105: 'Israel', 106: 'Italy',
    107: 'Jamaica', 108: 'Japan', 109: 'Jordan', 110: 'Kazakhstan',
    111: 'Kenya', 112: 'Kiribati', 113: 'North Korea', 114: 'South Korea',
    115: 'Kuwait', 116: 'Kyrgyzstan', 117: 'Laos', 118: 'Latvia',
    119: 'Lebanon', 120: 'Lesotho', 121: 'Liberia', 122: 'Libya',
    123: 'Liechtenstein', 124: 'Lithuania', 125: 'Luxembourg', 126: 'Macau',
    127: 'Macedonia', 128: 'Madagascar', 129: 'Malawi', 130: 'Malaysia',
    131: 'Maldives', 132: 'Mali', 133: 'Malta', 134: 'Marshall Islands',
    135: 'Martinique', 136: 'Mauritania', 137: 'Mauritius', 138: 'Mayotte',
    139: 'Mexico', 140: 'Micronesia', 141: 'Moldova', 142: 'Monaco',
    143: 'Mongolia', 144: 'Montserrat', 145: 'Morocco', 146: 'Mozambique',
    147: 'Myanmar', 148: 'Namibia', 149: 'Nauru Country', 150: 'Nepal',
    151: 'Netherlands', 152: 'Cura Sao', 153: 'New Caledonia',
    154: 'New Zealand', 155: 'Nicaragua', 156: 'Niger', 157: 'Nigeria',
    158: 'Niue', 159: 'Norfolk Island', 160: 'Northern Mariana Islands',
    161: 'Norway', 162: 'Oman', 163: 'Pakistan', 164: 'Palau',
    165: 'Palestinian Territories', 166: 'Panama', 167: 'Papua New Guinea',
    168: 'Paraguay', 169: 'Peru', 170: 'Philippines', 171: 'Pitcairn',
    172: 'Poland', 173: 'Portugal', 174: 'Puerto Rico', 175: 'Qatar',
    176: 'Reunion', 177: 'Romania', 178: 'Russia', 179: 'Rwanda',
    180: 'Saint Kitts And Nevis', 181: 'Saint Lucia',
    182: 'Saint Vincent And The Grenadines', 183: 'Samoa',
    184: 'San Marino', 185: 'Sao Tome And Principe', 186: 'Saudi Arabia',
    187: 'Senegal', 188: 'Seychelles', 189: 'Sierra Leone',
    190: 'Singapore', 191: 'Slovakia', 192: 'Slovenia',
    193: 'Solomon Islands', 194: 'Somalia', 195: 'South Africa',
    196: 'South Georgia And The South Sandwich Islands', 197: 'Spain',
    198: 'Sri Lanka', 199: 'Saint Helena',
    200: 'Saint Pierre And Miquelon', 201: 'Sudan', 202: 'Suriname',
    203: 'Svalbard And Jan Mayen Islands', 204: 'Swaziland', 205: 'Sweden',
    206: 'Switzerland', 207: 'Syria', 208: 'Taiwan', 209: 'Tajikistan',
    210: 'Tanzania', 211: 'Thailand', 212: 'Togo', 213: 'Tokelau Country',
    214: 'Tonga', 215: 'Trinidad And Tobago', 216: 'Tunisia',
    217: 'Turkey', 218: 'Turkmenistan', 219: 'Turks And Caicos Islands',
    220: 'Tuvalu Country', 221: 'Uganda', 222: 'Ukraine',
    223: 'United Arab Emirates', 224: 'United Kingdom',
    225: 'United States', 226: 'United States Minor Outlying Islands',
    227: 'Uruguay', 228: 'Uzbekistan', 229: 'Vanuatu',
    230: 'Vatican City State', 231: 'Venezuela', 232: 'Vietnam',
    233: 'British Virgin Islands', 234: 'United States Virgin Islands',
    235: 'Wallis And Futuna Islands', 236: 'Western Sahara', 237: 'Yemen',
    238: 'Canary Islands', 239: 'Zambia', 240: 'Zimbabwe',
    241: 'Clipperton Island', 242: 'Montenegro', 243: 'Serbia',
    244: 'Saint Barthelemy', 245: 'Saint Martin', 246: 'Latin America',
    247: 'Ascension Island', 248: 'Aland Islands', 249: 'Diego Garcia',
    250: 'Ceuta And Melilla', 251: 'Isle Of Man', 252: 'Jersey',
    253: 'Tristan Da Cunha', 254: 'South Sudan', 255: 'Bonaire',
    256: 'Sint Maarten', 257: 'Kosovo', 258: 'European Union',
    259: 'Outlying Oceania', 260: 'World', 261: 'Europe',
}

INSTRUMENTS = {
    0: '-', 1: 'Drums', 2: 'Djembe', 3: 'Electric Guitar',
    4: 'Acoustic Guitar', 5: 'Bass Guitar', 6: 'Keyboard',
    7: 'Synthesizer', 8: 'Grand Piano', 9: 'Accordion', 10: 'Vocal',
    11: 'Microphone', 12: 'Harmonica', 13: 'Trumpet', 14: 'Trombone',
    15: 'French Horn', 16: 'Tuba', 17: 'Saxophone', 18: 'Clarinet',
    19: 'Flute', 20: 'Violin', 21: 'Cello', 22: 'Double Bass',
    23: 'Recorder', 24: 'Streamer', 25: 'Listener', 26: 'Guitar Vocal',
    27: 'Keyboard Vocal', 28: 'Bodhran', 29: 'Bassoon', 30: 'Oboe',
    31: 'Harp', 32: 'Viola', 33: 'Congas', 34: 'Bongo', 35: 'Vocal Bass',
    36: 'Vocal Tenor', 37: 'Vocal Alto', 38: 'Vocal Soprano', 39: 'Banjo',
    40: 'Mandolin', 41: 'Ukulele', 42: 'Bass Ukulele',
    43: 'Vocal Baritone', 44: 'Vocal Lead', 45: 'Mountain Dulcimer',
    46: 'Scratching', 47: 'Rapping', 48: 'Vibraphone', 49: 'Conductor',
}

SKILLS = {0: '-', 1: 'Beginner', 2: 'Intermediate', 3: 'Expert'}
OPSYS  = {0: 'Windows', 1: 'MacOS', 2: 'Linux', 3: 'Android', 4: 'iOS', 5: 'Unix'}

# ── CRC (ported from servers.php CRC class) ───────────────────────────────────

def compute_crc(data: bytes) -> int:
    sr = ~0; bmask = 0x10000; poly = 0x1020
    for byte in data:
        for i in range(8):
            sr <<= 1
            if sr & bmask: sr |= 1
            if byte & (1 << (7 - i)): sr ^= 1
            if sr & 1: sr ^= poly
    return (~sr) & (bmask - 1)

def build_packet(msg_id: int, payload: bytes = b'', cnt: int = 0) -> bytes:
    header = struct.pack('<HHBH', 0, msg_id, cnt, len(payload))
    body = header + payload
    return body + struct.pack('<H', compute_crc(body))

def build_ackn(cnt: int, msg_id: int) -> bytes:
    """Acknowledge a connected-mode message (servers.php send_ackn): payload is the acked id."""
    return build_packet(ACKN, struct.pack('<H', msg_id), cnt)

# servers.php send_audio(): a 25-byte pseudo audio frame that makes a server open a channel
AUDIO_PROBE = struct.pack('BBB', 0, 255, 254) + b'\0' * 22

def parse_header(data: bytes):
    """Return (msg_id, cnt, payload) or None on CRC/length error."""
    if len(data) < 9:
        return None
    _, msg_id, cnt, length = struct.unpack_from('<HHBH', data, 0)
    if length + 9 != len(data):
        return None
    if compute_crc(data[:-2]) != struct.unpack('<H', data[-2:])[0]:
        return None
    return msg_id, cnt, data[7:-2]

# ── IP helpers ────────────────────────────────────────────────────────────────

def ip_from_numip(n: int) -> str:
    return socket.inet_ntoa(struct.pack('>I', n))

def numip_of(ip_str: str) -> int:
    return struct.unpack('>I', socket.inet_aton(ip_str))[0]

def numip_str(ip_str: str) -> str:
    """servers.php's numip: '4:' + hex of the packed IPv4 ('6:' for IPv6, so v4 sorts first)."""
    try:
        return '4:' + socket.inet_pton(socket.AF_INET, ip_str).hex()
    except OSError:
        return '6:' + socket.inet_pton(socket.AF_INET6, ip_str).hex()

_HOST_RE = re.compile(r'^([^\[:\]]+|\[(.*:.*:.*)\])(:(\d+))?$')   # servers.php parse_host()

def parse_host(s: str):
    """Return (host, port) exactly as servers.php parse_host(), or None when malformed."""
    m = _HOST_RE.match(s or '')
    if not m:
        return None
    host = m.group(2) if m.group(2) is not None else m.group(1)
    return host, int(m.group(4)) if m.group(4) is not None else 22124

def parse_features(payload: bytes) -> dict:
    feat = struct.unpack_from('<I', payload)[0]
    return {name: bool(feat & (1 << bit)) for bit, name in enumerate(FEATURE_BITS)}

def parse_u16_string(payload: bytes) -> str:
    n = struct.unpack_from('<H', payload)[0]
    return payload[2:2 + n].decode('utf-8', errors='replace')

# ── Version string parsing ────────────────────────────────────────────────────

_VER_RE = re.compile(r'((\d+)\.(\d+)\.(\d+)([^:]*))(:(.*))?' )

def parse_version(s: str):
    m = _VER_RE.match(s)
    if not m:
        return s, ''
    ver    = m.group(1)
    suffix = m.group(5) or ''
    ts     = m.group(7)
    if suffix == '':
        k = '='
    elif suffix.startswith(('rc', 'beta', 'alpha')):
        k = '<'
    elif not ts:
        k = '>'
    else:
        k = '?'; suffix = ts
    vsort = f"{int(m.group(2)):03d}{int(m.group(3)):03d}{int(m.group(4)):03d}{k}{suffix}"
    return ver, vsort

# ── Port pool — each probe holds one port from 22135–22149 ────────────────────

_port_pool    = list(range(CLIENT_PORT_START, CLIENT_PORT_START + CLIENT_PORT_RANGE))
_port_lock    = threading.Lock()
_port_sem     = threading.Semaphore(CLIENT_PORT_RANGE)
_ports_in_use = 0   # current concurrent port holders
_ports_peak   = 0   # high-water mark

def _acquire_port() -> int:
    global _ports_in_use, _ports_peak
    _port_sem.acquire()
    with _port_lock:
        _ports_in_use += 1
        if _ports_in_use > _ports_peak:
            _ports_peak = _ports_in_use
        return _port_pool.pop()

def _release_port(p: int):
    global _ports_in_use
    with _port_lock:
        _port_pool.append(p)
        _ports_in_use -= 1
    _port_sem.release()

# ── Persistent sweep socket (port 22134, same as servers.php CLIENT_PORT) ─────
# All directory sweeps share this one socket so NAT hole-punches persist across
# the 90s sweep interval. Sweeps are serialized via _sweep_lock.

_sweep_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
_sweep_sock.bind(('0.0.0.0', SWEEP_PORT))
_sweep_sock.settimeout(TIMEOUT_SEC)
_sweep_lock = threading.Lock()

# TEST 2026-08-22: overridden directories get a dedicated socket + lock, created lazily.
_sweep_extra      = {}                 # 'host:port' -> (socket, lock)
_sweep_extra_lock = threading.Lock()

def _sweep_channel(host: str, port: int):
    """Return (socket, lock) for this directory's sweep.

    Default is the shared 22134 socket serialized by _sweep_lock. A directory named in
    SWEEP_PORT_OVERRIDES gets its own bound socket and its own lock, so it sweeps
    concurrently with the others. The port must be STABLE: the directory advertises our
    source port to every registered server as the hole-punch return address.
    """
    key = f'{host}:{port}'
    p = SWEEP_PORT_OVERRIDES.get(key)
    if p is None:
        return _sweep_sock, _sweep_lock
    with _sweep_extra_lock:
        ch = _sweep_extra.get(key)
        if ch is None:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.bind(('0.0.0.0', p))
            s.settimeout(TIMEOUT_SEC)
            ch = (s, threading.Lock())
            _sweep_extra[key] = ch
            print(f'[sweep] {key} using DEDICATED sweep port {p}', file=sys.stderr, flush=True)
        return ch

# ── Packet parsing helpers ────────────────────────────────────────────────────

def _parse_server_list(payload: bytes, dir_ip: str, dir_port: int) -> list:
    servers = []; i = 0; n = len(payload)
    while i < n:
        if i + 12 > n: break
        numip_s, port_s, countryid, maxclients, perm, name_len = \
            struct.unpack_from('<IHHBBH', payload, i); i += 12
        name = payload[i:i + name_len].decode('utf-8', errors='replace'); i += name_len
        il = struct.unpack_from('<H', payload, i)[0]; i += 2
        ipaddrs = payload[i:i + il].decode('utf-8', errors='replace'); i += il
        cl = struct.unpack_from('<H', payload, i)[0]; i += 2
        city = payload[i:i + cl].decode('utf-8', errors='replace'); i += cl
        if numip_s == 0 and port_s == 0:
            s_ip, s_numip, s_port = dir_ip, numip_of(dir_ip), dir_port
        else:
            s_numip, s_port = numip_s, port_s
            s_ip = ip_from_numip(numip_s)
        servers.append({'ip': s_ip, 'port': s_port, 'numip': s_numip,
                        'name': name, 'countryid': countryid,
                        'country': COUNTRIES.get(countryid, 'Unknown'),
                        'city': city, 'maxclients': maxclients,
                        'perm': perm, 'ipaddrs': ipaddrs})
    return servers

def _parse_client_list(payload: bytes) -> list:
    clients = []; i = 0; plen = len(payload)
    while i + 14 <= plen:
        chanid, countryid, instrumentid, skillid, _ip, name_len = \
            struct.unpack_from('<BHIBIH', payload, i); i += 14
        name = payload[i:i + name_len].decode('utf-8', errors='replace'); i += name_len
        cl = struct.unpack_from('<H', payload, i)[0]; i += 2
        city = payload[i:i + cl].decode('utf-8', errors='replace'); i += cl
        clients.append({'chanid': chanid, 'countryid': countryid,
                        'instrumentid': instrumentid, 'skillid': skillid,
                        'country': COUNTRIES.get(countryid, 'Unknown'),
                        'instrument': INSTRUMENTS.get(instrumentid, 'Unknown'),
                        'skill': SKILLS.get(skillid, 'Unknown'),
                        'name': name, 'city': city})
    return clients

# ── UDP operations ────────────────────────────────────────────────────────────

def sweep_directory(host: str, port: int):
    """Query directory for server list, then collect server-initiated pings on the same socket.
    When the directory receives CLM_REQ_SERVER_LIST it tells each registered server to ping
    back to our source port — this is the NAT hole-punch that makes servers reachable.
    Returns (server_list, probe_results, None) or (None, None, error_str).
    probe_results: dict of 'ip:port' → {'ping': ms, 'nclients': n, 'clients': [...]}
    """
    try:
        ip = socket.gethostbyname(host)
    except socket.gaierror as e:
        return None, None, str(e)

    _sweep_s, _sweep_l = _sweep_channel(host, port)
    with _sweep_l:
        sock = _sweep_s

        # Phase 1: get server list from directory.
        # Buffer any 1002 pings that arrive while waiting for 1006 — the directory
        # introduction can trigger server pings before we receive the server list.
        server_list = None
        phase1_buffered = []   # (data, addr) pairs received before 1006 arrived
        for _ in range(MAX_ATTEMPTS):
            sock.sendto(build_packet(CLM_REQ_SERVER_LIST), (ip, port))
            try:
                while True:
                    data, addr = sock.recvfrom(32767)
                    parsed = parse_header(data)
                    if parsed and parsed[0] == CLM_SERVER_LIST:
                        server_list = _parse_server_list(parsed[2], ip, port)
                        break
                    elif parsed:
                        phase1_buffered.append((parsed, addr))
            except socket.timeout:
                pass
            if server_list is not None:
                break

        if server_list is None:
            return None, None, f"no response from {host}:{port} after {MAX_ATTEMPTS} attempts"

        # Phase 2: collect server-initiated pings. The directory has now notified each
        # registered server to send CLM_PING_MS_WITHNUMCLIENTS to our fixed sweep port — that
        # exchange opens the NAT hole-punch path.
        #
        # Two-ping protocol (matches servers.php): on the FIRST hole-punch ping we send a
        # ping back to the server so it sees bidirectional traffic; on the SECOND response
        # we send 1014. This is required for servers behind strict NAT — our outbound 1014
        # would be blocked if we skip the return ping.
        server_keys = {f"{s['ip']}:{s['port']}" for s in server_list}
        probe_results = {}   # ip:port → {ping, nclients, clients, [os, version, versionsort, features, welcome, port2]}
        first_ping_sent = set()   # keys where we sent a return ping (awaiting 2nd response)
        clients_requested = set()
        aliases = {}   # "ip:replyport" → listed key, for a server answering from another port
        port2 = {}     # listed key → that reply port (servers.php's port2)
        ms = int(time.time() * 1000) % 86400000
        return_ping = build_packet(CLM_PING_MS_WITHNUMCLIENTS, struct.pack('<IB', ms, 0))
        # servers.php fires these three at every server right after its second ping reply
        info_requests = (build_packet(CLM_REQ_VERSION_AND_OS),
                         build_packet(CLM_REQ_SERVER_FEATURES),
                         build_packet(CLM_REQ_WELCOME_MESSAGE))

        def _key_for(fip, fport):
            """Listed key for a reply, or None. servers.php port2: a reply from a listed IP on an
            unlisted port is taken for the one still-silent server at that IP."""
            key = f"{fip}:{fport}"
            if key in server_keys:
                return key
            if key in aliases:
                return aliases[key]
            cands = [k for k in server_keys
                     if k.rsplit(':', 1)[0] == fip and k not in probe_results]
            if len(cands) != 1:
                return None
            aliases[key] = cands[0]
            port2[cands[0]] = fport
            return cands[0]

        def _apply_info(key, mid, payload):
            """Version / features / welcome replies — the same fields the pool probe fills."""
            pr = probe_results.get(key)
            if pr is None:
                return False
            if mid == CLM_VERSION_AND_OS and len(payload) >= 3:
                os_id, vlen = struct.unpack_from('<BH', payload)
                pr['os'] = OPSYS.get(os_id, 'Unknown')
                pr['version'], pr['versionsort'] = parse_version(
                    payload[3:3 + vlen].decode('utf-8', errors='replace'))
            elif mid == CLM_SERVER_FEATURES and len(payload) >= 4:
                pr['features'] = parse_features(payload)
            elif mid == CLM_WELCOME_MESSAGE and len(payload) >= 2:
                pr['welcome'] = parse_u16_string(payload)
            else:
                return False
            return True

        def _process_p2_packet(parsed, fip, fport):
            """Process one Phase 2 packet in-place (modifies probe_results etc.)."""
            mid, _, payload = parsed
            key = _key_for(fip, fport)
            if key is None:
                return
            if mid == CLM_PING_MS_WITHNUMCLIENTS and len(payload) >= 5:
                timems_r, nclients = struct.unpack_from('<IB', payload)
                now_ms = int(time.time() * 1000) % 86400000
                rtt = now_ms - timems_r
                if key not in first_ping_sent:
                    probe_results[key] = {'ping': 0, 'nclients': nclients, 'clients': [],
                                          'port2': port2.get(key)}
                    sock.sendto(return_ping, (fip, fport))
                    first_ping_sent.add(key)
                    if nclients > 0:
                        print(f'[sweep] {host}:{port} 1st ping from {key} nclients={nclients} sent_return_ping', file=sys.stderr, flush=True)
                else:
                    probe_results[key]['ping'] = rtt if 0 <= rtt < 30000 else 0
                    probe_results[key]['nclients'] = nclients
                    for pkt in info_requests:
                        sock.sendto(pkt, (fip, fport))
                    if nclients > 0 and key not in clients_requested:
                        sock.sendto(build_packet(CLM_REQ_CONN_CLIENTS_LIST), (fip, fport))
                        clients_requested.add(key)
                        print(f'[sweep] {host}:{port} 2nd ping from {key} nclients={nclients} sent_1014', file=sys.stderr, flush=True)
            elif mid == CLM_CONN_CLIENTS_LIST and key in probe_results:
                probe_results[key]['clients'] = _parse_client_list(payload)
                print(f'[sweep] {host}:{port} got 1013 from {key} clients={len(probe_results[key]["clients"])}', file=sys.stderr, flush=True)
            else:
                _apply_info(key, mid, payload)

        # Replay pings buffered during Phase 1 before entering the live receive loop.
        for parsed_buf, (bip, bport) in phase1_buffered:
            _process_p2_packet(parsed_buf, bip, bport)

        deadline = time.time() + TIMEOUT_SEC
        if clients_requested:
            deadline = max(deadline, time.time() + TIMEOUT_SEC * MAX_ATTEMPTS)
        while time.time() < deadline:
            try:
                data, (fip, fport) = sock.recvfrom(32767)
                parsed = parse_header(data)
                if parsed:
                    prev_req = len(clients_requested)
                    _process_p2_packet(parsed, fip, fport)
                    if len(clients_requested) > prev_req:
                        deadline = max(deadline, time.time() + TIMEOUT_SEC * MAX_ATTEMPTS)
                    elif _key_for(fip, fport) in first_ping_sent - clients_requested:
                        deadline = max(deadline, time.time() + TIMEOUT_SEC)
            except socket.timeout:
                clients_pending = any(
                    not probe_results[k]['clients']
                    for k in clients_requested if k in probe_results
                )
                if len(probe_results) >= len(server_list) and not clients_pending:
                    break  # heard from all servers and all pending 1013s collected

        # Phase 3: active probe for servers that didn't ping back during the passive window.
        # Send CLM_PING from this same socket (same source port the directory advertised to
        # each server) — the hole-punch path is already open, so strict-firewall servers
        # like those behind certain NAT/firewall configs will respond here but not to
        # probe_server (which uses a different pool port they've never seen).
        missed = server_keys - set(probe_results.keys())
        if missed:
            ms = int(time.time() * 1000) % 86400000
            ping_pkt = build_packet(CLM_PING_MS_WITHNUMCLIENTS, struct.pack('<IB', ms, 0))
            for key in missed:
                s_ip, s_port_str = key.rsplit(':', 1)
                try:
                    sock.sendto(ping_pkt, (s_ip, int(s_port_str)))
                except OSError:
                    pass
            deadline = time.time() + TIMEOUT_SEC
            while time.time() < deadline:
                try:
                    data, (fip, fport) = sock.recvfrom(32767)
                    parsed = parse_header(data)
                    if not parsed:
                        continue
                    mid, _, payload = parsed
                    key = _key_for(fip, fport)
                    if key is None:
                        continue
                    if mid == CLM_PING_MS_WITHNUMCLIENTS and len(payload) >= 5:
                        if key in probe_results:
                            continue  # already have this server's ping
                        timems_r, nclients = struct.unpack_from('<IB', payload)
                        now_ms = int(time.time() * 1000) % 86400000
                        rtt = now_ms - timems_r
                        probe_results[key] = {'ping': rtt if 0 <= rtt < 30000 else 0,
                                              'nclients': nclients, 'clients': [],
                                              'port2': port2.get(key)}
                        for pkt in info_requests:
                            sock.sendto(pkt, (fip, fport))
                        deadline = max(deadline, time.time() + TIMEOUT_SEC)
                        if nclients > 0 and key not in clients_requested:
                            sock.sendto(build_packet(CLM_REQ_CONN_CLIENTS_LIST), (fip, fport))
                            clients_requested.add(key)
                            deadline = max(deadline, time.time() + TIMEOUT_SEC * MAX_ATTEMPTS)
                            print(f'[sweep] {host}:{port} phase3 ping from {key} nclients={nclients} sent_1014', file=sys.stderr, flush=True)
                    elif mid == CLM_CONN_CLIENTS_LIST and key in probe_results:
                        probe_results[key]['clients'] = _parse_client_list(payload)
                        print(f'[sweep] {host}:{port} phase3 got 1013 from {key} clients={len(probe_results[key]["clients"])}', file=sys.stderr, flush=True)
                    else:
                        _apply_info(key, mid, payload)
                except socket.timeout:
                    clients_pending = any(
                        not probe_results[k]['clients']
                        for k in clients_requested if k in probe_results
                    )
                    if not clients_pending:
                        break

        return server_list, probe_results, None


def probe_server(ip: str, port: int, wait_optional: bool = True):
    """Probe one server: 2-pass ping RTT, then servers.php's request burst — version, features,
    welcome, client list. A reply from the same IP on another port is servers.php's "port2":
    the conversation continues there and the row carries it. Features/welcome are optional
    (pre-1019 servers never answer them): once the required replies are in they get one more
    timeout, or none when wait_optional is False.
    Returns result dict; ping=-1 if unreachable."""
    lport = _acquire_port()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(TIMEOUT_SEC)
    try:
        sock.bind(('0.0.0.0', lport))

        result = {'ping': -1, 'nclients': 0, 'clients': [],
                  'version': '', 'versionsort': '', 'os': '',
                  'features': None, 'welcome': None, 'port2': None}
        dst          = (ip, port)
        ping_pass    = 0     # 0=need 1st, 1=need 2nd, 2=done
        want_clients = False
        got_version  = False
        got_clients  = False
        got_features = False
        got_welcome  = False
        grace        = None  # deadline for the optional replies once the required ones are in
        timeouts     = 0

        def send_ping():
            ms = int(time.time() * 1000) % 86400000
            sock.sendto(build_packet(CLM_PING_MS_WITHNUMCLIENTS,
                                     struct.pack('<IB', ms, 0)), dst)

        send_ping()

        deadline = time.time() + TIMEOUT_SEC * MAX_ATTEMPTS * 4
        while time.time() < deadline:
            try:
                data, (fip, fport) = sock.recvfrom(32767)
                if fip != ip:
                    continue          # stray packet from wrong IP — don't reset timeout
                timeouts = 0
                parsed = parse_header(data)
                if not parsed:
                    continue
                mid, _, payload = parsed

                if mid == CLM_PING_MS_WITHNUMCLIENTS:
                    if len(payload) < 5:
                        continue
                    if fport != dst[1]:
                        if ping_pass != 0:
                            continue      # already in conversation on another port
                        result['port2'] = fport
                        dst = (ip, fport)
                    timems_r, nclients = struct.unpack_from('<IB', payload)
                    if ping_pass == 0:
                        ping_pass = 1
                        result['nclients'] = nclients
                        want_clients = nclients > 0
                        send_ping()
                    elif ping_pass == 1:
                        ping_pass = 2
                        now_ms = int(time.time() * 1000) % 86400000
                        result['ping'] = now_ms - timems_r
                        result['nclients'] = nclients
                        want_clients = nclients > 0
                        sock.sendto(build_packet(CLM_REQ_VERSION_AND_OS), dst)
                        sock.sendto(build_packet(CLM_REQ_SERVER_FEATURES), dst)
                        sock.sendto(build_packet(CLM_REQ_WELCOME_MESSAGE), dst)
                        if want_clients:
                            sock.sendto(build_packet(CLM_REQ_CONN_CLIENTS_LIST), dst)

                elif mid == CLM_VERSION_AND_OS:
                    if len(payload) < 3:
                        continue
                    os_id, vlen = struct.unpack_from('<BH', payload)
                    vs = payload[3:3 + vlen].decode('utf-8', errors='replace')
                    result['os'] = OPSYS.get(os_id, 'Unknown')
                    result['version'], result['versionsort'] = parse_version(vs)
                    got_version = True

                elif mid == CLM_CONN_CLIENTS_LIST:
                    result['clients'] = _parse_client_list(payload)
                    got_clients = True

                elif mid == CLM_SERVER_FEATURES and len(payload) >= 4:
                    result['features'] = parse_features(payload)
                    got_features = True

                elif mid == CLM_WELCOME_MESSAGE and len(payload) >= 2:
                    result['welcome'] = parse_u16_string(payload)
                    got_welcome = True

                if ping_pass == 2 and got_version and (not want_clients or got_clients):
                    # Wait one timeout for BOTH optional replies. A server with no welcome text
                    # still answers 1022 with an empty 1021, and servers.php then emits
                    # welcome: "" — measured 2026-10-23 on 4 of 6 "missing" welcomes.
                    if not wait_optional or (got_features and got_welcome):
                        break
                    if grace is None:
                        grace = min(deadline, time.time() + TIMEOUT_SEC)
                if grace is not None and time.time() >= grace:
                    break

            except socket.timeout:
                if grace is not None:
                    break             # required replies are in; the optional ones did not come
                timeouts += 1
                if timeouts >= MAX_ATTEMPTS:
                    break
                if ping_pass == 0:
                    send_ping()
                elif ping_pass == 2 and want_clients and not got_clients:
                    sock.sendto(build_packet(CLM_REQ_CONN_CLIENTS_LIST), dst)

        return result  # ping=-1 if unreachable; caller handles
    finally:
        sock.close()
        _release_port(lport)

# ── Global state ──────────────────────────────────────────────────────────────

_STATE_LOCK = threading.Lock()

# keyed by "ip:port"
# fields: ip, port, numip, name, countryid, country, city, maxclients, perm,
#         ipaddrs, ping, nclients, clients, first_seen, last_absent, last_changed,
#         os, version, versionsort, _min_probe, _last_probe_start
SERVER_STATE: dict = {}

# keyed by "host:port"
# fields: host, port, servers (list of "ip:port"), last_sweep
DIRECTORY_STATE: dict = {}

_heap:      list  = []
_heap_cnt:  int   = 0
_heap_lock        = threading.Lock()

# Probe throughput counters (server probes only, not directory sweeps)
_probes_total    = 0
_watchdog_last   = 0.0  # last [watchdog] warn (see WATCHDOG_WARN_EVERY)
_reaped_total    = 0   # servers retired by the membership reaper (see REAP_AFTER)
_probes_t0       = time.time()   # set at startup
_tasks_submitted = 0   # total srv tasks dispatched to executor (including early-exit ones)

# Ping-density signal pushed from gather-server-data.py every ~60s
_active_pool:    int   = 0    # unique ALLOWED client IPs seen in last 180s
_active_pool_ts: float = 0.0  # Unix timestamp of last push
_today_peak:     list = [0] * 24  # max active_pool seen per UTC hour today (index = hour)
_yesterday_peak: list = [0] * 24  # _today_peak from the previous UTC day

# ── Activity tier (global probe-rate calibration) ─────────────────────────────────────

def _server_tier(ss):
    """Return (probe_active, probe_stable, probe_idle) for one server.
    Ranks the current hour against the server's own 24-int hourly event
    history. Quiet hours get longer probe intervals; busy hours shorter.
    Defaults to normal tier until the server has seen at least one event.
    """
    bins = ss['hourly_events']
    if sum(bins) == 0:
        return _TIER_RATES["normal"]
    h = int(time.time()) % 86400 // 3600
    current = bins[h]
    srt = sorted(bins)
    p33 = srt[8]    # 33rd percentile of 24 hourly values
    p67 = srt[16]   # 67th percentile of 24 hourly values
    if current <= p33:
        return _TIER_RATES["quiet"]
    elif current >= p67:
        return _TIER_RATES["busy"]
    else:
        return _TIER_RATES["normal"]

def _pool_threshold_locked(pct: float) -> int:
    """Value at given percentile of _yesterday_peak. Returns 0 if no data. Caller must hold _STATE_LOCK."""
    vals = sorted(_yesterday_peak)
    if not any(vals):
        return 0
    idx = (pct / 100.0) * (len(vals) - 1)
    lo, hi = int(idx), min(int(idx) + 1, len(vals) - 1)
    return round(vals[lo] + (vals[hi] - vals[lo]) * (idx - lo))

def _global_idle_cap_locked() -> float:
    """Max PROBE_IDLE for idle servers based on active_pool vs yesterday's percentile profile. Caller must hold _STATE_LOCK."""
    busy_thresh   = _pool_threshold_locked(GLOBAL_BUSY_PERCENTILE)
    normal_thresh = _pool_threshold_locked(GLOBAL_NORMAL_PERCENTILE)
    if not any(_yesterday_peak):
        return _TIER_RATES['quiet'][2]   # no calibration data yet → stay quiet
    n = _active_pool
    if busy_thresh > 0 and n >= busy_thresh:
        return _TIER_RATES['busy'][2]    # 10s
    if normal_thresh > 0 and n >= normal_thresh:
        return _TIER_RATES['normal'][2]  # 15s
    return _TIER_RATES['quiet'][2]       # 20s

def _server_idle_cap_locked(state) -> float:
    """Per-server idle cap: historically-quiet servers are not tightened by global activity."""
    ps_idle = _server_tier(state)[2]
    if ps_idle >= 20.0:
        return ps_idle
    return min(ps_idle, _global_idle_cap_locked())

def _schedule(when: float, key: str):
    global _heap_cnt
    with _heap_lock:
        _heap_cnt += 1
        heapq.heappush(_heap, (when, _heap_cnt, key))

def _heap_next_time() -> float:
    with _heap_lock:
        return _heap[0][0] if _heap else time.time() + 1.0

def _heap_pop_due() -> list:
    now = time.time(); keys = []
    with _heap_lock:
        while _heap and _heap[0][0] <= now:
            keys.append(heapq.heappop(_heap)[2])
    return keys

# ── Scheduler tasks (run in thread pool) ─────────────────────────────────────

def _do_dir_task(dir_key: str):
    """Sweep a directory, update SERVER_STATE/DIRECTORY_STATE, reschedule."""
    # dir_key = "dir:host:port"
    _, host_port = dir_key.split(':', 1)
    host, _, port_str = host_port.rpartition(':')
    port = int(port_str)

    # A directory adopted on request is dropped once nobody has asked for it for REAP_AFTER;
    # its servers then fall to the membership reaper like any unlisted server.
    with _STATE_LOCK:
        ds0 = DIRECTORY_STATE.get(host_port)
        if ds0 and ds0.get('adopted') and time.time() - ds0.get('last_request_time', 0.0) > REAP_AFTER:
            del DIRECTORY_STATE[host_port]
            print(f'[dir] {host_port} adopted on request, unasked for {REAP_AFTER:.0f}s — dropped',
                  file=sys.stderr, flush=True)
            return

    servers, probe_results, err = sweep_directory(host, port)
    if err:
        print(f"[dir] sweep failed {host}:{port}: {err}", file=sys.stderr, flush=True)
        _schedule(time.time() + DIRECTORY_SWEEP, dir_key)
        return

    now = time.time()
    new_keys = _apply_sweep(host, port, host_port, servers, probe_results, now)
    _schedule(now + DIRECTORY_SWEEP, dir_key)
    print(f"[dir] {host}:{port} → {len(servers)} servers, {len(new_keys)} new, "
          f"{len(probe_results)} pinged back", file=sys.stderr, flush=True)


def _apply_sweep(host, port, host_port, servers, probe_results, now, adopted=False):
    """Fold one directory sweep into DIRECTORY_STATE/SERVER_STATE and schedule the server
    probes it calls for. Returns the keys of servers seen for the first time."""
    new_keys = []
    with _STATE_LOCK:
        prev = DIRECTORY_STATE.get(host_port, {})
        DIRECTORY_STATE[host_port] = {
            'host': host, 'port': port,
            'servers': [f"{s['ip']}:{s['port']}" for s in servers],
            'last_sweep': now,
            'sweep_count': prev.get('sweep_count', 0) + 1,
            'last_request_time': prev.get('last_request_time', now if adopted else 0.0),
            'adopted': prev.get('adopted', adopted),
        }
        for s in servers:
            key = f"{s['ip']}:{s['port']}"
            if key not in SERVER_STATE:
                SERVER_STATE[key] = {
                    'ip': s['ip'], 'port': s['port'], 'numip': s['numip'],
                    'name': s['name'], 'countryid': s['countryid'],
                    'country': s['country'], 'city': s['city'],
                    'maxclients': s['maxclients'], 'perm': s['perm'],
                    'ipaddrs': s['ipaddrs'],
                    'ping': -1, 'nclients': 0, 'clients': [],
                    'first_seen': {}, 'last_absent': {}, 'last_changed': 0.0,
                    'hourly_events': [0] * 24,
                    '_cooldown': 0,
                    'os': '', 'version': '', 'versionsort': '',
                    'features': None, 'welcome': None, 'port2': None,
                    '_features_misses': 0,
                    '_min_probe': 0.0, '_last_probe_start': 0.0,
                    '_last_sweep_success': 0.0, '_last_listed': now,
                    '_probe_attempts': 0, '_probe_successes': 0,
                    '_probe_inflight': False,
                }
                new_keys.append(key)
            else:
                ss = SERVER_STATE[key]
                ss['_last_listed'] = now
                ss['name']       = s['name']
                ss['countryid']  = s['countryid']
                ss['country']    = s['country']
                ss['city']       = s['city']
                ss['maxclients'] = s['maxclients']
                ss['perm']       = s['perm']

        # Apply probe_results collected on the directory socket (hole-punch pings).
        # Servers that responded get their ping/nclients/clients updated directly here;
        # their scheduled probe tasks will handle ongoing refreshes at adaptive rates.
        for key, pr in probe_results.items():
            if key not in SERVER_STATE:
                continue
            ss = SERVER_STATE[key]
            ss['ping']     = pr['ping']
            ss['nclients'] = pr['nclients']
            for f in ('os', 'version', 'versionsort'):
                if pr.get(f):
                    ss[f] = pr[f]
            for f in ('features', 'welcome', 'port2'):      # an empty welcome IS a reply
                if pr.get(f) is not None:
                    ss[f] = pr[f]
            if pr.get('features') is not None:
                ss['_features_misses'] = 0
            ss['_probe_attempts']  += 1
            ss['_probe_successes'] += 1
            prev_sweep_time = ss['_last_probe_start']
            ss['_last_probe_start'] = now
            ss['_last_sweep_success'] = now
            new_clients = pr['clients']
            old_set = frozenset(
                (c['name'], c['countryid'], c['instrumentid'], c['city'])
                for c in ss['clients']
            )
            new_set = frozenset(
                (c['name'], c['countryid'], c['instrumentid'], c['city'])
                for c in new_clients
            )
            if new_set != old_set:
                ss['last_changed'] = now
                _h = int(now) % 86400 // 3600
                ss['hourly_events'][_h] += len(old_set - new_set) + len(new_set - old_set)
                for fkey in old_set - new_set:
                    ss['first_seen'].pop(fkey, None)
                    ss['last_absent'].pop(fkey, None)
            for c in new_clients:
                fkey = (c['name'], c['countryid'], c['instrumentid'], c['city'])
                if fkey not in ss['first_seen']:
                    ss['first_seen'][fkey] = now
                    ss['last_absent'][fkey] = prev_sweep_time if prev_sweep_time > 0 else now
            ss['clients'] = new_clients

    for key in new_keys:
        _schedule(now, f"srv:{key}")

    # Schedule immediate probes for servers that did NOT respond to the directory introduction
    # (they may require a direct probe, or may be genuinely unreachable)
    with _STATE_LOCK:
        for s in servers:
            key = f"{s['ip']}:{s['port']}"
            if key not in probe_results and key in SERVER_STATE:
                if now - SERVER_STATE[key]['_last_probe_start'] > DIRECTORY_SWEEP:
                    SERVER_STATE[key]['_min_probe'] = 0.0
                    _schedule(now, f"srv:{key}")

    return new_keys


def _do_srv_task(srv_key: str):
    """Probe one server, update state, reschedule at adaptive rate."""
    # srv_key = "srv:ip:port"
    _, ip_port = srv_key.split(':', 1)
    ip, _, port_str = ip_port.rpartition(':')
    port = int(port_str)

    global _reaped_total
    with _STATE_LOCK:
        if ip_port not in SERVER_STATE:
            return
        state = SERVER_STATE[ip_port]
        # Reap on membership, not reachability (see REAP_AFTER). Guard (b): only when
        # every directory has swept since the server was last listed — a down directory
        # therefore blocks all reaping rather than causing any.
        last_listed = state.get('_last_listed', 0.0)
        if last_listed > 0.0 and time.time() - last_listed > REAP_AFTER:
            min_sweep = min((ds['last_sweep'] for ds in DIRECTORY_STATE.values()),
                            default=0.0)
            if last_listed < min_sweep - DIRECTORY_SWEEP:
                del SERVER_STATE[ip_port]
                _reaped_total += 1
                print(f'[reap] {ip_port} name={state["name"]!r} '
                      f'unlisted={time.time() - last_listed:.0f}s — retired',
                      file=sys.stderr, flush=True)
                return  # token dies; any future listing recreates the server
        if time.time() < state['_min_probe']:
            state['_probe_inflight'] = False
            return  # stale heap entry — a probe already ran recently
        state['_min_probe'] = time.time() + 1.0  # prevent re-entry within 1s
        prev_probe_time = state['_last_probe_start']
        # A server that has never answered CLM_REQ_SERVER_FEATURES is not made to wait for it
        # on every probe; re-ask every 20th probe in case it was upgraded.
        wait_opt = state.get('_features_misses', 0) < 3 or state['_probe_attempts'] % 20 == 0

    global _probes_total
    _probes_total += 1
    probe_start = time.time()
    try:
        result = probe_server(ip, port, wait_optional=wait_opt)
    except Exception as exc:
        print(f'[srv] {ip}:{port} probe exception: {exc}', file=sys.stderr, flush=True)
        with _STATE_LOCK:
            if ip_port in SERVER_STATE:
                _idle = _server_idle_cap_locked(SERVER_STATE[ip_port])
                SERVER_STATE[ip_port]['_cooldown'] = 0
                SERVER_STATE[ip_port]['_min_probe'] = 0.0
                SERVER_STATE[ip_port]['_probe_inflight'] = False
            else:
                _idle = _TIER_RATES['normal'][2]
        _schedule(time.time() + _idle, srv_key)
        return
    now = time.time()

    with _STATE_LOCK:
        if ip_port not in SERVER_STATE:
            return
        state = SERVER_STATE[ip_port]
        state['_min_probe'] = 0.0
        state['_last_probe_start'] = probe_start
        state['_probe_attempts'] += 1
        if result is not None and result.get('ping', -1) >= 0:
            state['_probe_successes'] += 1

        sweep_age = now - state.get('_last_sweep_success', 0)
        sweep_fresh = sweep_age < DIRECTORY_SWEEP * 2

        if result is None:
            if not sweep_fresh:
                state['ping'] = -1
            rates = _server_tier(state)
            cap = rates[1] if state['nclients'] > 0 else _server_idle_cap_locked(state)
            state['_cooldown'] = min(state['_cooldown'] + 1, 20)
            interval = min(4.0 + state['_cooldown'], cap)
        else:
            # Only overwrite sweep-obtained data when the probe actually got a response.
            # A failed probe (ping=-1) from pool ports cannot reach NAT-strict servers;
            # don't clobber nclients/clients that the sweep correctly obtained.
            probe_ok = result['ping'] >= 0
            if probe_ok or not sweep_fresh:
                state['ping'] = result['ping']
            if probe_ok or not sweep_fresh:
                state['nclients']    = result['nclients']
                state['os']          = result['os']
                state['version']     = result['version']
                state['versionsort'] = result['versionsort']
            if probe_ok:
                if result['features'] is not None:
                    state['features'] = result['features']
                    state['_features_misses'] = 0
                elif wait_opt:
                    state['_features_misses'] = min(state.get('_features_misses', 0) + 1, 99)
                if result['welcome'] is not None:
                    state['welcome'] = result['welcome']
                if result['port2'] is not None:
                    state['port2'] = result['port2']

            old_set = frozenset(
                (c['name'], c['countryid'], c['instrumentid'], c['city'])
                for c in state['clients']
            )
            new_clients = result['clients'] if (probe_ok or not sweep_fresh) else state['clients']
            new_set = frozenset(
                (c['name'], c['countryid'], c['instrumentid'], c['city'])
                for c in new_clients
            )

            if new_set != old_set:
                state['last_changed'] = now
                _h = int(now) % 86400 // 3600
                state['hourly_events'][_h] += len(old_set - new_set) + len(new_set - old_set)
                state['_cooldown'] = 0
                interval = _server_tier(state)[0]
                for fkey in old_set - new_set:
                    state['first_seen'].pop(fkey, None)
                    state['last_absent'].pop(fkey, None)
            elif result['nclients'] > 0:
                state['_cooldown'] = min(state['_cooldown'] + 1, 20)
                interval = min(4.0 + state['_cooldown'], _server_tier(state)[1])
            else:
                state['_cooldown'] = min(state['_cooldown'] + 1, 20)
                interval = min(4.0 + state['_cooldown'], _server_idle_cap_locked(state))

            for c in new_clients:
                fkey = (c['name'], c['countryid'], c['instrumentid'], c['city'])
                if fkey not in state['first_seen']:
                    state['first_seen'][fkey] = now
                    state['last_absent'][fkey] = prev_probe_time

            state['clients']    = new_clients
            state['_min_probe'] = now + interval * 0.8

        state['_probe_inflight'] = False

    _schedule(now + interval, srv_key)

# ── Scheduler thread + executor ───────────────────────────────────────────────

_executor = ThreadPoolExecutor(max_workers=CLIENT_PORT_RANGE)

def _scheduler_loop():
    global _tasks_submitted
    while True:
        for key in _heap_pop_due():
            if key.startswith('dir:'):
                _executor.submit(_do_dir_task, key)
            else:
                ip_port = key[4:]
                with _STATE_LOCK:
                    state = SERVER_STATE.get(ip_port)
                    if state is None or state['_probe_inflight']:
                        continue
                    state['_probe_inflight'] = True
                _tasks_submitted += 1
                _executor.submit(_do_srv_task, key)
        with _STATE_LOCK:
            _n_inf = sum(1 for s in SERVER_STATE.values() if s.get('_probe_inflight'))
            _n_tot = len(SERVER_STATE)
        if _n_tot > 100 and _n_inf > _n_tot * WATCHDOG_QUEUE_RATIO:
            # Throttled: this loop runs ~1x/s, and an unthrottled warn would add ~86k
            # lines/day to a host whose disk is the tight resource (75% full, journald
            # already at its default cap).
            global _watchdog_last
            if time.time() - _watchdog_last > WATCHDOG_WARN_EVERY:
                _watchdog_last = time.time()
                print(f'[watchdog] queue depth {_n_inf}/{_n_tot} '
                      f'({100.0*_n_inf/_n_tot:.0f}%) — probe backlog starving sweeps',
                      file=sys.stderr, flush=True)
        sleep = max(0.05, min(_heap_next_time() - time.time(), 1.0))
        time.sleep(sleep)

# ── HTTP handler ──────────────────────────────────────────────────────────────

def _client_rows(ss: dict) -> list:
    """servers.php client objects, plus first_seen / last_absent (Unix floats) when known."""
    rows = []
    for c in ss['clients']:
        fkey = (c['name'], c['countryid'], c['instrumentid'], c['city'])
        entry = dict(c)
        fs = ss['first_seen'].get(fkey, 0.0)
        if fs > 0.0:
            entry['first_seen'] = fs
        la = ss['last_absent'].get(fkey, 0.0)
        if la > 0.0:
            entry['last_absent'] = la
        rows.append(entry)
    return rows


def _server_row(ss: dict, index: int, single: bool = False, name: str = None) -> dict:
    """One row in servers.php's shape and key order.
    Directory mode: the 14 keys set from the list entry and the ping reply, then the keys each
    UDP reply adds (port2, nclients, features, welcome, clients), index last.
    ?server= / ?query= mode: servers.php initialises index, name, numip, ip, port, ping, os,
    version, versionsort first. nclients/clients appear only when the server answered."""
    if single:
        row = {'index': index, 'name': ss['name'] if name is None else name,
               'numip': numip_str(ss['ip']), 'ip': ss['ip'], 'port': ss['port'],
               'ping': ss['ping'], 'os': ss['os'], 'version': ss['version'],
               'versionsort': ss['versionsort']}
    else:
        row = {'numip': numip_str(ss['ip']), 'port': ss['port'], 'countryid': ss['countryid'],
               'maxclients': ss['maxclients'], 'perm': ss['perm'], 'country': ss['country'],
               'name': ss['name'], 'ipaddrs': ss['ipaddrs'], 'city': ss['city'], 'ip': ss['ip'],
               'ping': ss['ping'], 'os': ss['os'], 'version': ss['version'],
               'versionsort': ss['versionsort']}
    if ss.get('port2') is not None:
        row['port2'] = ss['port2']
    if ss['ping'] >= 0:
        row['nclients'] = ss['nclients']
    if ss.get('features') is not None:
        row['features'] = ss['features']
    if ss.get('welcome') is not None:
        row['welcome'] = ss['welcome']
    if ss['ping'] >= 0 and ss['clients']:
        row['clients'] = _client_rows(ss)
    if not single:
        row['index'] = index
    return row


def _build_dir_rows(host_port: str, ds: dict, central: str = None) -> list:
    """Build the server-list JSON rows for one directory from current STATE (caller holds lock)."""
    result = []
    for ip_port in ds['servers']:
        ss = SERVER_STATE.get(ip_port)
        if not ss:
            continue
        row = _server_row(ss, len(result))
        if central is not None:
            row['central'] = central
        result.append(row)
    return result


def _rows_from_sweep(servers: list, probe_results: dict) -> list:
    """Directory rows straight from one sweep, for a directory we are not keeping."""
    rows = []
    for s in servers:
        pr = probe_results.get(f"{s['ip']}:{s['port']}", {})
        tmp = {**s, 'ping': pr.get('ping', -1), 'nclients': pr.get('nclients', 0),
               'clients': pr.get('clients', []), 'os': pr.get('os', ''),
               'version': pr.get('version', ''), 'versionsort': pr.get('versionsort', ''),
               'features': pr.get('features'), 'welcome': pr.get('welcome'),
               'port2': pr.get('port2'), 'first_seen': {}, 'last_absent': {}}
        rows.append(_server_row(tmp, len(rows)))
    return rows


# ── servers.php request modes served on demand ────────────────────────────────

_adopt_lock = threading.Lock()

def _directory_rows_on_demand(host: str, port: int):
    """?directory= for a directory we do not sample: sweep it now, as servers.php does on every
    request, and if there is room adopt it into the loop until nobody has asked for REAP_AFTER.
    None → caller answers ERR_NOT_FOUND (host does not resolve)."""
    host_port = f"{host}:{port}"
    try:
        socket.gethostbyname(host)
    except socket.gaierror:
        return None
    with _adopt_lock:                       # one on-demand sweep at a time
        with _STATE_LOCK:
            ds = DIRECTORY_STATE.get(host_port)
        if ds is None:
            servers, probe_results, err = sweep_directory(host, port)
            if err:
                print(f'[dir] on-demand sweep failed {host_port}: {err}', file=sys.stderr, flush=True)
                return []
            now = time.time()
            with _STATE_LOCK:
                room = sum(1 for d in DIRECTORY_STATE.values() if d.get('adopted')) < ADOPT_MAX
            if not room:
                return _rows_from_sweep(servers, probe_results)
            _apply_sweep(host, port, host_port, servers, probe_results, now, adopted=True)
            _schedule(now + DIRECTORY_SWEEP, f"dir:{host_port}")
            print(f'[dir] {host_port} adopted on request → {len(servers)} servers',
                  file=sys.stderr, flush=True)
    with _STATE_LOCK:
        ds = DIRECTORY_STATE.get(host_port)
        if ds is None:
            return []
        ds['last_request_time'] = time.time()
        return _build_dir_rows(host_port, ds)


def _single_server_rows(host: str, port: int, name: str):
    """?server= : a one-element list, or [] when the server never answered (servers.php deletes
    it). Served from state when the server is listed and was probed in the last minute.
    None → host does not resolve."""
    try:
        ip = socket.gethostbyname(host)
    except socket.gaierror:
        return None
    label = host if name is None else name
    key = f"{ip}:{port}"
    with _STATE_LOCK:
        ss = SERVER_STATE.get(key)
        if ss and ss['ping'] >= 0 and time.time() - ss['_last_probe_start'] < 60.0:
            return [_server_row(ss, 0, single=True, name=label)]
        times = (dict(ss['first_seen']), dict(ss['last_absent'])) if ss else ({}, {})
    r = probe_server(ip, port)
    if r['ping'] < 0:
        return []
    tmp = {'ip': ip, 'port': port, 'name': label, 'ping': r['ping'], 'os': r['os'],
           'version': r['version'], 'versionsort': r['versionsort'], 'nclients': r['nclients'],
           'clients': r['clients'], 'features': r['features'], 'welcome': r['welcome'],
           'port2': r['port2'], 'first_seen': times[0], 'last_absent': times[1]}
    return [_server_row(tmp, 0, single=True, name=label)]


def _query_welcome(ip: str, port: int) -> dict:
    """?query= : servers.php's short audio connection. Send one pseudo audio frame, acknowledge
    every connected-mode message, collect CLIENT_ID / RAWAUDIO_SUPPORTED / CHAT_TEXT (the welcome
    text), then disconnect twice. Returns only what the server sent."""
    out = {}
    lport = _acquire_port()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(TIMEOUT_SEC)
    try:
        sock.bind(('0.0.0.0', lport))
        sock.sendto(AUDIO_PROBE, (ip, port))
        attempts, got_any = 1, False
        deadline = time.time() + TIMEOUT_SEC * MAX_ATTEMPTS * 2
        while time.time() < deadline:
            try:
                data, (fip, fport) = sock.recvfrom(32767)
            except socket.timeout:
                if got_any or attempts >= MAX_ATTEMPTS:
                    break
                attempts += 1
                sock.sendto(AUDIO_PROBE, (ip, port))
                continue
            if fip != ip:
                continue
            parsed = parse_header(data)
            if not parsed:
                continue                  # audio frames back from the server, or garbage
            got_any = True
            mid, cnt, payload = parsed
            if ACKN < mid < CLM_START:
                sock.sendto(build_ackn(cnt, mid), (fip, fport))
            if mid == CLIENT_ID and payload:
                out['client_id'] = payload[0]
            elif mid == RAWAUDIO_SUPPORTED:
                out['rawaudio'] = True
            elif mid == CHAT_TEXT and len(payload) >= 2:
                out['welcome'] = parse_u16_string(payload)
                break
        for _ in range(2):
            sock.sendto(build_packet(CLM_DISCONNECTION), (ip, port))
        return out
    finally:
        sock.close()
        _release_port(lport)


def _query_rows(host: str, port: int, name: str):
    """?query= row: servers.php's single-server initialisation plus whatever the audio
    connection yielded. None → host does not resolve."""
    try:
        ip = socket.gethostbyname(host)
    except socket.gaierror:
        return None
    row = {'index': 0, 'name': host if name is None else name, 'numip': numip_str(ip), 'ip': ip,
           'port': port, 'ping': -1, 'os': '', 'version': '', 'versionsort': ''}
    row.update(_query_welcome(ip, port))
    return [row]


class Handler(BaseHTTPRequestHandler):
    timeout = 10        # per-connection socket timeout; a half-open client cannot hold a thread

    def do_GET(self):
        try:
            self._get()
        except Exception as exc:            # never let a request take the worker down
            print(f'[http] {self.address_string()} {self.path}: {exc!r}', file=sys.stderr, flush=True)
            try:
                self._send(500, {'error': 'internal error'})
            except Exception:
                pass

    def _admin(self) -> bool:
        """Operator-only routes: ADMIN_IPS, else 403."""
        if self.client_address[0] in ADMIN_IPS:
            return True
        self._send(403, {'error': 'forbidden'})
        return False

    def _get(self):
        p = urlparse(self.path)

        if p.path == '/servers/all':
            with _STATE_LOCK:
                result = []
                for host_port, ds in DIRECTORY_STATE.items():
                    result.extend(_build_dir_rows(host_port, ds, central=host_port))
            self._send(200, result)
            return

        if p.path == '/stats':
            if not self._admin():
                return
            now_s   = time.time()
            elapsed = now_s - _probes_t0
            with _heap_lock:
                heap_depth = len(_heap)
            with _STATE_LOCK:
                n_total     = len(SERVER_STATE)
                n_reachable = sum(1 for s in SERVER_STATE.values() if s['ping'] >= 0)
                n_clients     = sum(1 for s in SERVER_STATE.values() if s['nclients'] > 0)
                total_clients = sum(s['nclients'] for s in SERVER_STATE.values())
                n_active    = sum(1 for s in SERVER_STATE.values()
                                  if s['ping'] >= 0 and s['last_changed'] > now_s - 8.0)
                n_inflight  = sum(1 for s in SERVER_STATE.values() if s.get('_probe_inflight'))
                dirs_info = [
                    {
                        'host_port':        hp,
                        'last_sweep_ago_s': round(now_s - ds['last_sweep'], 1),
                        'server_count':     len(ds['servers']),
                        'sweep_count':      ds.get('sweep_count', 0),
                    }
                    for hp, ds in DIRECTORY_STATE.items()
                ]
                unreachable_servers = [
                    {
                        'ip_port':         ip_port,
                        'name':            s['name'],
                        'probe_attempts':  s['_probe_attempts'],
                        'probe_successes': s['_probe_successes'],
                        'last_probe_ago_s': (
                            round(now_s - s['_last_probe_start'], 1)
                            if s['_last_probe_start'] else None
                        ),
                    }
                    for ip_port, s in SERVER_STATE.items() if s['ping'] < 0
                ]
            self._send(200, {
                'uptime_s':   round(elapsed, 1),
                'port_pool': {
                    'total':    CLIENT_PORT_RANGE,
                    'in_use':   _ports_in_use,
                    'peak':     _ports_peak,
                    'headroom': CLIENT_PORT_RANGE - _ports_peak,
                },
                'heap_depth': heap_depth,
                'servers': {
                    'total':        n_total,
                    'reachable':    n_reachable,
                    'unreachable':  n_total - n_reachable,
                    'reaped_total': _reaped_total,
                    'with_clients':    n_clients,
                    'total_clients':   total_clients,
                    'active_tier':     n_active,
                    'active_pool':     _active_pool,
                    'active_pool_age_s': round(time.time() - _active_pool_ts, 1) if _active_pool_ts else None,
                    'today_peak':      list(_today_peak),
                    'yesterday_peak':  list(_yesterday_peak),
                    'pool_normal_thresh': _pool_threshold_locked(GLOBAL_NORMAL_PERCENTILE),
                    'pool_busy_thresh':   _pool_threshold_locked(GLOBAL_BUSY_PERCENTILE),
                },
                'probes': {
                    'submitted':  _tasks_submitted,
                    'completed':  _probes_total,
                    'inflight':   n_inflight,
                    'per_second': round(_probes_total / elapsed, 3) if elapsed > 0 else 0,
                    'per_minute': round(_probes_total / elapsed * 60, 1) if elapsed > 0 else 0,
                },
                'directories':          dirs_info,
                'unreachable_servers':  unreachable_servers,
                'activity': {
                    t: sum(
                        1 for sv in SERVER_STATE.values()
                        if _server_tier(sv) == _TIER_RATES[t]
                    )
                    for t in ('quiet', 'normal', 'busy')
                },
            })
            return

        if p.path == '/debug/stuck':
            if not self._admin():
                return
            with _STATE_LOCK:
                stuck = [
                    {'ip_port': k, 'name': s['name'],
                     'nclients': s['nclients'], 'ping': s['ping']}
                    for k, s in SERVER_STATE.items()
                    if s.get('_probe_inflight')
                ]
            self._send(200, {'stuck_count': len(stuck), 'servers': stuck})
            return

        if p.path not in ('/servers', '/servers.php'):
            self._send(404, {'error': 'not found'}); return

        # servers.php parameter handling, in its order: ?central= is the old name for
        # ?directory= and wins over it; then ?server=, then ?query=. &name= labels a single
        # server; &pretty pretty-prints. Error bodies are servers.php's, always HTTP 200.
        qs     = parse_qs(p.query, keep_blank_values=True)
        pretty = 'pretty' in qs
        name   = qs.get('name', [None])[0]
        if 'central' in qs:
            mode, raw = 'directory', qs['central'][0]
        elif 'directory' in qs:
            mode, raw = 'directory', qs['directory'][0]
        elif 'server' in qs:
            mode, raw = 'server', qs['server'][0]
        elif 'query' in qs:
            mode, raw = 'query', qs['query'][0]
        else:
            self._send(200, ERR_NO_TARGET, pretty); return
        hp = parse_host(raw)
        if hp is None:
            self._send(200, ERR_INVALID, pretty); return
        host, port = hp
        if ':' in host:
            # IPv6 literal: this sampler speaks IPv4; servers.php would try it and hear nothing
            self._send(200, [], pretty); return

        if mode == 'server':
            rows = _single_server_rows(host, port, name)
        elif mode == 'query':
            rows = _query_rows(host, port, name)
        else:
            rows = self._directory_rows(host, port)
        if rows is None:
            self._send(200, ERR_NOT_FOUND, pretty); return
        self._send(200, rows, pretty)

    def _directory_rows(self, host: str, port: int):
        """Known directory: answer from state and poke idle servers that are due. Unknown: sweep
        it on demand. A known directory ALWAYS yields a list — jamfan26 passes it straight
        through to a consumer that iterates it."""
        host_port = f"{host}:{port}"
        now_req = time.time()
        with _STATE_LOCK:
            ds = DIRECTORY_STATE.get(host_port)
            if ds is None:
                known = False
            else:
                known = True
                ds['last_request_time'] = now_req
                result = _build_dir_rows(host_port, ds)
                _idle_cap = _global_idle_cap_locked()
                servers_to_poke = [
                    ip_port for ip_port in ds.get('servers', [])
                    if ip_port in SERVER_STATE
                    and SERVER_STATE[ip_port]['nclients'] == 0
                    and now_req - SERVER_STATE[ip_port]['_last_probe_start'] > _idle_cap
                    and SERVER_STATE[ip_port]['_min_probe'] <= now_req
                ]
        if not known:
            return _directory_rows_on_demand(host, port)

        for ip_port in servers_to_poke:
            with _STATE_LOCK:
                if ip_port in SERVER_STATE:
                    SERVER_STATE[ip_port]['_min_probe'] = 0.0
            _schedule(now_req, f'srv:{ip_port}')
        return result

    def do_POST(self):
        try:
            p = urlparse(self.path)
            if p.path != '/ping-density':
                self._send(404, {'error': 'not found'}); return
            if not self._admin():
                return
            global _active_pool, _active_pool_ts, _today_peak
            length = int(self.headers.get('Content-Length', 0))
            if length > 4096:
                self._send(413, {'error': 'too large'}); return
            body = self.rfile.read(length)
            try:
                d = json.loads(body)
                _active_pool    = int(d['active_pool'])
                _active_pool_ts = float(d.get('ts', time.time()))
                h = time.gmtime(_active_pool_ts).tm_hour
                if _active_pool > _today_peak[h]:
                    _today_peak[h] = _active_pool
                self._send(200, {'ok': True, 'active_pool': _active_pool})
            except Exception as exc:
                self._send(400, {'error': str(exc)})
        except Exception as exc:
            print(f'[http] {self.address_string()} POST {self.path}: {exc!r}', file=sys.stderr, flush=True)
            try:
                self._send(500, {'error': 'internal error'})
            except Exception:
                pass

    def _send(self, code, data, pretty: bool = False):
        # servers.php: JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES, trailing newline;
        # &pretty adds JSON_PRETTY_PRINT (4-space indent, ": " and ",\n" separators).
        if pretty:
            text = json.dumps(data, ensure_ascii=False, indent=4)
        else:
            text = json.dumps(data, ensure_ascii=False, separators=(',', ':'))
        body = (text + '\n').encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        print(f"{self.address_string()} - {fmt % args}", file=sys.stderr, flush=True)

def _midnight_rollover_loop():
    """At each UTC midnight, roll _today_peak into _yesterday_peak and reset."""
    global _yesterday_peak, _today_peak
    while True:
        now_gm = time.gmtime()
        secs_until_midnight = (23 - now_gm.tm_hour) * 3600 + (59 - now_gm.tm_min) * 60 + (60 - now_gm.tm_sec)
        time.sleep(secs_until_midnight + 1)
        with _STATE_LOCK:
            _yesterday_peak = list(_today_peak)
            _today_peak = [0] * 24
        print(f"[MIDNIGHT_ROLLOVER] yesterday_peak={_yesterday_peak}", flush=True)

# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == '__main__':
    # Pre-seed all configured directories so first /servers/all call has data promptly.
    now = time.time()
    for d in DIRECTORIES:
        _schedule(now, f"dir:{d}")
    threading.Thread(target=_scheduler_loop, daemon=True, name='scheduler').start()
    threading.Thread(target=_midnight_rollover_loop, daemon=True, name='midnight-rollover').start()
    # Threaded: one slow or half-open client no longer stalls every other caller (measured on
    # the single-threaded server 2026-10-23: a second client got nothing until the first closed).
    srv = ThreadingHTTPServer(('0.0.0.0', HTTP_PORT), Handler)
    srv.daemon_threads = True
    print(f'jamulus-sampler daemon on :{HTTP_PORT} (udp {SWEEP_PORT}, pool '
          f'{CLIENT_PORT_START}-{CLIENT_PORT_START + CLIENT_PORT_RANGE - 1}, admin {sorted(ADMIN_IPS)})',
          flush=True)
    srv.serve_forever()
