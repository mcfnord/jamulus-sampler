#!/usr/bin/env python3
"""
jamulus-sampler daemon — persistent background prober replacing servers.php.
Background thread probes servers at adaptive rates; HTTP handler reads in-memory
state instantly without blocking on UDP.
Accepts GET /servers?central=<host>:<port> (also ?directory=).
Response: same JSON as servers.php, plus first_seen (Unix float) per client.
"""

import heapq
import json
import re
import socket
import struct
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

# ── Protocol constants ────────────────────────────────────────────────────────

CLIENT_PORT_START = 22135
CLIENT_PORT_RANGE = 15
TIMEOUT_SEC       = 0.5
MAX_ATTEMPTS      = 3

CLM_PING_MS_WITHNUMCLIENTS = 1002
CLM_SERVER_LIST             = 1006
CLM_REQ_SERVER_LIST         = 1007
CLM_VERSION_AND_OS          = 1011
CLM_REQ_VERSION_AND_OS      = 1012
CLM_CONN_CLIENTS_LIST       = 1013
CLM_REQ_CONN_CLIENTS_LIST   = 1014

PROBE_ACTIVE    =   3.0   # s — after client-set change
PROBE_STABLE    =  15.0   # s — clients present, stable
PROBE_IDLE      =  60.0   # s — no clients
DIRECTORY_SWEEP =  90.0   # s — re-query directory; NAT TTL measured ≥120s

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

def build_packet(msg_id: int, payload: bytes = b'') -> bytes:
    header = struct.pack('<HHBH', 0, msg_id, 0, len(payload))
    body = header + payload
    return body + struct.pack('<H', compute_crc(body))

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

# ── Port pool — each probe holds one port from 22134–22149 ────────────────────

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
    """Query directory for its server list only (no per-server pinging).
    Returns (list_of_server_dicts, None) or (None, error_str)."""
    try:
        ip = socket.gethostbyname(host)
    except socket.gaierror as e:
        return None, str(e)

    lport = _acquire_port()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(TIMEOUT_SEC)
    try:
        sock.bind(('0.0.0.0', lport))
        for _ in range(MAX_ATTEMPTS):
            sock.sendto(build_packet(CLM_REQ_SERVER_LIST), (ip, port))
            try:
                while True:
                    data, _ = sock.recvfrom(32767)
                    parsed = parse_header(data)
                    if parsed and parsed[0] == CLM_SERVER_LIST:
                        return _parse_server_list(parsed[2], ip, port), None
            except socket.timeout:
                pass
        return None, f"no response from {host}:{port} after {MAX_ATTEMPTS} attempts"
    finally:
        sock.close()
        _release_port(lport)


def probe_server(ip: str, port: int):
    """Probe one server: 2-pass ping RTT, version, client list.
    Returns result dict or None if unreachable."""
    lport = _acquire_port()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(TIMEOUT_SEC)
    try:
        sock.bind(('0.0.0.0', lport))

        result = {'ping': -1, 'nclients': 0, 'clients': [],
                  'version': '', 'versionsort': '', 'os': ''}
        ping_pass    = 0     # 0=need 1st, 1=need 2nd, 2=done
        want_clients = False
        got_version  = False
        got_clients  = False
        timeouts     = 0

        def send_ping():
            ms = int(time.time() * 1000) % 86400000
            sock.sendto(build_packet(CLM_PING_MS_WITHNUMCLIENTS,
                                     struct.pack('<IB', ms, 0)), (ip, port))

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

                if mid == CLM_PING_MS_WITHNUMCLIENTS and fport == port:
                    if len(payload) < 5:
                        continue
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
                        sock.sendto(build_packet(CLM_REQ_VERSION_AND_OS), (ip, port))
                        if want_clients:
                            sock.sendto(build_packet(CLM_REQ_CONN_CLIENTS_LIST),
                                        (ip, port))

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

                if ping_pass == 2 and got_version and (not want_clients or got_clients):
                    break

            except socket.timeout:
                timeouts += 1
                if timeouts >= MAX_ATTEMPTS:
                    break
                if ping_pass == 0:
                    send_ping()

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
_probes_t0       = time.time()   # set at startup
_tasks_submitted = 0   # total srv tasks dispatched to executor (including early-exit ones)

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

    servers, err = sweep_directory(host, port)
    if err:
        print(f"[dir] sweep failed {host}:{port}: {err}", file=sys.stderr, flush=True)
        _schedule(time.time() + DIRECTORY_SWEEP, dir_key)
        return

    now = time.time()
    new_keys = []
    with _STATE_LOCK:
        prev_sweep_count = DIRECTORY_STATE.get(host_port, {}).get('sweep_count', 0)
        DIRECTORY_STATE[host_port] = {
            'host': host, 'port': port,
            'servers': [f"{s['ip']}:{s['port']}" for s in servers],
            'last_sweep': now,
            'sweep_count': prev_sweep_count + 1,
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
                    'os': '', 'version': '', 'versionsort': '',
                    '_min_probe': 0.0, '_last_probe_start': 0.0,
                    '_probe_attempts': 0, '_probe_successes': 0,
                }
                new_keys.append(key)
            else:
                ss = SERVER_STATE[key]
                ss['name']       = s['name']
                ss['countryid']  = s['countryid']
                ss['country']    = s['country']
                ss['city']       = s['city']
                ss['maxclients'] = s['maxclients']
                ss['perm']       = s['perm']

    for key in new_keys:
        _schedule(now, f"srv:{key}")

    # After each sweep the NAT hole is open — immediately reprobe any ping=-1 servers
    with _STATE_LOCK:
        for s in servers:
            key = f"{s['ip']}:{s['port']}"
            if key not in new_keys and key in SERVER_STATE:
                if SERVER_STATE[key]['ping'] < 0:
                    SERVER_STATE[key]['_min_probe'] = 0.0
                    _schedule(now, f"srv:{key}")

    _schedule(now + DIRECTORY_SWEEP, dir_key)
    print(f"[dir] {host}:{port} → {len(servers)} servers, {len(new_keys)} new",
          file=sys.stderr, flush=True)


def _do_srv_task(srv_key: str):
    """Probe one server, update state, reschedule at adaptive rate."""
    # srv_key = "srv:ip:port"
    _, ip_port = srv_key.split(':', 1)
    ip, _, port_str = ip_port.rpartition(':')
    port = int(port_str)

    with _STATE_LOCK:
        if ip_port not in SERVER_STATE:
            return
        state = SERVER_STATE[ip_port]
        if time.time() < state['_min_probe']:
            return  # stale heap entry — a probe already ran recently
        state['_min_probe'] = time.time() + 1.0  # prevent re-entry within 1s
        prev_probe_time = state['_last_probe_start']

    probe_start = time.time()
    try:
        result = probe_server(ip, port)
    except Exception as exc:
        print(f'[srv] {ip}:{port} probe exception: {exc}', file=sys.stderr, flush=True)
        _schedule(time.time() + PROBE_IDLE, srv_key)
        with _STATE_LOCK:
            if ip_port in SERVER_STATE:
                SERVER_STATE[ip_port]['_min_probe'] = 0.0
        return
    now = time.time()

    global _probes_total
    _probes_total += 1

    with _STATE_LOCK:
        if ip_port not in SERVER_STATE:
            return
        state = SERVER_STATE[ip_port]
        state['_min_probe'] = 0.0
        state['_last_probe_start'] = probe_start
        state['_probe_attempts'] += 1
        if result is not None and result.get('ping', -1) >= 0:
            state['_probe_successes'] += 1

        if result is None:
            state['ping'] = -1
            # Empty server with flaky 1002: retry at PROBE_STABLE to catch new clients faster
            interval = PROBE_STABLE if state['nclients'] == 0 else PROBE_IDLE
        else:
            state['ping']        = result['ping']
            state['nclients']    = result['nclients']
            state['os']          = result['os']
            state['version']     = result['version']
            state['versionsort'] = result['versionsort']

            old_set = frozenset(
                (c['name'], c['countryid'], c['instrumentid'], c['city'])
                for c in state['clients']
            )
            new_clients = result['clients']
            new_set = frozenset(
                (c['name'], c['countryid'], c['instrumentid'], c['city'])
                for c in new_clients
            )

            if new_set != old_set:
                state['last_changed'] = now
                interval = PROBE_ACTIVE
            elif result['nclients'] > 0:
                interval = PROBE_STABLE
            else:
                interval = PROBE_IDLE

            for c in new_clients:
                fkey = (c['name'], c['countryid'], c['instrumentid'], c['city'])
                if fkey not in state['first_seen']:
                    state['first_seen'][fkey] = now
                    state['last_absent'][fkey] = prev_probe_time

            state['clients']    = new_clients
            state['_min_probe'] = now + interval * 0.8

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
                _tasks_submitted += 1
                _executor.submit(_do_srv_task, key)
        sleep = max(0.05, min(_heap_next_time() - time.time(), 1.0))
        time.sleep(sleep)

# ── HTTP handler ──────────────────────────────────────────────────────────────

def _build_dir_rows(host_port: str, ds: dict, central: str = None) -> list:
    """Build the server-list JSON rows for one directory from current STATE (caller holds lock)."""
    result = []
    for i, ip_port in enumerate(ds['servers']):
        ss = SERVER_STATE.get(ip_port)
        if not ss:
            continue
        clients = []
        for c in ss['clients']:
            fkey = (c['name'], c['countryid'], c['instrumentid'], c['city'])
            entry = {**c, 'first_seen': ss['first_seen'].get(fkey, 0.0)}
            la = ss['last_absent'].get(fkey, 0.0)
            if la > 0.0:
                entry['last_absent'] = la
            clients.append(entry)
        row = {
            'ip':          ss['ip'],
            'port':        ss['port'],
            'numip':       ss['numip'],
            'name':        ss['name'],
            'countryid':   ss['countryid'],
            'country':     ss['country'],
            'city':        ss['city'],
            'maxclients':  ss['maxclients'],
            'perm':        ss['perm'],
            'ipaddrs':     ss['ipaddrs'],
            'ping':        ss['ping'],
            'nclients':    ss['nclients'],
            'os':          ss['os'],
            'version':     ss['version'],
            'versionsort': ss['versionsort'],
            'index':       i,
        }
        if central is not None:
            row['central'] = central
        if clients:
            row['clients'] = clients
        result.append(row)
    return result


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        p = urlparse(self.path)

        if p.path == '/servers/all':
            with _STATE_LOCK:
                result = []
                for host_port, ds in DIRECTORY_STATE.items():
                    result.extend(_build_dir_rows(host_port, ds, central=host_port))
            self._send(200, result)
            return

        if p.path == '/stats':
            now_s   = time.time()
            elapsed = now_s - _probes_t0
            with _heap_lock:
                heap_depth = len(_heap)
            with _STATE_LOCK:
                n_total     = len(SERVER_STATE)
                n_reachable = sum(1 for s in SERVER_STATE.values() if s['ping'] >= 0)
                n_clients   = sum(1 for s in SERVER_STATE.values() if s['nclients'] > 0)
                n_active    = sum(1 for s in SERVER_STATE.values()
                                  if s['ping'] >= 0 and s['last_changed'] > now_s - PROBE_ACTIVE * 2)
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
                    'with_clients': n_clients,
                    'active_tier':  n_active,
                },
                'probes': {
                    'submitted':  _tasks_submitted,
                    'completed':  _probes_total,
                    'queued':     _tasks_submitted - _probes_total,
                    'per_second': round(_probes_total / elapsed, 3) if elapsed > 0 else 0,
                    'per_minute': round(_probes_total / elapsed * 60, 1) if elapsed > 0 else 0,
                },
                'directories':          dirs_info,
                'unreachable_servers':  unreachable_servers,
            })
            return

        if p.path != '/servers':
            self._send(404, {'error': 'not found'}); return

        qs  = parse_qs(p.query)
        raw = (qs.get('central') or qs.get('directory') or [None])[0]
        if not raw:
            self._send(400, {'error': 'no directory specified'}); return

        host, _, port_str = raw.rpartition(':')
        if not host:
            host, port_str = raw, '22124'
        port      = int(port_str)
        host_port = f"{host}:{port}"
        dir_key   = f"dir:{host_port}"

        with _STATE_LOCK:
            ds = DIRECTORY_STATE.get(host_port)
            if ds is None:
                _schedule(time.time(), dir_key)
                self._send(200, [])
                return
            result = _build_dir_rows(host_port, ds)

        self._send(200, result)

    def _send(self, code, data):
        body = (json.dumps(data, ensure_ascii=False, separators=(',', ':')) + '\n').encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        print(f"{self.address_string()} - {fmt % args}", file=sys.stderr, flush=True)

# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == '__main__':
    # Pre-seed all configured directories so first /servers/all call has data promptly.
    now = time.time()
    for d in DIRECTORIES:
        _schedule(now, f"dir:{d}")
    threading.Thread(target=_scheduler_loop, daemon=True, name='scheduler').start()
    srv = HTTPServer(('0.0.0.0', 5001), Handler)
    print('jamulus-sampler daemon on :5001', flush=True)
    srv.serve_forever()
