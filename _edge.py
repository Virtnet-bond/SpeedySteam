import json
import re
import socket
import urllib.request
from collections import defaultdict

HOST = re.compile(r"^cache\d+-(.+)\.steamcontent\.com$")
API = "https://api.steampowered.com/IContentServerDirectoryService/GetServersForSteamPipe/v1/"

seeds = [
    "cache1-ord1.steamcontent.com",
    "cache1-lhr1.steamcontent.com",
    "cache1-tyo3.steamcontent.com",
    "cache1-syd1.steamcontent.com",
    "cache1-gru1.steamcontent.com",
    "cache1-jnb1.steamcontent.com",
    "cache1-sgp1.steamcontent.com",
    "cache1-hkg1.steamcontent.com",
    "cache1-sea1.steamcontent.com",
    "cache1-lax2.steamcontent.com",
    "cache1-fra1.steamcontent.com",
    "cache1-eze1.steamcontent.com",
    "cache1-waw1.steamcontent.com",
    "cache1-sto1.steamcontent.com",
]


def one(ip, cell=0):
    url = (f"{API}?cell_id={cell}&max_servers=100&launcher_type=0&ip_override={ip}")
    with urllib.request.urlopen(url, timeout=15) as r:
        servers = json.load(r)["response"]["servers"]
    pops = []
    other = []
    for s in servers:
        host = s.get("host", "")
        m = HOST.match(host)
        if s.get("type") == "SteamCache" and m:
            if m.group(1) not in pops:
                pops.append(m.group(1))
        elif host not in other:
            other.append(f"{s.get('type')}:{host}")
    return pops, other


for host in seeds:
    try:
        ip = socket.getaddrinfo(host, 443, socket.AF_INET)[0][4][0]
    except Exception as e:
        print(host, "dns fail", e)
        continue
    try:
        pops, other = one(ip)
    except Exception as e:
        print(host, ip, "api fail", type(e).__name__, e)
        continue
    print(host, ip)
    print("  pops", pops)
    print("  other", other[:8])
