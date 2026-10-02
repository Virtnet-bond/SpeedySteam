import json
import re
import socket
import urllib.request
from concurrent.futures import ThreadPoolExecutor

HOST = re.compile(r"^cache\d+-(.+)\.steamcontent\.com$")
API = "https://api.steampowered.com/IContentServerDirectoryService/GetServersForSteamPipe/v1/"

# Major pops already known from the cell sweep.
seeds = [
    "ams1", "atl3", "dfw2", "eze1", "fra1", "fra2", "gru1", "hkg1", "iad1",
    "jnb1", "lax1", "lax2", "lhr1", "lim1", "ord1", "par1", "scl1", "sea1",
    "sgp1", "sto1", "sto2", "syd1", "tyo3", "vie1", "waw1",
    "adl-edgx", "bne-edgx", "mel-edgx",
]


def resolve(pop):
    for n in (1, 2, 3, 8, 10):
        host = f"cache{n}-{pop}.steamcontent.com"
        try:
            ip = socket.getaddrinfo(host, 443, socket.AF_INET)[0][4][0]
            return host, ip
        except Exception:
            continue
    return None, None


def query(ip):
    url = f"{API}?cell_id=0&max_servers=100&launcher_type=0&ip_override={ip}"
    with urllib.request.urlopen(url, timeout=15) as r:
        servers = json.load(r)["response"]["servers"]
    found = {}
    cdn = set()
    for s in servers:
        host = s.get("host", "")
        m = HOST.match(host)
        if s.get("type") == "SteamCache" and m:
            found.setdefault(m.group(1), set()).add(host)
        elif s.get("type") == "CDN":
            cdn.add(host)
    return found, cdn


hosts = {}
for pop in seeds:
    host, ip = resolve(pop)
    print("seed", pop, host, ip)
    if ip:
        hosts[pop] = (host, ip)

all_pops = dict(hosts)
cdn = set()
seen_ip = set()
queue = list(hosts)
while queue:
    pop = queue.pop()
    ip = all_pops[pop][1]
    if ip in seen_ip:
        continue
    seen_ip.add(ip)
    try:
        found, c = query(ip)
    except Exception as e:
        print(" fail", pop, e)
        continue
    cdn |= c
    new = [p for p in found if p not in all_pops]
    if new:
        print(" from", pop, "new", new)
    for p, hs in found.items():
        if p in all_pops:
            continue
        # resolve one host
        ip2 = None
        host2 = sorted(hs)[0]
        try:
            ip2 = socket.getaddrinfo(host2, 443, socket.AF_INET)[0][4][0]
        except Exception:
            pass
        if ip2:
            all_pops[p] = (host2, ip2)
            queue.append(p)

print("TOTAL pops", len(all_pops))
print(sorted(all_pops))
print("CDN", sorted(cdn))
