import json
import re
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

HOST = re.compile(r"^cache\d+-(.+)\.steamcontent\.com$")
API = "https://api.steampowered.com/IContentServerDirectoryService/GetServersForSteamPipe/v1/"


def fetch(cell, max_servers):
    url = (f"{API}?cell_id={cell}&max_servers={max_servers}&launcher_type=0"
           "&ip_override=0.0.0.0")
    try:
        with urllib.request.urlopen(url, timeout=15) as r:
            return cell, json.load(r)["response"]["servers"]
    except Exception:
        return cell, []


def collect(cells, max_servers):
    pops = defaultdict(set)
    other = defaultdict(int)
    with ThreadPoolExecutor(max_workers=24) as ex:
        for cell, servers in ex.map(lambda c: fetch(c, max_servers), cells):
            for s in servers:
                host = s.get("host", "")
                m = HOST.match(host)
                if s.get("type") == "SteamCache" and m:
                    pops[m.group(1)].add(host)
                else:
                    other[f"{s.get('type')}:{host}"] += 1
    return pops, other


# Named-looking range plus a wider sweep for cells the UI list misses.
cells = list(range(0, 400))
pops32, other32 = collect(cells, 32)
pops200, other200 = collect(cells, 200)
print("max32 pops", len(pops32), sorted(pops32))
print("max200 pops", len(pops200), sorted(pops200))
extra = sorted(set(pops200) - set(pops32))
print("only in 200", extra)
print("noncache 200", len(other200))
for k, n in sorted(other200.items(), key=lambda kv: -kv[1])[:25]:
    print(f"  {n:5} {k}")
