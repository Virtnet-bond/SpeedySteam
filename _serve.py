import socket
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor
sys.path.insert(0, r"c:\Users\Owner\Steam Tester")
import tester

root = tester.steam_root()
path = next((root / "depotcache").glob("1797881_*.manifest"))
sha = tester.parse_manifest(path, 900_000, 1)[0]
pops = """
adl-edgx adl-iioa akl-edgx akl-tpwr ala-kzkt ams1 atl3 bkk-jast bkk-true
bne-aubb bne-edgx bne-iioa bom2 bud-retn cfc-acol den-iwst dfw2 dxb1 eze1
fra1 fra2 got-cmhm gru1 gsv-retn hel-retn hkg1 hrk-retn iad1 iev-datl
ind-fdix jnb1 lax1 lax2 lhr1 lim1 maa2 mad1 mel-aubb mel-edgx mel-iioa
mfm-cdtm mmx-cmhm msp-mice ord1 par1 per-edgx per-iioa pit-pitx rix-retn
scl1 sea1 seo1 sgp1 sof-retn sto1 sto2 svo-retn syd1 tyo3 uln-untl vie1
vno-bite waw-extl waw1 yvr-shaw ywg-shaw yyc-shaw yyc-tsvy yyz-tsvy
""".split()

def host_of(pop):
    for n in (1, 2):
        host = f"cache{n}-{pop}.steamcontent.com"
        try:
            socket.getaddrinfo(host, 443, socket.AF_INET)
            return host
        except OSError:
            continue
    return None

def check(pop):
    host = host_of(pop)
    if not host:
        return pop, None, False
    url = f"https://{host}/depot/1797881/chunk/{sha}"
    try:
        with urllib.request.urlopen(url, timeout=8) as r:
            return pop, host, r.status == 200
    except Exception:
        return pop, host, False

with ThreadPoolExecutor(max_workers=24) as ex:
    rows = list(ex.map(check, pops))
ok = [p for p, h, good in rows if good]
bad = [p for p, h, good in rows if not good]
print("serve", len(ok))
print(" ".join(ok))
print("dead", len(bad))
print(" ".join(bad))
