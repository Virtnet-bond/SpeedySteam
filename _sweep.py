import socket
import sys
from concurrent.futures import ThreadPoolExecutor
sys.path.insert(0, r"c:\Users\Owner\Steam Tester")
import tester

CITIES = set(tester.CITIES) | {
    "adl", "akl", "ala", "bne", "bud", "cfc", "dsa", "got", "gsv", "hrk", "iev",
    "ind", "mel", "mfm", "mmx", "okc", "per", "pit", "rix", "seo", "svo", "uln",
    "vno", "ywg", "yyc", "ymo", "den", "msp", "iad", "ord",
}
PROVIDERS = (
    "edgx", "iwst", "retn", "shaw", "bell", "tsvy", "hwng", "aubb", "nxhp",
    "jast", "cdtm", "untl", "tpwr", "vgnz", "kzkt", "acol", "ask4", "datl",
    "extl", "cmhm", "bite", "fdix", "mice", "pitx", "iioa", "true",
)
cands = []
for city in sorted(CITIES):
    for n in (1, 2, 3):
        cands.append(f"{city}{n}")
    for prov in PROVIDERS:
        cands.append(f"{city}-{prov}")
print("candidates", len(cands))

def resolve(pop):
    for n in (1, 2):
        host = f"cache{n}-{pop}.steamcontent.com"
        try:
            socket.getaddrinfo(host, 443, socket.AF_INET)
            return pop, host
        except OSError:
            continue
    return None

with ThreadPoolExecutor(max_workers=40) as ex:
    found = [x for x in ex.map(resolve, cands) if x]
print("dns hits", len(found))
for pop, host in sorted(found):
    print(f"  {pop} {host}")
