import sys
sys.path.insert(0, r"c:\Users\Owner\Steam Tester")
import tester

root = tester.steam_root()
path = next((root / "depotcache").glob("1797881_*.manifest"))
sha = tester.parse_manifest(path, 900_000, 1)[0]
hosts = [
    "cache1-mad1.steamcontent.com",
    "cache1-seo1.steamcontent.com",
    "cache1-dxb1.steamcontent.com",
    "cache1-sof-retn.steamcontent.com",
    "cache1-hel-retn.steamcontent.com",
    "cache1-per-edgx.steamcontent.com",
    "cache1-pit-pitx.steamcontent.com",
    "cache1-msp-mice.steamcontent.com",
    "cache1-ind-fdix.steamcontent.com",
    "cache1-ord1.steamcontent.com",
]
for h in hosts:
    print(("ok " if tester.probe(h, 1797881, sha) else "NO "), h)
