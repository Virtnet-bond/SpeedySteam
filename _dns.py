import socket
import time

def ok(host):
    try:
        socket.getaddrinfo(host, 443, socket.AF_INET)
        return True
    except OSError:
        return False

t = time.perf_counter()
print("nonsense", ok("cache1-zzz-notreal.steamcontent.com"), f"{time.perf_counter()-t:.2f}s")
samples = [
    "cache1-ord1.steamcontent.com",
    "cache1-den-iwst.steamcontent.com",
    "cache1-yyz-bell.steamcontent.com",
    "cache1-seo1.steamcontent.com",
    "cache1-mad1.steamcontent.com",
    "cache1-dxb1.steamcontent.com",
    "cache1-bom1.steamcontent.com",
    "cache1-maa1.steamcontent.com",
    "cache1-per-edgx.steamcontent.com",
    "cache1-sof-retn.steamcontent.com",
    "cache1-hel-retn.steamcontent.com",
    "cache1-okc1.steamcontent.com",
    "cache1-atl1.steamcontent.com",
    "cache1-man1.steamcontent.com",
    "cache1-tyo1.steamcontent.com",
    "cache1-dfw1.steamcontent.com",
    "cache1-pit-pitx.steamcontent.com",
    "cache1-msp-mice.steamcontent.com",
    "cache1-ind-fdix.steamcontent.com",
]
for h in samples:
    print(("HIT " if ok(h) else "miss"), h)
