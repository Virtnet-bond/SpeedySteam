#!/usr/bin/env python3
"""Measure the Steam PoPs reachable from this connection and recommend a region.

Steam lets you pick a download region, but not which server answers you: that
is decided by a priority ranking Valve assigns per region, which has nothing to
do with how fast the server is from where you are. This finds the regions whose
top-ranked server is actually fast for you.

    steam-region-speedtest                 # scan, measure, recommend
    steam-region-speedtest --budget 150    # lighter on traffic
    steam-region-speedtest --json out.json

Needs Python 3.7+ and a Steam install with at least one game; no third-party
packages. It reads depot manifests from your own install and pulls those chunks
from the CDN to measure, which with the defaults is up to a few GB of traffic -
mind metered connections, and run it while nothing else is using the link or
the numbers will be wrong.

Not affiliated with Valve. It only makes the same requests the Steam client
does, but please don't loop it.

Three things that aren't obvious, and that shape the whole design:

- priority_class belongs to the (cell, host) pair, not to the PoP. Steam only
  opens connections to hosts in the winning class of whichever cell you pick,
  so a PoP can be fast and still be unreachable. The recommendation is one of
  those winners. Every other cache that still serves chunks is measured too,
  so a faster site that no region selects is visible in the table.
- Throughput needs up to ~5s to settle, and that time grows with RTT. A test
  that downloads a fixed amount therefore punishes distant PoPs - by as much as
  17x in practice. This samples the instantaneous rate and drops the ramp.
- A poor cache hit rate measures the origin fill rather than the network, so a
  cold result is thrown away and remeasured with the cache warmed by the first
  pass.
"""

import argparse
import json
import re
import socket
import ssl
import statistics
import struct
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

API = "https://api.steampowered.com/IContentServerDirectoryService/GetServersForSteamPipe/v1/"
STEAM_DIRS = ["~/.local/share/Steam", "~/.steam/steam", "~/.steam/debian-installation",
              "~/Library/Application Support/Steam"]
WIN_DIRS = [r"C:\Program Files (x86)\Steam", r"C:\Program Files\Steam", r"C:\Steam"]
STEAM_REG = r"Software\Valve\Steam"
MANIFEST_MAGIC = 0x71F617D0
HOST_RE = re.compile(r"^cache\d+-(.+)\.steamcontent\.com$")
FALLBACK_CELLS = range(231)  # only used if appinfo.vdf can't be read

CITIES = {
    "ams": "Amsterdam", "atl": "Atlanta", "bkk": "Bangkok", "bog": "Bogota",
    "bom": "Mumbai", "bos": "Boston", "bru": "Brussels", "cph": "Copenhagen",
    "del": "Delhi", "den": "Denver", "dfw": "Dallas", "dub": "Dublin",
    "dxb": "Dubai", "ewr": "Newark", "eze": "Buenos Aires", "fra": "Frankfurt",
    "gru": "Sao Paulo", "hel": "Helsinki", "hkg": "Hong Kong", "iad": "Washington DC",
    "ist": "Istanbul", "jkt": "Jakarta", "jnb": "Johannesburg", "las": "Las Vegas",
    "lax": "Los Angeles", "lhr": "London", "lim": "Lima", "lis": "Lisbon",
    "maa": "Chennai", "mad": "Madrid", "man": "Manchester", "mde": "Medellin",
    "mia": "Miami", "mnl": "Manila", "msp": "Minneapolis", "mxp": "Milan",
    "ord": "Chicago", "osl": "Oslo", "par": "Paris", "phx": "Phoenix",
    "scl": "Santiago", "sea": "Seattle", "sel": "Seoul", "sgp": "Singapore",
    "sha": "Shanghai", "sjc": "San Jose", "slc": "Salt Lake City", "sof": "Sofia",
    "sto": "Stockholm", "syd": "Sydney", "tpe": "Taipei", "tyo": "Tokyo",
    "uio": "Quito", "vie": "Vienna", "waw": "Warsaw", "yvr": "Vancouver",
    "yyz": "Toronto",
    "adl": "Adelaide", "akl": "Auckland", "ala": "Almaty", "bne": "Brisbane",
    "bud": "Budapest", "got": "Gothenburg", "hrk": "Kharkiv", "iev": "Kyiv",
    "ind": "Indianapolis", "mel": "Melbourne", "mfm": "Macau", "mmx": "Malmo",
    "okc": "Oklahoma City", "per": "Perth", "pit": "Pittsburgh", "rix": "Riga",
    "seo": "Seoul", "svo": "Moscow", "uln": "Ulaanbaatar", "vno": "Vilnius",
    "ywg": "Winnipeg", "yyc": "Calgary",
}

# City codes and ISP suffixes for caches the cell directory never lists.
# Dead names are dropped by DNS, and names that resolve but refuse the depot
# are dropped by a chunk probe.
EDGE_CITIES = set(CITIES) | {
    "adl", "akl", "ala", "bne", "bud", "cfc", "dsa", "got", "gsv", "hrk",
    "iev", "ind", "mel", "mfm", "mmx", "okc", "per", "pit", "rix", "seo",
    "svo", "uln", "vno", "ywg", "yyc", "ymo",
}
EDGE_PROVIDERS = (
    "edgx", "iwst", "retn", "shaw", "bell", "tsvy", "hwng", "aubb", "nxhp",
    "jast", "cdtm", "untl", "tpwr", "vgnz", "kzkt", "acol", "ask4", "datl",
    "extl", "cmhm", "bite", "fdix", "mice", "pitx", "iioa", "true",
)
# Suffixes that show up on certificates for caches the directory hides.
# Used only when the certificate log itself cannot be read.
EXTRA_PROVIDERS = (
    "ohix", "detx", "rise", "znog", "tkmy", "cnvg", "mhkx", "frst", "hnet",
    "tpix", "twds", "twfn", "tnbb", "dmsn", "wilh", "pxms", "init", "vtrn",
    "rgrs", "edgu", "pitc", "adyl", "unfq", "gzgh", "pcfc", "awnc", "thbb",
    "sprk", "tdeg", "tdst", "lebx", "oddo", "vocs", "tlia", "wavx", "pite",
    "giga", "askf", "napv", "alta", "slix", "mdco", "kcix", "sfix", "allo",
    "gmnt", "gnet", "mbcm", "bltc", "tetl", "wrix", "cbse", "pita", "ixgt",
    "artk", "cmix", "dnmc", "cool", "glbl", "pith", "topx", "bnet", "crix",
)


# loc_name prefixes and keys for the Europe download regions. UK is included;
# Russia and Turkey are not.
EU_PREFIXES = (
    "uk_", "de_", "fr_", "nl_", "ro_", "it_", "pl_", "es_", "dk_", "cz_",
    "gr_", "se_", "no_", "fi_", "ie_", "at_", "hu_", "lv_",
)
EU_LOCS = {
    "belgium", "switzerland", "luxembourg", "portugal", "bulgaria", "croatia",
    "lithuania", "iceland", "canary_islands",
    "ua_kiev", "ua_odessa", "ua_lviv", "ua_kharkiv", "ua_sevastopol",
}
# in_ is both India and Indonesia, and ua_dubai is the Emirates, not Ukraine.
ASIA_PREFIXES = ("jp_", "kr_", "cn_", "tw_", "th_", "ph_", "my_", "kz_", "pk_", "sa_")
ASIA_LOCS = {
    "singapore", "cambodia", "vietnam", "mongolia", "kazakhstan", "central_asia",
    "in_mumbai", "in_chennai", "in_delhi", "in_bangalore", "in_hyderabad", "in_kolkata",
    "in_jakarta", "ua_dubai", "kuwait", "israel",
}
OCEANIA_PREFIXES = ("au_", "nz_")
OCEANIA_LOCS = {"pacific_islands", "guam"}


def region_area(loc):
    if loc.startswith("us_"):
        return "us"
    if loc in EU_LOCS or loc.startswith(EU_PREFIXES):
        return "eu"
    if loc in ASIA_LOCS or loc.startswith(ASIA_PREFIXES):
        return "asia"
    if loc in OCEANIA_LOCS or loc.startswith(OCEANIA_PREFIXES):
        return "oceania"
    return ""


def city_of(pop):
    if pop == "akamai":
        return "Akamai"
    return CITIES.get(pop[:3], pop)


def pop_city(pop):
    """Airport code a cache hostname is addressed to: den-iwst and mad1 are den, mad."""
    return (pop or "").split("-")[0][:3]


# Hostname city codes for download regions whose cache is not a major PoP.
# Only a code that belongs to that region is searched, so a Chicago host is
# never filed under Columbus.
LOC_CODES = {
    "us_anchorage": ("anc",), "us_austin": ("aus",), "us_boston": ("bos",),
    "us_charlotte": ("clt",), "us_columbus": ("cmh",), "us_denver": ("den",),
    "us_detroit": ("dtw",), "us_honolulu": ("hnl",), "us_houston": ("iah", "hou"),
    "us_miami": ("mia",), "us_minneapolis": ("msp",), "us_newyork": ("ewr", "nyc", "jfk", "lga"),
    "us_philadelphia": ("phl",), "us_phoenix": ("phx",), "us_pittsburgh": ("pit",),
    "us_sacramento": ("smf",), "us_sandiego": ("san",), "us_sanjose": ("sjc",),
    "us_stlouis": ("stl",),
    "ca_calgary": ("yyc",), "ca_edmonton": ("yeg",), "ca_montreal": ("yul",),
    "ca_ottawa": ("yow",), "ca_toronto": ("yyz",), "ca_vancouver": ("yvr",),
    "ca_winnipeg": ("ywg",),
    "au_adelaide": ("adl",), "au_brisbane": ("bne",), "au_melbourne": ("mel",),
    "au_perth": ("per",), "nz_auckland": ("akl",),
    "br_belohorizonte": ("cnf",), "br_belem": ("bel",), "br_brasilia": ("bsb",),
    "br_campogrande": ("cgr",), "br_cuiaba": ("cgb",), "br_curitiba": ("cwb",),
    "br_manaus": ("mao",), "br_portoalegre": ("poa",), "br_recife": ("rec",),
    "br_rio": ("gig",), "br_salvador": ("ssa",),
    "es_barcelona": ("bcn",), "es_madrid": ("mad",), "es_malaga": ("agp",),
    "es_valencia": ("vlc",),
    "de_berlin": ("ber",), "de_dusseldorf": ("dus",), "de_hamburg": ("ham",),
    "de_munich": ("muc",), "fr_marseille": ("mrs",), "uk_manchester": ("man",),
    "it_milan": ("mxp",), "it_rome": ("fco", "rom"),
    "se_goteborg": ("got",), "se_malmo": ("mmx",), "pl_katowice": ("ktw",),
    "fi_helsinki": ("hel",), "no_oslo": ("osl",), "dk_copenhagen": ("cph",),
    "cz_prague": ("prg",), "gr_athens": ("ath",), "hu_budapest": ("bud",),
    "lv_riga": ("rix",), "lithuania": ("vno",), "bulgaria": ("sof",),
    "ua_kiev": ("iev",), "ua_kharkiv": ("hrk",), "ua_lviv": ("lwo",),
    "ua_odessa": ("ods",), "ua_dubai": ("dxb",),
    "ru_moscow": ("svo",), "ru_stpetersburg": ("led",), "ru_novosibirsk": ("ovb",),
    "ru_yekaterinburg": ("svx",), "ru_kazan": ("kzn",), "ru_rostov": ("rov",),
    "ru_irkutsk": ("ikt",), "ru_vladivostok": ("vvo",),
    "tr_istanbul": ("ist",), "tr_ankara": ("esb",), "tr_izmir": ("adb",),
    "kz_almaty": ("ala",), "mongolia": ("uln",), "guam": ("gum",),
    "in_mumbai": ("bom",), "in_chennai": ("maa",), "in_delhi": ("del",),
    "in_bangalore": ("blr",), "in_hyderabad": ("hyd",), "in_kolkata": ("ccu",),
    "in_jakarta": ("jkt",), "th_bangkok": ("bkk",), "ph_manila": ("mnl",),
    "ph_cebu": ("ceb",), "ph_davao": ("dvo",), "my_kualalumpur": ("kul",),
    "jp_osaka": ("osa", "itm"), "jp_fukuoka": ("fuk",), "jp_nagoya": ("ngo",),
    "jp_sapporo": ("spk",), "jp_sendai": ("sdj",), "kr_seoul": ("seo", "sel"),
    "kr_busan": ("pus",), "tw_taiwan": ("tpe", "tsa"),
    "mx_mexicocity": ("mex",), "co_bogota": ("bog",), "za_capetown": ("cpt",),
    "eg_cairo": ("cai",), "sa_riyadh": ("ruh",), "pk_karachi": ("khi",),
    "switzerland": ("zrh",), "belgium": ("bru",), "ke_nairobi": ("nbo",),
    "cn_kunming": ("kun",), "ecuador": ("uio", "gye"),
    "kazakhstan": ("nqz", "tse"), "venezuela": ("ccs",),
    "panama": ("pty", "gua", "sjo", "sap"),
    "ma_rabat": ("rba",), "ng_lagos": ("los",), "belarus": ("msq",),
    "bolivia": ("lpb", "vvi"), "cambodia": ("pnh",),
    "canary_islands": ("lpa", "tfs"), "puerto_rico": ("sju",),
    "caucasus": ("tbs", "evn"), "central_asia": ("tas", "fru"),
    "croatia": ("zag",), "iceland": ("kef",), "ie_dublin": ("dub",),
    "israel": ("tlv",), "kuwait": ("kwi",), "luxembourg": ("lux",),
    "maritius": ("mru",), "pacific_islands": ("ppt",), "ph_baguio": ("bag",),
    "portugal": ("lis", "opo"), "reunion": ("run",), "ro_bucharest": ("otp",),
    "ua_sevastopol": ("sip",), "vietnam": ("sgn", "han"),
}


def cdn_pop(host):
    host = (host or "").lower()
    if "akamai" in host:
        return "akamai"
    return host.split(".")[0] or host


# --- region names as they appear in the Steam UI -----------------------------

def _varint(b, i):
    r = s = 0
    while True:
        c = b[i]
        i += 1
        r |= (c & 0x7F) << s
        s += 7
        if not c & 0x80:
            return r, i


def _fields(b):
    i = 0
    while i < len(b):
        k, i = _varint(b, i)
        f, w = k >> 3, k & 7
        if w == 0:
            v, i = _varint(b, i)
        elif w == 2:
            n, i = _varint(b, i)
            v = b[i:i + n]
            i += n
        elif w == 5:
            v, i = b[i:i + 4], i + 4
        elif w == 1:
            v, i = b[i:i + 8], i + 8
        else:
            raise ValueError(f"wire type {w}")
        yield f, v


def _win_reg(value):
    try:
        import winreg
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, STEAM_REG) as k:
            return winreg.QueryValueEx(k, value)[0]
    except Exception:
        return None


def steam_root():
    cands = []
    if sys.platform == "win32":
        sp = _win_reg("SteamPath")
        if sp:
            cands.append(Path(sp))
        cands += [Path(d) for d in WIN_DIRS]
    cands += [Path(d).expanduser() for d in STEAM_DIRS]
    for p in cands:
        try:
            if (p / "depotcache").is_dir() or (p / "appcache").is_dir():
                return p
        except OSError:
            continue
    sys.exit("Couldn't find a Steam installation.")


def steam_language(root):
    if sys.platform == "win32":
        lang = _win_reg("Language")
        if lang:
            return str(lang)
    for cand in (root.parent / "registry.vdf", root / "registry.vdf"):
        try:
            m = re.search(r'"Language"\s+"(\w+)"', cand.read_text(errors="replace"))
            if m:
                return m.group(1)
        except Exception:
            pass
    return "english"


def load_cell_names(root, lang=None):
    """Return (cell id to Steam's label, cell id to the loc key)."""
    f = root / "appcache" / "appinfo.vdf"
    cells = {}
    try:
        data = f.read_bytes()
        if struct.unpack("<I", data[:4])[0] not in (0x07564429, 0x07564428):
            return {}, {}
        off = struct.unpack("<q", data[8:16])[0]
        cnt = struct.unpack("<I", data[off:off + 4])[0]
        tbl, i = [], off + 4
        for _ in range(cnt):
            j = data.index(b"\x00", i)
            tbl.append(data[i:j].decode("utf8", "replace"))
            i = j + 1
        key = struct.pack("<I", tbl.index("loc_name"))
        pat = re.compile(rb"\x00(.{4})\x01" + key + rb"([a-z0-9_]{2,40})\x00", re.S)
        for m in pat.finditer(data):
            idx = struct.unpack("<I", m.group(1))[0]
            if idx < len(tbl) and tbl[idx].isdigit():
                cells[int(tbl[idx])] = m.group(2).decode()
    except Exception:
        return {}, {}

    gui = {}
    lang = lang or steam_language(root)
    for name in (f"steamui_{lang}.txt", "steamui_english.txt"):
        p = root / "public" / name
        if p.is_file():
            try:
                gui.update(re.findall(r'"DownloadRegion_([a-z0-9_]+)"\s*"([^"]*)"',
                                      p.read_text(encoding="utf-8-sig", errors="replace")))
            except Exception:
                pass
        if gui:
            break
    names = {}
    locs = {}
    for cell, loc in cells.items():
        locs[cell] = loc
        names[cell] = gui.get(loc, loc.replace("_", " "))
    return names, locs


# --- chunks to pull ----------------------------------------------------------

def parse_manifest(path, min_chunk, want):
    data = path.read_bytes()
    if len(data) < 8 or struct.unpack("<I", data[:4])[0] != MANIFEST_MAGIC:
        return []
    ln = struct.unpack("<I", data[4:8])[0]
    out = []
    for f, v in _fields(data[8:8 + ln]):
        if f != 1:
            continue
        for f2, v2 in _fields(v):
            if f2 != 6:
                continue
            sha = size = None
            for f3, v3 in _fields(v2):
                if f3 == 1:
                    sha = v3.hex()
                elif f3 == 5:
                    size = v3
            if sha and size and size >= min_chunk:
                out.append(sha)
                if len(out) >= want:
                    return out
    return out


def probe(host, depot, sha, timeout=15):
    try:
        with urllib.request.urlopen(
                f"https://{host}/depot/{depot}/chunk/{sha}", timeout=timeout) as r:
            return r.status == 200
    except Exception:
        return False


def find_chunks(root, pool, min_chunk, probe_host):
    cands = []
    for sub in ("depotcache", "steamapps"):
        d = root / sub
        if d.is_dir():
            cands += list(d.glob("*.manifest"))
    cands.sort(key=lambda p: p.stat().st_size, reverse=True)
    if not cands:
        sys.exit(f"No manifests found under {root}. Install or update a game first.")
    for path in cands[:12]:
        try:
            depot = int(path.name.split("_")[0])
            chunks = parse_manifest(path, min_chunk, pool)
        except Exception:
            continue
        if len(chunks) >= 8 and probe(probe_host, depot, chunks[0]):
            return depot, chunks, path.name
    sys.exit("None of the local depots are still on the CDN. Update a game and retry.")


# --- cell sweep --------------------------------------------------------------

def servers_for_cell(cell, geolocate=False):
    # The edge geolocates the caller and ignores cell_id unless ip_override is
    # set, so every region comes back as this connection's local PoPs.
    # 0.0.0.0 turns that pin off. max_servers and launcher_type match the
    # client, which asks for 32 sources with launcher 0. geolocate=True is the
    # request the client itself makes, used to see whether a pick will stick.
    pin = "" if geolocate else "&ip_override=0.0.0.0"
    url = f"{API}?cell_id={cell}&max_servers=32&launcher_type=0{pin}"
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            return json.load(r)["response"]["servers"]
    except Exception:
        return []


def discover_edges(known, progress=None, cities=None):
    """Hostnames for regional caches the cell directory does not return.

    Candidates are {city}{1,2,3} and {city}-{isp}. Only names that resolve are
    returned; the caller still has to check that the depot is served.
    progress(done, total) is optional. cities limits the scan to those codes.
    """
    cities = EDGE_CITIES if cities is None else cities
    cands = []
    for city in cities:
        for n in (1, 2, 3):
            cands.append(f"{city}{n}")
        for prov in EDGE_PROVIDERS + EXTRA_PROVIDERS:
            cands.append(f"{city}-{prov}")
    cands = [p for p in cands if p not in known]

    def resolve(pop):
        for n in (1, 2):
            host = f"cache{n}-{pop}.steamcontent.com"
            try:
                socket.getaddrinfo(host, 443, socket.AF_INET)
                return pop, host
            except OSError:
                continue
        return None

    found = {}
    total = len(cands)
    done = 0
    with ThreadPoolExecutor(max_workers=40) as ex:
        futures = [ex.submit(resolve, pop) for pop in cands]
        for fut in as_completed(futures):
            done += 1
            item = fut.result()
            if item:
                pop, host = item
                found.setdefault(pop, set()).add(host)
            if progress and (done % 25 == 0 or done == total):
                progress(done, total)
    return found


CERT_HOST = re.compile(r"^(?:cache|dist)\d+-(.+)\.steamcontent\.com$")


def certificate_hosts(progress=None):
    """Cache domains published in the certificate log.

    Valve issues a certificate per cache hostname, so the log is the list of
    steamcontent.com domains, including city caches the directory only returns
    to a client in that city. progress(page, names) is optional.
    """
    found = set()
    after = None
    stalls = 0
    pages = 0
    while pages < 40 and stalls < 3:
        url = ("https://api.certspotter.com/v1/issuances?domain=steamcontent.com"
               "&include_subdomains=true&expand=dns_names")
        if after:
            url += "&after=" + after
        req = urllib.request.Request(url, headers={"User-Agent": "steam-tester"})
        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                batch = json.load(r)
        except urllib.error.HTTPError as e:
            if e.code == 429:
                stalls += 1
                time.sleep(1.5 * stalls)
                continue
            break
        except Exception:
            break
        stalls = 0
        pages += 1
        if not batch:
            break
        after = str(batch[-1].get("id") or "")
        for item in batch:
            for name in item.get("dns_names") or []:
                host = name.lower().strip().rstrip(".")
                if CERT_HOST.match(host) or host.endswith(".cdn.steampipe.steamcontent.com"):
                    found.add(host)
        if progress:
            progress(pages, len(found))
        if len(batch) < 100 or not after:
            break
    return found


def top_class_pops(servers):
    """PoPs in the priority class Steam opens for this server list."""
    bycls = {}
    for sv in servers:
        if sv.get("type") != "SteamCache":
            continue
        m = HOST_RE.match(sv.get("host", ""))
        if not m:
            continue
        bycls.setdefault(sv.get("priority_class") or 0, set()).add(m.group(1))
    if not bycls:
        return set()
    return bycls[max(bycls)]


def cell_winners(cells, workers=16, status=None):
    """Returns (winner per cell, hosts per PoP, home cell per PoP).

    A PoP's home cell is the one its own servers report. That answers "where
    this PoP is", which is a different question from "which cell to pick to
    reach it" - and the two often disagree.
    """
    win, hosts, home = {}, {}, {}
    cells = list(cells)
    total = len(cells)
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for done, (cell, servers) in enumerate(
                zip(cells, ex.map(servers_for_cell, cells)), 1):
            bycls = {}
            for sv in servers:
                if sv.get("type") != "SteamCache":
                    continue
                m = HOST_RE.match(sv.get("host", ""))
                if not m:
                    continue
                pop = m.group(1)
                hosts.setdefault(pop, set()).add(sv["host"])
                rep = sv.get("cell_id")
                if rep is not None:
                    h = home.setdefault(pop, {})
                    h[rep] = h.get(rep, 0) + 1
                bycls.setdefault(sv.get("priority_class") or 0, []).append(pop)
            if bycls:
                mx = max(bycls)
                win[cell] = (sorted(set(bycls[mx])), mx, len(bycls[mx]))
            else:
                cdn = {}
                for sv in servers:
                    if sv.get("type") != "CDN":
                        continue
                    host = sv.get("host") or ""
                    if "." not in host:
                        continue
                    pop = cdn_pop(host)
                    hosts.setdefault(pop, set()).add(host)
                    cdn.setdefault(sv.get("priority_class") or 0, []).append(pop)
                if cdn:
                    mx = max(cdn)
                    win[cell] = (sorted(set(cdn[mx])), mx, len(cdn[mx]))
            if status:
                status(done, total, len(hosts))
    return win, hosts, {p: max(v, key=v.get) for p, v in home.items()}


# --- measurement -------------------------------------------------------------

def steady(samples, ratio=0.8):
    """Drop the ramp: everything below 80% of the settled rate."""
    if len(samples) < 3:
        return samples, 0
    ref = statistics.median(samples[len(samples) // 2:])
    if ref <= 0:
        return samples, 0
    i = 0
    while i < len(samples) and samples[i] < ratio * ref:
        i += 1
    if len(samples) - i < 2:
        return samples[len(samples) // 2:], len(samples) // 2
    return samples[i:], i


def is_stable(vals, k=4, tol=0.2):
    if len(vals) < k:
        return False
    tail = vals[-k:]
    m = statistics.mean(tail)
    return m > 0 and statistics.pstdev(tail) / m < tol


class Result:
    def __init__(self, pop):
        self.pop = pop
        self.mbps = 0.0
        self.avg_mbps = None
        self.bytes = 0
        self.hits = self.misses = self.errors = 0
        self.secs = self.ramp = 0.0
        self.stable = self.rewarmed = False
        self.ms = self.ms_min = self.hitpct = None


def wait_if_paused(pause_event, *stops):
    """Block while paused. Returns False when a stop event is set."""
    while pause_event is not None and pause_event.is_set():
        if any(stop is not None and stop.is_set() for stop in stops):
            return False
        time.sleep(0.05)
    return True


def measure(pop, host, depot, chunks, conns, min_samples, max_secs, max_bytes,
            interval=0.5, on_rate=None, stop_event=None, ui_hz=60, pause_event=None):
    res = Result(pop)
    ctx = ssl.create_default_context()
    lock = threading.Lock()
    stop = threading.Event()
    st = {"bytes": 0, "idx": 0, "hits": 0, "misses": 0, "errors": 0}

    def worker():
        import http.client
        conn = None
        while not stop.is_set():
            if not wait_if_paused(pause_event, stop, stop_event):
                break
            try:
                if conn is None:
                    conn = http.client.HTTPSConnection(host, 443, context=ctx, timeout=20)
                with lock:
                    sha = chunks[st["idx"] % len(chunks)]
                    st["idx"] += 1
                conn.request("GET", f"/depot/{depot}/chunk/{sha}",
                             headers={"Connection": "keep-alive"})
                r = conn.getresponse()
                cache = (r.getheader("x-cache-status") or "").upper()
                with lock:
                    if cache == "HIT":
                        st["hits"] += 1
                    elif cache == "MISS":
                        st["misses"] += 1
                while not stop.is_set():
                    b = r.read(262144)
                    if not b:
                        break
                    with lock:
                        st["bytes"] += len(b)
                else:
                    break
            except Exception:
                with lock:
                    st["errors"] += 1
                try:
                    conn.close()
                except Exception:
                    pass
                conn = None
                if stop.is_set():
                    break
                time.sleep(0.2)
        try:
            conn.close()
        except Exception:
            pass

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(conns)]
    t0 = time.monotonic()
    for t in threads:
        t.start()

    samples = []
    buckets = []
    last_bytes = 0
    last_ui = t0
    bucket_bytes = 0
    bucket_start = t0
    tick = 1.0 / ui_hz if on_rate else interval
    while True:
        if pause_event is not None and pause_event.is_set():
            paused_at = time.monotonic()
            wait_if_paused(pause_event, stop_event)
            held = time.monotonic() - paused_at
            t0 += held
            with lock:
                cur = st["bytes"]
            last_bytes = cur
            last_ui = time.monotonic()
            bucket_bytes = cur
            bucket_start = last_ui
            if stop_event is not None and stop_event.is_set():
                break
            continue
        time.sleep(tick)
        now = time.monotonic() - t0
        wall = time.monotonic()
        with lock:
            cur, errs = st["bytes"], st["errors"]
        if on_rate:
            dt = wall - last_ui
            if dt > 0:
                on_rate(now, (cur - last_bytes) / dt / 1048576)
            last_bytes = cur
            last_ui = wall
        if on_rate is None or wall - bucket_start >= interval:
            span = wall - bucket_start
            if span > 0:
                delta = cur - bucket_bytes
                samples.append(delta / span / 1048576)
                buckets.append((delta, span))
            bucket_bytes = cur
            bucket_start = wall
        if stop_event is not None and stop_event.is_set():
            break
        if cur >= max_bytes or now >= max_secs:
            break
        if errs > conns * 4 and cur == 0:
            break
        vals, _ = steady(samples)
        if len(vals) >= min_samples and is_stable(vals):
            break

    wall = time.monotonic()
    with lock:
        cur = st["bytes"]
    span = wall - bucket_start
    if span >= 0.05 and cur > bucket_bytes:
        delta = cur - bucket_bytes
        samples.append(delta / span / 1048576)
        buckets.append((delta, span))

    stop.set()
    res.secs = time.monotonic() - t0
    for t in threads:
        t.join(timeout=5)
    with lock:
        res.bytes, res.hits = st["bytes"], st["hits"]
        res.misses, res.errors = st["misses"], st["errors"]
    if samples:
        vals, cut = steady(samples)
        res.mbps = statistics.median(vals)
        res.ramp = cut * interval
        res.stable = len(vals) >= min_samples and is_stable(vals)
        post = buckets[cut:]
        post_bytes = sum(b for b, _ in post)
        post_secs = sum(s for _, s in post)
        if post_secs > 0:
            res.avg_mbps = post_bytes / 1048576 / post_secs
    tot = res.hits + res.misses
    res.hitpct = (100 * res.hits / tot) if tot else None
    return res


def measure_hot(pop, host, depot, chunks, args, on_rate=None, stop_event=None,
                on_begin=None, pause_event=None):
    """Measure with the cache warm - a cold run times the origin, not the link."""
    mb = args.budget * 1048576
    if on_begin:
        on_begin(False)
    r = measure(pop, host, depot, chunks, args.conns, args.min_samples,
                args.max_secs, mb, on_rate=on_rate, stop_event=stop_event,
                pause_event=pause_event)
    if stop_event is not None and stop_event.is_set():
        return r
    if r.hitpct is not None and r.hitpct < args.min_hit:
        if on_begin:
            on_begin(True)
        r2 = measure(pop, host, depot, chunks, args.conns, args.min_samples,
                     args.max_secs, mb, on_rate=on_rate, stop_event=stop_event,
                     pause_event=pause_event)
        r2.rewarmed = True
        return r2
    return r


def tcp_latency(host, port=443, samples=9, timeout=5.0, pause_event=None, stop_event=None):
    """(median, minimum) TCP handshake time in ms.

    Resolves once up front: passing a hostname to create_connection charges the
    DNS lookup to every sample (measured: 164 ms against a real 13 ms). Nine
    samples because a few is not enough on hosts that queue their accepts - one
    PoP here alternates ~30 ms with 1100 ms spikes and five samples once put the
    median at 1093 ms.
    """
    try:
        ip = socket.getaddrinfo(host, port, socket.AF_INET)[0][4][0]
    except Exception:
        return None, None
    times = []
    for _ in range(samples):
        if not wait_if_paused(pause_event, stop_event):
            break
        if stop_event is not None and stop_event.is_set():
            break
        t0 = time.perf_counter()
        try:
            sk = socket.create_connection((ip, port), timeout=timeout)
            times.append((time.perf_counter() - t0) * 1000)
            sk.close()
        except Exception:
            pass
        time.sleep(0.05)
    return (statistics.median(times), min(times)) if times else (None, None)


# --- output ------------------------------------------------------------------

def region_label(cell, names):
    if cell is None:
        return "—"
    return names.get(cell, "?")


def table(results, where, pick_cell, names):
    pw = max([10] + [len(r.pop) for r in results])
    u = max([14] + [len(where[r.pop]) for r in results])
    w = max([15] + [len(region_label(pick_cell[r.pop][0], names)) for r in results])
    print(f"\n  {'PoP':<{pw}} {'located in':<{u}} {'pick in Steam':<{w}} "
          f"{'MB/s':>7} {'Mbit':>6} {'ms':>5} {'cache':>8}")
    print("  " + "-" * (32 + pw + u + w))
    for r in results:
        cell = pick_cell[r.pop][0]
        cache = f"{r.hitpct:.0f}% hit" if r.hitpct is not None else "-"
        ms = f"{r.ms:.0f}" if r.ms is not None else "-"
        mark = "" if r.stable else " ~"
        if r.hitpct is not None and r.hitpct < 50:
            mark += " !"
        if r.ms and r.ms_min and r.ms > 2.5 * r.ms_min:
            mark += " *"
        print(f"  {r.pop:<{pw}} {where[r.pop]:<{u}} {region_label(cell, names):<{w}} "
              f"{r.mbps:>7.1f} {r.mbps * 8.389:>6.0f} {ms:>5} {cache:>8}{mark}")
    print("\n  ~ rate never settled   ! poor cache hit rate   * erratic latency")


def prepare_catalog(root, names, pool=128, on_status=None, locs=None):
    """Scan Steam's download regions and the cache each one actually uses.

    on_status(message, fraction) reports progress from 0 to 1. The cell scan
    is almost all of that time; reading a local manifest is the short tail.
    """
    def say(msg, frac):
        if on_status:
            on_status(msg, frac)

    def cell_status(done, total, npops):
        frac = 0.9 * done / total if total else 0
        say(f"Scanning download regions  {done} / {total}   ·   {npops} servers", frac)

    say("Scanning download regions…", 0.0)
    win, hosts, home = cell_winners(
        sorted(names) if names else FALLBACK_CELLS, status=cell_status)
    if not win:
        raise RuntimeError("The API returned no servers.")

    pick_cell, rank, alts = {}, {}, {}
    for cell, (pops_, cls, nh) in sorted(win.items()):
        for pop in pops_:
            key = (1 if cell in names else 0, 1 if home.get(pop) == cell else 0, nh, -cell)
            alts.setdefault(pop, []).append(cell)
            if pop not in pick_cell or key > rank[pop]:
                pick_cell[pop], rank[pop] = (cell, cls, nh), key

    for pop in list(hosts):
        if pop not in pick_cell:
            pick_cell[pop] = (None, None, len(hosts[pop]))

    # One row per Steam download region. The server shown is one whose own
    # address is that region. A region the directory sends to another city is
    # left blank here, then filled below if a cache hostname for that city exists.
    if names:
        region_cells = sorted((c for c in names if c in win), key=lambda c: names[c].lower())
    else:
        region_cells = sorted(win)
    loc_pop, loc_host, row_label, row_loc = {}, {}, {}, {}
    for cell in region_cells:
        winners = win[cell][0]
        local = [p for p in winners if home.get(p) == cell]
        if local:
            pop = local[0]
        elif any(home.get(p) not in (None, cell) for p in winners):
            pop = None
        else:
            pop = winners[0]
        loc_pop[cell] = pop
        loc_host[cell] = sorted(hosts[pop])[0] if pop else None
        row_label[cell] = names.get(cell) or (city_of(pop) if pop else f"Cell {cell}")
        row_loc[cell] = (locs or {}).get(cell, "")

    seed = next((h for h in loc_host.values() if h), None)
    if seed is None:
        raise RuntimeError("Nothing to measure.")
    say("Reading a local depot manifest…", 0.92)
    depot, chunks, mname = find_chunks(root, pool, 900_000, seed)

    # City caches are named for that city (cmh-ohix, den-iwst) but the
    # directory only lists them for a client located there. The certificate
    # log publishes every cache domain; names it does not return are looked
    # up from the city code. A domain is kept only when it serves this depot.
    code_cell = {}
    for cell in region_cells:
        pop = loc_pop.get(cell)
        if pop and pop != "akamai":
            continue
        for code in LOC_CODES.get((locs or {}).get(cell, ""), ()):
            code_cell.setdefault(code, cell)
    if code_cell and chunks:
        say("Reading cache names…", 0.93)

        def cert_progress(_page, count):
            say(f"Reading cache names…  {count}", 0.93)

        candidates = {}

        def consider(pop, host):
            cell = code_cell.get(pop_city(pop))
            if cell is None or loc_pop.get(cell) not in (None, "akamai"):
                return
            candidates.setdefault(cell, []).append((pop, host))

        for pop, hostset in hosts.items():
            consider(pop, sorted(hostset)[0])

        pending = []
        for host in certificate_hosts(progress=cert_progress):
            m = CERT_HOST.match(host)
            if not m:
                continue
            pop = m.group(1)
            if pop_city(pop) in code_cell:
                pending.append((pop, host))

        def resolves(item):
            _pop, host = item
            try:
                socket.getaddrinfo(host, 443, socket.AF_INET)
                return item
            except OSError:
                return None

        with ThreadPoolExecutor(max_workers=40) as ex:
            for item in ex.map(resolves, pending):
                if item:
                    consider(*item)

        covered = {pop_city(pop) for items in candidates.values() for pop, _host in items}
        missing = {code for code in code_cell if code not in covered}
        if missing:
            say("Looking for local caches…", 0.95)

            def edge_progress(done, total):
                frac = 0.95 + 0.03 * done / total if total else 0.95
                say(f"Looking for local caches…  {done} / {total}", frac)

            found = discover_edges(set(hosts), progress=edge_progress, cities=missing)
            for pop, hostset in found.items():
                consider(pop, sorted(hostset)[0])

        used = {p for p in loc_pop.values() if p and p != "akamai"}
        jobs = [(cell, pop, host) for cell, items in candidates.items()
                for pop, host in items if pop not in used]
        sha = chunks[0]

        def check(job):
            cell, pop, host = job
            return job if probe(host, depot, sha, timeout=10) else None

        alive = []
        with ThreadPoolExecutor(max_workers=16) as ex:
            for job in ex.map(check, jobs):
                if job:
                    alive.append(job)
        alive.sort()
        taken_cells, taken_pops = set(), set(used)
        for cell, pop, host in alive:
            if cell in taken_cells or pop in taken_pops:
                continue
            taken_cells.add(cell)
            taken_pops.add(pop)
            hosts.setdefault(pop, set()).add(host)
            home[pop] = cell
            pick_cell[pop] = (cell, None, 1)
            loc_pop[cell] = pop
            loc_host[cell] = host

    # A region with no cache of its own is sent to another city's server.
    # That server is listed too, and marked as a mirror.
    mirrors = {}
    for cell in region_cells:
        pop = loc_pop.get(cell)
        if not pop:
            winners = win[cell][0]
            pop = winners[0]
            loc_pop[cell] = pop
            loc_host[cell] = sorted(hosts[pop])[0] if pop in hosts else None
        home_cell = home.get(pop)
        mirrors[cell] = bool(pop) and home_cell not in (None, cell)

    def can_pick(pop):
        cell = pick_cell[pop][0]
        if cell is None:
            return False
        return cell in names if names else True

    pops = sorted({p for p in loc_pop.values() if p})
    if not pops:
        raise RuntimeError("Nothing to measure.")
    say("Ready", 1.0)
    where = {p: (names.get(home.get(p)) or city_of(p)) for p in hosts}
    return {
        "win": win, "hosts": hosts, "home": home, "names": names,
        "pick_cell": pick_cell, "alts": alts, "depot": depot,
        "chunks": chunks, "mname": mname, "pops": pops,
        "locations": region_cells, "loc_pop": loc_pop, "loc_host": loc_host,
        "row_label": row_label, "row_loc": row_loc, "mirrors": mirrors,
        "locs": locs or {}, "can_pick": can_pick, "where": where,
    }


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    p = argparse.ArgumentParser(
        description="Measure the Steam PoPs you can actually reach, and pick one.")
    p.add_argument("--conns", type=int, default=16,
                   help="parallel connections per PoP (default 16)")
    p.add_argument("--budget", type=int, default=400,
                   help="MB cap per PoP (default 400; going much lower skews the "
                        "result against distant PoPs)")
    p.add_argument("--max-secs", type=float, default=20.0,
                   help="seconds cap per PoP (default 20)")
    p.add_argument("--min-samples", type=int, default=6,
                   help="settled 0.5s samples needed to accept a measurement (default 6)")
    p.add_argument("--min-hit", type=float, default=70.0,
                   help="cache hit %% below which a PoP is measured again (default 70)")
    p.add_argument("--tie", type=float, default=10.0,
                   help="%% window counted as a tie, broken by latency (default 10)")
    p.add_argument("--pool", type=int, default=128,
                   help="distinct chunks to cycle through (default 128)")
    p.add_argument("--lang", help="language for region names (default: your Steam's)")
    p.add_argument("--json", metavar="FILE", help="write the results to a JSON file")
    p.add_argument("--yes", action="store_true", help="skip the traffic confirmation")
    args = p.parse_args()

    root = steam_root()
    names, locs = load_cell_names(root, args.lang)
    try:
        cat = prepare_catalog(root, names, args.pool, locs=locs)
    except RuntimeError as e:
        sys.exit(str(e))
    win, hosts, home = cat["win"], cat["hosts"], cat["home"]
    pick_cell, alts = cat["pick_cell"], cat["alts"]
    depot, chunks, mname = cat["depot"], cat["chunks"], cat["mname"]
    pops, can_pick = cat["pops"], cat["can_pick"]
    locations = cat["locations"]
    print(f"  {len(locations)} download regions, {len(pops)} servers to measure")
    print(f"  depot {depot} ({mname}), {len(chunks)} chunks")

    print(f"\nMeasuring {len(pops)} PoPs: up to {args.budget} MB or "
          f"{args.max_secs:.0f}s each, whichever comes first.")
    print(f"  Worst case {len(pops) * args.budget / 1024:.1f} GB and "
          f"{len(pops) * args.max_secs / 60:.0f} min. Most PoPs stop early, "
          f"and slow ones\n  hit the time cap long before the traffic cap.")
    if not args.yes:
        try:
            if input("Go ahead? [y/N] ").strip().lower() not in ("y", "yes"):
                sys.exit(0)
        except (EOFError, KeyboardInterrupt):
            sys.exit(0)

    results = []
    for i, pop in enumerate(pops, 1):
        host = sorted(hosts[pop])[0]
        print(f"  [{i}/{len(pops)}] {pop:<12} ", end="", flush=True)
        ms, ms_min = tcp_latency(host)
        r = measure_hot(pop, host, depot, chunks, args)
        r.ms, r.ms_min = ms, ms_min
        results.append(r)
        note = "  (remeasured warm)" if r.rewarmed else ""
        if r.hitpct is not None and r.hitpct < args.min_hit:
            note = f"  ({r.hitpct:.0f}% hit, treat with care)"
        lat = f"{r.ms:5.0f} ms" if r.ms is not None else ""
        if r.ms and r.ms_min and r.ms > 2.5 * r.ms_min:
            lat += f" (min {r.ms_min:.0f}, erratic)"
        print(f"{r.mbps:7.1f} MB/s   {lat}{note}")

    results.sort(key=lambda r: -r.mbps)
    where = {r.pop: (names.get(home.get(r.pop)) or city_of(r.pop)) for r in results}
    table(results, where, pick_cell, names)

    choose = [r for r in results if can_pick(r.pop)] or results
    top = choose[0]
    if top.mbps < 1.0:
        print("\n  Every PoP measured under 1 MB/s. Something is wrong with the "
              "connection\n  or the CDN is refusing us - the ranking below means "
              "nothing.")
    band = [r for r in choose if r.mbps >= top.mbps * (1 - args.tie / 100)]
    pick = top
    if len(band) > 1 and top.ms is not None:
        cand = min((r for r in band if r.ms is not None), key=lambda r: r.ms)
        # Only trade throughput for latency when the latency win is real,
        # otherwise a 1 ms edge would cost several MB/s.
        if cand is not top and top.ms - cand.ms >= max(20.0, 0.20 * top.ms):
            pick = cand
        print(f"\n  within {args.tie:.0f}%, so latency decides: " + ", ".join(
            f"{r.pop} ({r.mbps:.0f} MB/s, {r.ms:.0f} ms)" for r in band))

    cell, cls, nh = pick_cell[pick.pop]
    pinned = top_class_pops(servers_for_cell(cell, geolocate=True))
    print(f"\n-> Pick \"{names.get(cell, '?')}\"  (cell {cell})")
    print(f"   You'll be served by {pick.pop} ({where[pick.pop]}): {pick.mbps:.1f} MB/s "
          f"({pick.mbps * 8.389:.0f} Mbit/s)" + (f", {pick.ms:.0f} ms" if pick.ms else ""))
    print(f"   Steam will use {nh} host(s) from class {cls} in that cell.")
    fastest = results[0]
    if fastest is not pick and not can_pick(fastest.pop):
        print(f"   {fastest.pop} ({where[fastest.pop]}) is faster at "
              f"{fastest.mbps:.1f} MB/s, but no download region selects it.")
    if pinned and pick.pop not in pinned:
        got = ", ".join(sorted(pinned))
        print(f"   Queried the way the Steam client does, this connection is still")
        print(f"   assigned {got} for that region. Confirm the host in")
        print(f"   logs/content_log.txt after changing the download region.")
    if pick is not top:
        print(f"   ({top.pop} is faster at {top.mbps:.1f} MB/s but sits at "
              f"{top.ms:.0f} ms against {pick.ms:.0f} ms)")

    same = sorted({names[c] for c in alts[pick.pop] if c != cell and c in names})
    if same:
        print(f"   {len(same)} other regions reach the same PoP:")
        line = []
        for n in same:
            if line and sum(len(x) + 3 for x in line) + len(n) > 72:
                print("     " + " | ".join(line))
                line = []
            line.append(n)
        if line:
            print("     " + " | ".join(line))

    if args.json:
        Path(args.json).write_text(json.dumps({
            "recommended": {"cell": cell, "region": names.get(cell), "pop": pick.pop},
            "pops": [{
                "pop": r.pop, "located_in": where[r.pop],
                "pick_cell": pick_cell[r.pop][0],
                "pick_region": names.get(pick_cell[r.pop][0]) if can_pick(r.pop) else None,
                "hosts_in_top_class": pick_cell[r.pop][2],
                "mbytes_s": round(r.mbps, 2), "mbit_s": round(r.mbps * 8.389),
                "latency_ms": round(r.ms) if r.ms else None,
                "latency_min_ms": round(r.ms_min) if r.ms_min else None,
                "hit_pct": round(r.hitpct) if r.hitpct is not None else None,
                "stable": r.stable, "rewarmed": r.rewarmed,
            } for r in results],
        }, indent=2, ensure_ascii=False))
        print(f"   JSON -> {args.json}")


# --- gui ---------------------------------------------------------------------

def launch_gui():
    import tkinter as tk
    from tkinter import ttk

    try:
        import ctypes
        ctypes.windll.shcore.SetProcessDpiAwareness(1)
    except Exception:
        pass

    BG = "SystemButtonFace"
    PANEL = "SystemButtonFace"
    RAISED = "SystemWindow"
    FG = "SystemWindowText"
    MUTED = "SystemGrayText"
    LINE = "SystemHighlight"
    LINE_DIM = "#9dc7ef"
    GRID = "#e6e6e6"
    BORDER = "SystemButtonShadow"
    base = Path(getattr(sys, "_MEIPASS", Path(__file__).resolve().parent))
    BANNER = base / "speedy_steam_header.png"

    def scale_banner(path, width, height):
        """Scale the graffiti banner with GDI+, then hand Tk a PNG it can draw."""
        import tempfile
        gdiplus = scale_banner.lib
        image = ctypes.c_void_p()
        if gdiplus.GdipCreateBitmapFromFile(str(path), ctypes.byref(image)) != 0:
            raise OSError(path)
        bitmap = ctypes.c_void_p()
        graphics = ctypes.c_void_p()
        dest = None
        try:
            if gdiplus.GdipCreateBitmapFromScan0(
                    width, height, 0, 0x00021808, None, ctypes.byref(bitmap)) != 0:
                raise OSError("bitmap")
            if gdiplus.GdipGetImageGraphicsContext(bitmap, ctypes.byref(graphics)) != 0:
                raise OSError("graphics")
            gdiplus.GdipSetInterpolationMode(graphics, 7)
            src_w = ctypes.c_uint()
            src_h = ctypes.c_uint()
            if gdiplus.GdipGetImageWidth(image, ctypes.byref(src_w)) != 0:
                raise OSError("width")
            if gdiplus.GdipGetImageHeight(image, ctypes.byref(src_h)) != 0:
                raise OSError("height")
            # Keep the middle half. The top and bottom quarters are the brick and drips.
            crop_h = max(1, src_h.value // 2)
            crop_y = (src_h.value - crop_h) // 2
            if gdiplus.GdipDrawImageRectRectI(
                    graphics, image, 0, 0, width, height,
                    0, crop_y, src_w.value, crop_h,
                    2, None, None, None) != 0:
                raise OSError("draw")
            handle = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
            handle.close()
            dest = handle.name
            if gdiplus.GdipSaveImageToFile(
                    bitmap, dest, ctypes.byref(scale_banner.png), None) != 0:
                raise OSError("save")
            photo = tk.PhotoImage(file=dest)
        finally:
            if graphics:
                gdiplus.GdipDeleteGraphics(graphics)
            if bitmap:
                gdiplus.GdipDisposeImage(bitmap)
            if image:
                gdiplus.GdipDisposeImage(image)
            if dest:
                try:
                    Path(dest).unlink()
                except OSError:
                    pass
        return photo

    def _banner_gdi():
        if getattr(scale_banner, "lib", None) is not None:
            return
        import uuid
        gdiplus = ctypes.WinDLL("gdiplus.dll")

        class Startup(ctypes.Structure):
            _fields_ = [
                ("GdiplusVersion", ctypes.c_uint32),
                ("DebugEventCallback", ctypes.c_void_p),
                ("SuppressBackgroundThread", ctypes.c_int),
                ("SuppressExternalCodecs", ctypes.c_int),
            ]

        class GUID(ctypes.Structure):
            _fields_ = [
                ("Data1", ctypes.c_uint32),
                ("Data2", ctypes.c_uint16),
                ("Data3", ctypes.c_uint16),
                ("Data4", ctypes.c_ubyte * 8),
            ]

        gdiplus.GdiplusStartup.argtypes = [
            ctypes.POINTER(ctypes.c_ulong), ctypes.POINTER(Startup), ctypes.c_void_p]
        gdiplus.GdiplusStartup.restype = ctypes.c_int
        gdiplus.GdipCreateBitmapFromFile.argtypes = [
            ctypes.c_wchar_p, ctypes.POINTER(ctypes.c_void_p)]
        gdiplus.GdipCreateBitmapFromFile.restype = ctypes.c_int
        gdiplus.GdipCreateBitmapFromScan0.argtypes = [
            ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_void_p)]
        gdiplus.GdipCreateBitmapFromScan0.restype = ctypes.c_int
        gdiplus.GdipGetImageGraphicsContext.argtypes = [
            ctypes.c_void_p, ctypes.POINTER(ctypes.c_void_p)]
        gdiplus.GdipGetImageGraphicsContext.restype = ctypes.c_int
        gdiplus.GdipSetInterpolationMode.argtypes = [ctypes.c_void_p, ctypes.c_int]
        gdiplus.GdipGetImageWidth.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint)]
        gdiplus.GdipGetImageWidth.restype = ctypes.c_int
        gdiplus.GdipGetImageHeight.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint)]
        gdiplus.GdipGetImageHeight.restype = ctypes.c_int
        gdiplus.GdipDrawImageRectRectI.argtypes = [
            ctypes.c_void_p, ctypes.c_void_p,
            ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_int,
            ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_int,
            ctypes.c_int, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p]
        gdiplus.GdipDrawImageRectRectI.restype = ctypes.c_int
        gdiplus.GdipSaveImageToFile.argtypes = [
            ctypes.c_void_p, ctypes.c_wchar_p, ctypes.POINTER(GUID), ctypes.c_void_p]
        gdiplus.GdipSaveImageToFile.restype = ctypes.c_int
        gdiplus.GdipDeleteGraphics.argtypes = [ctypes.c_void_p]
        gdiplus.GdipDisposeImage.argtypes = [ctypes.c_void_p]
        token = ctypes.c_ulong()
        if gdiplus.GdiplusStartup(ctypes.byref(token), ctypes.byref(Startup(1, None, 0, 0)), None) != 0:
            raise OSError("gdiplus")
        png = GUID()
        ctypes.memmove(ctypes.byref(png), uuid.UUID("557cf406-1a04-11d3-9a73-0000f81ef32e").bytes_le, 16)
        scale_banner.lib = gdiplus
        scale_banner.png = png

    class Banner(tk.Canvas):
        """Full-width graffiti title. Height follows the artwork as the window changes."""

        def __init__(self, master, path):
            probe = tk.PhotoImage(file=str(path))
            self._ratio = probe.height() / probe.width() / 2
            super().__init__(
                master, bg="#101218", highlightthickness=0, bd=0,
                height=max(1, round(1120 * self._ratio)))
            self._path = path
            self._photo = None
            self._width = 0
            self._pending = None
            self.bind("<Configure>", self._on_configure)

        def _on_configure(self, event):
            width = event.width
            if width < 40 or width == self._width:
                return
            if self._pending is not None:
                self.after_cancel(self._pending)
            self._pending = self.after(30, lambda width=width: self._draw(width))

        def _draw(self, width):
            self._pending = None
            width = max(self.winfo_width(), 40)
            if width == self._width and self._photo is not None:
                return
            height = max(1, round(width * self._ratio))
            self._width = width
            if self.winfo_height() != height:
                self.configure(height=height)
            try:
                _banner_gdi()
                photo = scale_banner(self._path, width, height)
            except Exception:
                photo = tk.PhotoImage(file=str(self._path))
                factor = max(1, int(round(photo.width() / width)))
                if factor > 1:
                    photo = photo.subsample(factor, factor)
            self._photo = photo
            self.delete("all")
            self.create_image(0, 0, image=self._photo, anchor="nw")

    class SpeedPlot(tk.Canvas):
        """Live MB/s trace. Samples arrive at 60 Hz; the stroke is a short rolling average."""

        # Chunk reads arrive in bursts, so the raw rate is a spike train.
        # Averaging the last three quarters of a second is the throughput
        # the line should show, and the pen eases toward that every frame.
        WINDOW = 0.75

        def __init__(self, master):
            super().__init__(master, bg=RAISED, highlightthickness=0, height=220)
            self.points = []
            self.raw = []
            self.ymax = 10.0
            self.series_name = ""
            self.live = 0.0
            self.lock = threading.Lock()
            self.incoming = []
            self.view = None
            self.bind("<Configure>", lambda _e: self.redraw())

        def begin(self, name):
            with self.lock:
                self.incoming.clear()
            self.points.clear()
            self.raw.clear()
            self.live = 0.0
            self.ymax = 10.0
            self.series_name = name
            self.view = None
            self.redraw()

        def capture(self):
            return (
                self.series_name,
                [(t, y) for t, y in self.points],
                self.ymax,
                self.live,
            )

        def show(self, name, points, ymax, live):
            self.view = (name, [(t, y) for t, y in points], max(float(ymax), 1.0), float(live))
            self.redraw()

        def _frame(self):
            if self.view is not None:
                return self.view
            return self.series_name, self.points, self.ymax, self.live

        def push(self, t, mbps):
            with self.lock:
                self.incoming.append((t, mbps if mbps > 0 else 0.0))

        def tick(self):
            with self.lock:
                batch = self.incoming
                self.incoming = []
            self.raw.extend(batch)
            if self.raw:
                latest = self.raw[-1][0]
                cutoff = latest - self.WINDOW
                drop = 0
                for ts, _ in self.raw:
                    if ts >= cutoff:
                        break
                    drop += 1
                if drop:
                    del self.raw[:drop]
                target = sum(v for _, v in self.raw) / len(self.raw)
                self.live += (target - self.live) * 0.22
                if batch:
                    self.points.append((latest, self.live))
                elif self.points:
                    self.points[-1] = (self.points[-1][0], self.live)
            peak = max((y for _, y in self.points), default=1.0)
            target_y = max(8.0, peak * 1.18)
            self.ymax += (target_y - self.ymax) * 0.08
            if self.view is None:
                self.redraw()

        def redraw(self):
            name, points, ymax, live = self._frame()
            w = max(self.winfo_width(), 10)
            h = max(self.winfo_height(), 10)
            if w < 40 or h < 40:
                return
            self.delete("all")
            left, right, top, bottom = 58, 18, 40, 28
            pw, ph = w - left - right, h - top - bottom
            for i in range(5):
                y = top + ph * i / 4
                val = ymax * (1 - i / 4)
                self.create_line(left, y, w - right, y, fill=GRID)
                self.create_text(left - 8, y, text=f"{val:.0f}", anchor="e",
                                 fill=MUTED, font=("Segoe UI", 9))
            if len(points) >= 2:
                tmax = max(points[-1][0], 0.25)
                pts = points
                # One vertex per couple of pixels. Denser than the screen
                # only costs frame time; the samples themselves stay at 60 Hz.
                stride = max(1, len(pts) // 180)
                if stride > 1:
                    pts = pts[::stride]
                    if pts[-1] is not points[-1]:
                        pts = pts + [points[-1]]
                coords = []
                for ts, yv in pts:
                    x = left + pw * (ts / tmax)
                    y = top + ph * (1 - min(yv, ymax) / ymax)
                    coords.extend((x, y))
                self.create_line(*coords, fill=LINE_DIM, width=7, smooth=True,
                                 splinesteps=12, capstyle="round", joinstyle="round")
                self.create_line(*coords, fill=LINE, width=2.5, smooth=True,
                                 splinesteps=12, capstyle="round", joinstyle="round")
                x, y = coords[-2], coords[-1]
                self.create_oval(x - 3.5, y - 3.5, x + 3.5, y + 3.5, fill=LINE, outline=RAISED)
                self.create_text(left, h - 6, text="0s", anchor="sw",
                                 fill=MUTED, font=("Segoe UI", 9))
                self.create_text(w - right, h - 6, text=f"{points[-1][0]:.1f}s",
                                 anchor="se", fill=MUTED, font=("Segoe UI", 9))
            self.create_text(left, 18, anchor="w", text=name or "Waiting",
                             fill=FG, font=("Segoe UI", 12, "bold"))
            self.create_text(w - right, 18, anchor="e", text=f"{live:.1f} MB/s",
                             fill=LINE, font=("Segoe UI", 18, "bold"))
            self.create_text(16, top + ph / 2, text="MB/s", angle=90,
                             fill=MUTED, font=("Segoe UI", 9))

    class FilterChip(tk.Canvas):
        """Rounded region filter. Same size in a grid, so the row doesn't stagger."""

        def __init__(self, master, text, command):
            super().__init__(
                master, height=32, bg=BG, highlightthickness=0, bd=0, cursor="arrow")
            self._text = text
            self._command = command
            self._enabled = False
            self._selected = False
            self._hover = False
            self._pressed = False
            self.bind("<Configure>", lambda _e: self._draw())
            self.bind("<ButtonPress-1>", self._on_press)
            self.bind("<ButtonRelease-1>", self._on_release)
            self.bind("<Enter>", self._on_enter)
            self.bind("<Leave>", self._on_leave)

        def configure(self, cnf=None, **kw):
            if cnf is None and not kw:
                return super().configure()
            if isinstance(cnf, dict):
                kw.update(cnf)
            elif cnf is not None:
                return super().configure(cnf)
            state = kw.pop("state", None)
            if state is not None:
                self._enabled = state == "normal"
                self.configure_cursor()
                self._draw()
            if kw:
                super().configure(**kw)

        def configure_cursor(self):
            super().configure(cursor="hand2" if self._enabled else "arrow")

        def set_selected(self, selected):
            selected = bool(selected)
            if selected == self._selected:
                return
            self._selected = selected
            self._draw()

        def _colors(self):
            if not self._enabled:
                if self._selected:
                    return LINE_DIM, "#7eb0e0", FG
                return BG, BORDER, MUTED
            if self._pressed or self._selected:
                return LINE, LINE, "SystemHighlightText"
            if self._hover:
                return LINE_DIM, "#7eb0e0", FG
            return RAISED, "#c5c5c5", FG

        def _rounded(self, x1, y1, x2, y2, radius, **kwargs):
            radius = min(radius, (x2 - x1) / 2, (y2 - y1) / 2)
            points = [
                x1 + radius, y1, x2 - radius, y1, x2, y1, x2, y1 + radius,
                x2, y2 - radius, x2, y2, x2 - radius, y2, x1 + radius, y2,
                x1, y2, x1, y2 - radius, x1, y1 + radius, x1, y1,
            ]
            return self.create_polygon(points, smooth=True, **kwargs)

        def _draw(self):
            width = self.winfo_width()
            height = self.winfo_height()
            if width < 8 or height < 8:
                return
            fill, outline, text = self._colors()
            self.delete("all")
            self._rounded(1, 1, width - 2, height - 2, 12, fill=outline, outline="")
            self._rounded(2, 2, width - 3, height - 3, 11, fill=fill, outline="")
            self.create_text(
                width / 2, height / 2, text=self._text, fill=text,
                font=("Segoe UI", 9))

        def _on_enter(self, _event):
            self._hover = True
            if self._enabled:
                self._draw()

        def _on_leave(self, _event):
            self._hover = False
            self._pressed = False
            self._draw()

        def _on_press(self, _event):
            if not self._enabled:
                return
            self._pressed = True
            self._draw()

        def _on_release(self, event):
            was_pressed = self._pressed
            self._pressed = False
            self._draw()
            if not self._enabled or not was_pressed:
                return
            if 0 <= event.x <= self.winfo_width() and 0 <= event.y <= self.winfo_height():
                self._command()

    class App:
        def __init__(self, root):
            self.root = root
            root.app = self
            root.title("Speedy Steam")
            root.geometry("1240x800")
            root.minsize(960, 760)
            root.configure(bg=BG)
            root.option_add("*Font", "{Segoe UI} 9")
            root.protocol("WM_DELETE_WINDOW", self._close)
            self.cat = None
            self.results = []
            self.stop_event = threading.Event()
            self.pause_event = threading.Event()
            self._resume_status = ""
            self.testing = False
            self._animating = False
            self._closed = False

            style = ttk.Style()
            for theme in ("vista", "xpnative"):
                if theme in style.theme_names():
                    style.theme_use(theme)
                    break
            style.configure("Treeview", font=("Segoe UI", 9), rowheight=22)
            style.configure("Treeview.Heading", font=("Segoe UI", 9))

            self.picked = set()
            self.marks = {}
            self._syncing = False

            if BANNER.is_file():
                Banner(root, BANNER).pack(fill="x")
            else:
                missing = tk.Frame(root, bg="#101218")
                missing.pack(fill="x")
                tk.Label(missing, text="Speedy Steam", bg="#101218", fg="white",
                         font=("Segoe UI", 22, "bold")).pack(padx=16, pady=18)
            ttk.Separator(root, orient="horizontal").pack(fill="x")

            body = tk.Frame(root, bg=BG)
            body.pack(fill="both", expand=True)

            side = tk.Frame(body, bg=BG, width=340)
            side.pack(side="left", fill="y")
            side.pack_propagate(False)
            ttk.Separator(body, orient="vertical").pack(side="left", fill="y")

            side_pad = ttk.LabelFrame(side, text="Download regions", padding=(10, 8))
            side_pad.pack(fill="both", expand=True, padx=10, pady=10)
            tk.Label(side_pad, text="Check the regions to test.  ↔ is another city's server.",
                     bg=BG, fg=MUTED, font=("Segoe UI", 9), wraplength=290,
                     justify="left").pack(anchor="w", pady=(0, 8))
            self.count_label = tk.Label(side_pad, text="Loading servers…", bg=BG, fg=MUTED,
                                         font=("Segoe UI", 9), anchor="w")
            self.count_label.pack(fill="x", pady=(0, 8))

            chips = tk.Frame(side_pad, bg=BG)
            chips.pack(fill="x", pady=(0, 8))
            chip_specs = (
                ("all", "All", self._select_all),
                ("none", "None", self._select_none),
                ("us", "US", lambda: self._toggle_areas("us")),
                ("eu", "Europe", lambda: self._toggle_areas("eu")),
                ("asia", "Asia", lambda: self._toggle_areas("asia")),
                ("oceania", "Oceania", lambda: self._toggle_areas("oceania")),
            )
            self._chips = {}
            self._tool_buttons = []
            for index, (key, text, cmd) in enumerate(chip_specs):
                chip = FilterChip(chips, text, cmd)
                chip.grid(row=index // 3, column=index % 3, sticky="nsew", padx=3, pady=3)
                self._chips[key] = chip
                self._tool_buttons.append(chip)
            for column in range(3):
                chips.columnconfigure(column, weight=1, uniform="filters")
            self._check_widgets = []

            list_wrap = tk.Frame(side_pad, bg=RAISED, highlightthickness=1,
                                 highlightbackground=BORDER)
            list_wrap.pack(fill="both", expand=True)
            self.list_canvas = tk.Canvas(
                list_wrap, bg=RAISED, highlightthickness=0, bd=0, width=240, height=120)
            list_scroll = tk.Scrollbar(list_wrap, orient="vertical",
                                       command=self.list_canvas.yview)
            self.list_inner = tk.Frame(self.list_canvas, bg=RAISED)
            self.list_inner.bind(
                "<Configure>",
                lambda e: self.list_canvas.configure(scrollregion=self.list_canvas.bbox("all")))
            self.list_canvas.create_window((0, 0), window=self.list_inner, anchor="nw", tags="inner")
            self.list_canvas.configure(yscrollcommand=list_scroll.set)
            list_scroll.pack(side="right", fill="y")
            self.list_canvas.pack(side="left", fill="both", expand=True)
            self.list_canvas.bind("<Configure>",
                                  lambda e: self.list_canvas.itemconfigure("inner", width=e.width))
            self.list_canvas.bind("<MouseWheel>", self._on_wheel)
            self.list_inner.bind("<MouseWheel>", self._on_wheel)

            self.traffic = tk.Label(side_pad, text="", bg=BG, fg=MUTED,
                                     font=("Segoe UI", 9), anchor="w", wraplength=290, justify="left")
            self.traffic.pack(fill="x", pady=(8, 8))
            actions = tk.Frame(side_pad, bg=BG)
            actions.pack(fill="x")
            self.start_btn = ttk.Button(
                actions, text="Start test", command=self.start_test, state="disabled")
            self.start_btn.pack(side="left", fill="x", expand=True)
            self.pause_btn = ttk.Button(
                actions, text="Pause", command=self.toggle_pause, state="disabled", width=8)
            self.pause_btn.pack(side="left", padx=(8, 0))
            self.stop_btn = ttk.Button(
                actions, text="Stop", command=self.stop_test, state="disabled")
            self.stop_btn.pack(side="left", padx=(8, 0))

            main = tk.Frame(body, bg=BG)
            main.pack(side="left", fill="both", expand=True)
            main_pad = tk.Frame(main, bg=BG)
            main_pad.pack(fill="both", expand=True, padx=12, pady=10)

            plot_card = ttk.LabelFrame(main_pad, text="Throughput", padding=(8, 6))
            plot_card.pack(fill="both", expand=True, pady=(0, 8))
            self.status = tk.Label(plot_card, text="Looking up download servers…",
                                   bg=BG, fg=MUTED, font=("Segoe UI", 9), anchor="w")
            self.status.pack(fill="x")
            self.plot = SpeedPlot(plot_card)
            self.plot.pack(fill="both", expand=True, pady=(6, 0))

            self.recommend_card = ttk.LabelFrame(main_pad, text="Recommendation", padding=(8, 6))
            self.recommend_card.pack(fill="x", pady=(0, 8))
            self.recommend = tk.Label(
                self.recommend_card, text="Run a test to compare servers.",
                bg=BG, fg=FG, anchor="w", justify="left",
                font=("Segoe UI", 11), wraplength=640)
            self.recommend.pack(fill="x")

            table_card = ttk.LabelFrame(main_pad, text="Results", padding=(8, 6))
            table_card.pack(fill="both", expand=True)
            cols = ("pop", "where", "mbps", "avg", "mbit", "ms", "region")
            self.tree = ttk.Treeview(table_card, columns=cols, show="headings", height=8)
            for key, title, width, anchor in (
                ("pop", "Server", 110, "w"), ("where", "Location", 150, "w"),
                ("mbps", "MB/s", 72, "e"), ("avg", "Average MB/s", 108, "e"),
                ("mbit", "Mbit/s", 80, "e"),
                ("ms", "Latency", 72, "e"), ("region", "Pick in Steam", 220, "w"),
            ):
                if key in ("mbps", "avg", "mbit", "ms"):
                    self.tree.heading(key, text=title, command=lambda k=key: self._sort_results(k))
                else:
                    self.tree.heading(key, text=title)
                self.tree.column(key, width=width, minwidth=width, anchor=anchor, stretch=False)
            tree_y = tk.Scrollbar(table_card, orient="vertical", command=self.tree.yview)
            self.tree_x = tk.Scrollbar(table_card, orient="horizontal", command=self.tree.xview)
            self.tree.configure(yscrollcommand=tree_y.set, xscrollcommand=self.tree_x.set)
            self.tree.grid(row=0, column=0, sticky="nsew", padx=(1, 0), pady=(1, 0))
            tree_y.grid(row=0, column=1, sticky="ns", pady=(1, 0))
            self.tree_x.grid(row=1, column=0, sticky="ew", padx=(1, 0))
            self.tree.bind("<Configure>", self._fit_results_scroll, add="+")
            table_card.rowconfigure(0, weight=1)
            table_card.columnconfigure(0, weight=1)
            self.tree.bind("<MouseWheel>", self._on_tree_wheel)
            self.tree.bind("<Shift-MouseWheel>", self._on_tree_wheel_x)
            self.tree.bind("<<TreeviewSelect>>", self._show_result_graph)
            self._filling_results = False
            self._sort_key = "mbps"
            self._sort_desc = True
            self._sort_titles = {
                "mbps": "MB/s", "avg": "Average MB/s", "mbit": "Mbit/s", "ms": "Latency",
            }
            self._refresh_sort_headings()

            self.loading = None
            self.load_msg = None
            self.load_bar = None
            self.load_pct = None
            self._load_anim = False
            self._want_loading = True
            self._load_text = "Scanning cells…"
            self._load_target = 0.0
            self._load_shown = 0.0
            self.root.after(0, self._open_loading)
            threading.Thread(target=self._load_catalog, daemon=True).start()

        def _open_loading(self):
            if not self._want_loading or self._closed or self.loading is not None:
                return
            win = tk.Toplevel(self.root)
            win.title("Loading servers")
            win.configure(bg=BG)
            win.transient(self.root)
            win.resizable(False, False)
            win.protocol("WM_DELETE_WINDOW", lambda: None)
            pad = tk.Frame(win, bg=BG)
            pad.pack(padx=22, pady=16)
            tk.Label(pad, text="Finding download servers", bg=BG, fg=FG,
                     font=("Segoe UI", 12)).pack(anchor="w")
            self.load_msg = tk.Label(
                pad, text=self._load_text, bg=BG, fg=MUTED,
                font=("Segoe UI", 9), wraplength=360, justify="left", anchor="w")
            self.load_msg.pack(anchor="w", pady=(6, 12))
            row = tk.Frame(pad, bg=BG)
            row.pack(anchor="w", fill="x")
            self.load_bar = ttk.Progressbar(row, length=280, mode="determinate", maximum=100)
            self.load_bar.pack(side="left")
            self.load_pct = tk.Label(row, text="0%", bg=BG, fg=FG,
                                     font=("Segoe UI", 9), width=5, anchor="e")
            self.load_pct.pack(side="left", padx=(10, 0))
            self.loading = win
            self._load_anim = True
            self.root.update_idletasks()
            win.update_idletasks()
            ww, wh = win.winfo_reqwidth(), win.winfo_reqheight()
            pw, ph = self.root.winfo_width(), self.root.winfo_height()
            px, py = self.root.winfo_rootx(), self.root.winfo_rooty()
            if pw < 100:
                px = (win.winfo_screenwidth() - ww) // 2
                py = (win.winfo_screenheight() - wh) // 2
            else:
                px = px + max(0, (pw - ww) // 2)
                py = py + max(0, (ph - wh) // 2)
            win.geometry(f"{ww}x{wh}+{px}+{py}")
            win.grab_set()
            win.lift()
            self._animate_load()

        def _animate_load(self):
            if not self._load_anim or self.loading is None or self._closed:
                return
            bar = self.load_bar
            if bar is None:
                return
            self._load_shown += (self._load_target - self._load_shown) * 0.14
            if abs(self._load_target - self._load_shown) < 0.001:
                self._load_shown = self._load_target
            shown = max(0.0, min(1.0, self._load_shown))
            bar["value"] = shown * 100
            if self.load_pct is not None:
                self.load_pct.configure(text=f"{round(shown * 100)}%")
            self.root.after(16, self._animate_load)

        def _set_load_status(self, msg, frac=None):
            self._load_text = msg
            if frac is not None:
                self._load_target = max(self._load_target, float(frac))
            self.status.configure(text=msg)
            if self.load_msg is not None:
                self.load_msg.configure(text=msg)

        def _close_loading(self):
            self._want_loading = False
            self._load_anim = False
            win = self.loading
            self.loading = None
            self.load_msg = None
            self.load_bar = None
            self.load_pct = None
            if win is None:
                return
            try:
                win.grab_release()
            except tk.TclError:
                pass
            try:
                win.destroy()
            except tk.TclError:
                pass

        def _on_wheel(self, event):
            self.list_canvas.yview_scroll(int(-event.delta / 120), "units")

        def _on_tree_wheel(self, event):
            self.tree.yview_scroll(int(-event.delta / 120), "units")
            return "break"

        def _on_tree_wheel_x(self, event):
            self.tree.xview_scroll(int(-event.delta / 120), "units")
            return "break"

        def _fit_results_scroll(self, _event=None):
            fits = self.tree.xview()[1] >= 0.999
            shown = bool(self.tree_x.winfo_ismapped())
            if fits and shown:
                self.tree_x.grid_remove()
            elif not fits and not shown:
                self.tree_x.grid()

        def _ordered(self):
            return list(self.cat["locations"])

        def _selected(self):
            return set(self.picked)

        def _sync_count(self):
            if not self.cat:
                return
            selected = self._selected()
            self.count_label.configure(text=f"{len(selected)} selected")
            hosts = {self.cat["loc_host"][cell] for cell in selected if self.cat["loc_host"].get(cell)}
            gb = len(hosts) * 400 / 1024
            self.traffic.configure(
                text=f"Up to {gb:.1f} GB across {len(hosts)} servers. "
                     "Each one stops once its rate settles.")
            self._sync_chips()

        def _paint(self, pop):
            var = self.marks.get(pop)
            if var is None:
                return
            self._syncing = True
            var.set(pop in self._selected())
            self._syncing = False

        def _click_server(self, pop):
            if self.testing or self._syncing:
                return
            if pop in self.picked:
                self.picked.discard(pop)
            else:
                self.picked.add(pop)
            self._paint(pop)
            self._sync_count()

        def _cells_in(self, *areas):
            wanted = set(areas)
            return {cell for cell in self.cat["locations"]
                    if region_area(self.cat["row_loc"].get(cell, "")) in wanted}

        def _sync_chips(self):
            if not self.cat or not getattr(self, "_chips", None):
                return
            locations = set(self.cat["locations"])

            def covers(*areas):
                cells = self._cells_in(*areas)
                return bool(cells) and cells <= self.picked

            self._chips["all"].set_selected(bool(locations) and locations <= self.picked)
            self._chips["none"].set_selected(not self.picked)
            self._chips["us"].set_selected(covers("us"))
            self._chips["eu"].set_selected(covers("eu"))
            self._chips["asia"].set_selected(covers("asia"))
            self._chips["oceania"].set_selected(covers("oceania"))

        def _select_all(self):
            if not self.cat or self.testing:
                return
            locations = set(self.cat["locations"])
            if locations and locations <= self.picked:
                self.picked.clear()
            else:
                self.picked = set(locations)
            self._apply_checks()
            self._sync_count()

        def _select_none(self):
            if not self.cat or self.testing:
                return
            self.picked.clear()
            self._apply_checks()
            self._sync_count()

        def _toggle_areas(self, *areas):
            if not self.cat or self.testing:
                return
            cells = self._cells_in(*areas)
            if cells and cells <= self.picked:
                self.picked -= cells
            else:
                self.picked |= cells
            self._apply_checks()
            self._sync_count()

        def _apply_checks(self):
            for pop in self.marks:
                self._paint(pop)

        def _set_list_state(self, state):
            for widget in self._tool_buttons:
                widget.configure(state=state)
            for widget in self._check_widgets:
                widget.configure(state=state)

        def _rebuild_list(self):
            if not self.cat:
                return
            for child in self.list_inner.winfo_children():
                child.destroy()
            self.marks = {}
            self._check_widgets = []
            for cell in self._ordered():
                region = self.cat["row_label"].get(cell) or self.cat["names"].get(cell, f"Cell {cell}")
                pop = self.cat["loc_pop"][cell] or "—"
                if self.cat.get("mirrors", {}).get(cell) and pop != "—":
                    pop = "↔ " + pop
                row = tk.Frame(self.list_inner, bg=RAISED)
                row.pack(fill="x", pady=1)
                var = tk.BooleanVar(value=cell in self._selected())
                self.marks[cell] = var
                cb = tk.Checkbutton(
                    row, variable=var, command=lambda c=cell: self._click_server(c),
                    bg=RAISED, activebackground=RAISED, highlightthickness=0, bd=0)
                cb.pack(side="left")
                self._check_widgets.append(cb)
                name = tk.Label(row, text=region, bg=RAISED, fg=FG,
                                font=("Segoe UI", 9), anchor="w")
                name.pack(side="left")
                name.bind("<Button-1>", lambda _e, widget=cb: widget.invoke())
                place = tk.Label(row, text=pop, bg=RAISED, fg=FG,
                                 font=("Segoe UI", 9), anchor="e")
                place.pack(side="right", padx=(8, 4))

                def bind_wheel(widget):
                    widget.bind("<MouseWheel>", self._on_wheel)
                    for child in widget.winfo_children():
                        bind_wheel(child)
                bind_wheel(row)

        def _load_catalog(self):
            try:
                root = steam_root()
                names, locs = load_cell_names(root)
                cat = prepare_catalog(root, names, locs=locs, on_status=lambda m, f=None: self.ui(
                    lambda m=m, f=f: self._set_load_status(m, f)))
            except (RuntimeError, SystemExit) as e:
                msg = str(e) or "Couldn't prepare the test."
                self.ui(lambda: (self._close_loading(), self.status.configure(text=msg)))
                return
            except Exception as e:
                msg = f"Couldn't prepare the test: {e}"
                self.ui(lambda: (self._close_loading(), self.status.configure(text=msg)))
                return

            def ready():
                if self._closed:
                    return
                self._close_loading()
                self.cat = cat
                n = len(cat["locations"])
                servers = len({p for p in cat["loc_pop"].values() if p})
                self.picked = set(cat["locations"])
                self._set_list_state("normal")
                self._rebuild_list()
                self.start_btn.configure(state="normal")
                self.status.configure(
                    text=f"{n} download regions  ·  {servers} servers  ·  depot {cat['depot']}")
                self._sync_count()
            self.ui(ready)

        def ui(self, fn):
            if not self._closed:
                self.root.after(0, fn)

        def start_test(self):
            if not self.cat or self.testing:
                return
            if not self._selected():
                self.status.configure(text="Choose at least one server.")
                return
            self.testing = True
            self.stop_event.clear()
            self.pause_event.clear()
            self._resume_status = ""
            self.results.clear()
            self.tree.delete(*self.tree.get_children())
            self.recommend.configure(text="Measuring…")
            self.plot.begin("Starting")
            self.start_btn.configure(state="disabled")
            self.pause_btn.configure(text="Pause", state="normal")
            self.stop_btn.configure(state="normal")
            self._set_list_state("disabled")
            self._animating = True
            self._anim()
            threading.Thread(target=self._run, daemon=True).start()

        def stop_test(self):
            self.stop_event.set()
            self.pause_event.clear()
            self.pause_btn.configure(text="Pause", state="disabled")
            self.status.configure(text="Stopping…")

        def toggle_pause(self):
            if not self.testing:
                return
            if self.pause_event.is_set():
                self.pause_event.clear()
                self.pause_btn.configure(text="Pause")
                if self._resume_status:
                    self.status.configure(text=self._resume_status)
            else:
                self.pause_event.set()
                self.pause_btn.configure(text="Resume")
                self.status.configure(text="Paused.")

        def _set_status(self, text):
            self._resume_status = text
            if not self.pause_event.is_set():
                self.status.configure(text=text)

        def _anim(self):
            if not self._animating or self._closed:
                return
            if not self.pause_event.is_set():
                self.plot.tick()
            self.root.after(16, self._anim)

        def _run(self):
            cat = self.cat
            chosen = self._selected()
            selected = [c for c in self._ordered() if c in chosen]
            groups = []
            index = {}
            for cell in selected:
                host = cat["loc_host"].get(cell)
                if not host:
                    continue
                if host not in index:
                    index[host] = len(groups)
                    groups.append((host, cat["loc_pop"][cell], []))
                groups[index[host]][2].append(cell)

            class Opts:
                budget = 400
                conns = 16
                min_samples = 6
                max_secs = 20.0
                min_hit = 70.0

            for i, (host, group_pop, cells) in enumerate(groups, 1):
                if not wait_if_paused(self.pause_event, self.stop_event):
                    break
                if self.stop_event.is_set():
                    break
                cell = self._pick_cell_for(group_pop, cells)
                pop = cat["loc_pop"].get(cell, group_pop)
                region = cat["row_label"].get(cell) or cat["names"].get(cell, pop)
                shown = f"↔ {pop}" if cat.get("mirrors", {}).get(cell) else pop
                self.ui(lambda i=i, region=region, shown=shown: self._set_status(
                    f"Testing {region}  ({shown})  {i}/{len(groups)}"))
                ms, ms_min = tcp_latency(
                    host, pause_event=self.pause_event, stop_event=self.stop_event)
                if not wait_if_paused(self.pause_event, self.stop_event):
                    break
                if self.stop_event.is_set():
                    break

                def on_begin(rewarm, pop=pop, region=region):
                    title = f"{'Remeasuring ' if rewarm else ''}{region}  ·  {shown}"
                    self.ui(lambda title=title: self.plot.begin(title))

                r = measure_hot(pop, host, cat["depot"], cat["chunks"], Opts,
                                on_rate=self.plot.push, stop_event=self.stop_event,
                                on_begin=on_begin, pause_event=self.pause_event)
                r.ms, r.ms_min = ms, ms_min
                copy = Result(pop)
                copy.__dict__.update(vars(r))
                copy.pop = pop
                copy.cell = cell
                posted = threading.Event()

                def publish(copy=copy, posted=posted):
                    if not self._closed:
                        self.plot.tick()
                        copy.graph = self.plot.capture()
                        self._add_results([copy])
                    posted.set()

                if self._closed:
                    return
                self.ui(publish)
                while not posted.wait(0.05):
                    if self._closed:
                        return

            self.ui(self._finish)

        def _pick_cell_for(self, pop, cells):
            """Region to show for this server: its own home, if that was selected."""
            home = self.cat["home"].get(pop)
            if home in cells:
                return home
            return cells[0]

        def _sort_value(self, item):
            if self._sort_key == "avg":
                return item.avg_mbps
            if self._sort_key == "ms":
                return item.ms
            if self._sort_key == "mbit":
                return item.mbps * 8.389
            return item.mbps

        def _sort_results(self, key):
            if key == self._sort_key:
                self._sort_desc = not self._sort_desc
            else:
                self._sort_key = key
                self._sort_desc = key != "ms"
            self._refresh_sort_headings()
            if self.results:
                self._add_results([])

        def _refresh_sort_headings(self):
            for key, title in self._sort_titles.items():
                mark = ""
                if key == self._sort_key:
                    mark = "  ▼" if self._sort_desc else "  ▲"
                self.tree.heading(key, text=title + mark, command=lambda k=key: self._sort_results(k))

        def _add_results(self, rows):
            self.results.extend(rows)
            self.results.sort(key=lambda item: (
                self._sort_value(item) is None,
                -(self._sort_value(item) or 0) if self._sort_desc else (self._sort_value(item) or 0),
            ))
            selected = self.tree.selection()
            self._filling_results = True
            self.tree.delete(*self.tree.get_children())
            cat = self.cat
            for item in self.results:
                cell = getattr(item, "cell", None)
                if cell is None or cell not in cat["names"]:
                    cell = cat["pick_cell"][item.pop][0]
                region = cat["row_label"].get(cell) or cat["names"].get(cell) or region_label(cell, cat["names"])
                server = item.pop
                if cat.get("mirrors", {}).get(cell):
                    server = "↔ " + server
                ms = f"{item.ms:.0f}" if item.ms is not None else "—"
                avg = f"{item.avg_mbps:.1f}" if item.avg_mbps is not None else "—"
                self.tree.insert("", "end", iid=str(id(item)), values=(
                    server, region, f"{item.mbps:.1f}", avg,
                    f"{item.mbps * 8.389:.0f}", ms, region,
                ))
            self._filling_results = False
            if selected and self.tree.exists(selected[0]):
                self.tree.selection_set(selected[0])
                self.tree.see(selected[0])

        def _show_result_graph(self, _event=None):
            if self._filling_results:
                return
            selected = self.tree.selection()
            if not selected:
                return
            iid = selected[0]
            for item in self.results:
                if str(id(item)) == iid and getattr(item, "graph", None):
                    self.plot.show(*item.graph)
                    return

        def _finish(self):
            self._animating = False
            self.testing = False
            self.plot.tick()
            if self.cat:
                self._set_list_state("normal")
                self.start_btn.configure(state="normal")
            self.pause_btn.configure(text="Pause", state="disabled")
            self.pause_event.clear()
            self.stop_btn.configure(state="disabled")
            if not self.results:
                if self.stop_event.is_set():
                    self.status.configure(text="Stopped before any server finished.")
                else:
                    self.status.configure(text="No selected region has a server at that location.")
                return
            cat = self.cat
            choose = list(self.results)
            top = choose[0]
            pick = top
            band = [r for r in choose if r.mbps >= top.mbps * 0.9]
            if len(band) > 1 and top.ms is not None:
                cand = min((r for r in band if r.ms is not None), key=lambda r: r.ms)
                if cand is not top and top.ms - cand.ms >= max(20.0, 0.20 * top.ms):
                    pick = cand
            cell = getattr(pick, "cell", None)
            if cell is None:
                cell = cat["pick_cell"][pick.pop][0]
            region = cat["row_label"].get(cell) or cat["names"].get(cell) or region_label(cell, cat["names"])
            server = f"↔ {pick.pop}" if cat.get("mirrors", {}).get(cell) else pick.pop
            text = f"Pick {region}  ·  {server}  ·  {pick.mbps:.1f} MB/s"
            if pick.ms is not None:
                text += f"  ·  {pick.ms:.0f} ms"
            self.recommend.configure(text=text)
            done = "Stopped." if self.stop_event.is_set() else "Done."
            self.status.configure(text=f"{done}  {len(self.results)} servers measured.")

        def _close(self):
            self._closed = True
            self._animating = False
            self._load_anim = False
            self.stop_event.set()
            self.pause_event.clear()
            self.root.destroy()

    root = tk.Tk()
    App(root)
    root.mainloop()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        if "--cli" in sys.argv:
            sys.argv.remove("--cli")
        try:
            main()
        except KeyboardInterrupt:
            print("\nInterrupted.")
            sys.exit(130)
    else:
        launch_gui()