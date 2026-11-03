"""Generate Apache Combined access logs, one file per day -> output/web_logs/access_YYYY-MM-DD.log
  python gen_web_logs.py backfill | python gen_web_logs.py daily [DATE]
Timestamps are Argentina local time (-0300), same wall-clock as orders.created_at in Postgres.
Also writes output/ip_geo_lookup.csv (first octet -> country). IPs whose first octet is NOT in it (and IPv6) = unknown location.
Logged-in users have auth_user = customer email; every web order has a session ending in POST /checkout/confirm.
HARD_MODE=1 (default) adds: unknown IPs/IPv6, HEAD/OPTIONS/PUT/DELETE, 301s, HTTP/2, scanner probes, odd/empty user-agents,
uptime monitor, some upper-case logins, corrupted + duplicated lines.
"""
import os, random
from datetime import datetime, time, timedelta
from common import *

BASE = "https://shop.b2bmarket.example"
SESSIONS_PER_ORDER = 7  # browse-only sessions per order; sets web conversion (~7%), raise for lower conversion / more lines
COUNTRY_IP = {"AR": [181, 186, 190, 200], "BR": [177, 189], "CL": [152, 146], "UY": [167], "US": [3, 52, 104],
              "ES": [88, 90], "MX": [148, 187], "DE": [46, 78], "FR": [51], "IN": [49], "CN": [36]}
UNKNOWN_OCTETS = [5, 23, 45, 62, 91, 103, 185, 212]  # not in ip_geo_lookup.csv
WEB_COUNTRY_W = {"AR": 50, "BR": 12, "CL": 8, "UY": 5, "US": 14, "ES": 6, "MX": 5}
DEVICE_UA = {
    "desktop": ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"],
    "mobile": ["Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1",
               "Mozilla/5.0 (Linux; Android 13; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
               "Mozilla/5.0 (Linux; Android 12; Pixel 6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Mobile Safari/537.36"],
    "tablet": ["Mozilla/5.0 (iPad; CPU OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1",
               "Mozilla/5.0 (Linux; Android 13; SM-X700) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"],
}
BOT_UA = ["Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
          "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
          "Mozilla/5.0 (compatible; AhrefsBot/7.0; +http://ahrefs.com/robot/)", "curl/7.88.1", "python-requests/2.31.0"]
ODD_UA = ["-", "-", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)", "Dalvik/2.1.0 (Linux; U; Android 10; SM-A105F)",
          "okhttp/4.9.3", "PostmanRuntime/7.32.3", "Java/11.0.2", "Go-http-client/1.1"]
SCANNER_UA = ["-", "zgrab/0.x", "masscan/1.3 (https://github.com/robertdavidgraham/masscan)", "Go-http-client/1.1",
              "python-requests/2.28.0", "Nmap Scripting Engine; https://nmap.org/book/nse.html"]
PROBES = ["/wp-login.php", "/xmlrpc.php", "/.env", "/.git/config", "/phpmyadmin/", "/admin.php", "/wp-admin/",
          "/cgi-bin/test.cgi", "/server-status"]
MONITOR_UA, MONITOR_IP = "UptimeRobot/2.0 (+http://www.uptimerobot.com/)", "52.14.20.7"
EXTERNAL_REFS = ["-", "-", "-", "https://www.google.com/", "https://www.google.com/", "https://www.linkedin.com/",
                 "https://www.bing.com/", "https://mail.google.com/"]
TERMS = ["box", "tape", "chair", "gloves", "paper", "monitor", "helmet", "cleaner", "drill", "kettle"]
# (path prefix, P(4xx), P(5xx)) - first match wins; default is 0.5% / 0.3%
ERRORS = [("/admin", .9, 0), ("/login", .08, .002), ("/api/orders", .01, .05), ("/checkout", .01, .04),
          ("/api/search", .02, .03), ("/products/", .04, .005)]


def status_for(rng, method, path):
    if path.startswith(tuple(PROBES)):  # scanners: mostly 404
        return rng.choices([404, 403, 400, 301], [70, 20, 5, 5])[0]
    if method in ("OPTIONS", "DELETE"):
        return 204
    if path.startswith(("/index.html", "/product/")):  # old URLs redirect
        return 301
    p4, p5 = .005, .003
    for prefix, a, b in ERRORS:
        if path.startswith(prefix):
            p4, p5 = a, b
            break
    r = rng.random()
    if r < p5:
        return rng.choice([500, 502, 503])
    if r < p5 + p4:
        return 403 if path.startswith("/admin") else 401 if path.startswith("/login") else rng.choice([400, 404, 404, 429])
    if path.startswith(("/static", "/images")) and rng.random() < .3:
        return 304
    return 302 if method == "POST" and path.startswith("/login") else 200


def emit(rows, rng, t, ip, user, method, path, ua, referer, force=None, proto="HTTP/1.1"):
    status = force or status_for(rng, method, path)
    if status in (304, 204) or method == "HEAD":
        size = 0
    elif status in (301, 302):
        size = rng.randint(180, 600)
    elif status >= 400:
        size = rng.randint(200, 1500)
    elif path.startswith(("/static", "/images")):
        size = rng.randint(10_000, 300_000)
    else:
        size = rng.randint(200, 5000) if path.startswith("/api") else rng.randint(5_000, 60_000)
    rows.append((t, f'{ip} - {user} [{t:%d/%b/%Y:%H:%M:%S} -0300] "{method} {path} {proto}" {status} {size or "-"} "{referer}" "{ua}"'))


def session(rows, rng, ip, ua, user, steps, start=None, end=None, bot=False, force_last=False, proto=None):
    gaps = [rng.randint(1, 3) if bot else rng.randint(15, 90) for _ in steps[1:]]
    t = end - timedelta(seconds=sum(gaps)) if end else start
    sid = f"{rng.getrandbits(48):012x}"
    proto = proto or (rng.choice(["HTTP/1.1", "HTTP/2.0", "HTTP/2.0"]) if HARD_MODE and not bot else "HTTP/1.1")
    referer = "-" if bot else rng.choice(EXTERNAL_REFS)
    for i, (method, path) in enumerate(steps):
        if i:
            t += timedelta(seconds=gaps[i - 1])
        full = path if bot else path + ("&" if "?" in path else "?") + "sid=" + sid
        emit(rows, rng, t, ip, user, method, full, ua, referer, 200 if force_last and i == len(steps) - 1 else None, proto)
        referer = "-" if bot else BASE + path
        if not bot and method == "GET" and rng.random() < .3:  # static asset hit
            emit(rows, rng, t, ip, user, "GET", rng.choice(["/static/app.js", "/static/style.css", "/images/logo.png"]), ua, referer, None, proto)


def make_ip(rng, country, company_id=None):
    if HARD_MODE and company_id and rng.random() < .6:  # employees of one company share an office IP
        r = random.Random(f"{SEED}-ip-{company_id}")
        return f"{r.choice(COUNTRY_IP[country])}.{r.randint(0, 255)}.{r.randint(0, 255)}.{r.randint(1, 254)}"
    return f"{rng.choice(COUNTRY_IP[country])}.{rng.randint(0, 255)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}"


def odd_ip(rng):  # location can't be found in the lookup table: unknown range or IPv6
    if rng.random() < .25:
        return f"2800:{rng.randint(0, 65535):x}:{rng.randint(0, 65535):x}::{rng.randint(1, 9999):x}"
    return f"{rng.choice(UNKNOWN_OCTETS)}.{rng.randint(0, 255)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}"


def browse_steps(rng, skus, logged_in):
    camp = rng.choice(CAMPAIGNS)
    landing = rng.choice(["/", "/products", "/products/category/" + slug(rng.choice(list(CATEGORIES)))])
    if not logged_in and rng.random() < .2:  # arrived from a campaign
        landing = f"/?utm_source={slug(camp[1])}&utm_medium={camp[2]}&utm_campaign={slug(camp[0])}"
    steps = [("GET", landing)] + ([("POST", "/login")] if logged_in else [])
    for _ in range(rng.choice([0, 0, 0, 1, 2, 3, 4])):  # many visitors bounce after the landing page
        steps.append(("GET", rng.choice(["/products/" + rng.choice(skus), "/products/" + rng.choice(skus),
                                          "/search?q=" + rng.choice(TERMS), "/api/search?q=" + rng.choice(TERMS),
                                          "/products", "/about"])))
    if logged_in and rng.random() < .4:
        steps += [("GET", "/account"), ("GET", "/api/orders")]
    if HARD_MODE:
        if rng.random() < .05:  # old URL -> 301
            steps.append(("GET", rng.choice(["/index.html", "/product/" + rng.choice(skus)])))
        if rng.random() < .03:  # CORS preflight
            steps.append(("OPTIONS", "/api/search"))
        if logged_in and rng.random() < .1:  # edit cart
            steps += [("PUT", "/api/cart"), ("DELETE", "/api/cart/item/1")]
    if rng.random() < .1:
        steps.append(("GET", "/cart"))  # abandoned cart
    if rng.random() < .01:
        steps.append(("GET", "/admin"))
    return steps


def login_name(rng, c):  # same person, sometimes typed in upper case (normalise with lower(trim()))
    return c["login"].upper() if HARD_MODE and rng.random() < .01 else c["login"]


def build_day(day):
    ref = build_reference()
    rng = random.Random(f"{SEED}-web-{day}")
    orders = build_orders(day)
    skus = [p["sku"] for p in ref["products"]]
    sku_of = {p["product_id"]: p["sku"] for p in ref["products"]}
    cust = {c["customer_id"]: c for c in ref["customers"]}
    iso = lambda company_id: COUNTRIES[ref["company_by_id"][company_id]["country"]]
    active = [c for c in ref["customers"] if c["created_at"].date() <= day]
    rows = []

    for o in (o for o in orders if o["channel"] == "web"):  # converting sessions: end with POST /checkout/confirm
        c = cust[o["customer_id"]]
        steps = ([("GET", "/"), ("POST", "/login"), ("GET", "/products")]
                 + [("GET", "/products/" + sku_of[i["product_id"]]) for i in o["items"]]
                 + [("POST", "/api/cart"), ("GET", "/cart"), ("GET", "/checkout"), ("POST", "/checkout"),
                    ("POST", "/checkout/confirm")])
        session(rows, rng, make_ip(rng, iso(o["buyer_company_id"]), o["buyer_company_id"]), rng.choice(DEVICE_UA[o["device"]]),
                login_name(rng, c), steps, end=o["created_at"], force_last=True)

    for _ in range(round(len(orders) * SESSIONS_PER_ORDER * rng.uniform(.8, 1.2))):  # non-converting traffic
        start = rand_ts(rng, day)
        if rng.random() < .04:  # bots: search engines from US, others from several countries
            ua = rng.choice(BOT_UA)
            search = "Googlebot" in ua or "bingbot" in ua
            ip = odd_ip(rng) if HARD_MODE and not search and rng.random() < .2 else make_ip(rng, "US" if search else rng.choice(["US", "DE", "FR", "IN", "CN"]))
            steps = [("GET", "/robots.txt")] + [("GET", "/products/" + rng.choice(skus)) for _ in range(rng.randint(5, 30))]
            session(rows, rng, ip, ua, "-", steps, start=start, bot=True)
            continue
        user, country, c = "-", rng.choices(list(WEB_COUNTRY_W), list(WEB_COUNTRY_W.values()))[0], None
        if active and rng.random() < .3:  # logged-in visitor who doesn't buy
            c = rng.choice(active)
            user, country = login_name(rng, c), iso(c["company_id"])
        device = rng.choices(list(DEVICE_UA), [45, 45, 10])[0]  # browsers skew mobile, buyers skew desktop
        ua, ip = rng.choice(DEVICE_UA[device]), make_ip(rng, country, c["company_id"] if c else None)
        if HARD_MODE and not c:  # anonymous visitors: some unknown IPs, some odd / empty user-agents
            ip = odd_ip(rng) if rng.random() < .04 else ip
            ua = rng.choice(ODD_UA) if rng.random() < .03 else ua
        session(rows, rng, ip, ua, user, browse_steps(rng, skus, user != "-"), start=start)

    if HARD_MODE:
        for _ in range(rng.randint(6, 15)):  # vulnerability scanners
            ip = odd_ip(rng) if rng.random() < .4 else make_ip(rng, rng.choice(["CN", "IN", "DE", "FR", "US"]))
            steps = [(rng.choice(["GET", "GET", "POST", "HEAD", "OPTIONS"]), rng.choice(PROBES)) for _ in range(rng.randint(3, 10))]
            session(rows, rng, ip, rng.choice(SCANNER_UA), "-", steps, start=rand_ts(rng, day), bot=True,
                    proto=rng.choice(["HTTP/1.0", "HTTP/1.1"]))
        for m in range(0, 24 * 60, 30):  # uptime monitor every 30 min
            emit(rows, rng, datetime.combine(day, time(m // 60, m % 60, rng.randint(0, 59))), MONITOR_IP, "-", "HEAD",
                 "/health", MONITOR_UA, "-", 200)

    rows.sort(key=lambda r: r[0])
    now, lines = utc_now(), []
    for t, line in rows:
        if t > now:
            continue
        r = rng.random()
        if r < .004:  # corrupted line: truncated / empty / garbage / broken quotes / junk suffix
            line = rng.choice([line[:rng.randint(10, len(line) - 1)], "", "-- GARBAGE --",
                               line.replace('"', ""), line + " |EXTRA|"])
        lines.append(line)
        if r > .996:  # duplicated line
            lines.append(line)
    return lines


mode, days = days_from_cli()
os.makedirs(f"{OUT_DIR}/web_logs", exist_ok=True)
with open(f"{OUT_DIR}/ip_geo_lookup.csv", "w") as f:
    f.write("first_octet,country_code\n" + "".join(f"{o},{c}\n" for c, octs in COUNTRY_IP.items() for o in octs))
total = 0
for day in days:
    lines = build_day(day)
    with open(f"{OUT_DIR}/web_logs/access_{day}.log", "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + ("\n" if lines else ""))
    total += len(lines)
print(f"{mode}: {len(days)} file(s), {total} log lines (HARD_MODE={int(HARD_MODE)})")