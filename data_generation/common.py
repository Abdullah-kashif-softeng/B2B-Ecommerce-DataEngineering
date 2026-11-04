"""Shared, deterministic reference data + order/lead builders for the 3 generators."""
import math, os, random, re, sys
from datetime import date, datetime, time, timedelta, timezone
from functools import lru_cache

SEED = 42
START_DATE = date(2024, 10, 29)  # backfill starts here (~2 years of history)
OUT_DIR = "output"
N_SUPPLIERS, N_BUYERS = 25, 400
# HARD_MODE=1 (default) injects wrong rows for the pipeline to catch; HARD_MODE=0 gives the clean version.
# Identity keys (customer email/id, company id, order timestamp) are never corrupted, so web user = order placer = lead contact.
HARD_MODE = os.getenv("HARD_MODE", "1") != "0"
BAD_X = 2.1  # multiplies the share of orders/items that get a bad value (fewer orders -> keep ~2,500 bad rows)

SEASON = {1: .9, 2: .85, 3: 1.0, 4: 1.0, 5: 1.05, 6: .85, 7: .75, 8: .8, 9: 1.0, 10: 1.15, 11: 1.4, 12: 1.5}  # Q4 peak, Jun-Aug dip
WEEKDAY = [1.1, 1.1, 1.05, 1.0, .9, .25, .1]  # Mon..Sun
HOUR_W = [1, 1, 1, 1, 1, 2, 4, 8, 14, 16, 14, 10, 8, 9, 10, 10, 8, 6, 4, 3, 3, 2, 2, 1]  # morning spike

COUNTRIES = {"Argentina": "AR", "Chile": "CL", "Uruguay": "UY", "Brazil": "BR"}
CITIES = [("Argentina", "Buenos Aires", 40), ("Argentina", "Córdoba", 12), ("Argentina", "Rosario", 10),
          ("Argentina", "Mendoza", 7), ("Argentina", "La Plata", 5), ("Argentina", "Tucumán", 4),
          ("Chile", "Santiago", 8), ("Chile", "Valparaíso", 2), ("Uruguay", "Montevideo", 5),
          ("Brazil", "São Paulo", 5), ("Brazil", "Porto Alegre", 2)]  # (country, city, weight) - HQ concentration
CATEGORIES = {  # category: (product nouns, cost low, cost high)
    "Electronics": (["USB Hub", "Wireless Mouse", "Keyboard", "Webcam", "Headset"], 15, 120),
    "Office Supplies": (["Paper Ream", "Stapler", "Pen Box", "Notebook", "Folder Set"], 2, 25),
    "Furniture": (["Desk Lamp", "Office Chair", "Filing Cabinet", "Shelf Unit", "Standing Desk"], 40, 400),
    "Packaging": (["Cardboard Box", "Bubble Wrap", "Packing Tape", "Pallet Wrap", "Mailer Bag"], 3, 40),
    "Cleaning": (["Floor Cleaner", "Disinfectant", "Mop Set", "Paper Towels", "Trash Bags"], 3, 30),
    "Tools": (["Drill", "Screwdriver Set", "Wrench Set", "Tape Measure", "Safety Gloves"], 8, 150),
    "Kitchen": (["Coffee Maker", "Water Dispenser", "Kettle", "Mug Set", "Microwave"], 10, 120),
    "Safety": (["Helmet", "Safety Vest", "First Aid Kit", "Fire Extinguisher", "Goggles"], 5, 90),
}
ADJ = ["Basic", "Pro", "Industrial", "Compact"]
NAME_A = ["Andes", "Pampa", "Sur", "Delta", "Norte", "Plata", "Atlas", "Condor", "Federal", "Horizon",
          "Mercurio", "Nova", "Patagonia", "Rio", "Sol", "Tango", "Union", "Vertice", "Zenith", "Austral"]
NAME_B = ["Logistica", "Comercial", "Distribuidora", "Insumos", "Industrial", "Servicios", "Trading",
          "Alimentos", "Tecnologia", "Mayorista", "Suministros", "Global", "Group", "Partners", "Import"]
NAME_L = ["Textil", "Metalurgica", "Agro", "Transportes", "Quimica", "Maderas", "Plasticos"]  # prospects only
SUFFIX = ["S.A.", "S.R.L.", "S.A.S."]
FIRST = ["Juan", "Maria", "Carlos", "Lucia", "Diego", "Sofia", "Martin", "Valentina", "Pablo", "Camila",
         "Nicolas", "Julieta", "Facundo", "Agustina", "Mateo"]
LAST = ["Gomez", "Rodriguez", "Fernandez", "Lopez", "Martinez", "Perez", "Garcia", "Sanchez", "Romero",
        "Diaz", "Torres", "Alvarez", "Ruiz", "Acosta", "Silva"]
INDUSTRIES = ["Retail", "Manufacturing", "Logistics", "Food & Beverage", "Construction", "Healthcare",
              "Technology", "Education", "Agriculture"]
CAMPAIGNS = [  # (campaign_name, source, medium, quality -> how likely its leads convert)
    ("Spring Search Brand", "Google Ads", "cpc", 1.0), ("LinkedIn ABM Q1", "LinkedIn", "paid_social", 1.6),
    ("Partner Referral", "Referral", "referral", 2.5), ("Trade Show BsAs", "Trade Show", "event", 2.0),
    ("Newsletter Promo", "Email", "email", 1.2), ("Facebook Awareness", "Facebook", "paid_social", 0.4),
    ("Organic Blog", "Organic", "organic", 0.9), ("Retargeting Display", "Google Ads", "display", 0.5)]


STREETS = ["Av. Corrientes", "Av. Libertador", "Calle San Martin", "Av. Rivadavia", "Calle Belgrano", "Av. Mitre",
           "Calle Sarmiento", "Av. 9 de Julio"]
DIAL = {"Argentina": "+54", "Chile": "+56", "Uruguay": "+598", "Brazil": "+55"}
PAY_TERMS = ["Net 15", "Net 30", "Net 30", "Net 45", "Prepaid"]
BRANDS = ["Nordex", "Prisma", "Kronos", "Alba", "Vektor", "Ondas", "Tekno"]
JOBS = ["Purchasing Manager", "Procurement Analyst", "Operations Director", "Office Manager", "CEO", "Logistics Coordinator"]
REPS = ["Ana Torres", "Luis Benitez", "Marcos Vidal", "Paula Ibarra"]


def utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def slug(s):
    return re.sub(r"\W+", "_", s.lower())


def days_from_cli():
    """`backfill` -> START_DATE..yesterday | `daily [YYYY-MM-DD]` -> one day (default today)."""
    mode = sys.argv[1] if len(sys.argv) > 1 else "daily"
    today = utc_now().date()
    if mode == "backfill":
        return mode, [START_DATE + timedelta(d) for d in range((today - START_DATE).days)]
    return mode, [date.fromisoformat(sys.argv[2]) if len(sys.argv) > 2 else today]


def rand_ts(rng, day):
    h = rng.choices(range(24), HOUR_W)[0]
    return datetime.combine(day, time(h, rng.randint(0, 59), rng.randint(0, 59)))


@lru_cache(maxsize=1)
def build_reference():
    """Companies, products, suppliers, customers. Same output on every run (seeded)."""
    rng = random.Random(SEED)
    used = set()

    def name():
        while True:
            n = f"{rng.choice(NAME_A)} {rng.choice(NAME_B)} {rng.choice(SUFFIX)}"
            if n not in used:
                used.add(n)
                return n

    def when(d):
        return datetime.combine(d, time(0)) + timedelta(seconds=rng.randint(0, 86399))

    companies, buyers = [], []
    for cid in range(1, N_SUPPLIERS + N_BUYERS + 1):
        is_sup = cid <= N_SUPPLIERS
        new = (not is_sup) and rng.random() < .45  # onboarded during the 2 years
        created = START_DATE + timedelta(rng.randint(30, 700)) if new else START_DATE - timedelta(rng.randint(30, 900))
        w = [c[2] * (2.5 if new and c[1] != "Buenos Aires" else 1) for c in CITIES]  # new customers grow outside BA
        country, city, _ = rng.choices(CITIES, w)[0]
        cuit = f"{rng.choice([30, 33])}-{rng.randint(10**7, 10**8 - 1)}-{rng.randint(0, 9)}"
        nm = name()
        dom = re.sub(r"[^a-z]", "", "".join(nm.lower().split()[:2]))
        c = dict(company_id=cid, name=nm, cuit=cuit if rng.random() > .1 else cuit.replace("-", ""),  # 10% malformed
                 company_type="supplier" if is_sup else "buyer", country=country, city=city,
                 rating=None if rng.random() < .05 else round(rng.uniform(2, 5), 1),
                 created_at=when(created), industry=rng.choice(INDUSTRIES), domain=dom,
                 address=f"{rng.choice(STREETS)} {rng.randint(100, 5000)}",
                 phone=None if rng.random() < .05 else f"{DIAL[country]} {rng.randint(10, 99)} {rng.randint(1000, 9999)}-{rng.randint(1000, 9999)}",
                 contact_email=f"info@{dom}.com", payment_terms=rng.choice(PAY_TERMS), is_active=rng.random() > .05)
        c["updated_at"] = c["created_at"]
        c["employee_count"] = rng.randint(20, 500)
        if not is_sup:
            c["sf"] = rng.lognormvariate(0, 1.1)  # size factor: drives order frequency, size, lead score (Pareto)
            c["employee_count"] = 2 + round(15 * c["sf"] * rng.uniform(.5, 1.5))
            if rng.random() < .28:  # one-time buyer: places exactly one order, ever
                base = max(created, START_DATE)
                c["one_time_day"] = base + timedelta(rng.randint(0, 720 - (base - START_DATE).days))
            if new and rng.random() < .9:  # acquired through a marketing lead
                c["lead_date"] = created - timedelta(rng.randint(5, 25))
                c["lead_campaign"] = rng.choices(CAMPAIGNS, [x[3] for x in CAMPAIGNS])[0]
            buyers.append(c)
        companies.append(c)

    products, i = [], 0
    for cat, (nouns, lo, hi) in CATEGORIES.items():
        for noun in nouns:
            for adj in ADJ:
                i += 1
                cost = rng.uniform(lo, hi)
                products.append(dict(product_id=i, sku=f"{cat[:3].upper()}-{i:04d}", name=f"{adj} {noun}", category=cat,
                                     list_price=round(cost * rng.uniform(1.25, 1.6), 2), cost=cost,
                                     created_at=when(START_DATE - timedelta(rng.randint(60, 900))),
                                     brand=rng.choice(BRANDS), unit_of_measure=rng.choice(["unit", "box", "pack", "set"]),
                                     weight_kg=round(rng.uniform(.1, 25), 2), barcode="779" + str(rng.randint(10**9, 10**10 - 1)),
                                     is_active=rng.random() > .05, min_order_qty=rng.choice([1, 1, 5, 10])))
    for p in products:
        p["updated_at"] = p["created_at"]
    ranks = rng.sample(range(1, len(products) + 1), len(products))
    for p, r in zip(products, ranks):
        p["pop"] = 1 / r ** .8  # long tail -> bestsellers and slow-movers

    sup_ids = list(range(1, N_SUPPLIERS + 1))
    sup_w = [1 / r ** .8 for r in rng.sample(sup_ids, N_SUPPLIERS)]  # supplier concentration
    sp = []
    for p in products:
        k = rng.choice([1, 2, 2, 3])
        chosen = set()
        while len(chosen) < k:
            chosen.add(rng.choices(sup_ids, sup_w)[0])
        p["supplier_ids"] = sorted(chosen)
        sp += [dict(supplier_id=s, product_id=p["product_id"], supplier_price=round(p["cost"] * rng.uniform(.92, 1.08), 2),
                    supplier_sku=f"S{s}-{p['sku']}", min_order_qty=rng.choice([1, 5, 10, 25]),
                    lead_time_days=rng.randint(2, 30), is_preferred=rng.random() < .3,
                    valid_from=p["created_at"].date(), created_at=p["created_at"], updated_at=p["created_at"])
               for s in p["supplier_ids"]]
    for r in sp:  # a link can't exist before its product or its supplier company
        r["created_at"] = r["updated_at"] = max(r["created_at"], companies[r["supplier_id"] - 1]["created_at"])
    suppliers = [dict(supplier_id=c["company_id"], company_id=c["company_id"], lead_time_days=rng.randint(2, 30),
                      contact_name=f"{rng.choice(FIRST)} {rng.choice(LAST)}", contact_email=f"sales@{c['domain']}.com",
                      min_order_value=rng.choice([0, 500, 1000, 2500]), is_preferred=rng.random() < .3,
                      created_at=c["created_at"], updated_at=c["created_at"]) for c in companies[:N_SUPPLIERS]]

    customers, by_co = [], {}
    for b in buyers:
        dom = b["domain"]
        for _ in range(rng.randint(2, 6)):
            cid = len(customers) + 1
            f, l = rng.choice(FIRST), rng.choice(LAST)
            login = f"{f}.{l}{cid}@{dom}.com".lower()  # clean email = auth_user in web logs
            customers.append(dict(
                customer_id=cid, company_id=b["company_id"], first_name=f, last_name=l, login=login,
                email=rng.choice([login.upper(), " " + login]) if rng.random() < .05 else login,  # messy in DB
                birth_date=date(rng.randint(1960, 2000), rng.randint(1, 12), rng.randint(1, 28)),
                gender=rng.choice(["Male", "female", "", None]) if rng.random() < .06 else rng.choice(["M", "F"]),
                phone=None if rng.random() < .08 else f"{DIAL[b['country']]} 9 {rng.randint(10, 99)} {rng.randint(1000, 9999)}-{rng.randint(1000, 9999)}",
                job_title=rng.choice(JOBS), is_active=rng.random() > .05, created_at=b["created_at"], updated_at=b["created_at"],
                document_number=str(rng.randint(10_000_000, 55_000_000))))
            by_co.setdefault(b["company_id"], []).append(customers[-1])

    bad = []  # manifest of injected errors: (table, primary_key, column, issue)
    if HARD_MODE:
        brng = random.Random(f"{SEED}-bad-ref")
        for c in companies:
            r = brng.random()
            if r < .02:
                c["cuit"] = None
                bad.append(("companies", c["company_id"], "cuit", "missing"))
            elif r < .07:
                c["cuit"] = brng.choice(["30-1234-5", "ABC-12345678-0", "00-00000000-0", "N/A", c["cuit"].replace("-", "")[:9]])
                bad.append(("companies", c["company_id"], "cuit", "broken_format"))
        for c in customers:  # email is never nulled: it is the link to the web logs
            r = brng.random()
            if r < .04:
                c["document_number"] = None
                bad.append(("customers", c["customer_id"], "document_number", "missing"))
            elif r < .06:
                c["document_number"] = brng.choice(["12.345", "ABC123", "0"])
                bad.append(("customers", c["customer_id"], "document_number", "malformed"))

    return dict(bad=bad, companies=companies, buyers=buyers, products=products, product_w=[p["pop"] for p in products],
                suppliers=suppliers, supplier_products=sp, customers=customers, customers_by_company=by_co,
                company_by_id={c["company_id"]: c for c in companies})


def corrupt_order(o, brng):
    """Inject wrong values into an order and its items. Never touches ids, customer, company or created_at."""
    bad, oid = [], o["order_id"]
    r = brng.random() / BAD_X
    if r < .008:
        o["order_date"] = None; bad.append(("orders", oid, "order_date", "missing"))
    elif r < .016:
        o["order_date"] = utc_now().date() + timedelta(brng.randint(30, 400)); bad.append(("orders", oid, "order_date", "future_date"))
    elif r < .024:
        o["expected_delivery_date"] = o["order_date"] - timedelta(brng.randint(1, 10)); bad.append(("orders", oid, "expected_delivery_date", "before_order_date"))
    elif r < .039:
        o["status"] = brng.choice([o["status"].upper(), o["status"].title(), " " + o["status"]]); bad.append(("orders", oid, "status", "inconsistent_format"))
    elif r < .042:
        o["shipping_cost"] = -abs(o["shipping_cost"] or 10); bad.append(("orders", oid, "shipping_cost", "negative"))
    for it in list(o["items"]):
        iid, r = it["order_item_id"], brng.random() / BAD_X
        if r < .006:
            it["unit_price"] = None; bad.append(("order_items", iid, "unit_price", "missing"))
        elif r < .009:
            it["unit_price"] = -it["unit_price"]; bad.append(("order_items", iid, "unit_price", "negative"))
        elif r < .011:
            it["unit_price"] = 0; bad.append(("order_items", iid, "unit_price", "zero"))
        elif r < .017:
            it["quantity"] = 0; bad.append(("order_items", iid, "quantity", "zero"))
        elif r < .019:
            it["quantity"] = -it["quantity"]; bad.append(("order_items", iid, "quantity", "negative"))
        elif r < .023:  # same line entered twice under a new line id
            n = len(o["items"]) + 1
            o["items"].append(dict(it, order_item_id=oid * 10 + n, line_number=n))
            bad.append(("order_items", oid * 10 + n, "order_id+product_id", "duplicate_line"))
    return bad


def build_orders(day):
    """All orders (with items + web device) for one day. Deterministic per day."""
    ref = build_reference()
    rng = random.Random(f"{SEED}-orders-{day}")
    age = (utc_now().date() - day).days
    active = [b for b in ref["buyers"] if b["created_at"].date() <= day]
    regular = [b for b in active if "one_time_day" not in b]
    once = [b for b in active if b.get("one_time_day") == day]  # one-time buyers place their single order today
    once_ids = {b["company_id"] for b in once}
    lam = .06 * len(active) * SEASON[day.month] * WEEKDAY[day.weekday()]
    n = max(0, round(rng.gauss(lam, math.sqrt(lam)))) if lam > 0 else 0
    stamps = sorted(rand_ts(rng, day) for _ in range(n + len(once)))
    cos = rng.choices(regular, [b["sf"] for b in regular], k=n) + once  # big companies order more (Pareto)
    rng.shuffle(cos)
    orders, brng = [], random.Random(f"{SEED}-bad-{day}")
    urng = random.Random(f"{SEED}-upd-{day}")  # separate stream: adding updated_at changes no existing value
    for i, (ts, co) in enumerate(zip(stamps, cos), 1):
        ts = max(ts, co["created_at"])  # an order can't exist before its buyer company (customers share its created_at)
        oid = int(f"{day:%Y%m%d}") * 1000 + i
        custs = ref["customers_by_company"][co["company_id"]]
        cust = custs[0] if co["company_id"] in once_ids else rng.choices(custs, [4] + [1] * (len(custs) - 1))[0]  # first customer = primary buyer, places most orders
        if rng.random() < .075:  # 5-10% cancellations
            status = "cancelled"
        elif age > 10:
            status = "delivered"
        else:
            status = rng.choice(["pending", "confirmed", "shipped", "delivered"][:min(age, 3) + 1])
        upd = ts if status == "pending" else max(ts, min(ts + timedelta(hours=urng.randint(1, 72)), utc_now()))  # last change of the order, never before created_at
        picks = {p["product_id"]: p for p in rng.choices(
            ref["products"], ref["product_w"], k=rng.choices([1, 2, 3, 4, 5, 6], [25, 25, 20, 15, 10, 5])[0])}
        items = []
        for j, p in enumerate(picks.values(), 1):
            disc = min(1.05, max(.8, 1 - .03 * math.log(co["sf"]) + rng.gauss(0, .03)))  # big buyers get discounts
            items.append(dict(order_item_id=oid * 10 + j, order_id=oid, product_id=p["product_id"],
                              supplier_id=rng.choice(p["supplier_ids"]),
                              quantity=max(1, round(rng.lognormvariate(1.8, .6) * co["sf"] ** .3)),
                              unit_price=round(p["list_price"] * disc, 2), line_number=j,
                              discount_pct=max(0, round((1 - disc) * 100, 1)), tax_rate=rng.choice([21.0, 21.0, 10.5]),
                              warehouse=rng.choice(["BsAs-Central", "Cordoba-Norte", "Rosario-Sur"])))
            items[-1]["line_total"] = round(items[-1]["quantity"] * items[-1]["unit_price"], 2)
            items[-1]["created_at"], items[-1]["updated_at"] = ts, upd
        ship = rng.choices(["standard", "express", "pickup"], [60, 25, 15])[0]
        orders.append(dict(order_id=oid, buyer_company_id=co["company_id"], customer_id=cust["customer_id"],
                           order_date=day, status=status, created_at=ts, updated_at=upd, items=items,
                           payment_method=rng.choices(["bank_transfer", "credit_card", "cheque", "cash_on_delivery"], [50, 25, 15, 10])[0],
                           shipping_method=ship, shipping_cost=0 if ship == "pickup" else round(rng.uniform(5, 30) * (2 if ship == "express" else 1), 2),
                           po_number=f"PO-{rng.randint(10000, 99999)}" if rng.random() > .2 else None,
                           expected_delivery_date=day + timedelta({"standard": rng.randint(3, 7), "express": rng.randint(1, 2), "pickup": 0}[ship]),
                           channel=rng.choices(["web", "sales_rep", "email", "phone"], [60, 20, 12, 8])[0],
                           device=rng.choices(["desktop", "mobile", "tablet"], [70, 22, 8])[0], bad=[]))
        if HARD_MODE:
            orders[-1]["bad"] = corrupt_order(orders[-1], brng)
    now = utc_now()
    return [o for o in orders if o["created_at"] <= now]  # today: only what already "happened"


def size_bin(sf):
    return "1-10" if sf < .5 else "11-50" if sf < 1 else "51-200" if sf < 2 else "201-500" if sf < 4 else "500+"


def build_leads(day):
    """Leads created on `day`: ones that became buyer companies ('Won') + prospects that never did."""
    ref = build_reference()
    rng = random.Random(f"{SEED}-leads-{day}")
    raw = []
    for b in ref["buyers"]:
        if b.get("lead_date") == day:  # score & volume track company size -> lead score correlates with order value
            raw.append(dict(camp=b["lead_campaign"], name=b["name"], country=b["country"], city=b["city"],
                            size=size_bin(b["sf"]), industry=b["industry"], status="Won",
                            cust=ref["customers_by_company"][b["company_id"]][0],
                            score=55 + 20 * math.log(b["sf"]) + rng.gauss(0, 8),
                            volume=5000 * b["sf"] * rng.uniform(.6, 1.4)))
    lam = 1.5 * SEASON[day.month] * (1 if day.weekday() < 5 else .2)
    for _ in range(max(0, round(rng.gauss(lam, math.sqrt(lam))))):
        country, city, _w = rng.choices(CITIES, [c[2] for c in CITIES])[0]
        sf = rng.lognormvariate(-.3, 1)
        raw.append(dict(camp=rng.choice(CAMPAIGNS), country=country, city=city, size=size_bin(sf),
                        name=f"{rng.choice(NAME_A)} {rng.choice(NAME_L)} {rng.choice(SUFFIX)}",
                        industry=rng.choice(INDUSTRIES), score=rng.gauss(40, 18), volume=5000 * sf * rng.uniform(.6, 1.4),
                        status=rng.choices(["New", "Contacted", "Qualified", "Proposal", "Lost"], [30, 25, 15, 10, 20])[0]))
    rng.shuffle(raw)
    today = utc_now().date()
    for i, r in enumerate(raw, 1):
        f, l = rng.choice(FIRST), rng.choice(LAST)
        dom = re.sub(r"[^a-z]", "", "".join(r["name"].lower().split()[:2]))
        r.update(contact_name=f"{f} {l}", contact_email=f"{f}.{l}@{dom}.com".lower(), job_title=rng.choice(JOBS),
                 phone=f"{DIAL[r['country']]} {rng.randint(10, 99)} {rng.randint(1000, 9999)}-{rng.randint(1000, 9999)}",
                 sales_rep=rng.choice(REPS),
                 last_contacted=None if r["status"] == "New" else min(day + timedelta(rng.randint(0, 10)), today))
    for r in raw:  # converted leads: the contact IS the customer who later orders / logs in on the web
        if r.get("cust"):
            cu = r["cust"]
            r.update(contact_name=f"{cu['first_name']} {cu['last_name']}", contact_email=cu["login"],
                     job_title=cu["job_title"], phone=cu["phone"])
    return [dict(lead_id=f"LD-{day:%Y%m%d}-{i:03d}", created_date=day, source=r["camp"][1], campaign_name=r["camp"][0],
                 utm_source=slug(r["camp"][1]), utm_medium=r["camp"][2], utm_campaign=slug(r["camp"][0]),
                 utm_content=rng.choice(["banner_a", "banner_b", "text_ad", None]), country=r["country"],
                 city=r["city"], company_name=r["name"], company_size=r["size"], industry=r["industry"],
                 lead_score=min(100, max(1, round(r["score"]))), estimated_order_volume=round(r["volume"]),
                 status=r["status"], contact_name=r["contact_name"], contact_email=r["contact_email"], phone=r["phone"],
                 job_title=r["job_title"], sales_rep=r["sales_rep"], last_contacted_date=r["last_contacted"])
            for i, r in enumerate(raw, 1)]