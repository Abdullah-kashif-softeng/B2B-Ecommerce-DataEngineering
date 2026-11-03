# B2B E-commerce Platform — Data Dictionary & KPI Source Mapping

> **As of:** 2026-11-02 · **Scope:** 3 sources → staging → warehouse → 15 KPIs
> **Built from:** `requirements.md`, `data_geenration/*.py` (the source of truth for *why* data looks the way it does), and the actual files in `data/output/` (profiled: 733 log files, 559 lead workbooks, the manifest, the geo lookup).
> **Important:** The PostgreSQL tables are *not* in `data/output/` (the generator writes straight into the DB). I rebuilt them in memory by running the same deterministic generator code and profiled that. Reference tables (companies, products, suppliers, customers) are exact. Orders/items may differ by a few % from your live DB — see §1.4. Every number marked **(measured)** comes from that profiling.

---

## 0. TL;DR — the 12 things you must know before writing any pipeline

1. **It's a Latin-American B2B office-supplies marketplace.** 25 suppliers sell 160 products (8 categories) to 400 buyer companies in AR/CL/UY/BR. Individual *customers* (2–6 per buyer company) place the orders on behalf of their company.
2. **Three sources, three different grains:** Postgres = *order line*; web logs = *HTTP request*; leads = *marketing lead*. They are joined only through **customer email** (logs ↔ DB ↔ leads) and **exact timestamp** (web order ↔ checkout hit).
3. **The data is deliberately dirty (`HARD_MODE=1`).** ~2,548 injected bad values in Postgres (ground truth in `bad_rows_manifest.csv`), ~2,170 corrupted + ~2,140 duplicated web-log lines, and a lot of mess in lead spreadsheets (casing, `$` strings, mixed date formats, typos, duplicates, junk title rows). **Cleaning rules are part of every KPI definition** — see §7.
4. **`line_total` is your safety net.** It's computed *before* corruption, so it stays correct when `unit_price`/`quantity` are null/zero/negative. Naive `qty × unit_price` understates net revenue by **~5%** (27.5M vs 29.0M).
5. **Revenue = merchandise only.** `line_total` excludes tax (`tax_rate` is informational) and shipping (`shipping_cost` is order-level). There is **no currency column**.
6. **Gross margin needs a join the DB doesn't make obvious:** product *cost* isn't stored anywhere. Use `supplier_products.supplier_price` for the `(supplier_id, product_id)` on the order line. 100% of lines match (measured).
7. **Order device is NOT in Postgres.** The generator knows it but doesn't persist it. Device only exists in web-log User-Agents → device KPIs *require* the web-order ↔ log join.
8. **🔴 Requirement gap — CDC:** requirements say "change tracking via `updated_at`", but **no table has an `updated_at` column**, and the daily generator mutates `orders.status` *in place* (40 open orders/day). A watermark on `created_at` will silently miss every status change. See §10.1.
9. **🔴 Requirement gap — SCD2:** the generator never changes any company attribute after load, so `dim_company` SCD2 would only ever have one version per company. See §10.2.
10. **Bots are ~28% of web lines** (search bots, scripts, scanners, uptime monitor, empty UAs). Every web KPI must filter them or the numbers are meaningless (e.g. "US" looks like 17.6% of traffic but is 8.4% of human sessions).
11. **Time zones are mixed:** logs carry `-0300`; Postgres timestamps are naive Argentina-local. Normalise once, at staging.
12. **Coverage gap:** web logs exist for 2024-10-29 → 2026-11-02 **except 2026-10-31 and 2026-11-01** (backfill ran 10-31 up to "yesterday"; next run was 11-02). Lead files also skip those days. Expect a 2-day hole in daily trends.

---

## 1. The big picture

### 1.1 What the business is

A B2B marketplace (`shop.b2bmarket.example`). **Suppliers** (company_type=`supplier`) supply **products**; **buyer companies** purchase them through the web shop (60% of orders), a sales rep (20%), email (12%) or phone (8%). Buyers are acquired through **marketing campaigns** (leads) or already existed before the 2-year window.

### 1.2 Entity relationship (Postgres) and how the other two sources attach

```
                         ┌──────────────┐
                         │  companies   │ company_type = 'supplier' (ids 1–25) | 'buyer' (ids 26–425)
                         │  PK company_id│
                         └──┬────────┬──┬┘
          supplier_id = company_id    │  │ buyer_company_id
                  ┌──────────┘        │  └───────────────┐
          ┌───────▼──────┐   company_id│                  │
          │  suppliers   │      ┌──────▼──────┐           │
          │ PK supplier_id│     │  customers   │  (2–6 people per buyer company)
          └───────┬──────┘      │ PK customer_id│
                  │             └──────┬───────┘
      ┌───────────▼───────────┐        │ customer_id
      │   supplier_products   │        │
      │ PK (supplier_id,      │   ┌────▼─────────────────┐
      │     product_id)       │   │       orders          │ PK order_id (= YYYYMMDD*1000 + seq)
      │ supplier_price = COGS │   │ buyer_company_id,     │
      └───────────┬───────────┘   │ customer_id, status…  │
                  │               └────┬─────────────────┘
          ┌───────▼───────┐            │ order_id
          │   products    │◄───────────┤
          │ PK product_id │   ┌────────▼───────────┐
          └───────────────┘   │    order_items      │ PK order_item_id (= order_id*10 + line)
                  ▲  product_id│ product_id,supplier_id│ ← supplier_id is one of the product's suppliers
                  └───────────┴──────────────────────┘

  ═══════════════ cross-source links (no FK exists – you build them) ═══════════════
  WEB LOGS  auth_user ──lower(trim)──► customers.email            (who is the visitor)
  WEB LOGS  POST /checkout/confirm @ ts ═ orders.created_at (to the second, same customer)  (which order)
  LEADS     contact_email ─lower(trim)─► customers.email  (only status='Won')  (which company the lead became)
  LEADS     company_name  ─clean──────► companies.name     (fallback; 93% match, email join = 100%)
  WEB LOGS  ip (first octet) ──────────► ip_geo_lookup.csv  (country)
  WEB LOGS  utm_campaign in landing URL ≈ LEADS.utm_campaign (same slug of campaign name) – traffic attribution only
```

### 1.3 Volumes & coverage (measured)

| Source | Physical form | Coverage | Volume |
|---|---|---|---|
| PostgreSQL (7 tables) | live DB (not in `output/`) | 2024-10-29 → 2026-11-02 (orders) | companies 425 · suppliers 25 · products 160 · supplier_products 313 · customers 1,592 · orders ≈ 11.0k · order_items ≈ 29.8k |
| Web logs | `output/web_logs/access_YYYY-MM-DD.log` (Apache Combined) | 733 daily files, 2024-10-29 → 2026-11-02; **missing 2026-10-31, 2026-11-01** | 534,586 lines (≈ 726/day, max 1,943) · 123 MB |
| Leads | `output/leads/leads_YYYY-MM-DD.xlsx` | 559 files (a file exists only on days with ≥1 lead), 2024-10-31 → 2026-11-02 | 1,130 rows → **1,103 distinct `lead_id`** |
| Geo reference | `output/ip_geo_lookup.csv` | static | 21 rows (first octet → country) |
| DQ ground truth | `output/bad_rows_manifest.csv` | Postgres only | 2,548 rows |

### 1.4 Caveats on the numbers in this document

- Orders ≤ 10 days old at generation time get an age-dependent status draw that **consumes the random stream differently**. Re-running the generator on a different day therefore changes the last ~10 days of orders. My rebuild ran on 2026-11-02; your backfill ran on 2026-10-31. Treat "last 10 days" numbers as approximate, and don't re-run `backfill` on another day without truncating Postgres (inserts are `ON CONFLICT DO NOTHING`, log files are overwritten → PG and logs could disagree).
- Revenue figures below use `line_total`, cancelled orders excluded, **duplicate lines not removed** (≈ +0.9% vs deduped) unless stated.
- No currency is defined. Everything is "currency units".

---

## 2. Source 1 — PostgreSQL operational database

Loaded by `gen_postgres.py`. `backfill` = 2 years to yesterday; `daily` = one day **plus status advancement of 40 random open orders**. Reference tables are loaded once and never change.

### 2.1 `companies` — one row per company (supplier *or* buyer)

**Grain:** 1 company. **PK:** `company_id`. **Rows:** 425 (25 suppliers + 400 buyers). Suppliers are ids 1–25, buyers 26–425.
**Feeds:** dim_company (SCD2), dim_supplier (via suppliers), geography. **KPIs:** K2 K5 K6 K7 K13

| Column | Type | Meaning | Values / distribution | Quality notes |
|---|---|---|---|---|
| `company_id` | INT PK | Surrogate id | 1–425 | — |
| `name` | TEXT | Legal name | "`{Andes} {Logistica} S.A.`" – unique | Used as fallback join to leads (`company_name`) |
| `cuit` | TEXT | Argentine tax ID `NN-NNNNNNNN-N` (prefix 30/33) | — | **~10% generated without hyphens (natural, NOT in manifest)** + injected: 8 `missing`, 30 `broken_format` (`N/A`, `ABC-12345678-0`, `00-00000000-0`, short). Chile/Uruguay/Brazil companies also carry Argentine-format CUITs (unrealistic) |
| `company_type` | TEXT | `supplier` / `buyer` | 25 / 400 | Discriminator — split into two dims |
| `country` | TEXT | **Full name**, not ISO | Argentina 341 · Brazil 35 · Chile 34 · Uruguay 15 | Leads sometimes use `AR`/upper/lower → conform to ISO-2 |
| `city` | TEXT | HQ city | BA, Córdoba, Rosario, Mendoza, La Plata, Tucumán, Santiago, Valparaíso, Montevideo, São Paulo, Porto Alegre | Non-ASCII chars (Córdoba, Tucumán, São Paulo, Valparaíso) — keep UTF-8. **HQ city, not ship-to** (no ship-to exists) |
| `rating` | NUMERIC(2,1) | Company rating | 2.0–5.0 | 5% NULL (17 rows) |
| `created_at` | TIMESTAMP | Onboarding date | 55% of buyers pre-date the window (30–900 days before 2024-10-29); **45% (180) onboarded during the 2 years** | Buyers **cannot order before this date**. Defines the "acquisition cohort" |
| `address` | TEXT | Street + number | random | Noise |
| `phone` | TEXT | Dial-code formatted | — | 5% NULL |
| `contact_email` | TEXT | `info@{domain}.com` | — | Company-level; ≠ customer email |
| `industry` | TEXT | 9 industries | Retail, Manufacturing, Logistics, F&B, Construction, Healthcare, Technology, Education, Agriculture | Uniform random |
| `employee_count` | INT | Headcount | Buyers: 3–533, median 17, **correlated with order volume** (hidden size factor) | Useful segmentation proxy |
| `payment_terms` | TEXT | Net 15 / Net 30 / Net 45 / Prepaid | Net 30 double-weighted | Static |
| `is_active` | BOOLEAN | Active flag | 5% false (17 rows) | **Ignored by order generator** — inactive companies still order |

**Hidden (not in DB):** a *size factor* `sf ~ lognormal(0, 1.1)` per buyer drives order frequency, line quantity, discount depth, `employee_count` and lead score. That's why revenue is Pareto-distributed. 28% of buyers (107) are **one-time buyers** (exactly one order ever).

### 2.2 `suppliers` — supplier-specific attributes

**Grain:** 1 supplier. **PK:** `supplier_id` (**= `company_id`** 1–25). **FK:** `company_id → companies`. **Rows:** 25. **KPIs:** K13 K14

| Column | Type | Meaning | Notes |
|---|---|---|---|
| `supplier_id` | INT PK | Same value as company_id | |
| `company_id` | INT FK | Link to company (name, country, city) | Supplier geography: 19 AR, 4 BR, 2 UY |
| `lead_time_days` | INT | 2–30 | **Different** from `supplier_products.lead_time_days` (product-level) — don't mix |
| `contact_name`, `contact_email` | TEXT | Sales contact | `sales@{domain}.com` |
| `min_order_value` | INT | 0 / 500 / 1000 / 2500 | **Not enforced** by order generator |
| `is_preferred` | BOOLEAN | ~30% true | **Independent** of `supplier_products.is_preferred` |
| `created_at` | TIMESTAMP | = company created_at | |

### 2.3 `products` — catalog

**Grain:** 1 product. **PK:** `product_id`. **Rows:** 160 = 8 categories × 5 product types × 4 adjectives (Basic/Pro/Industrial/Compact). **KPIs:** K3 K4 K14

| Column | Type | Meaning | Notes |
|---|---|---|---|
| `product_id` | INT PK | 1–160 | |
| `sku` | TEXT | `{CAT3}-{id:04d}`, e.g. `ELE-0001`, `OFF-0033` | **Same SKU appears in web-log URLs** (`/products/ELE-0001`) → product-level web interest |
| `name` | TEXT | "Pro Wireless Mouse" | |
| `category` | TEXT | Electronics, Office Supplies, Furniture, Packaging, Cleaning, Tools, Kitchen, Safety | 20 products each. **Price bands differ hugely** (Furniture list 70–500; Office Supplies 3.7–35) → revenue ranks by category are price-driven |
| `list_price` | NUMERIC(10,2) | Catalog price | = hidden cost × U(1.25, 1.6) |
| `brand` | TEXT | 7 brands | Random, unrelated to category |
| `unit_of_measure` | TEXT | unit / box / pack / set | |
| `weight_kg`, `barcode` | NUMERIC / TEXT | Noise | barcode 13 digits prefix `779` |
| `is_active` | BOOLEAN | 95% true (3 inactive) | **Inactive products still sell** |
| `min_order_qty` | INT | 1/1/5/10 | **Not enforced** |
| `created_at` | TIMESTAMP | 60–900 days before window | |

**Hidden:** popularity `pop = 1/rank^0.8` (Zipf) → **top 10 products = 50% of revenue, bottom 20 = 0.5%** (measured). Every product sells at least once. The hidden **cost** (`list_price / ~1.25–1.6`) is **not stored**.

### 2.4 `supplier_products` — who can supply what, at what price

**Grain:** 1 (supplier, product) pair. **PK:** `(supplier_id, product_id)`. **Rows:** 313. Each product has 1–3 suppliers (39 / 89 / 32). **KPIs:** K4 K14

| Column | Type | Meaning | Notes |
|---|---|---|---|
| `supplier_id`, `product_id` | INT FK | Pair | |
| `supplier_price` | NUMERIC(10,2) | **Supplier's price = your COGS proxy** | = hidden cost × U(0.92, 1.08). Single static price; no history |
| `supplier_sku` | TEXT | `S{supplier}-{sku}` | |
| `min_order_qty` | INT | 1/5/10/25 | Not enforced |
| `lead_time_days` | INT | 2–30 | Pair-level |
| `is_preferred` | BOOLEAN | ~30% | Pair-level |
| `valid_from` | DATE | = product created date | One validity period only |

**Supplier concentration is built in:** supplier weights are Zipf (`1/rank^0.8`), so a few suppliers cover many products (supplier 4 has 49 products, supplier 12 has 36).

### 2.5 `customers` — people who place orders

**Grain:** 1 person. **PK:** `customer_id`. **FK:** `company_id → companies` (buyers only). **Rows:** 1,592 (2–6 per buyer, mean ≈ 4). **KPIs:** K5 K6, and the bridge to web logs/leads.

| Column | Type | Meaning | Notes |
|---|---|---|---|
| `customer_id` | INT PK | | |
| `company_id` | INT FK | Employer | The **first** customer of each company is the primary buyer (4× weight in order assignment) |
| `birth_date` | DATE | 1960–2000 | Noise |
| `gender` | TEXT | `M`/`F` | **~7% dirty and NOT in manifest:** `female`, `Male`, `''`, NULL |
| `first_name`, `last_name` | TEXT | | |
| `email` | TEXT | **The cross-source key** | Clean form: `first.last{customer_id}@{domain}.com` lowercase. **5% messy** (UPPER-CASE or leading space) — NOT in manifest. Never NULL (by design). Web `auth_user` and lead `contact_email` use the clean form → always `lower(trim(email))` |
| `phone` | TEXT | | 8% NULL |
| `job_title` | TEXT | 6 titles | |
| `is_active` | BOOLEAN | 5% false (83 rows) | Ignored by order generator |
| `created_at` | TIMESTAMP | = buyer company created_at | |
| `document_number` | TEXT | DNI-like, 8 digits | Injected: 62 `missing`, 31 `malformed` (`12.345`, `ABC123`, `0`) |

### 2.6 `orders` — order header

**Grain:** 1 order. **PK:** `order_id` = `YYYYMMDD × 1000 + sequence` (so **the id encodes the creation date**; e.g. `20241128007`). **FKs:** `buyer_company_id → companies`, `customer_id → customers`. **Rows:** ≈ 11.0k (≈ 15/day). **KPIs:** K1 K2 K5 K6 K7 K8 K11 K12 K13

| Column | Type | Meaning | Values / distribution | Quality notes |
|---|---|---|---|---|
| `order_id` | BIGINT PK | | | |
| `buyer_company_id` | INT FK | The buying company | | Never corrupted |
| `customer_id` | INT FK | The person who ordered | | Never corrupted |
| `order_date` | DATE | Business date | = date(created_at) when clean | **Injected: 185 NULL, 211 future (`today + 30…400d`)** → fall back to `date(created_at)` |
| `status` | TEXT | Lifecycle | `pending → confirmed → shipped → delivered`, or `cancelled` | **335 injected `inconsistent_format`** (`DELIVERED`, `Delivered`, `" delivered"`) → `lower(trim())`. Cancellation ≈ **7.5%** (7.7% measured; target 5–10%). Orders > 10 days old are only `delivered`/`cancelled`. **Mutates in place** (see §10.1) |
| `created_at` | TIMESTAMP | Exact order time (Argentina local, naive) | Weekday-heavy; peaks 9–10h and 14–15h; Sat ≈ 0.25×, Sun ≈ 0.1× | **Never corrupted → the reliable timestamp.** Also the join key to `POST /checkout/confirm` |
| `payment_method` | TEXT | | bank_transfer 50% · credit_card 25% · cheque 15% · cash_on_delivery 10% | |
| `shipping_method` | TEXT | | standard 59% · express 26% · pickup 15% | |
| `shipping_cost` | NUMERIC(8,2) | Order-level; 0 for pickup; 5–30 (×2 express) | | **68 injected negatives**. **Not part of revenue** |
| `po_number` | TEXT | Buyer's purchase order | | 20% NULL (legitimately missing) |
| `expected_delivery_date` | DATE | order_date + 3–7 (std) / 1–2 (express) / 0 (pickup) | | **158 injected `before_order_date`** (+ ripple from corrupted order_date) |
| `channel` | TEXT | Order origin | web 60.7% · sales_rep 19.4% · email 12.2% · phone 7.7% | **Only `web` orders have a web-log footprint** |

**Hidden / not persisted:** `device` (desktop 70 / mobile 22 / tablet 8) — see §10.3.

### 2.7 `order_items` — order lines (the fact grain)

**Grain:** 1 product on 1 order. **PK:** `order_item_id` = `order_id × 10 + line_number`. **FKs:** `order_id`, `product_id`, `supplier_id`. **Rows:** ≈ 29.8k (avg 2.7 lines/order; 1–6 distinct products, up to 8 lines with duplicates). **KPIs:** K1–K4 K7 K13 K14

| Column | Type | Meaning | Notes |
|---|---|---|---|
| `order_item_id` | BIGINT PK | | |
| `order_id` | BIGINT FK | | |
| `product_id` | INT FK | | Zipf-weighted |
| `supplier_id` | INT FK | **Supplier actually used**, chosen uniformly from the product's suppliers | Always exists in `supplier_products` for that product (0 misses) → join for COGS |
| `quantity` | INT | Units | Median ≈ 9, mean 10.8, scaled up for big buyers. **Injected: 371 zero, 114 negative** |
| `unit_price` | NUMERIC(10,2) | Price actually charged | = list_price × disc, disc ∈ [0.80, 1.05] (big buyers get ~3%·ln(sf) discount). **Can be up to 5% *above* list.** **Injected: 411 NULL, 186 negative, 120 zero** |
| `line_number` | INT | 1..n | |
| `discount_pct` | NUMERIC(4,1) | (1 − disc)·100, floored at 0 | Informational; 0–20, mean 4.2. Already embedded in `unit_price` — **don't subtract it again** |
| `tax_rate` | NUMERIC(4,1) | 21.0 (2/3 of lines) or 10.5 | **Not included in `line_total`** (Argentine IVA rates, even for BR/CL/UY buyers) |
| `line_total` | NUMERIC(12,2) | `quantity × unit_price` computed **before** corruption | **Clean even when the other two are bad.** 789 rows disagree with `qty × price` — exactly the injected ones |
| `warehouse` | TEXT | BsAs-Central / Cordoba-Norte / Rosario-Sur | Uniform random — **unrelated to buyer geography** |

**Injected line problems:** **258 `duplicate_line`** — the same `(order_id, product_id)` entered twice under a new `order_item_id` (`order_id*10 + n`, `line_number = n`). Dedupe on `(order_id, product_id)`.

**Recovery math:** if exactly one of `{quantity, unit_price, line_total}` is bad, the other two reconstruct it (`unit_price = line_total/quantity`, `quantity = line_total/unit_price`). Revenue → use `line_total` after dedupe.

Impact on net revenue (measured): naive `Σ qty×unit_price` = **27.53M** · `Σ line_total` = **28.99M** · `Σ line_total` deduped = **28.74M**.

---

## 3. Source 2 — Web server logs (Apache Combined)

Files: `output/web_logs/access_YYYY-MM-DD.log`, one per day, UTF-8, `\n`-separated. Generated by `gen_web_logs.py`.

### 3.1 Line format

```
%h %l %u [%t] "%r" %>s %b "%{Referer}i" "%{User-Agent}i"
181.45.12.9 - ana.lopez12@patagoniatrading.com [10/Jun/2025:14:03:22 -0300] "POST /checkout/confirm?sid=4f9c0a1b22de HTTP/2.0" 200 1843 "https://shop.b2bmarket.example/checkout" "Mozilla/5.0 (Windows NT 10.0; …) Chrome/120…"
```

**Grain:** one HTTP request (not a page view, not a session). **Parse with regex** (the line has quoted fields containing spaces); tolerate `-` in `bytes`.

### 3.2 Raw fields

| # | Field | Type to store | Meaning | Observations (measured) | Notes |
|---|---|---|---|---|---|
| 1 | `ip` | STRING | Client IP | 74,882 distinct; **8,254 lines are IPv6** (`2800:…`) | Office IPs: 60% of a logged-in company's traffic comes from one fixed IP → **distinct IP ≠ distinct visitor** |
| 2 | `ident` | STRING | RFC1413 | Always `-` | Drop |
| 3 | `auth_user` | STRING | Logged-in customer's **email**, else `-` | 60.3% of lines anonymous; 1,839 distinct strings, **260 upper-case variants** | `lower(trim())` → `customers.email`. 100% of checkout users exist in `customers` |
| 4 | `timestamp` | TIMESTAMP | `dd/Mon/yyyy:HH:mm:ss -0300` | Range 2024-10-29 00:00:15 → 2026-11-02 10:02:27 | **Argentina local with explicit offset.** Orders' `created_at` is the same wall-clock (naive). **Partition by parsed timestamp, not file name** — 113 lines sit in a different day's file (sessions straddling midnight) |
| 5 | `method` | STRING | HTTP verb | GET 412k · POST 59k · HEAD 45k · OPTIONS 12k · PUT 2.2k · DELETE 2.2k | HEAD = uptime monitor/scanners; OPTIONS = CORS preflight |
| 6 | `path` | STRING | URL + query string | | Contains `?sid=<12 hex>` session id (browser sessions only) and sometimes `utm_*` (10,539 lines) → **normalise** (§3.3) |
| 7 | `protocol` | STRING | | HTTP/2.0 257k · HTTP/1.1 251k · HTTP/1.0 24k | HTTP/1.0 ≈ scanners |
| 8 | `status` | INT | | 200: 412,684 · 404: 37,869 · 302: 26,717 · 304: 22,884 · 403: 10,538 · 301: 6,030 · 204: 4,442 · 400: 4,216 · 401: 2,357 · 429: 1,689 · 5xx: 2,988 (500/502/503) | **Overall 4xx = 10.6%, 5xx = 0.56% — inflated by scanners** (§K10) |
| 9 | `bytes` | INT NULL | Response size | `-` for HEAD/204/304 | Static assets 10–300 KB. **No response-time field exists → no latency KPIs** |
| 10 | `referer` | STRING | | `-` 174k (direct/bot) · internal pages · external: google.com 20k, bing 10k, linkedin, mail.google.com | External referer set at session start → traffic-source classification (direct / organic search / social / email) |
| 11 | `user_agent` | STRING | | 27 distinct strings | See §3.4 |

### 3.3 Derived fields (compute in staging)

| Derived | Rule | Why |
|---|---|---|
| `session_id` | regex `[?&]sid=([0-9a-f]{12})` | **Only human browser sessions carry it** (81,256 sessions). Bots, scanners, the monitor and static asset hits (`/static`, `/images`) have **no sid** → for those use IP+UA+30-min gap if you need sessions |
| `path_clean` | strip `sid=` param | |
| `route` | `path_clean` without query | |
| `endpoint` (pattern) | `/products/ELE-0001` → `/products/{sku}`; `/products/category/x` → `/products/category/{c}`; `/api/cart/item/1` → `/api/cart/item/{n}`; probes → `probe` | **Required for K10** — otherwise 160+ SKU URLs fragment the error rate |
| `product_sku` | from `/products/{sku}` | joins to `products.sku` (product interest) |
| `utm_source/medium/campaign` | query params on landing URL | Only 20% of *anonymous* browse sessions; **converting sessions never carry UTM** → no web-side attribution of orders |
| `ua_class` / `device_type` | §3.4 | |
| `is_bot` | UA class ∈ {search_bot, script_client, monitor, empty_ua} or route = probe | |
| `country_code` | first octet of IPv4 → lookup; IPv6 or unknown octet → `UNKNOWN` | 6.5% of lines unknown |
| `event_ts_utc` | timestamp → UTC | for warehouse consistency |

### 3.4 User-Agent classification (the generator's menu)

| Class | Examples | Lines | Rule (apply in this order) |
|---|---|---|---|
| monitor | `UptimeRobot/2.0` | 35,147 | contains `UptimeRobot` (HEAD `/health` every 30 min from 52.14.20.7) |
| search_bot | Googlebot, bingbot, AhrefsBot | 35,633 | exact names |
| script_client | curl, python-requests, zgrab, masscan, Nmap, Go-http-client, PostmanRuntime, Java, okhttp | 67,790 | name match |
| empty_ua | `-` | 9,586 | |
| **tablet** | iPad, Android `SM-X700` | 37,024 | **test BEFORE mobile** — iPad UAs contain `Mobile/15E148`! Android tablets lack the `Mobile` token |
| mobile | iPhone, Android … `Mobile Safari` | 152,036 | |
| desktop | Windows, Macintosh, X11 (Chrome/Firefox/Safari/Edge) | 193,906 | |
| legacy / app | `MSIE 6.0` (671), `Dalvik/2.1.0` Android app (621) | | classify as `other`/`app` |

Human lines (desktop+mobile+tablet+app+legacy) = **72%**; the rest is non-human.

### 3.5 Site map & traffic (lines, measured)

`/products/{sku}` 110k · `/static` 51k · `/products` 46k · **probes (`/wp-login.php`, `/.env`, `/.git/config`, `/phpmyadmin/`, `/xmlrpc.php`, `/admin.php`, `/wp-admin/`, `/cgi-bin/test.cgi`, `/server-status`) 43.6k** · `/` 39k · `/health` 35k · `/login` 29k · `/images` 25k · `/products/category/{c}` 21k · `/api/search` 20k · `/about` 18k · `/search` 18k · `/cart` 14k · `/checkout` 13k · `/api/cart` 11k · `/account` 9k · `/api/orders` 9k · **`/checkout/confirm` 6.7k** · `/admin` 6k · `/robots.txt` 3k · legacy `/product/{sku}` + `/index.html` → 301.

**Converting session shape:** `GET /` → `POST /login` → `GET /products` → `GET /products/{sku}` ×(items) → `POST /api/cart` → `GET /cart` → `GET /checkout` → `POST /checkout` → **`POST /checkout/confirm`** (always 200). **Browse-only session:** landing → optional login → 0–4 page views → optional `/account`+`/api/orders` → optional abandoned `/cart` (10%) → rare `/admin` (1%).

### 3.6 Log-level data problems (HARD_MODE)

| Problem | Rate / count (measured) | Handling |
|---|---|---|
| Corrupted lines: empty, `-- GARBAGE --`, truncated, quotes stripped, ` \|EXTRA\|` suffix | 0.4% → **2,172 unparseable** (416 empty, 450 garbage, rest truncated/broken) | Quarantine to a bad-lines table; a tolerant prefix-parser can rescue ` \|EXTRA\|` lines and some truncations |
| Exact duplicate lines | 0.4% → **2,136** | Dedupe on (ip, ts, method, path, status, bytes, referer, ua) |
| Scanner/probe traffic | 43.6k lines, 95% 4xx | Flag, exclude from quality KPIs |
| Uptime monitor | 35,147 lines | Exclude |
| Mixed protocols (HTTP/2.0, 1.1, 1.0) | | Don't use as a signal |
| Upper-case logins | 260 variants | `lower(trim())` |
| Unknown IPs (IPv6 + 8 unmapped first octets 5,23,45,62,91,103,185,212) | 34,531 lines (6.5%) | Country = `UNKNOWN`; keep, don't drop |
| Old URLs → 301 | 3.6k | Not errors |

---

## 4. Source 3 — Marketing leads (Excel)

Files: `output/leads/leads_YYYY-MM-DD.xlsx`, sheet `Sheet1`, one per day **that has leads**. Generated by `gen_leads.py`.

**Grain:** 1 lead (one row per lead per file; a lead appears **once**, in the file of its creation day). 1,130 rows / **1,103 distinct `lead_id`** / **164 Won**.

> ⚠️ **A lead's `status` is a snapshot at creation and is never updated.** There is no stage history → you cannot compute stage-to-stage funnel conversion over time. Only `Won` leads connect to anything downstream.

### 4.1 Columns

| Column | Type to land as | Meaning | Values | Mess (rate) | Cleaning rule |
|---|---|---|---|---|---|
| `lead_id` | STRING | `LD-YYYYMMDD-NNN` | unique per lead | duplicated rows 2% (27) | dedupe on `lead_id` |
| `created_date` | → DATE | Lead creation date | = file date | **5%** as text `dd/mm/yyyy` (60 rows); **1%** future-dated (+60…400 days; 5 rows) | parse both formats; if > file date, fall back to file date |
| `source` | STRING | Channel | Google Ads, LinkedIn, Trade Show, Referral, Facebook, Email, Organic | **8%** UPPER / lower / padded spaces | `initcap(trim())` |
| `campaign_name` | STRING | 8 campaigns | LinkedIn ABM Q1, Trade Show BsAs, Partner Referral, Retargeting Display, Spring Search Brand, Facebook Awareness, Newsletter Promo, Organic Blog | **never corrupted** | most reliable campaign key |
| `utm_source` / `utm_medium` / `utm_campaign` | STRING | slugged source / medium / campaign | medium ∈ cpc, paid_social, event, referral, display, email, organic | clean | match web-log UTM slugs |
| `utm_content` | STRING | banner_a / banner_b / text_ad | ~25% NULL | | |
| `country` | STRING | AR/CL/UY/BR countries | | **8%** `AR`, `ARGENTINA`, `argentina`, `CL`… | map to ISO-2 |
| `city` | STRING | | | **4%** NULL (53) | allow NULL |
| `company_name` | STRING | Prospect/lead company | | **8%** typo (char dropped) + **5%** UPPER / dots removed / trailing spaces | normalise; **prefer email join** for Won |
| `company_size` | STRING | 1-10 · 11-50 · 51-200 · 201-500 · 500+ | binned from hidden size factor | clean | |
| `industry` | STRING | 9 industries | | clean | |
| `lead_score` | → INT | 1–100 | Won ≈ `55+20·ln(size)`±8; others ≈ N(40,18) | **5%** None/`N/A`/blank; **2%** out of range (150, −5, 999; 28 rows) | NULL if not 1–100 |
| `estimated_order_volume` | → INT | expected order value (currency units) | median ≈ 3,916 | **5%** NULL, **5%** text like `"$4,321"` (28 rows) | strip `$`/`,` → INT |
| `status` | STRING | Funnel stage (snapshot) | New, Contacted, Qualified, Proposal, Lost, **Won** | clean | |
| `contact_name` | STRING | | | | |
| `contact_email` | STRING | | **Won → the customer's real login email** (join key). Others random | **6%** upper-case/NULL; **3%** invalid (`@` removed, non-Won only, 32 rows) | `lower(trim())`; validate |
| `phone`, `job_title`, `sales_rep` | STRING | | 4 reps | phone 6% stripped/NULL | |
| `last_contacted_date` | DATE | | NULL for `New` | | |

**Structural problems:** ~3% of files (15/559) have a **junk title row** `"Marketing leads export"` in A1 with the real header on row 3 → Auto Loader/pandas must detect the header row. Dates may arrive as Excel datetimes *or* strings in the same column → read columns as text/object first.

### 4.2 How a lead becomes revenue (generator logic)

Won leads are **buyer companies that were onboarded during the window** (≈ 90% of the 180 new buyers → 164 Won). The lead is created 5–25 days before `companies.created_at`, with the first customer of the company as contact. The remaining ≈ 940 leads are prospects that never convert. **The 220 pre-existing buyers have no lead at all** → **38.3% of net revenue is lead-sourced, 61.7% is not** (measured).

Measured: 164/164 Won leads match a customer via **email**; only 153/164 (93%) match a company via **cleaned name**; 148/164 (90%) of the Won companies have ≥ 1 order; median lead → first order = **41.5 days** (after cleaning `created_date`; corrupted future dates produce negative lags).

---

## 5. Reference & control files

### 5.1 `ip_geo_lookup.csv` (21 rows) — `first_octet,country_code`

AR: 181, 186, 190, 200 · BR: 177, 189 · CL: 152, 146 · UY: 167 · US: 3, 52, 104 · ES: 88, 90 · MX: 148, 187 · DE: 46, 78 · FR: 51 · IN: 49 · CN: 36.
Country-level only (no city/region). IPv6 and the 8 "unknown" first octets deliberately miss → `UNKNOWN`. Search bots use US IPs; other bots use US/DE/FR/IN/CN.

### 5.2 `bad_rows_manifest.csv` (2,548 rows) — `table,primary_key,column,issue`

Ground truth for **Postgres only** (nothing for logs/leads). Use it to *test* your DQ rules: your pipeline's rejected-row counts should reconcile to these.

| table | column | issue | rows |
|---|---|---|---|
| companies | cuit | broken_format / missing | 30 / 8 |
| customers | document_number | malformed / missing | 31 / 62 |
| orders | order_date | missing / future_date | 185 / 211 |
| orders | expected_delivery_date | before_order_date | 158 |
| orders | status | inconsistent_format | 335 |
| orders | shipping_cost | negative | 68 |
| order_items | unit_price | missing / negative / zero | 411 / 186 / 120 |
| order_items | quantity | zero / negative | 371 / 114 |
| order_items | order_id+product_id | duplicate_line (pk = new line id) | 258 |

**Not in the manifest, but present:** customer email case/space (5%), gender variants (~7%), CUIT without hyphens (~10%), plus every log/lead problem in §3.6 and §4.1.

---

## 6. Cross-source joins (the "connections")

| Link | Join | Match rate | Notes |
|---|---|---|---|
| Visitor → customer | `lower(trim(auth_user)) = lower(trim(customers.email))` | 100% of checkout users | Only 40% of lines are logged in |
| Web checkout → order | customer email **and** `parsed_ts = orders.created_at` (to the second) | **98.7%** (6,641/6,730) in my rebuild; I expect ≥ 99% on your real DB — misses = corrupted lines (~0.4%) + drift in my in-memory rebuild of the last 10 days | Only `channel='web'` orders. Dedupe log lines first |
| Order → device | via the web link, device from UA class | UA-derived = generator device **99.8%** agreement | Only route to a device attribute |
| Won lead → company/customer | `lower(trim(contact_email)) = customers.email` | **100%** | Preferred |
| Won lead → company (fallback) | cleaned `company_name = companies.name` | 93% | Typos/UPPER break it |
| Order line → COGS | `(order_items.supplier_id, product_id) = supplier_products` | **100%** | |
| Order line → supplier company | `supplier_id = suppliers.supplier_id = companies.company_id` | 100% | |
| Web URL → product | `/products/{sku}` ↔ `products.sku` | 100% | |
| Log IP → country | first octet ↔ lookup | 93.5% of lines | |
| Web UTM → lead campaign | `utm_campaign` slug = leads `utm_campaign` | exact slugs | traffic only; cannot be tied to orders |
| Anonymous sessions → customer | **impossible** | — | No cookie/user id |

**Marketing attribution in one sentence:** *Lead → (email) → customer → company → orders* is the only attribution chain. UTM tags in the logs measure campaign **traffic**, never campaign **revenue**.

---

## 7. Data-quality catalogue (every problem, with its fix)

| # | Source | Problem | Volume | Detect | Fix | Affects |
|---|---|---|---|---|---|---|
| 1 | PG orders | `status` casing/spaces | 335 | `status != lower(trim(status))` | normalise | K1 K2 K6 (cancel filter!) |
| 2 | PG orders | `order_date` NULL | 185 | null | use `date(created_at)` | K1 |
| 3 | PG orders | `order_date` in the future | 211 | `> current_date` | use `date(created_at)` | K1 |
| 4 | PG orders | `expected_delivery_date < order_date` | 158 | compare | flag; recompute from shipping_method | delivery KPIs |
| 5 | PG orders | `shipping_cost < 0` | 68 | | `abs()` or NULL + flag | freight metrics |
| 6 | PG items | `unit_price` NULL/≤0 | 717 | | derive `line_total/quantity` | K1–K4 |
| 7 | PG items | `quantity` ≤ 0 | 485 | | derive `line_total/unit_price` | K1–K4 |
| 8 | PG items | duplicate (order_id, product_id) | 258 | `count(*) > 1` | keep one | K1–K4 K13 K14 |
| 9 | PG companies | CUIT null/broken/no-hyphen | 8 / 30 / ~10% | regex `^\d{2}-\d{8}-\d$` | flag `cuit_valid`; keep row | dims only |
| 10 | PG customers | `document_number` null/malformed | 62 / 31 | | flag | dims only |
| 11 | PG customers | email case/space; gender variants | 5% / 7% | | `lower(trim)`; map to M/F/Unknown | joins, dims |
| 12 | Logs | unparseable lines | 2,172 | regex fail | quarantine | all web |
| 13 | Logs | exact duplicates | 2,136 | | dedupe | all web |
| 14 | Logs | bots/scanners/monitor/empty UA | ~28% | UA + probe path | `is_bot` flag | K8 K9 K10 |
| 15 | Logs | unknown/IPv6 country | 6.5% | lookup miss | `UNKNOWN` | K9 |
| 16 | Logs | session id missing for bots/static | — | | n/a / sessionise | K8 |
| 17 | Logs | file day ≠ event day | 113 | | partition by parsed ts | daily trends |
| 18 | Logs | missing days | 2026-10-31, 11-01 | file listing | document; don't impute | trends |
| 19 | Leads | duplicate rows | 27 | | dedupe `lead_id` | K11 K12 |
| 20 | Leads | junk title row | 15 files | A1 text | detect header row | ingestion |
| 21 | Leads | mixed/future dates | 60 / 5 | type, `>` file date | parse; fallback | K11 |
| 22 | Leads | source/country casing | 8% / 8% | | initcap; ISO-2 | K11 |
| 23 | Leads | score None/`N/A`/out-of-range | 5% / 2% | | NULL | K12 |
| 24 | Leads | `$` strings / NULL volume | 5% / 5% | | strip → INT | K12 |
| 25 | Leads | company-name typo/UPPER | 8% / 5% | | join by email | K11 |
| 26 | Leads | invalid/null emails | 3% / 6% | no `@` | flag (non-Won only) | contact quality |

---

## 8. What the data tells (generator behaviours you should see in the output)

| Pattern | Evidence (measured) | Built by |
|---|---|---|
| **Seasonality** | Net revenue/month: trough Feb-25 595k; peaks Dec-25 1.84M and Oct-26 1.90M; Jun–Aug dip; Q4 peak | `SEASON` factors (Nov 1.4, Dec 1.5, Jul 0.75) |
| **Weekly cycle** | Orders by weekday Mon–Fri ≈ 1.9–2.2k, Sat 525, Sun 226 | `WEEKDAY` |
| **Intraday** | Orders peak 9–10h (1,247 at 9h) and 14–15h | `HOUR_W` |
| **Growth** | YTD net revenue 2026 (Jan–Oct) ≈ 13.3M vs 9.7M same months 2025 (**≈ +37%**), driven by 180 buyers onboarded during the window | 45% of buyers are "new" |
| **Pareto customers** | Top 20% of ordering buyers = **78.8%** of net revenue; top 10 buyers = 34.1%; #1 buyer (company 341) = 1.72M | lognormal size factor |
| **One-time buyers** | 107 buyers (28%) place exactly one order | `one_time_day` |
| **Long-tail products** | Top 10 products = 50% of revenue; bottom 20 = 0.5% | Zipf popularity |
| **Supplier concentration** | Top 1 = 20.7%, top 3 = 45.0%, top 5 = 59.0% | Zipf supplier weights |
| **Geography** | HQ concentration; new buyers skew *away* from Buenos Aires (2.5× weight) | `CITIES` weights |
| **Cancellations** | 7.7% of orders | 7.5% draw |
| **Discounting** | Realised price = 96.3% of list on average; large buyers discount more | `disc` formula |
| **Device behaviour** | desktop converts 12.5%, tablet 7.2%, mobile 4.1% | converting sessions drawn 70/22/8, browsing sessions 45/45/10 — **a synthetic signal, not a real behavioural truth** |
| **Lead quality** | Won leads' score tracks company size; Trade Show & Referral convert best | campaign "quality" multipliers |

Monthly baseline to reconcile your K1 view against (`line_total`, rebuilt orders; partial first/last months):

| Month | Orders | Gross | Net (ex-cancelled) | Month | Orders | Gross | Net (ex-cancelled) |
|---|---:|---:|---:|---|---:|---:|---:|
| 2024-10* | 49 | 104k | 93k  | 2025-11 | 587 | 1,688k | 1,608k |
| 2024-11 | 420 | 1,212k | 1,114k  | 2025-12 | 719 | 1,997k | 1,844k |
| 2024-12 | 528 | 1,407k | 1,276k  | 2026-01 | 422 | 1,238k | 1,108k |
| 2025-01 | 289 | 822k | 732k  | 2026-02 | 371 | 1,069k | 963k |
| 2025-02 | 268 | 658k | 595k  | 2026-03 | 525 | 1,499k | 1,399k |
| 2025-03 | 360 | 1,018k | 948k  | 2026-04 | 500 | 1,421k | 1,305k |
| 2025-04 | 369 | 1,073k | 986k  | 2026-05 | 553 | 1,644k | 1,490k |
| 2025-05 | 428 | 1,158k | 1,082k  | 2026-06 | 525 | 1,573k | 1,448k |
| 2025-06 | 344 | 861k | 797k  | 2026-07 | 425 | 1,198k | 1,080k |
| 2025-07 | 317 | 1,056k | 1,001k  | 2026-08 | 423 | 1,175k | 1,084k |
| 2025-08 | 345 | 932k | 869k  | 2026-09 | 566 | 1,622k | 1,535k |
| 2025-09 | 468 | 1,352k | 1,289k  | 2026-10 | 718 | 2,066k | 1,900k |
| 2025-10 | 552 | 1,521k | 1,418k  | 2026-11* | 13 | 40k | 28k |

\* partial month → MoM % for the first and last month is meaningless (Nov-24 shows +1,104%). Exclude partial months from MoM.

---

## 9. KPI ↔ source mapping (all 15)

Conventions: **"net"** = excludes `status = cancelled` after `lower(trim())`. **"line revenue"** = `line_total` after (order_id, product_id) dedupe. **"order date"** = `date(created_at)`. **"human"** = `is_bot = false`.

### Sales & Revenue

#### K1 — Monthly Revenue Trend
| | |
|---|---|
| **Question** | How are gross and net revenue moving month over month? |
| **Output grain** | month |
| **Measures** | Gross = Σ line revenue (all statuses) · Net = same excluding cancelled · MoM % = (net − prev net)/prev net · order count |
| **Sources** | `orders.created_at, status` · `order_items.line_total, order_id, product_id` |
| **Join** | `order_items.order_id = orders.order_id` |
| **Cleaning** | status normalise (#1) · date from `created_at` (#2,#3) · `line_total` not `qty×price` (#6,#7) · dedupe (#8) |
| **Expected** | table in §8; ≈ 7.7% of orders cancelled; net ≈ 28.7M cumulative |
| **Traps** | partial first/last month · the 2 missing-day weekend gap is only in logs/leads (orders may also be missing those days if PG daily wasn't run — check) |

#### K2 — Revenue by Company (Pareto)
| | |
|---|---|
| **Output grain** | buyer company (optionally × period) |
| **Measures** | net revenue, order count, cumulative revenue %, `is_top20pct` (rank ≤ 20% of *ordering* buyers) |
| **Sources** | `orders.buyer_company_id` · `order_items.line_total` · `companies.name/country/city/industry` |
| **Cleaning** | as K1; **use SCD2 version valid at order time** for attributes |
| **Expected** | 377 of 400 buyers have ordered; top 20% (75) = **78.8%**; top 10 = 34.1%; 107 buyers have exactly 1 order |
| **Traps** | Define the 20% over buyers *with orders*, say so; 23 buyers never ordered |

#### K3 — Revenue by Product / Category
| | |
|---|---|
| **Output grain** | product (and category roll-up), optionally × month |
| **Measures** | net revenue, units (`quantity`, cleaned), orders, rank, `is_bestseller` (top N / ≥ P80), `is_slow_mover` (bottom N or no sale in last 90 days) |
| **Sources** | `order_items.product_id, quantity, line_total` · `products.sku, name, category, list_price, is_active` |
| **Expected** | Furniture 11.8M (40.6%) · Kitchen 7.0M (24.3%) · Tools 4.2M (14.5%) · Electronics 2.4M (8.3%) · Safety 1.6M (5.4%) · Office 0.8M (2.8%) · Packaging 0.7M (2.5%) · Cleaning 0.45M (1.6%). Top products: id 123 (4.8M), id 47 (3.6M). All 160 sell at least once |
| **Traps** | Category rank is price-driven – also show **units**. Slow-mover needs a rule (all products have some sales). Inactive products still sell |

#### K4 — Gross Margin Analysis
| | |
|---|---|
| **Output grain** | product × supplier (roll up to category/supplier) |
| **Measures** | Revenue = `quantity × unit_price` · COGS = `quantity × supplier_price` · Margin % = 1 − COGS/Revenue · realised price vs `list_price` (avg `unit_price/list_price`, discount depth) |
| **Sources** | `order_items(supplier_id, product_id, quantity, unit_price)` · `supplier_products.supplier_price` · `products.list_price, category` |
| **Join** | `(order_items.supplier_id, order_items.product_id) = (supplier_products.supplier_id, product_id)` — 0 misses |
| **Cleaning** | Repair `unit_price`/`quantity` first (#6,#7) or exclude invalid lines; dedupe; net only |
| **Expected** | **overall ≈ 26.8%**; Kitchen 30.0 · Cleaning 28.5 · Tools 28.0 · Office 27.3 · Safety 27.0 · Electronics 25.1 · Furniture 24.9 · Packaging 24.5. Realised price ≈ **96.3%** of list |
| **Traps** | **Cost isn't a column** (proxy = supplier_price, static, no history). Prices can exceed list (up to +5%) → negative discount; `discount_pct` floors at 0 |

### Customer Intelligence

#### K5 — Customer Lifetime Value (CLV) & cohorts
| | |
|---|---|
| **Output grain** | buyer company (+ cohort roll-ups) |
| **Measures** | lifetime net spend (historical, not predictive), first/last order date, tenure, spend tier (e.g. quartile or A/B/C), acquisition cohort (month/quarter of `companies.created_at`, with a "Legacy (pre-window)" cohort), acquisition channel (via Won lead → campaign, else "Organic/Existing") |
| **Sources** | `orders` + `order_items` · `companies.created_at` · `leads` (Won, via email→customer→company) for channel |
| **Traps** | CLV of recently onboarded buyers is mechanically small – normalise by tenure. 55% of buyers have no real acquisition date (before window). Spend skew is extreme (**measured, deduped net, 377 ordering buyers:** median 19.7k · mean 76.2k · p25 3.0k · p75 63.5k · p90 173.7k · max 1.70M) |

#### K6 — Customer Activity
| | |
|---|---|
| **Output grain** | buyer company (and global totals), optionally × month |
| **Measures** | orders per company · AOV = net revenue / net orders · repeat purchase rate = buyers with ≥ 2 net orders / buyers with ≥ 1 · days between orders |
| **Sources** | `orders` · `order_items` · `customers` (to show who orders within a company) |
| **Expected** | orders/buyer: mean 27.1, median 11, p25 = 1, p75 = 30, max 378 · **AOV mean ≈ 2,835, median ≈ 1,893** · **repeat rate ≈ 71.6%** |
| **Traps** | Order grain vs customer(person) grain; the first customer of every company places the majority of orders; AOV is skewed by whales – show the median |

#### K7 — Geographic Sales
| | |
|---|---|
| **Output grain** | country × city (× month) |
| **Measures** | net revenue, orders, active buyers, revenue per buyer, growth (period over period, or share of new buyers) |
| **Sources** | `companies.country, city` (buyer's HQ) · `orders` · `order_items` |
| **Expected** | Argentina 85.3% · Brazil 7.2% · Chile 6.6% · Uruguay 0.9%. By city: Buenos Aires 30.9% · Córdoba 24.0% · Mendoza 10.0% · La Plata 9.8% · Tucumán 6.4% · Santiago 5.9% · Porto Alegre 4.8% · Rosario 4.1% · São Paulo 2.4% · Montevideo 0.9% · Valparaíso 0.7% |
| **Traps** | Geography = buyer HQ, **not delivery location**. Córdoba's 24% comes from a few whales (62 buyers). "Growth regions" = compare new-buyer revenue by city; generator pushes new buyers outside Buenos Aires. UTF-8 for city names |

### Web & Traffic Analytics

#### K8 — Traffic by Device Type
| | |
|---|---|
| **Output grain** | device_type (× day/month) |
| **Measures** | human sessions · share · pages/session · bounce (single-hit) rate · **conversion = sessions with `POST /checkout/confirm` / sessions** · (via order link) orders & AOV per device |
| **Sources** | logs `user_agent, path (sid), timestamp, auth_user` · `orders` (for AOV, via §6 link) |
| **Cleaning** | dedupe lines; drop bots/monitor/scripts/empty UA; sessionise by `sid`; classify with tablet-before-mobile |
| **Expected (human sessions, N = 79,662)** | desktop 37,459 (47.0%) conv **12.5%** · mobile 34,296 (43.1%) conv **4.1%** · tablet 7,907 (9.9%) conv **7.2%** · overall conv **8.4%** · bounce 22.4% |
| **Traps** | Count sessions, not lines. Only `channel = web` orders can convert (60% of orders). Device for orders only through the web link. Conversion gap is a built-in synthetic effect |

#### K9 — Geographic Web Traffic
| | |
|---|---|
| **Output grain** | country (× day/month) |
| **Measures** | human sessions, unique logged-in users, share, top sources (also by referer class: direct / organic search / social / email) |
| **Sources** | logs `ip, referer, path (sid)` · `ip_geo_lookup.csv` |
| **Expected (human sessions)** | AR 48,364 (60.7%) · BR 7,803 · US 6,716 · CL 6,212 · UY 3,153 · ES 2,845 · MX 2,495 · UNKNOWN 2,074 (2.6%). Bots add CN/IN/DE/FR and inflate US (17.6% of raw lines) |
| **Traps** | Filter bots first. Country only (no city). IPv6 → UNKNOWN. Office-shared IPs → don't count distinct IPs as visitors |

#### K10 — Web Activity Quality
| | |
|---|---|
| **Output grain** | endpoint pattern (× day) |
| **Measures** | requests, 4xx rate, 5xx rate, error rate, top error status, share of errors from humans vs bots |
| **Sources** | logs `path → endpoint`, `status`, UA flags |
| **Expected (humans only, ≈ values)** | `/api/orders` **5.4% 5xx** · `/checkout` **4.0% 5xx** (+0.9% 4xx) · `/api/search` **2.8% 5xx** + 1.8% 4xx · `/login` **8.1% 4xx** (401) · `/products/{sku}` **4.0% 4xx** · `/admin` **≈ 91% 4xx** (403) · others ≈ 0.4–0.5% 4xx, 0.3% 5xx. Overall: all traffic 4xx 10.6% / 5xx 0.56%; **humans only ≈ 2.0% / ≈ 0.7%** |
| **Traps** | Normalise paths (strip sid/SKU). Exclude scanner probes (95% 4xx) and `/health`. 3xx/304/204 are not errors; 429 is a 4xx. `/checkout/confirm` is always 200 by construction. No latency data |

### Lead Quality & Sales Funnel

#### K11 — Lead Conversion Funnel
| | |
|---|---|
| **Output grain** | campaign / source / medium (× month) |
| **Measures** | leads · Won leads · lead→Won % · Won with ≥ 1 order · lead→order % · days lead→first order · revenue per Won lead · cost data: **none** (no spend) |
| **Sources** | leads (`lead_id, created_date, source, campaign_name, utm_*, status, contact_email`) · `customers.email` · `orders` / `order_items` |
| **Chain** | lead.contact_email → customers.email → company_id → orders |
| **Cleaning** | dedupe `lead_id` · clean source/date · only `Won` rows have a conversion path |
| **Expected (distinct leads, N = 1,103)** | overall 14.9% Won. By campaign: Trade Show BsAs 24.5% · Partner Referral 23.3% · LinkedIn ABM 14.9% · Newsletter 14.9% · Organic Blog 13.0% · Spring Search Brand 12.0% · Retargeting Display 8.0% · Facebook Awareness 5.3%. 148/164 Won companies ordered |
| **Traps** | Status is a **snapshot** → no stage transition rates (New→Contacted→…). Use `campaign_name` rather than casing-messy `source`. Funnel top (impressions/clicks) doesn't exist. 62% of revenue has no lead |

#### K12 — Lead Quality vs Order Value
| | |
|---|---|
| **Output grain** | Won lead (→ company) ; score band summary |
| **Measures** | lead_score vs total net revenue per company (and vs `estimated_order_volume`) · correlation · avg revenue by score band · estimated vs actual |
| **Sources** | leads `lead_score, estimated_order_volume` · orders via K11 chain |
| **Expected (n = 138 usable Won leads)** | **Pearson 0.45 · Spearman 0.59** (score vs revenue); estimated volume vs revenue Spearman 0.63. Avg company revenue by score band: ≤ 40 → 5.5k · 40–55 → 14.9k · 55–70 → 37.7k · 70–85 → 62.8k · 85–100 → 404.7k |
| **Traps** | Only converted leads have order value (selection bias: Won scores are built from company size, non-Won from N(40,18), so score separates Won from the rest almost by construction). Clean `lead_score` first (None / `N/A` / 150 / −5 / 999). Revenue depends on tenure — consider revenue per month active. Spearman is more robust than Pearson (whales) |

### Supplier Performance

#### K13 — Supplier Revenue
| | |
|---|---|
| **Output grain** | supplier (× month) |
| **Measures** | net revenue, orders containing the supplier (`COUNT DISTINCT order_id`), units, revenue share, **concentration % from top N** (top 1/3/5), HHI |
| **Sources** | `order_items.supplier_id, line_total` · `suppliers` · `companies` (supplier name/country) |
| **Expected** | top 1 = **20.7%**, top 3 = **45.0%**, top 5 = **59.0%**; all 25 suppliers have sales |
| **Traps** | An order can contain several suppliers → **order counts are not additive across suppliers**. Use the supplier on the *line*, not "the product's supplier" (products have 1–3). Supplier name is in `companies` (via `suppliers.company_id`) |

#### K14 — Supplier-Product Performance
| | |
|---|---|
| **Output grain** | (supplier, product) — 313 combos |
| **Measures** | net revenue, units, margin % (K4 logic), discount depth, orders, plus `supplier_products.lead_time_days / supplier_price / is_preferred / min_order_qty` |
| **Sources** | `order_items` · `supplier_products` · `products` · `suppliers/companies` |
| **Expected** | top combos: (supplier 18, product 47) = 3.63M · (9, 123) = 1.66M · (18, 123) = 1.60M; all 313 combos have sales |
| **Traps** | Supplier is picked **uniformly** among a product's suppliers per line → within a product, revenue splits ~evenly, so differences are driven by **margin and price**, not demand. `is_preferred`/`lead_time` exist at both supplier and pair level — don't mix |

### Data Quality & Pipeline Health

#### K15 — Pipeline Execution Metrics
| | |
|---|---|
| **Output grain** | pipeline run × task (and per source table/file) |
| **Measures** | run duration, success/failure, retry count, rows read/loaded/rejected/quarantined, error rate, **data freshness** = now − max(source event ts), **CDC watermark lag** = now − stored watermark |
| **Sources** | **No generator output** — these come from *your* logging Delta tables (e.g. `etl_run_log`, `cdc_watermark`, `dq_rejects`) and Databricks Jobs run history / system tables |
| **What the data gives you to test it** | `bad_rows_manifest.csv` (2,548 expected PG rejects) · log quarantine (≈ 2,172 unparseable) and dedupe (≈ 2,136) · 27 duplicate lead rows · 15 lead files with junk rows · the 2 missing log days (freshness / gap alert) |
| **Traps** | Freshness per source: orders `max(created_at)`, logs `max(ts)`, leads `max(created_date)`. See §10.1 for why a `created_at` watermark can't capture status changes |

---

## 10. Gaps between requirements and data (decide these before building)

### 10.1 CDC: `updated_at` doesn't exist, and `orders.status` changes in place
`requirements.md` says "Change tracking via `updated_at` timestamps". The DDL in `gen_postgres.py` has **no `updated_at`** in any table, and `daily` runs `UPDATE orders SET status = …` for 40 random open orders with no timestamp change. A `created_at` watermark only ever sees new orders.
**Options:** (a) add `updated_at TIMESTAMP DEFAULT now()` + a trigger (or set it in `ADVANCE_OPEN_ORDERS`) to the DDL — the cleanest, realistic CDC; (b) re-extract a rolling window (e.g. last 14 days of `created_at`) every run and `MERGE` — works because only orders ≤ ~10 days old are non-final; (c) full-table hash compare for `orders`. Reference tables never change, so full refresh each run is fine for them.

### 10.2 SCD Type 2 has nothing to version
Companies, customers, products and supplier prices are loaded once and never modified. As generated, every `dim_company` row will have exactly one version, `is_current = true`. To make SCD2 (and "accurate historical attribution") real, extend the daily generator to mutate a few company attributes (city/country, `payment_terms`, `rating`, `is_active`) and also bump `updated_at` — see 10.1.

### 10.3 Order device isn't persisted
The generator assigns `device` (70/22/8) to each order but `orders` has no such column. Device KPIs must come from User-Agent through the checkout join. If you'd rather have it in the DB, add `orders.device`.

### 10.4 Other things to be aware of
- **No currency**; revenue excludes tax and shipping; "gross margin" is *merchandise margin*.
- **No cost column / no price history** → margin uses a single static `supplier_price`.
- **Lead funnel is a snapshot** → no stage velocity; no marketing spend → no CAC/ROI.
- **`is_active=false` and `min_order_qty` / `min_order_value` are not enforced** by the generator.
- **Warehouse is random** and unrelated to geography.
- **Time:** the generator treats the machine's UTC clock as Argentina local when cutting off "today" — the latest day is partial.
- **Missing days 2026-10-31 / 11-01** in logs and lead files.
- **Run-date dependence** of the last ~10 days of orders (see §1.4).
- **CUIT** is Argentine-format for non-Argentine companies.

---

## 11. Suggested model (to carry this into the warehouse design)

### 11.1 Staging (Unity Catalog, raw + metadata)
| Table | Source | Grain | Notes |
|---|---|---|---|
| `stg_pg_companies / suppliers / products / supplier_products / customers` | JDBC / federation | row | full refresh or CDC |
| `stg_pg_orders`, `stg_pg_order_items` | JDBC | row | CDC window (see 10.1) |
| `stg_web_log_raw` | Auto Loader (text) | 1 raw line + `_source_file`, `_ingested_at` | keep bad lines |
| `stg_web_log_parsed` | parse step | 1 request | derived fields of §3.3 |
| `stg_leads_raw` | Auto Loader (xlsx → all-string) | 1 row per file row | header-row detection |
| `ops_cdc_watermark`, `ops_etl_run_log`, `ops_dq_rejects` | pipeline | | feeds K15 |

### 11.2 Warehouse (star schema)
| Table | Grain | Key attributes |
|---|---|---|
| `dim_date` | day | year, quarter, month, week, weekday, is_weekend, season flag |
| `dim_company` **(SCD2)** | company version | type, country, city, industry, size, rating, payment_terms, is_active, valid_from/to, is_current |
| `dim_customer` | customer | company FK, email_clean, job_title, gender_clean |
| `dim_product` | product | sku, name, category, brand, list_price |
| `dim_supplier` | supplier | company attributes, lead_time_days, is_preferred |
| `dim_geography` / `ref_ip_country` | country (city) | ISO-2, name; first-octet lookup |
| `dim_device` | device type | desktop / mobile / tablet / bot / other |
| `dim_campaign` | campaign | campaign, source, medium |
| `dim_endpoint` | endpoint pattern | route group, is_api, is_static |
| **`fact_order_item`** (partition by order date) | order line | order_id, line_no, date, company_sk, customer_sk, product_sk, supplier_sk, quantity, unit_price, line_total, supplier_price, discount_pct, tax_rate, status (current), channel, is_cancelled |
| `fact_order` (optional) | order | shipping_cost, payment/shipping method, po_number, linked `session_id`, device |
| **`fact_web_request`** (partition by date) | HTTP request (human only, or flagged) | date, ts, session_id, endpoint_sk, status, bytes, device_sk, country, user email → customer_sk |
| `fact_web_session` (derived) | session | device, country, pages, converted flag, order_id |
| **`fact_lead`** | lead | created date, campaign_sk, score, est_volume, status, company_sk (Won) |
| `fact_pipeline_run` | run × task | duration, rows, status |

---

## 12. Glossary

- **Buyer company / supplier:** rows in `companies` with `company_type` buyer/supplier. Suppliers are also in `suppliers` (same id).
- **Customer:** a person at a buyer company who places orders and logs in to the web shop.
- **CUIT:** Argentine tax ID (`NN-NNNNNNNN-N`).
- **Net revenue:** merchandise revenue excluding cancelled orders, tax and shipping.
- **Won lead:** a lead that became a buyer company (the only leads with downstream orders).
- **Session (`sid`):** a browser visit identified by a 12-hex `sid` query param; bots/scanners/static assets have none.
- **Human traffic:** requests not classed as bot/monitor/script/empty-UA/probe.
- **Whale:** a high-size-factor buyer (few buyers ≈ most revenue).
- **HARD_MODE:** generator flag (default on) that injects the errors catalogued in §7.
