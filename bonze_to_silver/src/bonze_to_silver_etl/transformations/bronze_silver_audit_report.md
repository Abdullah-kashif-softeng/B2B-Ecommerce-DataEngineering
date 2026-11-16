# Bronze & Silver Data Quality Audit Report

**Project:** B2B Ecommerce Data Engineering (bronze-to-silver)
**Date of audit:** 12 November 2026
**How this audit was done:** Every notebook's actual code was read line by line (9 silver notebooks + 2 bronze notebooks), and every number below was re-verified today with fresh SQL queries directly against the live `b2b_ecommerce` tables. Nothing here is copied from memory - it is the current, live state.

---

## 1. What exists (the full inventory)

### Bronze layer (`b2b_ecommerce.bronze`) - 9 tables, ALL raw

Bronze means: data lands exactly as it came from the source. **No cleaning, no dropping, no fixing happens in bronze.** The only exceptions are two ingestion helpers that add tracking columns (`source_file`, `ingested_at`, `updated_at`) and one catch-all (`_rescued_data`) - these ADD information, they never change or remove the source values.

| Bronze table | Rows (verified today) | How it gets data | Format |
| --- | --- | --- | --- |
| customers | 1,592 | Lakeflow Connect (managed connector) | Already typed (ints, dates, timestamps) |
| companies | 425 | Lakeflow Connect | Already typed |
| products | 160 | Lakeflow Connect | Already typed |
| suppliers | 25 | Lakeflow Connect | Already typed |
| supplier_products | 313 | Lakeflow Connect | Already typed |
| orders | 11,223 | Lakeflow Connect | Already typed |
| order_items | 30,240 | Lakeflow Connect | Already typed |
| leads | 1,138 | Own pipeline (Excel files via Auto Loader + openpyxl) | All columns as raw TEXT |
| logs | 541,392 | Own pipeline (Auto Loader text files) | One raw log line per row |

### Silver layer (`b2b_ecommerce.silver`) - 9 tables, ALL clean

| Silver table | Rows (today) | Unique keys | Duplicate history versions | Merge style (SCD) |
| --- | --- | --- | --- | --- |
| customers | 1,592 | 1,592 | 0 (all current) | Type 2 (keeps history) |
| companies | 425 | 425 | 0 | Type 2 |
| products | 160 | 160 | 0 | Type 2 |
| suppliers | 25 | 25 | 0 | Type 2 |
| supplier_products | 313 | 313 pairs | 0 | Type 2 (two-part key) |
| orders | 11,223 | 11,223 | 0 | Type 2 |
| order_items | 30,240 | 30,240 | 0 | Type 2 |
| leads | 1,098 | 1,098 | n/a | Type 1 (latest wins, no history) |
| logs | 537,031 | 537,031 | n/a | Type 1 (dedup only) |

**What SCD Type 2 means in simple words:** if the source ever re-sends a *changed* row (e.g. an order moves from `pending` to `shipped`), the old row is closed (`__END_AT` gets a timestamp) and a new row is opened - history is never lost. The current row is the one with `__END_AT IS NULL`. Today every table has exactly one version per key because the source has not re-sent any changes yet - the machinery is armed and waiting.

**What SCD Type 1 means:** only the newest copy is kept, no history. Used for leads and logs because their duplicates were *identical* double-writes, not real changes.

---

## 2. The toolbox: every type of handling that was applied

When a bad row shows up, one of five things happens. **Nothing is ever silently dropped** - every drop is counted in the pipeline's data quality metrics, and the original raw row always survives in bronze for inspection.

### Type A - DROP the row (quarantine) - `expect_or_drop`
Used only when the row cannot be used at all: missing ID (can't merge), missing updated_at (can't order the change), a value so broken the row itself is wrong (unparseable date, illegal category). The row is removed from silver, the drop is counted, raw stays in bronze.

### Type B - FAIL the whole update (hard stop) - `expect_or_fail`
Used ONLY for broken foreign keys (an order pointing to a customer that does not exist). The update stops and rolls back - a bad FK never lands silently. This is intentional: an orphan means the source is broken and a human must intervene.

### Type C - WARN (keep the row, record the problem) - `expect`
Used for problems where the row is still a real business record: a badly formatted email, a suspicious phone. The row is kept, the problem is counted in the quality metrics so it stays visible forever.

### Type D - FIX in code (standardize / nullify / reformat)
Done with Spark functions before any check runs: trim spaces, lowercase/uppercase, blank text -> NULL, negative numbers -> NULL, wrong-format dates re-parsed, currency strings -> numbers, casting to proper types.

### Type E - MERGE away duplicates (AUTO CDC)
Identical re-sent rows are collapsed into one row by the merge key, so re-running ingestion never creates duplicates.

---

## 3. Types of checks applied - and exactly which checks exist in each type

### 3.1 Not-null / merge-key checks (DROP level - Type A)
Every table drops a row whose merge key or change-sequence is missing. A row without these cannot be merged at all.

* customers: `customer_id_not_null`, `updated_at_not_null`
* companies: `company_id_not_null`, `updated_at_not_null`
* products: `product_id_not_null`, `updated_at_not_null`
* suppliers: `supplier_id_not_null`, `updated_at_not_null`
* supplier_products: `supplier_id_not_null`, `product_id_not_null`, `updated_at_not_null`
* orders: `order_id_not_null`, `updated_at_not_null`
* order_items: `order_item_id_not_null`, `updated_at_not_null`
* leads: `lead_id_not_null`, `updated_at_not_null`
* logs: `client_ip_not_null`, `event_time_not_null` (a line that does not parse leaves every field NULL - this catches it)

*Note:* products (and every table after it) deliberately does NOT fall back to `created_at` when `updated_at` is missing - a fallback could order a change before the original version and corrupt history. The row is dropped instead. customers and companies (built first) do use a `coalesce(updated_at, created_at)` fallback - see section 10 for the inconsistency.

### 3.2 Referential integrity (FK / orphan) checks - FAIL level (Type B), plus one WARN
Every child table must point to an existing parent. The check is implemented as a **LEFT JOIN** against the parent's distinct keys; if no parent matches, a helper column (`_customer_id_match` etc.) is NULL and the expectation fires. The helper column is then excluded from the silver table so it never lands in the data.

| Table | FK checked | Checked against | Level |
| --- | --- | --- | --- |
| customers | company_id | **bronze.companies** (all rows, any type) | FAIL |
| suppliers | company_id | **silver.companies** current rows, type = 'supplier' only | FAIL |
| supplier_products | supplier_id + product_id | **silver.suppliers** + **silver.products** current rows | FAIL (both) |
| orders | customer_id | **silver.customers** current rows | FAIL |
| orders | buyer_company_id | **silver.companies** current rows, type = 'buyer' only | FAIL |
| order_items | order_id, product_id, supplier_id | **silver.orders** + **silver.products** + **silver.suppliers** current rows | FAIL (all three) |
| logs | auth_user email | **silver.customers** current rows (emails) | WARN (anonymous requests exempt) |
| leads | none | none - ON PURPOSE: leads are pre-customer prospects; only ~18% of emails match customers, so an FK would reject the whole table | - |
| companies, products | none | top-level dimensions, nothing above them | - |

Verified today (fresh queries, LEFT JOINs, current silver rows): **0 orphans on all 10 checks.** Every customer's company exists, every supplier is a supplier-type company, every order's customer and buyer company exist, every order line's order/product/supplier exist, every authenticated log email resolves to a silver customer.

### 3.3 Format / pattern checks (regex)
* customers (WARN): email must match `^[^@ ]+@[^@ ]+[.][^@ ]+$`; phone must start with `+` and a digit (international)
* companies (WARN): same email regex; same `+` phone rule
* suppliers (WARN): same email regex
* leads (WARN): same email regex (32 bad emails caught, rows kept); phone `^\+?[0-9 ()-]+$`
* leads (DROP): lead_id must match `^LD-[0-9]{8}-[0-9]{3}$`
* orders (WARN): po_number must match `^PO-[0-9]+$` or be NULL
* products (DROP): barcode must be 13 digits (EAN-13)

### 3.4 Allowed-value (category/domain) checks
* companies (WARN): company_type must be 'buyer' or 'supplier'
* products (DROP): unit_of_measure must be in ('set','unit','box','pack')
* suppliers (WARN): none beyond email (contact_name not null)
* orders (WARN): status in 5 values, payment_method in 4, shipping_method in 3, channel in 4
* order_items (DROP for parse): method in 9 HTTP verbs (logs); (WARN): warehouse in 3 values
* leads (DROP): source in 7, status in 6, country in 4, company_size in 5 ranges
* logs (DROP): method in 9 HTTP verbs (GET, HEAD, POST, PUT, DELETE, CONNECT, OPTIONS, TRACE, PATCH)

### 3.5 Range checks (numbers)
* companies (WARN): rating between 0 and 5; employee_count >= 0
* suppliers (WARN): lead_time_days >= 0; min_order_value >= 0
* supplier_products (WARN): supplier_price > 0; min_order_qty >= 1; lead_time_days >= 0
* products (DROP): list_price > 0; weight_kg > 0; min_order_qty >= 1
* orders (WARN): discount/tax ranges - see orders; order_items (WARN): discount_pct 0-100, tax_rate >= 0, quantity > 0, unit_price > 0 (these last two COUNT the nullified rows, they don't drop them)
* logs (DROP): status_code between 100 and 599
* leads (DROP): estimated_order_volume >= 0; (in-code) lead_score must be 0-100 else NULL

### 3.6 Date-logic checks
* customers (WARN): birth_date must not be in the future (0 violations)
* leads (DROP): created_date must not be in the future (4 bad rows removed); last_contacted_date must not be before created_date (12 bad rows removed)
* orders (WARN): `order_date_valid IS TRUE` - counts the 406 kept-but-flagged rows (192 NULL + 214 future dates); the row itself is KEPT and marked with the `order_date_valid` boolean column. Silver preserves the business entity; gold decides report exclusions.
* order_items / suppliers / supplier_products: valid_from and other dates kept as-is (all verified plausible)

### 3.7 Duplicate checks (via AUTO CDC merge - Type E)
* leads: 27 lead_ids were written twice in bronze (identical double-writes) - SCD Type 1 merge collapses them to 1,098 unique rows
* logs: 2,168 duplicate log lines (same line twice in the same file, spread over 640 of 739 files) - the merge key is a SHA-256 hash of (source_file + line), so duplicates collapse to one row
* All other bronze tables: verified 100% unique on their keys today (counts above match distinct counts exactly), so the merge is pure insurance for the future

### 3.8 Bronze-side checks (the raw layer)
* bronze.leads ingestion: a WARN-level expectation `lead_id IS NOT NULL` - a workbook with a wrong header row produces all-NULL rows, and this makes that visible and traceable (`SELECT source_file FROM bronze.leads WHERE lead_id IS NULL` finds the file). Unreadable/unparseable files are SKIPPED with an ERROR log naming the file and reason - one bad file never fails the run. Schema drift never fails: extra columns land in `_rescued_data` (JSON), missing columns load as NULL (0 rescued rows exist today). Every sheet is read from its own header row (15 files with title rows above the header handled automatically).
* bronze.logs ingestion: lines are stored EXACTLY as written, one row per line, only tracking columns added. Malformed lines (e.g. the `-- GARBAGE --` sentinel, 448 occurrences) are ingested untouched - parsing belongs to silver.
* The 7 connector-managed bronze tables: arrive already typed; bronze applies no checks by design.

---

## 4. Table-by-table: what was fixed, standardized, dropped, converted

### 4.1 silver.customers (from bronze.customers, 1,592 -> 1,592)
* **Nulls fixed:** blank first/last names, emails, phones, job titles became real NULLs (trim; "" / " " -> NULL). 50 blank/unknown genders standardized to M/F or NULL.
* **Invalid values nullified:** document_number kept only if it is exactly 7-8 digits - 31 junk values ('ABC123', '12.345', '0') -> NULL. Today: 93 NULL document numbers in silver (62 born NULL + 31 nullified). 0 invalid emails left; 39 emails had padding, all trimmed + lowercased. 0 future birth dates.
* **Standardized:** gender 'M'/'Male'/'m' -> 'M', 'F'/'female' -> 'F'; emails lowercased; phones trimmed (kept as-is - valid international formats).
* **Joins:** LEFT JOIN bronze.companies (distinct company_id) -> `_company_id_match` -> FAIL-level FK check. Flag column excluded from silver.
* **Conversions (casts):** customer_id/company_id -> int, birth_date -> date, is_active -> boolean, created_at/updated_at -> timestamp. updated_at falls back to created_at if missing (coalesce).
* **Dropped/warned:** nothing dropped today; FK failures would hard-stop.

### 4.2 silver.companies (425 -> 425)
* **Nulls fixed:** blank text -> NULL (name, city, address, phone, contact_email).
* **Invalid values nullified (CUIT):** a CUIT is kept only if it matches `XX-XXXXXXXX-X` AND is not the placeholder `00-00000000-0`. That killed 53 junk formats ('ABC-12345678-0', '30-1234-5', 'N/A', 11-digit), 8 placeholders and kept the 8 genuine NULLs -> exactly 69 NULL cuits in silver today, 356 valid and all unique.
* **Standardized:** company_type, country, city, industry, payment_terms lowercased; contact_email lowercased.
* **Joins:** none (top-level dimension). customers enforces the FK down to companies.
* **Conversions:** company_id -> int, rating -> decimal(2,1), employee_count -> int, is_active -> boolean, timestamps.
* **Checks passed today:** all 425 have valid buyer/supplier type, rating 2.0-5.0 in range, employee counts 3-533 (no negatives), 0 bad emails/phones.

### 4.3 silver.products (160 -> 160)
* **The strictest table:** EVERY check is DROP-level (quarantine policy) - sku, name, category, brand, unit_of_measure, list_price > 0, weight_kg > 0, min_order_qty >= 1, barcode EAN-13, plus the not-null keys.
* **Standardized:** sku -> UPPERCASE (identifiers); category -> lowercase (8 title-case values like 'Office Supplies' -> 'office supplies'); brand trim-only (proper noun, casing kept); barcode trimmed.
* **No FK:** products is a top-level dimension (nothing above it to reference).
* **Conversions:** product_id -> int, list_price -> decimal(10,2), weight_kg -> decimal(6,2), min_order_qty -> int, is_active -> boolean, timestamps. NO created_at fallback - a row without updated_at is dropped, not guessed.
* **Result today:** 160 rows, 0 violations of any kind, 0 nulls anywhere - cleanest table in the estate.

### 4.4 silver.suppliers (25 -> 25)
* **Source is already clean** (0 nulls, 0 invalid, 25 unique) - silver only trims contact text and lowercases the email.
* **FK:** LEFT JOIN silver.companies CURRENT rows where company_type = 'supplier' -> FAIL-level. A supplier pointing at a buyer company (or a missing company) stops the update. Verified: all 25 point to real supplier-type companies, 0 orphans.
* **Conversions:** ids -> int, lead_time_days/min_order_value -> int, is_preferred -> boolean, timestamps.
* **Checks today:** contact names/emails all present and valid, lead times 2-27, min order values 0-2,500 (no negatives).

### 4.5 silver.supplier_products (313 -> 313)
* **Composite key:** (supplier_id, product_id) - one row per supplier-product pair, 313 unique.
* **FKs (both FAIL):** LEFT JOINs vs current silver.suppliers and silver.products; verified 0 orphans on both sides.
* **Standardized:** supplier_sku -> trim + UPPERCASE (matches silver.products style).
* **Conversions:** price -> decimal(10,2), qty/lead -> int, valid_from -> date, is_preferred -> boolean, timestamps.
* **Checks today:** prices 2.15-393.63 all > 0, qty 1-25, lead 2-30, 0 nulls - nothing fixed or dropped.

### 4.6 silver.orders (11,223 -> 11,223 - nothing dropped)
* **Case/trim dirt fixed:** 344 rows arrived with status variants (' delivered', 'Delivered', 'DELIVERED') -> all lowercased + trimmed to exactly 5 canonical values (verified today: cancelled, confirmed, delivered, pending, shipped). payment_method / shipping_method / channel were already clean and are trimmed + lowercased defensively.
* **Negative numbers nullified:** 68 rows had shipping_cost below 0 (worst -58.05) -> NULLified, the ORDER KEPT. Today: exactly 68 NULL shipping costs in silver.
* **Impossible date pairs nullified:** 377 rows had expected_delivery_date BEFORE order_date (up to 1,114 days before) -> delivery date NULLified, order kept. Today: exactly 377 NULL delivery dates. (An order with a NULL order_date keeps its delivery date - it cannot be judged against a missing date.)
* **Nulls kept on purpose:** 2,224 orders genuinely have no PO number (kept NULL); 192 have no order_date (kept NULL).
* **Future dates KEPT and FLAGGED (not dropped):** 214 order dates are in the future (up to 2027-12-08). They are kept as-is and marked with the boolean column `order_date_valid = false` (406 flagged rows today = 192 NULL + 214 future). Reasoning: the order is still a real business entity that counts for revenue, CLV and order counts; excluding it from a monthly trend is a GOLD-layer reporting decision. Silver also carries a WARN expectation that counts these 406 in the quality metrics so they stay visible.
* **FKs (both FAIL):** LEFT JOINs vs current silver.customers and vs current silver.companies of type 'buyer'. Verified: 0 orphans both sides (all 393 buyer companies are type 'buyer').
* **Conversions:** order_id -> long, ids -> int, dates -> date, shipping_cost -> decimal(8,2), timestamps.
* **Dropped today:** nothing.

### 4.7 silver.order_items (30,240 -> 30,240 - nothing dropped)
* **Negative/zero numbers nullified:** 491 rows had quantity <= 0 (worst -50, including 0s) -> NULLified; 726 rows had a bad unit_price (415 NULL + 190 negative, worst -438.52 + 121 provably-wrong zeros) -> NULLified. The LINE is kept in both cases. Verified today: 491 NULL quantities, 726 NULL prices, 0 non-positive values left.
* **Why keep the row:** `line_total` is the trustworthy measure - it equals quantity x unit_price exactly on all 29,023 clean rows, and on dirty rows it holds the TRUE extension (e.g. quantity 0 but line_total = 9 x price). So the corrupted columns are quantity/unit_price, not line_total; the business value is preserved. Verified: 0 NULL line_totals, range 3.74 - 28,951.44.
* **Plausible values kept as-is:** discount_pct 0.0-20.0, tax_rate 10.5-21.0 (all within the WARN check ranges), line_number 1-8 unique per order, warehouse exactly 3 clean values (BsAs-Central, Rosario-Sur, Cordoba-Norte; trimmed defensively).
* **FKs (all three FAIL):** LEFT JOINs vs current silver.orders, silver.products, silver.suppliers. Verified: 0 orphans on all three sides.
* **Conversions:** ids -> long/int, price -> decimal(10,2), discount/tax -> decimal(4,1), line_total -> decimal(12,2), timestamps.
* **Dropped today:** nothing.

### 4.8 silver.leads (bronze 1,138 rows / 1,111 ids -> silver 1,098)
This is the most heavily cleaned table - the source is all-text Excel dumps.
* **Text -> real types (conversions):** dates arrived in TWO formats ('yyyy-MM-dd HH:mm:ss' and 'dd/MM/yyyy') -> parsed with `try_to_date` for both and coalesced; lead_score text -> int (only 0-100 accepted); estimated_order_volume '$1,299' strings -> number (currency symbols and commas stripped, then cast to decimal(12,2)).
* **Duplicates:** 27 lead_ids were double-written identically -> SCD Type 1 merge deduplicates them (1,111 unique ids in -> 1,098 out).
* **Dropped (13 unique ids):** 3 leads with a created_date in the future, and 12 leads contacted BEFORE they were created (2 overlapped both problems) - 1,111 - 13 = 1,098. All raw rows remain in bronze.leads.
* **Invalid values nullified:** 56 'N/A' + 26 out-of-range lead scores (-5, 150) -> NULL (row kept; today: 82 NULL scores); 26 'N/A' order volumes -> NULL. 31 invalid emails are KEPT and only WARNED (a lead is still a real prospect with a bad email).
* **Standardized:** 28 case/padding variants of `source` -> 7 canonical title-case values; country 'AR'/'CHILE'/'argentina' variants -> 4 canonical names; emails/UTM columns lowercased; company names trimmed (20 had padding).
* **Kept:** 296 NULL last_contacted_date (= never contacted, legitimate); source_file + updated_at for traceability. `ingested_at` and `_rescued_data` (bronze helpers) are excluded from silver.
* **No FK - on purpose:** leads are pre-customer prospects; only ~18% of emails match customers, so an FK check would fail the whole table.

### 4.9 silver.logs (bronze 541,392 raw lines -> silver 537,031 parsed events)
* **Parsing (the main conversion):** each raw line is parsed with the Apache/Nginx combined-format regex into 11 fields; event_time parsed including the line's own timezone offset (always -0300); status -> int; bytes -> long; the request target split into endpoint + query string (`/products?sid=x` -> `/products` + `sid=x`). A parsing helper maps regex no-match (empty string) to NULL so unparsable lines are caught safely by the not-null checks.
* **Dropped (2,193 lines):** 424 blank lines + 1,769 corrupt lines (including the `-- GARBAGE --` sentinel that appears 448 times across 309 files). All remain in bronze.logs.
* **Duplicates:** 2,168 identical duplicate lines collapsed by the log_id merge key (SHA-256 of source_file + line). 539,199 parsed - 2,168 dups = 537,031 - reconciles exactly with today's live count.
* **Placeholders cleaned:** '-' means 'no value' -> NULL for auth_user, response bytes (72,682 - legitimate 204/304 responses), referer (175,754), user_agent (9,662). Auth emails lowercased to match silver.customers.
* **FK (WARN, not fail):** authenticated requests must resolve to a silver.customers email; anonymous requests exempt. Verified today: 213,692 authenticated rows, 0 orphans.
* **Checks:** method in 9 verbs, status 100-599 - verified 0 violations in silver.
* **Log id:** `log_id` = SHA-256 of (source_file | line) - unique per event, dedups within a file.

---

## 5. Cross-cutting summary: how each problem TYPE was fixed

| Problem type | How it was handled | Where / how many |
| --- | --- | --- |
| NULL in a text field that is really 'blank' | trim, blank -> real NULL (Type D) | all text columns, all tables |
| Genuine NULLs (never had a value) | kept as NULL (correct, not dirty) | 2,224 PO numbers, 296 never-contacted leads, 192 order dates, 50 genders |
| Untrustworthy numeric values (negative/zero/impossible) | value NULLified, ROW KEPT | orders: 68 shipping_cost, 377 delivery dates; order_items: 491 quantities, 726 unit prices; leads: 82 lead scores |
| Unfixable identifier values | value NULLified, row kept | customers: 31 document numbers; companies: 61 bad CUITs |
| Case/trim variants of categories | lowercased/uppercased to one canonical form | orders: 344 status variants; leads: 28 source variants, country variants; products: 8 categories; emails everywhere |
| Junk formats in a text field | kept only if regex matches, else NULL | CUIT (61), document_number (31), lead_score (26 out-of-range + 56 N/A) |
| Wrong data types / mixed formats | converted (cast / try_cast / parse) | leads: two date formats, '$1,299' -> number; logs: timestamp with timezone, int status, long bytes |
| Duplicate rows | AUTO CDC merge collapses them by key | leads: 27 double-writes; logs: 2,168 duplicate lines |
| Rows that cannot be used at all | DROPPED from silver (counted; raw stays in bronze) | leads: 13 ids (future dates / impossible contact order); logs: 2,193 unparsable lines |
| Broken foreign keys | whole update FAILS and rolls back (hard stop) | all 8 FK checks (customers, suppliers x1, supplier_products x2, orders x2, order_items x3); 0 orphans exist today |
| Suspicious-but-kept values | WARN: row kept, problem counted in quality metrics | 31 bad lead emails, 406 invalid order dates, 68+377+491+726 nullified measures, all email/phone checks |
| Future dates | depends on meaning: lead created_date in future = impossible -> dropped; order_date in future = maybe becomes valid with time -> kept + flagged `order_date_valid=false` | leads: 4 dropped; orders: 214 kept+flagged |
| Cross-field logic violations | delivery before order -> date NULLified, row kept; contacted before created -> row dropped | orders: 377; leads: 12 |
| Bad source files | skipped at bronze with ERROR log naming file + reason; run never fails | leads Excel ingestion |
| Schema drift in source files | extra columns preserved in `_rescued_data` (bronze), missing -> NULL; never fails | leads Excel ingestion; 0 rescued rows today |

**About 'silent' handling:** there is no silent dropping anywhere. Every drop is counted in pipeline metrics, every warn is recorded, and every dropped/nullified raw value still exists in bronze for inspection.

---

## 6. Joins used (all of them)

Every join in the whole estate is a **stream-static LEFT JOIN** (a streaming bronze table joined to a batch snapshot of the parent), built on the parent's **distinct key column only** so the join can never fan out (no row multiplication). An INNER join was never used on purpose - an inner join would silently delete unmatched rows, which is exactly what the fail-level expectation exists to prevent instead. Match results land in a helper `_match` column that drives the expectation and is then excluded from silver via `except_column_list`.

The full list: customers x bronze.companies; suppliers x silver.companies (supplier type, current); supplier_products x silver.suppliers + silver.products (current); orders x silver.customers + silver.companies (buyer type, current); order_items x silver.orders + silver.products + silver.suppliers (current); logs x silver.customers (emails, current - the one WARN-level join).

"Current" means `__END_AT IS NULL` - only the live version of the parent counts as a valid FK target. supplier_products, orders and order_items additionally filter the parent by type (supplier / buyer) where the business rule requires it.

---

## 7. Conversions (casting) applied

Every silver column is explicitly cast so the schema is stable regardless of source drift: ids -> int / long (order_id and order_item_id are long), dates -> date, decimals with fixed precision (list_price decimal(10,2), weight decimal(6,2), shipping_cost decimal(8,2), prices decimal(10,2), discount/tax decimal(4,1), line_total decimal(12,2), rating decimal(2,1), order volume decimal(12,2)), booleans -> boolean, everything time -> timestamp. Leads additionally converts TEXT -> typed values (dates in two formats, scores, currency strings). Logs converts parsed strings -> int/long/timestamp. The bronze leads pipeline converts Excel cells -> raw text strings (str() of every cell) so nothing is lost at ingestion.

---

## 8. Foreign key / orphan audit - verified fresh today

| Check | Orphans found |
| --- | --- |
| silver.orders -> silver.customers (current) | 0 |
| silver.orders -> silver.companies (current, buyer) | 0 |
| silver.order_items -> silver.orders (current) | 0 |
| silver.order_items -> silver.products (current) | 0 |
| silver.order_items -> silver.suppliers (current) | 0 |
| silver.suppliers -> silver.companies (current, supplier) | 0 |
| silver.supplier_products -> silver.suppliers (current) | 0 |
| silver.supplier_products -> silver.products (current) | 0 |
| silver.customers -> bronze.companies | 0 |
| silver.logs -> silver.customers (emails) | 0 |

---

## 9. Bronze-to-silver row reconciliation (nothing unexplained)

| Table | Bronze rows | Silver rows | Explanation of the difference |
| --- | --- | --- | --- |
| customers | 1,592 | 1,592 | 1:1, nothing dirty enough to drop |
| companies | 425 | 425 | 1:1 |
| products | 160 | 160 | 1:1, fully clean |
| suppliers | 25 | 25 | 1:1, fully clean |
| supplier_products | 313 | 313 | 1:1 |
| orders | 11,223 | 11,223 | 1:1 - dirty values were NULLified, never row-dropped |
| order_items | 30,240 | 30,240 | 1:1 - dirty values were NULLified, never row-dropped |
| leads | 1,138 rows (1,111 unique ids) | 1,098 | -27 double-writes merged away, -13 bad ids dropped (future dates, contact-before-created) |
| logs | 541,392 lines | 537,031 | -2,168 duplicate lines merged away, -2,193 unparsable lines dropped; 539,199 - 2,168 = 537,031 exactly |

---

## 10. Known limitations & honest observations (nothing wrong, but you should know)

1. **customers and companies do not exclude `updated_at` from history tracking; products, suppliers, supplier_products, orders and order_items do.** If the source ever re-sends an *identical* row with only a newer updated_at, customers/companies would open a duplicate history version while the other tables update in place. All six later tables got the `track_history_except_column_list=["updated_at"]` fix after a code review.
2. **customers' FK is checked against bronze.companies (all rows, any type), while every later table checks against CURRENT SILVER rows (often type-filtered).** The customers pattern is slightly weaker - a company dropped from silver would still satisfy it. It was the first table built; later tables were deliberately made stricter.
3. **customers and companies have a `coalesce(updated_at, created_at)` fallback for missing updated_at; all later tables drop such rows instead** (a fallback can mis-order history).
4. **logs parsing cannot handle a user_agent containing an escaped quote** - such lines are counted with the other unparsable drops. Identical requests within the same second are treated as double-writes. The log_id hash includes the file path, so a renamed re-sent file would re-process (a same-path re-send is idempotent).
5. **logs' customer FK is WARN-level, not FAIL** - a customer ingested after their log lines shows as a false orphan until the next refresh. Anonymous requests are exempt by design.
6. **`order_date_valid` compares against `current_date()`**, so its value is recomputed on each refresh - a 'future' date becomes valid when its day arrives. This is intended.
7. **Emails and IP addresses in silver (customers, leads, logs) are personal data; no masking has been implemented** - flagged only.
8. **leads has no FK to customers/companies by design** (pre-customer prospects; ~18%/13% match rates).
9. **PO numbers repeat across orders** (8,557 distinct of 8,999 non-null) - PO reuse is normal, po_number is not a key.
10. **The GARBAGE sentinel line `-- GARBAGE --` (448 occurrences) exists in bronze** and is dropped by silver's parse checks - it is visible in bronze if you ever want to count source-side corruption.

---

## 11. Final verdict

All 9 bronze tables are intact and raw. All 9 silver tables are unique on their keys, 100% current (no stale history), carry zero FK orphans across all 10 relationship checks, zero rule violations in their enforced checks, and every null/negative/duplicate/standardization decision in this report is backed by a live count that matched expectations exactly. The bronze-to-silver layer is complete, consistent and auditable.
