"""Seed the operational PostgreSQL DB.
  python gen_postgres.py backfill      # 2 years of history up to yesterday
  python gen_postgres.py daily [DATE]  # one day (default today) + advances status of ~40 open orders (simulates source changes)
Connection: env PG_DSN (default below). Safe to re-run (ON CONFLICT DO NOTHING).
"""
import csv, os
import psycopg2
from psycopg2.extras import execute_values
from common import build_reference, build_orders, days_from_cli, utc_now, HARD_MODE, OUT_DIR

DSN = os.getenv("PG_DSN", "host=localhost dbname=b2b user=postgres password=postgres")

DDL = """
CREATE TABLE IF NOT EXISTS companies (company_id INT PRIMARY KEY, name TEXT, cuit TEXT, company_type TEXT,
    country TEXT, city TEXT, rating NUMERIC(2,1), address TEXT, phone TEXT, contact_email TEXT,
    industry TEXT, employee_count INT, payment_terms TEXT, is_active BOOLEAN, created_at TIMESTAMP, updated_at TIMESTAMP);
CREATE TABLE IF NOT EXISTS products (product_id INT PRIMARY KEY, sku TEXT, name TEXT, category TEXT,
    list_price NUMERIC(10,2), brand TEXT, unit_of_measure TEXT, weight_kg NUMERIC(6,2), barcode TEXT,
    is_active BOOLEAN, min_order_qty INT, created_at TIMESTAMP, updated_at TIMESTAMP);
CREATE TABLE IF NOT EXISTS suppliers (supplier_id INT PRIMARY KEY, company_id INT REFERENCES companies (company_id),
    lead_time_days INT, contact_name TEXT, contact_email TEXT, min_order_value INT, is_preferred BOOLEAN,
    created_at TIMESTAMP, updated_at TIMESTAMP);
CREATE TABLE IF NOT EXISTS supplier_products (supplier_id INT REFERENCES suppliers (supplier_id),
    product_id INT REFERENCES products (product_id), supplier_price NUMERIC(10,2), supplier_sku TEXT,
    min_order_qty INT, lead_time_days INT, is_preferred BOOLEAN, valid_from DATE, created_at TIMESTAMP, updated_at TIMESTAMP,
    PRIMARY KEY (supplier_id, product_id));
CREATE TABLE IF NOT EXISTS customers (customer_id INT PRIMARY KEY, company_id INT REFERENCES companies (company_id),
    birth_date DATE, gender TEXT, first_name TEXT, last_name TEXT, email TEXT, phone TEXT, job_title TEXT,
    is_active BOOLEAN, document_number TEXT, created_at TIMESTAMP, updated_at TIMESTAMP);
CREATE TABLE IF NOT EXISTS orders (order_id BIGINT PRIMARY KEY, buyer_company_id INT REFERENCES companies (company_id),
    customer_id INT REFERENCES customers (customer_id), order_date DATE, status TEXT,
    payment_method TEXT, shipping_method TEXT, shipping_cost NUMERIC(8,2), po_number TEXT,
    expected_delivery_date DATE, channel TEXT, created_at TIMESTAMP, updated_at TIMESTAMP);
CREATE TABLE IF NOT EXISTS order_items (order_item_id BIGINT PRIMARY KEY, order_id BIGINT REFERENCES orders (order_id),
    product_id INT REFERENCES products (product_id), supplier_id INT REFERENCES suppliers (supplier_id),
    quantity INT, unit_price NUMERIC(10,2), line_number INT, discount_pct NUMERIC(4,1), tax_rate NUMERIC(4,1),
    line_total NUMERIC(12,2), warehouse TEXT, created_at TIMESTAMP, updated_at TIMESTAMP);
"""

# DB-side guard for CDC: created_at is immutable, and any UPDATE that forgets to move updated_at still moves it.
TRIGGERS = """
CREATE OR REPLACE FUNCTION touch_row() RETURNS trigger AS $$
BEGIN
  NEW.created_at := OLD.created_at;
  IF COALESCE(NEW.updated_at <= OLD.updated_at, true) THEN  -- not moved forward (or NULL): force it forward
    NEW.updated_at := GREATEST(clock_timestamp() AT TIME ZONE 'UTC', OLD.updated_at + interval '1 microsecond');
  END IF;
  RETURN NEW;
END $$ LANGUAGE plpgsql;
DO $$ DECLARE t text; BEGIN
  FOREACH t IN ARRAY ARRAY['companies','products','suppliers','supplier_products','customers','orders','order_items'] LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS trg_touch ON %I', t);
    EXECUTE format('CREATE TRIGGER trg_touch BEFORE UPDATE ON %I FOR EACH ROW EXECUTE FUNCTION touch_row()', t);
  END LOOP;
END $$;
"""

# core KPI columns first, then the "realism" extras
COLS = {
    "companies": "company_id name cuit company_type country city rating | address phone contact_email industry employee_count payment_terms is_active created_at updated_at",
    "products": "product_id sku name category list_price | brand unit_of_measure weight_kg barcode is_active min_order_qty created_at updated_at",
    "suppliers": "supplier_id company_id | lead_time_days contact_name contact_email min_order_value is_preferred created_at updated_at",
    "supplier_products": "supplier_id product_id supplier_price | supplier_sku min_order_qty lead_time_days is_preferred valid_from created_at updated_at",
    "customers": "customer_id company_id birth_date gender | first_name last_name email phone job_title is_active document_number created_at updated_at",
    "orders": "order_id buyer_company_id customer_id order_date status | payment_method shipping_method shipping_cost po_number expected_delivery_date channel created_at updated_at",
    "order_items": "order_item_id order_id product_id supplier_id quantity unit_price | line_number discount_pct tax_rate line_total warehouse created_at updated_at",
}

# orders and their items change together, so both move updated_at (CDC sees the items too)
ADVANCE_OPEN_ORDERS = """
WITH advanced AS (
  UPDATE orders SET updated_at = %(now)s,
    status = CASE status WHEN 'pending' THEN 'confirmed' WHEN 'confirmed' THEN 'shipped' ELSE 'delivered' END
  WHERE order_id IN (SELECT order_id FROM orders WHERE status IN ('pending','confirmed','shipped')
                     ORDER BY random() LIMIT 40)
  RETURNING order_id, updated_at)
UPDATE order_items i SET updated_at = a.updated_at FROM advanced a WHERE i.order_id = a.order_id;
"""

# the 5 reference tables also change over time (non-KPI columns, value always differs), so their updated_at moves too
UPDATE_DIMENSIONS = """
UPDATE companies SET updated_at = %(now)s,
  payment_terms = CASE payment_terms WHEN 'Net 15' THEN 'Net 30' WHEN 'Net 30' THEN 'Net 45' WHEN 'Net 45' THEN 'Prepaid' ELSE 'Net 15' END
 WHERE company_id IN (SELECT company_id FROM companies ORDER BY random() LIMIT 5);
UPDATE products SET updated_at = %(now)s,
  min_order_qty = CASE min_order_qty WHEN 1 THEN 5 WHEN 5 THEN 10 ELSE 1 END
 WHERE product_id IN (SELECT product_id FROM products ORDER BY random() LIMIT 5);
UPDATE suppliers SET updated_at = %(now)s,
  min_order_value = CASE min_order_value WHEN 0 THEN 500 WHEN 500 THEN 1000 WHEN 1000 THEN 2500 ELSE 0 END
 WHERE supplier_id IN (SELECT supplier_id FROM suppliers ORDER BY random() LIMIT 2);
UPDATE supplier_products SET updated_at = %(now)s,
  lead_time_days = CASE WHEN lead_time_days >= 30 THEN 2 ELSE lead_time_days + 1 END
 WHERE (supplier_id, product_id) IN (SELECT supplier_id, product_id FROM supplier_products ORDER BY random() LIMIT 5);
UPDATE customers SET updated_at = %(now)s,
  job_title = CASE job_title WHEN 'Purchasing Manager' THEN 'Procurement Analyst' WHEN 'Procurement Analyst' THEN 'Operations Director'
              WHEN 'Operations Director' THEN 'Office Manager' WHEN 'Office Manager' THEN 'CEO' WHEN 'CEO' THEN 'Logistics Coordinator'
              ELSE 'Purchasing Manager' END
 WHERE customer_id IN (SELECT customer_id FROM customers ORDER BY random() LIMIT 5);
"""

# late changes to OLD rows (30+ days): a missing PO number gets filled in, an item is re-allocated to another warehouse.
# the parent row moves with the child, so orders and their items stay consistent for CDC.
UPDATE_OLD_ROWS = """
WITH fixed AS (
  UPDATE orders SET updated_at = %(now)s,
    po_number = 'PO-' || lpad((10000 + floor(random() * 90000))::int::text, 5, '0')
  WHERE order_id IN (SELECT order_id FROM orders
                     WHERE po_number IS NULL AND created_at < %(now)s - interval '30 days'
                     ORDER BY random() LIMIT 5)
  RETURNING order_id, updated_at)
UPDATE order_items i SET updated_at = f.updated_at FROM fixed f WHERE i.order_id = f.order_id;
WITH moved AS (
  UPDATE order_items SET updated_at = %(now)s,
    warehouse = CASE warehouse WHEN 'BsAs-Central' THEN 'Cordoba-Norte' WHEN 'Cordoba-Norte' THEN 'Rosario-Sur' ELSE 'BsAs-Central' END
  WHERE order_item_id IN (SELECT order_item_id FROM order_items
                          WHERE created_at < %(now)s - interval '30 days' ORDER BY random() LIMIT 5)
  RETURNING order_id, updated_at)
UPDATE orders o SET updated_at = GREATEST(o.updated_at, m.u)
  FROM (SELECT order_id, max(updated_at) AS u FROM moved GROUP BY order_id) m WHERE o.order_id = m.order_id;
"""


def insert(cur, table, rows, load_ts=None):
    cols = [c for c in COLS[table].split() if c != "|"]
    if rows:
        vals = [[r[c] for c in cols] for r in rows]
        if load_ts is not None:  # CDC: a new row must be newer than anything already ingested -> updated_at >= load time
            u = cols.index("updated_at")
            for v in vals:
                v[u] = max(v[u], load_ts)
        execute_values(cur, f"INSERT INTO {table} ({','.join(cols)}) VALUES %s ON CONFLICT DO NOTHING",
                       vals, page_size=5000)


mode, days = days_from_cli()
ref = build_reference()
with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
    cur.execute(DDL)
    cur.execute(TRIGGERS)
    now = utc_now()  # one timestamp for the whole run
    cur.execute("SELECT EXISTS (SELECT 1 FROM orders)")
    has_data = cur.fetchone()[0]
    # first backfill into an empty DB keeps simulated history (first ingestion is a full snapshot);
    # any later insert (daily, past-date daily, re-run backfill) is stamped with load time so a cursor on updated_at can't skip it
    load_ts = now if (mode == "daily" or has_data) else None
    for t in ["companies", "products", "suppliers", "supplier_products", "customers"]:
        insert(cur, t, ref[t], load_ts)
    n_orders, bad = 0, (list(ref["bad"]) if mode == "backfill" else [])
    for day in days:
        orders = build_orders(day)
        insert(cur, "orders", orders, load_ts)
        insert(cur, "order_items", [i for o in orders for i in o["items"]], load_ts)
        n_orders += len(orders)
        bad += [b for o in orders for b in o["bad"]]
    if mode == "daily":
        cur.execute(ADVANCE_OPEN_ORDERS, {"now": now})  # status change -> updated_at moves (this is what CDC picks up)
        cur.execute(UPDATE_DIMENSIONS, {"now": now})
        cur.execute(UPDATE_OLD_ROWS, {"now": now})

# manifest of every injected error = ground truth for checking your data-quality rules
os.makedirs(OUT_DIR, exist_ok=True)
path = f"{OUT_DIR}/bad_rows_manifest.csv"
seen = set(map(tuple, csv.reader(open(path)))) if os.path.exists(path) and mode == "daily" else set()
with open(path, "w" if mode == "backfill" else "a", newline="") as f:
    w = csv.writer(f)
    if not seen:
        w.writerow(["table", "primary_key", "column", "issue"])
    w.writerows(b for b in map(lambda x: tuple(map(str, x)), bad) if b not in seen)
print(f"{mode}: {len(days)} day(s), {n_orders} orders generated, {len(bad)} injected bad values (HARD_MODE={int(HARD_MODE)})")