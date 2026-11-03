"""
B2B pipeline: run the data generators, then load the results.

  gen_postgres ──> gen_web_logs ──> gen_leads ──> upload_to_s3   data/output files -> S3
       └──────────> sync_to_neon                                 source Postgres (docker) -> Neon, upsert on primary key

No schedule for now: trigger it manually.
  mode "daily" (default): generates and loads one day = the run's logical date, or the day the run was created.
  mode "backfill":        generators run `backfill` (2 years up to yesterday); full sync + whole data/output uploaded.

The day is fixed when the run is created (not when each task starts), so retries and midnight rollovers stay consistent.
Connections / variables (airflow_settings.yaml): postgres_default (source), neon_postgres, aws_default, s3_bucket.
"""
import re
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import timedelta
from pathlib import Path

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import BaseHook, Param, Variable, dag, task

GEN_DIR = "/usr/local/airflow/data_generation"  # generator scripts (mounted from ../data_generation)
DATA_DIR = "/usr/local/airflow/data"  # scripts write to ./output, i.e. the project's data/output folder
OUTPUT_DIR = Path(DATA_DIR) / "output"

# parent tables first (foreign keys) -> primary key columns used for the upsert
TABLES = {
    "companies": ("company_id",),
    "products": ("product_id",),
    "suppliers": ("supplier_id",),
    "supplier_products": ("supplier_id", "product_id"),
    "customers": ("customer_id",),
    "orders": ("order_id",),
    "order_items": ("order_item_id",),
}

# generator arguments: `backfill`, or `daily <date>` with the date fixed per run
RUN_DAY = "{{ (dag_run.logical_date or dag_run.run_after).strftime('%Y-%m-%d') }}"
SCRIPT_ARGS = "{% if params.mode == 'backfill' %}backfill{% else %}daily " + RUN_DAY + "{% endif %}"
# gen_postgres.py reads the database it writes to from PG_DSN; build it from the postgres_default connection
PG_DSN = (
    "host={{ conn.postgres_default.host }} port={{ conn.postgres_default.port }} "
    "dbname={{ conn.postgres_default.schema }} user={{ conn.postgres_default.login }} "
    "password={{ conn.postgres_default.password }}"
)


def pg_connect(conn_id):
    """psycopg2 connection built from an Airflow connection (the hook may return a psycopg3 one)."""
    import psycopg2

    c = BaseHook.get_connection(conn_id)
    return psycopg2.connect(host=c.host, port=c.port, dbname=c.schema, user=c.login, password=c.password, **c.extra_dejson)


def generator(script, env=None):
    return BashOperator(
        task_id=script,
        bash_command=f"cd {DATA_DIR} && python {GEN_DIR}/{script}.py {SCRIPT_ARGS}",
        env=env,
        append_env=True,
    )


@dag(
    schedule=None,  # manual trigger only for now
    max_active_runs=1,  # the generators share the data/output folder
    params={"mode": Param("daily", enum=["daily", "backfill"], description="daily = one day, backfill = full history")},
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=1),
    },
    tags=["b2b"],
)
def b2b_pipeline():
    gen_postgres = generator("gen_postgres", env={"PG_DSN": PG_DSN})
    gen_web_logs = generator("gen_web_logs")
    gen_leads = generator("gen_leads")

    @task
    def sync_to_neon(**context):
        from psycopg2.extras import execute_values

        # reuse the DDL from gen_postgres.py so Neon always has the same schema as the source
        match = re.search(r'DDL = """(.*?)"""', Path(f"{GEN_DIR}/gen_postgres.py").read_text(), re.S)
        if not match:
            raise ValueError("DDL block not found in gen_postgres.py")
        # a backfill inserts old rows after newer ones were synced, so the updated_at watermark can't be trusted
        full_sync = context["params"]["mode"] == "backfill"

        with closing(pg_connect("postgres_default")) as src, closing(pg_connect("neon_postgres")) as dst:
            with dst.cursor() as cur:
                cur.execute(match.group(1))
            dst.commit()

            for table, pk in TABLES.items():
                where, args = "", None
                if not full_sync:  # only rows changed since the last sync (3-day lookback; the upsert is idempotent)
                    with dst.cursor() as cur:
                        cur.execute(f"SELECT max(updated_at) FROM {table}")
                        watermark = cur.fetchone()[0]
                    if watermark:
                        where, args = "WHERE updated_at >= %s", (watermark - timedelta(days=3),)

                with src.cursor() as cur:
                    cur.execute(f"SELECT * FROM {table} LIMIT 0")
                    cols = [c.name for c in cur.description]
                updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c not in pk)
                upsert = (
                    f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s "
                    f"ON CONFLICT ({', '.join(pk)}) DO UPDATE SET {updates}"
                )

                total = 0
                with src.cursor(name=f"sync_{table}") as read, dst.cursor() as write:
                    read.itersize = 5000
                    read.execute(f"SELECT {', '.join(cols)} FROM {table} {where}", args)
                    while rows := read.fetchmany(5000):
                        execute_values(write, upsert, rows, page_size=5000)
                        total += len(rows)
                dst.commit()
                print(f"{table}: {total} rows upserted into Neon")

    @task
    def upload_to_s3(**context):
        if context["params"]["mode"] == "backfill":
            files = sorted(p for p in OUTPUT_DIR.rglob("*") if p.is_file())
        else:  # the run's day + the reference files
            dag_run = context["dag_run"]
            day = (dag_run.logical_date or dag_run.run_after).date().isoformat()
            files = [
                OUTPUT_DIR / "web_logs" / f"access_{day}.log",
                OUTPUT_DIR / "ip_geo_lookup.csv",
                OUTPUT_DIR / "bad_rows_manifest.csv",
            ]
            missing = [p.name for p in files if not p.exists()]
            if missing:
                raise FileNotFoundError(f"generators did not produce: {missing}")
            leads = OUTPUT_DIR / "leads" / f"leads_{day}.xlsx"
            if leads.exists():  # no leads file on days without leads
                files.append(leads)
        if not files:
            raise FileNotFoundError(f"nothing to upload in {OUTPUT_DIR}")

        bucket = Variable.get("s3_bucket")
        hook = S3Hook(aws_conn_id="aws_default")
        hook.get_conn()  # resolve the connection once, before the worker threads start

        def upload(path):
            hook.load_file(filename=str(path), key=path.relative_to(OUTPUT_DIR).as_posix(), bucket_name=bucket, replace=True)

        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(upload, files))  # list() re-raises any upload error
        print(f"uploaded {len(files)} files to s3://{bucket}")

    gen_postgres >> gen_web_logs >> gen_leads >> upload_to_s3()
    gen_postgres >> sync_to_neon()


b2b_pipeline()
