# Databricks notebook source
# MAGIC %md
# MAGIC ## FHIR Raw Layer – Incremental API Ingestion
# MAGIC
# MAGIC Fetches data from the public HAPI FHIR R4 API for each configured resource
# MAGIC over a date range, paginates through all pages, and stores every raw JSON
# MAGIC response page as a file on DBFS.  A metadata Delta table records every
# MAGIC API call (URL, page, record count, status) for full audit trail.
# MAGIC
# MAGIC **Resources ingested (in order):** Patient → Encounter → Observation → Condition
# MAGIC
# MAGIC **Widgets:**
# MAGIC | Widget | Default | Description |
# MAGIC |---|---|---|
# MAGIC | `catalog` | `databricksazureproject` | Unity Catalog name |
# MAGIC | `schema`  | `fhir` | Schema prefix |
# MAGIC | `start_date` | `2024-01-01` | Inclusive start date (yyyy-MM-dd) |
# MAGIC | `end_date`   | `2024-01-03` | Exclusive end date (yyyy-MM-dd) |

# COMMAND ----------
# MAGIC %run ../fhir_config

# COMMAND ----------
import requests
import time
import uuid
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType
)

# COMMAND ----------
# ── Widget parameters ─────────────────────────────────────────────────────────
dbutils.widgets.text("catalog",    "databricksazureproject")
dbutils.widgets.text("schema",     "fhir")
dbutils.widgets.text("start_date", "2024-01-01")
dbutils.widgets.text("end_date",   "2024-01-03")

CATALOG     = dbutils.widgets.get("catalog")
SCHEMA      = dbutils.widgets.get("schema")
START_DATE  = dbutils.widgets.get("start_date")
END_DATE    = dbutils.widgets.get("end_date")

print(f"Catalog={CATALOG} | Schema={SCHEMA} | Range=[{START_DATE}, {END_DATE})")

# COMMAND ----------
# ── Create schemas if not present ────────────────────────────────────────────
spark.sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}_bronze`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}_silver`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}_gold`")

# COMMAND ----------
# ── Metadata table DDL ────────────────────────────────────────────────────────
META_TABLE = api_metadata_table(CATALOG, SCHEMA)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {META_TABLE} (
        run_id              STRING,
        resource_name       STRING,
        ingest_date         DATE,
        page_number         INT,
        api_url             STRING,
        records_on_page     INT,
        total_records       INT,
        extraction_timestamp TIMESTAMP,
        status              STRING,
        error_message       STRING
    )
    USING DELTA
    PARTITIONED BY (ingest_date)
""")

# COMMAND ----------
# ── Helper: build initial URL for a resource / date ───────────────────────────
def build_initial_url(resource: str, day: date) -> str:
    next_day = day + timedelta(days=1)
    return (
        f"{FHIR_BASE_URL}/{resource}"
        f"?_lastUpdated=ge{day.isoformat()}T00:00:00Z"
        f"&_lastUpdated=lt{next_day.isoformat()}T00:00:00Z"
        f"&_count={PAGE_SIZE}"
        f"&_format=json"
    )

def get_next_link(bundle: dict) -> Optional[str]:
    for link in bundle.get("link", []):
        if link.get("relation") == "next":
            return link.get("url")
    return None

# COMMAND ----------
# ── Helper: fetch one page with retry ─────────────────────────────────────────
def fetch_page(url: str) -> dict:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            if attempt == MAX_RETRIES:
                raise
            print(f"  Attempt {attempt} failed ({exc}). Retrying in {RETRY_BACKOFF}s …")
            time.sleep(RETRY_BACKOFF)

# COMMAND ----------
# ── Helper: write one raw page to DBFS ────────────────────────────────────────
def save_raw_page(resource: str, ingest_date: str, page_num: int, data: dict) -> str:
    path = raw_path(CATALOG, SCHEMA, resource, ingest_date)
    file_path = f"{path}/page_{page_num:04d}.json"
    dbutils.fs.put(file_path, json.dumps(data), overwrite=True)
    return file_path

# COMMAND ----------
# ── Helper: log a metadata row ────────────────────────────────────────────────
def log_metadata(run_id, resource, day, page_num, url, records, total, ts, status, error=""):
    row = spark.createDataFrame([{
        "run_id":               run_id,
        "resource_name":        resource,
        "ingest_date":          day,
        "page_number":          page_num,
        "api_url":              url,
        "records_on_page":      records,
        "total_records":        total,
        "extraction_timestamp": ts,
        "status":               status,
        "error_message":        error,
    }])
    row.write.format("delta").mode("append").saveAsTable(META_TABLE)

# COMMAND ----------
# ── Main ingestion loop ───────────────────────────────────────────────────────
run_id     = str(uuid.uuid4())
start_dt   = datetime.strptime(START_DATE, "%Y-%m-%d").date()
end_dt     = datetime.strptime(END_DATE,   "%Y-%m-%d").date()

# Iterate each day in the range
current_day = start_dt
while current_day < end_dt:
    day_str = current_day.isoformat()
    print(f"\n{'='*60}")
    print(f"Date: {day_str}")

    for resource in FHIR_RESOURCES:
        print(f"  Resource: {resource}")
        url       = build_initial_url(resource, current_day)
        page_num  = 1
        total_rec = 0

        while url:
            ts = datetime.utcnow()
            try:
                bundle     = fetch_page(url)
                entries    = bundle.get("entry", [])
                total      = bundle.get("total", 0)
                records_on = len(entries)
                total_rec += records_on

                save_raw_page(resource, day_str, page_num, bundle)
                log_metadata(run_id, resource, current_day, page_num,
                             url, records_on, total, ts, "success")

                print(f"    Page {page_num}: {records_on} records (total available: {total})")

                url      = get_next_link(bundle)
                page_num += 1

            except Exception as exc:
                log_metadata(run_id, resource, current_day, page_num,
                             url, 0, 0, ts, "failed", str(exc))
                print(f"    ERROR on page {page_num}: {exc}")
                break   # skip remaining pages for this resource/day on persistent failure

        print(f"    → {total_rec} records saved for {resource} on {day_str}")

    current_day += timedelta(days=1)

print("\nRaw ingestion complete.")
