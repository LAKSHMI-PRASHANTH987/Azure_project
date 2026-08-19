# Databricks notebook source
# MAGIC %md
# MAGIC ## FHIR Bronze Layer – JSON to Delta
# MAGIC
# MAGIC Reads raw FHIR JSON pages stored by the Raw ingestion notebook, parses
# MAGIC each FHIR Bundle, flattens the resource entries into tabular rows, adds
# MAGIC metadata columns, and writes the result to Bronze Delta tables.
# MAGIC
# MAGIC - Runs **after** `fhir_raw_ingest` for the same date range.
# MAGIC - Uses `mergeSchema=true` so schema evolution is handled automatically.
# MAGIC - Partitioned by `ingest_date` so reprocessing a date is idempotent
# MAGIC   (partition overwrite).
# MAGIC
# MAGIC **Metadata columns added:**
# MAGIC | Column | Description |
# MAGIC |---|---|
# MAGIC | `extraction_timestamp` | UTC time the API page was fetched |
# MAGIC | `api_url` | Full URL used to fetch the page |
# MAGIC | `ingest_date` | Calendar date of the incremental load |
# MAGIC | `bronze_load_timestamp` | UTC time this notebook wrote the row |
# MAGIC | `record_hash` | SHA-256 of business columns (for SCD2 downstream) |

# COMMAND ----------
# MAGIC %run ../fhir_config

# COMMAND ----------
import json
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DateType

# COMMAND ----------
# ── Widget parameters ─────────────────────────────────────────────────────────
dbutils.widgets.text("catalog",    "databricksazureproject")
dbutils.widgets.text("schema",     "fhir")
dbutils.widgets.text("start_date", "2024-01-01")
dbutils.widgets.text("end_date",   "2024-01-03")

CATALOG    = dbutils.widgets.get("catalog")
SCHEMA     = dbutils.widgets.get("schema")
START_DATE = dbutils.widgets.get("start_date")
END_DATE   = dbutils.widgets.get("end_date")

# COMMAND ----------
# ── Helper: parse raw JSON pages for one resource/date into rows ───────────────
def parse_raw_pages(resource: str, ingest_date: str) -> list[dict]:
    """Reads all JSON pages for a resource/date, returns list of flattened rows."""
    path    = raw_path(CATALOG, SCHEMA, resource, ingest_date)
    flatten = FLATTEN_MAP[resource]
    rows    = []

    try:
        files = dbutils.fs.ls(path)
    except Exception:
        print(f"  No raw files found at {path} – skipping.")
        return rows

    for f in sorted(files, key=lambda x: x.name):
        if not f.name.endswith(".json"):
            continue
        raw_text = dbutils.fs.head(f.path, 1 << 24)   # up to 16 MB
        bundle   = json.loads(raw_text)
        entries  = bundle.get("entry") or []

        # Reconstruct the URL that produced this page (stored in Bundle.link.self)
        self_url = next(
            (lk["url"] for lk in bundle.get("link", []) if lk.get("relation") == "self"),
            f.path
        )

        for entry in entries:
            res_obj = entry.get("resource") or {}
            if res_obj.get("resourceType") != resource:
                continue
            flat_row = flatten(res_obj)
            flat_row["extraction_timestamp"] = datetime.utcnow().isoformat()
            flat_row["api_url"]              = self_url
            flat_row["ingest_date"]          = ingest_date
            rows.append(flat_row)

    return rows

# COMMAND ----------
# ── Helper: write rows to a Bronze Delta table (partition overwrite) ───────────
def write_bronze(resource: str, rows: list[dict], ingest_date: str) -> None:
    if not rows:
        print(f"  No rows to write for {resource}.")
        return

    id_col  = RESOURCE_ID_COL[resource]
    tbl     = bronze_table(CATALOG, SCHEMA, resource)

    df = (
        spark.createDataFrame(rows)
        .withColumn("ingest_date",          F.lit(ingest_date).cast(DateType()))
        .withColumn("bronze_load_timestamp", F.current_timestamp())
    )

    # Compute record hash from business columns (add after all metadata cols exist)
    hash_udf = F.udf(lambda row_json: compute_hash(json.loads(row_json)), StringType())
    df = df.withColumn(
        "record_hash",
        hash_udf(F.to_json(F.struct(*[c for c in df.columns])))
    )

    # Deduplicate within the batch: keep latest entry per resource id
    df = df.dropDuplicates([id_col])

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"ingest_date = '{ingest_date}'")
        .option("mergeSchema", "true")
        .saveAsTable(tbl)
    )
    print(f"  Written {df.count()} rows → {tbl}")

# COMMAND ----------
# ── Main loop ─────────────────────────────────────────────────────────────────
start_dt   = datetime.strptime(START_DATE, "%Y-%m-%d").date()
end_dt     = datetime.strptime(END_DATE,   "%Y-%m-%d").date()

current_day = start_dt
while current_day < end_dt:
    day_str = current_day.isoformat()
    print(f"\n{'='*60}\nBronze processing for date: {day_str}")

    for resource in FHIR_RESOURCES:
        print(f"  Resource: {resource}")
        rows = parse_raw_pages(resource, day_str)
        write_bronze(resource, rows, day_str)

    current_day += timedelta(days=1)

print("\nBronze layer complete.")
