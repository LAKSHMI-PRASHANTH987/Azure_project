# Databricks notebook source
# MAGIC %md
# MAGIC ## FHIR Silver Layer – Cleansing & SCD Type 2
# MAGIC
# MAGIC Reads new records from the Bronze Delta tables and applies
# MAGIC **Slowly Changing Dimension Type 2** logic to maintain a full history in
# MAGIC the Silver layer.
# MAGIC
# MAGIC ### SCD Type 2 Strategy
# MAGIC | Scenario | Action |
# MAGIC |---|---|
# MAGIC | Brand-new resource ID | INSERT row with `is_current=true`, `effective_start_date=today`, `effective_end_date=NULL` |
# MAGIC | Existing ID, hash unchanged | No-op (skip) |
# MAGIC | Existing ID, hash changed | UPDATE old row (`is_current=false`, `effective_end_date=today-1`) then INSERT new version |
# MAGIC
# MAGIC ### Silver columns added
# MAGIC | Column | Description |
# MAGIC |---|---|
# MAGIC | `effective_start_date` | Date this version became active |
# MAGIC | `effective_end_date` | Date this version was superseded (`NULL` = current) |
# MAGIC | `is_current` | Boolean flag – `true` for the active version |
# MAGIC | `record_version` | Monotonically increasing version number per resource ID |
# MAGIC | `silver_load_timestamp` | UTC timestamp when this row was written |

# COMMAND ----------
# MAGIC %run ../fhir_config

# COMMAND ----------
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, BooleanType, IntegerType, TimestampType

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

print(f"Silver SCD2 | Catalog={CATALOG} | Schema={SCHEMA} | Range=[{START_DATE}, {END_DATE})")

# COMMAND ----------
# ── Helper: create Silver table if absent ─────────────────────────────────────
def ensure_silver_table(resource: str, sample_df) -> None:
    """Creates the silver table from the bronze schema plus SCD2 audit columns."""
    tbl = silver_table(CATALOG, SCHEMA, resource)
    if not spark.catalog.tableExists(tbl):
        (
            sample_df
            .limit(0)
            .withColumn("effective_start_date",  F.current_date().cast(DateType()))
            .withColumn("effective_end_date",    F.lit(None).cast(DateType()))
            .withColumn("is_current",            F.lit(True).cast(BooleanType()))
            .withColumn("record_version",        F.lit(1).cast(IntegerType()))
            .withColumn("silver_load_timestamp", F.current_timestamp().cast(TimestampType()))
            .write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(tbl)
        )
        print(f"  Created silver table {tbl}")

# COMMAND ----------
# ── SCD Type 2 merge for one resource ─────────────────────────────────────────
def apply_scd2(resource: str) -> None:
    id_col    = RESOURCE_ID_COL[resource]
    bronze_tbl = bronze_table(CATALOG, SCHEMA, resource)
    silver_tbl = silver_table(CATALOG, SCHEMA, resource)

    # ── Load today's bronze data ──────────────────────────────────────────────
    try:
        new_df = spark.read.table(bronze_tbl).filter(
            F.col("ingest_date") == F.lit(PROCESS_DATE).cast(DateType())  # PROCESS_DATE set by outer loop
        )
    except Exception as exc:
        print(f"  Could not load bronze for {resource}: {exc}")
        return

    if new_df.rdd.isEmpty():
        print(f"  No new bronze data for {resource} on {PROCESS_DATE}")
        return

    ensure_silver_table(resource, new_df)

    # ── Read current silver (is_current = true) ───────────────────────────────
    current_silver = spark.read.table(silver_tbl).filter(F.col("is_current") == True)

    # ── Identify changed records (same ID, different hash) ────────────────────
    changed_ids = (
        new_df.alias("new")
        .join(current_silver.alias("cur"), on=id_col, how="inner")
        .filter(F.col("new.record_hash") != F.col("cur.record_hash"))
        .select(F.col(f"new.{id_col}").alias(id_col))
    )

    # ── Identify brand-new records (not in silver at all) ─────────────────────
    all_silver_ids = spark.read.table(silver_tbl).select(id_col).distinct()
    new_ids = (
        new_df.select(id_col)
        .join(all_silver_ids, on=id_col, how="left_anti")
        .distinct()
    )

    ids_to_insert = changed_ids.union(new_ids).distinct()
    records_to_insert = new_df.join(ids_to_insert, on=id_col, how="inner")

    if records_to_insert.rdd.isEmpty():
        print(f"  No changes detected for {resource} on {PROCESS_DATE}")
        return

    # ── Step 1: expire changed records in silver ──────────────────────────────
    (
        DeltaTable.forName(spark, silver_tbl).alias("target")
        .merge(
            changed_ids.alias("src"),
            f"target.{id_col} = src.{id_col} AND target.is_current = true"
        )
        .whenMatchedUpdate(set={
            "is_current":            F.lit(False),
            "effective_end_date":    F.date_sub(F.lit(PROCESS_DATE).cast(DateType()), 1),
            "silver_load_timestamp": F.current_timestamp(),
        })
        .execute()
    )

    # ── Step 2: compute version numbers for new rows ──────────────────────────
    max_versions = (
        spark.read.table(silver_tbl)
        .groupBy(id_col)
        .agg(F.max("record_version").alias("max_ver"))
    )

    insert_df = (
        records_to_insert
        .join(max_versions, on=id_col, how="left")
        .withColumn(
            "record_version",
            F.coalesce(F.col("max_ver"), F.lit(0)).cast(IntegerType()) + F.lit(1)
        )
        .drop("max_ver")
        .withColumn("effective_start_date",  F.lit(PROCESS_DATE).cast(DateType()))
        .withColumn("effective_end_date",    F.lit(None).cast(DateType()))
        .withColumn("is_current",            F.lit(True).cast(BooleanType()))
        .withColumn("silver_load_timestamp", F.current_timestamp())
    )

    # ── Step 3: append new versions ───────────────────────────────────────────
    (
        insert_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(silver_tbl)
    )

    ins_count = insert_df.count()
    print(f"  {resource}: {ins_count} rows inserted/updated in silver (SCD2).")

# COMMAND ----------
# ── Run SCD2 for all resources across every date in the range ────────────────
start_dt    = datetime.strptime(START_DATE, "%Y-%m-%d").date()
end_dt      = datetime.strptime(END_DATE,   "%Y-%m-%d").date()
current_day = start_dt

while current_day < end_dt:
    PROCESS_DATE = current_day.isoformat()   # used inside apply_scd2
    print(f"\n{'='*60}\nSilver SCD2 for date: {PROCESS_DATE}")
    for resource in FHIR_RESOURCES:
        print(f"  Resource: {resource}")
        apply_scd2(resource)
    current_day += timedelta(days=1)

print("\nSilver layer complete.")
