# Databricks notebook source
# MAGIC %md
# MAGIC ## FHIR Gold Layer – Delta Live Tables
# MAGIC
# MAGIC Reads current (is_current = true) records from Silver tables and produces
# MAGIC analytics-ready Gold tables optimised for reporting.
# MAGIC
# MAGIC ### Tables produced
# MAGIC | Table | Description |
# MAGIC |---|---|
# MAGIC | `gold_patient` | Clean patient dimension |
# MAGIC | `gold_encounter` | Encounters enriched with patient demographics |
# MAGIC | `gold_observation` | Observations enriched with patient + encounter |
# MAGIC | `gold_condition` | Conditions enriched with patient + encounter |
# MAGIC | `gold_patient_health_summary` | Aggregated per-patient health metrics |
# MAGIC
# MAGIC Pipeline configuration must supply `source_catalog` and `source_schema`
# MAGIC so the fully-qualified Silver table names can be resolved at runtime.

# COMMAND ----------
import dlt
from pyspark.sql import functions as F

# ── Resolve source catalog/schema from pipeline configuration ─────────────────
source_catalog = spark.conf.get("source_catalog")
source_schema  = spark.conf.get("source_schema")


def silver(resource: str):
    """Return the fully-qualified Silver table name."""
    return f"{source_catalog}.{source_schema}_silver.{resource.lower()}"


def current(resource: str):
    """Read only the current (is_current=true) Silver rows for a resource."""
    return spark.read.table(silver(resource)).filter(F.col("is_current") == True)

# COMMAND ----------
# ── gold_patient ──────────────────────────────────────────────────────────────
@dlt.table(
    name    = "gold_patient",
    comment = "Current patient dimension – one row per patient",
    table_properties = {"quality": "gold"},
)
def gold_patient():
    return (
        current("Patient")
        .select(
            "patient_id", "family_name", "given_name", "gender",
            "birth_date", "city", "state", "country", "postal_code",
            "phone", "email", "language", "active",
            "effective_start_date", "record_version", "silver_load_timestamp",
        )
    )

# COMMAND ----------
# ── gold_encounter ────────────────────────────────────────────────────────────
@dlt.table(
    name    = "gold_encounter",
    comment = "Encounters enriched with patient demographics",
    table_properties = {"quality": "gold"},
)
def gold_encounter():
    enc = current("Encounter")
    pat = (
        current("Patient")
        .select(
            F.col("patient_id").alias("pat_id"),
            F.concat_ws(" ", "given_name", "family_name").alias("patient_name"),
            "gender", "birth_date",
        )
    )
    return (
        enc.join(pat, enc.patient_id == pat.pat_id, how="left")
        .select(
            "encounter_id", "status", "class_code", "class_display",
            "type_code", "type_display",
            enc.patient_id, "patient_name", "gender", "birth_date",
            "period_start", "period_end", "reason_code", "reason_display",
            "service_provider",
            enc.effective_start_date, enc.record_version,
        )
    )

# COMMAND ----------
# ── gold_observation ──────────────────────────────────────────────────────────
@dlt.table(
    name    = "gold_observation",
    comment = "Clinical observations enriched with patient and encounter context",
    table_properties = {"quality": "gold"},
)
def gold_observation():
    obs = current("Observation")
    pat = (
        current("Patient")
        .select(
            F.col("patient_id").alias("pat_id"),
            F.concat_ws(" ", "given_name", "family_name").alias("patient_name"),
            "gender", "birth_date",
        )
    )
    enc = (
        current("Encounter")
        .select(
            F.col("encounter_id").alias("enc_id"),
            F.col("class_code").alias("enc_class"),
            F.col("period_start").alias("enc_start"),
        )
    )
    return (
        obs
        .join(pat, obs.patient_id == pat.pat_id, how="left")
        .join(enc, obs.encounter_id == enc.enc_id, how="left")
        .select(
            "observation_id", "status", "category", "code", "code_display",
            obs.patient_id, "patient_name", "gender", "birth_date",
            obs.encounter_id, "enc_class", "enc_start",
            "effective_datetime", "issued",
            "value_quantity", "value_unit", "value_string", "value_code",
            "component_code", "component_value", "component_unit",
            obs.effective_start_date, obs.record_version,
        )
    )

# COMMAND ----------
# ── gold_condition ────────────────────────────────────────────────────────────
@dlt.table(
    name    = "gold_condition",
    comment = "Clinical conditions enriched with patient and encounter context",
    table_properties = {"quality": "gold"},
)
def gold_condition():
    cond = current("Condition")
    pat  = (
        current("Patient")
        .select(
            F.col("patient_id").alias("pat_id"),
            F.concat_ws(" ", "given_name", "family_name").alias("patient_name"),
            "gender", "birth_date",
        )
    )
    enc  = (
        current("Encounter")
        .select(
            F.col("encounter_id").alias("enc_id"),
            F.col("class_code").alias("enc_class"),
        )
    )
    return (
        cond
        .join(pat,  cond.patient_id == pat.pat_id,  how="left")
        .join(enc,  cond.encounter_id == enc.enc_id, how="left")
        .select(
            "condition_id", "clinical_status", "verification_status",
            "category", "code", "code_display",
            cond.patient_id, "patient_name", "gender", "birth_date",
            cond.encounter_id, "enc_class",
            "onset_datetime", "abatement_datetime", "recorded_date", "severity",
            cond.effective_start_date, cond.record_version,
        )
    )

# COMMAND ----------
# ── gold_patient_health_summary ───────────────────────────────────────────────
@dlt.table(
    name    = "gold_patient_health_summary",
    comment = "Aggregated per-patient health metrics across encounters, observations, conditions",
    table_properties = {"quality": "gold"},
)
def gold_patient_health_summary():
    pat  = dlt.read("gold_patient")
    enc  = dlt.read("gold_encounter")
    obs  = dlt.read("gold_observation")
    cond = dlt.read("gold_condition")

    enc_agg = (
        enc.groupBy("patient_id")
        .agg(
            F.count("encounter_id").alias("total_encounters"),
            F.min("period_start").alias("first_encounter_date"),
            F.max("period_start").alias("last_encounter_date"),
        )
    )
    obs_agg = (
        obs.groupBy("patient_id")
        .agg(F.count("observation_id").alias("total_observations"))
    )
    cond_agg = (
        cond.groupBy("patient_id")
        .agg(
            F.count("condition_id").alias("total_conditions"),
            F.count(
                F.when(F.col("clinical_status") == "active", 1)
            ).alias("active_conditions"),
        )
    )

    return (
        pat
        .join(enc_agg,  on="patient_id", how="left")
        .join(obs_agg,  on="patient_id", how="left")
        .join(cond_agg, on="patient_id", how="left")
        .select(
            "patient_id", "family_name", "given_name", "gender", "birth_date",
            "city", "state", "country", "active",
            F.coalesce("total_encounters",   F.lit(0)).alias("total_encounters"),
            "first_encounter_date", "last_encounter_date",
            F.coalesce("total_observations", F.lit(0)).alias("total_observations"),
            F.coalesce("total_conditions",   F.lit(0)).alias("total_conditions"),
            F.coalesce("active_conditions",  F.lit(0)).alias("active_conditions"),
        )
    )
