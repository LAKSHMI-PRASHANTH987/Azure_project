# Databricks notebook source
# MAGIC %md
# MAGIC ## FHIR Pipeline – Shared Configuration & Utilities
# MAGIC
# MAGIC Run this notebook via `%run` from any other FHIR pipeline notebook to load
# MAGIC constants, path helpers, and resource-specific flattening functions.

# COMMAND ----------
import json
import hashlib
from datetime import datetime, date, timedelta
from typing import Optional

# COMMAND ----------
# ── API constants ─────────────────────────────────────────────────────────────
FHIR_BASE_URL  = "https://hapi.fhir.org/baseR4"
FHIR_RESOURCES = ["Patient", "Encounter", "Observation", "Condition"]
PAGE_SIZE      = 50          # records per page (FHIR _count param)
MAX_RETRIES    = 3
RETRY_BACKOFF  = 2           # seconds between retries

RESOURCE_ID_COL = {
    "Patient":     "patient_id",
    "Encounter":   "encounter_id",
    "Observation": "observation_id",
    "Condition":   "condition_id",
}

# Columns excluded from record-hash computation (pure metadata / audit columns)
HASH_EXCLUDE = {
    "extraction_timestamp", "api_url", "ingest_date",
    "bronze_load_timestamp", "effective_start_date",
    "effective_end_date", "is_current", "record_hash",
    "silver_load_timestamp", "record_version",
}

# COMMAND ----------
# ── Path / table name helpers ─────────────────────────────────────────────────
def raw_path(catalog: str, schema: str, resource: str, ingest_date: str) -> str:
    """DBFS path where raw JSON pages are stored for a given resource/date."""
    return f"dbfs:/fhir/{catalog}/{schema}/raw/{resource}/date={ingest_date}"

def bronze_table(catalog: str, schema: str, resource: str) -> str:
    return f"{catalog}.{schema}_bronze.{resource.lower()}"

def silver_table(catalog: str, schema: str, resource: str) -> str:
    return f"{catalog}.{schema}_silver.{resource.lower()}"

def gold_table(catalog: str, schema: str, resource: str) -> str:
    return f"{catalog}.{schema}_gold.{resource.lower()}"

def api_metadata_table(catalog: str, schema: str) -> str:
    return f"{catalog}.{schema}_bronze.api_metadata"

# COMMAND ----------
# ── Hash helper ───────────────────────────────────────────────────────────────
def compute_hash(row_dict: dict) -> str:
    """SHA-256 of business columns only (metadata cols excluded)."""
    data = {k: v for k, v in sorted(row_dict.items()) if k not in HASH_EXCLUDE}
    return hashlib.sha256(
        json.dumps(data, default=str, sort_keys=True).encode()
    ).hexdigest()

# COMMAND ----------
# ── Resource-specific FHIR flatteners ─────────────────────────────────────────
def flatten_patient(res: dict) -> dict:
    name     = (res.get("name") or [{}])[0]
    address  = (res.get("address") or [{}])[0]
    telecom  = {t.get("system"): t.get("value") for t in (res.get("telecom") or [])}
    comm     = (res.get("communication") or [{}])[0]
    return {
        "patient_id":   res.get("id"),
        "family_name":  name.get("family"),
        "given_name":   " ".join(name.get("given") or []) or None,
        "gender":       res.get("gender"),
        "birth_date":   res.get("birthDate"),
        "city":         address.get("city"),
        "state":        address.get("state"),
        "country":      address.get("country"),
        "postal_code":  address.get("postalCode"),
        "phone":        telecom.get("phone"),
        "email":        telecom.get("email"),
        "language":     (comm.get("language") or {}).get("coding", [{}])[0].get("code"),
        "active":       res.get("active"),
        "last_updated": (res.get("meta") or {}).get("lastUpdated"),
    }

def flatten_encounter(res: dict) -> dict:
    type_coding   = ((res.get("type") or [{}])[0].get("coding") or [{}])[0]
    reason_coding = ((res.get("reasonCode") or [{}])[0].get("coding") or [{}])[0]
    return {
        "encounter_id":     res.get("id"),
        "status":           res.get("status"),
        "class_code":       (res.get("class") or {}).get("code"),
        "class_display":    (res.get("class") or {}).get("display"),
        "type_code":        type_coding.get("code"),
        "type_display":     type_coding.get("display"),
        "patient_id":       (res.get("subject") or {}).get("reference", "").replace("Patient/", ""),
        "period_start":     (res.get("period") or {}).get("start"),
        "period_end":       (res.get("period") or {}).get("end"),
        "reason_code":      reason_coding.get("code"),
        "reason_display":   reason_coding.get("display"),
        "service_provider": (res.get("serviceProvider") or {}).get("display"),
        "last_updated":     (res.get("meta") or {}).get("lastUpdated"),
    }

def flatten_observation(res: dict) -> dict:
    code_coding = ((res.get("code") or {}).get("coding") or [{}])[0]
    value_qty   = res.get("valueQuantity") or {}
    category    = ((res.get("category") or [{}])[0].get("coding") or [{}])[0]
    component   = (res.get("component") or [{}])[0]
    comp_qty    = component.get("valueQuantity") or {}
    comp_code   = ((component.get("code") or {}).get("coding") or [{}])[0]
    return {
        "observation_id":   res.get("id"),
        "status":           res.get("status"),
        "category":         category.get("code"),
        "code":             code_coding.get("code"),
        "code_display":     code_coding.get("display"),
        "patient_id":       (res.get("subject") or {}).get("reference", "").replace("Patient/", ""),
        "encounter_id":     (res.get("encounter") or {}).get("reference", "").replace("Encounter/", ""),
        "effective_datetime": res.get("effectiveDateTime") or (res.get("effectivePeriod") or {}).get("start"),
        "issued":           res.get("issued"),
        "value_quantity":   value_qty.get("value"),
        "value_unit":       value_qty.get("unit"),
        "value_string":     res.get("valueString"),
        "value_code":       ((res.get("valueCodeableConcept") or {}).get("coding") or [{}])[0].get("code"),
        "component_code":   comp_code.get("code"),
        "component_value":  comp_qty.get("value"),
        "component_unit":   comp_qty.get("unit"),
        "last_updated":     (res.get("meta") or {}).get("lastUpdated"),
    }

def flatten_condition(res: dict) -> dict:
    clinical      = ((res.get("clinicalStatus") or {}).get("coding") or [{}])[0]
    verification  = ((res.get("verificationStatus") or {}).get("coding") or [{}])[0]
    code_coding   = ((res.get("code") or {}).get("coding") or [{}])[0]
    category      = ((res.get("category") or [{}])[0].get("coding") or [{}])[0]
    severity      = ((res.get("severity") or {}).get("coding") or [{}])[0]
    return {
        "condition_id":          res.get("id"),
        "clinical_status":       clinical.get("code"),
        "verification_status":   verification.get("code"),
        "category":              category.get("code"),
        "code":                  code_coding.get("code"),
        "code_display":          code_coding.get("display"),
        "patient_id":            (res.get("subject") or {}).get("reference", "").replace("Patient/", ""),
        "encounter_id":          (res.get("encounter") or {}).get("reference", "").replace("Encounter/", ""),
        "onset_datetime":        res.get("onsetDateTime") or (res.get("onsetPeriod") or {}).get("start"),
        "abatement_datetime":    res.get("abatementDateTime"),
        "recorded_date":         res.get("recordedDate"),
        "severity":              severity.get("code"),
        "last_updated":          (res.get("meta") or {}).get("lastUpdated"),
    }

FLATTEN_MAP = {
    "Patient":     flatten_patient,
    "Encounter":   flatten_encounter,
    "Observation": flatten_observation,
    "Condition":   flatten_condition,
}

print("FHIR config loaded.")
