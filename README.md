# Spotify Streaming Analytics - Azure Data Engineering Project

> End-to-end data engineering solution on Azure Cloud using Medallion Architecture to process Spotify streaming data from Azure SQL Database to Delta Lake with incremental CDC, SCD Type 2, and real-time alerting

---

## 📊 Architecture Overview

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Azure SQL     │────▶│  Azure Data     │────▶│  ADLS Gen2      │────▶│   Databricks    │────▶│  Delta Live     │
│   Database      │     │  Factory (CDC)  │     │  Bronze Layer   │     │  Silver Layer   │     │  Tables (Gold)  │
│                 │     │                 │     │  (Parquet)      │     │  (Delta)        │     │  (SCD Type 2)   │
└─────────────────┘     └─────────────────┘     └─────────────────┘     └─────────────────┘     └─────────────────┘
                                │                                                                        │
                                │                                                                        ▼
                                ▼                                                               ┌─────────────────┐
                        ┌─────────────────┐                                                     │  Unity Catalog  │
                        │  Azure Logic    │                                                     │  (Governance)   │
                        │  Apps (Alerts)  │                                                     └─────────────────┘
                        └─────────────────┘
```

---

## 🛠️ Technology Stack

| Category | Technologies |
|----------|-------------|
| **Cloud Platform** | Azure Data Factory, ADLS Gen2, Azure SQL Database, Azure Databricks, Logic Apps |
| **Data Engineering** | ETL/ELT, Change Data Capture (CDC), Medallion Architecture, SCD Type 1 & 2 |
| **Spark/Databricks** | PySpark, Structured Streaming, Delta Lake, Delta Live Tables, Auto Loader |
| **Languages** | Python, SQL, Jinja2 Templating |
| **DevOps** | Databricks Asset Bundles (DAB), Unity Catalog, pytest, Git |
| **File Formats** | Parquet (Snappy compression), Delta, JSON |

---

## 📁 Project Structure

```
AzureProject/
├── factory/                          # ADF Factory configuration
│   └── df-azureproject-factory-lp.json
├── linkedService/                    # Azure service connections
│   ├── AzureDataLakeStorage1.json    # ADLS Gen2 connection
│   └── AzureSqlDatabase1.json        # Azure SQL connection
├── dataset/                          # ADF datasets
│   ├── AzureSqldatabse1.json         # SQL source dataset
│   ├── Parquet_dynamic.json          # Parameterized Parquet sink
│   └── Json_dynamic.json             # Parameterized JSON for CDC tracking
├── pipeline/                         # ADF pipelines
│   └── incremental_pipeline_loop.json # Main CDC pipeline
└── spotify_dab/                      # Databricks Asset Bundle
    └── spotify_dab/
        ├── databricks.yml            # DAB configuration (dev/prod)
        ├── pyproject.toml            # Python project config
        ├── resources/
        │   ├── sample_job.job.yml    # Databricks job definition
        │   └── spotify_dab_etl.pipeline.yml  # DLT pipeline config
        ├── src/
        │   ├── silver/
        │   │   └── silver_dimension.py    # Silver layer transformations
        │   └── gold/
        │       └── dlt/
        │           ├── transformations/
        │           │   ├── DimUser.py     # User dimension (SCD2)
        │           │   ├── DimTrack.py    # Track dimension (SCD2)
        │           │   ├── DimDate.py     # Date dimension (SCD2)
        │           │   └── FactStream.py  # Streaming fact (SCD1)
        │           └── utilities/
        │               └── utils.py       # Custom UDFs
        ├── utils/
        │   └── transformation.py     # Reusable transformation class
        ├── Jinja/
        │   └── jinja_notebook.py     # Dynamic SQL with Jinja
        └── tests/
            ├── conftest.py           # pytest fixtures
            └── sample_taxis_test.py  # Unit tests
```

---

## 🔄 Data Pipeline Layers

### 1. Azure Data Factory (Data Ingestion Layer)

**Key Features:**
- **Parameterized Incremental ETL Pipeline**: Single reusable pipeline with ForEach loops to dynamically extract data from 5 source tables
- **Change Data Capture (CDC)**: Tracks watermark timestamps to extract only modified records
- **Dynamic CDC Watermark Management**: Lookup activities read last processed timestamps; Script activities capture max CDC values
- **Conditional Branching**: IfCondition activities handle empty loads by deleting zero-byte files
- **Real-time Alerting**: Azure Logic Apps webhook integration for pipeline success/failure notifications

**Source Tables:**
| Table | CDC Column | Type |
|-------|-----------|------|
| DimUser | updated_at | Dimension |
| DimTrack | updated_at | Dimension |
| DimDate | date | Dimension |
| DimArtist | updated_at | Dimension |
| FactStream | stream_timestamp | Fact |

---

### 2. Bronze Layer (ADLS Gen2 - Raw Data)

**Key Features:**
- Raw data storage in **Parquet format** with Snappy compression
- Organized folder structure: `bronze/{table_name}/{table_name}_{timestamp}`
- JSON-based CDC metadata files for watermark tracking
- Immutable audit trail for data lineage

---

### 3. Silver Layer (Databricks - Cleansed Data)

**Key Features:**
- **Auto Loader** for streaming ingestion with schema evolution (`addNewColumns` mode)
- **Structured Streaming** with checkpoint locations for exactly-once processing
- Data cleansing: case standardization, deduplication, schema normalization
- Output: **Delta format** tables in Unity Catalog (`spotifycata.silver.*`)

**Sample Transformation:**
```python
df_user = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("rescuedDataColumn", "_rescued_data")
    .load("abfss://bronze@storageaccountprojectlp.dfs.core.windows.net/DimUser")
)

df_user.writeStream.format("delta")
    .outputMode("append")
    .trigger(once=True)
    .toTable("spotifycata.silver.DimUser")
```

---

### 4. Gold Layer (Delta Live Tables - Business Data)

**Key Features:**
- **SCD Type 2** for dimensions using `dlt.create_auto_cdc_flow()` with `__START_AT` and `__END_AT` tracking
- **SCD Type 1** for fact tables (current state only)
- **Data Quality Expectations** using `@dlt.expect_all_or_drop` decorators
- Custom **UDFs** for validation (e.g., email format validation)

**DLT Example (SCD Type 2):**
```python
import dlt

expectations = {"rule_1": "user_id IS NOT NULL"}

@dlt.table
@dlt.expect_all_or_drop(expectations)
def dimuser_Stg():
    return spark.readStream.table("spotifycata.silver.dimuser")

dlt.create_streaming_table("dimuser")
dlt.create_auto_cdc_flow(
    target="dimuser",
    source="dimuser_stg",
    keys=["user_id"],
    sequence_by="updated_at",
    stored_as_scd_type=2
)
```

---

## ⚙️ CI/CD & DevOps

### Databricks Asset Bundles (DAB)

**Environment Configuration:**
```yaml
targets:
  dev:
    mode: development
    variables:
      catalog: databricksazureproject
      schema: ${workspace.current_user.short_name}
  prod:
    mode: production
    variables:
      catalog: databricksazureproject
      schema: prod
```

**Job Scheduling:**
- Daily automated runs with `periodic` trigger
- Multi-task workflows: Notebook → Python Wheel → DLT Pipeline Refresh
- Parameterized with catalog/schema variables

---

## 🧪 Testing

- **pytest** with Databricks Connect for local development
- Fixture-based test data loading (JSON/CSV)
- Unit tests for transformation logic

```python
def test_find_all_taxis():
    results = taxis.find_all_taxis()
    assert results.count() > 5
```

---

## 🚀 Getting Started

### Prerequisites
- Azure Subscription with Data Factory, ADLS Gen2, SQL Database
- Azure Databricks workspace
- Python 3.10+

### Deployment
1. Deploy ADF artifacts using ARM templates or Azure DevOps
2. Configure Databricks Asset Bundle:
   ```bash
   cd spotify_dab/spotify_dab
   databricks bundle deploy -t dev
   ```
3. Run the job:
   ```bash
   databricks bundle run sample_job -t dev
   ```

---

## 📈 Resume Highlights

### Key Accomplishments
- ✦ Designed **parameterized ADF pipeline** with ForEach loops for incremental CDC extraction from 5+ tables
- ✦ Implemented **Medallion Architecture** (Bronze → Silver → Gold) on ADLS Gen2 with Delta Lake
- ✦ Built **Spark Structured Streaming ETL** with Auto Loader for real-time ingestion with schema evolution
- ✦ Developed **Delta Live Tables** pipelines with SCD Type 2 for dimensions and SCD Type 1 for facts
- ✦ Configured **data quality expectations** in DLT with automated validation and bad record handling
- ✦ Implemented **CI/CD using Databricks Asset Bundles** with dev/prod environment separation
- ✦ Integrated **Azure Logic Apps webhooks** for real-time pipeline failure alerting

---

## 📄 License

This project is for educational and portfolio purposes.

---

## 👤 Author

**Prashanth L**  
*Azure Data Engineer*
