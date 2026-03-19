# GCS → Dataflow → BigQuery Pipeline

A data pipeline that ingests multiple CSV files from local desktop to Google Cloud Storage, transforms and merges them using Apache Beam on Dataflow, and loads the results into BigQuery — orchestrated by Cloud Composer (Airflow).

---

## Architecture

```
Local Desktop
     │
     │  gsutil cp
     ▼
GCS (csv-pipeline-raw/input/)
     │
     │  GCSObjectsWithPrefixExistenceSensor
     ▼
Cloud Composer (Airflow DAG)
     │
     │  BeamRunPythonPipelineOperator
     ▼
Dataflow (Apache Beam)
     │  - Parse CSV rows
     │  - Type conversions
     │  - Add file_name, upload_time, row_hash
     │  - Deduplicate rows
     ▼
BigQuery (sales.sales_data)
     │
     ▼
GCS (csv-pipeline-raw/processed/)  ← archived after load
```

---

## Project Structure

```
pipeline/
├── pipeline.py       # Apache Beam pipeline (reads CSVs, transforms, writes to BQ)
├── dag.py            # Airflow DAG (orchestrates the pipeline on a schedule)
└── README.md         # This file
```

---

## Prerequisites

- Google Cloud SDK installed on your local machine
- Python 3.11+
- A GCP project with billing enabled
- PowerShell (Windows) or bash (Mac/Linux)

---

## GCP Resources

| Resource | Name | Purpose |
|---|---|---|
| GCS Bucket | `csv-pipeline-raw` | Raw CSV uploads from desktop |
| GCS Bucket | `csv-pipeline-staging` | Dataflow temp and staging files |
| GCS Bucket | `asia-southeast1-csv-pipelin-*-bucket` | Auto-created by Composer for DAGs |
| BigQuery Dataset | `sales` | Destination dataset |
| BigQuery Table | `sales.sales_data` | Merged and deduplicated output table |
| Cloud Composer | `csv-pipeline-env` | Airflow environment (Composer 2, small) |
| Service Account | `composer-sa@...` | Used by Composer workers and Dataflow |

---

## Setup

### Step 1 — Authenticate

```powershell
gcloud auth login
gcloud config set project YOUR_PROJECT_ID
gcloud auth application-default login
```

### Step 2 — Enable APIs

```powershell
gcloud services enable storage.googleapis.com `
  dataflow.googleapis.com `
  bigquery.googleapis.com `
  composer.googleapis.com
```

### Step 3 — Grant IAM roles to your user account

```powershell
$USER_EMAIL = "your@gmail.com"
$PROJECT_ID = "your-project-id"

gcloud projects add-iam-policy-binding $PROJECT_ID --member="user:$USER_EMAIL" --role="roles/storage.admin"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="user:$USER_EMAIL" --role="roles/dataflow.developer"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="user:$USER_EMAIL" --role="roles/bigquery.dataEditor"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="user:$USER_EMAIL" --role="roles/bigquery.jobUser"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="user:$USER_EMAIL" --role="roles/composer.worker"
```

### Step 4 — Create and configure Composer service account

Create the service account in Console → IAM → Service Accounts → Create, then grant it roles:

```powershell
$COMPOSER_SA = "composer-sa@your-project-id.iam.gserviceaccount.com"

gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$COMPOSER_SA" --role="roles/composer.worker"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$COMPOSER_SA" --role="roles/storage.admin"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$COMPOSER_SA" --role="roles/dataflow.admin"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$COMPOSER_SA" --role="roles/dataflow.worker"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$COMPOSER_SA" --role="roles/bigquery.dataEditor"
gcloud projects add-iam-policy-binding $PROJECT_ID --member="serviceAccount:$COMPOSER_SA" --role="roles/bigquery.jobUser"

# Allow composer-sa to act as itself (required by Dataflow workers)
gcloud iam service-accounts add-iam-policy-binding $COMPOSER_SA `
  --member="serviceAccount:$COMPOSER_SA" --role="roles/iam.serviceAccountUser"

# Allow your user account to act as composer-sa
gcloud iam service-accounts add-iam-policy-binding $COMPOSER_SA `
  --member="user:$USER_EMAIL" --role="roles/iam.serviceAccountUser"
```

### Step 5 — Create GCS buckets

```powershell
$REGION = "asia-southeast1"

gsutil mb -l $REGION gs://csv-pipeline-raw
gsutil mb -l $REGION gs://csv-pipeline-staging

$tmpFile = New-TemporaryFile
gsutil cp $tmpFile.FullName gs://csv-pipeline-raw/input/.keep
gsutil cp $tmpFile.FullName gs://csv-pipeline-raw/processed/.keep
Remove-Item $tmpFile.FullName
```

### Step 6 — Create BigQuery dataset

```powershell
bq --location=$REGION mk --dataset your-project-id:sales
```

The table `sales.sales_data` is created automatically by the pipeline on first run using `CREATE_IF_NEEDED`.

### Step 7 — Create Composer environment

```powershell
gcloud composer environments create csv-pipeline-env `
  --location=$REGION `
  --image-version=composer-2-airflow-2 `
  --environment-size=small `
  --service-account=$COMPOSER_SA
```

Takes 15–20 minutes to provision.

### Step 8 — Add apache-beam to Composer

After the environment is `RUNNING`, go to:

```
Console → Composer → csv-pipeline-env → PyPI packages → Add package
  Package name:        apache-beam
  Extras and version:  [gcp]==2.55.0
```

> **Do NOT pin grpcio or grpcio-status** — this causes environment update failures.

### Step 9 — Get Composer bucket name

```powershell
gcloud composer environments describe csv-pipeline-env `
  --location=$REGION `
  --format="value(config.dagGcsPrefix)"
```

Copy the bucket name from the output (everything between `gs://` and `/dags`).

### Step 10 — Upload pipeline and DAG files

```powershell
$COMPOSER_BUCKET = "asia-southeast1-your-bucket-name"

gsutil cp pipeline.py gs://$COMPOSER_BUCKET/staging/pipeline.py
gsutil cp dag.py      gs://$COMPOSER_BUCKET/dags/dag.py
```

---

## Usage

### Upload new CSV files

```powershell
gsutil cp "C:\path\to\csvs\*.csv" gs://csv-pipeline-raw/input/
```

The DAG runs every 8 hours automatically. To trigger manually:

```powershell
gcloud composer environments run csv-pipeline-env `
  --location=asia-southeast1 `
  dags trigger -- csv_to_bq_pipeline
```

### Monitor pipeline

- **Airflow UI**: Console → Composer → csv-pipeline-env → Airflow UI
- **Dataflow Jobs**: Console → Dataflow → Jobs → look for `csv-merge-<date>`
- **BigQuery**: Console → BigQuery → sales.sales_data

### Query loaded data

```sql
-- Row count
SELECT COUNT(*) as total_rows FROM `your-project-id.sales.sales_data`;

-- Check which files were loaded
SELECT file_name, COUNT(*) as rows, MIN(upload_time) as loaded_at
FROM `your-project-id.sales.sales_data`
GROUP BY file_name
ORDER BY loaded_at DESC;

-- Clear all data for a fresh reload
TRUNCATE TABLE `your-project-id.sales.sales_data`;
```

---

## Pipeline Details

### pipeline.py — What it does

| Step | Description |
|---|---|
| Read CSVs | Reads all `*.csv` files from `gs://csv-pipeline-raw/input/` |
| Parse rows | Casts types (INTEGER, FLOAT, DATE), skips header rows |
| Add audit columns | Adds `file_name`, `upload_time`, `row_hash` to every row |
| Deduplicate | Groups by `row_hash` (MD5 of order_number + order_line_number + order_date), keeps one row per group |
| Write to BQ | Appends to `sales.sales_data`, creates table if it doesn't exist |

### dag.py — DAG tasks

| Task | Operator | Description |
|---|---|---|
| `wait_for_csv` | `GCSObjectsWithPrefixExistenceSensor` | Waits for files in `input/`, checks every 5 mins |
| `run_dataflow` | `BeamRunPythonPipelineOperator` | Submits `pipeline.py` to Dataflow |
| `archive_processed` | `GCSToGCSOperator` | Moves files from `input/` to `processed/` after load |

### Output table schema

| Column | Type | Description |
|---|---|---|
| order_number | INTEGER | Order identifier |
| quantity_ordered | INTEGER | Units ordered |
| price_each | FLOAT | Unit price |
| order_line_number | INTEGER | Line item number |
| sales | FLOAT | Total sale amount |
| order_date | DATE | Date of order |
| status | STRING | Order status |
| qtr_id | INTEGER | Quarter |
| month_id | INTEGER | Month |
| year_id | INTEGER | Year |
| product_line | STRING | Product category |
| msrp | FLOAT | Manufacturer suggested retail price |
| product_code | STRING | Product identifier |
| customer_name | STRING | Customer name |
| phone | STRING | Contact phone |
| address_line1 | STRING | Address line 1 |
| address_line2 | STRING | Address line 2 |
| city | STRING | City |
| state | STRING | State |
| postal_code | STRING | Postal code |
| country | STRING | Country |
| territory | STRING | Sales territory |
| contact_lastname | STRING | Contact last name |
| contact_firstname | STRING | Contact first name |
| deal_size | STRING | Deal size category |
| file_name | STRING | Source CSV filename |
| upload_time | TIMESTAMP | UTC timestamp when row was processed |
| row_hash | STRING | MD5 hash used for deduplication |

---

## Cost Estimates (asia-southeast1)

| Resource | Config | Est. Cost |
|---|---|---|
| GCS | < 5 GB | ~$0.10/month |
| Dataflow | e2-standard-2, 1 worker, ~15 min/run | ~$0.03/run |
| BigQuery | < 1 TB queries | Free tier |
| Composer 2 small | Running 24/7 | ~$3–5/day |
| **Composer 2 small** | **Paused when idle** | **~$0.40/day** |

> Pause Composer when not in use — it is the only significant ongoing cost.

---

## Cost Control

### Pause Composer scheduler

```powershell
gcloud composer environments update csv-pipeline-env `
  --location=asia-southeast1 `
  --update-env-variables=AIRFLOW__SCHEDULER__MAX_DAGRUNS_TO_CREATE_PER_LOOP=0
```

### Resume Composer scheduler

```powershell
gcloud composer environments update csv-pipeline-env `
  --location=asia-southeast1 `
  --update-env-variables=AIRFLOW__SCHEDULER__MAX_DAGRUNS_TO_CREATE_PER_LOOP=10
```

### Delete environment when done

```powershell
gcloud composer environments delete csv-pipeline-env --location=asia-southeast1
```

---

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `Cannot import DataflowCreatePythonJobOperator` | Removed in newer provider versions | Use `BeamRunPythonPipelineOperator` from `airflow.providers.apache.beam` |
| `Unable to parse template file` | Operator using Flex Templates API on a `.py` file | Remove `dataflow_config` block from operator |
| `404 bucket does not exist` | Recreated Composer env generates a new bucket | Re-run describe command, update `COMPOSER_BUCKET` variable |
| `Cannot act as service account` | Missing `iam.serviceAccountUser` binding | Grant `roles/iam.serviceAccountUser` on the SA to itself |
| `Missing dataflow.workItems.lease` | SA missing `dataflow.worker` role | Grant `roles/dataflow.worker` to Composer SA |
| `bigquery.jobs.create permission denied` | Dataflow worker SA missing BQ role | Grant `roles/bigquery.jobUser` to Composer SA |
| `ZONE_RESOURCE_POOL_EXHAUSTED` | Zone out of capacity for machine type | Use `e2-standard-2` instead of `n1-standard-1`, remove zone pin |
| `Job already exists` | Same job name reused same day | Add `{{ ts_nodash }}` to `job_name` in pipeline options |
| `DELETE must have WHERE clause` | BigQuery DML restriction | Use `WHERE true` or `TRUNCATE TABLE` instead |
| `grpcio dependency conflict` | Pinning grpcio in Composer PyPI | Do not pin grpcio — the warning is harmless, ignore it |

---

## Region Note

All resources use `asia-southeast1` (Singapore) — the closest GCP region to the Philippines. This minimises latency and avoids cross-region data transfer charges between GCS, Dataflow, and BigQuery.
