# pipeline.py
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import csv, io, hashlib
from datetime import datetime, timezone

PROJECT = "de-side-projects-2026"
RAW_BUCKET  = "csv-pipeline-raw"
REGION  = "asia-southeast1"
COMPOSER_BUCKET = "asia-southeast1-csv-pipelin-f5674975-bucket"

def parse_csv_row(element):
    filename, line = element

    if line.lower().startswith("order_number"):
        return

    reader = csv.DictReader(
        io.StringIO(line),
        fieldnames=[
            "order_number","quantity_ordered","price_each","order_line_number",
            "sales","order_date","status","qtr_id","month_id","year_id",
            "product_line","msrp","product_code","customer_name","phone",
            "address_line1","address_line2","city","state","postal_code",
            "country","territory","contact_lastname","contact_firstname","deal_size"
        ]
    )

    for row in reader:
        # Type conversions
        int_fields   = ["order_number","quantity_ordered","order_line_number","qtr_id","month_id","year_id"]
        float_fields = ["price_each","sales","msrp"]
        date_fields  = ["order_date"]

        for f in int_fields:
            try:
                row[f] = int(row[f]) if row[f] else None
            except:
                row[f] = None

        for f in float_fields:
            try:
                row[f] = float(row[f]) if row[f] else None
            except:
                row[f] = None

        for f in date_fields:
            try:
                row[f] = datetime.strptime(row[f], "%Y-%m-%d").date().isoformat() if row[f] else None
            except:
                row[f] = None

        # New columns
        row["file_name"]    = filename.split("/")[-1]           # just the filename, no path
        row["upload_time"]  = datetime.now(timezone.utc).isoformat()  # UTC timestamp

        # Dedup key — hash of business key fields
        # Change the fields inside to whatever uniquely identifies a row in your CSV
        dedup_fields = f"{row['order_number']}_{row['order_line_number']}_{row['order_date']}"
        row["row_hash"] = hashlib.md5(dedup_fields.encode()).hexdigest()

        yield row


def dedup_rows(rows):
    """Keep only one row per row_hash."""
    seen = {}
    for row in rows:
        key = row["row_hash"]
        if key not in seen:
            seen[key] = row
    return seen.values()


runner = "DataflowRunner"

options = PipelineOptions(
    project=PROJECT,
    region=REGION,
    runner=runner,
    temp_location=f"gs://{COMPOSER_BUCKET}/staging/temp",
    staging_location=f"gs://{COMPOSER_BUCKET}/staging",
    machine_type="e2-standard-2",
    max_num_workers=1,
    use_public_ips=False,
    service_account_email="composer-sa@de-side-projects-2026.iam.gserviceaccount.com",
)

with beam.Pipeline(options=options) as p:
    (
        p
        | "Read CSVs"    >> beam.io.ReadFromTextWithFilename(f"gs://{RAW_BUCKET}/input/*.csv")
        | "Parse rows"   >> beam.FlatMap(parse_csv_row)
        | "Key by hash"  >> beam.Map(lambda row: (row["row_hash"], row))
        | "Group by key" >> beam.GroupByKey()
        | "Dedup"        >> beam.Map(lambda kv: list(kv[1])[0])  # keep first of each hash group
        | "Write to BQ"  >> WriteToBigQuery(
            f"{PROJECT}:sales.sales_data",
            schema={
                "fields": [
                    {"name": "order_number",      "type": "INTEGER",   "mode": "NULLABLE"},
                    {"name": "quantity_ordered",  "type": "INTEGER",   "mode": "NULLABLE"},
                    {"name": "price_each",        "type": "FLOAT",     "mode": "NULLABLE"},
                    {"name": "order_line_number", "type": "INTEGER",   "mode": "NULLABLE"},
                    {"name": "sales",             "type": "FLOAT",     "mode": "NULLABLE"},
                    {"name": "order_date",        "type": "DATE",      "mode": "NULLABLE"},
                    {"name": "status",            "type": "STRING",    "mode": "NULLABLE"},
                    {"name": "qtr_id",            "type": "INTEGER",   "mode": "NULLABLE"},
                    {"name": "month_id",          "type": "INTEGER",   "mode": "NULLABLE"},
                    {"name": "year_id",           "type": "INTEGER",   "mode": "NULLABLE"},
                    {"name": "product_line",      "type": "STRING",    "mode": "NULLABLE"},
                    {"name": "msrp",              "type": "FLOAT",     "mode": "NULLABLE"},
                    {"name": "product_code",      "type": "STRING",    "mode": "NULLABLE"},
                    {"name": "customer_name",     "type": "STRING",    "mode": "NULLABLE"},
                    {"name": "phone",             "type": "STRING",    "mode": "NULLABLE"},
                    {"name": "address_line1",     "type": "STRING",    "mode": "NULLABLE"},
                    {"name": "address_line2",     "type": "STRING",    "mode": "NULLABLE"},
                    {"name": "city",              "type": "STRING",    "mode": "NULLABLE"},
                    {"name": "state",             "type": "STRING",    "mode": "NULLABLE"},
                    {"name": "postal_code",       "type": "STRING",    "mode": "NULLABLE"},
                    {"name": "country",           "type": "STRING",    "mode": "NULLABLE"},
                    {"name": "territory",         "type": "STRING",    "mode": "NULLABLE"},
                    {"name": "contact_lastname",  "type": "STRING",    "mode": "NULLABLE"},
                    {"name": "contact_firstname", "type": "STRING",    "mode": "NULLABLE"},
                    {"name": "deal_size",         "type": "STRING",    "mode": "NULLABLE"},
                    {"name": "file_name",         "type": "STRING",    "mode": "NULLABLE"},
                    {"name": "upload_time",       "type": "TIMESTAMP", "mode": "NULLABLE"},
                    {"name": "row_hash",          "type": "STRING",    "mode": "NULLABLE"},
                ]
            },
            write_disposition="WRITE_APPEND",
            create_disposition="CREATE_IF_NEEDED",
        )
    )