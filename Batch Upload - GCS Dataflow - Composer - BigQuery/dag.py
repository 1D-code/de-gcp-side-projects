# dag.py
from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from datetime import datetime

RAW_BUCKET = "csv-pipeline-raw"
COMPOSER_BUCKET = "asia-southeast1-csv-pipelin-f5674975-bucket"

with DAG(
    dag_id="csv_to_bq_pipeline",
    schedule_interval="0 */8 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    wait_for_csv = GCSObjectsWithPrefixExistenceSensor(
        task_id="wait_for_csv",
        bucket=RAW_BUCKET,
        prefix="input/",
        mode="poke",
        poke_interval=300,
        timeout=3600,
    )

    run_dataflow = BeamRunPythonPipelineOperator(
        task_id="run_dataflow",
        runner="DataflowRunner",
        py_file=f"gs://{COMPOSER_BUCKET}/staging/pipeline.py",
        pipeline_options={
            "project": "de-side-projects-2026",
            "region": "asia-southeast1",
            "job_name": "csv-merge-{{ ds_nodash }}",
            "machine_type": "e2-standard-2", 
            "max_num_workers": "1",
            "temp_location": f"gs://{COMPOSER_BUCKET}/staging/temp",
            "staging_location": f"gs://{COMPOSER_BUCKET}/staging",
            "service_account_email": "composer-sa@de-side-projects-2026.iam.gserviceaccount.com",  # add this
        },
        py_interpreter="python3",
        py_requirements=[],
        py_system_site_packages=True,
    )

    archive_files = GCSToGCSOperator(
        task_id="archive_processed",
        source_bucket=RAW_BUCKET,
        source_object="input/",
        destination_bucket=RAW_BUCKET,
        destination_object="processed/",
        move_object=True,
    )

    wait_for_csv >> run_dataflow >> archive_files