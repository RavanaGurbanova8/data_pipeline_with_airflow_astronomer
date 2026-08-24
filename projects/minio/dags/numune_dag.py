from datetime import datetime
from pathlib import Path
import re
import os
import io

import pyarrow.parquet as pq
from sqlalchemy import create_engine, text

from airflow import DAG
from airflow.operators.python import PythonOperator
from minio import Minio

# ---- MinIO Konfiqurasiyası ----
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = "data-bucket"

# ---- Local Data Qovluğu ----
LOCAL_DATA_DIR = "/app/data"

# ---- TimescaleDB Konfiqurasiyası ----
TS_USER = os.getenv("TS_USER", "timescale")
TS_PASSWORD = os.getenv("TS_PASSWORD", "timescale_123")
TS_DB = os.getenv("TS_DB", "timescale")
TS_HOST = "timescaledb"
TS_PORT = "5432"
TABLE_NAME = "combined_disease"

CHUNK_SIZE = 100_000

PATTERN = r"^combined_disease_(\d{4})-(\d{2})-(\d{2})\.parquet$"


def upload_parquet_to_minio():
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)

    parquet_files = list(Path(LOCAL_DATA_DIR).glob("*.parquet"))

    for file_path in parquet_files:
        match = re.match(PATTERN, file_path.name)
        if not match:
            print(f"Skipped: {file_path.name}")
            continue

        year, month, day = match.groups()
        object_name = f"{year}/{month}/{day}/{file_path.name}"
#bucketdeki faylları yoxla, əgər varsa, yükləmədən keç
        try:
            client.stat_object(MINIO_BUCKET, object_name)
            print(f"Already exists in MinIO, skipping: {object_name}")
            continue
        except Exception:
            pass

        client.fput_object(
            bucket_name=MINIO_BUCKET,
            object_name=object_name,
            file_path=str(file_path),
        )
        print(f"Uploaded: {object_name}")


def load_parquet_from_minio_to_timescaledb():
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

    engine = create_engine(
        f"postgresql+psycopg2://{TS_USER}:{TS_PASSWORD}@{TS_HOST}:{TS_PORT}/{TS_DB}"
    )

    # ---- YENİ: İzləmə cədvəli, yoxdursa, yarat ----
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS processed_files (
                file_name TEXT PRIMARY KEY,
                processed_at TIMESTAMP DEFAULT NOW()
            )
        """))
        #conn.commit() burda begin isletmisik deye ehtiyac yoxdur error verecek bu

    objects = client.list_objects(MINIO_BUCKET, recursive=True)

    for obj in objects:
        if not obj.object_name.endswith(".parquet"):
            continue

        # ---- YENİ: Artıq, TimescaleDB-yə, yazılıbmı, yoxla ----
        #idempotencyni tetbiq edirik eger cedvelde hemin fayllar yazilibsa skip olunacaq.
        with engine.begin() as conn:
            result = conn.execute(
                text("SELECT 1 FROM processed_files WHERE file_name = :name"),
                {"name": obj.object_name},
            ).fetchone()

        if result:
            print(f"Already processed, skipping: {obj.object_name}")
            continue

        print(f"Processing: {obj.object_name}")

        response = client.get_object(MINIO_BUCKET, obj.object_name)
        parquet_bytes = response.read()
        response.close()
        response.release_conn()

        parquet_file = pq.ParquetFile(io.BytesIO(parquet_bytes))

        total_rows = 0
        for batch in parquet_file.iter_batches(batch_size=CHUNK_SIZE):
            df_chunk = batch.to_pandas()

            df_chunk.to_sql(
                TABLE_NAME,
                engine,
                if_exists="append",
                index=False,
            )

            total_rows += len(df_chunk)
            del df_chunk

        # ---- YENİ: Bu faylın, emal olunduğunu, qeyd et ----
        with engine.begin() as conn:
            conn.execute(
                text("INSERT INTO processed_files (file_name) VALUES (:name)"),
                {"name": obj.object_name},
            )
            #conn.commit() burda begin isletmisik deye ehtiyac yoxdur error verecek bu

        print(f"Inserted {total_rows} rows from {obj.object_name}")


with DAG(
    dag_id="upload_parquet_to_minio",
    start_date=datetime(2026, 8, 10),
    end_date=datetime(2026, 8, 12),
    schedule= '20 10,13,15 * * *',
    catchup=True,
    tags=["minio", "parquet", "timescaledb"],
) as dag:

    upload_task = PythonOperator(
        task_id="upload_parquet_files",
        python_callable=upload_parquet_to_minio,
    )

    load_task = PythonOperator(
        task_id="load_to_timescaledb",
        python_callable=load_parquet_from_minio_to_timescaledb,
    )

    upload_task >> load_task