import os
from datetime import datetime

import duckdb
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# load env variables
ACCESS = os.getenv("GCS_ACCESS_KEY")
SECRET = os.getenv("GCS_SECRET")
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
BUCKET_PATH_BRONZE = os.getenv("GCS_BUCKET_PATH_BRONZE")
BUCKET_PATH_SILVER = os.getenv("GCS_BUCKET_PATH_SILVER")
BUCKET_PATH_GOLD = os.getenv("GCS_BUCKET_PATH_GOLD")


def setup_duckdb_connection(
    ACCESS, SECRET, db_path: str = ":memory:", read_only: str = False
):
    """
    Sets up a duckdb connection and configures it for S3 access.
    """
    try:
        duckdb_connection = duckdb.connect(database=db_path, read_only=read_only)
        duckdb_connection.sql("INSTALL httpfs")
        duckdb_connection.sql("LOAD httpfs")
        duckdb_connection.sql(f"SET s3_access_key_id='{ACCESS}'")
        duckdb_connection.sql(f"SET s3_secret_access_key='{SECRET}'")
        duckdb_connection.sql("SET s3_endpoint='storage.googleapis.com'")
        return duckdb_connection
    except Exception as e:
        print(f"Failed to setup DuckDB connection: {e}")
        raise


def load_parquet_from_bucket(
    duckdb_connection: duckdb.DuckDBPyConnection, table_name: str, bucket_path: str
):
    """
    Loads parquet data from S3 and creates a table in DuckDB
    """
    try:
        duckdb_connection.sql(
            f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM 's3://{bucket_path}'"
        )
    except Exception as e:
        print(f"Failed to load parquet from bucket: {e}")
        raise


def main(table_name=str):
    bucket_path = f"{BUCKET_NAME}/{BUCKET_PATH_GOLD}/*/*/*/*.parquet"

    # Establish a connection to DuckDB
    duckdb_conn = setup_duckdb_connection(ACCESS, SECRET)

    # Load data
    load_parquet_from_bucket(
        duckdb_conn,
        table_name,
        bucket_path,
    )

    duckdb_conn.close()


if __name__ == "__main__":
    main()
