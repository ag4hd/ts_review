from pyspark.sql import SparkSession


def test_parquet():
    path = "/Users/ashigaik/Documents/test/ts_review/tests/output/ingestion_date=2025-10-15"

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("ts_review_integration_test")
        .getOrCreate()
    )

    spark.read.parquet(path).show()