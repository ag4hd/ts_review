from pyspark.sql.functions import (
    current_timestamp,
    col,
    row_number,
    coalesce,
    to_date,
    to_timestamp
)
from ts_review.schemas.ts_raw_schema import ingest_schema
from pyspark.sql import (
    DataFrame,
    Window
)
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType


def read_csv_pyspark(spark: SparkSession, input_path: str) -> DataFrame:
    df_raw = (
        spark.read
        .format("csv")
        .option("header", "true")
        .schema(ingest_schema)
        .load(input_path)
        .withColumn("last_updated", current_timestamp())
    )

    return df_raw


def deduplicate_data(df: DataFrame) -> DataFrame:
    """
    Remove duplicate records by review_id, keeping latest_records.
    based on review_date and last_updated timestamp.
    """
    sort_col = coalesce(col("review_date"), col("last_updated"))
    window_spec = Window.partitionBy("review_id").orderBy(sort_col.desc())
    df_deduplicate = (
        df.withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    return df_deduplicate


def write_transformed_data_partitioned(df: DataFrame, output_path: str, spark: SparkSession):
    """
    write the transformed data to the output path partitioned by ingestion_date
    """
    # Derive date partition
    df = df.withColumn("ingestion_date", to_date(col("last_updated")))

    # Get current ingestion date
    ingestion_date = df.select("ingestion_date").first()["ingestion_date"]

    if DeltaTable.isDeltaTable(spark, output_path):
        (
            df
            .write
            .format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"ingestion_date = '{ingestion_date}'")
            .partitionBy("ingestion_date")
            .save(output_path)
        )
    else:
        (
            df
            .write
            .format("delta")
            .mode("overwrite")
            .partitionBy("ingestion_date")
            .save(output_path)
        )


RENAME_MAP = {
    "Review Id": "review_id",
    "Reviewer Name": "reviewer_name",
    "Review Title": "review_title",
    "Review Rating": "review_rating",
    "Review Content": "review_content",
    "Review IP Address": "review_ip_address",
    "Business Id": "business_id",
    "Business Name": "business_name",
    "Reviewer Id": "reviewer_id",
    "Email Address": "email_address",
    "Reviewer Country": "reviewer_country",
    "Review Date": "review_date",
}


def ts_rename_columns(df: DataFrame) -> DataFrame:
    for src, dst in RENAME_MAP.items():
        if src in df.columns:
            df = df.withColumnRenamed(src, dst)
    return df


def cast_to_internal_types(df: DataFrame) -> DataFrame:
    """
    Cast string raw fields into curated/internal types.
    - review_rating -> int
    - review_date   -> timestamp (handles offsets like +0200)
    """
    if "review_rating" in df.columns:
        df = df.withColumn("review_rating", col("review_rating").cast(IntegerType()))
    if "review_date" in df.columns:
        # Try robust timestamp parse; Spark understands many formats
        df = df.withColumn("review_date", to_timestamp(col("review_date")))
    return df
