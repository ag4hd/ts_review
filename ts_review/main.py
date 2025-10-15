import argparse
import logging
from pyspark.sql import SparkSession
import os
from ts_review.api_data_to_csv.api_data_to_csv import write_df_to_csv, apply_governance_tags
from ts_review.transformation import raw_transformation
from ts_review.utils import read_csv_pyspark, write_transformed_data_partitioned
from ts_review.validation.expectations.apply_expectations import apply_expectations

# Initialize logger for module
logger = logging.getLogger(__name__)


def get_cli_args(args=None):
    parser = argparse.ArgumentParser(description="Trust Pilot Review Data")
    parser.add_argument(
        "--input_path",
        required=True,
        help="Path to input CSV file (raw reviews)"
    )
    parser.add_argument(
        "--output_path",
        required=True,
        help="Path to Delta output directory"
    )
    parser.add_argument(
        "--expectations_path",
        default="ts_review/validation/expectations/ts_expectations.json",
        help="Path to expectations JSON file"
    )
    parser.add_argument(
        "--role",
        default="analytics",
        choices=["legal", "data_engineering", "data_science", "marketing"],
        help="User role for applying governance masking (default: analytics)"
    )
    args = parser.parse_args(args=args)
    logger.info(
        f"""
        Input path = {args.input_path}
        Output path = {args.output_path}
        Role = {args.role}
        """
    )

    return args


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("spark-ts-review")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def main() -> None:
    logging.info("Starting ts_review main function")

    args = get_cli_args()
    spark = create_spark_session()
    logger.info("Spark session created successfully")

    # Read raw data
    df_raw = read_csv_pyspark(spark, args.input_path)
    logger.info(f"Raw data read successfully from CSV {df_raw.count()} rows")

    # Transform data
    df_stage = raw_transformation(df_raw)
    logger.info(f"Data transformed successfully {df_stage.count()} rows")
    df_validated = apply_expectations(df_stage, args.expectations_path)

    # Write transformed data
    write_transformed_data_partitioned(df_validated, args.output_path, spark)
    logger.info(f"Transformed data written successfully to {args.output_path}")

    df_governed = apply_governance_tags(df_validated, role=args.role)

    write_df_to_csv(df_governed, os.path.join(args.output_path, f"{args.role}_final_output.csv"))


if __name__ == "__main__":
    main()



