import os
import shutil
import pytest
from pyspark.sql import Row
from ts_review.api_data_to_csv.api_data_to_csv import (
    apply_governance_tags,
    write_df_to_csv
)
from ts_review.governance.data_policy_tags import data_policy_tags
from ts_review.governance.pii_masking_rules import masking_rules_by_tag


def test_apply_governance_tags_hash_sha256(spark_session):
    """
         Ensures hash masking is applied correctly to sensitive columns.
    """
    df = spark_session.createDataFrame(
        [("john.doe@example.com", "192.168.0.1")],
        ["email_address", "review_ip_address"]
    )

    # when
    masked_df = apply_governance_tags(df)

    # then
    masked_row = masked_df.collect()[0]

    # email_address should be hashed (not original)
    assert masked_row["email_address"] != "john.doe@example.com"
    assert len(masked_row["email_address"]) == 64  # SHA256 length

    # IP should be partially masked
    assert masked_row["review_ip_address"].endswith("***")


def test_apply_governance_tags_no_change_for_unmasked_columns(spark_session):
    """
    Ensure that applying governance tags does not fail and leaves
    unmasked columns unchanged when all expected columns exist.
    """
    df = spark_session.createDataFrame(
        [
            (
                "Test Business",      # business_name
                "john.doe@example.com",  # email_address
                "192.168.0.1",          # review_ip_address
                "John Doe",              # reviewer_name
                "Hello world!"           # review_content
            )
        ],
        ["business_name","email_address", "review_ip_address", "reviewer_name", "review_content"]
    )

    # When
    result_df = apply_governance_tags(df)

    # Then
    assert result_df.count() == df.count()  # no row loss
    assert set(result_df.columns) == set(df.columns)

    # reviewer_name should be redacted
    masked = result_df.collect()[0]
    assert masked["reviewer_name"] == "[REDACTED]"



def test_write_df_to_csv_creates_file(tmp_path, spark_session):
    """
        Validates that CSV is written with header and data.
    """
    output_path = str(tmp_path / "output_csv")

    df = spark_session.createDataFrame(
        [("A1", "Test Reviewer", 5)],
        ["review_id", "reviewer_name", "review_rating"]
    )

    write_df_to_csv(df, output_path, coalesce_one=True)

    # verify folder created
    assert os.path.exists(output_path)

    # find written CSV file
    csv_files = [f for f in os.listdir(output_path) if f.endswith(".csv")]
    assert csv_files, "No CSV file written"

    # read back and validate content
    written_df = spark_session.read.option("header", "true").csv(output_path)
    assert written_df.count() == 1
    assert written_df.columns == ["review_id", "reviewer_name", "review_rating"]

    shutil.rmtree(output_path)
