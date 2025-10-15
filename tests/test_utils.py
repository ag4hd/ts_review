import shutil
from tests.conftest import (
    input_data,
    assert_dataframe_equal,
    get_current_timestamp_lit,
    duplicate_input_data,
    internal_write_schema,
    old_data, input_schema,
    get_current_timestamp,
)
from ts_review.utils import (
    read_csv_pyspark,
    deduplicate_data,
    write_transformed_data_partitioned,
    ts_rename_columns,
    cast_to_internal_types,
)
from pytest import MonkeyPatch
import tempfile
from pyspark.sql.functions import to_date, col, lit


input_file_path = "tests/resources/test_tp_reviews.csv"


def test_read_csv_pyspark(spark_session, monkeypatch: MonkeyPatch):
    # given
    monkeypatch.setattr("ts_review.utils.current_timestamp", get_current_timestamp_lit)

    # when
    actual_df = read_csv_pyspark(spark_session, input_file_path)

    expected_results = spark_session.createDataFrame(input_data, schema=input_schema)
    expected_df = expected_results

    # then
    assert_dataframe_equal(actual_df, expected_df)


def test_deduplicate_data(spark_session, monkeypatch: MonkeyPatch):
    # given
    monkeypatch.setattr("ts_review.utils.current_timestamp", get_current_timestamp_lit)
    duplicate_input_df = spark_session.createDataFrame(duplicate_input_data, schema=internal_write_schema)

    # when
    actual_df = deduplicate_data(duplicate_input_df)

    expected_df = spark_session.createDataFrame(input_data, schema=internal_write_schema)

    # then
    assert_dataframe_equal(actual_df, expected_df)


def test_write_transformed_data_partitioned(spark_session, tmpdir: str, monkeypatch: MonkeyPatch):
    # given
    monkeypatch.setattr("ts_review.utils.current_timestamp", get_current_timestamp_lit)
    df = spark_session.createDataFrame(input_data, schema=internal_write_schema)

    actual_df = df.withColumn("ingestion_date", to_date(col("last_updated")))
    temp_dir = tempfile.mkdtemp()

    # when
    write_transformed_data_partitioned(actual_df, temp_dir, spark_session)
    output_df = spark_session.read.format("delta").load(temp_dir)

    # then
    assert_dataframe_equal(actual_df, output_df)
    shutil.rmtree(temp_dir)


def test_write_transformed_data_partitioned_already_old_date_record(
        spark_session,
        tmpdir: str,
        monkeypatch: MonkeyPatch
):
    # given
    monkeypatch.setattr("ts_review.utils.current_timestamp", get_current_timestamp_lit)
    temp_dir = tempfile.mkdtemp()
    # First write old data
    df_old = spark_session.createDataFrame(old_data, schema=internal_write_schema)
    df_old = df_old.withColumn("ingestion_date", to_date("last_updated"))
    df_old.write.format("delta").mode("overwrite").partitionBy("ingestion_date").save(temp_dir)

    df = spark_session.createDataFrame(input_data, schema=internal_write_schema)
    df = df.withColumn("ingestion_date", to_date("last_updated"))

    actual_df = df_old.unionByName(df)

    # when
    write_transformed_data_partitioned(df, temp_dir, spark_session)
    output_df = spark_session.read.format("delta").load(temp_dir)

    # then
    assert_dataframe_equal(actual_df, output_df)
    shutil.rmtree(temp_dir)


def test_write_transformed_data_partitioned_already_current_date_record(
        spark_session,
        tmpdir: str,
        monkeypatch: MonkeyPatch
):
    # given
    monkeypatch.setattr("ts_review.utils.current_timestamp", get_current_timestamp_lit)
    temp_dir = tempfile.mkdtemp()
    # First write current data
    df_current = spark_session.createDataFrame(input_data, schema=internal_write_schema)
    df_current = df_current.withColumn("ingestion_date", to_date("last_updated"))
    df_current.write.format("delta").mode("overwrite").partitionBy("ingestion_date").save(temp_dir)

    # New data with same ingestion date but different content
    new_data = [
        ("9e4feb04-717f-4568-8c77-e5784ae6fb2d", "Sara Cook", "Perfect Match", 2,
         "The transaction went seamlessly from start to finish. The purchase arrived on time and was exactly what I exceeded my expectations. Has some room for improvement. 🥉 😴",
         "143.190.221.173", "24a6a92a-f745-455f-b669-f2f02842039f", "Artisan Coffee Roasters",
         "9f51330e-f123-48b6-88fb-00020e824fc2", "sara.cook@example.no", "France", "2024-10-17 10:43:39+0200", get_current_timestamp()),
    ]

    new_df = spark_session.createDataFrame(new_data, schema=internal_write_schema)

    # when
    write_transformed_data_partitioned(new_df, temp_dir, spark_session)
    actual_df = spark_session.read.format("delta").load(temp_dir)

    expected_df = spark_session.createDataFrame(new_data, schema=internal_write_schema)
    expected_df = expected_df.withColumn("ingestion_date", to_date("last_updated"))

    # then
    assert_dataframe_equal(actual_df, expected_df)

    shutil.rmtree(temp_dir)


def test_ts_rename_columns(spark_session):
    # given
    df_raw = spark_session.createDataFrame(input_data, schema=input_schema)
    # when
    actual_renamed = ts_rename_columns(df_raw)

    expected_renamed = spark_session.createDataFrame(input_data, schema=internal_write_schema)
    # then
    assert actual_renamed.columns == expected_renamed.columns


def test_cast_to_internal_types(spark_session):
    # given
    df_raw = spark_session.createDataFrame(input_data, schema=input_schema)
    df_renamed = ts_rename_columns(df_raw)

    # when
    actual_df_casted = cast_to_internal_types(df_renamed)

    # then
    # review_rating should become IntegerType
    review_rating_type = dict(actual_df_casted.dtypes)["review_rating"]
    assert review_rating_type == "int"

    # review_date should become timestamp
    review_date_type = dict(actual_df_casted.dtypes)["review_date"]
    assert review_date_type == "timestamp"
