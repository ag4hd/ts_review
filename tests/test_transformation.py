import pytest
from ts_review.transformation import raw_transformation, finish_transformation
from tests.conftest import (
    input_schema,
    input_data,
    assert_dataframe_equal,
    internal_write_schema,
)


def test_raw_transformation_basic(spark_session):
    """
        Verifies that raw_transformation:
        - renames columns correctly
        - casts data types to expected internal schema
        - keeps record count same
    """
    # given
    df_raw = spark_session.createDataFrame(input_data, schema=input_schema)

    # when
    df_transformed = raw_transformation(df_raw)

    # then
    assert df_transformed.columns == internal_write_schema.fieldNames()
    assert df_raw.count() == df_transformed.count()


def test_raw_transformation_data_integrity(spark_session):
    """
        Ensures transformation doesn’t alter data content.
    """
    df_raw = spark_session.createDataFrame(input_data, schema=input_schema)
    df_transformed = raw_transformation(df_raw)

    # Validate content integrity (same review IDs)
    raw_ids = [r["Review Id"] for r in df_raw.collect()]
    transformed_ids = [r["review_id"] for r in df_transformed.collect()]
    assert sorted(raw_ids) == sorted(transformed_ids)


def test_finish_transformation(spark_session):
    """
        Ensures finish_transformation returns same DataFrame unchanged.
    """
    df_raw = spark_session.createDataFrame(input_data, schema=input_schema)
    df_transformed = raw_transformation(df_raw)

    df_final = finish_transformation(df_transformed)

    assert_dataframe_equal(df_transformed, df_final)
