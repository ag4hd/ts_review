from pyspark.sql import DataFrame, SparkSession
from ts_review.utils import (
    ts_rename_columns,
    cast_to_internal_types,
    write_transformed_data_partitioned,
)


def raw_transformation(df_raw: DataFrame) -> DataFrame:
    """
    raw transformation step:
      1) rename columns to internal names
      2) cast columns to internal types
    """
    df = ts_rename_columns(df_raw)
    df = cast_to_internal_types(df)
    return df


def finish_transformation(df: DataFrame) -> DataFrame:
    """
    finish transformation step (if any)
    """
    return df

