from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)

ingest_schema = StructType([
    StructField("Review Id", StringType(), True),
    StructField("Reviewer Name", StringType(), True),
    StructField("Review Title", StringType(), True),
    StructField("Review Rating", StringType(), True),
    StructField("Review Content", StringType(), True),
    StructField("Review IP Address", StringType(), True),
    StructField("Business Id", StringType(), True),
    StructField("Business Name", StringType(), True),
    StructField("Reviewer Id", StringType(), True),
    StructField("Email Address", StringType(), True),
    StructField("Reviewer Country", StringType(), True),
    StructField("Review Date", StringType(), True),
])
