from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DateType
)

curated_schema = StructType([
    StructField("review_id", StringType(), False),
    StructField("reviewer_name", StringType(), True),   # masked later
    StructField("review_title", StringType(), True),
    StructField("review_rating", IntegerType(), True),
    StructField("review_content", StringType(), True),
    StructField("review_ip_address", StringType(), True),   # masked later
    StructField("business_id", StringType(), True),
    StructField("business_name", StringType(), True),   # masked later
    StructField("reviewer_id", StringType(), True),
    StructField("email_address", StringType(), True),       # masked later
    StructField("reviewer_country", StringType(), True),
    StructField("review_date", DateType(), True),
    StructField("last_updated", DateType(), True)
])
