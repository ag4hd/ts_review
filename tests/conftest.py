from datetime import datetime
from pyspark.sql import SparkSession
import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import lit

fixed_date = datetime(2025, 1, 1, 12, 0, 0, 0)

old_date = datetime(2020, 1, 1, 12, 0, 0, 0)


def get_current_timestamp():
    return fixed_date


def get_current_timestamp_lit():
    return lit(fixed_date)


def old_data_lit():
    return old_date


@pytest.fixture(scope="session")
def spark_session():
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("ts_reviewer_test")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def normalize_schema(schema: StructType) -> StructType:
    """
    Returns a new schema with all fields marked as nullable=True
    so that schema comparison ignores nullable differences.
    """
    return StructType([
        StructField(field.name, field.dataType, nullable=True)
        for field in schema.fields
    ])


def assert_dataframe_equal(df1, df2):
    # Ignore nullable differences
    if normalize_schema(df1.schema) != normalize_schema(df2.schema):
        raise AssertionError(f"Schema mismatch (ignoring nullable):\n{df1.schema}\n!=\n{df2.schema}")

    diff = df1.exceptAll(df2).union(df2.exceptAll(df1))
    count = diff.count()
    if count:
        raise AssertionError(f"{count} rows differ between DataFrames")


input_schema = StructType([
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
    StructField("last_updated", TimestampType(), False)
])

input_data = [
    ("9e4feb04-717f-4568-8c77-e5784ae6fb2d", "Sara Cook", "Perfect Match", 2,
     "The transaction went seamlessly from start to finish. The purchase arrived on time and was exactly what I exceeded my expectations. Has some room for improvement. 🥉 😴",
     "143.190.221.173", "24a6a92a-f745-455f-b669-f2f02842039f", "Artisan Coffee Roasters",
     "9f51330e-f123-48b6-88fb-00020e824fc2", "sara.cook@example.no", "France", "2024-10-17 10:43:39+0200", get_current_timestamp()),

    ("ee73bcf8-38ab-40fd-b1c7-5376c644b65f", "Roger Neal", "Delayed Response", 1,
     "The order I received was exceptional. It exceeded functionality expectations and didn't function as described. Very thrilled. Exceeded what I thought possible.",
     "98.209.142.194", "60c10a30-c9ca-4c09-8a48-7bbf7c54d47d", "Coastal Restaurant Group",
     "939c79a3-a6a5-4516-9a06-1885354391e6", "roger.neal@example.edu", "Greece", "2023-11-12 07:44:04+0200", get_current_timestamp()),

    ("b3f91012-5d11-4ce2-ae48-3518017a0b59", "Rachael Morris", "Missing Items", 4,
     "The decent of the item seemed suspicious. It didn't meet my hopes. Meets basic requirements adequately.",
     "25.23.187.45", "5eed43ad-3f05-4a1f-b821-ad7977dfd3bd", "Innovative Web Design",
     "acd32ca6-06d3-4a40-ac11-4193560cf065", "rachael.morris@example.id", "Thailand", "2024-04-19 10:54:44+0700", get_current_timestamp()),

    ("54a870c8-70c8-443c-90d0-ca03c894eec5", "Spencer Smith", "Delayed Response", 1,
     "I recently used this service and I must say I was extremely satisfied. The customer service was outstanding, and the quality was exactly what I wanted. Hoping they improve in the future.",
     "197.206.64.224", "fb9e551e-2663-4df5-960e-7e4c33a2baf2", "Natural Wellness Spa",
     "f1189105-739b-4084-9254-06703d85d598", "spencer.smith@example.net", "Canada", "2025-02-21 00:00:44-0500", get_current_timestamp()),

    ("22fcc566-dd8c-4d32-a918-8cffb7c7a8ef", "Mark Christian", "Terrible Experience", 3,
     "I recently used this service and I must say I was extremely satisfied. The product quality was poor, and the support exceeded my expectations. A solid choice for anyone considering. 🙂 🥈",
     "94.0.115.199", "0efc15c0-c004-4ec3-9b80-3188a4943fd5", "Pinnacle Financial Services",
     "0202f60f-0957-4526-a9fe-a8ce0e3b1e0e", "mark.christian@example.ru", "Colombia", "2024-03-26 08:39:43-0500", get_current_timestamp()),

    ("fe7e153c-dc57-4014-8955-3302cdf4ac7e", "Jennifer Guerrero", "Disappointing Experience", 3,
     "The promotional content for the order was misleading. It did not accurately represent the features of the order.",
     "89.206.155.63", "e21a2f26-ac2e-4a98-bc8d-1ae7f8fb2485", "GreenLeaf Organic Foods",
     "d4eea22f-9280-4a47-8a63-7b2a5a85bf38", "jennifer.guerrero@example.nl", "South Korea", "2025-04-05 09:25:56+0900", get_current_timestamp()),

    ("d11a1553-1fe8-4110-92f8-1db54d4fab6c", "George Holt", "Average Service", 3,
     "Shipping was lightning quick, and the product arrived professionally packaged and in unusable.",
     "105.178.58.211", "24a6a92a-f745-455f-b669-f2f02842039f", "Artisan Coffee Roasters",
     "3b6aa039-3ded-4991-a301-6fab4d727252", "george.holt@example.fr", "Netherlands", "2024-08-11 06:55:25+0200", get_current_timestamp()),

    ("088f5847-1b1f-459c-a4d8-1e7d1026f683", "Sara Hernandez", "Responsive Support Team", 5,
     "The purchase I received was average. It had several bugs and performed flawlessly. Very satisfied. Meets basic requirements adequately.",
     "198.151.157.66", "60c10a30-c9ca-4c09-8a48-7bbf7c54d47d", "Coastal Restaurant Group",
     "3240a895-2a86-4889-85c3-8bacfa4aba57", "sara.hernandez@example.be", "Germany", "2024-06-20 15:58:42+0200", get_current_timestamp()),

    ("04006ab0-a5d8-4043-a443-57bf64c771ac", "Raymond Robinson", "Perfect Transaction", 5,
     "Several pieces were missing from my order, and attempts to get help have been letdown. Could be better but serves its purpose.",
     "223.76.183.185", "fd108a35-6750-4176-944f-21c97011552b", "Urban Style Clothing",
     "ca8540d4-b01d-4ae4-8e0d-babc4ab3cb69", "raymond.robinson@example.net", "Morocco", "2024-12-29 17:21:49+0100", get_current_timestamp()),
]

duplicate_input_record = [
    ("04006ab0-a5d8-4043-a443-57bf64c771ac", "Raymond Robinson", "Perfect Transaction", 5,
     "Several pieces were missing from my order, and attempts to get help have been letdown. Could be better but serves its purpose.",
     "223.76.183.185", "fd108a35-6750-4176-944f-21c97011552b", "Urban Style Clothing",
     "ca8540d4-b01d-4ae4-8e0d-babc4ab3cb69", "raymond.robinson@example.net", "Morocco", "2024-12-29 17:21:49+0100", get_current_timestamp()),
]

duplicate_input_data = input_data + duplicate_input_record

internal_write_schema = StructType([
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
    StructField("review_date", StringType(), True),
    StructField("last_updated", TimestampType(), True)
])


old_data = [
    ("9e4feb04-717f-4568-8c77-e5784ae6fb2d", "Sara Cook", "Perfect Match", 2,
     "The transaction went seamlessly from start to finish. The purchase arrived on time and was exactly what I exceeded my expectations. Has some room for improvement. 🥉 😴",
     "143.190.221.173", "24a6a92a-f745-455f-b669-f2f02842039f", "Artisan Coffee Roasters",
     "9f51330e-f123-48b6-88fb-00020e824fc2", "sara.cook@example.no", "France", "2024-10-17 10:43:39+0200", old_data_lit()),
]

