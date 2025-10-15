import glob
import shutil

from fastapi import FastAPI, Query
from fastapi.responses import StreamingResponse
import io, sys
from pathlib import Path
from pyspark.sql import SparkSession
from ts_review.main import main, create_spark_session
from ts_review.api_data_to_csv.api_data_to_csv import apply_governance_tags

app = FastAPI(title="Trustpilot Reviews Data API")


BASE_DIR = Path(__file__).parent
INPUT_PATH = BASE_DIR / "api_resources" / "tp_reviews.csv"
OUTPUT_PATH = BASE_DIR / "api_results"
EXPECTATIONS_PATH = BASE_DIR / "api_resources" / "ts_expectations.json"


@app.get("/reviews")
def get_reviews(
        business_id: str | None = Query(None),
        reviewer_id: str | None = Query(None),
        email_address: str | None = Query(None),
        role: str = Query(..., description="User role: legal | data_engineering | data_science | marketing | analytics"),
        refresh: bool = Query(False, description="Run full ETL before serving"),
):
    """
    Returns governed review data as CSV for ad-hoc legal requests.
    Optional: `refresh=true` will run full ETL pipeline first.
    """

    spark = create_spark_session()
    # If requested, re-run the full ETL pipeline
    if not OUTPUT_PATH.exists() or refresh:
        sys.argv = [
            "main.py",
            "--input_path", str(INPUT_PATH),
            "--output_path", str(OUTPUT_PATH),
            "--expectations_path", str(EXPECTATIONS_PATH),
            "--role", role,
        ]
        main()  # runs your full ETL pipeline

    df = spark.read.format("delta").load(str(OUTPUT_PATH))

    # Apply filters dynamically
    if business_id:
        df = df.filter(df.business_id == business_id)
    if reviewer_id:
        df = df.filter(df.reviewer_id == reviewer_id)
    if email_address:
        df = df.filter(df.email_address == email_address)

    # Apply governance
    df_governed = apply_governance_tags(df)

    # --- Spark-native CSV export (no pandas) ---
    tmp_dir = Path("/tmp/api_export")
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    # Write CSV to temporary location
    df_governed.coalesce(1).write.mode("overwrite").option("header", True).csv(str(tmp_dir))

    # Find the part file Spark generated
    part_files = glob.glob(str(tmp_dir / "part-*.csv"))
    if not part_files:
        raise RuntimeError("CSV export failed: no output file found.")

    part_file = part_files[0]
    with open(part_file, "r", encoding="utf-8") as f:
        csv_content = f.read()

    # Clean up temp folder (optional)
    shutil.rmtree(tmp_dir, ignore_errors=True)

    # --- Stream CSV as HTTP response ---
    csv_stream = io.StringIO(csv_content)
    return StreamingResponse(
        iter([csv_stream.getvalue().encode("utf-8")]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=reviews.csv"},
    )
