import shutil
from pathlib import Path
from pyspark.sql import SparkSession
from ts_review.main import main


def test_integration_main_with_roles(tmp_path, monkeypatch):
    """
    Full integration test using actual CSV under tests/resources,
    applying role-based governance and writing CSV to test/output.
    Uses real command-line args to invoke main().
    """

    # ---- Setup Spark ----
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("ts_review_integration_test")
        .getOrCreate()
    )

    # ---- Prepare paths ----
    base_dir = Path(__file__).parent
    print(base_dir)
    input_path = base_dir / "resources" / "test_tp_reviews.csv"
    output_dir = base_dir / "output"
    expectations_path = base_dir / "resources" / "ts_expectations.json"
    print(expectations_path, "EXPECTATIONS PATH")
    output_dir.mkdir(exist_ok=True)

    # ---- Monkeypatch sys.argv or simulate CLI args ----
    import sys
    sys.argv = [
        "main.py",
        "--input_path", str(input_path),
        "--output_path", str(output_dir),
        "--expectations_path", str(expectations_path),
        "--role", "analyst",  # test one role per run
    ]

    # ---- Run the actual pipeline ----
    main()

    # ---- Verify output ----
    generated_files = list(output_dir.glob("*.csv"))
    assert generated_files, "Expected governed CSV output file"
    assert generated_files[0].stat().st_size > 0, "CSV should not be empty"

    # ---- Cleanup ----
    shutil.rmtree(output_dir)
    spark.stop()
