import subprocess
import os
import sys
import tempfile
from pyspark.sql import SparkSession

def main():
    # 1. Create a temporary working directory for the pipeline project
    project_dir = tempfile.mkdtemp(prefix="test_pipeline_")
    os.chdir(project_dir)

    # 2. Initialize a new pipeline project
    subprocess.run(["spark-pipelines", "init", "--name", "test_pipeline"], check=True)

    # 3. Move into the project directory
    os.chdir("test_pipeline")

    # 5. Run the pipeline via the CLI
    print("Executing spark-pipelines run...")
    subprocess.run(["spark-pipelines", "run", "--remote", "sc://localhost:15002"], check=True)

    # 6. After the pipeline runs, start a SparkSession to verify results
    spark = SparkSession.builder \
        .remote("sc://localhost:15002") \
        .appName("verify-spark-pipelines") \
        .getOrCreate()

    # 7. List tables in the default database/catalog
    table_names = [t.name for t in spark.catalog.listTables()]
    print("Tables found in catalog:", table_names)

    # 8. Assert that at least one output table exists (e.g., example_sql_materialized_view)
    expected = "example_sql_materialized_view"
    if expected in table_names:
        print(f"Found expected pipeline output table: {expected}")
    else:
        raise AssertionError(f"Expected table '{expected}' not found in the catalog.")

    spark.stop()
    print("All checks passed!")


if __name__ == "__main__":
    main()
