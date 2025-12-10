#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from pathlib import Path

SPEC = """
name: {{ name }}
storage: {{ storage_root }}
libraries:
  - glob:
      include: transformations/**
"""

PYTHON_EXAMPLE = """from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession

spark = SparkSession.active()

@dp.materialized_view
def example_python_materialized_view() -> DataFrame:
    return spark.range(10)
"""

SQL_EXAMPLE = """CREATE MATERIALIZED VIEW example_sql_materialized_view AS
SELECT id FROM example_python_materialized_view
WHERE id % 2 = 0
"""


def init(name: str) -> None:
    """Generates a simple pipeline project."""
    project_dir = Path.cwd() / name
    if project_dir.exists():
        raise FileExistsError(
            f"Directory '{name}' already exists. "
            "Please choose a different name or remove the existing directory."
        )
    project_dir.mkdir(parents=True, exist_ok=False)

    # Create the storage directory
    storage_dir = project_dir / "pipeline-storage"
    storage_dir.mkdir(parents=True)

    # Create absolute file URI for storage path
    storage_path = f"file://{storage_dir.resolve()}"

    # Write the spec file to the project directory
    spec_file = project_dir / "spark-pipeline.yml"
    with open(spec_file, "w") as f:
        spec_content = SPEC.replace("{{ name }}", name).replace("{{ storage_root }}", storage_path)
        f.write(spec_content)

    # Create the transformations directory
    transformations_dir = project_dir / "transformations"
    transformations_dir.mkdir(parents=True)

    # Create the Python example file
    python_example_file = transformations_dir / "example_python_materialized_view.py"
    with open(python_example_file, "w") as f:
        f.write(PYTHON_EXAMPLE)

    # Create the SQL example file
    sql_example_file = transformations_dir / "example_sql_materialized_view.sql"
    with open(sql_example_file, "w") as f:
        f.write(SQL_EXAMPLE)

    print(f"Pipeline project '{name}' created successfully. To run your pipeline:")
    print(f"cd '{name}'")
    print("spark-pipelines run")
