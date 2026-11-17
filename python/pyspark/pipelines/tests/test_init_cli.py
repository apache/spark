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
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pyspark.testing.connectutils import (
    ReusedConnectTestCase,
    connect_requirement_message,
    should_test_connect,
)
from pyspark.testing.sqlutils import SPARK_HOME
from pyspark.testing.utils import have_yaml, yaml_requirement_message

if should_test_connect and have_yaml:
    from pyspark.pipelines.cli import (
        change_dir,
        find_pipeline_spec,
        init,
        load_pipeline_spec,
        register_definitions,
    )
    from pyspark.pipelines.init_cli import hive_is_available
    from pyspark.pipelines.tests.local_graph_element_registry import LocalGraphElementRegistry


@unittest.skipIf(
    not should_test_connect or not have_yaml,
    connect_requirement_message or yaml_requirement_message,
)
class InitCLITests(ReusedConnectTestCase):
    def test_init(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            project_name = "test_project"
            with change_dir(Path(temp_dir)):
                with patch("pyspark.pipelines.init_cli.hive_is_available", return_value=True):
                    init(project_name)
            with change_dir(Path(temp_dir) / project_name):
                spec_path = find_pipeline_spec(Path.cwd())
                spec = load_pipeline_spec(spec_path)
                assert spec.name == project_name
                self.assertEqual(spec.configuration, {"spark.sql.catalogImplementation": "hive"})

                # Verify that the storage path is an absolute URI with file scheme
                expected_storage_path = f"file://{Path.cwd() / 'pipeline-storage'}"
                self.assertEqual(spec.storage, expected_storage_path)

                # Verify that the storage directory was created
                self.assertTrue((Path.cwd() / "pipeline-storage").exists())

                registry = LocalGraphElementRegistry()
                register_definitions(spec_path, registry, spec, self.spark, "test_graph_id")
                self.assertEqual(len(registry.outputs), 1)
                self.assertEqual(registry.outputs[0].name, "example_python_materialized_view")
                self.assertEqual(len(registry.flows), 1)
                self.assertEqual(registry.flows[0].name, "example_python_materialized_view")
                self.assertEqual(registry.flows[0].target, "example_python_materialized_view")
                self.assertEqual(len(registry.sql_files), 1)
                self.assertEqual(
                    registry.sql_files[0].file_path,
                    Path("transformations") / "example_sql_materialized_view.sql",
                )

    def test_init_without_hive(self):
        # SPARK-61826: a Spark installation without Hive cannot start a session with the Hive
        # catalog setting, so init leaves the configuration section out of the spec there.
        with tempfile.TemporaryDirectory() as temp_dir:
            project_name = "test_project"
            with change_dir(Path(temp_dir)):
                with patch("pyspark.pipelines.init_cli.hive_is_available", return_value=False):
                    init(project_name)
            spec = load_pipeline_spec(find_pipeline_spec(Path(temp_dir) / project_name))
            self.assertEqual(spec.name, project_name)
            self.assertEqual(spec.configuration, {})

    def test_hive_is_available(self):
        # SPARK-61826: like the launcher, the check looks for a spark-hive_ jar in SPARK_HOME/jars
        # or, in a source build, in assembly/target/scala-<version>/jars.
        with tempfile.TemporaryDirectory() as spark_home:
            with patch.dict(os.environ, {"SPARK_HOME": spark_home, "SPARK_SCALA_VERSION": "2.13"}):
                self.assertFalse(hive_is_available())

                build_jars_dir = Path(spark_home) / "assembly" / "target" / "scala-2.13" / "jars"
                build_jars_dir.mkdir(parents=True)
                (build_jars_dir / "spark-sql_2.13-4.1.0.jar").touch()
                self.assertFalse(hive_is_available())
                (build_jars_dir / "spark-hive_2.13-4.1.0.jar").touch()
                self.assertTrue(hive_is_available())

                # The jars directory of a binary distribution wins over the one of a source build.
                dist_jars_dir = Path(spark_home) / "jars"
                dist_jars_dir.mkdir()
                self.assertFalse(hive_is_available())
                (dist_jars_dir / "spark-hive_2.13-4.1.0.jar").touch()
                self.assertTrue(hive_is_available())

    def test_run_generated_project_twice(self):
        # SPARK-61826: the tables created by the first run of a generated project must still be
        # known to a second run in a new process, which updates them. That takes the Hive catalog
        # setting init writes, so the test applies to Spark builds with Hive only.
        if not hive_is_available():
            self.skipTest("The Spark installation has no Hive, so init writes no catalog setting")
        with tempfile.TemporaryDirectory() as temp_dir:
            project_name = "test_project"
            with change_dir(Path(temp_dir)):
                init(project_name)
            project_dir = Path(temp_dir) / project_name
            spec = load_pipeline_spec(find_pipeline_spec(project_dir))
            self.assertEqual(spec.configuration, {"spark.sql.catalogImplementation": "hive"})

            python_example = project_dir / "transformations" / "example_python_materialized_view.py"
            python_table_dir = project_dir / "spark-warehouse" / "example_python_materialized_view"
            sql_table_dir = project_dir / "spark-warehouse" / "example_sql_materialized_view"

            self.run_pipeline(project_dir)
            self.assertEqual(self.spark.read.parquet(str(python_table_dir)).count(), 10)
            self.assertEqual(self.spark.read.parquet(str(sql_table_dir)).count(), 5)

            # The second run must find the tables of the first one and recompute them.
            python_example.write_text(
                python_example.read_text().replace("spark.range(10)", "spark.range(20)")
            )
            self.run_pipeline(project_dir)
            self.assertEqual(self.spark.read.parquet(str(python_table_dir)).count(), 20)
            self.assertEqual(self.spark.read.parquet(str(sql_table_dir)).count(), 10)

            # Both runs started from the project directory, so the metastore they share is there.
            self.assertTrue((project_dir / "metastore_db").is_dir())

    def run_pipeline(self, project_dir: Path) -> None:
        """Runs `spark-pipelines run` from the project directory in its own process."""
        command = [os.path.join(SPARK_HOME, "bin", "spark-pipelines"), "run"]
        env = dict(os.environ)
        env.update(
            {
                "PYSPARK_PYTHON": sys.executable,
                "PYSPARK_DRIVER_PYTHON": sys.executable,
                # The run starts a local Spark Connect server. In testing mode it binds an
                # ephemeral port, which keeps it clear of the server of the test session.
                "SPARK_TESTING": "1",
            }
        )
        # The test runner's submit arguments move the warehouse of a Spark session it starts to a
        # directory of its own. A user's run keeps its tables under the project directory, which
        # is where this test reads them from.
        env.pop("PYSPARK_SUBMIT_ARGS", None)
        proc = subprocess.run(
            command, cwd=project_dir, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
        output = proc.stdout.decode("utf-8")
        self.assertEqual(0, proc.returncode, output)
        for flow_name in ["example_python_materialized_view", "example_sql_materialized_view"]:
            self.assertIn(f"{flow_name} has COMPLETED", output)

    def test_init_existing_directory(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            project_name = "test_project"
            with change_dir(Path(temp_dir)):
                init(project_name)

                with self.assertRaises(FileExistsError) as context:
                    init(project_name)

                expected_message = (
                    f"Directory '{project_name}' already exists. "
                    "Please choose a different name or remove the existing directory."
                )
                self.assertEqual(str(context.exception), expected_message)


if __name__ == "__main__":
    from pyspark.testing import main

    main()
