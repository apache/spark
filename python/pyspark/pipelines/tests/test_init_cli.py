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
import unittest
import tempfile
from pathlib import Path

from pyspark.testing.connectutils import (
    ReusedConnectTestCase,
    should_test_connect,
    connect_requirement_message,
)
from pyspark.testing.utils import have_yaml, yaml_requirement_message


if should_test_connect and have_yaml:
    from pyspark.pipelines.cli import (
        change_dir,
        find_pipeline_spec,
        load_pipeline_spec,
        init,
        register_definitions,
    )
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
                init(project_name)
            with change_dir(Path(temp_dir) / project_name):
                spec_path = find_pipeline_spec(Path.cwd())
                spec = load_pipeline_spec(spec_path)
                assert spec.name == project_name
                registry = LocalGraphElementRegistry()
                register_definitions(spec_path, registry, spec)
                self.assertEqual(len(registry.datasets), 1)
                self.assertEqual(registry.datasets[0].name, "example_python_materialized_view")
                self.assertEqual(len(registry.flows), 1)
                self.assertEqual(registry.flows[0].name, "example_python_materialized_view")
                self.assertEqual(registry.flows[0].target, "example_python_materialized_view")
                self.assertEqual(len(registry.sql_files), 1)
                self.assertEqual(
                    registry.sql_files[0].file_path,
                    Path("transformations") / "example_sql_materialized_view.sql",
                )


if __name__ == "__main__":
    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
