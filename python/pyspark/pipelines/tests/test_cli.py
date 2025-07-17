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
import textwrap
from pathlib import Path

from pyspark.errors import PySparkException
from pyspark.testing.connectutils import (
    should_test_connect,
    connect_requirement_message,
)
from pyspark.testing.utils import have_yaml, yaml_requirement_message

if should_test_connect and have_yaml:
    from pyspark.pipelines.cli import (
        change_dir,
        find_pipeline_spec,
        load_pipeline_spec,
        register_definitions,
        unpack_pipeline_spec,
        DefinitionsGlob,
        PipelineSpec,
        run,
    )
    from pyspark.pipelines.tests.local_graph_element_registry import LocalGraphElementRegistry


@unittest.skipIf(
    not should_test_connect or not have_yaml,
    connect_requirement_message or yaml_requirement_message,
)
class CLIUtilityTests(unittest.TestCase):
    def test_load_pipeline_spec(self):
        with tempfile.NamedTemporaryFile(mode="w") as tmpfile:
            tmpfile.write(
                """
                {
                    "name": "test_pipeline",
                    "catalog": "test_catalog",
                    "database": "test_database",
                    "configuration": {
                        "key1": "value1",
                        "key2": "value2"
                    },
                    "definitions": [
                        {"glob": {"include": "test_include"}}
                    ]
                }
                """
            )
            tmpfile.flush()
            spec = load_pipeline_spec(Path(tmpfile.name))
            assert spec.name == "test_pipeline"
            assert spec.catalog == "test_catalog"
            assert spec.database == "test_database"
            assert spec.configuration == {"key1": "value1", "key2": "value2"}
            assert len(spec.definitions) == 1
            assert spec.definitions[0].include == "test_include"

    def test_load_pipeline_spec_name_is_required(self):
        with tempfile.NamedTemporaryFile(mode="w") as tmpfile:
            tmpfile.write(
                """
                {
                    "catalog": "test_catalog",
                    "database": "test_database",
                    "configuration": {
                        "key1": "value1",
                        "key2": "value2"
                    },
                    "definitions": [
                        {"glob": {"include": "test_include"}}
                    ]
                }
                """
            )
            tmpfile.flush()
            with self.assertRaises(PySparkException) as context:
                load_pipeline_spec(Path(tmpfile.name))
            self.assertEqual(
                context.exception.getCondition(), "PIPELINE_SPEC_MISSING_REQUIRED_FIELD"
            )
            self.assertEqual(context.exception.getMessageParameters(), {"field_name": "name"})

    def test_load_pipeline_spec_schema_fallback(self):
        with tempfile.NamedTemporaryFile(mode="w") as tmpfile:
            tmpfile.write(
                """
                {
                    "name": "test_pipeline",
                    "catalog": "test_catalog",
                    "schema": "test_database",
                    "configuration": {
                        "key1": "value1",
                        "key2": "value2"
                    },
                    "definitions": [
                        {"glob": {"include": "test_include"}}
                    ]
                }
                """
            )
            tmpfile.flush()
            spec = load_pipeline_spec(Path(tmpfile.name))
            assert spec.catalog == "test_catalog"
            assert spec.database == "test_database"
            assert spec.configuration == {"key1": "value1", "key2": "value2"}
            assert len(spec.definitions) == 1
            assert spec.definitions[0].include == "test_include"

    def test_load_pipeline_spec_invalid(self):
        with tempfile.NamedTemporaryFile(mode="w") as tmpfile:
            tmpfile.write(
                """
                {
                    "catalogtypo": "test_catalog",
                    "configuration": {
                        "key1": "value1",
                        "key2": "value2"
                    },
                    "definitions": [
                        {"glob": {"include": "test_include"}}
                    ]
                }
                """
            )
            tmpfile.flush()
            with self.assertRaises(PySparkException) as context:
                load_pipeline_spec(Path(tmpfile.name))
            self.assertEqual(context.exception.getCondition(), "PIPELINE_SPEC_UNEXPECTED_FIELD")
            self.assertEqual(
                context.exception.getMessageParameters(), {"field_name": "catalogtypo"}
            )

    def test_unpack_empty_pipeline_spec(self):
        empty_spec = PipelineSpec(
            name="test_pipeline", catalog=None, database=None, configuration={}, definitions=[]
        )
        self.assertEqual(unpack_pipeline_spec({"name": "test_pipeline"}), empty_spec)

    def test_unpack_pipeline_spec_bad_configuration(self):
        with self.assertRaises(TypeError) as context:
            unpack_pipeline_spec({"name": "test_pipeline", "configuration": "not_a_dict"})
        self.assertIn("should be a dict", str(context.exception))

        with self.assertRaises(TypeError) as context:
            unpack_pipeline_spec({"name": "test_pipeline", "configuration": {"key": {}}})
        self.assertIn("key", str(context.exception))

        with self.assertRaises(TypeError) as context:
            unpack_pipeline_spec({"name": "test_pipeline", "configuration": {1: "something"}})
        self.assertIn("int", str(context.exception))

    def test_find_pipeline_spec_in_current_directory(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            spec_path = Path(temp_dir) / "pipeline.yaml"
            with spec_path.open("w") as f:
                f.write(
                    """
                    {
                        "catalog": "test_catalog",
                        "configuration": {},
                        "definitions": []
                    }
                    """
                )

            found_spec = find_pipeline_spec(Path(temp_dir))
            self.assertEqual(found_spec, spec_path)

    def test_find_pipeline_spec_in_current_directory_yml(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            spec_path = Path(temp_dir) / "pipeline.yml"
            with spec_path.open("w") as f:
                f.write(
                    """
                    {
                        "catalog": "test_catalog",
                        "configuration": {},
                        "definitions": []
                    }
                    """
                )

            found_spec = find_pipeline_spec(Path(temp_dir))
            self.assertEqual(found_spec, spec_path)

    def test_find_pipeline_spec_in_current_directory_yml_and_yaml(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with (Path(temp_dir) / "pipeline.yml").open("w") as f:
                f.write("")

            with (Path(temp_dir) / "pipeline.yaml").open("w") as f:
                f.write("")

            with self.assertRaises(PySparkException) as context:
                find_pipeline_spec(Path(temp_dir))
            self.assertEqual(context.exception.getCondition(), "MULTIPLE_PIPELINE_SPEC_FILES_FOUND")
            self.assertEqual(context.exception.getMessageParameters(), {"dir_path": temp_dir})

    def test_find_pipeline_spec_in_parent_directory(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            parent_dir = Path(temp_dir)
            child_dir = Path(temp_dir) / "child"
            child_dir.mkdir()
            spec_path = parent_dir / "pipeline.yaml"
            with spec_path.open("w") as f:
                f.write(
                    """
                    {
                        "catalog": "test_catalog",
                        "configuration": {},
                        "definitions": []
                    }
                    """
                )

            found_spec = find_pipeline_spec(Path(child_dir))
            self.assertEqual(found_spec, spec_path)

    def test_register_definitions(self):
        spec = PipelineSpec(
            name="test_pipeline",
            catalog=None,
            database=None,
            configuration={},
            definitions=[DefinitionsGlob(include="subdir1/*")],
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            outer_dir = Path(temp_dir)
            subdir1 = outer_dir / "subdir1"
            subdir1.mkdir()
            subdir2 = outer_dir / "subdir2"
            subdir2.mkdir()
            with (subdir1 / "definitions.py").open("w") as f:
                f.write(
                    textwrap.dedent(
                        """
                        from pyspark import pipelines as sdp
                        @sdp.materialized_view
                        def mv1():
                            raise NotImplementedError()
                    """
                    )
                )

            with (subdir2 / "definitions.py").open("w") as f:
                f.write(
                    textwrap.dedent(
                        """
                        from pyspark import pipelines as sdp
                        def mv2():
                            raise NotImplementedError()
                    """
                    )
                )

            registry = LocalGraphElementRegistry()
            register_definitions(outer_dir / "pipeline.yaml", registry, spec)
            self.assertEqual(len(registry.datasets), 1)
            self.assertEqual(registry.datasets[0].name, "mv1")

    def test_register_definitions_file_raises_error(self):
        """Errors raised while executing definitions code should make it to the outer context."""
        spec = PipelineSpec(
            name="test_pipeline",
            catalog=None,
            database=None,
            configuration={},
            definitions=[DefinitionsGlob(include="*")],
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            outer_dir = Path(temp_dir)
            with (outer_dir / "definitions.py").open("w") as f:
                f.write("raise RuntimeError('This is a test exception')")

            registry = LocalGraphElementRegistry()
            with self.assertRaises(RuntimeError) as context:
                register_definitions(outer_dir / "pipeline.yml", registry, spec)
            self.assertIn("This is a test exception", str(context.exception))

    def test_register_definitions_unsupported_file_extension_matches_glob(self):
        spec = PipelineSpec(
            name="test_pipeline",
            catalog=None,
            database=None,
            configuration={},
            definitions=[DefinitionsGlob(include="*")],
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            outer_dir = Path(temp_dir)
            with (outer_dir / "definitions.java").open("w") as f:
                f.write("")

            registry = LocalGraphElementRegistry()
            with self.assertRaises(PySparkException) as context:
                register_definitions(outer_dir, registry, spec)
            self.assertEqual(
                context.exception.getCondition(), "PIPELINE_UNSUPPORTED_DEFINITIONS_FILE_EXTENSION"
            )

    def test_python_import_current_directory(self):
        """Tests that the Python system path is resolved relative to the dir containing the pipeline
        spec file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            outer_dir = Path(temp_dir)
            inner_dir1 = outer_dir / "inner1"
            inner_dir1.mkdir()
            inner_dir2 = outer_dir / "inner2"
            inner_dir2.mkdir()

            with (inner_dir1 / "defs.py").open("w") as f:
                f.write(
                    textwrap.dedent(
                        """
                        import sys
                        sys.path.append(".")
                        import mypackage.my_module
                        """
                    )
                )

            inner_dir1_mypackage = inner_dir1 / "mypackage"
            inner_dir1_mypackage.mkdir()

            with (inner_dir1_mypackage / "__init__.py").open("w") as f:
                f.write("")

            with (inner_dir1_mypackage / "my_module.py").open("w") as f:
                f.write("")

            registry = LocalGraphElementRegistry()
            with change_dir(inner_dir2):
                register_definitions(
                    inner_dir1 / "pipeline.yaml",
                    registry,
                    PipelineSpec(
                        name="test_pipeline",
                        catalog=None,
                        database=None,
                        configuration={},
                        definitions=[DefinitionsGlob(include="defs.py")],
                    ),
                )

    def test_full_refresh_all_conflicts_with_full_refresh(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a minimal pipeline spec
            spec_path = Path(temp_dir) / "pipeline.yaml"
            with spec_path.open("w") as f:
                f.write('{"name": "test_pipeline"}')

            # Test that providing both --full-refresh-all and --full-refresh raises an exception
            with self.assertRaises(PySparkException) as context:
                run(
                    spec_path=spec_path,
                    full_refresh=["table1", "table2"],
                    full_refresh_all=True,
                    refresh=[],
                    dry=False,
                )

            self.assertEqual(
                context.exception.getCondition(), "CONFLICTING_PIPELINE_REFRESH_OPTIONS"
            )
            self.assertEqual(
                context.exception.getMessageParameters(), {"conflicting_option": "--full_refresh"}
            )

    def test_full_refresh_all_conflicts_with_refresh(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a minimal pipeline spec
            spec_path = Path(temp_dir) / "pipeline.yaml"
            with spec_path.open("w") as f:
                f.write('{"name": "test_pipeline"}')

            # Test that providing both --full-refresh-all and --refresh raises an exception
            with self.assertRaises(PySparkException) as context:
                run(
                    spec_path=spec_path,
                    full_refresh=[],
                    full_refresh_all=True,
                    refresh=["table1", "table2"],
                    dry=False,
                )

            self.assertEqual(
                context.exception.getCondition(), "CONFLICTING_PIPELINE_REFRESH_OPTIONS"
            )
            self.assertEqual(
                context.exception.getMessageParameters(),
                {"conflicting_option": "--refresh"},
            )

    def test_full_refresh_all_conflicts_with_both(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a minimal pipeline spec
            spec_path = Path(temp_dir) / "pipeline.yaml"
            with spec_path.open("w") as f:
                f.write('{"name": "test_pipeline"}')

            # Test that providing --full-refresh-all with both other options raises an exception
            # (it should catch the first conflict - full_refresh)
            with self.assertRaises(PySparkException) as context:
                run(
                    spec_path=spec_path,
                    full_refresh=["table1"],
                    full_refresh_all=True,
                    refresh=["table2"],
                    dry=False,
                )

            self.assertEqual(
                context.exception.getCondition(), "CONFLICTING_PIPELINE_REFRESH_OPTIONS"
            )

    def test_parse_table_list_single_table(self):
        """Test parsing a single table name."""
        from pyspark.pipelines.cli import parse_table_list

        result = parse_table_list("table1")
        self.assertEqual(result, ["table1"])

    def test_parse_table_list_multiple_tables(self):
        """Test parsing multiple table names."""
        from pyspark.pipelines.cli import parse_table_list

        result = parse_table_list("table1,table2,table3")
        self.assertEqual(result, ["table1", "table2", "table3"])

    def test_parse_table_list_with_spaces(self):
        """Test parsing table names with spaces."""
        from pyspark.pipelines.cli import parse_table_list

        result = parse_table_list("table1, table2 , table3")
        self.assertEqual(result, ["table1", "table2", "table3"])


if __name__ == "__main__":
    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
