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
import unittest
from unittest import mock

from sparktestsupport import modules
from sparktestsupport.utils import (
    determine_dangling_python_tests,
    determine_modules_for_files,
    determine_modules_to_test,
    determine_shell_script_test_goals,
    identify_changed_files_from_git_commits,
)


class DetermineModulesForFilesTests(unittest.TestCase):
    def test_github_files_only_count_in_github_actions(self):
        for environ, expected in (({}, set()), ({"GITHUB_ACTIONS": "true"}, {modules.root})):
            with self.subTest(environ=environ), mock.patch.dict(os.environ, environ, clear=True):
                self.assertEqual(
                    determine_modules_for_files([".github/workflows/build_main.yml"]), expected
                )


class IdentifyChangedFilesFromGitHistoryTests(unittest.TestCase):
    def changed_modules(self, patch_sha, target_ref):
        changed_files = identify_changed_files_from_git_commits(patch_sha, target_ref=target_ref)
        return determine_modules_for_files(changed_files)

    def test_graphx_commit_selects_graphx_module(self):
        self.assertEqual(self.changed_modules("fc0a1475ef", "5da21f07"), {modules.graphx})

    def test_unmatched_commit_selects_root_module(self):
        self.assertIn(modules.root, self.changed_modules("50a0496a43", "6765ef9"))


class DetermineModulesToTestTests(unittest.TestCase):
    def test_root_short_circuits_dependents(self):
        for module in (modules.root, modules.build, modules.core, modules.launcher):
            with self.subTest(module=module.name):
                self.assertEqual(determine_modules_to_test([module]), [modules.root])

    def test_sql_dependents(self):
        expected = {
            "avro",
            "connect",
            "docker-integration-tests",
            "examples",
            "hive",
            "hive-thriftserver",
            "mllib",
            "pipelines",
            "protobuf",
            "pyspark-connect",
            "pyspark-ml",
            "pyspark-ml-connect",
            "pyspark-mllib",
            "pyspark-pandas",
            "pyspark-pandas-connect",
            "pyspark-pandas-slow",
            "pyspark-pandas-slow-connect",
            "pyspark-pipelines",
            "pyspark-sql",
            "pyspark-structured-streaming",
            "pyspark-structured-streaming-connect",
            "pyspark-testing",
            "repl",
            "sparkr",
            "sql",
            "sql-kafka-0-10",
        }
        for changed, deduplicated in (
            ([modules.sql], True),
            ([modules.sparkr, modules.sql], False),
        ):
            with self.subTest(changed=changed, deduplicated=deduplicated):
                result = determine_modules_to_test(changed, deduplicated=deduplicated)
                self.assertCountEqual([module.name for module in result], expected)

    def test_core_and_sql_without_deduplication(self):
        expected = {
            "avro",
            "catalyst",
            "connect",
            "core",
            "credential-aws",
            "docker-integration-tests",
            "examples",
            "graphx",
            "hive",
            "hive-thriftserver",
            "mllib",
            "mllib-local",
            "pipelines",
            "protobuf",
            "pyspark-connect",
            "pyspark-core",
            "pyspark-errors",
            "pyspark-ml",
            "pyspark-ml-connect",
            "pyspark-mllib",
            "pyspark-pandas",
            "pyspark-pandas-connect",
            "pyspark-pandas-slow",
            "pyspark-pandas-slow-connect",
            "pyspark-pipelines",
            "pyspark-resource",
            "pyspark-sql",
            "pyspark-streaming",
            "pyspark-structured-streaming",
            "pyspark-structured-streaming-connect",
            "pyspark-testing",
            "repl",
            "root",
            "sparkr",
            "sql",
            "sql-kafka-0-10",
            "streaming",
            "streaming-kafka-0-10",
            "streaming-kinesis-asl",
        }
        result = determine_modules_to_test([modules.sql, modules.core], deduplicated=False)
        self.assertCountEqual([module.name for module in result], expected)


class DetermineShellScriptTestGoalsTests(unittest.TestCase):
    def test_preserves_order_and_deduplicates_goals(self):
        a, b, c = (modules.ShellScriptTestGoal(f"dev/{name}") for name in "abc")
        with mock.patch.object(modules, "all_modules", []):
            first = modules.Module("first", [], [], shell_script_test_goals=[b, a])
            duplicate_b = modules.ShellScriptTestGoal("./dev//b")
            second = modules.Module("second", [], [], shell_script_test_goals=[c, duplicate_b])
            self.assertEqual(
                determine_shell_script_test_goals([first, second], modules.TestStage.PRE_BUILD),
                (b, a, c),
            )

    def test_root_includes_test_dependencies(self):
        self.assertIn(
            modules.ShellScriptTestGoal("dev/test-dependencies.sh"),
            determine_shell_script_test_goals([modules.root], modules.TestStage.PRE_BUILD),
        )


class DetermineDanglingPythonTestsTests(unittest.TestCase):
    def setUp(self):
        # Pretend every changed file exists so results do not depend on the working directory.
        self.enterContext(mock.patch("os.path.exists", return_value=True))

    def test_unregistered_pyspark_test_is_dangling(self):
        filename = "python/pyspark/sql/tests/test_unregistered.py"
        self.assertEqual(determine_dangling_python_tests([filename]), {filename})

    def test_test_outside_python_is_not_dangling(self):
        filename = "dev/sparktestsupport/tests/test_utils.py"
        self.assertEqual(determine_dangling_python_tests([filename]), set())
