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
from pathlib import Path
from unittest import mock

from sparktestsupport import SPARK_HOME, modules
from sparktestsupport.shellutils import is_exe
from sparktestsupport.utils import determine_modules_for_files, determine_modules_to_test


class IsIgnoredFileTests(unittest.TestCase):
    def test_dots_in_patterns_are_literal(self):
        self.assertTrue(modules.is_ignored_file(".asf.yaml"))
        self.assertFalse(modules.is_ignored_file("xasfZyaml"))

    def test_sparktestsupport_tests_are_ignored(self):
        self.assertTrue(modules.is_ignored_file("dev/sparktestsupport/tests/test_modules.py"))

    def test_sparktestsupport_source_is_not_ignored(self):
        self.assertFalse(modules.is_ignored_file("dev/sparktestsupport/modules.py"))


class ShellScriptTestGoalTests(unittest.TestCase):
    def test_default_stage(self):
        goal = modules.ShellScriptTestGoal("dev/example")
        self.assertIs(goal.stage, modules.TestStage.PRE_BUILD)

    def test_normalizes_and_hashes_equivalent_paths(self):
        goal = modules.ShellScriptTestGoal("dev/example")
        equivalents = (
            modules.ShellScriptTestGoal(Path("dev/example")),
            modules.ShellScriptTestGoal("./dev//example"),
        )
        self.assertEqual(goal.path, Path("dev/example"))
        for equivalent in equivalents:
            with self.subTest(goal=equivalent):
                self.assertEqual(equivalent.path, goal.path)
                self.assertEqual(equivalent, goal)
        self.assertEqual(len({goal, *equivalents}), 1)

    def test_rejects_invalid_stages(self):
        for stage in ("post_build", "pre_build", None):
            with self.subTest(stage=stage):
                with self.assertRaises(TypeError) as error:
                    modules.ShellScriptTestGoal("dev/example", stage)
                self.assertEqual(str(error.exception), f"Unsupported test stage: {stage!r}")

    def test_rejects_invalid_paths(self):
        absolute_path = Path.cwd() / "example"
        for path in (
            str(absolute_path),
            absolute_path,
            "",
            "  ",
            ".",
            "../example",
            "dev/../example",
        ):
            with self.subTest(path=path):
                with self.assertRaises(ValueError) as error:
                    modules.ShellScriptTestGoal(path)
                self.assertEqual(
                    str(error.exception),
                    f"Shell script path must be relative to repo root: {str(path)!r}",
                )


class ModuleTests(unittest.TestCase):
    def test_duplicate_name_does_not_change_module_graph(self):
        with mock.patch.object(modules, "all_modules", []):
            dependency = modules.Module("dependency", [], [])
            original = modules.Module("core", [], [])
            with self.assertRaisesRegex(ValueError, "module name 'core' is already in use"):
                modules.Module("core", [dependency], [])
            self.assertEqual(modules.all_modules, [dependency, original])
            self.assertEqual(dependency.dependent_modules, set())

    def test_shell_script_test_goals_accept_sequences(self):
        goal = modules.ShellScriptTestGoal("dev/lint")
        with mock.patch.object(modules, "all_modules", []):
            for index, goals in enumerate(([goal], (goal,))):
                with self.subTest(goals=goals):
                    module = modules.Module(f"valid-{index}", [], [], shell_script_test_goals=goals)
                    self.assertEqual(module.shell_script_test_goals, goals)

    def test_shell_script_test_goals_require_a_sequence(self):
        goal = modules.ShellScriptTestGoal("dev/lint")
        invalid_goals = (
            "dev/lint-java",
            b"dev/lint-java",
            goal,
            (item for item in [goal]),
        )
        with mock.patch.object(modules, "all_modules", []):
            dependency = modules.Module("dependency", [], [])
            for goals in invalid_goals:
                with self.subTest(goals=goals):
                    with self.assertRaisesRegex(
                        TypeError, "^invalid: shell_script_test_goals must be a sequence$"
                    ):
                        modules.Module("invalid", [dependency], [], shell_script_test_goals=goals)
                    self.assertEqual(modules.all_modules, [dependency])
                    self.assertEqual(dependency.dependent_modules, set())

    def test_shell_script_test_goals_reject_invalid_items(self):
        goal = modules.ShellScriptTestGoal("dev/lint")
        with mock.patch.object(modules, "all_modules", []):
            dependency = modules.Module("dependency", [], [])
            for goals in (["dev/lint-java"], [goal, "dev/lint-java"]):
                with self.subTest(goals=goals):
                    with self.assertRaisesRegex(
                        TypeError, "^invalid: expected ShellScriptTestGoal, got 'dev/lint-java'$"
                    ):
                        modules.Module("invalid", [dependency], [], shell_script_test_goals=goals)
                    self.assertEqual(modules.all_modules, [dependency])
                    self.assertEqual(dependency.dependent_modules, set())


class ModuleDefinitionsTests(unittest.TestCase):
    def test_all_pyspark_modules_depend_on_base(self):
        for module in modules.all_modules:
            if module.name.startswith("pyspark-") and module is not modules.pyspark_base:
                with self.subTest(module=module.name):
                    self.assertIn(modules.pyspark_base, modules.transitive_dependencies(module))

    def test_no_module_depends_on_root(self):
        for module in modules.all_modules:
            with self.subTest(module=module.name):
                self.assertNotIn(modules.root, module.dependencies)

    def test_root_includes_all_build_profiles(self):
        expected = {
            flag
            for module in modules.all_modules
            if module is not modules.root
            for flag in module.build_profile_flags
        }
        self.assertEqual(set(modules.root.build_profile_flags), expected)

    def test_shell_script_test_goals_are_executable(self):
        for module in modules.all_modules:
            for goal in module.shell_script_test_goals:
                with self.subTest(module=module.name, goal=str(goal.path)):
                    self.assertTrue(is_exe(os.path.join(SPARK_HOME, goal.path)))

    def test_streaming_kafka_does_not_match_sql_kafka(self):
        self.assertTrue(
            modules.streaming_kafka_0_10.contains_file("connector/kafka-0-10/test.scala")
        )
        self.assertFalse(
            modules.streaming_kafka_0_10.contains_file("connector/kafka-0-10-sql/test.scala")
        )

    def test_any_python_change_selects_periodic_tests(self):
        for filename in (
            "python/pyspark/test.py",
            "python/run-tests.py",
            "python/packaging/setup.py",
        ):
            with self.subTest(filename=filename):
                self.assertIn(modules.pyspark_periodic, determine_modules_for_files([filename]))

    def test_kinesis_changes_select_kinesis_module(self):
        kinesis_files = ["connector/kinesis-asl/src/test/scala/KinesisTest.scala"]
        self.assertIn(modules.streaming_kinesis_asl, determine_modules_for_files(kinesis_files))

    def test_streaming_changes_do_not_select_kinesis_tests(self):
        for deduplicated in (True, False):
            with self.subTest(deduplicated=deduplicated):
                modules_to_test = determine_modules_to_test(
                    [modules.streaming], deduplicated=deduplicated
                )
                self.assertNotIn(modules.root, modules_to_test)
                self.assertNotIn(modules.streaming_kinesis_asl, modules_to_test)
