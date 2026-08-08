#!/usr/bin/env python3

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

"""Validate, combine, and run class-level JVM and PySpark test targets proposed by Copilot CLI.

Copilot receives a commit diff and returns relevance-ranked ``JVM:`` and ``PYTHON:``
lines. It does not choose a shell command: this helper resolves each accepted name against
the checked-out Spark test catalog before the workflow runs it.
"""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys

from sparktestsupport import modules


SPARK_HOME = Path(__file__).resolve().parents[1]
# Keep the post-merge job bounded even when a broad change has many related tests.
MAX_JVM_TESTS = 20
MAX_PYTHON_TESTS = 20
PACKAGE_PATTERN = re.compile(r"^\s*package\s+([A-Za-z_][\w.]*)\s*(?:\{|;|$)", re.MULTILINE)
# This deliberately recognizes only conventional top-level Scala or Java suite declarations. A
# conservative catalog is preferable to trying to execute a helper, abstract base, or nested class.
SUITE_PATTERN = re.compile(
    r"^\s*(?:(?:public|protected|private|abstract|final|static)\s+)*"
    r"class\s+([A-Za-z_][\w]*(?:Suite|Test))\b",
    re.MULTILINE,
)
# Accept only the documented, machine-readable response format and ignore explanatory Copilot text.
SELECTION_PATTERN = re.compile(r"^(JVM|PYTHON):\s*([A-Za-z_][\w.]*)\s*$")
HADOOP_PROFILES = {"hadoop3": ["-Phadoop-3"]}
# Some test source trees belong to more than one build module. These prefixes identify the
# corresponding unambiguous SBT test project before the generic module lookup below.
SPECIAL_SBT_GOALS = (
    ("sql/connect/client/jdbc/", "connect-client-jdbc/test"),
    ("sql/connect/client/jvm/", "connect-client-jvm/test"),
    ("sql/connect/server/", "connect/test"),
    ("connector/kafka-0-10-token-provider/", "token-provider-kafka-0-10/test"),
    ("connector/kafka-0-10/", "streaming-kafka-0-10/test"),
    ("common/network-yarn/", "network-yarn/test"),
    ("resource-managers/yarn/", "yarn/test"),
)


def resolve_jvm_test_target(relative_path, matching_modules):
    """Return the module and SBT target for a test source, if it is unambiguous."""
    for prefix, target in SPECIAL_SBT_GOALS:
        if relative_path.startswith(prefix):
            # Parent and child module paths can both match a source file. Prefer the explicitly
            # listed child project so SBT compiles and runs the suite in its owning module.
            module = next(module for module in matching_modules if target in module.sbt_test_goals)
            return module, target
    test_modules = [module for module in matching_modules if module.sbt_test_goals]
    test_goals = [goal for module in test_modules for goal in module.sbt_test_goals]
    if len(test_goals) == 1:
        return test_modules[0], test_goals[0]
    return None


def jvm_suites_in_file(path):
    """Return fully-qualified Scala or Java suite names declared by a source file."""
    contents = path.read_text(encoding="utf-8")
    package_match = PACKAGE_PATTERN.search(contents)
    if package_match is None:
        return []
    # The suite name alone is insufficient for SBT's ``testOnly``; construct its FQCN from the
    # package declaration so the selector is never asked to infer module-local package names.
    return [f"{package_match.group(1)}.{suite}" for suite in SUITE_PATTERN.findall(contents)]


def jvm_test_catalog():
    """Return exact Scala or Java suites that have one unambiguous SBT test project."""
    catalog = {}
    test_source_files = list(SPARK_HOME.glob("**/src/test/scala/**/*.scala"))
    test_source_files.extend(SPARK_HOME.glob("**/src/test/java/**/*.java"))
    for path in test_source_files:
        relative_path = str(path.relative_to(SPARK_HOME))
        # SparkR tests are intentionally outside this workflow's JVM/Python scope.
        if relative_path.startswith("R/"):
            continue
        matching_modules = [
            module for module in modules.all_modules if module.contains_file(relative_path)
        ]
        # Skip sources that cannot be mapped to one runnable SBT target rather than guessing.
        target = resolve_jvm_test_target(relative_path, matching_modules)
        if target is None:
            continue
        module, sbt_test_goal = target
        for suite in jvm_suites_in_file(path):
            catalog.setdefault(
                suite,
                {
                    "environment": module.environ,
                    "profiles": list(module.build_profile_flags),
                    "target": sbt_test_goal,
                },
            )
    return catalog


def python_test_catalog():
    """Return runnable PySpark unittest and doctest modules."""
    catalog = set()
    # Test files are not all listed in module metadata, so discover them directly from the tree.
    for path in SPARK_HOME.glob("python/pyspark/**/test_*.py"):
        relative_path = path.relative_to(SPARK_HOME / "python")
        catalog.add(".".join(relative_path.with_suffix("").parts))
    # Module metadata includes doctest targets, such as pyspark.sql.types, that have no test_ file.
    for module in modules.all_modules:
        catalog.update(module.python_test_goals)
    return catalog


def validate_selection(selection):
    # Copilot output is untrusted. Only exact catalog entries may reach the test runner.
    jvm_catalog = jvm_test_catalog()
    python_catalog = python_test_catalog()
    jvm_tests = []
    python_tests = []
    selected_jvm_tests = set()
    for line in selection.splitlines():
        match = SELECTION_PATTERN.fullmatch(line)
        if match is None:
            continue
        kind, test_name = match.groups()
        if kind == "JVM" and test_name in jvm_catalog:
            if test_name not in selected_jvm_tests:
                jvm_tests.append({"suite": test_name, **jvm_catalog[test_name]})
                selected_jvm_tests.add(test_name)
        elif kind == "PYTHON" and test_name in python_catalog and test_name not in python_tests:
            python_tests.append(test_name)
        # Cap each language independently; continue until both caps are reached so one language
        # cannot prevent valid selections in the other.
        if len(jvm_tests) >= MAX_JVM_TESTS and len(python_tests) >= MAX_PYTHON_TESTS:
            break
    # The response order is the selector's relevance ranking, so retain the first valid targets.
    return {
        "python": python_tests[:MAX_PYTHON_TESTS],
        "jvm": jvm_tests[:MAX_JVM_TESTS],
    }


def merge_selections(selections):
    """Deduplicate validated selections while preserving their commit order."""
    if not selections:
        return {"python": [], "jvm": []}
    jvm_catalog = jvm_test_catalog()
    python_catalog = python_test_catalog()
    jvm_tests = []
    python_tests = []
    selected_jvm_tests = set()
    selected_python_tests = set()
    for selection in selections:
        for test in selection.get("jvm", []):
            if not isinstance(test, dict):
                continue
            suite = test.get("suite")
            if suite in jvm_catalog and suite not in selected_jvm_tests:
                # Reconstruct metadata from the checkout rather than trusting serialized input.
                expected = {"suite": suite, **jvm_catalog[suite]}
                if test == expected:
                    jvm_tests.append(expected)
                    selected_jvm_tests.add(suite)
        for module in selection.get("python", []):
            if module in python_catalog and module not in selected_python_tests:
                python_tests.append(module)
                selected_python_tests.add(module)
    # This command is retained for callers that combine independently validated results. The
    # workflow currently validates just the pushed tip commit, not its full commit history.
    return {"python": python_tests, "jvm": jvm_tests}


def run_jvm_tests(selection):
    jvm_catalog = jvm_test_catalog()
    selected_tests = []
    for test in selection["jvm"]:
        suite = test["suite"]
        expected = {
            "environment": test["environment"],
            "profiles": test["profiles"],
            "target": test["target"],
        }
        if suite not in jvm_catalog or jvm_catalog[suite] != expected:
            raise ValueError(f"Invalid JVM test selection: {suite}")
        selected_tests.append(test)
    # The selected JSON crosses job/artifact boundaries. Revalidate it in the runner so a stale
    # or modified artifact cannot add an arbitrary SBT project, profile, or environment variable.
    hadoop_profile = os.environ.get("HADOOP_PROFILE", "hadoop3")
    if hadoop_profile not in HADOOP_PROFILES:
        raise ValueError(f"Unsupported Hadoop profile: {hadoop_profile}")
    for test in selected_tests:
        # Run one suite per SBT invocation so failures identify the exact selected class.
        # Module metadata records an SBT test task such as ``core/test``. Replace that task with
        # ``testOnly`` rather than appending it, which would incorrectly create
        # ``core/test/testOnly``.
        if not test["target"].endswith("test"):
            raise ValueError(f"Invalid SBT test target: {test['target']}")
        test_only_command = f"{test['target'][:-4]}testOnly {test['suite']}"
        environment = os.environ.copy()
        environment.update(test["environment"])
        subprocess.run(
            [
                str(SPARK_HOME / "build" / "sbt"),
                *HADOOP_PROFILES[hadoop_profile],
                *test["profiles"],
                test_only_command,
            ],
            check=True,
            env=environment,
        )


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("validate")
    subparsers.add_parser("merge")
    subparsers.add_parser("run-jvm")
    args = parser.parse_args()
    if args.command == "validate":
        json.dump(validate_selection(sys.stdin.read()), sys.stdout, separators=(",", ":"))
    elif args.command == "merge":
        selections = [json.loads(line) for line in sys.stdin if line.strip()]
        json.dump(merge_selections(selections), sys.stdout, separators=(",", ":"))
    else:
        run_jvm_tests(json.load(sys.stdin))


if __name__ == "__main__":
    main()
