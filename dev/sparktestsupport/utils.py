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

import os
import sys
import subprocess
from sparktestsupport import modules
from sparktestsupport.shellutils import run_cmd
from sparktestsupport.toposort import toposort_flatten

# -------------------------------------------------------------------------------------------------
# Functions for traversing module dependency graph
# -------------------------------------------------------------------------------------------------


def determine_modules_for_files(filenames):
    """
    Given a list of filenames, return the set of modules that contain those files.
    If a file is not associated with a more specific submodule, then this method will consider that
    file to belong to the 'root' module. `.github` directory is counted only in GitHub Actions,
    and `README.md` is always ignored.

    >>> sorted(x.name for x in determine_modules_for_files(["python/pyspark/a.py", "sql/core/foo"]))
    ['pyspark-core', 'pyspark-errors', 'sql']
    >>> [x.name for x in determine_modules_for_files(["file_not_matched_by_any_subproject"])]
    ['root']
    >>> [x.name for x in determine_modules_for_files(["sql/README.md"])]
    []
    """
    changed_modules = set()
    for filename in filenames:
        if filename.endswith("README.md"):
            continue
        if ("GITHUB_ACTIONS" not in os.environ) and filename.startswith(".github"):
            continue
        matched_at_least_one_module = False
        for module in modules.all_modules:
            if module.contains_file(filename):
                changed_modules.add(module)
                matched_at_least_one_module = True
        if not matched_at_least_one_module:
            changed_modules.add(modules.root)
    return changed_modules


def identify_changed_files_from_git_commits(patch_sha, target_branch=None, target_ref=None):
    """
    Given a git commit and target ref, use the set of files changed in the diff in order to
    determine which modules' tests should be run.

    >>> [x.name for x in determine_modules_for_files( \
            identify_changed_files_from_git_commits("fc0a1475ef", target_ref="5da21f07"))]
    ['graphx']
    >>> 'root' in [x.name for x in determine_modules_for_files( \
         identify_changed_files_from_git_commits("50a0496a43", target_ref="6765ef9"))]
    True
    """
    if target_branch is None and target_ref is None:
        raise AttributeError("must specify either target_branch or target_ref")
    elif target_branch is not None and target_ref is not None:
        raise AttributeError("must specify either target_branch or target_ref, not both")
    if target_branch is not None:
        diff_target = target_branch
        run_cmd(["git", "fetch", "origin", str(target_branch + ":" + target_branch)])
    else:
        diff_target = target_ref
    raw_output = subprocess.check_output(
        ["git", "diff", "--name-only", patch_sha, diff_target], universal_newlines=True
    )
    # Remove any empty strings
    return [f for f in raw_output.split("\n") if f]


def determine_modules_to_test(changed_modules, deduplicated=True):
    """
    Given a set of modules that have changed, compute the transitive closure of those modules'
    dependent modules in order to determine the set of modules that should be tested.

    Returns a topologically-sorted list of modules (ties are broken by sorting on module names).
    If ``deduplicated`` is disabled, the modules are returned without tacking the deduplication
    by dependencies into account.

    >>> [x.name for x in determine_modules_to_test([modules.root])]
    ['root']
    >>> [x.name for x in determine_modules_to_test([modules.build])]
    ['root']
    >>> [x.name for x in determine_modules_to_test([modules.core])]
    ['root']
    >>> [x.name for x in determine_modules_to_test([modules.launcher])]
    ['root']
    >>> [x.name for x in determine_modules_to_test([modules.graphx])]
    ['graphx', 'examples']
    >>> sorted([x.name for x in determine_modules_to_test([modules.sql])])
    ... # doctest: +NORMALIZE_WHITESPACE
    ['avro', 'connect', 'docker-integration-tests', 'examples', 'hive', 'hive-thriftserver',
     'mllib', 'protobuf', 'pyspark-connect', 'pyspark-ml', 'pyspark-ml-connect', 'pyspark-mllib',
     'pyspark-pandas', 'pyspark-pandas-connect-part0', 'pyspark-pandas-connect-part1',
     'pyspark-pandas-connect-part2', 'pyspark-pandas-connect-part3', 'pyspark-pandas-slow',
     'pyspark-sql', 'pyspark-testing', 'repl', 'sparkr', 'sql', 'sql-kafka-0-10']
    >>> sorted([x.name for x in determine_modules_to_test(
    ...     [modules.sparkr, modules.sql], deduplicated=False)])
    ... # doctest: +NORMALIZE_WHITESPACE
    ['avro', 'connect', 'docker-integration-tests', 'examples', 'hive', 'hive-thriftserver',
     'mllib', 'protobuf', 'pyspark-connect', 'pyspark-ml', 'pyspark-ml-connect', 'pyspark-mllib',
     'pyspark-pandas', 'pyspark-pandas-connect-part0', 'pyspark-pandas-connect-part1',
     'pyspark-pandas-connect-part2', 'pyspark-pandas-connect-part3', 'pyspark-pandas-slow',
     'pyspark-sql', 'pyspark-testing', 'repl', 'sparkr', 'sql', 'sql-kafka-0-10']
    >>> sorted([x.name for x in determine_modules_to_test(
    ...     [modules.sql, modules.core], deduplicated=False)])
    ... # doctest: +NORMALIZE_WHITESPACE
    ['avro', 'catalyst', 'connect', 'core', 'docker-integration-tests', 'examples', 'graphx',
     'hive', 'hive-thriftserver', 'mllib', 'mllib-local', 'protobuf', 'pyspark-connect',
     'pyspark-core', 'pyspark-ml', 'pyspark-ml-connect', 'pyspark-mllib', 'pyspark-pandas',
     'pyspark-pandas-connect-part0', 'pyspark-pandas-connect-part1', 'pyspark-pandas-connect-part2',
     'pyspark-pandas-connect-part3', 'pyspark-pandas-slow', 'pyspark-resource', 'pyspark-sql',
     'pyspark-streaming', 'pyspark-testing', 'repl', 'root', 'sparkr', 'sql', 'sql-kafka-0-10',
     'streaming', 'streaming-kafka-0-10', 'streaming-kinesis-asl']
    """
    modules_to_test = set()
    for module in changed_modules:
        modules_to_test = modules_to_test.union(
            determine_modules_to_test(module.dependent_modules, deduplicated)
        )
    modules_to_test = modules_to_test.union(set(changed_modules))

    if not deduplicated:
        return modules_to_test

    # If we need to run all of the tests, then we should short-circuit and return 'root'
    if modules.root in modules_to_test:
        return [modules.root]
    return toposort_flatten(
        {m: set(m.dependencies).intersection(modules_to_test) for m in modules_to_test}, sort=True
    )


def determine_tags_to_exclude(changed_modules):
    tags = []
    for m in modules.all_modules:
        if m not in changed_modules:
            tags += m.test_tags
    return tags


def _test():
    import doctest

    failure_count = doctest.testmod()[0]
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
