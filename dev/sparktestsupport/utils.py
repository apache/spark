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
import re
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
    and `README.md`, `AGENTS.md`, `CONTRIBUTING.md` are always ignored.

    >>> sorted(x.name for x in determine_modules_for_files(["python/pyspark/a.py", "sql/core/foo"]))
    ['pyspark-core', 'pyspark-errors', 'sql']
    >>> [x.name for x in determine_modules_for_files(["file_not_matched_by_any_subproject"])]
    ['root']
    >>> [x.name for x in determine_modules_for_files(["sql/README.md"])]
    []
    >>> [x.name for x in determine_modules_for_files(["AGENTS.md"])]
    []
    >>> [x.name for x in determine_modules_for_files(["CONTRIBUTING.md"])]
    []
    """
    changed_modules = set()
    for filename in filenames:
        if filename.endswith(("README.md", "AGENTS.md", "CONTRIBUTING.md")):
            continue
        if filename in (
            "scalastyle-config.xml",
            "dev/checkstyle.xml",
            "dev/checkstyle-suppressions.xml",
        ):
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


def check_upgraded_pom_dependencies(
    patch_sha, target_branch=None, target_ref=None, buffer_days=7, verbose=True
):
    """
    Check whether the pom.xml dependency upgrade has been released at least `buffer_days` days ago.

    Raise ValueError if the dependency is released within the last `buffer_days` days.
    """

    def get_release_timestamp(group_id, artifact_id, version):
        import urllib.request
        from email.utils import parsedate_to_datetime

        host = os.environ.get(
            "MAVEN_MIRROR_URL", "https://maven-central.storage-download.googleapis.com/maven2"
        )
        url = f"{host}/{group_id.replace('.', '/')}/{artifact_id}/{version}/{artifact_id}-{version}.pom"
        req = urllib.request.Request(url, method="HEAD")
        try:
            with urllib.request.urlopen(req) as response:
                return parsedate_to_datetime(response.headers.get("Last-Modified")).timestamp()
        except Exception:
            return None

    if target_branch is None and target_ref is None:
        raise AttributeError("must specify either target_branch or target_ref")
    elif target_branch is not None and target_ref is not None:
        raise AttributeError("must specify either target_branch or target_ref, not both")
    if target_branch is not None:
        diff_target = target_branch
        run_cmd(["git", "fetch", "origin", str(target_branch + ":" + target_branch)])
    else:
        diff_target = target_ref
    # The correct grammar is git diff <old> <new>, but identify_changed_files_from_git_commits
    # uses it differently. It doesn't matter for that function because it only needs the file
    # name, but we need to know which change is "new" to locate the new version.
    raw_output = subprocess.check_output(
        ["git", "diff", diff_target, patch_sha, ":(top)pom.xml"], universal_newlines=True
    )

    changed_versions = []

    # "+    <oro.version>2.0.9</oro.version>" -> "oro.version", "2.0.9"
    new_version_regex = r"^\+\s*<(?P<dependency>.*?\.version)>(?P<version>.*?)</.*?>"
    for line in raw_output.split("\n"):
        if match := re.match(new_version_regex, line):
            changed_versions.append((match.group("dependency"), match.group("version")))

    if changed_versions:
        # Okay now we parse the pom.xml to find the real dependency name
        import datetime
        import xml.etree.ElementTree as ET

        if verbose:
            print("Changed version in pom.xml detected:")
            for dep, ver in changed_versions:
                print(f"  {dep}: {ver}")

        root_dir = os.path.join(os.path.dirname(__file__), "..", "..")
        pom_path = os.path.join(root_dir, "pom.xml")
        tree = ET.parse(pom_path)
        root = tree.getroot()
        namespace = re.match(r"\{(.*?)\}project", root.tag).group(1)
        ns = {"m": namespace}
        for dependency in root.findall(".//m:dependency", ns):
            group_id = dependency.find("m:groupId", ns).text
            artifact_id = dependency.find("m:artifactId", ns).text
            version = dependency.find("m:version", ns)
            if version is not None:
                version = version.text

            for dep, ver in changed_versions:
                template = "${" + dep + "}"
                if version is not None and template in version:
                    version = version.replace("${" + dep + "}", ver)
                elif template in artifact_id:
                    artifact_id = artifact_id.replace("${" + dep + "}", ver)
                else:
                    # If we can't find the related upgrade version, just skip
                    continue
                release_timestamp = get_release_timestamp(group_id, artifact_id, version)
                if release_timestamp is None:
                    raise ValueError(
                        f"Could not find release date for {group_id}:{artifact_id}:{version}"
                    )

                release_date = datetime.datetime.fromtimestamp(release_timestamp).date()
                if verbose:
                    print(f"  {group_id}:{artifact_id}:{version} released on {release_date}")
                if release_date > datetime.datetime.now().date() - datetime.timedelta(
                    days=buffer_days
                ):
                    raise ValueError(
                        f"Dependency {group_id}:{artifact_id}:{version} is released within the last {buffer_days} days"
                    )


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
     'pyspark-pandas', 'pyspark-pandas-connect', 'pyspark-pandas-slow',
     'pyspark-pandas-slow-connect', 'pyspark-pipelines', 'pyspark-sql',
     'pyspark-structured-streaming', 'pyspark-structured-streaming-connect',
     'pyspark-testing', 'repl', 'sparkr', 'sql', 'sql-kafka-0-10']
    >>> sorted([x.name for x in determine_modules_to_test(
    ...     [modules.sparkr, modules.sql], deduplicated=False)])
    ... # doctest: +NORMALIZE_WHITESPACE
    ['avro', 'connect', 'docker-integration-tests', 'examples', 'hive', 'hive-thriftserver',
     'mllib', 'protobuf', 'pyspark-connect', 'pyspark-ml', 'pyspark-ml-connect', 'pyspark-mllib',
     'pyspark-pandas', 'pyspark-pandas-connect', 'pyspark-pandas-slow',
     'pyspark-pandas-slow-connect', 'pyspark-pipelines', 'pyspark-sql',
     'pyspark-structured-streaming', 'pyspark-structured-streaming-connect',
     'pyspark-testing', 'repl', 'sparkr', 'sql', 'sql-kafka-0-10']
    >>> sorted([x.name for x in determine_modules_to_test(
    ...     [modules.sql, modules.core], deduplicated=False)])
    ... # doctest: +NORMALIZE_WHITESPACE
    ['avro', 'catalyst', 'connect', 'core', 'docker-integration-tests', 'examples', 'graphx',
     'hive', 'hive-thriftserver', 'mllib', 'mllib-local', 'protobuf', 'pyspark-connect',
     'pyspark-core', 'pyspark-install', 'pyspark-ml', 'pyspark-ml-connect', 'pyspark-mllib',
     'pyspark-pandas', 'pyspark-pandas-connect', 'pyspark-pandas-slow',
     'pyspark-pandas-slow-connect', 'pyspark-pipelines', 'pyspark-resource', 'pyspark-sql',
     'pyspark-streaming', 'pyspark-structured-streaming', 'pyspark-structured-streaming-connect',
     'pyspark-testing', 'repl', 'root', 'sparkr', 'sql', 'sql-kafka-0-10', 'streaming',
     'streaming-kafka-0-10', 'streaming-kinesis-asl']
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


def determine_dangling_python_tests(changed_files):
    """
    Given a list of changed files, return the set of Python tests that are not associated with any
    module.
    """
    dangling_tests = set()
    for filename in changed_files:
        if os.path.exists(filename) and modules.root.missing_potential_python_test(filename):
            dangling_tests.add(filename)
    return dangling_tests


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
