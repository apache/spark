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

import itertools
import os
import re
import sys
from functools import total_ordering
from pathlib import Path, PurePath

all_modules = []
all_modules_for_file_detection = []

# These are `pathlib.PurePath` glob-style patterns with some customization:
#   - Bare patterns match a file name at any depth.
#   - Leading slashes anchor at the repository root.
#   - A trailing slash denotes an ignored directory subtree.
# See: https://docs.python.org/3/library/pathlib.html#pathlib.PurePath.match
#
# Rejected alternatives:
# - Regexes present a footgun in that `.` needs to be carefully escaped but
#   often isn't.
# - `.gitignore`-style patterns would be ideal but don't have support in the
#   standard library.
ignored_file_patterns = (
    ".asf.yaml",
    ".gitignore",
    "AGENTS.md",
    "CONTRIBUTING.md",
    "README.md",
    "/LICENSE-binary",
    "/NOTICE-binary",
    "/scalastyle-config.xml",
    "/SECURITY.md",
    "/dev/checkstyle-suppressions.xml",
    "/dev/checkstyle.xml",
    "/dev/create_jira_and_branch.py",
    "/dev/create_spark_jira.py",
    "/dev/create-release/",
    "/dev/lint-python",
    "/dev/lint-scala",
    "/dev/make-distribution.sh",
    "/dev/merge_spark_pr.py",
    "/dev/pr_merge_status.py",
    "/dev/reformat-python",
    "/dev/requirements.txt",
    "/dev/spark_merge_footer.py",
    "/dev/spark-test-image/lint/Dockerfile",
    "/dev/structured_logging_style.py",
    "/ui-test/package-lock.json",
    "/ui-test/package.json",
)


def is_ignored_file(filename: str) -> bool:
    """
    Return whether a repository-relative path should be ignored when selecting
    test modules.

    Bare patterns match a file name at any depth:
    >>> is_ignored_file("python/README.md")
    True

    Leading slashes anchor at the repository root:
    >>> is_ignored_file("SECURITY.md")
    True
    >>> is_ignored_file("docs/SECURITY.md")
    False

    A trailing slash ignores a directory subtree:
    >>> is_ignored_file("dev/create-release/spark-rm/Dockerfile")
    True

    Non-matches fall through:
    >>> is_ignored_file("xasfZyaml")
    False
    """
    path = PurePath("/") / filename
    for pattern in ignored_file_patterns:
        # TODO: When Python 3.13 becomes the minimum supported version, migrate
        # to `PurePath.full_match` and use `**` patterns instead of this custom
        # trailing slash behavior.
        if pattern.endswith("/"):
            ignored_directory = PurePath(pattern)
            if path == ignored_directory or ignored_directory in path.parents:
                return True
        elif path.match(pattern):
            return True
    return False


@total_ordering
class Module(object):
    """
    A module is the basic abstraction in our test runner script. Each module owns a set of files,
    may define test commands, and declares its dependencies on other modules. We use modules to
    define a dependency graph that lets us determine which tests to run based on which files have
    changed.
    """

    def __init__(
        self,
        name,
        dependencies,
        source_file_regexes,
        build_profile_flags=(),
        environ=None,
        sbt_test_goals=(),
        python_test_goals=(),
        excluded_python_implementations=(),
        test_tags=(),
        should_run_r_tests=False,
        should_run_build_tests=False,
        _is_internal=False,
    ):
        """
        Define a new module.

        :param name: A short module name, for display in logging and error messages.
        :param dependencies: A set of dependencies for this module. This should only include direct
            dependencies; transitive dependencies are resolved automatically.
        :param source_file_regexes: a set of regexes that match source files belonging to this
            module. These regexes are applied by attempting to match at the beginning of the
            filename strings.
        :param build_profile_flags: A set of profile flags that should be passed to Maven or SBT in
            order to build and test this module (e.g. '-PprofileName').
        :param environ: A dict of environment variables that should be set when files in this
            module are changed.
        :param sbt_test_goals: A set of SBT test goals for testing this module.
        :param python_test_goals: A set of Python test goals for testing this module.
        :param excluded_python_implementations: A set of Python implementations that are not
            supported by this module's Python components. The values in this set should match
            strings returned by Python's `platform.python_implementation()`.
        :param test_tags A set of tags that will be excluded when running unit tests if the module
            is not explicitly changed.
        :param should_run_r_tests: If true, changes in this module will trigger all R tests.
        :param should_run_build_tests: If true, changes in this module will trigger build tests.
        """
        self.name = name
        self.dependencies = dependencies
        self.source_file_prefixes = source_file_regexes
        self.sbt_test_goals = sbt_test_goals
        self.build_profile_flags = build_profile_flags
        self.environ = environ or {}
        self.python_test_goals = python_test_goals
        self.excluded_python_implementations = excluded_python_implementations
        self.test_tags = test_tags
        self.should_run_r_tests = should_run_r_tests
        self.should_run_build_tests = should_run_build_tests
        self.is_internal = _is_internal

        self.dependent_modules = set()
        for dep in self.dependencies:
            dep.dependent_modules.add(self)
        all_modules_for_file_detection.append(self)
        if not _is_internal:
            all_modules.append(self)

    def contains_file(self, filename):
        return any(re.match(p, filename) for p in self.source_file_prefixes)

    def missing_potential_python_test(self, filename):
        """
        Check if the given filename is missing from the module if it is a Python test file.

        Return True if it is a test file and is not included in the module.
        """
        if not _is_python_test_file(filename):
            return False
        path = Path(filename)
        module_path = ".".join(path.parts)[:-3]  # Remove the ".py" suffix
        return not any(module_path.endswith(test) for test in self.python_test_goals)

    def __repr__(self):
        return "Module<%s>" % self.name

    def __lt__(self, other):
        return self.name < other.name

    def __eq__(self, other):
        return self.name == other.name

    def __ne__(self, other):
        return not (self.name == other.name)

    def __hash__(self):
        return hash(self.name)


def _is_python_test_file(filename):
    return re.match(r"test_.*\.py$", Path(filename).name) is not None


class SourceModule(Module):
    """An internal graph node containing production source files but no runnable tests."""

    def __init__(self, name, dependencies, source_file_regexes):
        super().__init__(
            name=name,
            dependencies=dependencies,
            source_file_regexes=source_file_regexes,
            _is_internal=True,
        )


class TestModule(Module):
    """
    A public graph node containing test files and their runnable test goals.

    Dependencies may contain both source modules and other test modules. A dependency on another
    test module means changes to those tests also trigger this module.

    Spark SQL tests, for example, depend on their source module and the Catalyst tests:
    >>> spark_sql in spark_sql_test.dependencies
    True
    >>> catalyst_test in spark_sql_test.dependencies
    True
    """

    def __init__(
        self,
        name,
        dependencies,
        test_file_regexes,
        build_profile_flags=(),
        environ=None,
        sbt_test_goals=(),
        python_test_goals=(),
        excluded_python_implementations=(),
        test_tags=(),
        should_run_r_tests=False,
        should_run_build_tests=False,
    ):
        super().__init__(
            name=name,
            dependencies=dependencies,
            source_file_regexes=test_file_regexes,
            build_profile_flags=build_profile_flags,
            environ=environ,
            sbt_test_goals=sbt_test_goals,
            python_test_goals=python_test_goals,
            excluded_python_implementations=excluded_python_implementations,
            test_tags=test_tags,
            should_run_r_tests=should_run_r_tests,
            should_run_build_tests=should_run_build_tests,
        )


class PythonTestModule(TestModule):
    """A test module whose test files are listed by its Python test goals."""

    def __init__(self, python_test_goals=(), **kwargs):
        test_file_regexes = [
            re.escape(f"python/{goal.replace('.', '/')}.py") + "$"
            for goal in python_test_goals
            if goal.rsplit(".", 1)[-1].startswith("test_")
        ]
        super().__init__(
            python_test_goals=python_test_goals,
            test_file_regexes=test_file_regexes,
            **kwargs,
        )


def _source_file_regexes(*file_regexes):
    return [
        rf"(?!.*(?:^|/)src/test(?:/|$))(?!.*(?:^|/)test_.*\.py$){file_regex}"
        for file_regex in file_regexes
    ]


def _jvm_test_file_regexes(*file_regexes):
    return [rf"(?={file_regex})(?=.*(?:^|/)src/test(?:/|$))" for file_regex in file_regexes]


tags = Module(
    name="tags",
    dependencies=[],
    source_file_regexes=[
        "common/tags/",
    ],
)

utils_java = SourceModule(
    name="utils-java-source",
    dependencies=[tags],
    source_file_regexes=_source_file_regexes("common/utils-java/"),
)

utils_java_test = TestModule(
    name="utils-java",
    dependencies=[utils_java, tags],
    test_file_regexes=_jvm_test_file_regexes("common/utils-java/"),
    sbt_test_goals=[
        "common-utils-java/test",
    ],
)

utils = SourceModule(
    name="utils-source",
    dependencies=[tags, utils_java],
    source_file_regexes=_source_file_regexes("common/utils/"),
)

utils_test = TestModule(
    name="utils",
    dependencies=[utils, tags, utils_java_test],
    test_file_regexes=_jvm_test_file_regexes("common/utils/"),
    sbt_test_goals=[
        "common-utils/test",
    ],
)

kvstore = SourceModule(
    name="kvstore-source",
    dependencies=[tags],
    source_file_regexes=_source_file_regexes("common/kvstore/"),
)

kvstore_test = TestModule(
    name="kvstore",
    dependencies=[kvstore, tags],
    test_file_regexes=_jvm_test_file_regexes("common/kvstore/"),
    sbt_test_goals=[
        "kvstore/test",
    ],
)

network_common = SourceModule(
    name="network-common-source",
    dependencies=[tags, utils_java],
    source_file_regexes=_source_file_regexes("common/network-common/"),
)

network_common_test = TestModule(
    name="network-common",
    dependencies=[network_common, tags, utils_java_test],
    test_file_regexes=_jvm_test_file_regexes("common/network-common/"),
    sbt_test_goals=[
        "network-common/test",
    ],
)

network_shuffle = SourceModule(
    name="network-shuffle-source",
    dependencies=[tags],
    source_file_regexes=_source_file_regexes("common/network-shuffle/"),
)

network_shuffle_test = TestModule(
    name="network-shuffle",
    dependencies=[network_shuffle, tags],
    test_file_regexes=_jvm_test_file_regexes("common/network-shuffle/"),
    sbt_test_goals=[
        "network-shuffle/test",
    ],
)

unsafe = SourceModule(
    name="unsafe-source",
    dependencies=[tags, utils],
    source_file_regexes=_source_file_regexes("common/unsafe"),
)

unsafe_test = TestModule(
    name="unsafe",
    dependencies=[unsafe, tags, utils_test],
    test_file_regexes=_jvm_test_file_regexes("common/unsafe"),
    sbt_test_goals=[
        "unsafe/test",
    ],
)

launcher = SourceModule(
    name="launcher-source",
    dependencies=[tags],
    source_file_regexes=_source_file_regexes("launcher/"),
)

launcher_test = TestModule(
    name="launcher",
    dependencies=[launcher, tags],
    test_file_regexes=_jvm_test_file_regexes("launcher/"),
    sbt_test_goals=[
        "launcher/test",
    ],
)

sketch = SourceModule(
    name="sketch-source",
    dependencies=[tags],
    source_file_regexes=_source_file_regexes("common/sketch/"),
)

sketch_test = TestModule(
    name="sketch",
    dependencies=[sketch, tags],
    test_file_regexes=_jvm_test_file_regexes("common/sketch/"),
    sbt_test_goals=["sketch/test"],
)

variant = SourceModule(
    name="variant-source",
    dependencies=[tags],
    source_file_regexes=_source_file_regexes("common/variant/"),
)

variant_test = TestModule(
    name="variant",
    dependencies=[variant, tags],
    test_file_regexes=_jvm_test_file_regexes("common/variant/"),
    sbt_test_goals=["variant/test"],
)

udf_worker = SourceModule(
    name="udf-worker-source",
    dependencies=[tags],
    source_file_regexes=_source_file_regexes("udf/worker/"),
)

udf_worker_test = TestModule(
    name="udf-worker",
    dependencies=[udf_worker, tags],
    test_file_regexes=_jvm_test_file_regexes("udf/worker/"),
    sbt_test_goals=[
        "udf-worker-core/test",
    ],
)

core = SourceModule(
    name="core-source",
    dependencies=[kvstore, network_common, network_shuffle, unsafe, launcher, utils],
    source_file_regexes=_source_file_regexes("core/"),
)

core_test = TestModule(
    name="core",
    dependencies=[
        core,
        kvstore_test,
        network_common_test,
        network_shuffle_test,
        unsafe_test,
        launcher_test,
        utils_test,
    ],
    test_file_regexes=_jvm_test_file_regexes("core/"),
    sbt_test_goals=[
        "core/test",
    ],
)

api = SourceModule(
    name="api-source",
    dependencies=[utils, unsafe],
    source_file_regexes=_source_file_regexes("sql/api/"),
)

api_test = TestModule(
    name="api",
    dependencies=[api, utils_test, unsafe_test],
    test_file_regexes=_jvm_test_file_regexes("sql/api/"),
    sbt_test_goals=[
        "sql-api/test",
    ],
)

catalyst = SourceModule(
    name="catalyst-source",
    dependencies=[tags, sketch, variant, core, api],
    source_file_regexes=_source_file_regexes("sql/catalyst/"),
)

catalyst_test = TestModule(
    name="catalyst",
    dependencies=[catalyst, tags, sketch_test, variant_test, core_test, api_test],
    test_file_regexes=_jvm_test_file_regexes("sql/catalyst/"),
    sbt_test_goals=[
        "catalyst/test",
    ],
    environ=(
        None if "GITHUB_ACTIONS" not in os.environ else {"ENABLE_DOCKER_INTEGRATION_TESTS": "1"}
    ),
)

spark_sql = SourceModule(
    name="sql-source",
    dependencies=[catalyst],
    source_file_regexes=_source_file_regexes(
        "sql/core/",
        "python/pyspark/sql/worker/",  # analyze_udtf is invoked and tested in JVM
    ),
)

spark_sql_test = TestModule(
    name="sql",
    dependencies=[spark_sql, catalyst_test],
    test_file_regexes=_jvm_test_file_regexes("sql/core/"),
    sbt_test_goals=[
        "sql/test",
    ],
    environ=(
        None if "GITHUB_ACTIONS" not in os.environ else {"ENABLE_DOCKER_INTEGRATION_TESTS": "1"}
    ),
)

hive = SourceModule(
    name="hive-source",
    dependencies=[spark_sql],
    source_file_regexes=_source_file_regexes(
        "sql/hive/",
        "bin/spark-sql",
    ),
)

hive_test = TestModule(
    name="hive",
    dependencies=[hive, spark_sql_test],
    test_file_regexes=_jvm_test_file_regexes("sql/hive/"),
    build_profile_flags=[
        "-Phive",
    ],
    sbt_test_goals=[
        "hive/test",
    ],
    test_tags=["org.apache.spark.tags.ExtendedHiveTest"],
)

repl = SourceModule(
    name="repl-source",
    dependencies=[hive],
    source_file_regexes=_source_file_regexes("repl/"),
)

repl_test = TestModule(
    name="repl",
    dependencies=[repl, hive_test],
    test_file_regexes=_jvm_test_file_regexes("repl/"),
    sbt_test_goals=[
        "repl/test",
    ],
)

hive_thriftserver = SourceModule(
    name="hive-thriftserver-source",
    dependencies=[hive],
    source_file_regexes=_source_file_regexes(
        "sql/hive-thriftserver",
        "sbin/start-thriftserver.sh",
    ),
)

hive_thriftserver_test = TestModule(
    name="hive-thriftserver",
    dependencies=[hive_thriftserver, hive_test],
    test_file_regexes=_jvm_test_file_regexes("sql/hive-thriftserver"),
    build_profile_flags=[
        "-Phive-thriftserver",
    ],
    sbt_test_goals=[
        "hive-thriftserver/test",
    ],
)

avro = SourceModule(
    name="avro-source",
    dependencies=[spark_sql],
    source_file_regexes=_source_file_regexes("connector/avro"),
)

avro_test = TestModule(
    name="avro",
    dependencies=[avro, spark_sql_test],
    test_file_regexes=_jvm_test_file_regexes("connector/avro"),
    sbt_test_goals=[
        "avro/test",
    ],
)

sql_kafka = SourceModule(
    name="sql-kafka-0-10-source",
    dependencies=[spark_sql],
    source_file_regexes=_source_file_regexes("connector/kafka-0-10-sql"),
)

sql_kafka_test = TestModule(
    name="sql-kafka-0-10",
    dependencies=[sql_kafka, spark_sql_test],
    test_file_regexes=_jvm_test_file_regexes("connector/kafka-0-10-sql"),
    sbt_test_goals=[
        "sql-kafka-0-10/test",
    ],
)

profiler = Module(
    name="profiler",
    dependencies=[],
    build_profile_flags=["-Pjvm-profiler"],
    source_file_regexes=[
        "connector/profiler",
    ],
)

protobuf = SourceModule(
    name="protobuf-source",
    dependencies=[spark_sql],
    source_file_regexes=_source_file_regexes("connector/protobuf"),
)

protobuf_test = TestModule(
    name="protobuf",
    dependencies=[protobuf, spark_sql_test],
    test_file_regexes=_jvm_test_file_regexes("connector/protobuf"),
    sbt_test_goals=[
        "protobuf/test",
    ],
)

graphx = SourceModule(
    name="graphx-source",
    dependencies=[tags, core],
    source_file_regexes=_source_file_regexes("graphx/"),
)

graphx_test = TestModule(
    name="graphx",
    dependencies=[graphx, tags, core_test],
    test_file_regexes=_jvm_test_file_regexes("graphx/"),
    sbt_test_goals=["graphx/test"],
)

streaming = SourceModule(
    name="streaming-source",
    dependencies=[tags, core],
    source_file_regexes=_source_file_regexes("streaming"),
)

streaming_test = TestModule(
    name="streaming",
    dependencies=[streaming, tags, core_test],
    test_file_regexes=_jvm_test_file_regexes("streaming"),
    sbt_test_goals=[
        "streaming/test",
    ],
)


# Don't set the dependencies because changes in other modules should not trigger Kinesis tests.
# Kinesis tests depends on external Amazon kinesis service. We should run these tests only when
# files in streaming_kinesis_asl are changed, so that if Kinesis experiences an outage, we don't
# fail other PRs.
streaming_kinesis_asl = SourceModule(
    name="streaming-kinesis-asl-source",
    dependencies=[tags, core],
    source_file_regexes=_source_file_regexes(
        "connector/kinesis-asl/",
        "connector/kinesis-asl-assembly/",
    ),
)

streaming_kinesis_asl_test = TestModule(
    name="streaming-kinesis-asl",
    dependencies=[streaming_kinesis_asl, tags, core_test],
    test_file_regexes=_jvm_test_file_regexes(
        "connector/kinesis-asl/",
        "connector/kinesis-asl-assembly/",
    ),
    build_profile_flags=[
        "-Pkinesis-asl",
    ],
    environ={"ENABLE_KINESIS_TESTS": "0"},
    sbt_test_goals=[
        "streaming-kinesis-asl/test",
    ],
)


credential_aws = SourceModule(
    name="credential-aws-source",
    dependencies=[tags, core],
    source_file_regexes=_source_file_regexes(
        "connector/credential-aws/",
        "connector/credential-aws-integration-tests/",
    ),
)

credential_aws_test = TestModule(
    name="credential-aws",
    dependencies=[credential_aws, tags, core_test],
    test_file_regexes=_jvm_test_file_regexes(
        "connector/credential-aws/",
        "connector/credential-aws-integration-tests/",
    ),
    build_profile_flags=[
        "-Pcredential-aws",
    ],
    sbt_test_goals=[
        "credential-aws/test",
    ],
)


streaming_kafka_0_10 = SourceModule(
    name="streaming-kafka-0-10-source",
    dependencies=[streaming, core],
    source_file_regexes=_source_file_regexes(
        # The ending "/" is necessary otherwise it will include "sql-kafka" codes
        "connector/kafka-0-10/",
        "connector/kafka-0-10-assembly",
        "connector/kafka-0-10-token-provider",
    ),
)

streaming_kafka_0_10_test = TestModule(
    name="streaming-kafka-0-10",
    dependencies=[streaming_kafka_0_10, streaming_test, core_test],
    test_file_regexes=_jvm_test_file_regexes(
        "connector/kafka-0-10/",
        "connector/kafka-0-10-assembly",
        "connector/kafka-0-10-token-provider",
    ),
    sbt_test_goals=["streaming-kafka-0-10/test", "token-provider-kafka-0-10/test"],
)


mllib_local = SourceModule(
    name="mllib-local-source",
    dependencies=[tags, core],
    source_file_regexes=_source_file_regexes("mllib-local"),
)

mllib_local_test = TestModule(
    name="mllib-local",
    dependencies=[mllib_local, tags, core_test],
    test_file_regexes=_jvm_test_file_regexes("mllib-local"),
    sbt_test_goals=[
        "mllib-local/test",
    ],
)


mllib = SourceModule(
    name="mllib-source",
    dependencies=[mllib_local, streaming, spark_sql],
    source_file_regexes=_source_file_regexes(
        "data/mllib/",
        "mllib/",
    ),
)

mllib_test = TestModule(
    name="mllib",
    dependencies=[mllib, mllib_local_test, streaming_test, spark_sql_test],
    test_file_regexes=_jvm_test_file_regexes("mllib/"),
    sbt_test_goals=[
        "mllib/test",
    ],
)

pipelines = SourceModule(
    name="pipelines-source",
    dependencies=[spark_sql],
    source_file_regexes=_source_file_regexes("sql/pipelines"),
)

pipelines_test = TestModule(
    name="pipelines",
    dependencies=[pipelines, spark_sql_test],
    test_file_regexes=_jvm_test_file_regexes("sql/pipelines"),
    sbt_test_goals=[
        "pipelines/test",
    ],
)

connect = SourceModule(
    name="connect-source",
    dependencies=[hive, avro, protobuf, mllib],
    source_file_regexes=_source_file_regexes("sql/connect"),
)

connect_test = TestModule(
    name="connect",
    dependencies=[connect, hive_test, avro_test, protobuf_test, mllib_test],
    test_file_regexes=_jvm_test_file_regexes("sql/connect"),
    sbt_test_goals=[
        "connect/test",
        "connect-client-jvm/test",
        "connect-client-jdbc/test",
    ],
)

examples = SourceModule(
    name="examples-source",
    dependencies=[graphx, mllib, streaming, hive],
    source_file_regexes=_source_file_regexes("examples/"),
)

examples_test = TestModule(
    name="examples",
    dependencies=[examples, graphx_test, mllib_test, streaming_test, hive_test],
    test_file_regexes=_jvm_test_file_regexes("examples/"),
    sbt_test_goals=[
        "examples/test",
    ],
)

pyspark_core = SourceModule(
    name="pyspark-core-source",
    dependencies=[core],
    source_file_regexes=_source_file_regexes(
        "python/(?!pyspark/(ml|mllib|sql|streaming|pandas|resource|testing))"
    ),
)

pyspark_core_test = PythonTestModule(
    name="pyspark-core",
    dependencies=[pyspark_core],
    python_test_goals=[
        # doctests
        "pyspark.conf",
        "pyspark.core.rdd",
        "pyspark.core.context",
        "pyspark.core.broadcast",
        "pyspark.accumulators",
        "pyspark.core.files",
        "pyspark.serializers",
        "pyspark.profiler",
        "pyspark.shuffle",
        "pyspark.taskcontext",
        "pyspark.util",
        # unittests
        "pyspark.tests.test_appsubmit",
        "pyspark.tests.test_broadcast",
        "pyspark.tests.test_conf",
        "pyspark.tests.test_context",
        "pyspark.tests.test_daemon",
        "pyspark.tests.test_join",
        "pyspark.tests.test_memory_profiler",
        "pyspark.tests.test_pin_thread",
        "pyspark.tests.test_profiler",
        "pyspark.tests.test_rdd",
        "pyspark.tests.test_rddbarrier",
        "pyspark.tests.test_rddsampler",
        "pyspark.tests.test_readwrite",
        "pyspark.tests.test_serializers",
        "pyspark.tests.test_shuffle",
        "pyspark.tests.test_spark_message_receiver",
        "pyspark.tests.test_statcounter",
        "pyspark.tests.test_taskcontext",
        "pyspark.tests.test_util",
        "pyspark.tests.test_worker",
        "pyspark.tests.test_stage_sched",
        "pyspark.tests.test_zero_copy_byte_stream",
        # unittests for upstream projects
        "pyspark.tests.upstream.numpy.test_numpy_ufunc_type_coercion",
        "pyspark.tests.upstream.pandas.test_pandas_api_types",
        "pyspark.tests.upstream.pandas.test_pandas_series_astype",
        "pyspark.tests.upstream.pyarrow.test_pyarrow_array_cast",
        "pyspark.tests.upstream.pyarrow.test_pyarrow_array_from_pandas_default",
        "pyspark.tests.upstream.pyarrow.test_pyarrow_array_from_pandas_non_default",
        "pyspark.tests.upstream.pyarrow.test_pyarrow_array_type_inference",
        "pyspark.tests.upstream.pyarrow.test_pyarrow_arrow_to_pandas_default",
        "pyspark.tests.upstream.pyarrow.test_pyarrow_arrow_to_pandas_non_default",
        "pyspark.tests.upstream.pyarrow.test_pyarrow_dataframe_from_pandas",
        "pyspark.tests.upstream.pyarrow.test_pyarrow_ignore_timezone",
        "pyspark.tests.upstream.pyarrow.test_pyarrow_scalar_type_coercion",
        "pyspark.tests.upstream.pyarrow.test_pyarrow_scalar_type_inference",
        "pyspark.tests.upstream.pyarrow.test_pyarrow_table_cast",
        "pyspark.tests.upstream.pyarrow.test_pyarrow_table_to_pandas",
        "pyspark.tests.upstream.pyarrow.test_pyarrow_type_coercion",
    ],
)

pyspark_sql = SourceModule(
    name="pyspark-sql-source",
    dependencies=[pyspark_core, hive, avro, protobuf],
    source_file_regexes=_source_file_regexes("python/pyspark/sql"),
)

pyspark_sql_test = PythonTestModule(
    name="pyspark-sql",
    dependencies=[pyspark_sql],
    python_test_goals=[
        # doctests
        "pyspark.sql.types",
        "pyspark.sql.context",
        "pyspark.sql.session",
        "pyspark.sql.conf",
        "pyspark.sql.catalog",
        "pyspark.sql.classic.column",
        "pyspark.sql.classic.dataframe",
        "pyspark.sql.classic.window",
        "pyspark.sql.datasource",
        "pyspark.sql.group",
        "pyspark.sql.functions.builtin",
        "pyspark.sql.functions.partitioning",
        "pyspark.sql.merge",
        "pyspark.sql.readwriter",
        "pyspark.sql.udf",
        "pyspark.sql.udtf",
        "pyspark.sql.avro.functions",
        "pyspark.sql.protobuf.functions",
        "pyspark.sql.pandas.conversion",
        "pyspark.sql.pandas.functions",
        "pyspark.sql.pandas.map_ops",
        "pyspark.sql.pandas.group_ops",
        "pyspark.sql.pandas.types",
        "pyspark.sql.pandas.serializers",
        "pyspark.sql.pandas.typehints",
        "pyspark.sql.pandas.utils",
        "pyspark.sql.observation",
        "pyspark.sql.tvf",
        # unittests
        "pyspark.eval_handlers.tests.test_arrow_eval_type_handlers",
        "pyspark.eval_handlers.tests.test_base_eval_type_handlers",
        "pyspark.sql.tests.test_artifact",
        "pyspark.sql.tests.test_catalog",
        "pyspark.sql.tests.test_column",
        "pyspark.sql.tests.test_conf",
        "pyspark.sql.tests.test_context",
        "pyspark.sql.tests.test_sql_context",
        "pyspark.sql.tests.test_dataframe",
        "pyspark.sql.tests.test_pipelined_shuffle",
        "pyspark.sql.tests.test_collection",
        "pyspark.sql.tests.test_dataframe_creation",
        "pyspark.sql.tests.test_conversion",
        "pyspark.sql.tests.test_dataframe_query_context",
        "pyspark.sql.tests.test_listener",
        "pyspark.sql.tests.test_observation",
        "pyspark.sql.tests.test_repartition",
        "pyspark.sql.tests.test_stat",
        "pyspark.sql.tests.test_datasources",
        "pyspark.sql.tests.test_errors",
        "pyspark.sql.tests.test_functions",
        "pyspark.sql.tests.test_group",
        "pyspark.sql.tests.test_sql",
        "pyspark.sql.tests.test_job_cancellation",
        "pyspark.sql.tests.arrow.test_arrow",
        "pyspark.sql.tests.arrow.test_arrow_map",
        "pyspark.sql.tests.arrow.test_arrow_cogrouped_map",
        "pyspark.sql.tests.arrow.test_arrow_cogrouped_map_misc",
        "pyspark.sql.tests.arrow.test_arrow_grouped_map",
        "pyspark.sql.tests.arrow.test_arrow_python_aggregator",
        "pyspark.sql.tests.arrow.test_arrow_python_udf",
        "pyspark.sql.tests.arrow.test_arrow_python_udf_cached",
        "pyspark.sql.tests.arrow.test_arrow_udf",
        "pyspark.sql.tests.arrow.test_arrow_udf_grouped_agg",
        "pyspark.sql.tests.arrow.test_arrow_udf_scalar",
        "pyspark.sql.tests.arrow.test_arrow_udf_window",
        "pyspark.sql.tests.arrow.test_arrow_udf_typehints",
        "pyspark.sql.tests.arrow.test_arrow_udtf",
        "pyspark.sql.tests.pandas.test_pandas_cogrouped_map",
        "pyspark.sql.tests.pandas.test_pandas_cogrouped_map_misc",
        "pyspark.sql.tests.pandas.test_pandas_grouped_map",
        "pyspark.sql.tests.pandas.test_pandas_map",
        "pyspark.sql.tests.pandas.test_pandas_udf",
        "pyspark.sql.tests.pandas.test_pandas_udf_grouped_agg",
        "pyspark.sql.tests.pandas.test_pandas_udf_scalar",
        "pyspark.sql.tests.pandas.test_pipelined_udf",
        "pyspark.sql.tests.pandas.test_pandas_udf_typehints",
        "pyspark.sql.tests.pandas.test_pandas_udf_typehints_with_future_annotations",
        "pyspark.sql.tests.pandas.test_pandas_udf_window",
        "pyspark.sql.tests.pandas.test_pandas_sqlmetrics",
        "pyspark.sql.tests.pandas.test_converter",
        "pyspark.sql.tests.test_python_datasource",
        "pyspark.sql.tests.test_readwriter",
        "pyspark.sql.tests.test_serde",
        "pyspark.sql.tests.test_session",
        "pyspark.sql.tests.test_nearest_by_join",
        "pyspark.sql.tests.test_zip",
        "pyspark.sql.tests.test_subquery",
        "pyspark.sql.tests.test_types",
        "pyspark.sql.tests.test_geographytype",
        "pyspark.sql.tests.test_geometrytype",
        "pyspark.sql.tests.test_python_worker_env",
        "pyspark.sql.tests.test_udf",
        "pyspark.sql.tests.test_udf_combinations",
        "pyspark.sql.tests.test_udf_in_higher_order_function",
        "pyspark.sql.tests.test_udf_profiler",
        "pyspark.sql.tests.test_udf_transpile_hypothesis",
        "pyspark.sql.tests.test_udf_transpile_parity",
        "pyspark.sql.tests.test_udf_transpile_unit",
        "pyspark.sql.tests.test_unified_udf",
        "pyspark.sql.tests.test_udtf",
        "pyspark.sql.tests.test_tvf",
        "pyspark.sql.tests.test_utils",
        "pyspark.sql.tests.test_resources",
        "pyspark.sql.tests.plot.test_frame_plot",
        "pyspark.sql.tests.plot.test_frame_plot_plotly",
        "pyspark.sql.tests.test_connect_compatibility",
        "pyspark.sql.tests.coercion.test_pandas_udf_input_type",
        "pyspark.sql.tests.coercion.test_python_udf_input_type",
        "pyspark.sql.tests.coercion.test_pandas_udf_return_type",
        "pyspark.sql.tests.coercion.test_python_udf_return_type",
        "pyspark.sql.tests.df_golden.test_df_golden",
        "pyspark.sql.tests.df_golden.test_df_golden_framework",
    ],
)

pyspark_testing = SourceModule(
    name="pyspark-testing-source",
    dependencies=[pyspark_core, pyspark_sql],
    source_file_regexes=_source_file_regexes("python/pyspark/testing"),
)

pyspark_testing_test = PythonTestModule(
    name="pyspark-testing",
    dependencies=[pyspark_testing],
    python_test_goals=[
        # doctests
        "pyspark.testing.utils",
        "pyspark.testing.pandasutils",
        # unittests
        "pyspark.testing.tests.test_changed_files",
        "pyspark.testing.tests.test_fail",
        "pyspark.testing.tests.test_fail_in_set_up_class",
        "pyspark.testing.tests.test_no_tests",
        "pyspark.testing.tests.test_pass_all",
        "pyspark.testing.tests.test_skip_all",
        "pyspark.testing.tests.test_skip_class",
        "pyspark.testing.tests.test_skip_set_up_class",
    ],
)

pyspark_resource = SourceModule(
    name="pyspark-resource-source",
    dependencies=[pyspark_core],
    source_file_regexes=_source_file_regexes("python/pyspark/resource"),
)

pyspark_resource_test = PythonTestModule(
    name="pyspark-resource",
    dependencies=[pyspark_resource],
    python_test_goals=[
        # doctests
        "pyspark.resource.profile",
        # unittests
        "pyspark.resource.tests.test_resources",
        "pyspark.resource.tests.test_connect_resources",
    ],
)


pyspark_streaming = SourceModule(
    name="pyspark-streaming-source",
    dependencies=[pyspark_core, streaming, streaming_kinesis_asl],
    source_file_regexes=_source_file_regexes("python/pyspark/streaming"),
)

pyspark_streaming_test = PythonTestModule(
    name="pyspark-streaming",
    dependencies=[pyspark_streaming],
    python_test_goals=[
        # doctests
        "pyspark.streaming.util",
        # unittests
        "pyspark.streaming.tests.test_context",
        "pyspark.streaming.tests.test_dstream",
        "pyspark.streaming.tests.test_kinesis",
        "pyspark.streaming.tests.test_listener",
    ],
)


pyspark_structured_streaming = SourceModule(
    name="pyspark-structured-streaming-source",
    dependencies=[pyspark_core, pyspark_streaming, pyspark_sql, sql_kafka],
    source_file_regexes=_source_file_regexes(
        "python/pyspark/sql/streaming",
        "python/pyspark/sql/pandas",
        "python/pyspark/sql/worker",
    ),
)

pyspark_structured_streaming_test = PythonTestModule(
    name="pyspark-structured-streaming",
    dependencies=[pyspark_structured_streaming],
    python_test_goals=[
        # doctests
        "pyspark.sql.streaming.query",
        "pyspark.sql.streaming.readwriter",
        "pyspark.sql.streaming.listener",
        # unittests
        "pyspark.sql.tests.test_python_streaming_datasource",
        "pyspark.sql.tests.streaming.test_streaming",
        "pyspark.sql.tests.streaming.test_streaming_foreach",
        "pyspark.sql.tests.streaming.test_streaming_foreach_batch",
        "pyspark.sql.tests.streaming.test_streaming_kafka_rtm",
        "pyspark.sql.tests.streaming.test_streaming_listener",
        "pyspark.sql.tests.streaming.test_streaming_offline_state_repartition",
        "pyspark.sql.tests.pandas.test_pandas_grouped_map_with_state",
        "pyspark.sql.tests.pandas.streaming.test_pandas_transform_with_state",
        "pyspark.sql.tests.pandas.streaming.test_pandas_transform_with_state_checkpoint_v2",
        "pyspark.sql.tests.pandas.streaming.test_pandas_transform_with_state_state_variable",
        "pyspark.sql.tests.pandas.streaming."
        "test_pandas_transform_with_state_state_variable_checkpoint_v2",
        "pyspark.sql.tests.pandas.streaming.test_transform_with_state",
        "pyspark.sql.tests.pandas.streaming.test_transform_with_state_checkpoint_v2",
        "pyspark.sql.tests.pandas.streaming.test_transform_with_state_state_variable",
        "pyspark.sql.tests.pandas.streaming.test_transform_with_state_state_variable_checkpoint_v2",
        "pyspark.sql.tests.pandas.streaming.test_tws_tester",
    ],
)

pyspark_mllib = SourceModule(
    name="pyspark-mllib-source",
    dependencies=[pyspark_core, pyspark_streaming, pyspark_sql, mllib],
    source_file_regexes=_source_file_regexes("python/pyspark/mllib"),
)

pyspark_mllib_test = PythonTestModule(
    name="pyspark-mllib",
    dependencies=[pyspark_mllib],
    python_test_goals=[
        # doctests
        "pyspark.mllib.classification",
        "pyspark.mllib.clustering",
        "pyspark.mllib.evaluation",
        "pyspark.mllib.feature",
        "pyspark.mllib.fpm",
        "pyspark.mllib.linalg.__init__",
        "pyspark.mllib.linalg.distributed",
        "pyspark.mllib.random",
        "pyspark.mllib.recommendation",
        "pyspark.mllib.regression",
        "pyspark.mllib.stat._statistics",
        "pyspark.mllib.stat.KernelDensity",
        "pyspark.mllib.tree",
        "pyspark.mllib.util",
        # unittests
        "pyspark.mllib.tests.test_algorithms",
        "pyspark.mllib.tests.test_feature",
        "pyspark.mllib.tests.test_linalg",
        "pyspark.mllib.tests.test_stat",
        "pyspark.mllib.tests.test_streaming_algorithms",
        "pyspark.mllib.tests.test_util",
    ],
)


pyspark_ml = SourceModule(
    name="pyspark-ml-source",
    dependencies=[pyspark_core, pyspark_mllib],
    source_file_regexes=_source_file_regexes("python/pyspark/ml/"),
)

pyspark_ml_test = PythonTestModule(
    name="pyspark-ml",
    dependencies=[pyspark_ml],
    python_test_goals=[
        # doctests
        "pyspark.ml.classification",
        "pyspark.ml.clustering",
        "pyspark.ml.evaluation",
        "pyspark.ml.feature",
        "pyspark.ml.fpm",
        "pyspark.ml.functions",
        "pyspark.ml.image",
        "pyspark.ml.linalg.__init__",
        "pyspark.ml.recommendation",
        "pyspark.ml.regression",
        "pyspark.ml.stat",
        "pyspark.ml.tuning",
        # unittests
        "pyspark.ml.tests.test_algorithms",
        "pyspark.ml.tests.test_als",
        "pyspark.ml.tests.test_fpm",
        "pyspark.ml.tests.test_base",
        "pyspark.ml.tests.test_evaluation",
        "pyspark.ml.tests.test_feature",
        "pyspark.ml.tests.test_functions",
        "pyspark.ml.tests.test_image",
        "pyspark.ml.tests.test_linalg",
        "pyspark.ml.tests.test_model_cache",
        "pyspark.ml.tests.test_param",
        "pyspark.ml.tests.test_persistence",
        "pyspark.ml.tests.test_pipeline",
        "pyspark.ml.tests.test_tuning",
        "pyspark.ml.tests.test_ovr",
        "pyspark.ml.tests.test_stat",
        "pyspark.ml.tests.test_training_summary",
        "pyspark.ml.tests.tuning.test_tuning",
        "pyspark.ml.tests.tuning.test_cv_io_basic",
        "pyspark.ml.tests.tuning.test_cv_io_nested",
        "pyspark.ml.tests.tuning.test_cv_io_pipeline",
        "pyspark.ml.tests.tuning.test_tvs_io_basic",
        "pyspark.ml.tests.tuning.test_tvs_io_nested",
        "pyspark.ml.tests.tuning.test_tvs_io_pipeline",
        "pyspark.ml.tests.test_dl_util",
        "pyspark.ml.tests.test_util",
        "pyspark.ml.tests.test_wrapper",
        "pyspark.ml.torch.tests.test_distributor",
        "pyspark.ml.torch.tests.test_log_communication",
        "pyspark.ml.torch.tests.test_data_loader",
        "pyspark.ml.deepspeed.tests.test_deepspeed_distributor",
        "pyspark.ml.tests.connect.test_legacy_mode_summarizer",
        "pyspark.ml.tests.connect.test_legacy_mode_evaluation",
        "pyspark.ml.tests.connect.test_legacy_mode_feature",
        "pyspark.ml.tests.connect.test_legacy_mode_classification",
        "pyspark.ml.tests.connect.test_legacy_mode_pipeline",
        "pyspark.ml.tests.connect.test_legacy_mode_tuning",
        "pyspark.ml.tests.test_classification",
        "pyspark.ml.tests.test_regression",
        "pyspark.ml.tests.test_clustering",
    ],
)

pyspark_install = SourceModule(
    name="pyspark-install-source",
    dependencies=[],
    source_file_regexes=_source_file_regexes(
        # Python package tests will be triggered with this module
        # Any changes in python/ should trigger this module
        # This module won't be executed for post-commit CIs so it's cheap
        "python/",
        "python/pyspark/install.py",
        "python/pyspark/tests/test_install_spark.py",
    ),
)

pyspark_install_test = PythonTestModule(
    name="pyspark-install",
    dependencies=[pyspark_install],
    python_test_goals=[
        "pyspark.tests.test_import_spark",
        "pyspark.tests.test_install_spark",
    ],
)

pyspark_pandas = SourceModule(
    name="pyspark-pandas-source",
    dependencies=[pyspark_core, pyspark_sql],
    source_file_regexes=_source_file_regexes("python/pyspark/pandas/"),
)

pyspark_pandas_test = PythonTestModule(
    name="pyspark-pandas",
    dependencies=[pyspark_pandas],
    python_test_goals=[
        # doctests
        "pyspark.pandas.accessors",
        "pyspark.pandas.base",
        "pyspark.pandas.categorical",
        "pyspark.pandas.config",
        "pyspark.pandas.datetimes",
        "pyspark.pandas.exceptions",
        "pyspark.pandas.extensions",
        "pyspark.pandas.groupby",
        "pyspark.pandas.indexing",
        "pyspark.pandas.internal",
        "pyspark.pandas.mlflow",
        "pyspark.pandas.namespace",
        "pyspark.pandas.numpy_compat",
        "pyspark.pandas.sql_processor",
        "pyspark.pandas.sql_formatter",
        "pyspark.pandas.strings",
        "pyspark.pandas.supported_api_gen",
        "pyspark.pandas.utils",
        "pyspark.pandas.window",
        "pyspark.pandas.indexes.base",
        "pyspark.pandas.indexes.category",
        "pyspark.pandas.indexes.datetimes",
        "pyspark.pandas.indexes.timedelta",
        "pyspark.pandas.indexes.multi",
        "pyspark.pandas.spark.accessors",
        "pyspark.pandas.spark.utils",
        "pyspark.pandas.typedef.typehints",
        # unittests
        "pyspark.pandas.tests.test_arrow_interface",
        "pyspark.pandas.tests.test_categorical",
        "pyspark.pandas.tests.test_config",
        "pyspark.pandas.tests.test_extension",
        "pyspark.pandas.tests.test_frame_spark",
        "pyspark.pandas.tests.test_generic_functions",
        "pyspark.pandas.tests.test_indexops_spark",
        "pyspark.pandas.tests.test_internal",
        "pyspark.pandas.tests.test_namespace",
        "pyspark.pandas.tests.test_numpy_compat",
        "pyspark.pandas.tests.test_repr",
        "pyspark.pandas.tests.test_spark_functions",
        "pyspark.pandas.tests.test_scalars",
        "pyspark.pandas.tests.test_sql",
        "pyspark.pandas.tests.test_typedef",
        "pyspark.pandas.tests.test_utils",
        "pyspark.pandas.tests.computation.test_any_all",
        "pyspark.pandas.tests.computation.test_apply_func",
        "pyspark.pandas.tests.computation.test_binary_ops",
        "pyspark.pandas.tests.computation.test_combine",
        "pyspark.pandas.tests.computation.test_compute",
        "pyspark.pandas.tests.computation.test_corr",
        "pyspark.pandas.tests.computation.test_corrwith",
        "pyspark.pandas.tests.computation.test_cov",
        "pyspark.pandas.tests.computation.test_cumulative",
        "pyspark.pandas.tests.computation.test_describe",
        "pyspark.pandas.tests.computation.test_eval",
        "pyspark.pandas.tests.computation.test_idxmax_idxmin",
        "pyspark.pandas.tests.computation.test_melt",
        "pyspark.pandas.tests.computation.test_missing_data",
        "pyspark.pandas.tests.computation.test_pivot",
        "pyspark.pandas.tests.computation.test_pivot_table",
        "pyspark.pandas.tests.computation.test_pivot_table_adv",
        "pyspark.pandas.tests.computation.test_pivot_table_multi_idx",
        "pyspark.pandas.tests.computation.test_pivot_table_multi_idx_adv",
        "pyspark.pandas.tests.computation.test_stats",
        "pyspark.pandas.tests.data_type_ops.test_as_type",
        "pyspark.pandas.tests.data_type_ops.test_base",
        "pyspark.pandas.tests.data_type_ops.test_binary_ops",
        "pyspark.pandas.tests.data_type_ops.test_boolean_ops",
        "pyspark.pandas.tests.data_type_ops.test_categorical_ops",
        "pyspark.pandas.tests.data_type_ops.test_complex_ops",
        "pyspark.pandas.tests.data_type_ops.test_date_ops",
        "pyspark.pandas.tests.data_type_ops.test_datetime_ops",
        "pyspark.pandas.tests.data_type_ops.test_null_ops",
        "pyspark.pandas.tests.data_type_ops.test_num_ops",
        "pyspark.pandas.tests.data_type_ops.test_num_ops_integral_ext",
        "pyspark.pandas.tests.data_type_ops.test_num_ops_integral_ext_astype_cmp",
        "pyspark.pandas.tests.data_type_ops.test_num_ops_fractional_ext",
        "pyspark.pandas.tests.data_type_ops.test_num_ops_fractional_ext_astype_cmp",
        "pyspark.pandas.tests.data_type_ops.test_num_arithmetic",
        "pyspark.pandas.tests.data_type_ops.test_num_mod",
        "pyspark.pandas.tests.data_type_ops.test_num_mul_div",
        "pyspark.pandas.tests.data_type_ops.test_num_pow",
        "pyspark.pandas.tests.data_type_ops.test_num_reverse",
        "pyspark.pandas.tests.data_type_ops.test_string_ops",
        "pyspark.pandas.tests.data_type_ops.test_udt_ops",
        "pyspark.pandas.tests.data_type_ops.test_timedelta_ops",
        "pyspark.pandas.tests.plot.test_frame_plot",
        "pyspark.pandas.tests.plot.test_frame_plot_matplotlib",
        "pyspark.pandas.tests.plot.test_frame_plot_plotly",
        "pyspark.pandas.tests.plot.test_series_plot",
        "pyspark.pandas.tests.plot.test_series_plot_matplotlib",
        "pyspark.pandas.tests.plot.test_series_plot_plotly",
        "pyspark.pandas.tests.frame.test_interpolate",
        "pyspark.pandas.tests.frame.test_interpolate_error",
        "pyspark.pandas.tests.frame.test_attrs",
        "pyspark.pandas.tests.frame.test_axis",
        "pyspark.pandas.tests.frame.test_constructor",
        "pyspark.pandas.tests.frame.test_conversion",
        "pyspark.pandas.tests.frame.test_reindexing",
        "pyspark.pandas.tests.frame.test_reshaping",
        "pyspark.pandas.tests.frame.test_spark",
        "pyspark.pandas.tests.frame.test_take",
        "pyspark.pandas.tests.frame.test_take_adv",
        "pyspark.pandas.tests.frame.test_time_series",
        "pyspark.pandas.tests.frame.test_truncate",
        "pyspark.pandas.tests.series.test_interpolate",
        "pyspark.pandas.tests.series.test_datetime",
        "pyspark.pandas.tests.series.test_string_ops_adv",
        "pyspark.pandas.tests.series.test_string_ops_basic",
        "pyspark.pandas.tests.series.test_all_any",
        "pyspark.pandas.tests.series.test_arg_ops",
        "pyspark.pandas.tests.series.test_as_of",
        "pyspark.pandas.tests.series.test_as_type",
        "pyspark.pandas.tests.series.test_compute",
        "pyspark.pandas.tests.series.test_conversion",
        "pyspark.pandas.tests.series.test_cumulative",
        "pyspark.pandas.tests.series.test_index",
        "pyspark.pandas.tests.series.test_missing_data",
        "pyspark.pandas.tests.series.test_series",
        "pyspark.pandas.tests.series.test_sort",
        "pyspark.pandas.tests.series.test_stat",
        "pyspark.pandas.tests.resample.test_on",
        "pyspark.pandas.tests.resample.test_error",
        "pyspark.pandas.tests.resample.test_frame",
        "pyspark.pandas.tests.resample.test_missing",
        "pyspark.pandas.tests.resample.test_series",
        "pyspark.pandas.tests.resample.test_timezone",
        "pyspark.pandas.tests.reshape.test_get_dummies",
        "pyspark.pandas.tests.reshape.test_get_dummies_kwargs",
        "pyspark.pandas.tests.reshape.test_get_dummies_multiindex",
        "pyspark.pandas.tests.reshape.test_get_dummies_object",
        "pyspark.pandas.tests.reshape.test_get_dummies_prefix",
        "pyspark.pandas.tests.reshape.test_merge_asof",
        "pyspark.pandas.tests.window.test_expanding",
        "pyspark.pandas.tests.window.test_expanding_adv",
        "pyspark.pandas.tests.window.test_expanding_error",
        "pyspark.pandas.tests.window.test_groupby_expanding",
        "pyspark.pandas.tests.window.test_groupby_expanding_adv",
        "pyspark.pandas.tests.window.test_ewm_error",
        "pyspark.pandas.tests.window.test_ewm_mean",
        "pyspark.pandas.tests.window.test_groupby_ewm_mean",
        "pyspark.pandas.tests.window.test_missing",
        "pyspark.pandas.tests.window.test_rolling",
        "pyspark.pandas.tests.window.test_rolling_adv",
        "pyspark.pandas.tests.window.test_rolling_count",
        "pyspark.pandas.tests.window.test_rolling_error",
        "pyspark.pandas.tests.window.test_groupby_rolling",
        "pyspark.pandas.tests.window.test_groupby_rolling_adv",
        "pyspark.pandas.tests.window.test_groupby_rolling_count",
        "pyspark.pandas.tests.io.test_io",
        "pyspark.pandas.tests.io.test_csv",
        "pyspark.pandas.tests.io.test_feather",
        "pyspark.pandas.tests.io.test_stata",
        "pyspark.pandas.tests.io.test_dataframe_conversion",
        "pyspark.pandas.tests.io.test_dataframe_spark_io",
        "pyspark.pandas.tests.io.test_series_conversion",
        # fallback
        "pyspark.pandas.tests.frame.test_asfreq",
        "pyspark.pandas.tests.frame.test_asof",
        "pyspark.pandas.tests.frame.test_combine",
    ],
)

pyspark_pandas_slow_test = PythonTestModule(
    name="pyspark-pandas-slow",
    dependencies=[pyspark_pandas],
    python_test_goals=[
        # doctests
        "pyspark.pandas.frame",
        "pyspark.pandas.generic",
        "pyspark.pandas.series",
        # unittests
        "pyspark.pandas.tests.indexes.test_default",
        "pyspark.pandas.tests.indexes.test_category",
        "pyspark.pandas.tests.indexes.test_timedelta",
        "pyspark.pandas.tests.indexes.test_basic",
        "pyspark.pandas.tests.indexes.test_getattr",
        "pyspark.pandas.tests.indexes.test_name",
        "pyspark.pandas.tests.indexes.test_conversion",
        "pyspark.pandas.tests.indexes.test_drop",
        "pyspark.pandas.tests.indexes.test_level",
        "pyspark.pandas.tests.indexes.test_missing",
        "pyspark.pandas.tests.indexes.test_repeat",
        "pyspark.pandas.tests.indexes.test_sort",
        "pyspark.pandas.tests.indexes.test_stat",
        "pyspark.pandas.tests.indexes.test_symmetric_diff",
        "pyspark.pandas.tests.indexes.test_take",
        "pyspark.pandas.tests.indexes.test_unique",
        "pyspark.pandas.tests.indexes.test_asof",
        "pyspark.pandas.tests.indexes.test_astype",
        "pyspark.pandas.tests.indexes.test_delete",
        "pyspark.pandas.tests.indexes.test_diff",
        "pyspark.pandas.tests.indexes.test_insert",
        "pyspark.pandas.tests.indexes.test_map",
        "pyspark.pandas.tests.indexes.test_append",
        "pyspark.pandas.tests.indexes.test_intersection",
        "pyspark.pandas.tests.indexes.test_monotonic",
        "pyspark.pandas.tests.indexes.test_union",
        "pyspark.pandas.tests.indexes.test_datetime",
        "pyspark.pandas.tests.indexes.test_datetime_at",
        "pyspark.pandas.tests.indexes.test_datetime_between",
        "pyspark.pandas.tests.indexes.test_datetime_ceil",
        "pyspark.pandas.tests.indexes.test_datetime_floor",
        "pyspark.pandas.tests.indexes.test_datetime_iso",
        "pyspark.pandas.tests.indexes.test_datetime_map",
        "pyspark.pandas.tests.indexes.test_datetime_property",
        "pyspark.pandas.tests.indexes.test_datetime_round",
        "pyspark.pandas.tests.indexes.test_align",
        "pyspark.pandas.tests.indexes.test_indexing",
        "pyspark.pandas.tests.indexes.test_indexing_adv",
        "pyspark.pandas.tests.indexes.test_indexing_basic",
        "pyspark.pandas.tests.indexes.test_indexing_iloc",
        "pyspark.pandas.tests.indexes.test_indexing_loc",
        "pyspark.pandas.tests.indexes.test_indexing_loc_2d",
        "pyspark.pandas.tests.indexes.test_indexing_loc_multi_idx",
        "pyspark.pandas.tests.indexes.test_reindex",
        "pyspark.pandas.tests.indexes.test_rename",
        "pyspark.pandas.tests.indexes.test_reset_index",
        "pyspark.pandas.tests.groupby.test_aggregate",
        "pyspark.pandas.tests.groupby.test_apply_func",
        "pyspark.pandas.tests.groupby.test_corr",
        "pyspark.pandas.tests.groupby.test_cov",
        "pyspark.pandas.tests.groupby.test_cumulative",
        "pyspark.pandas.tests.groupby.test_describe",
        "pyspark.pandas.tests.groupby.test_groupby",
        "pyspark.pandas.tests.groupby.test_grouping",
        "pyspark.pandas.tests.groupby.test_head_tail",
        "pyspark.pandas.tests.groupby.test_index",
        "pyspark.pandas.tests.groupby.test_missing",
        "pyspark.pandas.tests.groupby.test_missing_data",
        "pyspark.pandas.tests.groupby.test_nlargest_nsmallest",
        "pyspark.pandas.tests.groupby.test_raises",
        "pyspark.pandas.tests.groupby.test_rank",
        "pyspark.pandas.tests.groupby.test_size",
        "pyspark.pandas.tests.groupby.test_split_apply",
        "pyspark.pandas.tests.groupby.test_split_apply_count",
        "pyspark.pandas.tests.groupby.test_split_apply_mean",
        "pyspark.pandas.tests.groupby.test_split_apply_first",
        "pyspark.pandas.tests.groupby.test_split_apply_last",
        "pyspark.pandas.tests.groupby.test_split_apply_min_max",
        "pyspark.pandas.tests.groupby.test_split_apply_skew",
        "pyspark.pandas.tests.groupby.test_split_apply_std",
        "pyspark.pandas.tests.groupby.test_split_apply_var",
        "pyspark.pandas.tests.groupby.test_stat",
        "pyspark.pandas.tests.groupby.test_stat_adv",
        "pyspark.pandas.tests.groupby.test_stat_ddof",
        "pyspark.pandas.tests.groupby.test_stat_func",
        "pyspark.pandas.tests.groupby.test_stat_median",
        "pyspark.pandas.tests.groupby.test_stat_prod",
        "pyspark.pandas.tests.groupby.test_value_counts",
        "pyspark.pandas.tests.diff_frames_ops.test_align",
        "pyspark.pandas.tests.diff_frames_ops.test_arithmetic",
        "pyspark.pandas.tests.diff_frames_ops.test_arithmetic_ext",
        "pyspark.pandas.tests.diff_frames_ops.test_arithmetic_ext_float",
        "pyspark.pandas.tests.diff_frames_ops.test_arithmetic_chain",
        "pyspark.pandas.tests.diff_frames_ops.test_arithmetic_chain_ext",
        "pyspark.pandas.tests.diff_frames_ops.test_arithmetic_chain_ext_float",
        "pyspark.pandas.tests.diff_frames_ops.test_assign_frame",
        "pyspark.pandas.tests.diff_frames_ops.test_assign_series",
        "pyspark.pandas.tests.diff_frames_ops.test_basic",
        "pyspark.pandas.tests.diff_frames_ops.test_bitwise",
        "pyspark.pandas.tests.diff_frames_ops.test_combine_first",
        "pyspark.pandas.tests.diff_frames_ops.test_compare_series",
        "pyspark.pandas.tests.diff_frames_ops.test_concat_inner",
        "pyspark.pandas.tests.diff_frames_ops.test_concat_outer",
        "pyspark.pandas.tests.diff_frames_ops.test_basic_slow",
        "pyspark.pandas.tests.diff_frames_ops.test_cov",
        "pyspark.pandas.tests.diff_frames_ops.test_corrwith",
        "pyspark.pandas.tests.diff_frames_ops.test_dot_frame",
        "pyspark.pandas.tests.diff_frames_ops.test_dot_series",
        "pyspark.pandas.tests.diff_frames_ops.test_error",
        "pyspark.pandas.tests.diff_frames_ops.test_index",
        "pyspark.pandas.tests.diff_frames_ops.test_series",
        "pyspark.pandas.tests.diff_frames_ops.test_setitem_frame",
        "pyspark.pandas.tests.diff_frames_ops.test_setitem_series",
        "pyspark.pandas.tests.diff_frames_ops.test_groupby",
        "pyspark.pandas.tests.diff_frames_ops.test_groupby_aggregate",
        "pyspark.pandas.tests.diff_frames_ops.test_groupby_apply",
        "pyspark.pandas.tests.diff_frames_ops.test_groupby_cumulative",
        "pyspark.pandas.tests.diff_frames_ops.test_groupby_diff",
        "pyspark.pandas.tests.diff_frames_ops.test_groupby_diff_len",
        "pyspark.pandas.tests.diff_frames_ops.test_groupby_fillna",
        "pyspark.pandas.tests.diff_frames_ops.test_groupby_filter",
        "pyspark.pandas.tests.diff_frames_ops.test_groupby_shift",
        "pyspark.pandas.tests.diff_frames_ops.test_groupby_split_apply_combine",
        "pyspark.pandas.tests.diff_frames_ops.test_groupby_transform",
        "pyspark.pandas.tests.diff_frames_ops.test_groupby_expanding",
        "pyspark.pandas.tests.diff_frames_ops.test_groupby_expanding_adv",
        "pyspark.pandas.tests.diff_frames_ops.test_groupby_expanding_count",
        "pyspark.pandas.tests.diff_frames_ops.test_groupby_rolling",
        "pyspark.pandas.tests.diff_frames_ops.test_groupby_rolling_adv",
        "pyspark.pandas.tests.diff_frames_ops.test_groupby_rolling_count",
    ],
)

pyspark_connect = SourceModule(
    name="pyspark-connect-source",
    dependencies=[pyspark_sql, connect],
    source_file_regexes=_source_file_regexes("python/pyspark/sql/connect"),
)

pyspark_connect_test = PythonTestModule(
    name="pyspark-connect",
    dependencies=[pyspark_connect, pyspark_sql_test],
    python_test_goals=[
        # sql doctests
        "pyspark.sql.connect.catalog",
        "pyspark.sql.connect.conf",
        "pyspark.sql.connect.group",
        "pyspark.sql.connect.session",
        "pyspark.sql.connect.window",
        "pyspark.sql.connect.column",
        "pyspark.sql.connect.merge",
        "pyspark.sql.connect.readwriter",
        "pyspark.sql.connect.dataframe",
        "pyspark.sql.connect.functions.builtin",
        "pyspark.sql.connect.functions.partitioning",
        "pyspark.sql.connect.observation",
        "pyspark.sql.connect.avro.functions",
        "pyspark.sql.connect.protobuf.functions",
        "pyspark.sql.connect.streaming.readwriter",
        "pyspark.sql.connect.streaming.query",
        "pyspark.sql.connect.tvf",
        # sql unittests
        "pyspark.sql.tests.connect.test_connect_plan",
        "pyspark.sql.tests.connect.test_connect_basic",
        "pyspark.sql.tests.connect.test_connect_dataframe_property",
        "pyspark.sql.tests.connect.test_connect_channel",
        "pyspark.sql.tests.connect.test_connect_clone_session",
        "pyspark.sql.tests.connect.test_parity_python_worker_env",
        "pyspark.sql.tests.connect.test_connect_error",
        "pyspark.sql.tests.connect.test_connect_function",
        "pyspark.sql.tests.connect.test_connect_collection",
        "pyspark.sql.tests.connect.test_connect_column",
        "pyspark.sql.tests.connect.test_connect_dataframe_creation",
        "pyspark.sql.tests.connect.test_connect_readwriter",
        "pyspark.sql.tests.connect.test_connect_retry",
        "pyspark.sql.tests.connect.test_connect_session",
        "pyspark.sql.tests.connect.test_connect_local_server",
        "pyspark.sql.tests.connect.test_connect_local_server_pool",
        "pyspark.sql.tests.connect.test_connect_stat",
        "pyspark.sql.tests.connect.test_parity_geographytype",
        "pyspark.sql.tests.connect.test_parity_geometrytype",
        "pyspark.sql.tests.connect.test_parity_datasources",
        "pyspark.sql.tests.connect.test_parity_errors",
        "pyspark.sql.tests.connect.test_connect_context",
        "pyspark.sql.tests.connect.test_parity_sql_context",
        "pyspark.sql.tests.connect.test_parity_catalog",
        "pyspark.sql.tests.connect.test_parity_conf",
        "pyspark.sql.tests.connect.test_parity_serde",
        "pyspark.sql.tests.connect.test_parity_functions",
        "pyspark.sql.tests.connect.test_parity_group",
        "pyspark.sql.tests.connect.test_parity_sql",
        "pyspark.sql.tests.connect.test_parity_job_cancellation",
        "pyspark.sql.tests.connect.test_parity_dataframe",
        "pyspark.sql.tests.connect.test_parity_dataframe_query_context",
        "pyspark.sql.tests.connect.test_parity_collection",
        "pyspark.sql.tests.connect.test_parity_dataframe_creation",
        "pyspark.sql.tests.connect.test_parity_observation",
        "pyspark.sql.tests.connect.test_parity_repartition",
        "pyspark.sql.tests.connect.test_parity_stat",
        "pyspark.sql.tests.connect.test_parity_nearest_by_join",
        "pyspark.sql.tests.connect.test_parity_zip",
        "pyspark.sql.tests.connect.test_parity_subquery",
        "pyspark.sql.tests.connect.test_parity_types",
        "pyspark.sql.tests.connect.test_parity_column",
        "pyspark.sql.tests.connect.test_parity_readwriter",
        "pyspark.sql.tests.connect.test_parity_udf",
        "pyspark.sql.tests.connect.test_parity_udf_in_higher_order_function",
        "pyspark.sql.tests.connect.test_parity_udf_combinations",
        "pyspark.sql.tests.connect.test_parity_udf_profiler",
        "pyspark.sql.tests.connect.test_parity_unified_udf",
        "pyspark.sql.tests.connect.test_parity_memory_profiler",
        "pyspark.sql.tests.connect.test_parity_udtf",
        "pyspark.sql.tests.connect.test_parity_tvf",
        "pyspark.sql.tests.connect.test_parity_python_datasource",
        "pyspark.sql.tests.connect.test_parity_frame_plot",
        "pyspark.sql.tests.connect.test_parity_frame_plot_plotly",
        "pyspark.sql.tests.connect.test_parity_utils",
        "pyspark.sql.tests.connect.client.test_artifact",
        "pyspark.sql.tests.connect.client.test_artifact_localcluster",
        "pyspark.sql.tests.connect.client.test_client",
        "pyspark.sql.tests.connect.client.test_client_call_stack_trace",
        "pyspark.sql.tests.connect.client.test_client_retries",
        "pyspark.sql.tests.connect.client.test_reattach",
        "pyspark.sql.tests.connect.test_parity_resources",
        "pyspark.sql.tests.connect.shell.test_progress",
        "pyspark.sql.tests.connect.test_df_debug",
        "pyspark.sql.tests.connect.arrow.test_parity_arrow",
        "pyspark.sql.tests.connect.arrow.test_parity_arrow_map",
        "pyspark.sql.tests.connect.arrow.test_parity_arrow_grouped_map",
        "pyspark.sql.tests.connect.arrow.test_parity_arrow_cogrouped_map",
        "pyspark.sql.tests.connect.arrow.test_parity_arrow_cogrouped_map_misc",
        "pyspark.sql.tests.connect.arrow.test_parity_arrow_python_aggregator",
        "pyspark.sql.tests.connect.arrow.test_parity_arrow_python_udf",
        "pyspark.sql.tests.connect.arrow.test_parity_arrow_udf",
        "pyspark.sql.tests.connect.arrow.test_parity_arrow_udf_scalar",
        "pyspark.sql.tests.connect.arrow.test_parity_arrow_udf_grouped_agg",
        "pyspark.sql.tests.connect.arrow.test_parity_arrow_udf_window",
        "pyspark.sql.tests.connect.arrow.test_parity_arrow_udtf",
        "pyspark.sql.tests.connect.pandas.test_parity_pandas_map",
        "pyspark.sql.tests.connect.pandas.test_parity_pandas_grouped_map",
        "pyspark.sql.tests.connect.pandas.test_parity_pandas_cogrouped_map",
        "pyspark.sql.tests.connect.pandas.test_parity_pandas_cogrouped_map_misc",
        "pyspark.sql.tests.connect.pandas.test_parity_pandas_udf",
        "pyspark.sql.tests.connect.pandas.test_parity_pandas_udf_scalar",
        "pyspark.sql.tests.connect.pandas.test_parity_pandas_udf_grouped_agg",
        "pyspark.sql.tests.connect.pandas.test_parity_pandas_udf_window",
    ],
)

pyspark_structured_streaming_connect_test = PythonTestModule(
    name="pyspark-structured-streaming-connect",
    dependencies=[
        pyspark_connect,
        pyspark_structured_streaming,
        pyspark_structured_streaming_test,
    ],
    python_test_goals=[
        # unittests
        "pyspark.sql.tests.connect.test_parity_python_streaming_datasource",
        "pyspark.sql.tests.connect.streaming.test_listener",
        "pyspark.sql.tests.connect.streaming.test_parity_streaming",
        "pyspark.sql.tests.connect.streaming.test_parity_listener",
        "pyspark.sql.tests.connect.streaming.test_parity_foreach",
        "pyspark.sql.tests.connect.streaming.test_parity_foreach_batch",
        "pyspark.sql.tests.connect.pandas.streaming.test_parity_pandas_grouped_map_with_state",
        "pyspark.sql.tests.connect.pandas.streaming.test_parity_pandas_transform_with_state",
        "pyspark.sql.tests.connect.pandas.streaming."
        "test_parity_pandas_transform_with_state_state_variable",
        "pyspark.sql.tests.connect.pandas.streaming.test_parity_transform_with_state",
        "pyspark.sql.tests.connect.pandas.streaming."
        "test_parity_transform_with_state_state_variable",
    ],
)


pyspark_ml_connect = SourceModule(
    name="pyspark-ml-connect-source",
    dependencies=[pyspark_connect, pyspark_ml],
    source_file_regexes=_source_file_regexes("python/pyspark/ml/connect"),
)

pyspark_ml_connect_test = PythonTestModule(
    name="pyspark-ml-connect",
    dependencies=[pyspark_ml_connect, pyspark_ml_test],
    python_test_goals=[
        # ml doctests
        "pyspark.ml.connect.functions",
        # ml unittests
        "pyspark.ml.tests.connect.test_connect_cache",
        "pyspark.ml.tests.connect.test_connect_function",
        "pyspark.ml.tests.connect.test_parity_torch_distributor",
        "pyspark.ml.tests.connect.test_parity_torch_data_loader",
        "pyspark.ml.tests.connect.test_connect_summarizer",
        "pyspark.ml.tests.connect.test_connect_evaluation",
        "pyspark.ml.tests.connect.test_connect_feature",
        "pyspark.ml.tests.connect.test_connect_classification",
        "pyspark.ml.tests.connect.test_connect_pipeline",
        "pyspark.ml.tests.connect.test_connect_tuning",
        "pyspark.ml.tests.connect.test_connect_model_offloading",
        "pyspark.ml.tests.connect.test_parity_als",
        "pyspark.ml.tests.connect.test_parity_fpm",
        "pyspark.ml.tests.connect.test_parity_classification",
        "pyspark.ml.tests.connect.test_parity_regression",
        "pyspark.ml.tests.connect.test_parity_clustering",
        "pyspark.ml.tests.connect.test_parity_evaluation",
        "pyspark.ml.tests.connect.test_parity_feature",
        "pyspark.ml.tests.connect.test_parity_functions",
        "pyspark.ml.tests.connect.test_parity_pipeline",
        "pyspark.ml.tests.connect.test_parity_tuning",
        "pyspark.ml.tests.connect.test_parity_ovr",
        "pyspark.ml.tests.connect.test_parity_stat",
    ],
)


pyspark_pandas_connect_test = PythonTestModule(
    name="pyspark-pandas-connect",
    dependencies=[pyspark_connect, pyspark_pandas, pyspark_pandas_test, pyspark_pandas_slow_test],
    python_test_goals=[
        # pandas-on-Spark unittests
        "pyspark.pandas.tests.connect.test_parity_arrow_interface",
        "pyspark.pandas.tests.connect.test_parity_categorical",
        "pyspark.pandas.tests.connect.test_parity_config",
        "pyspark.pandas.tests.connect.test_parity_extension",
        "pyspark.pandas.tests.connect.test_parity_frame_spark",
        "pyspark.pandas.tests.connect.test_parity_generic_functions",
        "pyspark.pandas.tests.connect.test_parity_indexops_spark",
        "pyspark.pandas.tests.connect.test_parity_internal",
        "pyspark.pandas.tests.connect.test_parity_namespace",
        "pyspark.pandas.tests.connect.test_parity_numpy_compat",
        "pyspark.pandas.tests.connect.test_parity_repr",
        "pyspark.pandas.tests.connect.test_parity_scalars",
        "pyspark.pandas.tests.connect.test_parity_spark_functions",
        "pyspark.pandas.tests.connect.test_parity_sql",
        "pyspark.pandas.tests.connect.test_parity_typedef",
        "pyspark.pandas.tests.connect.test_parity_utils",
        "pyspark.pandas.tests.connect.computation.test_parity_any_all",
        "pyspark.pandas.tests.connect.computation.test_parity_apply_func",
        "pyspark.pandas.tests.connect.computation.test_parity_binary_ops",
        "pyspark.pandas.tests.connect.computation.test_parity_combine",
        "pyspark.pandas.tests.connect.computation.test_parity_compute",
        "pyspark.pandas.tests.connect.computation.test_parity_cov",
        "pyspark.pandas.tests.connect.computation.test_parity_corr",
        "pyspark.pandas.tests.connect.computation.test_parity_corrwith",
        "pyspark.pandas.tests.connect.computation.test_parity_cumulative",
        "pyspark.pandas.tests.connect.computation.test_parity_describe",
        "pyspark.pandas.tests.connect.computation.test_parity_eval",
        "pyspark.pandas.tests.connect.computation.test_parity_idxmax_idxmin",
        "pyspark.pandas.tests.connect.computation.test_parity_melt",
        "pyspark.pandas.tests.connect.computation.test_parity_missing_data",
        "pyspark.pandas.tests.connect.computation.test_parity_pivot",
        "pyspark.pandas.tests.connect.computation.test_parity_pivot_table",
        "pyspark.pandas.tests.connect.computation.test_parity_pivot_table_adv",
        "pyspark.pandas.tests.connect.computation.test_parity_pivot_table_multi_idx",
        "pyspark.pandas.tests.connect.computation.test_parity_pivot_table_multi_idx_adv",
        "pyspark.pandas.tests.connect.computation.test_parity_stats",
        "pyspark.pandas.tests.connect.data_type_ops.test_parity_as_type",
        "pyspark.pandas.tests.connect.data_type_ops.test_parity_base",
        "pyspark.pandas.tests.connect.data_type_ops.test_parity_binary_ops",
        "pyspark.pandas.tests.connect.data_type_ops.test_parity_boolean_ops",
        "pyspark.pandas.tests.connect.data_type_ops.test_parity_categorical_ops",
        "pyspark.pandas.tests.connect.data_type_ops.test_parity_complex_ops",
        "pyspark.pandas.tests.connect.data_type_ops.test_parity_date_ops",
        "pyspark.pandas.tests.connect.data_type_ops.test_parity_datetime_ops",
        "pyspark.pandas.tests.connect.data_type_ops.test_parity_null_ops",
        "pyspark.pandas.tests.connect.data_type_ops.test_parity_num_ops",
        "pyspark.pandas.tests.connect.data_type_ops.test_parity_num_ops_integral_ext",
        "pyspark.pandas.tests.connect.data_type_ops.test_parity_num_ops_integral_ext_astype_cmp",
        "pyspark.pandas.tests.connect.data_type_ops.test_parity_num_ops_fractional_ext",
        "pyspark.pandas.tests.connect.data_type_ops.test_parity_num_ops_fractional_ext_astype_cmp",
        "pyspark.pandas.tests.connect.data_type_ops.test_parity_num_reverse",
        "pyspark.pandas.tests.connect.data_type_ops.test_parity_string_ops",
        "pyspark.pandas.tests.connect.data_type_ops.test_parity_udt_ops",
        "pyspark.pandas.tests.connect.data_type_ops.test_parity_timedelta_ops",
        "pyspark.pandas.tests.connect.data_type_ops.test_parity_num_arithmetic",
        "pyspark.pandas.tests.connect.data_type_ops.test_parity_num_mod",
        "pyspark.pandas.tests.connect.data_type_ops.test_parity_num_mul_div",
        "pyspark.pandas.tests.connect.data_type_ops.test_parity_num_pow",
        "pyspark.pandas.tests.connect.plot.test_parity_frame_plot",
        "pyspark.pandas.tests.connect.plot.test_parity_frame_plot_matplotlib",
        "pyspark.pandas.tests.connect.plot.test_parity_frame_plot_plotly",
        "pyspark.pandas.tests.connect.plot.test_parity_series_plot",
        "pyspark.pandas.tests.connect.plot.test_parity_series_plot_matplotlib",
        "pyspark.pandas.tests.connect.plot.test_parity_series_plot_plotly",
        "pyspark.pandas.tests.connect.frame.test_parity_attrs",
        "pyspark.pandas.tests.connect.frame.test_parity_axis",
        "pyspark.pandas.tests.connect.frame.test_parity_constructor",
        "pyspark.pandas.tests.connect.frame.test_parity_conversion",
        "pyspark.pandas.tests.connect.frame.test_parity_reindexing",
        "pyspark.pandas.tests.connect.frame.test_parity_reshaping",
        "pyspark.pandas.tests.connect.frame.test_parity_spark",
        "pyspark.pandas.tests.connect.frame.test_parity_take",
        "pyspark.pandas.tests.connect.frame.test_parity_take_adv",
        "pyspark.pandas.tests.connect.frame.test_parity_time_series",
        "pyspark.pandas.tests.connect.frame.test_parity_truncate",
        "pyspark.pandas.tests.connect.frame.test_parity_interpolate",
        "pyspark.pandas.tests.connect.frame.test_parity_interpolate_error",
        "pyspark.pandas.tests.connect.series.test_parity_datetime",
        "pyspark.pandas.tests.connect.series.test_parity_string_ops_adv",
        "pyspark.pandas.tests.connect.series.test_parity_string_ops_basic",
        "pyspark.pandas.tests.connect.series.test_parity_all_any",
        "pyspark.pandas.tests.connect.series.test_parity_arg_ops",
        "pyspark.pandas.tests.connect.series.test_parity_as_of",
        "pyspark.pandas.tests.connect.series.test_parity_as_type",
        "pyspark.pandas.tests.connect.series.test_parity_compute",
        "pyspark.pandas.tests.connect.series.test_parity_conversion",
        "pyspark.pandas.tests.connect.series.test_parity_cumulative",
        "pyspark.pandas.tests.connect.series.test_parity_index",
        "pyspark.pandas.tests.connect.series.test_parity_missing_data",
        "pyspark.pandas.tests.connect.series.test_parity_series",
        "pyspark.pandas.tests.connect.series.test_parity_sort",
        "pyspark.pandas.tests.connect.series.test_parity_stat",
        "pyspark.pandas.tests.connect.series.test_parity_interpolate",
        "pyspark.pandas.tests.connect.resample.test_parity_frame",
        "pyspark.pandas.tests.connect.resample.test_parity_series",
        "pyspark.pandas.tests.connect.resample.test_parity_error",
        "pyspark.pandas.tests.connect.resample.test_parity_missing",
        "pyspark.pandas.tests.connect.resample.test_parity_on",
        "pyspark.pandas.tests.connect.resample.test_parity_timezone",
        "pyspark.pandas.tests.connect.reshape.test_parity_get_dummies",
        "pyspark.pandas.tests.connect.reshape.test_parity_get_dummies_kwargs",
        "pyspark.pandas.tests.connect.reshape.test_parity_get_dummies_multiindex",
        "pyspark.pandas.tests.connect.reshape.test_parity_get_dummies_object",
        "pyspark.pandas.tests.connect.reshape.test_parity_get_dummies_prefix",
        "pyspark.pandas.tests.connect.reshape.test_parity_merge_asof",
        "pyspark.pandas.tests.connect.window.test_parity_ewm_error",
        "pyspark.pandas.tests.connect.window.test_parity_ewm_mean",
        "pyspark.pandas.tests.connect.window.test_parity_groupby_ewm_mean",
        "pyspark.pandas.tests.connect.window.test_parity_missing",
        "pyspark.pandas.tests.connect.window.test_parity_rolling",
        "pyspark.pandas.tests.connect.window.test_parity_rolling_adv",
        "pyspark.pandas.tests.connect.window.test_parity_rolling_count",
        "pyspark.pandas.tests.connect.window.test_parity_rolling_error",
        "pyspark.pandas.tests.connect.window.test_parity_groupby_rolling",
        "pyspark.pandas.tests.connect.window.test_parity_groupby_rolling_adv",
        "pyspark.pandas.tests.connect.window.test_parity_groupby_rolling_count",
        "pyspark.pandas.tests.connect.window.test_parity_expanding",
        "pyspark.pandas.tests.connect.window.test_parity_expanding_adv",
        "pyspark.pandas.tests.connect.window.test_parity_expanding_error",
        "pyspark.pandas.tests.connect.window.test_parity_groupby_expanding",
        "pyspark.pandas.tests.connect.window.test_parity_groupby_expanding_adv",
        "pyspark.pandas.tests.connect.io.test_parity_io",
        "pyspark.pandas.tests.connect.io.test_parity_csv",
        "pyspark.pandas.tests.connect.io.test_parity_feather",
        "pyspark.pandas.tests.connect.io.test_parity_stata",
        "pyspark.pandas.tests.connect.io.test_parity_dataframe_conversion",
        "pyspark.pandas.tests.connect.io.test_parity_dataframe_spark_io",
        "pyspark.pandas.tests.connect.io.test_parity_series_conversion",
        # fallback
        "pyspark.pandas.tests.connect.frame.test_parity_asfreq",
        "pyspark.pandas.tests.connect.frame.test_parity_asof",
        "pyspark.pandas.tests.connect.frame.test_parity_combine",
    ],
)

pyspark_pandas_slow_connect_test = PythonTestModule(
    name="pyspark-pandas-slow-connect",
    dependencies=[pyspark_connect, pyspark_pandas, pyspark_pandas_test, pyspark_pandas_slow_test],
    python_test_goals=[
        # pandas-on-Spark unittests
        "pyspark.pandas.tests.connect.indexes.test_parity_default",
        "pyspark.pandas.tests.connect.indexes.test_parity_category",
        "pyspark.pandas.tests.connect.indexes.test_parity_timedelta",
        "pyspark.pandas.tests.connect.indexes.test_parity_basic",
        "pyspark.pandas.tests.connect.indexes.test_parity_getattr",
        "pyspark.pandas.tests.connect.indexes.test_parity_name",
        "pyspark.pandas.tests.connect.indexes.test_parity_conversion",
        "pyspark.pandas.tests.connect.indexes.test_parity_drop",
        "pyspark.pandas.tests.connect.indexes.test_parity_level",
        "pyspark.pandas.tests.connect.indexes.test_parity_missing",
        "pyspark.pandas.tests.connect.indexes.test_parity_repeat",
        "pyspark.pandas.tests.connect.indexes.test_parity_sort",
        "pyspark.pandas.tests.connect.indexes.test_parity_stat",
        "pyspark.pandas.tests.connect.indexes.test_parity_symmetric_diff",
        "pyspark.pandas.tests.connect.indexes.test_parity_take",
        "pyspark.pandas.tests.connect.indexes.test_parity_unique",
        "pyspark.pandas.tests.connect.indexes.test_parity_asof",
        "pyspark.pandas.tests.connect.indexes.test_parity_astype",
        "pyspark.pandas.tests.connect.indexes.test_parity_delete",
        "pyspark.pandas.tests.connect.indexes.test_parity_diff",
        "pyspark.pandas.tests.connect.indexes.test_parity_insert",
        "pyspark.pandas.tests.connect.indexes.test_parity_map",
        "pyspark.pandas.tests.connect.indexes.test_parity_align",
        "pyspark.pandas.tests.connect.indexes.test_parity_indexing",
        "pyspark.pandas.tests.connect.indexes.test_parity_indexing_adv",
        "pyspark.pandas.tests.connect.indexes.test_parity_indexing_basic",
        "pyspark.pandas.tests.connect.indexes.test_parity_indexing_iloc",
        "pyspark.pandas.tests.connect.indexes.test_parity_indexing_loc",
        "pyspark.pandas.tests.connect.indexes.test_parity_indexing_loc_2d",
        "pyspark.pandas.tests.connect.indexes.test_parity_indexing_loc_multi_idx",
        "pyspark.pandas.tests.connect.indexes.test_parity_reindex",
        "pyspark.pandas.tests.connect.indexes.test_parity_rename",
        "pyspark.pandas.tests.connect.indexes.test_parity_reset_index",
        "pyspark.pandas.tests.connect.indexes.test_parity_datetime",
        "pyspark.pandas.tests.connect.indexes.test_parity_datetime_at",
        "pyspark.pandas.tests.connect.indexes.test_parity_datetime_between",
        "pyspark.pandas.tests.connect.indexes.test_parity_append",
        "pyspark.pandas.tests.connect.indexes.test_parity_intersection",
        "pyspark.pandas.tests.connect.indexes.test_parity_monotonic",
        "pyspark.pandas.tests.connect.indexes.test_parity_union",
        "pyspark.pandas.tests.connect.indexes.test_parity_datetime_ceil",
        "pyspark.pandas.tests.connect.indexes.test_parity_datetime_floor",
        "pyspark.pandas.tests.connect.indexes.test_parity_datetime_iso",
        "pyspark.pandas.tests.connect.indexes.test_parity_datetime_map",
        "pyspark.pandas.tests.connect.indexes.test_parity_datetime_property",
        "pyspark.pandas.tests.connect.indexes.test_parity_datetime_round",
        "pyspark.pandas.tests.connect.groupby.test_parity_stat",
        "pyspark.pandas.tests.connect.groupby.test_parity_stat_adv",
        "pyspark.pandas.tests.connect.groupby.test_parity_stat_ddof",
        "pyspark.pandas.tests.connect.groupby.test_parity_stat_func",
        "pyspark.pandas.tests.connect.groupby.test_parity_stat_median",
        "pyspark.pandas.tests.connect.groupby.test_parity_stat_prod",
        "pyspark.pandas.tests.connect.groupby.test_parity_aggregate",
        "pyspark.pandas.tests.connect.groupby.test_parity_apply_func",
        "pyspark.pandas.tests.connect.groupby.test_parity_corr",
        "pyspark.pandas.tests.connect.groupby.test_parity_cov",
        "pyspark.pandas.tests.connect.groupby.test_parity_cumulative",
        "pyspark.pandas.tests.connect.groupby.test_parity_missing_data",
        "pyspark.pandas.tests.connect.groupby.test_parity_split_apply",
        "pyspark.pandas.tests.connect.groupby.test_parity_split_apply_count",
        "pyspark.pandas.tests.connect.groupby.test_parity_split_apply_first",
        "pyspark.pandas.tests.connect.groupby.test_parity_split_apply_mean",
        "pyspark.pandas.tests.connect.groupby.test_parity_split_apply_last",
        "pyspark.pandas.tests.connect.groupby.test_parity_split_apply_min_max",
        "pyspark.pandas.tests.connect.groupby.test_parity_split_apply_skew",
        "pyspark.pandas.tests.connect.groupby.test_parity_split_apply_std",
        "pyspark.pandas.tests.connect.groupby.test_parity_split_apply_var",
        "pyspark.pandas.tests.connect.groupby.test_parity_index",
        "pyspark.pandas.tests.connect.groupby.test_parity_describe",
        "pyspark.pandas.tests.connect.groupby.test_parity_head_tail",
        "pyspark.pandas.tests.connect.groupby.test_parity_groupby",
        "pyspark.pandas.tests.connect.groupby.test_parity_grouping",
        "pyspark.pandas.tests.connect.groupby.test_parity_missing",
        "pyspark.pandas.tests.connect.groupby.test_parity_nlargest_nsmallest",
        "pyspark.pandas.tests.connect.groupby.test_parity_raises",
        "pyspark.pandas.tests.connect.groupby.test_parity_rank",
        "pyspark.pandas.tests.connect.groupby.test_parity_size",
        "pyspark.pandas.tests.connect.groupby.test_parity_value_counts",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_groupby_rolling",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_groupby_rolling_adv",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_groupby_rolling_count",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_dot_frame",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_dot_series",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_error",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_align",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_basic_slow",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_cov",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_corrwith",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_index",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_series",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_setitem_frame",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_setitem_series",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_arithmetic",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_arithmetic_ext",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_arithmetic_ext_float",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_arithmetic_chain",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_arithmetic_chain_ext",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_arithmetic_chain_ext_float",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_assign_frame",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_assign_series",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_basic",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_bitwise",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_combine_first",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_compare_series",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_concat_inner",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_concat_outer",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_groupby",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_groupby_aggregate",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_groupby_apply",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_groupby_cumulative",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_groupby_diff",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_groupby_diff_len",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_groupby_fillna",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_groupby_filter",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_groupby_split_apply_combine",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_groupby_expanding",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_groupby_expanding_adv",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_groupby_expanding_count",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_groupby_shift",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_groupby_transform",
    ],
)


pyspark_errors = SourceModule(
    name="pyspark-errors-source",
    dependencies=[pyspark_core],
    source_file_regexes=_source_file_regexes("python/pyspark/errors"),
)

pyspark_errors_test = PythonTestModule(
    name="pyspark-errors",
    dependencies=[pyspark_errors],
    python_test_goals=[
        # unittests
        "pyspark.errors.tests.test_connect_errors_conversion",
        "pyspark.errors.tests.test_errors",
        "pyspark.errors.tests.test_traceback",
        "pyspark.errors.tests.connect.test_parity_traceback",
    ],
)

pyspark_logger = SourceModule(
    name="pyspark-logger-source",
    dependencies=[],
    source_file_regexes=_source_file_regexes("python/pyspark/logger"),
)

pyspark_logger_test = PythonTestModule(
    name="pyspark-logger",
    dependencies=[pyspark_logger],
    python_test_goals=[
        # doctests
        "pyspark.logger.logger",
        # unittests
        "pyspark.logger.tests.test_logger",
        "pyspark.logger.tests.connect.test_parity_logger",
    ],
)

pyspark_pipelines = SourceModule(
    name="pyspark-pipelines-source",
    dependencies=[pyspark_core, pyspark_sql, pyspark_connect],
    source_file_regexes=_source_file_regexes("python/pyspark/pipelines"),
)

pyspark_pipelines_test = PythonTestModule(
    name="pyspark-pipelines",
    dependencies=[pyspark_pipelines],
    python_test_goals=[
        "pyspark.pipelines.tests.test_add_pipeline_analysis_context",
        "pyspark.pipelines.tests.test_auto_cdc_flow",
        "pyspark.pipelines.tests.test_block_session_mutations",
        "pyspark.pipelines.tests.test_cli",
        "pyspark.pipelines.tests.test_decorators",
        "pyspark.pipelines.tests.test_graph_element_registry",
        "pyspark.pipelines.tests.test_init_cli",
        "pyspark.pipelines.tests.test_spark_connect",
    ],
)

sparkr = Module(
    name="sparkr",
    dependencies=[hive, mllib],
    source_file_regexes=[
        "R/",
        "dev/spark-test-image/sparkr/",
    ],
    should_run_r_tests=True,
)


docs = Module(
    name="docs",
    dependencies=[],
    source_file_regexes=[
        "docs/",
        "python/docs/",
    ],
)

build = Module(
    name="build",
    dependencies=[],
    source_file_regexes=[
        ".*pom.xml",
        "dev/test-dependencies.sh",
    ],
    should_run_build_tests=True,
)

yarn = SourceModule(
    name="yarn-source",
    dependencies=[],
    source_file_regexes=_source_file_regexes(
        "resource-managers/yarn/",
        "common/network-yarn/",
    ),
)

yarn_test = TestModule(
    name="yarn",
    dependencies=[yarn],
    test_file_regexes=_jvm_test_file_regexes(
        "resource-managers/yarn/",
        "common/network-yarn/",
    ),
    build_profile_flags=["-Pyarn"],
    sbt_test_goals=[
        "yarn/test",
        "network-yarn/test",
    ],
    test_tags=["org.apache.spark.tags.ExtendedYarnTest"],
)

kubernetes = SourceModule(
    name="kubernetes-source",
    dependencies=[],
    source_file_regexes=_source_file_regexes("resource-managers/kubernetes"),
)

kubernetes_test = TestModule(
    name="kubernetes",
    dependencies=[kubernetes],
    test_file_regexes=_jvm_test_file_regexes("resource-managers/kubernetes"),
    build_profile_flags=["-Pkubernetes", "-Pvolcano"],
    sbt_test_goals=["kubernetes/test"],
)

hadoop_cloud = SourceModule(
    name="hadoop-cloud-source",
    dependencies=[],
    source_file_regexes=_source_file_regexes("hadoop-cloud"),
)

hadoop_cloud_test = TestModule(
    name="hadoop-cloud",
    dependencies=[hadoop_cloud],
    test_file_regexes=_jvm_test_file_regexes("hadoop-cloud"),
    build_profile_flags=["-Phadoop-cloud"],
    sbt_test_goals=["hadoop-cloud/test"],
)

spark_ganglia_lgpl = Module(
    name="spark-ganglia-lgpl",
    dependencies=[],
    build_profile_flags=["-Pspark-ganglia-lgpl"],
    source_file_regexes=[
        "connector/spark-ganglia-lgpl",
    ],
)

docker_integration_tests = SourceModule(
    name="docker-integration-tests-source",
    dependencies=[spark_sql],
    source_file_regexes=_source_file_regexes("connector/docker-integration-tests"),
)

docker_integration_tests_test = TestModule(
    name="docker-integration-tests",
    dependencies=[docker_integration_tests, spark_sql_test],
    build_profile_flags=["-Pdocker-integration-tests"],
    test_file_regexes=_jvm_test_file_regexes("connector/docker-integration-tests"),
    sbt_test_goals=["docker-integration-tests/test"],
    environ=(
        None if "GITHUB_ACTIONS" not in os.environ else {"ENABLE_DOCKER_INTEGRATION_TESTS": "1"}
    ),
    test_tags=["org.apache.spark.tags.DockerTest"],
)

# The root module is a dummy module which is used to run all of the tests.
# No other modules should directly depend on this module.
root = Module(
    name="root",
    dependencies=[build, core],  # Changes to build should trigger all tests.
    source_file_regexes=[],
    # In order to run all of the tests, enable every test profile:
    build_profile_flags=list(
        set(itertools.chain.from_iterable(m.build_profile_flags for m in all_modules))
    ),
    sbt_test_goals=[
        "test",
    ],
    python_test_goals=list(itertools.chain.from_iterable(m.python_test_goals for m in all_modules)),
    should_run_r_tests=True,
    should_run_build_tests=True,
)


def _test():
    import doctest

    failure_count = doctest.testmod()[0]
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
