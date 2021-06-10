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

from functools import total_ordering
import itertools
import os
import re
import glob

from sparktestsupport import SPARK_HOME

all_modules = []


def _discover_python_unittests(paths):
    if not paths:
        return set([])
    tests = set([])
    pyspark_path = os.path.join(SPARK_HOME, "python")
    for path in paths:
        # Discover the test*.py in every path
        files = glob.glob(os.path.join(pyspark_path, path, "test*.py"))
        for f in files:
            # Convert 'pyspark_path/pyspark/tests/test_abc.py' to 'pyspark.tests.test_abc'
            file2module = os.path.relpath(f, pyspark_path)[:-3].replace("/", ".")
            tests.add(file2module)
    return tests


@total_ordering
class Module(object):
    """
    A module is the basic abstraction in our test runner script. Each module consists of a set
    of source files, a set of test commands, and a set of dependencies on other modules. We use
    modules to define a dependency graph that let us determine which tests to run based on which
    files have changed.
    """

    def __init__(self, name, dependencies, source_file_regexes, build_profile_flags=(),
                 environ=None, sbt_test_goals=(), python_test_goals=(),
                 excluded_python_implementations=(), test_tags=(), should_run_r_tests=False,
                 should_run_build_tests=False, python_test_paths=(), python_excluded_tests=()):
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
        :param python_test_paths: A set of python test paths to be discovered
        :param python_excluded_tests: A set of excluded Python tests
        """
        self.name = name
        self.dependencies = dependencies
        self.source_file_prefixes = source_file_regexes
        self.sbt_test_goals = sbt_test_goals
        self.build_profile_flags = build_profile_flags
        self.environ = environ or {}
        discovered_goals = _discover_python_unittests(python_test_paths)
        # Final goals = Manual specified goals + Discoverd goals - Excluded goals
        all_goals = set(python_test_goals) | set(discovered_goals)
        self.python_test_goals = sorted(list(set(all_goals) - set(python_excluded_tests)))
        self.excluded_python_implementations = excluded_python_implementations
        self.test_tags = test_tags
        self.should_run_r_tests = should_run_r_tests
        self.should_run_build_tests = should_run_build_tests

        self.dependent_modules = set()
        for dep in dependencies:
            dep.dependent_modules.add(self)
        all_modules.append(self)

    def contains_file(self, filename):
        return any(re.match(p, filename) for p in self.source_file_prefixes)

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

tags = Module(
    name="tags",
    dependencies=[],
    source_file_regexes=[
        "common/tags/",
    ]
)

kvstore = Module(
    name="kvstore",
    dependencies=[tags],
    source_file_regexes=[
        "common/kvstore/",
    ],
    sbt_test_goals=[
        "kvstore/test",
    ],
)

network_common = Module(
    name="network-common",
    dependencies=[tags],
    source_file_regexes=[
        "common/network-common/",
    ],
    sbt_test_goals=[
        "network-common/test",
    ],
)

network_shuffle = Module(
    name="network-shuffle",
    dependencies=[tags],
    source_file_regexes=[
        "common/network-shuffle/",
    ],
    sbt_test_goals=[
        "network-shuffle/test",
    ],
)

unsafe = Module(
    name="unsafe",
    dependencies=[tags],
    source_file_regexes=[
        "common/unsafe",
    ],
    sbt_test_goals=[
        "unsafe/test",
    ],
)

launcher = Module(
    name="launcher",
    dependencies=[tags],
    source_file_regexes=[
        "launcher/",
    ],
    sbt_test_goals=[
        "launcher/test",
    ],
)

core = Module(
    name="core",
    dependencies=[kvstore, network_common, network_shuffle, unsafe, launcher],
    source_file_regexes=[
        "core/",
    ],
    sbt_test_goals=[
        "core/test",
    ],
)

catalyst = Module(
    name="catalyst",
    dependencies=[tags, core],
    source_file_regexes=[
        "sql/catalyst/",
    ],
    sbt_test_goals=[
        "catalyst/test",
    ],
)

sql = Module(
    name="sql",
    dependencies=[catalyst],
    source_file_regexes=[
        "sql/core/",
    ],
    sbt_test_goals=[
        "sql/test",
    ],
)

hive = Module(
    name="hive",
    dependencies=[sql],
    source_file_regexes=[
        "sql/hive/",
        "bin/spark-sql",
    ],
    build_profile_flags=[
        "-Phive",
    ],
    sbt_test_goals=[
        "hive/test",
    ],
    test_tags=[
        "org.apache.spark.tags.ExtendedHiveTest"
    ]
)

repl = Module(
    name="repl",
    dependencies=[hive],
    source_file_regexes=[
        "repl/",
    ],
    sbt_test_goals=[
        "repl/test",
    ],
)

hive_thriftserver = Module(
    name="hive-thriftserver",
    dependencies=[hive],
    source_file_regexes=[
        "sql/hive-thriftserver",
        "sbin/start-thriftserver.sh",
    ],
    build_profile_flags=[
        "-Phive-thriftserver",
    ],
    sbt_test_goals=[
        "hive-thriftserver/test",
    ]
)

avro = Module(
    name="avro",
    dependencies=[sql],
    source_file_regexes=[
        "external/avro",
    ],
    sbt_test_goals=[
        "avro/test",
    ]
)

sql_kafka = Module(
    name="sql-kafka-0-10",
    dependencies=[sql],
    source_file_regexes=[
        "external/kafka-0-10-sql",
    ],
    sbt_test_goals=[
        "sql-kafka-0-10/test",
    ]
)

sketch = Module(
    name="sketch",
    dependencies=[tags],
    source_file_regexes=[
        "common/sketch/",
    ],
    sbt_test_goals=[
        "sketch/test"
    ]
)

graphx = Module(
    name="graphx",
    dependencies=[tags, core],
    source_file_regexes=[
        "graphx/",
    ],
    sbt_test_goals=[
        "graphx/test"
    ]
)

streaming = Module(
    name="streaming",
    dependencies=[tags, core],
    source_file_regexes=[
        "streaming",
    ],
    sbt_test_goals=[
        "streaming/test",
    ]
)


# Don't set the dependencies because changes in other modules should not trigger Kinesis tests.
# Kinesis tests depends on external Amazon kinesis service. We should run these tests only when
# files in streaming_kinesis_asl are changed, so that if Kinesis experiences an outage, we don't
# fail other PRs.
streaming_kinesis_asl = Module(
    name="streaming-kinesis-asl",
    dependencies=[tags, core],
    source_file_regexes=[
        "external/kinesis-asl/",
        "external/kinesis-asl-assembly/",
    ],
    build_profile_flags=[
        "-Pkinesis-asl",
    ],
    environ={
        "ENABLE_KINESIS_TESTS": "1"
    },
    sbt_test_goals=[
        "streaming-kinesis-asl/test",
    ]
)


streaming_kafka_0_10 = Module(
    name="streaming-kafka-0-10",
    dependencies=[streaming, core],
    source_file_regexes=[
        # The ending "/" is necessary otherwise it will include "sql-kafka" codes
        "external/kafka-0-10/",
        "external/kafka-0-10-assembly",
        "external/kafka-0-10-token-provider",
    ],
    sbt_test_goals=[
        "streaming-kafka-0-10/test",
        "token-provider-kafka-0-10/test"
    ]
)


mllib_local = Module(
    name="mllib-local",
    dependencies=[tags, core],
    source_file_regexes=[
        "mllib-local",
    ],
    sbt_test_goals=[
        "mllib-local/test",
    ]
)


mllib = Module(
    name="mllib",
    dependencies=[mllib_local, streaming, sql],
    source_file_regexes=[
        "data/mllib/",
        "mllib/",
    ],
    sbt_test_goals=[
        "mllib/test",
    ]
)


examples = Module(
    name="examples",
    dependencies=[graphx, mllib, streaming, hive],
    source_file_regexes=[
        "examples/",
    ],
    sbt_test_goals=[
        "examples/test",
    ]
)

pyspark_core = Module(
    name="pyspark-core",
    dependencies=[core],
    source_file_regexes=[
        "python/(?!pyspark/(ml|mllib|sql|streaming))"
    ],
    python_test_goals=[
        # doctests
        "pyspark.rdd",
        "pyspark.context",
        "pyspark.conf",
        "pyspark.broadcast",
        "pyspark.accumulators",
        "pyspark.serializers",
        "pyspark.profiler",
        "pyspark.shuffle",
        "pyspark.util",
    ],
    python_test_paths=[
        "pyspark/tests"
    ],
)

pyspark_sql = Module(
    name="pyspark-sql",
    dependencies=[pyspark_core, hive, avro],
    source_file_regexes=[
        "python/pyspark/sql"
    ],
    python_test_goals=[
        # doctests
        "pyspark.sql.types",
        "pyspark.sql.context",
        "pyspark.sql.session",
        "pyspark.sql.conf",
        "pyspark.sql.catalog",
        "pyspark.sql.column",
        "pyspark.sql.dataframe",
        "pyspark.sql.group",
        "pyspark.sql.functions",
        "pyspark.sql.readwriter",
        "pyspark.sql.streaming",
        "pyspark.sql.udf",
        "pyspark.sql.window",
        "pyspark.sql.avro.functions",
        "pyspark.sql.pandas.conversion",
        "pyspark.sql.pandas.map_ops",
        "pyspark.sql.pandas.group_ops",
        "pyspark.sql.pandas.types",
        "pyspark.sql.pandas.serializers",
        "pyspark.sql.pandas.typehints",
        "pyspark.sql.pandas.utils",
    ],
    python_test_paths=[
        "pyspark/sql/tests"
    ],
)


pyspark_resource = Module(
    name="pyspark-resource",
    dependencies=[
        pyspark_core
    ],
    source_file_regexes=[
        "python/pyspark/resource"
    ],
    python_test_goals=[
        # unittests
        "pyspark.resource.tests.test_resources",
    ],
    python_test_paths=[
        "pyspark/resource/tests"
    ],
)


pyspark_streaming = Module(
    name="pyspark-streaming",
    dependencies=[
        pyspark_core,
        streaming,
        streaming_kinesis_asl
    ],
    source_file_regexes=[
        "python/pyspark/streaming"
    ],
    python_test_goals=[
        # doctests
        "pyspark.streaming.util",
    ],
    python_test_paths=[
        "pyspark/streaming/tests"
    ],
)


pyspark_mllib = Module(
    name="pyspark-mllib",
    dependencies=[pyspark_core, pyspark_streaming, pyspark_sql, mllib],
    source_file_regexes=[
        "python/pyspark/mllib"
    ],
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
    ],
    excluded_python_implementations=[
        "PyPy"  # Skip these tests under PyPy since they require numpy and it isn't available there
    ],
    python_test_paths=[
        "pyspark/mllib/tests"
    ],
)


pyspark_ml = Module(
    name="pyspark-ml",
    dependencies=[pyspark_core, pyspark_mllib],
    source_file_regexes=[
        "python/pyspark/ml/"
    ],
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
    ],
    excluded_python_implementations=[
        "PyPy"  # Skip these tests under PyPy since they require numpy and it isn't available there
    ],
    python_test_paths=[
        "pyspark/ml/tests"
    ],
)

pyspark_pandas_slow_unittests = [
    # unittests
    "pyspark.pandas.tests.indexes.test_base",
    "pyspark.pandas.tests.indexes.test_datetime",
    "pyspark.pandas.tests.test_dataframe",
    "pyspark.pandas.tests.test_groupby",
    "pyspark.pandas.tests.test_indexing",
    "pyspark.pandas.tests.test_ops_on_diff_frames",
    "pyspark.pandas.tests.test_ops_on_diff_frames_groupby",
    "pyspark.pandas.tests.test_series",
    "pyspark.pandas.tests.test_stats",
]

pyspark_pandas = Module(
    name="pyspark-pandas",
    dependencies=[pyspark_core, pyspark_sql],
    source_file_regexes=[
        "python/pyspark/pandas/"
    ],
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
        "pyspark.pandas.ml",
        "pyspark.pandas.mlflow",
        "pyspark.pandas.namespace",
        "pyspark.pandas.numpy_compat",
        "pyspark.pandas.sql_processor",
        "pyspark.pandas.strings",
        "pyspark.pandas.utils",
        "pyspark.pandas.window",
        "pyspark.pandas.indexes.base",
        "pyspark.pandas.indexes.category",
        "pyspark.pandas.indexes.datetimes",
        "pyspark.pandas.indexes.multi",
        "pyspark.pandas.indexes.numeric",
        "pyspark.pandas.spark.accessors",
        "pyspark.pandas.spark.utils",
        "pyspark.pandas.typedef.typehints",
    ],
    excluded_python_implementations=[
        "PyPy"  # Skip these tests under PyPy since they require numpy, pandas, and pyarrow and
        # they aren't available there
    ],
    python_test_paths=[
        "pyspark/pandas/tests",
        "pyspark/pandas/tests/data_type_ops",
        "pyspark/pandas/tests/indexes",
        "pyspark/pandas/tests/plot",
    ],
    python_excluded_tests=pyspark_pandas_slow_unittests,
)

pyspark_pandas_slow = Module(
    name="pyspark-pandas-slow",
    dependencies=[pyspark_core, pyspark_sql],
    source_file_regexes=[
        "python/pyspark/pandas/"
    ],
    python_test_goals=[
        # doctests
        "pyspark.pandas.frame",
        "pyspark.pandas.generic",
        "pyspark.pandas.series",
    ] + pyspark_pandas_slow_unittests,
    excluded_python_implementations=[
        "PyPy"  # Skip these tests under PyPy since they require numpy, pandas, and pyarrow and
        # they aren't available there
    ]
)

sparkr = Module(
    name="sparkr",
    dependencies=[hive, mllib],
    source_file_regexes=[
        "R/",
    ],
    should_run_r_tests=True
)


docs = Module(
    name="docs",
    dependencies=[],
    source_file_regexes=[
        "docs/",
    ]
)

build = Module(
    name="build",
    dependencies=[],
    source_file_regexes=[
        ".*pom.xml",
        "dev/test-dependencies.sh",
    ],
    should_run_build_tests=True
)

yarn = Module(
    name="yarn",
    dependencies=[],
    source_file_regexes=[
        "resource-managers/yarn/",
        "common/network-yarn/",
    ],
    build_profile_flags=["-Pyarn"],
    sbt_test_goals=[
        "yarn/test",
        "network-yarn/test",
    ],
    test_tags=[
        "org.apache.spark.tags.ExtendedYarnTest"
    ]
)

mesos = Module(
    name="mesos",
    dependencies=[],
    source_file_regexes=["resource-managers/mesos/"],
    build_profile_flags=["-Pmesos"],
    sbt_test_goals=["mesos/test"]
)

kubernetes = Module(
    name="kubernetes",
    dependencies=[],
    source_file_regexes=["resource-managers/kubernetes"],
    build_profile_flags=["-Pkubernetes"],
    sbt_test_goals=["kubernetes/test"]
)

hadoop_cloud = Module(
    name="hadoop-cloud",
    dependencies=[],
    source_file_regexes=["hadoop-cloud"],
    build_profile_flags=["-Phadoop-cloud"],
    sbt_test_goals=["hadoop-cloud/test"]
)

spark_ganglia_lgpl = Module(
    name="spark-ganglia-lgpl",
    dependencies=[],
    build_profile_flags=["-Pspark-ganglia-lgpl"],
    source_file_regexes=[
        "external/spark-ganglia-lgpl",
    ]
)

docker_integration_tests = Module(
    name="docker-integration-tests",
    dependencies=[],
    build_profile_flags=["-Pdocker-integration-tests"],
    source_file_regexes=["external/docker-integration-tests"],
    sbt_test_goals=["docker-integration-tests/test"],
    environ=None if "GITHUB_ACTIONS" not in os.environ else {
        "ENABLE_DOCKER_INTEGRATION_TESTS": "1"
    },
    test_tags=[
        "org.apache.spark.tags.DockerTest"
    ]
)

# The root module is a dummy module which is used to run all of the tests.
# No other modules should directly depend on this module.
root = Module(
    name="root",
    dependencies=[build, core],  # Changes to build should trigger all tests.
    source_file_regexes=[],
    # In order to run all of the tests, enable every test profile:
    build_profile_flags=list(set(
        itertools.chain.from_iterable(m.build_profile_flags for m in all_modules))),
    sbt_test_goals=[
        "test",
    ],
    python_test_goals=list(itertools.chain.from_iterable(m.python_test_goals for m in all_modules)),
    should_run_r_tests=True,
    should_run_build_tests=True
)
