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
import re

all_modules = []


class Module(object):
    """
    A module is the basic abstraction in our test runner script. Each module consists of a set of
    source files, a set of test commands, and a set of dependencies on other modules. We use modules
    to define a dependency graph that lets determine which tests to run based on which files have
    changed.
    """

    def __init__(self, name, dependencies, source_file_regexes, build_profile_flags=(), environ={},
                 sbt_test_goals=(), python_test_goals=(), blacklisted_python_implementations=(),
                 should_run_r_tests=False):
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
        :param blacklisted_python_implementations: A set of Python implementations that are not
            supported by this module's Python components. The values in this set should match
            strings returned by Python's `platform.python_implementation()`.
        :param should_run_r_tests: If true, changes in this module will trigger all R tests.
        """
        self.name = name
        self.dependencies = dependencies
        self.source_file_prefixes = source_file_regexes
        self.sbt_test_goals = sbt_test_goals
        self.build_profile_flags = build_profile_flags
        self.environ = environ
        self.python_test_goals = python_test_goals
        self.blacklisted_python_implementations = blacklisted_python_implementations
        self.should_run_r_tests = should_run_r_tests

        self.dependent_modules = set()
        for dep in dependencies:
            dep.dependent_modules.add(self)
        all_modules.append(self)

    def contains_file(self, filename):
        return any(re.match(p, filename) for p in self.source_file_prefixes)


sql = Module(
    name="sql",
    dependencies=[],
    source_file_regexes=[
        "sql/(?!hive-thriftserver)",
        "bin/spark-sql",
    ],
    build_profile_flags=[
        "-Phive",
    ],
    sbt_test_goals=[
        "catalyst/test",
        "sql/test",
        "hive/test",
    ]
)


hive_thriftserver = Module(
    name="hive-thriftserver",
    dependencies=[sql],
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


graphx = Module(
    name="graphx",
    dependencies=[],
    source_file_regexes=[
        "graphx/",
    ],
    sbt_test_goals=[
        "graphx/test"
    ]
)


streaming = Module(
    name="streaming",
    dependencies=[],
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
    dependencies=[],
    source_file_regexes=[
        "extras/kinesis-asl/",
        "extras/kinesis-asl-assembly/",
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


streaming_zeromq = Module(
    name="streaming-zeromq",
    dependencies=[streaming],
    source_file_regexes=[
        "external/zeromq",
    ],
    sbt_test_goals=[
        "streaming-zeromq/test",
    ]
)


streaming_twitter = Module(
    name="streaming-twitter",
    dependencies=[streaming],
    source_file_regexes=[
        "external/twitter",
    ],
    sbt_test_goals=[
        "streaming-twitter/test",
    ]
)


streaming_mqtt = Module(
    name="streaming-mqtt",
    dependencies=[streaming],
    source_file_regexes=[
        "external/mqtt",
        "external/mqtt-assembly",
    ],
    sbt_test_goals=[
        "streaming-mqtt/test",
    ]
)


streaming_kafka = Module(
    name="streaming-kafka",
    dependencies=[streaming],
    source_file_regexes=[
        "external/kafka",
        "external/kafka-assembly",
    ],
    sbt_test_goals=[
        "streaming-kafka/test",
    ]
)


streaming_flume_sink = Module(
    name="streaming-flume-sink",
    dependencies=[streaming],
    source_file_regexes=[
        "external/flume-sink",
    ],
    sbt_test_goals=[
        "streaming-flume-sink/test",
    ]
)


streaming_flume = Module(
    name="streaming-flume",
    dependencies=[streaming],
    source_file_regexes=[
        "external/flume",
    ],
    sbt_test_goals=[
        "streaming-flume/test",
    ]
)


streaming_flume_assembly = Module(
    name="streaming-flume-assembly",
    dependencies=[streaming_flume, streaming_flume_sink],
    source_file_regexes=[
        "external/flume-assembly",
    ]
)


mllib = Module(
    name="mllib",
    dependencies=[streaming, sql],
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
    dependencies=[graphx, mllib, streaming, sql],
    source_file_regexes=[
        "examples/",
    ],
    sbt_test_goals=[
        "examples/test",
    ]
)


pyspark_core = Module(
    name="pyspark-core",
    dependencies=[],
    source_file_regexes=[
        "python/(?!pyspark/(ml|mllib|sql|streaming))"
    ],
    python_test_goals=[
        "pyspark.rdd",
        "pyspark.context",
        "pyspark.conf",
        "pyspark.broadcast",
        "pyspark.accumulators",
        "pyspark.serializers",
        "pyspark.profiler",
        "pyspark.shuffle",
        "pyspark.tests",
    ]
)


pyspark_sql = Module(
    name="pyspark-sql",
    dependencies=[pyspark_core, sql],
    source_file_regexes=[
        "python/pyspark/sql"
    ],
    python_test_goals=[
        "pyspark.sql.types",
        "pyspark.sql.context",
        "pyspark.sql.column",
        "pyspark.sql.dataframe",
        "pyspark.sql.group",
        "pyspark.sql.functions",
        "pyspark.sql.readwriter",
        "pyspark.sql.window",
        "pyspark.sql.tests",
    ]
)


pyspark_streaming = Module(
    name="pyspark-streaming",
    dependencies=[
        pyspark_core,
        streaming,
        streaming_kafka,
        streaming_flume_assembly,
        streaming_mqtt,
        streaming_kinesis_asl
    ],
    source_file_regexes=[
        "python/pyspark/streaming"
    ],
    python_test_goals=[
        "pyspark.streaming.util",
        "pyspark.streaming.tests",
    ]
)


pyspark_mllib = Module(
    name="pyspark-mllib",
    dependencies=[pyspark_core, pyspark_streaming, pyspark_sql, mllib],
    source_file_regexes=[
        "python/pyspark/mllib"
    ],
    python_test_goals=[
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
        "pyspark.mllib.tests",
    ],
    blacklisted_python_implementations=[
        "PyPy"  # Skip these tests under PyPy since they require numpy and it isn't available there
    ]
)


pyspark_ml = Module(
    name="pyspark-ml",
    dependencies=[pyspark_core, pyspark_mllib],
    source_file_regexes=[
        "python/pyspark/ml/"
    ],
    python_test_goals=[
        "pyspark.ml.feature",
        "pyspark.ml.classification",
        "pyspark.ml.clustering",
        "pyspark.ml.recommendation",
        "pyspark.ml.regression",
        "pyspark.ml.tuning",
        "pyspark.ml.tests",
        "pyspark.ml.evaluation",
    ],
    blacklisted_python_implementations=[
        "PyPy"  # Skip these tests under PyPy since they require numpy and it isn't available there
    ]
)

sparkr = Module(
    name="sparkr",
    dependencies=[sql, mllib],
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


ec2 = Module(
    name="ec2",
    dependencies=[],
    source_file_regexes=[
        "ec2/",
    ]
)


# The root module is a dummy module which is used to run all of the tests.
# No other modules should directly depend on this module.
root = Module(
    name="root",
    dependencies=[],
    source_file_regexes=[],
    # In order to run all of the tests, enable every test profile:
    build_profile_flags=list(set(
        itertools.chain.from_iterable(m.build_profile_flags for m in all_modules))),
    sbt_test_goals=[
        "test",
    ],
    python_test_goals=list(itertools.chain.from_iterable(m.python_test_goals for m in all_modules)),
    should_run_r_tests=True
)
