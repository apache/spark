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

    def __init__(self, name, dependencies, source_file_regexes, build_profile_flags=(),
                 sbt_test_goals=(), should_run_python_tests=False, should_run_r_tests=False):
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
        :param sbt_test_goals: A set of SBT test goals for testing this module.
        :param should_run_python_tests: If true, changes in this module will trigger Python tests.
            For now, this has the effect of causing _all_ Python tests to be run, although in the
            future this should be changed to run only a subset of the Python tests that depend
            on this module.
        :param should_run_r_tests: If true, changes in this module will trigger all R tests.
        """
        self.name = name
        self.dependencies = dependencies
        self.source_file_prefixes = source_file_regexes
        self.sbt_test_goals = sbt_test_goals
        self.build_profile_flags = build_profile_flags
        self.should_run_python_tests = should_run_python_tests
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


streaming_kinesis_asl = Module(
    name="kinesis-asl",
    dependencies=[streaming],
    source_file_regexes=[
        "extras/kinesis-asl/",
    ],
    build_profile_flags=[
        "-Pkinesis-asl",
    ],
    sbt_test_goals=[
        "kinesis-asl/test",
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
    name="streaming_flume",
    dependencies=[streaming],
    source_file_regexes=[
        "external/flume",
    ],
    sbt_test_goals=[
        "streaming-flume/test",
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


pyspark = Module(
    name="pyspark",
    dependencies=[mllib, streaming, streaming_kafka, sql],
    source_file_regexes=[
        "python/"
    ],
    should_run_python_tests=True
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
    build_profile_flags=
        list(set(itertools.chain.from_iterable(m.build_profile_flags for m in all_modules))),
    sbt_test_goals=[
        "test",
    ],
    should_run_python_tests=True,
    should_run_r_tests=True
)
