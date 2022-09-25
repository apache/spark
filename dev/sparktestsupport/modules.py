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

all_modules = []


@total_ordering
class Module(object):
    """
    A module is the basic abstraction in our test runner script. Each module consists of a set
    of source files, a set of test commands, and a set of dependencies on other modules. We use
    modules to define a dependency graph that let us determine which tests to run based on which
    files have changed.
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
    ],
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
    environ=None
    if "GITHUB_ACTIONS" not in os.environ
    else {"ENABLE_DOCKER_INTEGRATION_TESTS": "1"},
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
    environ=None
    if "GITHUB_ACTIONS" not in os.environ
    else {"ENABLE_DOCKER_INTEGRATION_TESTS": "1"},
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
    test_tags=["org.apache.spark.tags.ExtendedHiveTest"],
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
    ],
)

avro = Module(
    name="avro",
    dependencies=[sql],
    source_file_regexes=[
        "connector/avro",
    ],
    sbt_test_goals=[
        "avro/test",
    ],
)

sql_kafka = Module(
    name="sql-kafka-0-10",
    dependencies=[sql],
    source_file_regexes=[
        "connector/kafka-0-10-sql",
    ],
    sbt_test_goals=[
        "sql-kafka-0-10/test",
    ],
)

sketch = Module(
    name="sketch",
    dependencies=[tags],
    source_file_regexes=[
        "common/sketch/",
    ],
    sbt_test_goals=["sketch/test"],
)

graphx = Module(
    name="graphx",
    dependencies=[tags, core],
    source_file_regexes=[
        "graphx/",
    ],
    sbt_test_goals=["graphx/test"],
)

streaming = Module(
    name="streaming",
    dependencies=[tags, core],
    source_file_regexes=[
        "streaming",
    ],
    sbt_test_goals=[
        "streaming/test",
    ],
)


# Don't set the dependencies because changes in other modules should not trigger Kinesis tests.
# Kinesis tests depends on external Amazon kinesis service. We should run these tests only when
# files in streaming_kinesis_asl are changed, so that if Kinesis experiences an outage, we don't
# fail other PRs.
streaming_kinesis_asl = Module(
    name="streaming-kinesis-asl",
    dependencies=[tags, core],
    source_file_regexes=[
        "connector/kinesis-asl/",
        "connector/kinesis-asl-assembly/",
    ],
    build_profile_flags=[
        "-Pkinesis-asl",
    ],
    environ={"ENABLE_KINESIS_TESTS": "1"},
    sbt_test_goals=[
        "streaming-kinesis-asl/test",
    ],
)


streaming_kafka_0_10 = Module(
    name="streaming-kafka-0-10",
    dependencies=[streaming, core],
    source_file_regexes=[
        # The ending "/" is necessary otherwise it will include "sql-kafka" codes
        "connector/kafka-0-10/",
        "connector/kafka-0-10-assembly",
        "connector/kafka-0-10-token-provider",
    ],
    sbt_test_goals=["streaming-kafka-0-10/test", "token-provider-kafka-0-10/test"],
)


mllib_local = Module(
    name="mllib-local",
    dependencies=[tags, core],
    source_file_regexes=[
        "mllib-local",
    ],
    sbt_test_goals=[
        "mllib-local/test",
    ],
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
    ],
)


examples = Module(
    name="examples",
    dependencies=[graphx, mllib, streaming, hive],
    source_file_regexes=[
        "examples/",
    ],
    sbt_test_goals=[
        "examples/test",
    ],
)

pyspark_core = Module(
    name="pyspark-core",
    dependencies=[core],
    source_file_regexes=["python/(?!pyspark/(ml|mllib|sql|streaming))"],
    python_test_goals=[
        # doctests
        "pyspark.rdd",
        "pyspark.context",
        "pyspark.conf",
        "pyspark.broadcast",
        "pyspark.accumulators",
        "pyspark.files",
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
        "pyspark.tests.test_install_spark",
        "pyspark.tests.test_join",
        "pyspark.tests.test_profiler",
        "pyspark.tests.test_rdd",
        "pyspark.tests.test_rddbarrier",
        "pyspark.tests.test_rddsampler",
        "pyspark.tests.test_readwrite",
        "pyspark.tests.test_serializers",
        "pyspark.tests.test_shuffle",
        "pyspark.tests.test_statcounter",
        "pyspark.tests.test_taskcontext",
        "pyspark.tests.test_util",
        "pyspark.tests.test_worker",
    ],
)

pyspark_sql = Module(
    name="pyspark-sql",
    dependencies=[pyspark_core, hive, avro],
    source_file_regexes=["python/pyspark/sql"],
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
        "pyspark.sql.streaming.query",
        "pyspark.sql.streaming.readwriter",
        "pyspark.sql.streaming.listener",
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
        "pyspark.sql.observation",
        # unittests
        "pyspark.sql.tests.test_arrow",
        "pyspark.sql.tests.test_catalog",
        "pyspark.sql.tests.test_column",
        "pyspark.sql.tests.test_conf",
        "pyspark.sql.tests.test_context",
        "pyspark.sql.tests.test_dataframe",
        "pyspark.sql.tests.test_datasources",
        "pyspark.sql.tests.test_functions",
        "pyspark.sql.tests.test_group",
        "pyspark.sql.tests.test_pandas_cogrouped_map",
        "pyspark.sql.tests.test_pandas_grouped_map",
        "pyspark.sql.tests.test_pandas_grouped_map_with_state",
        "pyspark.sql.tests.test_pandas_map",
        "pyspark.sql.tests.test_arrow_map",
        "pyspark.sql.tests.test_pandas_udf",
        "pyspark.sql.tests.test_pandas_udf_grouped_agg",
        "pyspark.sql.tests.test_pandas_udf_scalar",
        "pyspark.sql.tests.test_pandas_udf_typehints",
        "pyspark.sql.tests.test_pandas_udf_typehints_with_future_annotations",
        "pyspark.sql.tests.test_pandas_udf_window",
        "pyspark.sql.tests.test_readwriter",
        "pyspark.sql.tests.test_serde",
        "pyspark.sql.tests.test_session",
        "pyspark.sql.tests.test_streaming",
        "pyspark.sql.tests.test_streaming_listener",
        "pyspark.sql.tests.test_types",
        "pyspark.sql.tests.test_udf",
        "pyspark.sql.tests.test_udf_profiler",
        "pyspark.sql.tests.test_utils",
    ],
)

pyspark_sql = Module(
    name="pyspark-sql-connect",
    dependencies=[pyspark_core, hive, avro],
    source_file_regexes=["python/pyspark/sql/connect"],
    python_test_goals=[
        # doctests
        # No doctests yet.
        # unittests
        "pyspark.sql.tests.connect.test_column_expressions",
        "pyspark.sql.tests.connect.test_plan_only",
        "pyspark.sql.tests.connect.test_select_ops",
        "pyspark.sql.tests.connect.test_spark_connect",
    ],
    excluded_python_implementations=[
        "PyPy"  # Skip these tests under PyPy since they require numpy, pandas, and pyarrow and
        # they aren't available there
    ],
)

pyspark_resource = Module(
    name="pyspark-resource",
    dependencies=[pyspark_core],
    source_file_regexes=["python/pyspark/resource"],
    python_test_goals=[
        # doctests
        "pyspark.resource.profile",
        # unittests
        "pyspark.resource.tests.test_resources",
    ],
)


pyspark_streaming = Module(
    name="pyspark-streaming",
    dependencies=[pyspark_core, streaming, streaming_kinesis_asl],
    source_file_regexes=["python/pyspark/streaming"],
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


pyspark_mllib = Module(
    name="pyspark-mllib",
    dependencies=[pyspark_core, pyspark_streaming, pyspark_sql, mllib],
    source_file_regexes=["python/pyspark/mllib"],
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
    excluded_python_implementations=[
        "PyPy"  # Skip these tests under PyPy since they require numpy and it isn't available there
    ],
)


pyspark_ml = Module(
    name="pyspark-ml",
    dependencies=[pyspark_core, pyspark_mllib],
    source_file_regexes=["python/pyspark/ml/"],
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
        "pyspark.ml.tests.test_base",
        "pyspark.ml.tests.test_evaluation",
        "pyspark.ml.tests.test_feature",
        "pyspark.ml.tests.test_image",
        "pyspark.ml.tests.test_linalg",
        "pyspark.ml.tests.test_param",
        "pyspark.ml.tests.test_persistence",
        "pyspark.ml.tests.test_pipeline",
        "pyspark.ml.tests.test_stat",
        "pyspark.ml.tests.test_training_summary",
        "pyspark.ml.tests.test_tuning",
        "pyspark.ml.tests.test_util",
        "pyspark.ml.tests.test_wrapper",
    ],
    excluded_python_implementations=[
        "PyPy"  # Skip these tests under PyPy since they require numpy and it isn't available there
    ],
)

pyspark_pandas = Module(
    name="pyspark-pandas",
    dependencies=[pyspark_core, pyspark_sql],
    source_file_regexes=["python/pyspark/pandas/"],
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
        "pyspark.pandas.indexes.numeric",
        "pyspark.pandas.spark.accessors",
        "pyspark.pandas.spark.utils",
        "pyspark.pandas.typedef.typehints",
        # unittests
        "pyspark.pandas.tests.data_type_ops.test_base",
        "pyspark.pandas.tests.data_type_ops.test_binary_ops",
        "pyspark.pandas.tests.data_type_ops.test_boolean_ops",
        "pyspark.pandas.tests.data_type_ops.test_categorical_ops",
        "pyspark.pandas.tests.data_type_ops.test_complex_ops",
        "pyspark.pandas.tests.data_type_ops.test_date_ops",
        "pyspark.pandas.tests.data_type_ops.test_datetime_ops",
        "pyspark.pandas.tests.data_type_ops.test_null_ops",
        "pyspark.pandas.tests.data_type_ops.test_num_ops",
        "pyspark.pandas.tests.data_type_ops.test_string_ops",
        "pyspark.pandas.tests.data_type_ops.test_udt_ops",
        "pyspark.pandas.tests.data_type_ops.test_timedelta_ops",
        "pyspark.pandas.tests.indexes.test_category",
        "pyspark.pandas.tests.indexes.test_timedelta",
        "pyspark.pandas.tests.plot.test_frame_plot",
        "pyspark.pandas.tests.plot.test_frame_plot_matplotlib",
        "pyspark.pandas.tests.plot.test_frame_plot_plotly",
        "pyspark.pandas.tests.plot.test_series_plot",
        "pyspark.pandas.tests.plot.test_series_plot_matplotlib",
        "pyspark.pandas.tests.plot.test_series_plot_plotly",
        "pyspark.pandas.tests.test_categorical",
        "pyspark.pandas.tests.test_config",
        "pyspark.pandas.tests.test_csv",
        "pyspark.pandas.tests.test_dataframe_conversion",
        "pyspark.pandas.tests.test_dataframe_spark_io",
        "pyspark.pandas.tests.test_default_index",
        "pyspark.pandas.tests.test_expanding",
        "pyspark.pandas.tests.test_extension",
        "pyspark.pandas.tests.test_frame_spark",
        "pyspark.pandas.tests.test_generic_functions",
        "pyspark.pandas.tests.test_indexops_spark",
        "pyspark.pandas.tests.test_internal",
        "pyspark.pandas.tests.test_namespace",
        "pyspark.pandas.tests.test_numpy_compat",
        "pyspark.pandas.tests.test_ops_on_diff_frames_groupby_expanding",
        "pyspark.pandas.tests.test_ops_on_diff_frames_groupby_rolling",
        "pyspark.pandas.tests.test_repr",
        "pyspark.pandas.tests.test_resample",
        "pyspark.pandas.tests.test_reshape",
        "pyspark.pandas.tests.test_rolling",
        "pyspark.pandas.tests.test_scalars",
        "pyspark.pandas.tests.test_series_conversion",
        "pyspark.pandas.tests.test_series_datetime",
        "pyspark.pandas.tests.test_series_string",
        "pyspark.pandas.tests.test_spark_functions",
        "pyspark.pandas.tests.test_sql",
        "pyspark.pandas.tests.test_typedef",
        "pyspark.pandas.tests.test_utils",
        "pyspark.pandas.tests.test_window",
    ],
    excluded_python_implementations=[
        "PyPy"  # Skip these tests under PyPy since they require numpy, pandas, and pyarrow and
        # they aren't available there
    ],
)

pyspark_pandas_slow = Module(
    name="pyspark-pandas-slow",
    dependencies=[pyspark_core, pyspark_sql],
    source_file_regexes=["python/pyspark/pandas/"],
    python_test_goals=[
        # doctests
        "pyspark.pandas.frame",
        "pyspark.pandas.generic",
        "pyspark.pandas.series",
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
    ],
    excluded_python_implementations=[
        "PyPy"  # Skip these tests under PyPy since they require numpy, pandas, and pyarrow and
        # they aren't available there
    ],
)

sparkr = Module(
    name="sparkr",
    dependencies=[hive, mllib],
    source_file_regexes=[
        "R/",
    ],
    should_run_r_tests=True,
)


docs = Module(
    name="docs",
    dependencies=[],
    source_file_regexes=[
        "docs/",
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
    test_tags=["org.apache.spark.tags.ExtendedYarnTest"],
)

mesos = Module(
    name="mesos",
    dependencies=[],
    source_file_regexes=["resource-managers/mesos/"],
    build_profile_flags=["-Pmesos"],
    sbt_test_goals=["mesos/test"],
)

kubernetes = Module(
    name="kubernetes",
    dependencies=[],
    source_file_regexes=["resource-managers/kubernetes"],
    build_profile_flags=["-Pkubernetes"],
    sbt_test_goals=["kubernetes/test"],
)

hadoop_cloud = Module(
    name="hadoop-cloud",
    dependencies=[],
    source_file_regexes=["hadoop-cloud"],
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

docker_integration_tests = Module(
    name="docker-integration-tests",
    dependencies=[sql],
    build_profile_flags=["-Pdocker-integration-tests"],
    source_file_regexes=["connector/docker-integration-tests"],
    sbt_test_goals=["docker-integration-tests/test"],
    environ=None
    if "GITHUB_ACTIONS" not in os.environ
    else {"ENABLE_DOCKER_INTEGRATION_TESTS": "1"},
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
