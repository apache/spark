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

sketch = Module(
    name="sketch",
    dependencies=[tags],
    source_file_regexes=[
        "common/sketch/",
    ],
    sbt_test_goals=["sketch/test"],
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
    dependencies=[tags, sketch, core],
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

protobuf = Module(
    name="protobuf",
    dependencies=[sql],
    source_file_regexes=[
        "connector/protobuf",
    ],
    sbt_test_goals=[
        "protobuf/test",
    ],
)

connect = Module(
    name="connect",
    dependencies=[hive, avro, protobuf],
    source_file_regexes=[
        "connector/connect",
    ],
    build_profile_flags=["-Pconnect"],
    sbt_test_goals=[
        "connect/test",
        "connect-client-jvm/test",
    ],
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
        "pyspark.tests.test_memory_profiler",
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
        "pyspark.tests.test_stage_sched",
    ],
)

pyspark_sql = Module(
    name="pyspark-sql",
    dependencies=[pyspark_core, hive, avro, protobuf],
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
        "pyspark.sql.udtf",
        "pyspark.sql.window",
        "pyspark.sql.avro.functions",
        "pyspark.sql.protobuf.functions",
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
        "pyspark.sql.tests.test_arrow_python_udf",
        "pyspark.sql.tests.test_catalog",
        "pyspark.sql.tests.test_column",
        "pyspark.sql.tests.test_conf",
        "pyspark.sql.tests.test_context",
        "pyspark.sql.tests.test_dataframe",
        "pyspark.sql.tests.test_datasources",
        "pyspark.sql.tests.test_errors",
        "pyspark.sql.tests.test_functions",
        "pyspark.sql.tests.test_group",
        "pyspark.sql.tests.pandas.test_pandas_cogrouped_map",
        "pyspark.sql.tests.pandas.test_pandas_grouped_map",
        "pyspark.sql.tests.pandas.test_pandas_grouped_map_with_state",
        "pyspark.sql.tests.pandas.test_pandas_map",
        "pyspark.sql.tests.test_arrow_map",
        "pyspark.sql.tests.pandas.test_pandas_udf",
        "pyspark.sql.tests.pandas.test_pandas_udf_grouped_agg",
        "pyspark.sql.tests.pandas.test_pandas_udf_scalar",
        "pyspark.sql.tests.pandas.test_pandas_udf_typehints",
        "pyspark.sql.tests.pandas.test_pandas_udf_typehints_with_future_annotations",
        "pyspark.sql.tests.pandas.test_pandas_udf_window",
        "pyspark.sql.tests.test_pandas_sqlmetrics",
        "pyspark.sql.tests.test_readwriter",
        "pyspark.sql.tests.test_serde",
        "pyspark.sql.tests.test_session",
        "pyspark.sql.tests.streaming.test_streaming",
        "pyspark.sql.tests.streaming.test_streaming_foreach",
        "pyspark.sql.tests.streaming.test_streaming_foreach_batch",
        "pyspark.sql.tests.streaming.test_streaming_listener",
        "pyspark.sql.tests.test_types",
        "pyspark.sql.tests.test_udf",
        "pyspark.sql.tests.test_udf_profiler",
        "pyspark.sql.tests.test_udtf",
        "pyspark.sql.tests.test_utils",
    ],
)

pyspark_testing = Module(
    name="pyspark-testing",
    dependencies=[pyspark_core, pyspark_sql],
    source_file_regexes=["python/pyspark/testing"],
    python_test_goals=[
        # doctests
        "pyspark.testing.utils",
        "pyspark.testing.pandasutils",
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
        "pyspark.ml.tests.test_functions",
        "pyspark.ml.tests.test_image",
        "pyspark.ml.tests.test_linalg",
        "pyspark.ml.tests.test_model_cache",
        "pyspark.ml.tests.test_param",
        "pyspark.ml.tests.test_persistence",
        "pyspark.ml.tests.test_pipeline",
        "pyspark.ml.tests.test_stat",
        "pyspark.ml.tests.test_training_summary",
        "pyspark.ml.tests.tuning.test_tuning",
        "pyspark.ml.tests.tuning.test_cv_io_basic",
        "pyspark.ml.tests.tuning.test_cv_io_nested",
        "pyspark.ml.tests.tuning.test_cv_io_pipeline",
        "pyspark.ml.tests.tuning.test_tvs_io_basic",
        "pyspark.ml.tests.tuning.test_tvs_io_nested",
        "pyspark.ml.tests.tuning.test_tvs_io_pipeline",
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
        "pyspark.pandas.tests.data_type_ops.test_num_arithmetic",
        "pyspark.pandas.tests.data_type_ops.test_num_reverse",
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
        "pyspark.pandas.tests.test_ewm",
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
        "pyspark.pandas.tests.indexes.test_align",
        "pyspark.pandas.tests.indexes.test_indexing",
        "pyspark.pandas.tests.indexes.test_reindex",
        "pyspark.pandas.tests.indexes.test_rename",
        "pyspark.pandas.tests.indexes.test_reset_index",
        "pyspark.pandas.tests.computation.test_any_all",
        "pyspark.pandas.tests.computation.test_apply_func",
        "pyspark.pandas.tests.computation.test_binary_ops",
        "pyspark.pandas.tests.computation.test_combine",
        "pyspark.pandas.tests.computation.test_compute",
        "pyspark.pandas.tests.computation.test_corrwith",
        "pyspark.pandas.tests.computation.test_cov",
        "pyspark.pandas.tests.computation.test_cumulative",
        "pyspark.pandas.tests.computation.test_describe",
        "pyspark.pandas.tests.computation.test_eval",
        "pyspark.pandas.tests.computation.test_melt",
        "pyspark.pandas.tests.computation.test_missing_data",
        "pyspark.pandas.tests.computation.test_pivot",
        "pyspark.pandas.tests.frame.test_attrs",
        "pyspark.pandas.tests.frame.test_constructor",
        "pyspark.pandas.tests.frame.test_conversion",
        "pyspark.pandas.tests.frame.test_reindexing",
        "pyspark.pandas.tests.frame.test_reshaping",
        "pyspark.pandas.tests.frame.test_spark",
        "pyspark.pandas.tests.frame.test_take",
        "pyspark.pandas.tests.frame.test_time_series",
        "pyspark.pandas.tests.frame.test_truncate",
        "pyspark.pandas.tests.io.test_io",
        "pyspark.pandas.tests.groupby.test_aggregate",
        "pyspark.pandas.tests.groupby.test_apply_func",
        "pyspark.pandas.tests.groupby.test_cumulative",
        "pyspark.pandas.tests.groupby.test_describe",
        "pyspark.pandas.tests.groupby.test_groupby",
        "pyspark.pandas.tests.groupby.test_head_tail",
        "pyspark.pandas.tests.groupby.test_index",
        "pyspark.pandas.tests.groupby.test_missing_data",
        "pyspark.pandas.tests.groupby.test_split_apply",
        "pyspark.pandas.tests.groupby.test_stat",
        "pyspark.pandas.tests.test_indexing",
        "pyspark.pandas.tests.test_ops_on_diff_frames",
        "pyspark.pandas.tests.test_ops_on_diff_frames_groupby",
        "pyspark.pandas.tests.diff_frames_ops.test_align",
        "pyspark.pandas.tests.diff_frames_ops.test_basic_slow",
        "pyspark.pandas.tests.diff_frames_ops.test_cov_corrwith",
        "pyspark.pandas.tests.diff_frames_ops.test_dot_frame",
        "pyspark.pandas.tests.diff_frames_ops.test_dot_series",
        "pyspark.pandas.tests.diff_frames_ops.test_index",
        "pyspark.pandas.tests.diff_frames_ops.test_series",
        "pyspark.pandas.tests.diff_frames_ops.test_setitem_frame",
        "pyspark.pandas.tests.diff_frames_ops.test_setitem_series",
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
        "pyspark.pandas.tests.test_stats",
    ],
    excluded_python_implementations=[
        "PyPy"  # Skip these tests under PyPy since they require numpy, pandas, and pyarrow and
        # they aren't available there
    ],
)


pyspark_connect = Module(
    name="pyspark-connect",
    dependencies=[pyspark_sql, pyspark_ml, connect],
    source_file_regexes=[
        "python/pyspark/sql/connect",
        "python/pyspark/ml/connect",
    ],
    python_test_goals=[
        # sql doctests
        "pyspark.sql.connect.catalog",
        "pyspark.sql.connect.conf",
        "pyspark.sql.connect.group",
        "pyspark.sql.connect.session",
        "pyspark.sql.connect.window",
        "pyspark.sql.connect.column",
        "pyspark.sql.connect.readwriter",
        "pyspark.sql.connect.dataframe",
        "pyspark.sql.connect.functions",
        "pyspark.sql.connect.avro.functions",
        "pyspark.sql.connect.protobuf.functions",
        "pyspark.sql.connect.streaming.readwriter",
        "pyspark.sql.connect.streaming.query",
        # sql unittests
        "pyspark.sql.tests.connect.test_connect_plan",
        "pyspark.sql.tests.connect.test_connect_basic",
        "pyspark.sql.tests.connect.test_connect_function",
        "pyspark.sql.tests.connect.test_connect_column",
        "pyspark.sql.tests.connect.test_parity_arrow",
        "pyspark.sql.tests.connect.test_parity_arrow_python_udf",
        "pyspark.sql.tests.connect.test_parity_datasources",
        "pyspark.sql.tests.connect.test_parity_errors",
        "pyspark.sql.tests.connect.test_parity_catalog",
        "pyspark.sql.tests.connect.test_parity_conf",
        "pyspark.sql.tests.connect.test_parity_serde",
        "pyspark.sql.tests.connect.test_parity_functions",
        "pyspark.sql.tests.connect.test_parity_group",
        "pyspark.sql.tests.connect.test_parity_dataframe",
        "pyspark.sql.tests.connect.test_parity_types",
        "pyspark.sql.tests.connect.test_parity_column",
        "pyspark.sql.tests.connect.test_parity_readwriter",
        "pyspark.sql.tests.connect.test_parity_udf",
        "pyspark.sql.tests.connect.test_parity_udtf",
        "pyspark.sql.tests.connect.test_parity_pandas_udf",
        "pyspark.sql.tests.connect.test_parity_pandas_map",
        "pyspark.sql.tests.connect.test_parity_arrow_map",
        "pyspark.sql.tests.connect.test_parity_pandas_grouped_map",
        "pyspark.sql.tests.connect.test_parity_pandas_cogrouped_map",
        "pyspark.sql.tests.connect.test_utils",
        "pyspark.sql.tests.connect.client.test_artifact",
        "pyspark.sql.tests.connect.client.test_client",
        "pyspark.sql.tests.connect.streaming.test_parity_streaming",
        "pyspark.sql.tests.connect.streaming.test_parity_listener",
        "pyspark.sql.tests.connect.streaming.test_parity_foreach",
        "pyspark.sql.tests.connect.streaming.test_parity_foreach_batch",
        "pyspark.sql.tests.connect.test_parity_pandas_grouped_map_with_state",
        "pyspark.sql.tests.connect.test_parity_pandas_udf_scalar",
        "pyspark.sql.tests.connect.test_parity_pandas_udf_grouped_agg",
        "pyspark.sql.tests.connect.test_parity_pandas_udf_window",
        # ml doctests
        "pyspark.ml.connect.functions",
        # ml unittests
        "pyspark.ml.tests.connect.test_connect_function",
        "pyspark.ml.tests.connect.test_parity_torch_distributor",
        "pyspark.ml.tests.connect.test_parity_torch_data_loader",
        "pyspark.ml.tests.connect.test_connect_summarizer",
        "pyspark.ml.tests.connect.test_connect_evaluation",
        "pyspark.ml.tests.connect.test_connect_feature",
        "pyspark.ml.tests.connect.test_connect_classification",
        "pyspark.ml.tests.connect.test_connect_pipeline",
        "pyspark.ml.tests.connect.test_connect_tuning",
    ],
    excluded_python_implementations=[
        "PyPy"  # Skip these tests under PyPy since they require numpy, pandas, and pyarrow and
        # they aren't available there
    ],
)


pyspark_pandas_connect = Module(
    name="pyspark-pandas-connect",
    dependencies=[pyspark_connect, pyspark_pandas],
    source_file_regexes=[
        "python/pyspark/pandas",
    ],
    python_test_goals=[
        # pandas-on-Spark unittests
        "pyspark.pandas.tests.connect.data_type_ops.test_parity_base",
        "pyspark.pandas.tests.connect.data_type_ops.test_parity_binary_ops",
        "pyspark.pandas.tests.connect.data_type_ops.test_parity_boolean_ops",
        "pyspark.pandas.tests.connect.data_type_ops.test_parity_categorical_ops",
        "pyspark.pandas.tests.connect.data_type_ops.test_parity_complex_ops",
        "pyspark.pandas.tests.connect.data_type_ops.test_parity_date_ops",
        "pyspark.pandas.tests.connect.data_type_ops.test_parity_datetime_ops",
        "pyspark.pandas.tests.connect.data_type_ops.test_parity_null_ops",
        "pyspark.pandas.tests.connect.data_type_ops.test_parity_num_ops",
        "pyspark.pandas.tests.connect.data_type_ops.test_parity_num_arithmetic",
        "pyspark.pandas.tests.connect.data_type_ops.test_parity_num_reverse",
        "pyspark.pandas.tests.connect.data_type_ops.test_parity_string_ops",
        "pyspark.pandas.tests.connect.data_type_ops.test_parity_udt_ops",
        "pyspark.pandas.tests.connect.data_type_ops.test_parity_timedelta_ops",
        "pyspark.pandas.tests.connect.indexes.test_parity_category",
        "pyspark.pandas.tests.connect.indexes.test_parity_timedelta",
        "pyspark.pandas.tests.connect.plot.test_parity_frame_plot",
        "pyspark.pandas.tests.connect.plot.test_parity_frame_plot_matplotlib",
        "pyspark.pandas.tests.connect.plot.test_parity_frame_plot_plotly",
        "pyspark.pandas.tests.connect.plot.test_parity_series_plot",
        "pyspark.pandas.tests.connect.plot.test_parity_series_plot_matplotlib",
        "pyspark.pandas.tests.connect.plot.test_parity_series_plot_plotly",
        "pyspark.pandas.tests.connect.test_parity_categorical",
        "pyspark.pandas.tests.connect.test_parity_config",
        "pyspark.pandas.tests.connect.test_parity_csv",
        "pyspark.pandas.tests.connect.test_parity_dataframe_conversion",
        "pyspark.pandas.tests.connect.test_parity_dataframe_spark_io",
        "pyspark.pandas.tests.connect.test_parity_default_index",
        "pyspark.pandas.tests.connect.test_parity_expanding",
        "pyspark.pandas.tests.connect.test_parity_extension",
        "pyspark.pandas.tests.connect.test_parity_ewm",
        "pyspark.pandas.tests.connect.test_parity_frame_spark",
        "pyspark.pandas.tests.connect.test_parity_generic_functions",
        "pyspark.pandas.tests.connect.test_parity_indexops_spark",
        "pyspark.pandas.tests.connect.test_parity_internal",
        "pyspark.pandas.tests.connect.test_parity_namespace",
        "pyspark.pandas.tests.connect.test_parity_numpy_compat",
        "pyspark.pandas.tests.connect.test_parity_ops_on_diff_frames_groupby_expanding",
        "pyspark.pandas.tests.connect.test_parity_ops_on_diff_frames_groupby_rolling",
        "pyspark.pandas.tests.connect.test_parity_repr",
        "pyspark.pandas.tests.connect.test_parity_resample",
        "pyspark.pandas.tests.connect.test_parity_reshape",
        "pyspark.pandas.tests.connect.test_parity_rolling",
        "pyspark.pandas.tests.connect.test_parity_scalars",
        "pyspark.pandas.tests.connect.test_parity_series_conversion",
        "pyspark.pandas.tests.connect.test_parity_series_datetime",
        "pyspark.pandas.tests.connect.test_parity_series_string",
        "pyspark.pandas.tests.connect.test_parity_spark_functions",
        "pyspark.pandas.tests.connect.test_parity_sql",
        "pyspark.pandas.tests.connect.test_parity_typedef",
        "pyspark.pandas.tests.connect.test_parity_utils",
        "pyspark.pandas.tests.connect.test_parity_window",
        "pyspark.pandas.tests.connect.indexes.test_parity_base",
    ],
    excluded_python_implementations=[
        "PyPy"  # Skip these tests under PyPy since they require numpy, pandas, and pyarrow and
        # they aren't available there
    ],
)


# This module should contain the same test list with 'pyspark_pandas_slow' for maintenance.
pyspark_pandas_slow_connect = Module(
    name="pyspark-pandas-slow-connect",
    dependencies=[pyspark_connect, pyspark_pandas_slow],
    source_file_regexes=[
        "python/pyspark/pandas",
    ],
    python_test_goals=[
        # pandas-on-Spark unittests
        "pyspark.pandas.tests.connect.indexes.test_parity_datetime",
        "pyspark.pandas.tests.connect.indexes.test_parity_align",
        "pyspark.pandas.tests.connect.indexes.test_parity_indexing",
        "pyspark.pandas.tests.connect.indexes.test_parity_reindex",
        "pyspark.pandas.tests.connect.indexes.test_parity_rename",
        "pyspark.pandas.tests.connect.indexes.test_parity_reset_index",
        "pyspark.pandas.tests.connect.computation.test_parity_any_all",
        "pyspark.pandas.tests.connect.computation.test_parity_apply_func",
        "pyspark.pandas.tests.connect.computation.test_parity_binary_ops",
        "pyspark.pandas.tests.connect.computation.test_parity_combine",
        "pyspark.pandas.tests.connect.computation.test_parity_compute",
        "pyspark.pandas.tests.connect.computation.test_parity_cov",
        "pyspark.pandas.tests.connect.computation.test_parity_corrwith",
        "pyspark.pandas.tests.connect.computation.test_parity_cumulative",
        "pyspark.pandas.tests.connect.computation.test_parity_describe",
        "pyspark.pandas.tests.connect.computation.test_parity_eval",
        "pyspark.pandas.tests.connect.computation.test_parity_melt",
        "pyspark.pandas.tests.connect.computation.test_parity_missing_data",
        "pyspark.pandas.tests.connect.computation.test_parity_pivot",
        "pyspark.pandas.tests.connect.frame.test_parity_attrs",
        "pyspark.pandas.tests.connect.frame.test_parity_constructor",
        "pyspark.pandas.tests.connect.frame.test_parity_conversion",
        "pyspark.pandas.tests.connect.frame.test_parity_reindexing",
        "pyspark.pandas.tests.connect.frame.test_parity_reshaping",
        "pyspark.pandas.tests.connect.frame.test_parity_spark",
        "pyspark.pandas.tests.connect.frame.test_parity_take",
        "pyspark.pandas.tests.connect.frame.test_parity_time_series",
        "pyspark.pandas.tests.connect.frame.test_parity_truncate",
        "pyspark.pandas.tests.connect.io.test_parity_io",
        "pyspark.pandas.tests.connect.groupby.test_parity_aggregate",
        "pyspark.pandas.tests.connect.groupby.test_parity_apply_func",
        "pyspark.pandas.tests.connect.groupby.test_parity_cumulative",
        "pyspark.pandas.tests.connect.groupby.test_parity_describe",
        "pyspark.pandas.tests.connect.groupby.test_parity_groupby",
        "pyspark.pandas.tests.connect.groupby.test_parity_head_tail",
        "pyspark.pandas.tests.connect.groupby.test_parity_index",
        "pyspark.pandas.tests.connect.groupby.test_parity_missing_data",
        "pyspark.pandas.tests.connect.groupby.test_parity_split_apply",
        "pyspark.pandas.tests.connect.groupby.test_parity_stat",
        "pyspark.pandas.tests.connect.test_parity_indexing",
        "pyspark.pandas.tests.connect.test_parity_ops_on_diff_frames",
        "pyspark.pandas.tests.connect.test_parity_ops_on_diff_frames_groupby",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_align",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_basic_slow",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_cov_corrwith",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_dot_frame",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_dot_series",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_index",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_series",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_setitem_frame",
        "pyspark.pandas.tests.connect.diff_frames_ops.test_parity_setitem_series",
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
        "pyspark.pandas.tests.connect.test_parity_stats",
    ],
    excluded_python_implementations=[
        "PyPy"  # Skip these tests under PyPy since they require numpy, pandas, and pyarrow and
        # they aren't available there
    ],
)


pyspark_errors = Module(
    name="pyspark-errors",
    dependencies=[],
    source_file_regexes=[
        # SPARK-44544: Force the execution of pyspark_errors when there are any changes
        # in PySpark, since the Python Packaging Tests is only enabled within this module.
        # This module is the smallest Python test module, it contains only 1 test file
        # and normally takes < 2 seconds, so the additional cost is small.
        "python/",
        "python/pyspark/errors",
    ],
    python_test_goals=[
        # unittests
        "pyspark.errors.tests.test_errors",
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
    build_profile_flags=["-Pkubernetes", "-Pvolcano"],
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
