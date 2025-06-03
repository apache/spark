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

from contextlib import contextmanager
import inspect
import tempfile
import unittest
import os
import sys
import warnings
from io import StringIO
from typing import Iterator, cast

from pyspark import SparkConf
from pyspark.errors import PySparkValueError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf, udf
from pyspark.sql.window import Window
from pyspark.profiler import UDFBasicProfiler
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.testing.utils import (
    have_pandas,
    have_pyarrow,
    have_flameprof,
    pandas_requirement_message,
    pyarrow_requirement_message,
)


def _do_computation(spark, *, action=lambda df: df.collect(), use_arrow=False):
    @udf("long", useArrow=use_arrow)
    def add1(x):
        return x + 1

    @udf("long", useArrow=use_arrow)
    def add2(x):
        return x + 2

    df = spark.range(10).select(add1("id"), add2("id"), add1("id"), add2(col("id") + 1))
    action(df)


class UDFProfilerTests(unittest.TestCase):
    def setUp(self):
        self._old_sys_path = list(sys.path)
        class_name = self.__class__.__name__
        conf = SparkConf().set("spark.python.profile", "true")
        self.spark = (
            SparkSession.builder.master("local[4]")
            .config(conf=conf)
            .appName(class_name)
            .getOrCreate()
        )
        self.sc = self.spark.sparkContext

    def tearDown(self):
        self.spark.stop()
        sys.path = self._old_sys_path

    def test_udf_profiler(self):
        _do_computation(self.spark)

        profilers = self.sc.profiler_collector.profilers
        self.assertEqual(4, len(profilers))

        old_stdout = sys.stdout
        try:
            sys.stdout = io = StringIO()
            self.sc.show_profiles()
        finally:
            sys.stdout = old_stdout

        with tempfile.TemporaryDirectory(prefix="test_udf_profiler") as d:
            self.sc.dump_profiles(d)

            for i, udf_name in enumerate(["add1", "add2", "add1", "add2"]):
                id, profiler, _ = profilers[i]
                with self.subTest(id=id, udf_name=udf_name):
                    stats = profiler.stats()
                    self.assertTrue(stats is not None)
                    width, stat_list = stats.get_print_list([])
                    func_names = [func_name for fname, n, func_name in stat_list]
                    self.assertTrue(udf_name in func_names)

                    self.assertTrue(udf_name in io.getvalue())
                    self.assertTrue("udf_%d.pstats" % id in os.listdir(d))

    def test_custom_udf_profiler(self):
        class TestCustomProfiler(UDFBasicProfiler):
            def show(self, id):
                self.result = "Custom formatting"

        self.sc.profiler_collector.udf_profiler_cls = TestCustomProfiler

        _do_computation(self.spark)

        profilers = self.sc.profiler_collector.profilers
        self.assertEqual(4, len(profilers))
        _, profiler, _ = profilers[0]
        self.assertTrue(isinstance(profiler, TestCustomProfiler))

        self.sc.show_profiles()
        self.assertEqual("Custom formatting", profiler.result)

    # Unsupported
    def exec_pandas_udf_iter_to_iter(self):
        import pandas as pd

        @pandas_udf("int")
        def iter_to_iter(batch_ser: Iterator[pd.Series]) -> Iterator[pd.Series]:
            for ser in batch_ser:
                yield ser + 1

        self.spark.range(10).select(iter_to_iter("id")).collect()

    # Unsupported
    def exec_map(self):
        import pandas as pd

        def map(pdfs: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
            for pdf in pdfs:
                yield pdf[pdf.id == 1]

        df = self.spark.createDataFrame([(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0)], ("id", "v"))
        df.mapInPandas(map, schema=df.schema).collect()

    @unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
    def test_unsupported(self):
        with warnings.catch_warnings(record=True) as warns:
            warnings.simplefilter("always")
            self.exec_pandas_udf_iter_to_iter()
            user_warns = [warn.message for warn in warns if isinstance(warn.message, UserWarning)]
            self.assertTrue(len(user_warns) > 0)
            self.assertTrue(
                "Profiling UDFs with iterators input/output is not supported" in str(user_warns[0])
            )

        with warnings.catch_warnings(record=True) as warns:
            warnings.simplefilter("always")
            self.exec_map()
            user_warns = [warn.message for warn in warns if isinstance(warn.message, UserWarning)]
            self.assertTrue(len(user_warns) > 0)
            self.assertTrue(
                "Profiling UDFs with iterators input/output is not supported" in str(user_warns[0])
            )


class UDFProfiler2TestsMixin:
    @contextmanager
    def trap_stdout(self):
        old_stdout = sys.stdout
        sys.stdout = io = StringIO()
        try:
            yield io
        finally:
            sys.stdout = old_stdout

    @property
    def profile_results(self):
        return self.spark._profiler_collector._perf_profile_results

    def assert_udf_profile_present(self, udf_id, expected_line_count_prefix):
        """
        Assert that the performance profile for a given UDF ID is present and correctly formatted.

        This checks the output of `spark.profile.show()` for the specified UDF ID, ensures that
        it includes a line matching the expected line count prefix and file name, and optionally
        verifies that `spark.profile.render()` produces SVG output when flameprof is available.
        """
        with self.trap_stdout() as io:
            self.spark.profile.show(udf_id, type="perf")
        self.assertIn(f"Profile of UDF<id={udf_id}>", io.getvalue())
        self.assertRegex(
            io.getvalue(),
            f"{expected_line_count_prefix}.*{os.path.basename(inspect.getfile(_do_computation))}",
        )

        if have_flameprof:
            self.assertIn("svg", self.spark.profile.render(udf_id))

    def test_perf_profiler_udf(self):
        _do_computation(self.spark)

        # Without the conf enabled, no profile results are collected.
        self.assertEqual(0, len(self.profile_results), str(list(self.profile_results)))

        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "perf"}):
            _do_computation(self.spark)

        self.assertEqual(3, len(self.profile_results), str(list(self.profile_results)))

        with self.trap_stdout():
            self.spark.profile.show(type="perf")

        with tempfile.TemporaryDirectory(prefix="test_perf_profiler_udf") as d:
            self.spark.profile.dump(d, type="perf")

            for id in self.profile_results:
                self.assert_udf_profile_present(udf_id=id, expected_line_count_prefix=10)
                self.assertTrue(f"udf_{id}_perf.pstats" in os.listdir(d))

    @unittest.skipIf(
        not have_pandas or not have_pyarrow,
        cast(str, pandas_requirement_message or pyarrow_requirement_message),
    )
    def test_perf_profiler_udf_with_arrow(self):
        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "perf"}):
            _do_computation(self.spark, use_arrow=True)

        self.assertEqual(3, len(self.profile_results), str(list(self.profile_results)))

        for id in self.profile_results:
            self.assert_udf_profile_present(udf_id=id, expected_line_count_prefix=10)

    def test_perf_profiler_udf_multiple_actions(self):
        def action(df):
            df.collect()
            df.show()

        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "perf"}):
            _do_computation(self.spark, action=action)

        self.assertEqual(3, len(self.profile_results), str(list(self.profile_results)))

        for id in self.profile_results:
            self.assert_udf_profile_present(udf_id=id, expected_line_count_prefix=20)

    def test_perf_profiler_udf_registered(self):
        @udf("long")
        def add1(x):
            return x + 1

        self.spark.udf.register("add1", add1)

        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "perf"}):
            self.spark.sql("SELECT id, add1(id) add1 FROM range(10)").collect()

        self.assertEqual(1, len(self.profile_results), str(self.profile_results.keys()))

        for id in self.profile_results:
            self.assert_udf_profile_present(udf_id=id, expected_line_count_prefix=10)

    @unittest.skipIf(
        not have_pandas or not have_pyarrow,
        cast(str, pandas_requirement_message or pyarrow_requirement_message),
    )
    def test_perf_profiler_pandas_udf(self):
        @pandas_udf("long")
        def add1(x):
            return x + 1

        @pandas_udf("long")
        def add2(x):
            return x + 2

        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "perf"}):
            df = self.spark.range(10, numPartitions=2).select(
                add1("id"), add2("id"), add1("id"), add2(col("id") + 1)
            )
            df.collect()

        self.assertEqual(3, len(self.profile_results), str(self.profile_results.keys()))

        for id in self.profile_results:
            self.assert_udf_profile_present(udf_id=id, expected_line_count_prefix=2)

    @unittest.skipIf(
        not have_pandas or not have_pyarrow,
        cast(str, pandas_requirement_message or pyarrow_requirement_message),
    )
    def test_perf_profiler_pandas_udf_iterator_not_supported(self):
        import pandas as pd

        @pandas_udf("long")
        def add1(x):
            return x + 1

        @pandas_udf("long")
        def add2(iter: Iterator[pd.Series]) -> Iterator[pd.Series]:
            for s in iter:
                yield s + 2

        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "perf"}):
            df = self.spark.range(10, numPartitions=2).select(
                add1("id"), add2("id"), add1("id"), add2(col("id") + 1)
            )
            df.collect()

        self.assertEqual(1, len(self.profile_results), str(self.profile_results.keys()))

        for id in self.profile_results:
            self.assert_udf_profile_present(udf_id=id, expected_line_count_prefix=2)

    @unittest.skipIf(
        not have_pandas or not have_pyarrow,
        cast(str, pandas_requirement_message or pyarrow_requirement_message),
    )
    def test_perf_profiler_map_in_pandas_not_supported(self):
        df = self.spark.createDataFrame([(1, 21), (2, 30)], ("id", "age"))

        def filter_func(iterator):
            for pdf in iterator:
                yield pdf[pdf.id == 1]

        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "perf"}):
            df.mapInPandas(filter_func, df.schema).show()

        self.assertEqual(0, len(self.profile_results), str(self.profile_results.keys()))

    @unittest.skipIf(
        not have_pandas or not have_pyarrow,
        cast(str, pandas_requirement_message or pyarrow_requirement_message),
    )
    def test_perf_profiler_pandas_udf_window(self):
        # WindowInPandasExec
        import pandas as pd

        @pandas_udf("double")
        def mean_udf(v: pd.Series) -> float:
            return v.mean()

        df = self.spark.createDataFrame(
            [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)], ("id", "v")
        )
        w = Window.partitionBy("id").orderBy("v").rowsBetween(-1, 0)

        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "perf"}):
            df.withColumn("mean_v", mean_udf("v").over(w)).show()

        self.assertEqual(1, len(self.profile_results), str(self.profile_results.keys()))

        for id in self.profile_results:
            self.assert_udf_profile_present(udf_id=id, expected_line_count_prefix=5)

    @unittest.skipIf(
        not have_pandas or not have_pyarrow,
        cast(str, pandas_requirement_message or pyarrow_requirement_message),
    )
    def test_perf_profiler_aggregate_in_pandas(self):
        # AggregateInPandasExec
        import pandas as pd

        @pandas_udf("double")
        def min_udf(v: pd.Series) -> float:
            return v.min()

        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "perf"}):
            df = self.spark.createDataFrame(
                [(2, "Alice"), (3, "Alice"), (5, "Bob"), (10, "Bob")], ["age", "name"]
            )
            df.groupBy(df.name).agg(min_udf(df.age)).show()

        self.assertEqual(1, len(self.profile_results), str(self.profile_results.keys()))

        for id in self.profile_results:
            self.assert_udf_profile_present(udf_id=id, expected_line_count_prefix=2)

    @unittest.skipIf(
        not have_pandas or not have_pyarrow,
        cast(str, pandas_requirement_message or pyarrow_requirement_message),
    )
    def test_perf_profiler_group_apply_in_pandas(self):
        # FlatMapGroupsInBatchExec
        df = self.spark.createDataFrame(
            [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)], ("id", "v")
        )

        def normalize(pdf):
            v = pdf.v
            return pdf.assign(v=(v - v.mean()) / v.std())

        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "perf"}):
            df.groupby("id").applyInPandas(normalize, schema="id long, v double").show()

        self.assertEqual(1, len(self.profile_results), str(self.profile_results.keys()))

        for id in self.profile_results:
            self.assert_udf_profile_present(udf_id=id, expected_line_count_prefix=2)

    @unittest.skipIf(
        not have_pandas or not have_pyarrow,
        cast(str, pandas_requirement_message or pyarrow_requirement_message),
    )
    def test_perf_profiler_cogroup_apply_in_pandas(self):
        # FlatMapCoGroupsInBatchExec
        import pandas as pd

        df1 = self.spark.createDataFrame(
            [(20000101, 1, 1.0), (20000101, 2, 2.0), (20000102, 1, 3.0), (20000102, 2, 4.0)],
            ("time", "id", "v1"),
        )
        df2 = self.spark.createDataFrame(
            [(20000101, 1, "x"), (20000101, 2, "y")], ("time", "id", "v2")
        )

        def asof_join(left, right):
            return pd.merge_asof(left, right, on="time", by="id")

        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "perf"}):
            df1.groupby("id").cogroup(df2.groupby("id")).applyInPandas(
                asof_join, schema="time int, id int, v1 double, v2 string"
            ).show()

        self.assertEqual(1, len(self.profile_results), str(self.profile_results.keys()))

        for id in self.profile_results:
            self.assert_udf_profile_present(udf_id=id, expected_line_count_prefix=2)

    @unittest.skipIf(
        not have_pandas or not have_pyarrow,
        cast(str, pandas_requirement_message or pyarrow_requirement_message),
    )
    def test_perf_profiler_group_apply_in_arrow(self):
        # FlatMapGroupsInBatchExec
        import pyarrow.compute as pc

        df = self.spark.createDataFrame(
            [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)], ("id", "v")
        )

        def normalize(table):
            v = table.column("v")
            norm = pc.divide(pc.subtract(v, pc.mean(v)), pc.stddev(v, ddof=1))
            return table.set_column(1, "v", norm)

        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "perf"}):
            df.groupby("id").applyInArrow(normalize, schema="id long, v double").show()

        self.assertEqual(1, len(self.profile_results), str(self.profile_results.keys()))

        for id in self.profile_results:
            self.assert_udf_profile_present(udf_id=id, expected_line_count_prefix=2)

    @unittest.skipIf(
        not have_pandas or not have_pyarrow,
        cast(str, pandas_requirement_message or pyarrow_requirement_message),
    )
    def test_perf_profiler_cogroup_apply_in_arrow(self):
        import pyarrow as pa

        df1 = self.spark.createDataFrame([(1, 1.0), (2, 2.0), (1, 3.0), (2, 4.0)], ("id", "v1"))
        df2 = self.spark.createDataFrame([(1, "x"), (2, "y")], ("id", "v2"))

        def summarize(left, right):
            return pa.Table.from_pydict({"left": [left.num_rows], "right": [right.num_rows]})

        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "perf"}):
            df1.groupby("id").cogroup(df2.groupby("id")).applyInArrow(
                summarize, schema="left long, right long"
            ).show()

        self.assertEqual(1, len(self.profile_results), str(self.profile_results.keys()))

        for id in self.profile_results:
            self.assert_udf_profile_present(udf_id=id, expected_line_count_prefix=2)

    def test_perf_profiler_render(self):
        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "perf"}):
            _do_computation(self.spark)
        self.assertEqual(3, len(self.profile_results), str(list(self.profile_results)))

        id = list(self.profile_results.keys())[0]

        if have_flameprof:
            self.assertIn("svg", self.spark.profile.render(id))
            self.assertIn("svg", self.spark.profile.render(id, type="perf"))
            self.assertIn("svg", self.spark.profile.render(id, renderer="flameprof"))

        with self.assertRaises(PySparkValueError) as pe:
            self.spark.profile.render(id, type="unknown")

        self.check_error(
            exception=pe.exception,
            errorClass="VALUE_NOT_ALLOWED",
            messageParameters={
                "arg_name": "type",
                "allowed_values": "['perf', 'memory']",
            },
        )

        with self.assertRaises(PySparkValueError) as pe:
            self.spark.profile.render(id, type="memory")

        self.check_error(
            exception=pe.exception,
            errorClass="VALUE_NOT_ALLOWED",
            messageParameters={
                "arg_name": "(type, renderer)",
                "allowed_values": "[('perf', None), ('perf', 'flameprof')]",
            },
        )

        with self.assertRaises(PySparkValueError) as pe:
            self.spark.profile.render(id, renderer="unknown")

        self.check_error(
            exception=pe.exception,
            errorClass="VALUE_NOT_ALLOWED",
            messageParameters={
                "arg_name": "(type, renderer)",
                "allowed_values": "[('perf', None), ('perf', 'flameprof')]",
            },
        )

        with self.trap_stdout() as io:
            self.spark.profile.show(id, type="perf")
        show_value = io.getvalue()

        with self.trap_stdout() as io:
            self.spark.profile.render(
                id, renderer=lambda s: s.sort_stats("time", "cumulative").print_stats()
            )
        render_value = io.getvalue()

        self.assertIn(render_value, show_value)

    def test_perf_profiler_clear(self):
        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "perf"}):
            _do_computation(self.spark)
        self.assertEqual(3, len(self.profile_results), str(list(self.profile_results)))

        for id in self.profile_results:
            self.spark.profile.clear(id)
            self.assertNotIn(id, self.profile_results)
        self.assertEqual(0, len(self.profile_results), str(list(self.profile_results)))

        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "perf"}):
            _do_computation(self.spark)
        self.assertEqual(3, len(self.profile_results), str(list(self.profile_results)))

        self.spark.profile.clear(type="memory")
        self.assertEqual(3, len(self.profile_results), str(list(self.profile_results)))
        self.spark.profile.clear(type="perf")
        self.assertEqual(0, len(self.profile_results), str(list(self.profile_results)))

        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "perf"}):
            _do_computation(self.spark)
        self.assertEqual(3, len(self.profile_results), str(list(self.profile_results)))

        self.spark.profile.clear()
        self.assertEqual(0, len(self.profile_results), str(list(self.profile_results)))


class UDFProfiler2Tests(UDFProfiler2TestsMixin, ReusedSQLTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.spark._profiler_collector._accumulator._value = None


if __name__ == "__main__":
    from pyspark.sql.tests.test_udf_profiler import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
