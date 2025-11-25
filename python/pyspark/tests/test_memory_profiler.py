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
import inspect
import tempfile
import unittest
import warnings
from contextlib import contextmanager
from io import StringIO
from typing import cast, Iterator
from unittest import mock

from pyspark import SparkConf
from pyspark.profiler import has_memory_profiler
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf, udf
from pyspark.sql.window import Window
from pyspark.testing.sqlutils import (
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
    ReusedSQLTestCase,
)
from pyspark.testing.utils import PySparkTestCase


def _do_computation(spark, *, action=lambda df: df.collect(), use_arrow=False):
    @udf("long", useArrow=use_arrow)
    def add1(x):
        return x + 1

    @udf("long", useArrow=use_arrow)
    def add2(x):
        return x + 2

    df = spark.range(10).select(add1("id"), add2("id"), add1("id"), add2(col("id") + 1))
    action(df)


@unittest.skipIf(
    "COVERAGE_PROCESS_START" in os.environ, "Flaky with coverage enabled, skipping for now."
)
@unittest.skipIf(not has_memory_profiler, "Must have memory-profiler installed.")
@unittest.skipIf(not have_pandas, pandas_requirement_message)
class MemoryProfilerTests(PySparkTestCase):
    def setUp(self):
        from pyspark import SparkContext

        self._old_sys_path = list(sys.path)
        class_name = self.__class__.__name__
        conf = SparkConf().set("spark.python.profile.memory", "true")
        self.sc = SparkContext("local[4]", class_name, conf=conf)
        self.spark = SparkSession(sparkContext=self.sc)

    def test_code_map(self):
        from pyspark.profiler import CodeMapForUDF

        code_map = CodeMapForUDF(include_children=False, backend="psutil")

        def f(x):
            return x + 1

        code = f.__code__
        code_map.add(code)
        code_map.add(code)  # no-op, will return directly

        self.assertIn(code, code_map)
        self.assertEqual(len(code_map._toplevel), 1)

    def test_udf_line_profiler(self):
        from pyspark.profiler import UDFLineProfiler

        profiler = UDFLineProfiler()

        def f(x):
            return x + 1

        profiler.add_function(f)
        self.assertTrue(profiler.code_map)

    def test_memory_profiler(self):
        self.exec_python_udf()

        profilers = self.sc.profiler_collector.profilers
        self.assertEqual(1, len(profilers))
        id, profiler, _ = profilers[0]
        stats = profiler.stats()
        self.assertTrue(stats is not None)

        with mock.patch("sys.stdout", new=StringIO()) as fake_out:
            self.sc.show_profiles()
        self.assertTrue("plus_one" in fake_out.getvalue())

        with tempfile.TemporaryDirectory(prefix="test_memory_profiler") as d:
            self.sc.dump_profiles(d)
            self.assertTrue(f"udf_{id}_memory.txt" in os.listdir(d))

    def test_profile_pandas_udf(self):
        udfs = [self.exec_pandas_udf_ser_to_ser, self.exec_pandas_udf_ser_to_scalar]
        udf_names = ["ser_to_ser", "ser_to_scalar"]
        for f, f_name in zip(udfs, udf_names):
            f()
            with mock.patch("sys.stdout", new=StringIO()) as fake_out:
                self.sc.show_profiles()
            self.assertTrue(f_name in fake_out.getvalue())

        with warnings.catch_warnings(record=True) as warns:
            warnings.simplefilter("always")
            self.exec_pandas_udf_iter_to_iter()
            user_warns = [warn.message for warn in warns if isinstance(warn.message, UserWarning)]
            self.assertTrue(len(user_warns) > 0)
            self.assertTrue(
                "Profiling UDFs with iterators input/output is not supported" in str(user_warns[0])
            )

    def test_profile_pandas_function_api(self):
        apis = [self.exec_grouped_map]
        f_names = ["grouped_map"]
        for api, f_name in zip(apis, f_names):
            api()
            with mock.patch("sys.stdout", new=StringIO()) as fake_out:
                self.sc.show_profiles()
            self.assertTrue(f_name in fake_out.getvalue())

        with warnings.catch_warnings(record=True) as warns:
            warnings.simplefilter("always")
            self.exec_map()
            user_warns = [warn.message for warn in warns if isinstance(warn.message, UserWarning)]
            self.assertTrue(len(user_warns) > 0)
            self.assertTrue(
                "Profiling UDFs with iterators input/output is not supported" in str(user_warns[0])
            )

    def exec_python_udf(self):
        @udf("int")
        def plus_one(v):
            return v + 1

        self.spark.range(10).select(plus_one("id")).collect()

    def exec_pandas_udf_ser_to_ser(self):
        import pandas as pd

        @pandas_udf("int")
        def ser_to_ser(ser: pd.Series) -> pd.Series:
            return ser + 1

        self.spark.range(10).select(ser_to_ser("id")).collect()

    def exec_pandas_udf_ser_to_scalar(self):
        import pandas as pd

        @pandas_udf("int")
        def ser_to_scalar(ser: pd.Series) -> float:
            return ser.median()

        self.spark.range(10).select(ser_to_scalar("id")).collect()

    # Unsupported
    def exec_pandas_udf_iter_to_iter(self):
        import pandas as pd

        @pandas_udf("int")
        def iter_to_iter(batch_ser: Iterator[pd.Series]) -> Iterator[pd.Series]:
            for ser in batch_ser:
                yield ser + 1

        self.spark.range(10).select(iter_to_iter("id")).collect()

    def exec_grouped_map(self):
        import pandas as pd

        def grouped_map(pdf: pd.DataFrame) -> pd.DataFrame:
            return pdf.assign(v=pdf.v - pdf.v.mean())

        df = self.spark.createDataFrame([(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0)], ("id", "v"))
        df.groupby("id").applyInPandas(grouped_map, schema="id long, v double").collect()

    # Unsupported
    def exec_map(self):
        import pandas as pd

        def map(pdfs: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
            for pdf in pdfs:
                yield pdf[pdf.id == 1]

        df = self.spark.createDataFrame([(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0)], ("id", "v"))
        df.mapInPandas(map, schema=df.schema).collect()


@unittest.skipIf(
    "COVERAGE_PROCESS_START" in os.environ, "Fails with coverage enabled, skipping for now."
)
@unittest.skipIf(not has_memory_profiler, "Must have memory-profiler installed.")
class MemoryProfiler2TestsMixin:
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
        return self.spark._profiler_collector._memory_profile_results

    @property
    def perf_profile_results(self):
        return self.spark._profiler_collector._perf_profile_results

    def test_memory_profiler_udf(self):
        _do_computation(self.spark)

        # Without the conf enabled, no profile results are collected.
        self.assertEqual(0, len(self.profile_results), str(list(self.profile_results)))

        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "memory"}):
            _do_computation(self.spark)

        self.assertEqual(3, len(self.profile_results), str(list(self.profile_results)))

        with self.trap_stdout() as io_all:
            self.spark.profile.show(type="memory")

        with tempfile.TemporaryDirectory(prefix="test_memory_profiler_udf") as d:
            self.spark.profile.dump(d, type="memory")

            for id in self.profile_results:
                self.assertIn(f"Profile of UDF<id={id}>", io_all.getvalue())

                with self.trap_stdout() as io:
                    self.spark.profile.show(id, type="memory")

                self.assertIn(f"Profile of UDF<id={id}>", io.getvalue())
                self.assertRegex(
                    io.getvalue(), f"Filename.*{os.path.basename(inspect.getfile(_do_computation))}"
                )
                self.assertTrue(f"udf_{id}_memory.txt" in os.listdir(d))

    @unittest.skipIf(
        not have_pandas or not have_pyarrow,
        cast(str, pandas_requirement_message or pyarrow_requirement_message),
    )
    def test_memory_profiler_udf_with_arrow(self):
        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "memory"}):
            _do_computation(self.spark, use_arrow=True)

        self.assertEqual(3, len(self.profile_results), str(list(self.profile_results)))

        for id in self.profile_results:
            with self.trap_stdout() as io:
                self.spark.profile.show(id, type="memory")

            self.assertIn(f"Profile of UDF<id={id}>", io.getvalue())
            self.assertRegex(
                io.getvalue(), f"Filename.*{os.path.basename(inspect.getfile(_do_computation))}"
            )

    def test_memory_profiler_udf_multiple_actions(self):
        def action(df):
            df.collect()
            df.show()

        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "memory"}):
            _do_computation(self.spark, action=action)

        self.assertEqual(3, len(self.profile_results), str(list(self.profile_results)))

        for id in self.profile_results:
            with self.trap_stdout() as io:
                self.spark.profile.show(id, type="memory")

            self.assertIn(f"Profile of UDF<id={id}>", io.getvalue())
            self.assertRegex(
                io.getvalue(), f"Filename.*{os.path.basename(inspect.getfile(_do_computation))}"
            )

    def test_memory_profiler_udf_registered(self):
        @udf("long")
        def add1(x):
            return x + 1

        self.spark.udf.register("add1", add1)

        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "memory"}):
            self.spark.sql("SELECT id, add1(id) add1 FROM range(10)").collect()

        self.assertEqual(1, len(self.profile_results), str(self.profile_results.keys()))

        for id in self.profile_results:
            with self.trap_stdout() as io:
                self.spark.profile.show(id, type="memory")

            self.assertIn(f"Profile of UDF<id={id}>", io.getvalue())
            self.assertRegex(
                io.getvalue(), f"Filename.*{os.path.basename(inspect.getfile(_do_computation))}"
            )

    @unittest.skipIf(
        not have_pandas or not have_pyarrow,
        cast(str, pandas_requirement_message or pyarrow_requirement_message),
    )
    def test_memory_profiler_pandas_udf(self):
        @pandas_udf("long")
        def add1(x):
            return x + 1

        @pandas_udf("long")
        def add2(x):
            return x + 2

        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "memory"}):
            df = self.spark.range(10, numPartitions=2).select(
                add1("id"), add2("id"), add1("id"), add2(col("id") + 1)
            )
            df.collect()

        self.assertEqual(3, len(self.profile_results), str(self.profile_results.keys()))

        for id in self.profile_results:
            with self.trap_stdout() as io:
                self.spark.profile.show(id, type="memory")

            self.assertIn(f"Profile of UDF<id={id}>", io.getvalue())
            self.assertRegex(
                io.getvalue(), f"Filename.*{os.path.basename(inspect.getfile(_do_computation))}"
            )

    @unittest.skipIf(
        not have_pandas or not have_pyarrow,
        cast(str, pandas_requirement_message or pyarrow_requirement_message),
    )
    def test_memory_profiler_pandas_udf_iterator_not_supported(self):
        import pandas as pd

        @pandas_udf("long")
        def add1(x):
            return x + 1

        @pandas_udf("long")
        def add2(iter: Iterator[pd.Series]) -> Iterator[pd.Series]:
            for s in iter:
                yield s + 2

        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "memory"}):
            df = self.spark.range(10, numPartitions=2).select(
                add1("id"), add2("id"), add1("id"), add2(col("id") + 1)
            )
            df.collect()

        self.assertEqual(1, len(self.profile_results), str(self.profile_results.keys()))

        for id in self.profile_results:
            with self.trap_stdout() as io:
                self.spark.profile.show(id, type="memory")

            self.assertIn(f"Profile of UDF<id={id}>", io.getvalue())
            self.assertRegex(
                io.getvalue(), f"Filename.*{os.path.basename(inspect.getfile(_do_computation))}"
            )

    @unittest.skipIf(
        not have_pandas or not have_pyarrow,
        cast(str, pandas_requirement_message or pyarrow_requirement_message),
    )
    def test_memory_profiler_map_in_pandas_not_supported(self):
        df = self.spark.createDataFrame([(1, 21), (2, 30)], ("id", "age"))

        def filter_func(iterator):
            for pdf in iterator:
                yield pdf[pdf.id == 1]

        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "memory"}):
            df.mapInPandas(filter_func, df.schema).show()

        self.assertEqual(0, len(self.profile_results), str(self.profile_results.keys()))

    @unittest.skipIf(
        not have_pandas or not have_pyarrow,
        cast(str, pandas_requirement_message or pyarrow_requirement_message),
    )
    def test_memory_profiler_pandas_udf_window(self):
        # WindowInPandasExec
        import pandas as pd

        @pandas_udf("double")
        def mean_udf(v: pd.Series) -> float:
            return v.mean()

        df = self.spark.createDataFrame(
            [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)], ("id", "v")
        )
        w = Window.partitionBy("id").orderBy("v").rowsBetween(-1, 0)

        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "memory"}):
            df.withColumn("mean_v", mean_udf("v").over(w)).show()

        self.assertEqual(1, len(self.profile_results), str(self.profile_results.keys()))

        for id in self.profile_results:
            with self.trap_stdout() as io:
                self.spark.profile.show(id, type="memory")

            self.assertIn(f"Profile of UDF<id={id}>", io.getvalue())
            self.assertRegex(
                io.getvalue(), f"Filename.*{os.path.basename(inspect.getfile(_do_computation))}"
            )

    @unittest.skipIf(
        not have_pandas or not have_pyarrow,
        cast(str, pandas_requirement_message or pyarrow_requirement_message),
    )
    def test_memory_profiler_aggregate_in_pandas(self):
        # AggregateInPandasExec
        import pandas as pd

        @pandas_udf("double")
        def min_udf(v: pd.Series) -> float:
            return v.min()

        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "memory"}):
            df = self.spark.createDataFrame(
                [(2, "Alice"), (3, "Alice"), (5, "Bob"), (10, "Bob")], ["age", "name"]
            )
            df.groupBy(df.name).agg(min_udf(df.age)).show()

        self.assertEqual(1, len(self.profile_results), str(self.profile_results.keys()))

        for id in self.profile_results:
            with self.trap_stdout() as io:
                self.spark.profile.show(id, type="memory")

            self.assertIn(f"Profile of UDF<id={id}>", io.getvalue())
            self.assertRegex(
                io.getvalue(), f"Filename.*{os.path.basename(inspect.getfile(_do_computation))}"
            )

    @unittest.skipIf(
        not have_pandas or not have_pyarrow,
        cast(str, pandas_requirement_message or pyarrow_requirement_message),
    )
    def test_memory_profiler_group_apply_in_pandas(self):
        # FlatMapGroupsInBatchExec
        df = self.spark.createDataFrame(
            [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)], ("id", "v")
        )

        def normalize(pdf):
            v = pdf.v
            return pdf.assign(v=(v - v.mean()) / v.std())

        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "memory"}):
            df.groupby("id").applyInPandas(normalize, schema="id long, v double").show()

        self.assertEqual(1, len(self.profile_results), str(self.profile_results.keys()))

        for id in self.profile_results:
            with self.trap_stdout() as io:
                self.spark.profile.show(id, type="memory")

            self.assertIn(f"Profile of UDF<id={id}>", io.getvalue())
            self.assertRegex(
                io.getvalue(), f"Filename.*{os.path.basename(inspect.getfile(_do_computation))}"
            )

    @unittest.skipIf(
        not have_pandas or not have_pyarrow,
        cast(str, pandas_requirement_message or pyarrow_requirement_message),
    )
    def test_memory_profiler_cogroup_apply_in_pandas(self):
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

        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "memory"}):
            df1.groupby("id").cogroup(df2.groupby("id")).applyInPandas(
                asof_join, schema="time int, id int, v1 double, v2 string"
            ).show()

        self.assertEqual(1, len(self.profile_results), str(self.profile_results.keys()))

        for id in self.profile_results:
            with self.trap_stdout() as io:
                self.spark.profile.show(id, type="memory")

            self.assertIn(f"Profile of UDF<id={id}>", io.getvalue())
            self.assertRegex(
                io.getvalue(), f"Filename.*{os.path.basename(inspect.getfile(_do_computation))}"
            )

    @unittest.skipIf(
        not have_pandas or not have_pyarrow,
        cast(str, pandas_requirement_message or pyarrow_requirement_message),
    )
    def test_memory_profiler_group_apply_in_arrow(self):
        # FlatMapGroupsInBatchExec
        import pyarrow.compute as pc

        df = self.spark.createDataFrame(
            [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)], ("id", "v")
        )

        def normalize(table):
            v = table.column("v")
            norm = pc.divide(pc.subtract(v, pc.mean(v)), pc.stddev(v, ddof=1))
            return table.set_column(1, "v", norm)

        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "memory"}):
            df.groupby("id").applyInArrow(normalize, schema="id long, v double").show()

        self.assertEqual(1, len(self.profile_results), str(self.profile_results.keys()))

        for id in self.profile_results:
            with self.trap_stdout() as io:
                self.spark.profile.show(id, type="memory")

            self.assertIn(f"Profile of UDF<id={id}>", io.getvalue())
            self.assertRegex(
                io.getvalue(), f"Filename.*{os.path.basename(inspect.getfile(_do_computation))}"
            )

    @unittest.skipIf(
        not have_pandas or not have_pyarrow,
        cast(str, pandas_requirement_message or pyarrow_requirement_message),
    )
    def test_memory_profiler_cogroup_apply_in_arrow(self):
        import pyarrow as pa

        df1 = self.spark.createDataFrame([(1, 1.0), (2, 2.0), (1, 3.0), (2, 4.0)], ("id", "v1"))
        df2 = self.spark.createDataFrame([(1, "x"), (2, "y")], ("id", "v2"))

        def summarize(left, right):
            return pa.Table.from_pydict({"left": [left.num_rows], "right": [right.num_rows]})

        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "memory"}):
            df1.groupby("id").cogroup(df2.groupby("id")).applyInArrow(
                summarize, schema="left long, right long"
            ).show()

        self.assertEqual(1, len(self.profile_results), str(self.profile_results.keys()))

        for id in self.profile_results:
            with self.trap_stdout() as io:
                self.spark.profile.show(id, type="memory")

            self.assertIn(f"Profile of UDF<id={id}>", io.getvalue())
            self.assertRegex(
                io.getvalue(), f"Filename.*{os.path.basename(inspect.getfile(_do_computation))}"
            )

    def test_memory_profiler_clear(self):
        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "memory"}):
            _do_computation(self.spark)
        self.assertEqual(3, len(self.profile_results), str(list(self.profile_results)))

        for id in list(self.profile_results.keys()):
            self.spark.profile.clear(id)
            self.assertNotIn(id, self.profile_results)
        self.assertEqual(0, len(self.profile_results), str(list(self.profile_results)))

        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "memory"}):
            _do_computation(self.spark)
        self.assertEqual(3, len(self.profile_results), str(list(self.profile_results)))

        self.spark.profile.clear(type="perf")
        self.assertEqual(3, len(self.profile_results), str(list(self.profile_results)))
        self.spark.profile.clear(type="memory")
        self.assertEqual(0, len(self.profile_results), str(list(self.profile_results)))

        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "memory"}):
            _do_computation(self.spark)
        self.assertEqual(3, len(self.profile_results), str(list(self.profile_results)))

        self.spark.profile.clear()
        self.assertEqual(0, len(self.profile_results), str(list(self.profile_results)))

    def test_profilers_clear(self):
        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "memory"}):
            _do_computation(self.spark)
        with self.sql_conf({"spark.sql.pyspark.udf.profiler": "perf"}):
            _do_computation(self.spark)

        self.assertEqual(3, len(self.profile_results), str(list(self.profile_results)))

        # clear a specific memory profile
        some_id = next(iter(self.profile_results))
        self.spark.profile.clear(some_id, type="memory")
        self.assertEqual(2, len(self.profile_results), str(list(self.profile_results)))
        self.assertEqual(3, len(self.perf_profile_results), str(list(self.perf_profile_results)))

        # clear a specific perf profile
        some_id = next(iter(self.perf_profile_results))
        self.spark.profile.clear(some_id, type="perf")
        self.assertEqual(2, len(self.perf_profile_results), str(list(self.perf_profile_results)))
        self.assertEqual(2, len(self.profile_results), str(list(self.profile_results)))

        # clear all memory profiles
        self.spark.profile.clear(type="memory")
        self.assertEqual(0, len(self.profile_results), str(list(self.profile_results)))
        self.assertEqual(2, len(self.perf_profile_results), str(list(self.perf_profile_results)))

        # clear all perf profiles
        self.spark.profile.clear(type="perf")
        self.assertEqual(0, len(self.perf_profile_results), str(list(self.perf_profile_results)))


class MemoryProfiler2Tests(MemoryProfiler2TestsMixin, ReusedSQLTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.spark._profiler_collector._accumulator._value = None


if __name__ == "__main__":
    from pyspark.tests.test_memory_profiler import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
