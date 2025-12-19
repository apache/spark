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

import unittest
from typing import Iterator, Tuple

from pyspark.sql import functions as sf
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
from pyspark.sql.types import LongType
from pyspark.testing.utils import (
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.util import PythonEvalType


@unittest.skipIf(
    not have_pandas or not have_pyarrow,
    pandas_requirement_message or pyarrow_requirement_message,
)
class UnifiedUDFTestsMixin:
    def test_scalar_pandas_udf(self):
        import pandas as pd

        @udf(returnType=LongType())
        def pd_add1(ser: pd.Series) -> pd.Series:
            assert isinstance(ser, pd.Series)
            return ser + 1

        self.assertEqual(pd_add1.evalType, PythonEvalType.SQL_SCALAR_PANDAS_UDF)

        df = self.spark.range(0, 10)
        expected = df.select((df.id + 1).alias("res")).collect()

        result1 = df.select(pd_add1("id").alias("res")).collect()
        self.assertEqual(result1, expected)

        with self.temp_func("pd_add1"):
            self.spark.udf.register("pd_add1", pd_add1)
            result2 = self.spark.sql("SELECT pd_add1(id) AS res FROM range(0, 10)").collect()
            self.assertEqual(result2, expected)

    def test_scalar_pandas_udf_II(self):
        import pandas as pd

        @udf(returnType=LongType())
        def pd_add(ser1: pd.Series, ser2: pd.Series) -> pd.Series:
            assert isinstance(ser1, pd.Series)
            assert isinstance(ser2, pd.Series)
            return ser1 + ser2

        self.assertEqual(pd_add.evalType, PythonEvalType.SQL_SCALAR_PANDAS_UDF)

        df = self.spark.range(0, 10)
        expected = df.select((df.id + df.id).alias("res")).collect()

        result1 = df.select(pd_add("id", "id").alias("res")).collect()
        self.assertEqual(result1, expected)

        with self.temp_func("pd_add"):
            self.spark.udf.register("pd_add", pd_add)
            result2 = self.spark.sql("SELECT pd_add(id, id) AS res FROM range(0, 10)").collect()
            self.assertEqual(result2, expected)

    def test_scalar_pandas_iter_udf(self):
        import pandas as pd

        @udf(returnType=LongType())
        def pd_add1_iter(it: Iterator[pd.Series]) -> Iterator[pd.Series]:
            for ser in it:
                assert isinstance(ser, pd.Series)
                yield ser + 1

        self.assertEqual(pd_add1_iter.evalType, PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF)

        df = self.spark.range(0, 10)
        expected = df.select((df.id + 1).alias("res")).collect()

        result1 = df.select(pd_add1_iter("id").alias("res")).collect()
        self.assertEqual(result1, expected)

        with self.temp_func("pd_add1_iter"):
            self.spark.udf.register("pd_add1_iter", pd_add1_iter)
            result2 = self.spark.sql("SELECT pd_add1_iter(id) AS res FROM range(0, 10)").collect()
            self.assertEqual(result2, expected)

    def test_scalar_pandas_iter_udf_II(self):
        import pandas as pd

        @udf(returnType=LongType())
        def pd_add_iter(it: Iterator[Tuple[pd.Series, pd.Series]]) -> Iterator[pd.Series]:
            for ser1, ser2 in it:
                assert isinstance(ser1, pd.Series)
                assert isinstance(ser2, pd.Series)
                yield ser1 + ser2

        self.assertEqual(pd_add_iter.evalType, PythonEvalType.SQL_SCALAR_PANDAS_ITER_UDF)

        df = self.spark.range(0, 10)
        expected = df.select((df.id + df.id).alias("res")).collect()

        result1 = df.select(pd_add_iter("id", "id").alias("res")).collect()
        self.assertEqual(result1, expected)

        with self.temp_func("pd_add_iter"):
            self.spark.udf.register("pd_add_iter", pd_add_iter)
            result2 = self.spark.sql(
                "SELECT pd_add_iter(id, id) AS res FROM range(0, 10)"
            ).collect()
            self.assertEqual(result2, expected)

    def test_grouped_agg_pandas_udf(self):
        import pandas as pd

        @udf(returnType=LongType())
        def pd_max(ser: pd.Series) -> int:
            assert isinstance(ser, pd.Series)
            return ser.max()

        self.assertEqual(pd_max.evalType, PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF)

        df = self.spark.range(0, 10)
        expected = df.select(sf.max("id").alias("res")).collect()

        result1 = df.select(pd_max("id").alias("res")).collect()
        self.assertEqual(result1, expected)

        with self.temp_func("pd_max"):
            self.spark.udf.register("pd_max", pd_max)
            result2 = self.spark.sql("SELECT pd_max(id) AS res FROM range(0, 10)").collect()
            self.assertEqual(result2, expected)

    def test_window_agg_pandas_udf(self):
        import pandas as pd

        @udf(returnType=LongType())
        def pd_win_max(ser: pd.Series) -> int:
            assert isinstance(ser, pd.Series)
            return ser.max()

        self.assertEqual(pd_win_max.evalType, PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF)

        df = (
            self.spark.range(10)
            .withColumn("vs", sf.array([sf.lit(i * 1.0) + sf.col("id") for i in range(20, 30)]))
            .withColumn("v", sf.explode("vs"))
            .drop("vs")
            .withColumn("w", sf.lit(1.0))
        )

        w = (
            Window.partitionBy("id")
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            .orderBy("v")
        )

        expected = df.withColumn("res", sf.max("v").over(w)).collect()

        result1 = df.withColumn("res", pd_win_max("v").over(w)).collect()
        self.assertEqual(result1, expected)

        with self.tempView("pd_tbl"), self.temp_func("pd_win_max"):
            df.createOrReplaceTempView("pd_tbl")
            self.spark.udf.register("pd_win_max", pd_win_max)

            result2 = self.spark.sql(
                """
                SELECT *, pd_win_max(v) OVER (
                    PARTITION BY id
                    ORDER BY v
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS res FROM pd_tbl
                """
            ).collect()
            self.assertEqual(result2, expected)

    def test_scalar_arrow_udf(self):
        import pyarrow as pa

        @udf(returnType=LongType())
        def pa_add1(arr: pa.Array) -> pa.Array:
            assert isinstance(arr, pa.Array)
            return pa.compute.add(arr, 1)

        self.assertEqual(pa_add1.evalType, PythonEvalType.SQL_SCALAR_ARROW_UDF)

        df = self.spark.range(0, 10)
        expected = df.select((df.id + 1).alias("res")).collect()

        result1 = df.select(pa_add1("id").alias("res")).collect()
        self.assertEqual(result1, expected)

        with self.temp_func("pa_add1"):
            self.spark.udf.register("pa_add1", pa_add1)
            result2 = self.spark.sql("SELECT pa_add1(id) AS res FROM range(0, 10)").collect()
            self.assertEqual(result2, expected)

    def test_scalar_arrow_udf_II(self):
        import pyarrow as pa

        @udf(returnType=LongType())
        def pa_add(arr1: pa.Array, arr2: pa.Array) -> pa.Array:
            assert isinstance(arr1, pa.Array)
            assert isinstance(arr2, pa.Array)
            return pa.compute.add(arr1, arr2)

        self.assertEqual(pa_add.evalType, PythonEvalType.SQL_SCALAR_ARROW_UDF)

        df = self.spark.range(0, 10)
        expected = df.select((df.id + df.id).alias("res")).collect()

        result1 = df.select(pa_add("id", "id").alias("res")).collect()
        self.assertEqual(result1, expected)

        with self.temp_func("pa_add"):
            self.spark.udf.register("pa_add", pa_add)
            result2 = self.spark.sql("SELECT pa_add(id, id) AS res FROM range(0, 10)").collect()
            self.assertEqual(result2, expected)

    def test_scalar_arrow_iter_udf(self):
        import pyarrow as pa

        @udf(returnType=LongType())
        def pa_add1_iter(it: Iterator[pa.Array]) -> Iterator[pa.Array]:
            for arr in it:
                assert isinstance(arr, pa.Array)
                yield pa.compute.add(arr, 1)

        self.assertEqual(pa_add1_iter.evalType, PythonEvalType.SQL_SCALAR_ARROW_ITER_UDF)

        df = self.spark.range(0, 10)
        expected = df.select((df.id + 1).alias("res")).collect()

        result1 = df.select(pa_add1_iter("id").alias("res")).collect()
        self.assertEqual(result1, expected)

        with self.temp_func("pa_add1_iter"):
            self.spark.udf.register("pa_add1_iter", pa_add1_iter)
            result2 = self.spark.sql("SELECT pa_add1_iter(id) AS res FROM range(0, 10)").collect()
            self.assertEqual(result2, expected)

    def test_scalar_arrow_iter_udf_II(self):
        import pyarrow as pa

        @udf(returnType=LongType())
        def pa_add_iter(it: Iterator[Tuple[pa.Array, pa.Array]]) -> Iterator[pa.Array]:
            for arr1, arr2 in it:
                assert isinstance(arr1, pa.Array)
                assert isinstance(arr2, pa.Array)
                yield pa.compute.add(arr1, arr2)

        self.assertEqual(pa_add_iter.evalType, PythonEvalType.SQL_SCALAR_ARROW_ITER_UDF)

        df = self.spark.range(0, 10)
        expected = df.select((df.id + df.id).alias("res")).collect()

        result1 = df.select(pa_add_iter("id", "id").alias("res")).collect()
        self.assertEqual(result1, expected)

        with self.temp_func("pa_add_iter"):
            self.spark.udf.register("pa_add_iter", pa_add_iter)
            result2 = self.spark.sql(
                "SELECT pa_add_iter(id, id) AS res FROM range(0, 10)"
            ).collect()
            self.assertEqual(result2, expected)

    def test_grouped_agg_arrow_udf(self):
        import pyarrow as pa

        @udf(returnType=LongType())
        def pa_max(arr: pa.Array) -> pa.Scalar:
            assert isinstance(arr, pa.Array)
            return pa.compute.max(arr)

        self.assertEqual(pa_max.evalType, PythonEvalType.SQL_GROUPED_AGG_ARROW_UDF)

        df = self.spark.range(0, 10)
        expected = df.select(sf.max("id").alias("res")).collect()

        result1 = df.select(pa_max("id").alias("res")).collect()
        self.assertEqual(result1, expected)

        with self.temp_func("pa_max"):
            self.spark.udf.register("pa_max", pa_max)
            result2 = self.spark.sql("SELECT pa_max(id) AS res FROM range(0, 10)").collect()
            self.assertEqual(result2, expected)

    def test_grouped_agg_arrow_iter_udf(self):
        import pyarrow as pa

        @udf(returnType=LongType())
        def pa_sum_iter(it: Iterator[pa.Array]) -> int:
            total = 0
            for arr in it:
                total += pa.compute.sum(arr).as_py()
            return total

        self.assertEqual(pa_sum_iter.evalType, PythonEvalType.SQL_GROUPED_AGG_ARROW_ITER_UDF)

        df = self.spark.range(0, 10)
        expected = df.select(sf.sum("id").alias("res")).collect()

        result1 = df.select(pa_sum_iter("id").alias("res")).collect()
        self.assertEqual(result1, expected)

        with self.temp_func("pa_sum_iter"):
            self.spark.udf.register("pa_sum_iter", pa_sum_iter)
            result2 = self.spark.sql("SELECT pa_sum_iter(id) AS res FROM range(0, 10)").collect()
            self.assertEqual(result2, expected)

    def test_window_agg_arrow_udf(self):
        import pyarrow as pa

        @udf(returnType=LongType())
        def pa_win_max(arr: pa.Array) -> pa.Scalar:
            assert isinstance(arr, pa.Array)
            return pa.compute.max(arr)

        self.assertEqual(pa_win_max.evalType, PythonEvalType.SQL_GROUPED_AGG_ARROW_UDF)

        df = (
            self.spark.range(10)
            .withColumn("vs", sf.array([sf.lit(i * 1.0) + sf.col("id") for i in range(20, 30)]))
            .withColumn("v", sf.explode("vs"))
            .drop("vs")
            .withColumn("w", sf.lit(1.0))
        )

        w = (
            Window.partitionBy("id")
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            .orderBy("v")
        )

        expected = df.withColumn("mean_v", sf.max("v").over(w)).collect()

        result1 = df.withColumn("mean_v", pa_win_max("v").over(w)).collect()
        self.assertEqual(result1, expected)

        with self.tempView("pa_tbl"), self.temp_func("pa_win_max"):
            df.createOrReplaceTempView("pa_tbl")
            self.spark.udf.register("pa_win_max", pa_win_max)

            result2 = self.spark.sql(
                """
                SELECT *, pa_win_max(v) OVER (
                    PARTITION BY id
                    ORDER BY v
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS res FROM pa_tbl
                """
            ).collect()
            self.assertEqual(result2, expected)

    def test_regular_python_udf(self):
        import pandas as pd
        import pyarrow as pa

        with self.sql_conf({"spark.sql.execution.pythonUDF.arrow.enabled": False}):

            @udf(returnType=LongType())
            def f1(x):
                return x + 1

            @udf(returnType=LongType())
            def f2(x: int) -> int:
                return x + 1

            # Cannot infer a vectorized UDF type
            @udf(returnType=LongType())
            def f3(x: int) -> pd.Series:
                return x + 1

            # Cannot infer a vectorized UDF type
            @udf(returnType=LongType())
            def f4(x: int) -> pa.Array:
                return x + 1

            # useArrow is explicitly set to false
            @udf(returnType=LongType(), useArrow=False)
            def f5(x: pd.Series) -> pd.Series:
                return x + 1

            # useArrow is explicitly set to false
            @udf(returnType=LongType(), useArrow=False)
            def f6(x: pa.Array) -> pa.Array:
                return x + 1

            expected = self.spark.range(10).select((sf.col("id") + 1).alias("res")).collect()
            for f in [f1, f2, f3, f4, f5, f6]:
                self.assertEqual(f.evalType, PythonEvalType.SQL_BATCHED_UDF)
                result = self.spark.range(10).select(f("id").alias("res")).collect()
                self.assertEqual(result, expected)

    def test_arrow_optimized_python_udf(self):
        import pandas as pd
        import pyarrow as pa

        @udf(returnType=LongType(), useArrow=True)
        def f1(x):
            return x + 1

        @udf(returnType=LongType(), useArrow=True)
        def f2(x: int) -> int:
            return x + 1

        # useArrow is explicitly set
        @udf(returnType=LongType(), useArrow=True)
        def f3(x: pd.Series) -> pd.Series:
            return x + 1

        # useArrow is explicitly set
        @udf(returnType=LongType(), useArrow=True)
        def f4(x: pa.Array) -> pa.Array:
            return x + 1

        expected = self.spark.range(10).select((sf.col("id") + 1).alias("res")).collect()
        for f in [f1, f2, f3, f4]:
            self.assertEqual(f.evalType, PythonEvalType.SQL_ARROW_BATCHED_UDF)
            result = self.spark.range(10).select(f("id").alias("res")).collect()
            self.assertEqual(result, expected)

    def test_0_args(self):
        with self.sql_conf({"spark.sql.execution.pythonUDF.arrow.enabled": False}):

            @udf()
            def f1() -> int:
                return 1

            @udf(returnType=LongType())
            def f2() -> int:
                return 1

            for f in [f1, f2]:
                self.assertEqual(f.evalType, PythonEvalType.SQL_BATCHED_UDF)
                result = self.spark.range(10).select(f().alias("res")).collect()
                self.assertEqual(len(result), 10)


class UnifiedUDFTests(UnifiedUDFTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.sql.tests.test_unified_udf import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
