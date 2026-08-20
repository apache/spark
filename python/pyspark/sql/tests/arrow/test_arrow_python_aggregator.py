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
from decimal import Decimal

from pyspark.errors import AnalysisException, PySparkValueError
from pyspark.sql import functions as sf
from pyspark.sql.window import Window
from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    LongType,
    StructField,
    StructType,
)
from pyspark.util import PythonEvalType
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.testing.utils import (
    have_pyarrow,
    pyarrow_requirement_message,
)


if have_pyarrow:
    from pyspark.sql.aggregator import Aggregator
    from pyspark.sql.functions import udaf

    class Mean(Aggregator):
        @property
        def bufferSchema(self):
            return StructType([StructField("sum", DoubleType()), StructField("count", LongType())])

        @property
        def outputType(self):
            return DoubleType()

        def zero(self):
            return (0.0, 0)

        def reduce(self, buffer, value):
            (v,) = value
            if v is None:  # ignore nulls, like SQL avg
                return buffer
            return (buffer[0] + v, buffer[1] + 1)

        def merge(self, b1, b2):
            return (b1[0] + b2[0], b1[1] + b2[1])

        def finish(self, buffer):
            return buffer[0] / buffer[1] if buffer[1] else None

    class DecimalSum(Aggregator):
        # Non-trivial output/buffer type: the result column and the intermediate buffer are both
        # DecimalType, exercising explicit Arrow typing of the emitted arrays (a bare
        # ``pa.array([Decimal(...)])`` would infer a decimal type whose precision/scale need not
        # match the declared one).
        @property
        def bufferSchema(self):
            return StructType([StructField("total", DecimalType(20, 4))])

        @property
        def outputType(self):
            return DecimalType(20, 4)

        def zero(self):
            return (Decimal(0),)

        def reduce(self, buffer, value):
            (v,) = value
            return buffer if v is None else (buffer[0] + Decimal(str(v)),)

        def merge(self, b1, b2):
            return (b1[0] + b2[0],)

        def finish(self, buffer):
            return buffer[0]

    class SumSquares(Aggregator):
        @property
        def bufferSchema(self):
            return StructType([StructField("sumsq", DoubleType())])

        @property
        def outputType(self):
            return DoubleType()

        def zero(self):
            return (0.0,)

        def reduce(self, buffer, value):
            (v,) = value
            return (buffer[0] + float(v) * float(v),)

        def merge(self, b1, b2):
            return (b1[0] + b2[0],)

        def finish(self, buffer):
            return buffer[0]


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowPythonAggregatorTestsMixin:
    def _data(self):
        # 100 rows across 5 keys; repartition so each key is split across partitions,
        # exercising map-side PARTIAL combine + post-shuffle FINAL merge.
        return (
            self.spark.range(0, 100)
            .select((sf.col("id") % 5).alias("k"), sf.col("id").cast("double").alias("v"))
            .repartition(4, sf.col("v") % 3)
        )

    def test_incremental_aggregator_matches_builtin_mean(self):
        df = self._data()
        result = df.groupBy("k").agg(udaf(Mean())(sf.col("v")).alias("m")).orderBy("k").collect()
        expected = df.groupBy("k").agg(sf.avg("v").alias("m")).orderBy("k").collect()
        got = {r["k"]: r["m"] for r in result}
        exp = {r["k"]: r["m"] for r in expected}
        self.assertEqual(got, exp)

    def test_incremental_aggregator_no_group(self):
        df = self._data()
        result = df.agg(udaf(Mean())(sf.col("v")).alias("m")).collect()
        expected = df.agg(sf.avg("v").alias("m")).collect()
        self.assertAlmostEqual(result[0]["m"], expected[0]["m"], places=6)

    def test_incremental_aggregator_empty_global_input(self):
        # A global aggregation over empty input must still return one identity row: finish(zero).
        empty = self._data().limit(0)
        result = empty.agg(udaf(Mean())(sf.col("v")).alias("m")).collect()
        expected = empty.agg(sf.avg("v").alias("m")).collect()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["m"], expected[0]["m"])

    def test_incremental_aggregator_custom_buffer(self):
        df = self._data()
        result = (
            df.groupBy("k").agg(udaf(SumSquares())(sf.col("v")).alias("s")).orderBy("k").collect()
        )
        expected = (
            df.groupBy("k").agg(sf.sum(sf.col("v") * sf.col("v")).alias("s")).orderBy("k").collect()
        )
        got = {r["k"]: r["s"] for r in result}
        exp = {r["k"]: r["s"] for r in expected}
        for k in exp:
            self.assertAlmostEqual(got[k], exp[k], places=6)

    def test_incremental_aggregator_decimal_output(self):
        # Non-trivial output/buffer type (DecimalType), crossing the shuffle as a decimal buffer
        # and emitted as a decimal result -- guards the explicit Arrow typing of both stages.
        df = self._data()
        result = (
            df.groupBy("k").agg(udaf(DecimalSum())(sf.col("v")).alias("s")).orderBy("k").collect()
        )
        expected = df.groupBy("k").agg(sf.sum("v").alias("s")).orderBy("k").collect()
        got = {r["k"]: r["s"] for r in result}
        exp = {r["k"]: r["s"] for r in expected}
        for k in exp:
            self.assertIsInstance(got[k], Decimal)
            self.assertAlmostEqual(float(got[k]), exp[k], places=4)

    def test_incremental_aggregator_null_inputs(self):
        # reduce must tolerate null input values; the null-skipping Mean should match SQL avg,
        # including a group whose values are all null (identity buffer -> finish returns None).
        df = self.spark.createDataFrame(
            [("a", 1.0), ("a", None), ("a", 3.0), ("b", None), ("b", None)],
            "k string, v double",
        )
        result = df.groupBy("k").agg(udaf(Mean())(sf.col("v")).alias("m")).orderBy("k").collect()
        expected = df.groupBy("k").agg(sf.avg("v").alias("m")).orderBy("k").collect()
        got = {r["k"]: r["m"] for r in result}
        exp = {r["k"]: r["m"] for r in expected}
        self.assertEqual(got, exp)

    def test_multiple_incremental_aggregators(self):
        # Two aggregators with different buffer schemas over the same input in one agg call.
        df = self._data()
        result = (
            df.groupBy("k")
            .agg(
                udaf(Mean())(sf.col("v")).alias("m"),
                udaf(SumSquares())(sf.col("v")).alias("s"),
            )
            .orderBy("k")
            .collect()
        )
        expected = (
            df.groupBy("k")
            .agg(
                sf.avg("v").alias("m"),
                sf.sum(sf.col("v") * sf.col("v")).alias("s"),
            )
            .orderBy("k")
            .collect()
        )
        got_m = {r["k"]: r["m"] for r in result}
        got_s = {r["k"]: r["s"] for r in result}
        for r in expected:
            self.assertAlmostEqual(got_m[r["k"]], r["m"], places=6)
            self.assertAlmostEqual(got_s[r["k"]], r["s"], places=6)

    def test_result_independent_of_partition_count(self):
        # Partial buffers must merge to the same result regardless of how keys are split.
        base = self.spark.range(0, 60).select(
            (sf.col("id") % 3).alias("k"), sf.col("id").cast("double").alias("v")
        )
        results = []
        for n in (1, 2, 7):
            rows = (
                base.repartition(n, sf.col("v"))
                .groupBy("k")
                .agg(udaf(Mean())(sf.col("v")).alias("m"))
                .orderBy("k")
                .collect()
            )
            results.append({r["k"]: r["m"] for r in rows})
        self.assertEqual(results[0], results[1])
        self.assertEqual(results[1], results[2])

    def test_sql_registration(self):
        # Register the aggregator and invoke it from SQL text.
        df = self._data()
        df.createOrReplaceTempView("agg_input")
        self.spark.udf.register("my_mean", udaf(Mean()))
        result = self.spark.sql(
            "SELECT k, my_mean(v) AS m FROM agg_input GROUP BY k ORDER BY k"
        ).collect()
        expected = df.groupBy("k").agg(sf.avg("v").alias("m")).orderBy("k").collect()
        got = {r["k"]: r["m"] for r in result}
        exp = {r["k"]: r["m"] for r in expected}
        self.assertEqual(got, exp)

    def test_named_arguments(self):
        # A named argument (both DataFrame and SQL forms) must feed the aggregator's value tuple,
        # not be silently dropped.
        df = self._data()
        result = df.groupBy("k").agg(udaf(Mean())(v=sf.col("v")).alias("m")).orderBy("k").collect()
        expected = df.groupBy("k").agg(sf.avg("v").alias("m")).orderBy("k").collect()
        self.assertEqual({r["k"]: r["m"] for r in result}, {r["k"]: r["m"] for r in expected})

        df.createOrReplaceTempView("agg_input")
        self.spark.udf.register("my_mean", udaf(Mean()))
        sql_result = self.spark.sql(
            "SELECT k, my_mean(v => v) AS m FROM agg_input GROUP BY k ORDER BY k"
        ).collect()
        self.assertEqual({r["k"]: r["m"] for r in sql_result}, {r["k"]: r["m"] for r in expected})

    def test_distinct_and_filter_rejected(self):
        # Neither DISTINCT nor FILTER is honored by the two-stage operator, so both must be
        # rejected at analysis rather than silently returning the non-distinct/unfiltered result.
        df = self._data()
        df.createOrReplaceTempView("agg_input")
        self.spark.udf.register("my_mean", udaf(Mean()))
        with self.assertRaises(AnalysisException):
            self.spark.sql("SELECT my_mean(DISTINCT v) FROM agg_input GROUP BY k").collect()
        with self.assertRaises(AnalysisException):
            self.spark.sql(
                "SELECT my_mean(v) FILTER (WHERE v > 0) FROM agg_input GROUP BY k"
            ).collect()

    def test_pivot_rejected(self):
        # ResolvePivot must reject the incremental aggregator (its null-ignoring fallback rewrite
        # would produce wrong results), like it already rejects pandas UDAFs.
        df = self.spark.createDataFrame(
            [("a", "x", 1.0), ("a", "y", 2.0), ("b", "x", 3.0)],
            "k string, p string, v double",
        )
        with self.assertRaises(AnalysisException):
            df.groupBy("k").pivot("p").agg(udaf(Mean())(sf.col("v"))).collect()

    def test_window_unbounded(self):
        # Unbounded partition frame: every row gets its whole group's aggregate. Cross-checked
        # against the equivalent SQL window aggregate.
        df = self._data()
        w = Window.partitionBy("k")
        result = df.withColumn("m", udaf(Mean())(sf.col("v")).over(w)).orderBy("k", "v").collect()
        expected = df.withColumn("m", sf.avg("v").over(w)).orderBy("k", "v").collect()
        self.assertEqual(len(result), len(expected))
        for r, e in zip(result, expected):
            self.assertAlmostEqual(r["m"], e["m"], places=6)

    def test_window_running_frame(self):
        # Ordered, growing frame (unbounded preceding .. current row): a running aggregate that
        # exercises the per-row bounded-frame path in the worker.
        df = self._data()
        w = (
            Window.partitionBy("k")
            .orderBy("v")
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
        result = df.withColumn("m", udaf(Mean())(sf.col("v")).over(w)).orderBy("k", "v").collect()
        expected = df.withColumn("m", sf.avg("v").over(w)).orderBy("k", "v").collect()
        self.assertEqual(len(result), len(expected))
        for r, e in zip(result, expected):
            self.assertAlmostEqual(r["m"], e["m"], places=6)

    def test_window_sliding_frame(self):
        # Sliding frame (1 preceding .. 1 following) with a custom single-field buffer aggregator.
        df = self._data()
        w = Window.partitionBy("k").orderBy("v").rowsBetween(-1, 1)
        result = (
            df.withColumn("s", udaf(SumSquares())(sf.col("v")).over(w)).orderBy("k", "v").collect()
        )
        expected = (
            df.withColumn("s", sf.sum(sf.col("v") * sf.col("v")).over(w))
            .orderBy("k", "v")
            .collect()
        )
        self.assertEqual(len(result), len(expected))
        for r, e in zip(result, expected):
            self.assertAlmostEqual(r["s"], e["s"], places=4)

    def test_window_bounded_preceding_frame(self):
        # A fixed number of preceding rows exercises both branches of the running-buffer
        # optimization: the lower bound is clamped to 0 for the first rows (running buffer is
        # extended in place) and then advances (each frame is refolded from zero).
        df = self._data()
        w = Window.partitionBy("k").orderBy("v").rowsBetween(-3, Window.currentRow)
        result = df.withColumn("m", udaf(Mean())(sf.col("v")).over(w)).orderBy("k", "v").collect()
        expected = df.withColumn("m", sf.avg("v").over(w)).orderBy("k", "v").collect()
        self.assertEqual(len(result), len(expected))
        for r, e in zip(result, expected):
            self.assertAlmostEqual(r["m"], e["m"], places=6)

    def test_window_decimal_output(self):
        # A non-trivial (Decimal) output type over a window, exercising the explicit
        # ``pa.array(..., type=result_type)`` typing on the window path.
        df = self._data()
        w = Window.partitionBy("k")
        result = (
            df.withColumn("s", udaf(DecimalSum())(sf.col("v")).over(w)).orderBy("k", "v").collect()
        )
        expected = df.withColumn("s", sf.sum("v").over(w)).orderBy("k", "v").collect()
        self.assertEqual(len(result), len(expected))
        for r, e in zip(result, expected):
            self.assertIsInstance(r["s"], Decimal)
            self.assertAlmostEqual(float(r["s"]), e["s"], places=4)

    def test_window_mixed_python_udf_rejected(self):
        # An incremental aggregator and a grouped-agg pandas UDF over the same window are both
        # Python window functions but use different eval types, so they cannot share one operator.
        # This must raise a clear analysis error rather than an internal assertion.
        from pyspark.sql.functions import pandas_udf, PandasUDFType

        @pandas_udf("double", PandasUDFType.GROUPED_AGG)
        def pandas_mean(v):
            return v.mean()

        df = self._data()
        w = Window.partitionBy("k")
        with self.assertRaises(AnalysisException) as ctx:
            df.select(udaf(Mean())(sf.col("v")).over(w), pandas_mean(sf.col("v")).over(w)).collect()
        self.assertEqual(
            ctx.exception.getCondition(),
            "UNSUPPORTED_FEATURE.MULTIPLE_PYTHON_UDF_TYPES_IN_WINDOW",
        )

    def test_mixed_with_other_aggregate_rejected(self):
        # An incremental aggregator mixed with another aggregate in one Aggregate is unsupported;
        # the error must be the dedicated (non-pandas) placement error.
        df = self._data()
        with self.assertRaises(AnalysisException) as ctx:
            df.groupBy("k").agg(
                udaf(Mean())(sf.col("v")).alias("m"), sf.count("*").alias("c")
            ).collect()
        self.assertEqual(ctx.exception.getCondition(), "INVALID_PYTHON_UDF_PLACEMENT")

    def test_duplicate_buffer_field_names_rejected(self):
        # Duplicate buffer field names silently collapse on the map side and then fail with an
        # opaque Arrow error post-shuffle; reject them where the aggregator is created.
        class DupBuffer(Mean):
            @property
            def bufferSchema(self):
                return StructType([StructField("x", DoubleType()), StructField("x", LongType())])

        with self.assertRaises(PySparkValueError) as ctx:
            udaf(DupBuffer())
        self.check_error(
            exception=ctx.exception,
            errorClass="DUPLICATED_FIELD_NAME_IN_ARROW_STRUCT",
            messageParameters={"field_names": "x"},
        )

    def test_float_grouping_keys_normalized(self):
        # 0.0 / -0.0 must fall into one group, and NaN keys (unequal to themselves) must group
        # together, matching SQL aggregate semantics -- guards grouping-key normalization and the
        # NaN handling of the map-side hash combine.
        zeros = self.spark.createDataFrame(
            [(0.0, 1.0), (-0.0, 2.0), (0.0, 3.0)], "k double, v double"
        )
        zero_rows = zeros.groupBy("k").agg(udaf(Mean())(sf.col("v")).alias("m")).collect()
        self.assertEqual(len(zero_rows), 1)
        self.assertAlmostEqual(zero_rows[0]["m"], 2.0, places=6)

        nans = self.spark.createDataFrame(
            [(float("nan"), 1.0), (float("nan"), 3.0)], "k double, v double"
        )
        nan_rows = nans.groupBy("k").agg(udaf(Mean())(sf.col("v")).alias("m")).collect()
        self.assertEqual(len(nan_rows), 1)
        self.assertAlmostEqual(nan_rows[0]["m"], 2.0, places=6)

    def test_complex_grouping_key(self):
        # A struct grouping key exercises the map-side hash combine's canonicalization of complex
        # keys (dict -> hashable) as well as the authoritative FINAL re-grouping.
        df = self.spark.createDataFrame(
            [(1, "a", 1.0), (1, "a", 3.0), (2, "b", 10.0)],
            "i int, s string, v double",
        ).select(sf.struct("i", "s").alias("k"), sf.col("v"))
        result = df.groupBy("k").agg(udaf(Mean())(sf.col("v")).alias("m")).collect()
        got = {(r["k"]["i"], r["k"]["s"]): r["m"] for r in result}
        self.assertEqual(got, {(1, "a"): 2.0, (2, "b"): 10.0})

    def test_bounded_map_side_combine(self):
        # A small maxRecordsPerBatch forces the map-side PARTIAL stage to flush its per-key buffer
        # in bounded chunks (emitting duplicate keys that the FINAL stage re-merges authoritatively)
        # instead of holding every key for the whole partition. The result must be unchanged --
        # this guards the cap/flush + chunked-emission path against OOM on high-cardinality keys.
        df = self._data()
        with self.sql_conf({"spark.sql.execution.arrow.maxRecordsPerBatch": 2}):
            result = (
                df.groupBy("k").agg(udaf(Mean())(sf.col("v")).alias("m")).orderBy("k").collect()
            )
        expected = df.groupBy("k").agg(sf.avg("v").alias("m")).orderBy("k").collect()
        self.assertEqual({r["k"]: r["m"] for r in result}, {r["k"]: r["m"] for r in expected})

    def test_missing_buffer_schema_rejected(self):
        # udaf() enforces a struct buffer schema up front, but a low-level construction (or a
        # malformed Connect proto) can build an incremental aggregator UDF without one. That must
        # surface a classed planner error, not a bare IllegalArgumentException / ClassCastException.
        from pyspark.sql.utils import is_remote

        if is_remote():
            from pyspark.sql.connect.udf import UserDefinedFunction
        else:
            from pyspark.sql.udf import UserDefinedFunction  # type: ignore[assignment]

        bad = UserDefinedFunction(
            Mean(),
            returnType=DoubleType(),
            name="bad_mean",
            evalType=PythonEvalType.SQL_GROUPED_AGG_ARROW_INCREMENTAL_FINAL_UDF,
            deterministic=True,
        )._wrapped()
        df = self._data()
        with self.assertRaises(AnalysisException) as ctx:
            df.groupBy("k").agg(bad(sf.col("v"))).collect()
        self.assertEqual(ctx.exception.getCondition(), "INVALID_PYTHON_AGGREGATOR_BUFFER_SCHEMA")

    def test_mixed_pandas_udaf_and_incremental_rejected(self):
        # Mixing a grouped-agg pandas UDAF with an incremental aggregator in one Aggregate is
        # unsupported. It falls through to the dedicated placement error, which must name BOTH
        # offending functions rather than dropping the co-offending pandas UDAF.
        from pyspark.sql.functions import pandas_udf, PandasUDFType

        @pandas_udf("double", PandasUDFType.GROUPED_AGG)
        def pandas_mean(v):
            return v.mean()

        df = self._data()
        with self.assertRaises(AnalysisException) as ctx:
            df.groupBy("k").agg(
                udaf(Mean())(sf.col("v")).alias("m"),
                pandas_mean(sf.col("v")).alias("pm"),
            ).collect()
        self.assertEqual(ctx.exception.getCondition(), "INVALID_PYTHON_UDF_PLACEMENT")
        message = ctx.exception.getMessage()
        self.assertIn("Mean", message)
        self.assertIn("pandas_mean", message)


class ArrowPythonAggregatorTests(ArrowPythonAggregatorTestsMixin, ReusedSQLTestCase):
    pass


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowPythonAggregatorProfilerTests(unittest.TestCase):
    # Profiling is not supported for incremental aggregators: their ``func`` is an ``Aggregator``
    # object, so the profiler wrappers would either break the worker (a plain function has no
    # ``bufferSchema`` / ``zero`` / ``reduce``) or fail on the driver in
    # ``inspect.getsourcelines(f.__code__)``. Enabling a profiler must therefore fall back to the
    # non-profiled path (with a warning) and still compute the correct result, not crash. These
    # confs are set at session creation, so this needs its own session (classic only).
    def _run(self, conf_key):
        import warnings

        from pyspark import SparkConf
        from pyspark.sql import SparkSession

        conf = SparkConf().set(conf_key, "true")
        spark = (
            SparkSession.builder.master("local[4]")
            .config(conf=conf)
            .appName(self.__class__.__name__)
            .getOrCreate()
        )
        try:
            df = spark.range(0, 20).select(
                (sf.col("id") % 3).alias("k"), sf.col("id").cast("double").alias("v")
            )
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                result = (
                    df.groupBy("k").agg(udaf(Mean())(sf.col("v")).alias("m")).orderBy("k").collect()
                )
            expected = df.groupBy("k").agg(sf.avg("v").alias("m")).orderBy("k").collect()
            self.assertEqual({r["k"]: r["m"] for r in result}, {r["k"]: r["m"] for r in expected})
            self.assertTrue(
                any("incremental Python aggregators" in str(w.message) for w in caught),
                "expected an unsupported-profiling warning",
            )
        finally:
            spark.stop()

    def test_cpu_profiler_falls_back(self):
        self._run("spark.python.profile")

    def test_memory_profiler_falls_back(self):
        self._run("spark.python.profile.memory")


if __name__ == "__main__":
    from pyspark.testing import main

    main()
