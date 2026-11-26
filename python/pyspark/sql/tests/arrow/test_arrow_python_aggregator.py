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
import datetime
import unittest
from decimal import Decimal

from pyspark.errors import AnalysisException, PySparkNotImplementedError, PySparkValueError
from pyspark.sql import functions as sf
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    CharType,
    DecimalType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.testing.utils import (
    have_pyarrow,
    pyarrow_requirement_message,
)
from pyspark.util import PythonEvalType

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

    class CollectList(Aggregator):
        # Updates its list buffer in place and returns that very list from ``finish``, as the
        # Aggregator contract allows.
        @property
        def bufferSchema(self):
            return StructType([StructField("values", ArrayType(DoubleType()))])

        @property
        def outputType(self):
            return ArrayType(DoubleType())

        def zero(self):
            return ([],)

        def reduce(self, buffer, value):
            (v,) = value
            if v is not None:  # skip nulls, like collect_list
                buffer[0].append(v)
            return buffer

        def merge(self, b1, b2):
            b1[0].extend(b2[0])
            return b1

        def finish(self, buffer):
            return buffer[0]

    class CollectListStruct(CollectList):
        # Like CollectList, but nests the buffer's list in a struct, so a shallow copy of the
        # ``finish`` result would still share it with the buffer.
        @property
        def outputType(self):
            return StructType([StructField("values", ArrayType(DoubleType()))])

        def finish(self, buffer):
            return (buffer[0],)

    class ConcatStrings(Aggregator):
        # Appends each non-null value to a bytearray in place, comma-separated like
        # ``concat_ws(",", collect_list(...))``, and returns that very bytearray from ``finish``:
        # a mutable value of a string type, which is not nested.
        @property
        def bufferSchema(self):
            return StructType([StructField("s", BinaryType())])

        @property
        def outputType(self):
            return StringType()

        def zero(self):
            return (bytearray(),)

        def reduce(self, buffer, value):
            (s,) = value
            if s is not None:
                if buffer[0]:
                    buffer[0].extend(b",")
                buffer[0].extend(s.encode("utf-8"))
            return buffer

        def merge(self, b1, b2):
            if b1[0] and b2[0]:
                b1[0].extend(b",")
            b1[0].extend(b2[0])
            return b1

        def finish(self, buffer):
            return buffer[0]

    class ConcatBinary(ConcatStrings):
        @property
        def outputType(self):
            return BinaryType()


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

    def test_char_varchar_buffer_schema_rejected(self):
        class BadBuffer(Aggregator):
            @property
            def bufferSchema(self):
                return StructType([StructField("s", CharType(3))])

            @property
            def outputType(self):
                return DoubleType()

            def zero(self):
                return ("",)

            def reduce(self, buffer, value):
                return buffer

            def merge(self, b1, b2):
                return b1

            def finish(self, buffer):
                return 0.0

        with self.assertRaises(PySparkNotImplementedError) as pe:
            udaf(BadBuffer())
        self.check_error(
            exception=pe.exception,
            errorClass="CHAR_VARCHAR_NOT_SUPPORTED_IN_PYTHON",
            messageParameters={
                "feature": "Python UDAF buffer schemas",
                "data_type": "struct<s:char(3)>",
            },
        )

    def test_char_varchar_output_type_rejected(self):
        class BadOutput(Aggregator):
            @property
            def bufferSchema(self):
                return StructType([StructField("n", LongType())])

            @property
            def outputType(self):
                return CharType(3)

            def zero(self):
                return (0,)

            def reduce(self, buffer, value):
                return buffer

            def merge(self, b1, b2):
                return b1

            def finish(self, buffer):
                return "a"

        with self.assertRaises(PySparkNotImplementedError) as pe:
            udaf(BadOutput())
        self.check_error(
            exception=pe.exception,
            errorClass="CHAR_VARCHAR_NOT_SUPPORTED_IN_PYTHON",
            messageParameters={
                "feature": "Python UDF return types",
                "data_type": "char(3)",
            },
        )

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

    def _event_data(self):
        # With a 5 minute gap, the rows of key 1 at 10:00, 10:03 and 10:07 chain into one session
        # (10:07 overlaps only the session that 10:03 extends), and so do the rows of key 2 at 10:01
        # and 10:04. The later rows of each key start sessions of their own.
        def ts(minute):
            return datetime.datetime(2026, 1, 1, 10, minute)

        return self.spark.createDataFrame(
            [
                (1, ts(0), 1.0),
                (1, ts(3), 3.0),
                (1, ts(7), 5.0),
                (1, ts(20), 7.0),
                (2, ts(1), 2.0),
                (2, ts(4), 4.0),
                (2, ts(30), 6.0),
            ],
            "k int, ts timestamp, v double",
        ).repartition(3)

    def test_session_window(self):
        # A batch session window starts as one session per row, so the overlapping sessions must
        # be merged before they are aggregated, as for the built-in aggregate functions.
        df = self._event_data()
        mean = udaf(Mean())
        groupings = {
            "session window": ([sf.session_window("ts", "5 minutes")], 3),
            "key and session window": (["k", sf.session_window("ts", "5 minutes")], 4),
        }
        for name, (keys, num_sessions) in groupings.items():
            with self.subTest(grouping=name):
                result = df.groupBy(*keys).agg(mean(sf.col("v")).alias("m")).collect()
                expected = df.groupBy(*keys).agg(sf.avg("v").alias("m")).collect()
                self.assertEqual(len(expected), num_sessions)
                self.assertEqual(sorted(result), sorted(expected))

        with self.subTest(grouping="SQL"):
            df.createOrReplaceTempView("session_input")
            self.spark.udf.register("session_mean", mean)
            query = (
                "SELECT k, session_window.start AS start, session_window.end AS end, {}(v) AS m "
                "FROM session_input GROUP BY k, session_window(ts, '5 minutes')"
            )
            result = self.spark.sql(query.format("session_mean")).collect()
            expected = self.spark.sql(query.format("avg")).collect()
            self.assertEqual(sorted(result), sorted(expected))

    def test_time_window(self):
        # Tumbling and sliding windows need no merging: each row belongs to its fixed windows.
        df = self._event_data()
        windows = {
            "tumbling": sf.window("ts", "5 minutes"),
            "sliding": sf.window("ts", "5 minutes", "1 minute"),
        }
        for name, window in windows.items():
            with self.subTest(window=name):
                result = df.groupBy(window).agg(udaf(Mean())(sf.col("v")).alias("m")).collect()
                expected = df.groupBy(window).agg(sf.avg("v").alias("m")).collect()
                self.assertEqual(sorted(result), sorted(expected))

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

    def _data_with_ties_and_nulls(self):
        # 60 rows across 3 keys, with ties and nulls in ``v``, and ``v`` as a string in ``s``.
        # Each key has two nulls, which an ordered window sorts first. ``id`` breaks the ties of
        # ROWS frames, so that every frame is deterministic; a RANGE frame includes all peers of a
        # row, which share ``v`` and ``s``.
        return (
            self.spark.range(0, 60)
            .select(
                "id",
                (sf.col("id") % 3).alias("k"),
                sf.when(sf.col("id") % 11 == 0, sf.lit(None))
                .otherwise((sf.col("id") % 4).cast("double"))
                .alias("v"),
            )
            .withColumn("s", sf.col("v").cast("string"))
            .repartition(4, sf.col("id") % 2)
        )

    def test_window_frames_with_in_place_buffer(self):
        # ``reduce`` may update its buffer in place, and while a frame's lower bound stays fixed,
        # later rows keep reducing into the buffer that earlier rows were finished from. Each row
        # must still get its own frame's value when ``finish`` returns the buffer's list, nests it
        # in a struct, or returns its bytearray as a string or binary value.
        df = self._data_with_ties_and_nulls()
        by_v = Window.partitionBy("k").orderBy("v")
        by_v_id = Window.partitionBy("k").orderBy("v", "id")
        frames = {
            # RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW. The peers of a row (its ties and
            # nulls in ``v``) share its frame, so ``finish`` runs again on a buffer that did not
            # grow.
            "default": by_v,
            "rows": by_v_id.rowsBetween(Window.unboundedPreceding, Window.currentRow),
            # The upper bound runs ahead of the current row.
            "range": by_v.rangeBetween(Window.unboundedPreceding, 1),
            # The lower bound stays at the first row for the first three rows, then advances.
            # The third row extends the buffer that the first two (nulls) were finished from.
            "sliding": by_v_id.rowsBetween(-2, Window.currentRow),
        }
        for name, w in frames.items():
            with self.subTest(frame=name):
                result = (
                    df.select(
                        "id",
                        udaf(CollectList())(sf.col("v")).over(w).alias("l"),
                        udaf(CollectListStruct())(sf.col("v")).over(w).alias("ls"),
                        udaf(ConcatStrings())(sf.col("s")).over(w).alias("cs"),
                        udaf(ConcatBinary())(sf.col("s")).over(w).alias("cb"),
                    )
                    .orderBy("id")
                    .collect()
                )
                concat = sf.concat_ws(",", sf.collect_list("s").over(w))
                expected = (
                    df.select(
                        "id",
                        sf.collect_list("v").over(w).alias("l"),
                        sf.struct(sf.collect_list("v").over(w).alias("values")).alias("ls"),
                        concat.alias("cs"),
                        concat.cast("binary").alias("cb"),
                    )
                    .orderBy("id")
                    .collect()
                )
                self.assertEqual(result, expected)

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
        from pyspark.sql.functions import PandasUDFType, pandas_udf

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
        from pyspark.sql.functions import PandasUDFType, pandas_udf

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
