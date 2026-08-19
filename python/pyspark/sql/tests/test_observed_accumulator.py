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

from pyspark.errors import PySparkValueError
from pyspark.sql import functions as sf
from pyspark.sql.observed_accumulator import (
    _find_accumulator_in_closure,
    _find_accumulators_in_closure,
)
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.testing.utils import have_pyarrow, pyarrow_requirement_message


def _set_merge(a, v):
    # Associative union used by the custom-merge accumulator tests (arbitrary, non-numeric).
    return a | ({v} if not isinstance(v, (set, frozenset)) else v)


class ObservedAccumulatorTestsMixin:
    def _rows(self):
        # ids 0..29; multiples of 3 are "bad" (unparseable).
        return [("nan_x",) if i % 3 == 0 else (str(i),) for i in range(30)]

    def _bad_count(self):
        return sum(1 for i in range(30) if i % 3 == 0)

    def test_plain_udf_records_and_output_is_scalar(self):
        # A plain @udf (no wrapper) that references acc; closure detection must pick it up.
        acc = self.spark.accumulator("bad")

        @sf.udf("double")
        def parse(s):
            try:
                return float(s)
            except (ValueError, TypeError):
                acc.add(1)
                return None

        df = self.spark.createDataFrame(self._rows(), "raw string")
        out = df.withColumn("v", parse(sf.col("raw")))
        self.assertEqual(dict(out.dtypes)["v"], "double")  # rewritten struct -> scalar (no exec)
        rows = out.collect()  # single execution; value is cumulative across queries
        self.assertEqual(len(rows), 30)
        self.assertEqual(acc.value, self._bad_count())

    def test_udf_evaluated_once_per_row(self):
        acc = self.spark.accumulator("bad2")
        calls = self.spark.sparkContext.accumulator(0)  # classic accumulator, counts invocations

        @sf.udf("double")
        def parse(s):
            calls.add(1)
            try:
                return float(s)
            except (ValueError, TypeError):
                acc.add(1)
                return None

        df = self.spark.createDataFrame(self._rows(), "raw string")
        df.withColumn("v", parse(sf.col("raw"))).collect()
        self.assertEqual(calls.value, 30)
        self.assertEqual(acc.value, self._bad_count())

    def _check_matches_classic(self, name, rows, contrib):
        # Run a classic SparkContext accumulator (the reference) and an observed accumulator in the
        # same UDF over the same single execution; with no retries they must agree.
        classic = self.spark.sparkContext.accumulator(0)
        observed = self.spark.accumulator("xref_" + name)

        @sf.udf("string")
        def f(s):
            v = contrib(s)
            classic.add(v)
            observed.add(v)
            return s

        self.spark.createDataFrame(rows, "v string").withColumn("x", f(sf.col("v"))).collect()
        self.assertEqual(observed.value, classic.value, name)

    def test_matches_sparkcontext_accumulator(self):
        # Cross-validate the observed accumulator against the classic one across several scenarios.
        self._check_matches_classic("count", [("1",), ("x",), ("2",)], lambda s: 1)
        self._check_matches_classic("sum_len", [("a",), ("bb",), ("ccc",)], lambda s: len(s))
        self._check_matches_classic("weighted", [("1",), ("2",), ("3",)], lambda s: int(s) * 2)

    def test_value_is_cumulative_across_queries(self):
        acc = self.spark.accumulator("bad3")

        @sf.udf("double")
        def parse(s):
            try:
                return float(s)
            except (ValueError, TypeError):
                acc.add(1)
                return None

        df = self.spark.createDataFrame(self._rows(), "raw string")
        df.withColumn("v", parse(sf.col("raw"))).collect()
        df.withColumn("v", parse(sf.col("raw"))).collect()
        self.assertEqual(acc.value, self._bad_count() * 2)

    def test_sum_of_added_terms(self):
        # add() accumulates arbitrary terms, not just counts.
        acc = self.spark.accumulator("weight")

        @sf.udf("long")
        def weigh(x):
            acc.add(int(x))
            return int(x)

        df = self.spark.range(0, 10).select(sf.col("id").cast("string").alias("raw"))
        df.withColumn("v", weigh(sf.col("raw"))).collect()
        self.assertEqual(acc.value, sum(range(10)))

    def test_double_valued_accumulator(self):
        acc = self.spark.accumulator("weight_d", 0.0)  # float zero -> double-valued

        @sf.udf("double")
        def f(x):
            acc.add(float(x) * 0.5)
            return float(x)

        df = self.spark.range(1, 4).select(sf.col("id").cast("string").alias("raw"))
        df.withColumn("v", f(sf.col("raw"))).collect()
        self.assertAlmostEqual(acc.value, (1 + 2 + 3) * 0.5)

    def test_exact_long_accumulator(self):
        # Integer accumulators are exact (like a classic LongAccumulator): summing values past
        # 2^53 must not lose precision the way a Double delta would.
        acc = self.spark.accumulator("bignum")
        big = 2**53 + 1

        @sf.udf("long")
        def f(x):
            acc.add(big)
            return x

        self.spark.range(0, 3).withColumn("y", f(sf.col("id"))).collect()
        self.assertEqual(acc.value, big * 3)

    def test_multiple_accumulators_in_one_udf(self):
        # A single UDF may reference several accumulators (numeric + custom); all are harvested.
        bad = self.spark.accumulator("m_bad")
        total = self.spark.accumulator("m_total")
        keys = self.spark.accumulator("m_keys", set(), merge=_set_merge)

        @sf.udf("double")
        def parse(s):
            total.add(1)
            keys.add(s[0])
            try:
                return float(s)
            except (ValueError, TypeError):
                bad.add(1)
                return None

        df = self.spark.createDataFrame([("1",), ("x",), ("2",)], "raw string")
        df.withColumn("v", parse(sf.col("raw"))).collect()
        self.assertEqual(total.value, 3)
        self.assertEqual(bad.value, 1)  # only "x"
        self.assertEqual(keys.value, {"1", "x", "2"})

    def test_accumulator_detected_in_captured_container(self):
        # Detection reaches accumulators held in a captured list/dict, not just direct closure
        # cells or globals.
        accs = [self.spark.accumulator("c0"), self.spark.accumulator("c1")]

        @sf.udf("long")
        def f(x):
            accs[0].add(1)
            accs[1].add(10)
            return x

        self.spark.range(0, 4).withColumn("y", f(sf.col("id"))).collect()
        self.assertEqual(accs[0].value, 4)
        self.assertEqual(accs[1].value, 40)

    def test_cross_session_use_is_rejected(self):
        # An accumulator is harvested by its creating session; reading it while a different session
        # is active would silently return 0, so it must raise instead.
        other = self.spark.newSession()  # a different session; self.spark stays active
        acc = other.accumulator("xsession")
        with self.assertRaises(Exception):
            acc.value

    def test_same_named_accumulators_across_sessions_isolated(self):
        # The registry is keyed by session, not bare name, so two sessions that each create an
        # accumulator named "shared" write to disjoint slots (regression for the global-name bug).
        other = self.spark.newSession()
        acc_a = self.spark.accumulator("shared_iso")
        acc_b = other.accumulator("shared_iso")

        @sf.udf("string")
        def fa(s):
            acc_a.add(1)
            return s

        @sf.udf("string")
        def fb(s):
            acc_b.add(1)
            return s

        # Run the other-session query first, then self.spark's, so self.spark is left as the active
        # session (a query sets the thread's active session) -- both for the guard below and so we
        # do not leak an active session into other tests in this reused-session suite.
        other.createDataFrame([("x",), ("y",)], "raw string").withColumn(
            "v", fb(sf.col("raw"))
        ).collect()
        self.spark.createDataFrame([("a",), ("b",), ("c",)], "raw string").withColumn(
            "v", fa(sf.col("raw"))
        ).collect()
        # self.spark is active, so acc_a.value passes the cross-session guard; it must be 3, not 5.
        self.assertEqual(acc_a.value, 3)

    def test_mapinpandas_numeric_accumulator(self):
        acc = self.spark.accumulator("rows_mip")

        def count_rows(it):
            for pdf in it:
                acc.add(len(pdf))
                yield pdf

        df = self.spark.range(0, 30).select(sf.col("id").cast("string").alias("raw"))
        df.mapInPandas(count_rows, df.schema).count()
        self.assertEqual(acc.value, 30)

    def _kv_df(self):
        return self.spark.createDataFrame([(1, "a"), (1, "b"), (2, "a")], "k int, v string")

    def test_applyinpandas_custom_merge_accumulator(self):
        # Arbitrary (non-numeric) merge via serialized partials + collect_list + driver fold.
        acc = self.spark.accumulator("keys_aip", set(), merge=_set_merge)

        def g(pdf):
            for x in pdf["v"]:
                acc.add(x)
            return pdf

        self._kv_df().groupBy("k").applyInPandas(g, "k int, v string").count()
        self.assertEqual(acc.value, {"a", "b"})

    def test_mapinpandas_custom_merge_accumulator(self):
        acc = self.spark.accumulator("keys_mip_c", set(), merge=_set_merge)

        def f(it):
            for pdf in it:
                for x in pdf["v"]:
                    acc.add(x)
                yield pdf

        self._kv_df().mapInPandas(f, "k int, v string").count()
        self.assertEqual(acc.value, {"a", "b"})

    @unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
    def test_mapinarrow_custom_merge_accumulator(self):
        acc = self.spark.accumulator("keys_mia_c", set(), merge=_set_merge)

        def f(it):
            for batch in it:
                for x in batch.column("v").to_pylist():
                    acc.add(x)
                yield batch

        self._kv_df().mapInArrow(f, "k int, v string").count()
        self.assertEqual(acc.value, {"a", "b"})

    @unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
    def test_applyinarrow_custom_merge_accumulator(self):
        acc = self.spark.accumulator("keys_aia_c", set(), merge=_set_merge)

        def g(table):
            for x in table.column("v").to_pylist():
                acc.add(x)
            return table

        self._kv_df().groupBy("k").applyInArrow(g, "k int, v string").count()
        self.assertEqual(acc.value, {"a", "b"})

    def test_accumulator_in_higher_order_function_is_rejected(self):
        # Accumulators inside higher-order function lambdas (transform/filter/...) are not
        # supported yet (a follow-up); they must fail fast rather than silently mis-count.
        acc = self.spark.accumulator("hof")

        @sf.udf("double")
        def dbl(x):
            acc.add(1)
            return float(x) * 2

        df = self.spark.createDataFrame([([1, 2, 3],), ([4, 5],)], "arr array<int>")
        with self.assertRaises(Exception):
            df.select(sf.transform("arr", lambda x: dbl(x)).alias("out")).collect()

    def test_custom_merge_in_scalar_udf(self):
        # Custom-type accumulation inside a plain scalar UDF (parity with a classic AccumulatorV2):
        # each row emits a pickled partial, collect_list gathers them, the driver folds with merge.
        acc = self.spark.accumulator("keys_scalar", set(), merge=_set_merge)

        @sf.udf("string")
        def f(s):
            acc.add(s)
            return s

        df = self.spark.createDataFrame([("a",), ("b",), ("a",)], "v string")
        df.withColumn("x", f(sf.col("v"))).collect()
        self.assertEqual(acc.value, {"a", "b"})

    def test_accumulator_in_filter_is_observed(self):
        # An accumulator UDF used as a filter condition (a Filter node, not a projected column) is
        # rewritten and observed.
        bad = self.spark.accumulator("py_filter_bad")

        @sf.udf("boolean")
        def valid(s):
            try:
                float(s)
                return True
            except (ValueError, TypeError):
                bad.add(1)
                return False

        df = self.spark.createDataFrame([("1",), ("x",), ("2",), ("y",), ("z",)], "raw string")
        kept = df.filter(valid(sf.col("raw"))).collect()
        self.assertEqual(len(kept), 2)  # "1", "2"
        self.assertEqual(bad.value, 3)  # x, y, z

    def test_accumulator_composed_in_expression_is_observed(self):
        # An accumulator UDF nested inside a larger projected expression is observed, with normal
        # type coercion of the enclosing expression (here Long UDF value + Int literal -> Long): the
        # rule runs in the Resolution batch so coercion re-runs after the rewrite.
        acc = self.spark.accumulator("py_composed")

        @sf.udf("long")  # a Python int accumulator maps to SQL long (BIGINT)
        def f(x):
            acc.add(int(x))
            return int(x)

        df = self.spark.range(1, 4).select(sf.col("id").cast("string").alias("raw"))
        out = df.select((f(sf.col("raw")) + sf.lit(1)).alias("y")).collect()
        self.assertEqual(sorted(r["y"] for r in out), [2, 3, 4])
        self.assertEqual(acc.value, 1 + 2 + 3)

    def test_accumulator_in_unobservable_position_fails_fast(self):
        # An accumulator UDF in a position the rule cannot observe (here an aggregate grouping)
        # fails fast instead of silently returning 0.
        acc = self.spark.accumulator("py_unsupported")

        @sf.udf("string")
        def f(s):
            acc.add(1)
            return s

        df = self.spark.createDataFrame([("a",), ("b",)], "raw string")
        with self.assertRaises(Exception):
            df.groupBy(f(sf.col("raw"))).count().collect()

    def test_closure_detection(self):
        # Pure-Python detection logic (no Spark), covering closure-cell and no-capture cases.
        acc = self.spark.accumulator("detect")

        def make(a):
            def f(s):
                a.add(1)
                return s

            return f

        self.assertIs(_find_accumulator_in_closure(make(acc)), acc)
        self.assertIsNone(_find_accumulator_in_closure(lambda s: s))

    def test_closure_detection_covers_nested_shapes(self):
        # Detection must find an accumulator however it is held, since a miss silently returns 0.
        # Pure-Python (no Spark exec), one representative closure per reachability shape.
        def names(fn):
            return sorted(a._name for a in _find_accumulators_in_closure(fn))

        # Nested container ([[acc]]) and dict-of-list ({"k": [acc]}).
        a_list = self.spark.accumulator("n_list")
        box = [[a_list]]
        self.assertEqual(names(lambda s: box[0][0].add(1)), ["n_list"])
        a_dict = self.spark.accumulator("n_dict")
        d = {"k": [a_dict]}
        self.assertEqual(names(lambda s: d["k"][0].add(1)), ["n_dict"])

        # Attribute of a captured object (self.acc-style).
        a_attr = self.spark.accumulator("n_attr")

        class Holder:
            def __init__(self, a):
                self.a = a

        h = Holder(a_attr)
        self.assertEqual(names(lambda s: h.a.add(1)), ["n_attr"])

        # A helper the UDF calls (its closure and globals are followed).
        def make_enclosing():
            a = self.spark.accumulator("n_helper")

            def helper(s):
                a.add(1)
                return s

            return lambda s: helper(s)

        self.assertEqual(names(make_enclosing()), ["n_helper"])

        # A self-referential container must not loop, and no capture finds nothing.
        a_cyc = self.spark.accumulator("n_cyc")
        lst = [a_cyc]
        lst.append(lst)
        self.assertEqual(names(lambda s: lst[0].add(1)), ["n_cyc"])
        self.assertEqual(names(lambda s: s), [])

    def test_accumulator_detected_via_helper_function(self):
        # End-to-end: an accumulator referenced only inside a helper the UDF calls is harvested.
        acc = self.spark.accumulator("via_helper")

        def _helper(s):
            acc.add(1)
            return s

        @sf.udf("string")
        def f(s):
            return _helper(s)

        df = self.spark.createDataFrame([("a",), ("b",), ("c",)], "v string")
        df.withColumn("x", f(sf.col("v"))).collect()
        self.assertEqual(acc.value, 3)

    def test_accumulator_detected_on_object_attribute(self):
        # End-to-end: an accumulator held as an attribute of a captured object is harvested.
        acc = self.spark.accumulator("on_attr")

        class Box:
            def __init__(self, a):
                self.a = a

        box = Box(acc)

        @sf.udf("long")
        def f(x):
            box.a.add(1)
            return x

        self.spark.range(0, 4).withColumn("y", f(sf.col("id"))).collect()
        self.assertEqual(acc.value, 4)

    def test_same_accumulator_referenced_by_two_udfs_in_one_projection(self):
        # Two distinct UDFs both add to the same accumulator in one select, so every input row
        # contributes twice. Regression: the two deltas must combine into one __oa_metric_shared
        # column; a duplicate-named metric per UDF made the harvest read only the first and
        # silently drop the other (returning 3 instead of 6).
        acc = self.spark.accumulator("shared")

        @sf.udf("string")
        def f(s):
            acc.add(1)
            return s

        @sf.udf("string")
        def g(s):
            acc.add(1)
            return s

        df = self.spark.createDataFrame([("a",), ("b",), ("c",)], "raw string")
        df.select(f(sf.col("raw")).alias("f"), g(sf.col("raw")).alias("g")).collect()
        self.assertEqual(acc.value, 6)  # 3 rows * 2 UDFs

    def test_non_integer_add_to_int_accumulator_is_rejected(self):
        # A fractional value added to an integer accumulator is rejected up front, consistently for
        # every UDF flavor -- rather than silently truncating (row-at-a-time UDF) or crashing the
        # int64 delta Series (vectorized UDF). Whole-valued terms (incl. 2.0) are still accepted,
        # and a float accumulator takes fractional values.
        acc = self.spark.accumulator("int_acc")  # integer zero
        with self.assertRaises(PySparkValueError) as pe:
            acc.add(1.5)
        self.check_error(
            exception=pe.exception,
            errorClass="OBSERVED_ACCUMULATOR_NON_INTEGER_ADD",
            messageParameters={"term": "1.5", "name": "int_acc"},
        )
        acc.add(2)
        acc.add(2.0)
        self.spark.accumulator("float_acc", 0.0).add(1.5)


class ObservedAccumulatorTests(ObservedAccumulatorTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.testing import main

    main()
