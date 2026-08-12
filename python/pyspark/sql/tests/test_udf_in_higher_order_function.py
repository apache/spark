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

from pyspark.errors import AnalysisException
from pyspark.sql import functions as sf
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, DoubleType, IntegerType, StringType
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.testing.utils import (
    assertDataFrameEqual,
    have_pandas,
    have_pyarrow,
    pandas_requirement_message,
    pyarrow_requirement_message,
)


@unittest.skipIf(
    not have_pandas or not have_pyarrow, pandas_requirement_message or pyarrow_requirement_message
)
class UDFInHigherOrderFunctionTestsMixin:
    """Tests for scalar Python UDFs used inside higher-order function lambdas (SPARK-27052).

    ``ExtractPythonUDFFromLambda`` rewrites such a plan so the UDF is applied to the whole array
    outside the lambda. Each test asserts the *result*, comparing against the equivalent native
    expression wherever one exists, so that a rewrite that runs but computes the wrong thing
    fails rather than passing quietly.
    """

    def test_transform(self):
        df = self.spark.createDataFrame([([1, 2, 3],), ([],), ([10],)], "values array<int>")
        plus_one = udf(lambda x: x + 1, IntegerType())

        assertDataFrameEqual(
            df.select(sf.transform("values", lambda x: plus_one(x)).alias("r")),
            df.select(sf.transform("values", lambda x: x + 1).alias("r")),
        )

    def test_transform_null_array_and_null_elements(self):
        # A null array must stay null, and a null *element* must reach the UDF as None.
        df = self.spark.createDataFrame([([1, None, 3],), (None,), ([],)], "values array<int>")
        # Null-aware so the UDF itself can observe the null element.
        f = udf(lambda x: -1 if x is None else x * 2, IntegerType())

        assertDataFrameEqual(
            df.select(sf.transform("values", lambda x: f(x)).alias("r")),
            [([2, -1, 6],), (None,), ([],)],
        )

    def test_transform_udf_returning_null(self):
        df = self.spark.createDataFrame([([1, 2, 3],)], "values array<int>")
        f = udf(lambda x: None if x == 2 else x, IntegerType())

        assertDataFrameEqual(
            df.select(sf.transform("values", lambda x: f(x)).alias("r")),
            [([1, None, 3],)],
        )

    def test_transform_with_index(self):
        df = self.spark.createDataFrame([([10, 20, 30],), ([],)], "values array<int>")
        plus_one = udf(lambda x: x + 1, IntegerType())

        # The index parameter must still work once the element is read from the carrier struct.
        assertDataFrameEqual(
            df.select(sf.transform("values", lambda x, i: plus_one(x) + i).alias("r")),
            df.select(sf.transform("values", lambda x, i: (x + 1) + i).alias("r")),
        )

    def test_composition_around_udf_result(self):
        df = self.spark.createDataFrame([([1, 2, 3],)], "values array<int>")
        plus_one = udf(lambda x: x + 1, IntegerType())

        # Arithmetic, `when` and casts around the UDF result are ordinary JVM work.
        assertDataFrameEqual(
            df.select(
                sf.transform("values", lambda x: plus_one(x) * 2).alias("mul"),
                sf.transform(
                    "values", lambda x: sf.when(plus_one(x) > 2, sf.lit(1)).otherwise(sf.lit(0))
                ).alias("cond"),
                sf.transform("values", lambda x: plus_one(x).cast("string")).alias("cast"),
            ),
            df.select(
                sf.transform("values", lambda x: (x + 1) * 2).alias("mul"),
                sf.transform(
                    "values", lambda x: sf.when((x + 1) > 2, sf.lit(1)).otherwise(sf.lit(0))
                ).alias("cond"),
                sf.transform("values", lambda x: (x + 1).cast("string")).alias("cast"),
            ),
        )

    def test_udf_argument_is_expression_over_element(self):
        # `udf(x * 2)`: the argument is itself an expression over the element.
        df = self.spark.createDataFrame([([1, 2, 3],)], "values array<int>")
        plus_one = udf(lambda x: x + 1, IntegerType())

        assertDataFrameEqual(
            df.select(sf.transform("values", lambda x: plus_one(x * 2)).alias("r")),
            df.select(sf.transform("values", lambda x: x * 2 + 1).alias("r")),
        )

    def test_multiple_udfs_in_one_lambda(self):
        df = self.spark.createDataFrame([([1, 2, 3],)], "values array<int>")
        plus_one = udf(lambda x: x + 1, IntegerType())
        times_ten = udf(lambda x: x * 10, IntegerType())

        assertDataFrameEqual(
            df.select(sf.transform("values", lambda x: plus_one(x) + times_ten(x)).alias("r")),
            df.select(sf.transform("values", lambda x: (x + 1) + (x * 10)).alias("r")),
        )

    def test_nested_udfs(self):
        # `f(g(x))`: both are lifted, and compose as array UDFs outside the lambda.
        df = self.spark.createDataFrame([([1, 2, 3],)], "values array<int>")
        plus_one = udf(lambda x: x + 1, IntegerType())
        times_ten = udf(lambda x: x * 10, IntegerType())

        assertDataFrameEqual(
            df.select(sf.transform("values", lambda x: times_ten(plus_one(x))).alias("r")),
            df.select(sf.transform("values", lambda x: (x + 1) * 10).alias("r")),
        )

    def test_nested_udf_inside_composite_argument(self):
        # SPARK-27052: `f(g(x) + 1)` / `f(-g(x))`. The inner call is buried inside a composite
        # argument, not a direct child. The inner result must be substituted before lifting `f`,
        # or a raw `g` over the lambda variable would be left inside a lambda and mis-extracted
        # (SPARK-48706). The result must match evaluating the composition element-wise.
        df = self.spark.createDataFrame([([1, 2, 3],)], "values array<int>")
        plus_one = udf(lambda x: x + 1, IntegerType())
        times_ten = udf(lambda x: x * 10, IntegerType())

        assertDataFrameEqual(
            df.select(sf.transform("values", lambda x: times_ten(plus_one(x) + 1)).alias("r")),
            df.select(sf.transform("values", lambda x: ((x + 1) + 1) * 10).alias("r")),
        )
        assertDataFrameEqual(
            df.select(sf.transform("values", lambda x: times_ten(-plus_one(x))).alias("r")),
            df.select(sf.transform("values", lambda x: (-(x + 1)) * 10).alias("r")),
        )

    def test_udf_with_outer_column_argument(self):
        # A non-element argument must be broadcast to every element of its row.
        df = self.spark.createDataFrame([([1, 2], 100), ([3], 200)], "values array<int>, base int")
        add = udf(lambda x, b: x + b, IntegerType())

        assertDataFrameEqual(
            df.select(sf.transform("values", lambda x: add(x, sf.col("base"))).alias("r")),
            [([101, 102],), ([203],)],
        )

    def test_udf_with_constant_argument_only(self):
        # SPARK-27052: `transform(arr, x -> udf(lit(10)))` does not read the element, but the UDF
        # must still take the lambda's call domain: once per element, and zero times for an empty
        # or null array (where the lambda never runs), rather than once per row.
        df = self.spark.createDataFrame([([1, 2, 3],), ([],), (None,)], "values array<int>")
        const = udf(lambda v: v * 2, IntegerType())

        assertDataFrameEqual(
            df.select(sf.transform("values", lambda x: const(sf.lit(10))).alias("r")),
            [([20, 20, 20],), ([],), (None,)],
        )

    def test_filter(self):
        # `filter`'s result is built from the input elements, not the lambda's value.
        df = self.spark.createDataFrame([([1, 2, 3, 4],), ([],), (None,)], "values array<int>")
        is_even = udf(lambda x: x % 2 == 0, "boolean")

        assertDataFrameEqual(
            df.select(sf.filter("values", lambda x: is_even(x)).alias("r")),
            df.select(sf.filter("values", lambda x: (x % 2) == 0).alias("r")),
        )

    def test_exists_and_forall(self):
        df = self.spark.createDataFrame([([1, 2, 3],), ([2, 4],), ([],)], "values array<int>")
        is_even = udf(lambda x: x % 2 == 0, "boolean")

        assertDataFrameEqual(
            df.select(
                sf.exists("values", lambda x: is_even(x)).alias("e"),
                sf.forall("values", lambda x: is_even(x)).alias("f"),
            ),
            df.select(
                sf.exists("values", lambda x: (x % 2) == 0).alias("e"),
                sf.forall("values", lambda x: (x % 2) == 0).alias("f"),
            ),
        )

    def test_zip_with(self):
        # Two arrays at once. `arrays_zip` pads the shorter side with nulls, which is what
        # `zip_with` does itself, so differing lengths must agree with the native version.
        df = self.spark.createDataFrame(
            [([1, 2], [10, 20]), ([1, 2, 3], [10]), ([], []), (None, [1]), ([1], None)],
            "l array<int>, r array<int>",
        )
        add = udf(lambda a, b: (0 if a is None else a) + (0 if b is None else b), IntegerType())

        # Compare against the equivalent native expression with the same null handling.
        assertDataFrameEqual(
            df.select(sf.zip_with("l", "r", lambda a, b: add(a, b)).alias("r")),
            df.select(
                sf.zip_with(
                    "l",
                    "r",
                    lambda a, b: sf.coalesce(a, sf.lit(0)) + sf.coalesce(b, sf.lit(0)),
                ).alias("r")
            ),
        )

    def test_zip_with_udf_on_one_side_only(self):
        df = self.spark.createDataFrame([([1, 2], [10, 20])], "l array<int>, r array<int>")
        plus_one = udf(lambda x: x + 1, IntegerType())

        assertDataFrameEqual(
            df.select(sf.zip_with("l", "r", lambda a, b: plus_one(a) + b).alias("r")),
            [([12, 23],)],
        )

    def test_array_sort_with_udf_key(self):
        # When the UDF applies per element, it is precomputed as a sort key that the JVM comparator
        # compares (a UDF taking both elements is instead precomputed over the pairs; see the
        # pairwise test). This must actually reorder, not be a no-op.
        df = self.spark.createDataFrame([([3, 1, 2],), ([],), (None,)], "values array<int>")
        negate = udf(lambda x: -x, IntegerType())

        # Sorting by -x gives descending order.
        assertDataFrameEqual(
            df.select(
                sf.array_sort(
                    "values",
                    lambda a, b: sf.when(negate(a) < negate(b), sf.lit(-1))
                    .when(negate(a) > negate(b), sf.lit(1))
                    .otherwise(sf.lit(0)),
                ).alias("r")
            ),
            [([3, 2, 1],), ([],), (None,)],
        )

    def test_array_sort_pairwise_comparator(self):
        # One UDF call receiving both elements has no per-element key, so the UDF is precomputed
        # over every ordered pair and the comparator indexes that matrix. Assert an actual
        # reordering, not merely that the query runs.
        df = self.spark.createDataFrame(
            [([3, 1, 2],), ([],), (None,), ([5],), ([2, 2, 1],)], "values array<int>"
        )
        cmp_udf = udf(lambda a, b: (a > b) - (a < b), IntegerType())

        assertDataFrameEqual(
            df.select(sf.array_sort("values", lambda a, b: cmp_udf(a, b)).alias("r")),
            [([1, 2, 3],), ([],), (None,), ([5],), ([1, 2, 2],)],
        )

    def test_array_sort_pairwise_comparator_descending(self):
        # Reversing the comparator must reverse the order, which a no-op rewrite would not do.
        df = self.spark.createDataFrame([([3, 1, 2],)], "values array<int>")
        cmp_desc = udf(lambda a, b: (b > a) - (b < a), IntegerType())

        assertDataFrameEqual(
            df.select(sf.array_sort("values", lambda a, b: cmp_desc(a, b)).alias("r")),
            [([3, 2, 1],)],
        )

    def test_transform_keys_and_values(self):
        df = self.spark.createDataFrame(
            [
                ({"a": 1, "b": 2},),
            ],
            "m map<string,int>",
        )
        upper = udf(lambda s: s.upper(), StringType())
        plus_one = udf(lambda v: v + 1, IntegerType())

        assertDataFrameEqual(
            df.select(sf.transform_keys("m", lambda k, v: upper(k)).alias("r")),
            [({"A": 1, "B": 2},)],
        )
        assertDataFrameEqual(
            df.select(sf.transform_values("m", lambda k, v: plus_one(v)).alias("r")),
            [({"a": 2, "b": 3},)],
        )
        # The lambda may read both the key and the value.
        assertDataFrameEqual(
            df.select(sf.transform_values("m", lambda k, v: plus_one(v) + sf.length(k)).alias("r")),
            [({"a": 3, "b": 4},)],
        )

    def test_map_filter(self):
        df = self.spark.createDataFrame([({"a": 1, "b": 2, "c": 3},)], "m map<string,int>")
        is_odd = udf(lambda v: v % 2 == 1, "boolean")

        assertDataFrameEqual(
            df.select(sf.map_filter("m", lambda k, v: is_odd(v)).alias("r")),
            [({"a": 1, "c": 3},)],
        )

    def test_map_zip_with(self):
        # The visited key set is the union of both maps' keys; a key missing from one side gives
        # null on that side, matching map_zip_with's own semantics.
        df = self.spark.createDataFrame(
            [
                (
                    {"a": 1, "b": 2},
                    {"b": 20, "c": 30},
                )
            ],
            "l map<string,int>, r map<string,int>",
        )
        combine = udf(
            lambda a, b: (0 if a is None else a) * 100 + (0 if b is None else b), IntegerType()
        )

        assertDataFrameEqual(
            df.select(sf.map_zip_with("l", "r", lambda k, v1, v2: combine(v1, v2)).alias("r")),
            [({"a": 100, "b": 220, "c": 30},)],
        )

    def test_transform_values_result_type_equals_key_type(self):
        # SPARK-27052: transform_values on map<string,string> whose lambda also returns string.
        # The rewrite must replace the values, not the keys (dispatch is by function, not type).
        df = self.spark.createDataFrame([({"a": "x", "b": "y"},)], "m map<string,string>")
        tag = udf(lambda v: v + "!", StringType())
        assertDataFrameEqual(
            df.select(sf.transform_values("m", lambda k, v: tag(v)).alias("r")),
            [({"a": "x!", "b": "y!"},)],
        )

    def test_nondeterministic_udf_calls_are_distinct(self):
        # SPARK-27052: two calls to a nondeterministic UDF in one lambda must stay distinct, not be
        # collapsed into one shared value. `rand_add` returns x plus a per-call random draw, so
        # f(x) + f(x) equals 2x only if the two calls were (wrongly) deduplicated.
        import random

        df = self.spark.createDataFrame([([10, 20, 30],)], "values array<int>")
        rand_add = udf(
            lambda x: x + random.randint(1, 1_000_000), IntegerType()
        ).asNondeterministic()
        row = df.select(
            sf.transform("values", lambda x: rand_add(x) - rand_add(x)).alias("r")
        ).collect()[0]
        # If the two calls were deduplicated, every element would be exactly 0.
        self.assertTrue(any(v != 0 for v in row["r"]), row["r"])

    def test_fused_transforms_with_different_lengths(self):
        # SPARK-27052: two transforms over arrays of different per-row lengths and null layouts get
        # fused by ExtractPythonUDFs into one element-wise batch. Each UDF must be re-nested by its
        # own array's shape, not a shared one.
        df = self.spark.createDataFrame(
            [([1, 2, 3], [10]), ([], None), (None, [7, 8])],
            "a array<int>, b array<int>",
        )
        f = udf(lambda v: v + 1, IntegerType())
        assertDataFrameEqual(
            df.select(
                sf.transform("a", lambda x: f(x)).alias("ra"),
                sf.transform("b", lambda x: f(x)).alias("rb"),
            ),
            [([2, 3, 4], [11]), ([], None), (None, [8, 9])],
        )

    def test_element_and_return_types(self):
        df = self.spark.createDataFrame([(["a", "bb"],)], "values array<string>")
        upper_len = udf(lambda s: len(s), IntegerType())
        assertDataFrameEqual(
            df.select(sf.transform("values", lambda x: upper_len(x)).alias("r")),
            [([1, 2],)],
        )

        df2 = self.spark.createDataFrame([([1.5, 2.5],)], "values array<double>")
        half = udf(lambda v: v / 2, DoubleType())
        assertDataFrameEqual(
            df2.select(sf.transform("values", lambda x: half(x)).alias("r")),
            [([0.75, 1.25],)],
        )

        # A UDF returning a non-atomic type, so the re-nesting handles nested lists.
        df3 = self.spark.createDataFrame([([1, 2],)], "values array<int>")
        repeat = udf(lambda v: [v, v], ArrayType(IntegerType()))
        assertDataFrameEqual(
            df3.select(sf.transform("values", lambda x: repeat(x)).alias("r")),
            [([[1, 1], [2, 2]],)],
        )

    def test_long_arrays_and_many_rows(self):
        # Exercises batching: the wrapper evaluates all elements of a batch in one pass.
        df = self.spark.range(0, 200).select(
            sf.transform(
                sf.sequence(sf.lit(1), sf.lit(20)), lambda x: (x + sf.col("id")).cast("int")
            ).alias("values")
        )
        plus_one = udf(lambda x: x + 1, IntegerType())

        assertDataFrameEqual(
            df.select(sf.transform("values", lambda x: plus_one(x)).alias("r")),
            df.select(sf.transform("values", lambda x: x + 1).alias("r")),
        )

    def test_all_null_rows(self):
        df = self.spark.createDataFrame([(None,), (None,)], "values array<int>")
        plus_one = udf(lambda x: x + 1, IntegerType())
        assertDataFrameEqual(
            df.select(sf.transform("values", lambda x: plus_one(x)).alias("r")),
            [(None,), (None,)],
        )

    def test_empty_dataframe(self):
        df = self.spark.createDataFrame([], "values array<int>")
        plus_one = udf(lambda x: x + 1, IntegerType())
        self.assertEqual(
            df.select(sf.transform("values", lambda x: plus_one(x)).alias("r")).count(), 0
        )

    def test_udf_inside_nested_lambda(self):
        # A UDF in a *nested* lambda is lifted onto the fully flattened leaves and the nested
        # structure is rebuilt around the result: `transform(matrix, row -> transform(row, x ->
        # f(x)))` applies `f` to every leaf. The UDF runs once over all leaves (a depth-2 lift), and
        # the result is compared against the equivalent native expression.
        df = self.spark.createDataFrame(
            [([[1, 2], [3]],), ([[], [4, 5]],), (None,), ([None, [6]],)],
            "values array<array<int>>",
        )
        plus_one = udf(lambda x: x + 1, IntegerType())
        is_even = udf(lambda x: x % 2 == 0, "boolean")

        # transform inside transform.
        assertDataFrameEqual(
            df.select(
                sf.transform("values", lambda row: sf.transform(row, lambda x: plus_one(x))).alias(
                    "r"
                )
            ),
            df.select(
                sf.transform("values", lambda row: sf.transform(row, lambda x: x + 1)).alias("r")
            ),
        )
        # filter inside transform: the inner result length differs from the input, but the UDF still
        # runs over every leaf before the (JVM) filtering.
        assertDataFrameEqual(
            df.select(
                sf.transform("values", lambda row: sf.filter(row, lambda x: is_even(x))).alias("r")
            ),
            df.select(
                sf.transform("values", lambda row: sf.filter(row, lambda x: x % 2 == 0)).alias("r")
            ),
        )

    def test_udf_inside_nested_lambda_capturing_outer_variable(self):
        # The inner UDF reads both the inner element and the *enclosing* lambda's variable
        # (`sf.size(row)`, where `row` is the outer element), so its argument depends on two nesting
        # levels; the lift aligns both onto the leaves. Compared against the equivalent native
        # expression, over null outer rows, null inner arrays, and empty inner arrays.
        df = self.spark.createDataFrame(
            [([[1, 2], [3, 4]],), (None,), ([[]],), ([None, [5]],)],
            "values array<array<int>>",
        )
        add = udf(lambda a, b: a + b, IntegerType())
        assertDataFrameEqual(
            df.select(
                sf.transform(
                    "values", lambda row: sf.transform(row, lambda x: add(x, sf.size(row)))
                ).alias("r")
            ),
            df.select(
                sf.transform(
                    "values", lambda row: sf.transform(row, lambda x: x + sf.size(row))
                ).alias("r")
            ),
        )

    def test_udf_inside_three_level_nested_lambda(self):
        # Three levels of nesting: `f` is lifted to a depth-3 element-wise UDF, so the worker
        # flattens three array levels to the leaves and re-nests three levels.
        df = self.spark.createDataFrame(
            [([[[1, 2], [3]], [[4]]],), (None,), ([[[], None]],)],
            "values array<array<array<int>>>",
        )
        plus_one = udf(lambda x: x + 1, IntegerType())
        assertDataFrameEqual(
            df.select(
                sf.transform(
                    "values",
                    lambda a: sf.transform(a, lambda b: sf.transform(b, lambda x: plus_one(x))),
                ).alias("r")
            ),
            df.select(
                sf.transform(
                    "values",
                    lambda a: sf.transform(a, lambda b: sf.transform(b, lambda x: x + 1)),
                ).alias("r")
            ),
        )

    def test_vectorized_udf_inside_nested_lambda(self):
        # All four vectorized flavors lifted out of a nested lambda (depth 2): the worker flattens
        # two array levels to the leaves, runs the native-batch function once, and re-nests two
        # levels. Includes null outer rows, null inner arrays, and empty inner arrays.
        from typing import Iterator
        import pandas as pd
        import pyarrow as pa
        from pyspark.sql.functions import pandas_udf, arrow_udf

        df = self.spark.createDataFrame(
            [([[1, 2], [3]],), ([[], [4, 5]],), (None,), ([None, [6]],)],
            "values array<array<int>>",
        )

        @pandas_udf(IntegerType())
        def plus_one_pandas(s: pd.Series) -> pd.Series:
            return s + 1

        @arrow_udf(IntegerType())
        def plus_one_arrow(a: pa.Array) -> pa.Array:
            return pa.compute.add(a, 1)

        @pandas_udf(IntegerType())
        def plus_one_pandas_iter(it: Iterator[pd.Series]) -> Iterator[pd.Series]:
            for s in it:
                yield s + 1

        @arrow_udf(IntegerType())
        def plus_one_arrow_iter(it: Iterator[pa.Array]) -> Iterator[pa.Array]:
            for a in it:
                yield pa.compute.add(a, 1)

        native = df.select(
            sf.transform("values", lambda row: sf.transform(row, lambda x: x + 1)).alias("r")
        )
        for f in (plus_one_pandas, plus_one_arrow, plus_one_pandas_iter, plus_one_arrow_iter):
            assertDataFrameEqual(
                df.select(
                    sf.transform("values", lambda row: sf.transform(row, lambda x: f(x))).alias("r")
                ),
                native,
            )

    def test_nested_lambda_with_nondeterministic_inner_argument_fails(self):
        # The inner iterated argument is nondeterministic, so the rewrite - which references it more
        # than once - would evaluate it independently and misalign the results. It must keep failing
        # analysis at the nest root, even though the UDF itself is liftable.
        df = self.spark.createDataFrame([([[1, 2], [3]],)], "values array<array<int>>")
        plus_one = udf(lambda x: x + 1, IntegerType())
        with self.assertRaises(AnalysisException) as ctx:
            df.select(
                sf.transform(
                    "values", lambda row: sf.transform(sf.shuffle(row), lambda x: plus_one(x))
                )
            ).collect()
        self.assertIn("LAMBDA_FUNCTION_WITH_PYTHON_UDF", str(ctx.exception))

    def test_udf_outside_inner_higher_order_function(self):
        # The UDF applies to the outer array's element (itself an array), which *is* a real
        # column, so this is rewritable even though a higher-order function is also present.
        df = self.spark.createDataFrame([([[1, 2], [3]],)], "values array<array<int>>")
        total = udf(lambda a: sum(a), IntegerType())

        assertDataFrameEqual(
            df.select(sf.transform("values", lambda inner: total(inner)).alias("r")),
            [([3, 3],)],
        )

    def test_rewritable_higher_order_function_inside_outer_lambda(self):
        # A rewritable inner HOF over a *real* column, sitting inside an outer HOF's lambda:
        # `transform(arr2, i -> array_max(transform(arr, x -> f(x))) + i)`. Analysis accepts it
        # (the inner `transform` iterates the real column `arr`, not the outer lambda variable),
        # the inner UDF is lifted out of the inner lambda, and the resulting element-wise UDF stays
        # inside the outer lambda for `ExtractPythonUDFs` to extract per row. The result must match
        # the native computation for a deterministic UDF.
        df = self.spark.createDataFrame([([1, 2, 3], [10, 20])], "arr array<int>, arr2 array<int>")
        plus_one = udf(lambda x: x + 1, IntegerType())

        assertDataFrameEqual(
            df.select(
                sf.transform(
                    "arr2",
                    lambda i: sf.array_max(sf.transform("arr", lambda x: plus_one(x))) + i,
                ).alias("r")
            ),
            # array_max([2, 3, 4]) = 4; 4 + 10 = 14, 4 + 20 = 24
            [([14, 24],)],
        )

    def test_sql_string_syntax(self):
        # The rewrite is on the analyzed plan, so it must fire for the SQL string syntax too, not
        # only the DataFrame API.
        self.spark.udf.register("py_plus_one", udf(lambda x: x + 1, IntegerType()))
        with self.temp_view("t"):
            self.spark.createDataFrame([([1, 2, 3],)], "values array<int>").createOrReplaceTempView(
                "t"
            )
            assertDataFrameEqual(
                self.spark.sql("SELECT transform(values, x -> py_plus_one(x)) AS r FROM t"),
                [([2, 3, 4],)],
            )

    def test_kwargs_call(self):
        # A UDF called with a keyword argument is lifted too: the NamedArgumentExpression stays a
        # direct child of the lifted UDF (only its value becomes an aligned array), so the runner
        # still derives the kwargs mapping. add(x, y=10) = x + 10.
        df = self.spark.createDataFrame([([1, 2],), (None,)], "values array<int>")
        add = udf(lambda x, y: x + y, IntegerType())

        assertDataFrameEqual(
            df.select(sf.transform("values", lambda x: add(x, y=sf.lit(10))).alias("r")),
            [([11, 12],), (None,)],
        )

    def test_zero_argument_udf_still_fails(self):
        # A zero-argument UDF has no argument to carry the iterated array's shape, so the rewrite
        # cannot express it; it must keep failing analysis rather than crash the worker at runtime.
        df = self.spark.createDataFrame([([1, 2],)], "values array<int>")
        const = udf(lambda: 7, IntegerType())

        with self.assertRaises(AnalysisException) as ctx:
            df.select(sf.transform("values", lambda x: const())).collect()
        self.assertIn("LAMBDA_FUNCTION_WITH_PYTHON_UDF", str(ctx.exception))

    def test_nondeterministic_iterated_argument_still_fails(self):
        # The rewrite references the iterated argument several times; a nondeterministic one (e.g.
        # shuffle) would be evaluated independently per reference and misalign the results, so it
        # must fail analysis instead of being rewritten.
        df = self.spark.createDataFrame([([1, 2, 3, 4],)], "values array<int>")
        is_even = udf(lambda x: x % 2 == 0, "boolean")

        with self.assertRaises(AnalysisException) as ctx:
            df.select(sf.filter(sf.shuffle("values"), lambda x: is_even(x))).collect()
        self.assertIn("LAMBDA_FUNCTION_WITH_PYTHON_UDF", str(ctx.exception))

    def test_decimal_timestamp_and_struct_element_types(self):
        # Arrow conversion edge cases beyond int/double/string: decimal, timestamp, and a struct
        # return type. Each must round-trip through the element-wise wrapper correctly.
        import datetime
        from decimal import Decimal
        from pyspark.sql.types import (
            DecimalType,
            StructField,
            StructType,
            TimestampType,
        )

        dec_df = self.spark.createDataFrame(
            [([Decimal("1.50"), Decimal("2.25")],)], "values array<decimal(5,2)>"
        )
        add_half = udf(lambda v: v + Decimal("0.50"), DecimalType(5, 2))
        assertDataFrameEqual(
            dec_df.select(sf.transform("values", lambda x: add_half(x)).alias("r")),
            [([Decimal("2.00"), Decimal("2.75")],)],
        )

        ts_df = self.spark.createDataFrame(
            [([datetime.datetime(2020, 1, 1, 0, 0, 0)],)], "values array<timestamp>"
        )
        add_day = udf(lambda t: t + datetime.timedelta(days=1), TimestampType())
        assertDataFrameEqual(
            ts_df.select(sf.transform("values", lambda x: add_day(x)).alias("r")),
            [([datetime.datetime(2020, 1, 2, 0, 0, 0)],)],
        )

        struct_type = StructType([StructField("a", IntegerType()), StructField("b", IntegerType())])
        int_df = self.spark.createDataFrame([([1, 2],)], "values array<int>")
        to_struct = udf(lambda v: (v, v * 10), struct_type)
        assertDataFrameEqual(
            int_df.select(sf.transform("values", lambda x: to_struct(x)).alias("r")),
            [([(1, 10), (2, 20)],)],
        )

    def test_integration_with_joins_grouping_and_caching(self):
        left = self.spark.createDataFrame(
            [
                (1, [1, 2]),
                (
                    2,
                    [3],
                ),
            ],
            "k int, values array<int>",
        )
        right = self.spark.createDataFrame([(1,), (2,)], "k int")
        plus_one = udf(lambda x: x + 1, IntegerType())

        joined = left.join(right, "k").select(
            "k", sf.transform("values", lambda x: plus_one(x)).alias("r")
        )
        assertDataFrameEqual(joined, [(1, [2, 3]), (2, [4])])

        cached = left.select(sf.transform("values", lambda x: plus_one(x)).alias("r")).cache()
        try:
            assertDataFrameEqual(cached, [([2, 3],), ([4],)])
        finally:
            cached.unpersist()

        grouped = left.groupBy().agg(
            sf.sum(
                sf.aggregate(
                    sf.transform("values", lambda x: plus_one(x)), sf.lit(0), lambda a, x: a + x
                )
            ).alias("s")
        )
        # (1+1)+(2+1) + (3+1) = 9
        assertDataFrameEqual(grouped, [(9,)])

    def test_mixed_with_plain_python_udf(self):
        df = self.spark.createDataFrame([([1, 2],)], "values array<int>")
        plus_one = udf(lambda x: x + 1, IntegerType())
        size_udf = udf(lambda a: len(a), IntegerType())

        assertDataFrameEqual(
            df.select(
                sf.transform("values", lambda x: plus_one(x)).alias("r"),
                size_udf("values").alias("n"),
            ),
            [([2, 3], 2)],
        )

    def test_lambda_without_udf_is_unchanged(self):
        # The rewrite must be inert for plans that contain no Python UDF in a lambda.
        df = self.spark.createDataFrame([([1, 2, 3],)], "values array<int>")
        native = df.select(sf.transform("values", lambda x: x + 1).alias("r"))
        self.assertNotIn("pythonUDF", native._jdf.queryExecution().optimizedPlan().toString())
        assertDataFrameEqual(native, [([2, 3, 4],)])

    def test_disabled_by_conf(self):
        df = self.spark.createDataFrame([([1, 2],)], "values array<int>")
        plus_one = udf(lambda x: x + 1, IntegerType())
        with self.sql_conf({"spark.sql.execution.pythonUDF.inHigherOrderFunction.enabled": False}):
            with self.assertRaises(AnalysisException) as ctx:
                df.select(sf.transform("values", lambda x: plus_one(x))).collect()
            self.assertIn("LAMBDA_FUNCTION_WITH_PYTHON_UDF", str(ctx.exception))

    def test_udf_in_aggregate_fails(self):
        # `aggregate` / `reduce` is a sequential fold: the values a UDF sees are outputs of earlier
        # steps, not elements of a collection, so it cannot be applied once to the whole array. A
        # UDF anywhere in `aggregate` / `reduce` - `merge` or `finish` - must fail analysis.
        df = self.spark.createDataFrame([([1, 2],)], "values array<int>")
        plus_one = udf(lambda x: x + 1, IntegerType())
        double_it = udf(lambda acc: acc * 2, IntegerType())

        aggregates = [
            sf.aggregate("values", sf.lit(0), lambda acc, x: acc + plus_one(x)),
            sf.aggregate("values", sf.lit(0), lambda acc, x: plus_one(acc) + x),
            sf.aggregate("values", sf.lit(0), lambda acc, x: acc + x, lambda acc: double_it(acc)),
            # `reduce` is an alias of `aggregate`, so it is rejected the same way.
            sf.reduce("values", sf.lit(0), lambda acc, x: acc + plus_one(x)),
        ]
        for agg in aggregates:
            with self.assertRaises(AnalysisException) as ctx:
                df.select(agg).collect()
            self.assertIn("LAMBDA_FUNCTION_WITH_PYTHON_UDF", str(ctx.exception))

    def test_scalar_pandas_udf_in_lambda(self):
        # A vectorized scalar pandas UDF is lifted and applied over the flattened elements, so it
        # still receives a pandas Series (its native contract) once per batch, not per element.
        import pandas as pd
        from pyspark.sql.functions import pandas_udf

        df = self.spark.createDataFrame(
            [([1, 2, 3],), ([],), (None,), ([10, None],)], "values array<int>"
        )

        @pandas_udf(IntegerType())
        def plus_one_pandas(s: pd.Series) -> pd.Series:
            return s + 1

        assertDataFrameEqual(
            df.select(sf.transform("values", lambda x: plus_one_pandas(x)).alias("r")),
            [([2, 3, 4],), ([],), (None,), ([11, None],)],
        )
        # Arithmetic composition around the UDF result is ordinary JVM work.
        assertDataFrameEqual(
            df.select(sf.transform("values", lambda x: plus_one_pandas(x) * 2).alias("r")),
            [([4, 6, 8],), ([],), (None,), ([22, None],)],
        )

    def test_scalar_arrow_udf_in_lambda(self):
        # A vectorized scalar Arrow UDF is lifted the same way; it takes and returns a pyarrow
        # Array over the flattened elements.
        import pyarrow as pa
        from pyspark.sql.functions import arrow_udf

        df = self.spark.createDataFrame(
            [([1, 2, 3],), ([],), (None,), ([10, None],)], "values array<int>"
        )

        @arrow_udf(IntegerType())
        def plus_one_arrow(a: pa.Array) -> pa.Array:
            return pa.compute.add(a, 1)

        assertDataFrameEqual(
            df.select(sf.transform("values", lambda x: plus_one_arrow(x)).alias("r")),
            [([2, 3, 4],), ([],), (None,), ([11, None],)],
        )
        assertDataFrameEqual(
            df.select(sf.filter("values", lambda x: plus_one_arrow(x) > 2).alias("r")),
            [([2, 3],), ([],), (None,), ([10],)],
        )

    def test_scalar_pandas_iter_udf_in_lambda(self):
        # A scalar iterator pandas UDF keeps its iterator contract: it consumes and produces an
        # iterator of Series. The worker feeds it the flattened elements and re-groups the streamed
        # results back into arrays positionally, so output batch boundaries need not match input.
        from typing import Iterator
        import pandas as pd
        from pyspark.sql.functions import pandas_udf

        df = self.spark.createDataFrame(
            [([1, 2, 3],), ([],), (None,), ([10, 20],), ([5],)], "values array<int>"
        )

        @pandas_udf(IntegerType())
        def plus_one_iter(it: Iterator[pd.Series]) -> Iterator[pd.Series]:
            for s in it:
                yield s + 1

        assertDataFrameEqual(
            df.select(sf.transform("values", lambda x: plus_one_iter(x)).alias("r")),
            [([2, 3, 4],), ([],), (None,), ([11, 21],), ([6],)],
        )

    def test_scalar_arrow_iter_udf_in_lambda(self):
        # A scalar iterator Arrow UDF, lifted the same way as the pandas iterator variant.
        from typing import Iterator
        import pyarrow as pa
        from pyspark.sql.functions import arrow_udf

        df = self.spark.createDataFrame(
            [([1, 2, 3],), ([],), (None,), ([10, 20],), ([5],)], "values array<int>"
        )

        @arrow_udf(IntegerType())
        def plus_one_arrow_iter(it: Iterator[pa.Array]) -> Iterator[pa.Array]:
            for a in it:
                yield pa.compute.add(a, 1)

        assertDataFrameEqual(
            df.select(sf.transform("values", lambda x: plus_one_arrow_iter(x)).alias("r")),
            [([2, 3, 4],), ([],), (None,), ([11, 21],), ([6],)],
        )

    def test_scalar_pandas_iter_udf_multiple_arguments_differ_in_type(self):
        # A two-argument iterator UDF whose arguments have different element types: the array
        # element (int) and an outer column (string) that the rewrite repeats into an aligned
        # array. Each argument must be flattened with its own element type, not the first's.
        from typing import Iterator, Tuple
        import pandas as pd
        from pyspark.sql.functions import pandas_udf

        df = self.spark.createDataFrame(
            [([1, 2, 3], "a"), ([], "b"), (None, "c"), ([10], "d")],
            "values array<int>, tag string",
        )

        @pandas_udf(StringType())
        def tag_each(it: Iterator[Tuple[pd.Series, pd.Series]]) -> Iterator[pd.Series]:
            for x, t in it:
                yield t + x.astype("string")

        assertDataFrameEqual(
            df.select(sf.transform("values", lambda x: tag_each(x, sf.col("tag"))).alias("r")),
            [(["a1", "a2", "a3"],), ([],), (None,), (["d10"],)],
        )

    def test_scalar_pandas_iter_udf_timestamp_return_type(self):
        # A timestamp-returning pandas iterator UDF with a non-UTC session timezone: the result
        # chunks are typed with the session timezone, so the streamed buffer must take its type
        # from the first chunk rather than assuming UTC, or pa.concat_arrays would fail. Assert both
        # against the equivalent non-iterator pandas UDF (isolates the concat fix) and against a
        # native Spark expression computing the same instants (so a timezone bug common to both UDF
        # paths would still be caught, while going through identical driver-collection semantics).
        from typing import Iterator
        import pandas as pd
        from pyspark.sql.functions import pandas_udf
        from pyspark.sql.types import TimestampType

        with self.sql_conf({"spark.sql.session.timeZone": "America/Los_Angeles"}):
            df = self.spark.createDataFrame([([1, 2],), (None,), ([3],)], "values array<int>")

            def compute(x):
                return pd.to_datetime(x, unit="D", origin="2020-01-01")

            @pandas_udf(TimestampType())
            def to_ts(s: pd.Series) -> pd.Series:
                return compute(s)

            @pandas_udf(TimestampType())
            def to_ts_iter(it: Iterator[pd.Series]) -> Iterator[pd.Series]:
                for s in it:
                    yield compute(s)

            iter_df = df.select(sf.transform("values", lambda x: to_ts_iter(x)).alias("r"))
            # Native equivalent: pandas interprets the tz-naive origin in the session timezone, so
            # `timestamp_add(DAY, x, TIMESTAMP '2020-01-01 00:00:00')` (also session-local) matches.
            native_df = df.select(
                sf.transform(
                    "values",
                    lambda x: sf.timestamp_add("DAY", x, sf.lit("2020-01-01").cast("timestamp")),
                ).alias("r")
            )
            # Consistent with the non-iterator pandas UDF, and with the native instants.
            assertDataFrameEqual(
                iter_df,
                df.select(sf.transform("values", lambda x: to_ts(x)).alias("r")),
            )
            assertDataFrameEqual(iter_df, native_df)

    def test_scalar_iter_udf_over_all_empty_and_null_partition(self):
        # SPARK-58695: when a whole partition holds only empty/null arrays, the flattened inputs are
        # all zero-length and a skip-empty iterator UDF yields no chunks. Those rows still need one
        # (empty / null) output row each, or the positional JVM join drops them silently. Cover both
        # the pandas and Arrow iterator flavors.
        from typing import Iterator
        import pandas as pd
        import pyarrow as pa
        from pyspark.sql.functions import pandas_udf, arrow_udf

        # Single partition so the whole batch is empty/null arrays.
        df = self.spark.createDataFrame(
            [([],), (None,), ([],), (None,)], "values array<int>"
        ).coalesce(1)

        @pandas_udf(IntegerType())
        def skip_empty_pandas(it: Iterator[pd.Series]) -> Iterator[pd.Series]:
            for s in it:
                if len(s) == 0:
                    continue
                yield s + 1

        @arrow_udf(IntegerType())
        def skip_empty_arrow(it: Iterator[pa.Array]) -> Iterator[pa.Array]:
            for a in it:
                if len(a) == 0:
                    continue
                yield pa.compute.add(a, 1)

        for f in (skip_empty_pandas, skip_empty_arrow):
            assertDataFrameEqual(
                df.select(sf.transform("values", lambda x: f(x)).alias("r")),
                [([],), (None,), ([],), (None,)],
            )

    def test_scalar_pandas_iter_udf_timestamp_after_empty_batch(self):
        # A zero-length result chunk (from an input batch holding only empty/null arrays) must not
        # pin the output stream's timestamp type to the UTC-typed default: the rows it emits and
        # the rows emitted from a later real chunk (typed with the session timezone) would then
        # disagree, and the Arrow stream writer would reject the second output batch. Assert against
        # the equivalent non-iterator pandas UDF (identical driver-collection semantics), so the
        # check proves the schema fix without depending on absolute timezone offsets.
        from typing import Iterator
        import pandas as pd
        from pyspark.sql.functions import pandas_udf
        from pyspark.sql.types import TimestampType

        with self.sql_conf(
            {
                "spark.sql.session.timeZone": "America/Los_Angeles",
                # One row per Arrow batch so the empty-array row forms its own (first) batch.
                "spark.sql.execution.arrow.maxRecordsPerBatch": "1",
            }
        ):
            df = self.spark.createDataFrame([([],), ([1],)], "values array<int>").coalesce(1)

            def compute(x):
                return pd.to_datetime(x, unit="D", origin="2020-01-01")

            @pandas_udf(TimestampType())
            def to_ts(s: pd.Series) -> pd.Series:
                return compute(s)

            @pandas_udf(TimestampType())
            def to_ts_iter(it: Iterator[pd.Series]) -> Iterator[pd.Series]:
                for s in it:
                    yield compute(s)

            # Without the fix this raises ArrowInvalid ("different schema") writing the second
            # output batch; with it the iterator result matches the non-iterator pandas UDF.
            assertDataFrameEqual(
                df.select(sf.transform("values", lambda x: to_ts_iter(x)).alias("r")),
                df.select(sf.transform("values", lambda x: to_ts(x)).alias("r")),
            )

    def test_scalar_pandas_udf_struct_element_return_type(self):
        # A vectorized pandas UDF returning a struct element (a pandas.DataFrame per batch) inside a
        # lambda. Covers the struct-DataFrame result path of the element-wise pandas branch.
        import pandas as pd
        from pyspark.sql.functions import pandas_udf
        from pyspark.sql.types import StructType, StructField

        df = self.spark.createDataFrame([([1, 2, 3],), ([],), (None,)], "values array<int>")
        ret = StructType([StructField("v", IntegerType()), StructField("neg", IntegerType())])

        @pandas_udf(ret)
        def to_struct(s: pd.Series) -> pd.DataFrame:
            return pd.DataFrame({"v": s, "neg": -s})

        assertDataFrameEqual(
            df.select(sf.transform("values", lambda x: to_struct(x)).alias("r")),
            [
                ([(1, -1), (2, -2), (3, -3)],),
                ([],),
                (None,),
            ],
        )

    def test_chained_vectorized_udfs_in_lambda(self):
        # Nested calls f(g(x)) inside a lambda: g is lifted first, then f consumes g's array result.
        # Cover both the non-iterator and iterator vectorized flavors.
        from typing import Iterator
        import pandas as pd
        from pyspark.sql.functions import pandas_udf

        df = self.spark.createDataFrame(
            [([1, 2, 3],), ([],), (None,), ([10],)], "values array<int>"
        )

        @pandas_udf(IntegerType())
        def plus_one(s: pd.Series) -> pd.Series:
            return s + 1

        @pandas_udf(IntegerType())
        def times_two(s: pd.Series) -> pd.Series:
            return s * 2

        @pandas_udf(IntegerType())
        def plus_one_iter(it: Iterator[pd.Series]) -> Iterator[pd.Series]:
            for s in it:
                yield s + 1

        @pandas_udf(IntegerType())
        def times_two_iter(it: Iterator[pd.Series]) -> Iterator[pd.Series]:
            for s in it:
                yield s * 2

        # (x + 1) * 2, verified against the equivalent native expression.
        assertDataFrameEqual(
            df.select(sf.transform("values", lambda x: times_two(plus_one(x))).alias("r")),
            df.select(sf.transform("values", lambda x: (x + 1) * 2).alias("r")),
        )
        assertDataFrameEqual(
            df.select(
                sf.transform("values", lambda x: times_two_iter(plus_one_iter(x))).alias("r")
            ),
            df.select(sf.transform("values", lambda x: (x + 1) * 2).alias("r")),
        )

    def test_scalar_iter_udf_struct_element_return_type(self):
        # A scalar iterator pandas UDF returning a struct element (a pandas.DataFrame per batch)
        # inside a lambda. Covers the struct-DataFrame result path of the iterator element-wise
        # branch (Iterator[pd.DataFrame] contract).
        from typing import Iterator
        import pandas as pd
        from pyspark.sql.functions import pandas_udf
        from pyspark.sql.types import StructType, StructField

        df = self.spark.createDataFrame([([1, 2, 3],), ([],), (None,)], "values array<int>")
        ret = StructType([StructField("v", IntegerType()), StructField("neg", IntegerType())])

        @pandas_udf(ret)
        def to_struct_iter(it: Iterator[pd.Series]) -> Iterator[pd.DataFrame]:
            for s in it:
                yield pd.DataFrame({"v": s, "neg": -s})

        assertDataFrameEqual(
            df.select(sf.transform("values", lambda x: to_struct_iter(x)).alias("r")),
            [
                ([(1, -1), (2, -2), (3, -3)],),
                ([],),
                (None,),
            ],
        )

    def test_non_arrow_udf_is_also_supported(self):
        # A UDF created with useArrow=False is still rewritable; the generated array wrapper
        # is Arrow-based regardless of how the user's UDF was declared.
        df = self.spark.createDataFrame([([1, 2, 3],)], "values array<int>")
        plus_one = udf(lambda x: x + 1, IntegerType(), useArrow=False)

        assertDataFrameEqual(
            df.select(sf.transform("values", lambda x: plus_one(x)).alias("r")),
            [([2, 3, 4],)],
        )

    def test_string_return_type(self):
        df = self.spark.createDataFrame([([1, 2],), (None,)], "values array<int>")
        to_str = udf(lambda x: f"v{x}", StringType())
        assertDataFrameEqual(
            df.select(sf.transform("values", lambda x: to_str(x)).alias("r")),
            [(["v1", "v2"],), (None,)],
        )


class UDFInHigherOrderFunctionTests(UDFInHigherOrderFunctionTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.testing import main

    main()
