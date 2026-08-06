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
        df = self.spark.createDataFrame(
            [([1, None, 3],), (None,), ([],)], "values array<int>"
        )
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

    def test_udf_with_outer_column_argument(self):
        # A non-element argument must be broadcast to every element of its row.
        df = self.spark.createDataFrame(
            [([1, 2], 100), ([3], 200)], "values array<int>, base int"
        )
        add = udf(lambda x, b: x + b, IntegerType())

        assertDataFrameEqual(
            df.select(sf.transform("values", lambda x: add(x, sf.col("base"))).alias("r")),
            [([101, 102],), ([203],)],
        )

    def test_udf_with_constant_argument_only(self):
        # SPARK-27052: `transform(arr, x -> udf(lit(10)))` must still yield one result per
        # element rather than a single value.
        df = self.spark.createDataFrame([([1, 2, 3],), ([],)], "values array<int>")
        const = udf(lambda v: v * 2, IntegerType())

        assertDataFrameEqual(
            df.select(sf.transform("values", lambda x: const(sf.lit(10))).alias("r")),
            [([20, 20, 20],), ([],)],
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

    def test_aggregate_udf_on_element(self):
        # Only `merge`'s element argument iterates, so a UDF there is precomputable.
        df = self.spark.createDataFrame([([1, 2, 3],), ([],), (None,)], "values array<int>")
        plus_one = udf(lambda x: x + 1, IntegerType())

        assertDataFrameEqual(
            df.select(
                sf.aggregate("values", sf.lit(0), lambda acc, x: acc + plus_one(x)).alias("r")
            ),
            df.select(
                sf.aggregate("values", sf.lit(0), lambda acc, x: acc + (x + 1)).alias("r")
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
        # A comparator cannot be evaluated pairwise, but the UDF applied per element is a sort key
        # the JVM comparator compares. This must actually reorder, not be a no-op.
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

    def test_array_sort_pairwise_comparator_still_fails(self):
        # One UDF call receiving both elements has no per-element key, so it must keep failing.
        df = self.spark.createDataFrame([([3, 1, 2],)], "values array<int>")
        cmp_udf = udf(lambda a, b: (a > b) - (a < b), IntegerType())

        with self.assertRaises(AnalysisException) as ctx:
            df.select(sf.array_sort("values", lambda a, b: cmp_udf(a, b))).collect()
        self.assertIn("LAMBDA_FUNCTION_WITH_PYTHON_UDF", str(ctx.exception))

    def test_transform_keys_and_values(self):
        df = self.spark.createDataFrame([({"a": 1, "b": 2},),], "m map<string,int>")
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
            df.select(
                sf.transform_values("m", lambda k, v: plus_one(v) + sf.length(k)).alias("r")
            ),
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
            [({"a": 1, "b": 2}, {"b": 20, "c": 30},)], "l map<string,int>, r map<string,int>"
        )
        combine = udf(
            lambda a, b: (0 if a is None else a) * 100 + (0 if b is None else b), IntegerType()
        )

        assertDataFrameEqual(
            df.select(sf.map_zip_with("l", "r", lambda k, v1, v2: combine(v1, v2)).alias("r")),
            [({"a": 100, "b": 220, "c": 30},)],
        )

    def test_aggregate_udf_in_finish(self):
        # `finish` runs once on the final accumulator. Critically, a fold over a null array is
        # null and Spark does not evaluate `finish` for it, so the UDF must not see that null.
        df = self.spark.createDataFrame(
            [([1, 2, 3],), ([],), (None,)], "values array<int>"
        )
        # Deliberately null-unaware: it would raise if called on a null accumulator.
        double_it = udf(lambda acc: acc * 2, IntegerType())

        assertDataFrameEqual(
            df.select(
                sf.aggregate(
                    "values", sf.lit(0), lambda acc, x: acc + x, lambda acc: double_it(acc)
                ).alias("r")
            ),
            [(12,), (0,), (None,)],
        )

    def test_aggregate_udf_in_both_merge_and_finish(self):
        df = self.spark.createDataFrame([([1, 2, 3],), (None,)], "values array<int>")
        plus_one = udf(lambda x: x + 1, IntegerType())
        double_it = udf(lambda acc: acc * 2, IntegerType())

        # (1+1)+(2+1)+(3+1) = 9, doubled = 18
        assertDataFrameEqual(
            df.select(
                sf.aggregate(
                    "values",
                    sf.lit(0),
                    lambda acc, x: acc + plus_one(x),
                    lambda acc: double_it(acc),
                ).alias("r")
            ),
            [(18,), (None,)],
        )

    def test_reduce_alias(self):
        df = self.spark.createDataFrame([([1, 2, 3],)], "values array<int>")
        plus_one = udf(lambda x: x + 1, IntegerType())
        assertDataFrameEqual(
            df.select(sf.reduce("values", sf.lit(0), lambda acc, x: acc + plus_one(x)).alias("r")),
            [(9,)],
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

    def test_nested_higher_order_functions(self):
        # The inner array is the outer lambda's variable, so the UDF cannot be lifted in one pass.
        # The rule rewrites innermost-first and repeats to a fixed point, which resolves it.
        df = self.spark.createDataFrame(
            [([[1, 2], [3]],), ([],), (None,), ([None, []],)], "values array<array<int>>"
        )
        plus_one = udf(lambda x: x + 1, IntegerType())

        assertDataFrameEqual(
            df.select(
                sf.transform(
                    "values", lambda inner: sf.transform(inner, lambda x: plus_one(x))
                ).alias("r")
            ),
            df.select(
                sf.transform("values", lambda inner: sf.transform(inner, lambda x: x + 1)).alias(
                    "r"
                )
            ),
        )

    def test_nested_higher_order_functions_three_deep(self):
        df = self.spark.createDataFrame(
            [([[[1, 2], [3]], [[4]]],)], "values array<array<array<int>>>"
        )
        plus_one = udf(lambda x: x + 1, IntegerType())

        assertDataFrameEqual(
            df.select(
                sf.transform(
                    "values",
                    lambda a: sf.transform(a, lambda b: sf.transform(b, lambda x: plus_one(x))),
                ).alias("r")
            ),
            [([[[2, 3], [4]], [[5]]],)],
        )

    def test_nested_higher_order_function_mixed_kinds(self):
        # A `filter` inside a `transform`, with the UDF in the inner predicate.
        df = self.spark.createDataFrame([([[1, 2, 3], [4, 5]],)], "values array<array<int>>")
        is_even = udf(lambda x: x % 2 == 0, "boolean")

        assertDataFrameEqual(
            df.select(
                sf.transform(
                    "values", lambda inner: sf.filter(inner, lambda x: is_even(x))
                ).alias("r")
            ),
            [([[2], [4]],)],
        )

    def test_udf_outside_inner_higher_order_function(self):
        # The UDF applies to the outer array's element (itself an array), which *is* a real
        # column, so this is rewritable even though a higher-order function is also present.
        df = self.spark.createDataFrame([([[1, 2], [3]],)], "values array<array<int>>")
        total = udf(lambda a: sum(a), IntegerType())

        assertDataFrameEqual(
            df.select(sf.transform("values", lambda inner: total(inner)).alias("r")),
            [([3, 3],)],
        )

    def test_integration_with_joins_grouping_and_caching(self):
        left = self.spark.createDataFrame([(1, [1, 2]), (2, [3],)], "k int, values array<int>")
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
            sf.sum(sf.aggregate(
                sf.transform("values", lambda x: plus_one(x)), sf.lit(0), lambda a, x: a + x
            )).alias("s")
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
        with self.sql_conf(
            {"spark.sql.execution.pythonUDF.inHigherOrderFunction.enabled": False}
        ):
            with self.assertRaises(AnalysisException) as ctx:
                df.select(sf.transform("values", lambda x: plus_one(x))).collect()
            self.assertIn("LAMBDA_FUNCTION_WITH_PYTHON_UDF", str(ctx.exception))

    def test_unsupported_shapes_still_fail(self):
        df = self.spark.createDataFrame([([1, 2],)], "values array<int>")
        plus_one = udf(lambda x: x + 1, IntegerType())

        # A UDF on `aggregate`'s accumulator is sequential: there is no array to precompute
        # over, so it must keep failing rather than return a wrong answer.
        with self.assertRaises(AnalysisException) as ctx:
            df.select(
                sf.aggregate("values", sf.lit(0), lambda acc, x: plus_one(acc) + x)
            ).collect()
        self.assertIn("LAMBDA_FUNCTION_WITH_PYTHON_UDF", str(ctx.exception))

        # A UDF in `finish` is not rewritten: a fold over a null array is null, and native
        # Spark does not evaluate `finish` for it.
        with self.assertRaises(AnalysisException) as ctx:
            df.select(
                sf.aggregate(
                    "values", sf.lit(0), lambda acc, x: acc + x, lambda acc: plus_one(acc)
                )
            ).collect()
        self.assertIn("LAMBDA_FUNCTION_WITH_PYTHON_UDF", str(ctx.exception))

    def test_pandas_udf_in_lambda_still_fails(self):
        # A pandas UDF receives a Series, not one value per call, so the element-wise rewrite
        # is not valid for it.
        import pandas as pd
        from pyspark.sql.functions import pandas_udf

        df = self.spark.createDataFrame([([1, 2],)], "values array<int>")

        @pandas_udf(IntegerType())
        def plus_one_pandas(s: pd.Series) -> pd.Series:
            return s + 1

        with self.assertRaises(AnalysisException) as ctx:
            df.select(sf.transform("values", lambda x: plus_one_pandas(x))).collect()
        self.assertIn("LAMBDA_FUNCTION_WITH_PYTHON_UDF", str(ctx.exception))

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
    from pyspark.sql.tests.test_udf_in_higher_order_function import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
