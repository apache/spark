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
from inspect import signature
from typing import Union, Iterator, Tuple, get_type_hints

from pyspark.sql import functions as sf
from pyspark.testing.utils import (
    have_pandas,
    pandas_requirement_message,
    have_pyarrow,
    pyarrow_requirement_message,
    have_numpy,
    numpy_requirement_message,
)
from pyspark.testing.sqlutils import ReusedSQLTestCase
from pyspark.sql.pandas.typehints import infer_eval_type, infer_group_arrow_eval_type
from pyspark.sql.pandas.functions import arrow_udf, ArrowUDFType
from pyspark.sql import Row
from pyspark.util import PythonEvalType

if have_pyarrow:
    import pyarrow as pa


@unittest.skipIf(not have_pyarrow, pyarrow_requirement_message)
class ArrowUDFTypeHintsTests(ReusedSQLTestCase):
    def test_type_annotation_scalar(self):
        def func(col: pa.Array) -> pa.Array:
            pass

        self.assertEqual(
            infer_eval_type(signature(func), get_type_hints(func)), ArrowUDFType.SCALAR
        )

        def func(col: pa.Array, col1: pa.Array) -> pa.Array:
            pass

        self.assertEqual(
            infer_eval_type(signature(func), get_type_hints(func)), ArrowUDFType.SCALAR
        )

        def func(col: pa.Array, *args: pa.Array) -> pa.Array:
            pass

        self.assertEqual(
            infer_eval_type(signature(func), get_type_hints(func)), ArrowUDFType.SCALAR
        )

        def func(col: pa.Array, *args: pa.Array, **kwargs: pa.Array) -> pa.Array:
            pass

        self.assertEqual(
            infer_eval_type(signature(func), get_type_hints(func)), ArrowUDFType.SCALAR
        )

        def func(col: pa.Array, *, col2: pa.Array) -> pa.Array:
            pass

        self.assertEqual(
            infer_eval_type(signature(func), get_type_hints(func)), ArrowUDFType.SCALAR
        )

        # Union[pa.Array, pa.Array] equals to pa.Array
        def func(col: Union[pa.Array, pa.Array], *, col2: pa.Array) -> pa.Array:
            pass

        self.assertEqual(
            infer_eval_type(signature(func), get_type_hints(func)), ArrowUDFType.SCALAR
        )

    def test_type_annotation_scalar_iter(self):
        def func(iter: Iterator[pa.Array]) -> Iterator[pa.Array]:
            pass

        self.assertEqual(
            infer_eval_type(signature(func), get_type_hints(func)), ArrowUDFType.SCALAR_ITER
        )

        def func(iter: Iterator[Tuple[pa.Array, ...]]) -> Iterator[pa.Array]:
            pass

        self.assertEqual(
            infer_eval_type(signature(func), get_type_hints(func)), ArrowUDFType.SCALAR_ITER
        )

    def test_type_annotation_tuple_generics(self):
        def func(iter: Iterator[tuple[pa.Array, pa.Array]]) -> Iterator[pa.Array]:
            pass

        self.assertEqual(
            infer_eval_type(signature(func), get_type_hints(func)), ArrowUDFType.SCALAR_ITER
        )

        def func(iter: Iterator[tuple[pa.Array, ...]]) -> Iterator[pa.Array]:
            pass

        self.assertEqual(
            infer_eval_type(signature(func), get_type_hints(func)), ArrowUDFType.SCALAR_ITER
        )

        # Union[pa.Array, pa.Array] equals to pa.Array
        def func(iter: Iterator[tuple[Union[pa.Array, pa.Array], ...]]) -> Iterator[pa.Array]:
            pass

        self.assertEqual(
            infer_eval_type(signature(func), get_type_hints(func)), ArrowUDFType.SCALAR_ITER
        )

    def test_type_annotation_group_agg(self):
        def func(col: pa.Array) -> str:
            pass

        self.assertEqual(
            infer_eval_type(signature(func), get_type_hints(func)), ArrowUDFType.GROUPED_AGG
        )

        def func(col: pa.Array, col1: pa.Array) -> int:
            pass

        self.assertEqual(
            infer_eval_type(signature(func), get_type_hints(func)), ArrowUDFType.GROUPED_AGG
        )

        def func(col: pa.Array, *args: pa.Array) -> Row:
            pass

        self.assertEqual(
            infer_eval_type(signature(func), get_type_hints(func)), ArrowUDFType.GROUPED_AGG
        )

        def func(col: pa.Array, *args: pa.Array, **kwargs: pa.Array) -> str:
            pass

        self.assertEqual(
            infer_eval_type(signature(func), get_type_hints(func)), ArrowUDFType.GROUPED_AGG
        )

        def func(col: pa.Array, *, col2: pa.Array) -> float:
            pass

        self.assertEqual(
            infer_eval_type(signature(func), get_type_hints(func)), ArrowUDFType.GROUPED_AGG
        )

        # Union[pa.Array, pa.Array] equals to pa.Array
        def func(col: Union[pa.Array, pa.Array], *, col2: pa.Array) -> float:
            pass

        self.assertEqual(
            infer_eval_type(signature(func), get_type_hints(func)), ArrowUDFType.GROUPED_AGG
        )

        def func() -> float:
            pass

        self.assertEqual(
            infer_eval_type(signature(func), get_type_hints(func), "arrow"),
            ArrowUDFType.GROUPED_AGG,
        )

    def test_type_annotation_group_agg_iter(self):
        def func(iter: Iterator[pa.Array]) -> float:
            pass

        self.assertEqual(
            infer_eval_type(signature(func), get_type_hints(func)), ArrowUDFType.GROUPED_AGG_ITER
        )

        def func(iter: Iterator[Tuple[pa.Array, pa.Array]]) -> int:
            pass

        self.assertEqual(
            infer_eval_type(signature(func), get_type_hints(func)), ArrowUDFType.GROUPED_AGG_ITER
        )

        def func(iter: Iterator[Tuple[pa.Array, ...]]) -> str:
            pass

        self.assertEqual(
            infer_eval_type(signature(func), get_type_hints(func)), ArrowUDFType.GROUPED_AGG_ITER
        )

        # Union[pa.Array, pa.Array] equals to pa.Array
        def func(iter: Iterator[Union[pa.Array, pa.Array]]) -> float:
            pass

        self.assertEqual(
            infer_eval_type(signature(func), get_type_hints(func)), ArrowUDFType.GROUPED_AGG_ITER
        )

        def func(iter: Iterator[tuple[pa.Array, pa.Array]]) -> float:
            pass

        self.assertEqual(
            infer_eval_type(signature(func), get_type_hints(func)), ArrowUDFType.GROUPED_AGG_ITER
        )

    def test_type_annotation_group_map(self):
        def func(col: pa.Table) -> pa.Table:
            pass

        self.assertEqual(
            infer_group_arrow_eval_type(signature(func), get_type_hints(func)),
            PythonEvalType.SQL_GROUPED_MAP_ARROW_UDF,
        )

        def func(col: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
            pass

        self.assertEqual(
            infer_group_arrow_eval_type(signature(func), get_type_hints(func)),
            PythonEvalType.SQL_GROUPED_MAP_ARROW_ITER_UDF,
        )

        def func(col: Iterator[pa.Array]) -> Iterator[pa.Array]:
            pass

        self.assertEqual(infer_group_arrow_eval_type(signature(func), get_type_hints(func)), None)

    def test_type_annotation_negative(self):
        def func(col: str) -> pa.Array:
            pass

        self.assertRaisesRegex(
            NotImplementedError,
            "Unsupported signature.*str",
            infer_eval_type,
            signature(func),
            get_type_hints(func),
        )

        def func(col: pa.Array, col1: int) -> pa.Array:
            pass

        self.assertRaisesRegex(
            NotImplementedError,
            "Unsupported signature.*int",
            infer_eval_type,
            signature(func),
            get_type_hints(func),
        )

        def func(col: Union[pa.Array, str], col1: int) -> pa.Array:
            pass

        self.assertRaisesRegex(
            NotImplementedError,
            "Unsupported signature.*str",
            infer_eval_type,
            signature(func),
            get_type_hints(func),
        )

        def func(col: pa.Array) -> Tuple[pa.Array]:
            pass

        self.assertRaisesRegex(
            NotImplementedError,
            "Unsupported signature.*Tuple",
            infer_eval_type,
            signature(func),
            get_type_hints(func),
        )

        def func(col, *args: pa.Array) -> pa.Array:
            pass

        self.assertRaisesRegex(
            ValueError,
            "should be specified.*Array",
            infer_eval_type,
            signature(func),
            get_type_hints(func),
        )

        def func(col: pa.Array, *args: pa.Array, **kwargs: pa.Array):
            pass

        self.assertRaisesRegex(
            ValueError,
            "should be specified.*Array",
            infer_eval_type,
            signature(func),
            get_type_hints(func),
        )

        def func(col: pa.Array, *, col2) -> pa.Array:
            pass

        self.assertRaisesRegex(
            ValueError,
            "should be specified.*Array",
            infer_eval_type,
            signature(func),
            get_type_hints(func),
        )

    def test_scalar_udf_type_hint(self):
        df = self.spark.range(10).selectExpr("id", "id as v")

        def plus_one(v: pa.Array) -> pa.Array:
            return pa.compute.add(v, 1)

        plus_one = arrow_udf("long")(plus_one)
        actual = df.select(plus_one(df.v).alias("plus_one"))
        expected = df.selectExpr("(v + 1) as plus_one")
        self.assertEqual(expected.collect(), actual.collect())

    def test_scalar_iter_udf_type_hint(self):
        df = self.spark.range(10).selectExpr("id", "id as v")

        def plus_one(itr: Iterator[pa.Array]) -> Iterator[pa.Array]:
            for s in itr:
                yield pa.compute.add(s, 1)

        plus_one = arrow_udf("long")(plus_one)

        actual = df.select(plus_one(df.v).alias("plus_one"))
        expected = df.selectExpr("(v + 1) as plus_one")
        self.assertEqual(expected.collect(), actual.collect())

    @unittest.skipIf(not have_numpy, numpy_requirement_message)
    def test_group_agg_udf_type_hint(self):
        import numpy as np

        df = self.spark.range(10).selectExpr("id", "id as v")

        def weighted_mean(v: pa.Array, w: pa.Array) -> np.float64:
            return np.average(v, weights=w)

        weighted_mean = arrow_udf("double")(weighted_mean)

        actual = df.groupby("id").agg(weighted_mean(df.v, sf.lit(1.0))).sort("id")
        expected = df.groupby("id").agg(sf.mean(df.v).alias("weighted_mean(v, 1.0)")).sort("id")
        self.assertEqual(expected.collect(), actual.collect())

    @unittest.skipIf(not have_numpy, numpy_requirement_message)
    def test_group_agg_iter_udf_type_hint(self):
        import numpy as np

        df = self.spark.createDataFrame(
            [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)], ("id", "v")
        )

        def arrow_mean_iter(it: Iterator[pa.Array]) -> float:
            sum_val = 0.0
            cnt = 0
            for v in it:
                sum_val += pa.compute.sum(v).as_py()
                cnt += len(v)
            return sum_val / cnt if cnt > 0 else 0.0

        arrow_mean_iter = arrow_udf("double")(arrow_mean_iter)

        actual = df.groupby("id").agg(arrow_mean_iter(df["v"]).alias("mean")).sort("id")
        expected = df.groupby("id").agg(sf.mean(df["v"]).alias("mean")).sort("id")
        self.assertEqual(expected.collect(), actual.collect())

        # Test with multiple columns
        df2 = self.spark.createDataFrame(
            [(1, 1.0, 1.0), (1, 2.0, 2.0), (2, 3.0, 1.0), (2, 5.0, 2.0), (2, 10.0, 3.0)],
            ("id", "v", "w"),
        )

        def arrow_weighted_mean_iter(it: Iterator[Tuple[pa.Array, pa.Array]]) -> float:
            weighted_sum = 0.0
            weight = 0.0
            for v, w in it:
                weighted_sum += np.dot(v, w)
                weight += pa.compute.sum(w).as_py()
            return weighted_sum / weight if weight > 0 else 0.0

        arrow_weighted_mean_iter = arrow_udf("double")(arrow_weighted_mean_iter)

        actual2 = (
            df2.groupby("id")
            .agg(arrow_weighted_mean_iter(df2["v"], df2["w"]).alias("wm"))
            .sort("id")
        )
        # Expected weighted means:
        # Group 1: (1.0*1.0 + 2.0*2.0) / (1.0 + 2.0) = 5.0 / 3.0
        # Group 2: (3.0*1.0 + 5.0*2.0 + 10.0*3.0) / (1.0 + 2.0 + 3.0) = 43.0 / 6.0
        expected_results = [(1, 5.0 / 3.0), (2, 43.0 / 6.0)]
        actual_results = actual2.collect()
        self.assertEqual(len(actual_results), len(expected_results))
        for (id_val, exp_wm), actual_row in zip(expected_results, actual_results):
            self.assertEqual(actual_row["id"], id_val)
            self.assertAlmostEqual(actual_row["wm"], exp_wm, places=5)

    def test_string_type_annotation(self):
        def func(col: "pa.Array") -> "pa.Array":
            pass

        self.assertEqual(
            infer_eval_type(signature(func), get_type_hints(func)), ArrowUDFType.SCALAR
        )

        def func(col: "pa.Array", col1: "pa.Array") -> "pa.Array":
            pass

        self.assertEqual(
            infer_eval_type(signature(func), get_type_hints(func)), ArrowUDFType.SCALAR
        )

        def func(col: "pa.Array", *args: "pa.Array") -> "pa.Array":
            pass

        self.assertEqual(
            infer_eval_type(signature(func), get_type_hints(func)), ArrowUDFType.SCALAR
        )

        def func(col: "pa.Array", *args: "pa.Array", **kwargs: "pa.Array") -> "pa.Array":
            pass

        self.assertEqual(
            infer_eval_type(signature(func), get_type_hints(func)), ArrowUDFType.SCALAR
        )

        def func(col: "pa.Array", *, col2: "pa.Array") -> "pa.Array":
            pass

        self.assertEqual(
            infer_eval_type(signature(func), get_type_hints(func)), ArrowUDFType.SCALAR
        )

        # Union[pa.Array, pa.Array] equals to pa.Array
        def func(col: Union["pa.Array", "pa.Array"], *, col2: "pa.Array") -> "pa.Array":
            pass

        self.assertEqual(
            infer_eval_type(signature(func), get_type_hints(func)), ArrowUDFType.SCALAR
        )

    @unittest.skipIf(not have_pandas, pandas_requirement_message)
    def test_negative_with_pandas_udf(self):
        import pandas as pd

        with self.assertRaisesRegex(
            Exception,
            "Unsupported signature:.*pandas.core.series.Series.",
        ):

            @arrow_udf("long")
            def multiply_pandas(a: pd.Series, b: pd.Series) -> pd.Series:
                return a * b


if __name__ == "__main__":
    from pyspark.sql.tests.arrow.test_arrow_udf_typehints import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
