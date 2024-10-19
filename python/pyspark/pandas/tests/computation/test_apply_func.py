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
from datetime import datetime
import sys
import unittest
from typing import List

import numpy as np
import pandas as pd

from pyspark import pandas as ps
from pyspark.loose_version import LooseVersion
from pyspark.pandas.config import option_context
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


# This file contains test cases for 'Function application, GroupBy & Window'
# https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/frame.html#function-application-groupby-window
# as well as 'apply_batch*' and 'transform_batch*'.
class FrameApplyFunctionMixin:
    @property
    def pdf(self):
        return pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9], "b": [4, 5, 6, 3, 2, 1, 0, 0, 0]},
            index=np.random.rand(9),
        )

    @property
    def psdf(self):
        return ps.from_pandas(self.pdf)

    def test_apply(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 2, 3, 4, 5, 6] * 100,
                "b": [1.0, 1.0, 2.0, 3.0, 5.0, 8.0] * 100,
                "c": [1, 4, 9, 16, 25, 36] * 100,
            },
            columns=["a", "b", "c"],
            index=np.random.rand(600),
        )
        psdf = ps.DataFrame(pdf)

        self.assert_eq(
            psdf.apply(lambda x: x + 1).sort_index(), pdf.apply(lambda x: x + 1).sort_index()
        )
        self.assert_eq(
            psdf.apply(lambda x, b: x + b, args=(1,)).sort_index(),
            pdf.apply(lambda x, b: x + b, args=(1,)).sort_index(),
        )
        self.assert_eq(
            psdf.apply(lambda x, b: x + b, b=1).sort_index(),
            pdf.apply(lambda x, b: x + b, b=1).sort_index(),
        )

        with option_context("compute.shortcut_limit", 500):
            self.assert_eq(
                psdf.apply(lambda x: x + 1).sort_index(), pdf.apply(lambda x: x + 1).sort_index()
            )
            self.assert_eq(
                psdf.apply(lambda x, b: x + b, args=(1,)).sort_index(),
                pdf.apply(lambda x, b: x + b, args=(1,)).sort_index(),
            )
            self.assert_eq(
                psdf.apply(lambda x, b: x + b, b=1).sort_index(),
                pdf.apply(lambda x, b: x + b, b=1).sort_index(),
            )

        # returning a Series
        self.assert_eq(
            psdf.apply(lambda x: len(x), axis=1).sort_index(),
            pdf.apply(lambda x: len(x), axis=1).sort_index(),
        )
        self.assert_eq(
            psdf.apply(lambda x, c: len(x) + c, axis=1, c=100).sort_index(),
            pdf.apply(lambda x, c: len(x) + c, axis=1, c=100).sort_index(),
        )
        with option_context("compute.shortcut_limit", 500):
            self.assert_eq(
                psdf.apply(lambda x: len(x), axis=1).sort_index(),
                pdf.apply(lambda x: len(x), axis=1).sort_index(),
            )
            self.assert_eq(
                psdf.apply(lambda x, c: len(x) + c, axis=1, c=100).sort_index(),
                pdf.apply(lambda x, c: len(x) + c, axis=1, c=100).sort_index(),
            )

        with self.assertRaisesRegex(AssertionError, "the first argument should be a callable"):
            psdf.apply(1)

        with self.assertRaisesRegex(TypeError, "The given function.*1 or 'column'; however"):

            def f1(_) -> ps.DataFrame[int]:
                pass

            psdf.apply(f1, axis=0)

        with self.assertRaisesRegex(TypeError, "The given function.*0 or 'index'; however"):

            def f2(_) -> ps.Series[int]:
                pass

            psdf.apply(f2, axis=1)

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.apply(lambda x: x + 1).sort_index(), pdf.apply(lambda x: x + 1).sort_index()
        )
        with option_context("compute.shortcut_limit", 500):
            self.assert_eq(
                psdf.apply(lambda x: x + 1).sort_index(), pdf.apply(lambda x: x + 1).sort_index()
            )

        # returning a Series
        self.assert_eq(
            psdf.apply(lambda x: len(x), axis=1).sort_index(),
            pdf.apply(lambda x: len(x), axis=1).sort_index(),
        )
        with option_context("compute.shortcut_limit", 500):
            self.assert_eq(
                psdf.apply(lambda x: len(x), axis=1).sort_index(),
                pdf.apply(lambda x: len(x), axis=1).sort_index(),
            )

    def test_apply_with_type(self):
        pdf = self.pdf
        psdf = ps.from_pandas(pdf)

        def identify1(x) -> ps.DataFrame[int, int]:
            return x

        # Type hints set the default column names, and we use default index for
        # pandas API on Spark. Here we ignore both diff.
        actual = psdf.apply(identify1, axis=1)
        expected = pdf.apply(identify1, axis=1)
        self.assert_eq(sorted(actual["c0"].to_numpy()), sorted(expected["a"].to_numpy()))
        self.assert_eq(sorted(actual["c1"].to_numpy()), sorted(expected["b"].to_numpy()))

        def identify2(x) -> ps.DataFrame[slice("a", int), slice("b", int)]:  # noqa: F405
            return x

        actual = psdf.apply(identify2, axis=1)
        expected = pdf.apply(identify2, axis=1)
        self.assert_eq(sorted(actual["a"].to_numpy()), sorted(expected["a"].to_numpy()))
        self.assert_eq(sorted(actual["b"].to_numpy()), sorted(expected["b"].to_numpy()))

    def test_apply_batch(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 2, 3, 4, 5, 6] * 100,
                "b": [1.0, 1.0, 2.0, 3.0, 5.0, 8.0] * 100,
                "c": [1, 4, 9, 16, 25, 36] * 100,
            },
            columns=["a", "b", "c"],
            index=np.random.rand(600),
        )
        psdf = ps.DataFrame(pdf)

        self.assert_eq(
            psdf.pandas_on_spark.apply_batch(lambda pdf, a: pdf + a, args=(1,)).sort_index(),
            (pdf + 1).sort_index(),
        )
        with option_context("compute.shortcut_limit", 500):
            self.assert_eq(
                psdf.pandas_on_spark.apply_batch(lambda pdf: pdf + 1).sort_index(),
                (pdf + 1).sort_index(),
            )
            self.assert_eq(
                psdf.pandas_on_spark.apply_batch(lambda pdf, b: pdf + b, b=1).sort_index(),
                (pdf + 1).sort_index(),
            )

        with self.assertRaisesRegex(AssertionError, "the first argument should be a callable"):
            psdf.pandas_on_spark.apply_batch(1)

        with self.assertRaisesRegex(TypeError, "The given function.*frame as its type hints"):

            def f2(_) -> ps.Series[int]:
                pass

            psdf.pandas_on_spark.apply_batch(f2)

        with self.assertRaisesRegex(ValueError, "The given function should return a frame"):
            psdf.pandas_on_spark.apply_batch(lambda pdf: 1)

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.pandas_on_spark.apply_batch(lambda x: x + 1).sort_index(), (pdf + 1).sort_index()
        )
        with option_context("compute.shortcut_limit", 500):
            self.assert_eq(
                psdf.pandas_on_spark.apply_batch(lambda x: x + 1).sort_index(),
                (pdf + 1).sort_index(),
            )

    def test_apply_batch_with_type(self):
        pdf = self.pdf
        psdf = ps.from_pandas(pdf)

        def identify1(x) -> ps.DataFrame[int, int]:
            return x

        # Type hints set the default column names, and we use default index for
        # pandas API on Spark. Here we ignore both diff.
        actual = psdf.pandas_on_spark.apply_batch(identify1)
        expected = pdf
        self.assert_eq(sorted(actual["c0"].to_numpy()), sorted(expected["a"].to_numpy()))
        self.assert_eq(sorted(actual["c1"].to_numpy()), sorted(expected["b"].to_numpy()))

        def identify2(x) -> ps.DataFrame[slice("a", int), slice("b", int)]:  # noqa: F405
            return x

        actual = psdf.pandas_on_spark.apply_batch(identify2)
        expected = pdf
        self.assert_eq(sorted(actual["a"].to_numpy()), sorted(expected["a"].to_numpy()))
        self.assert_eq(sorted(actual["b"].to_numpy()), sorted(expected["b"].to_numpy()))

        pdf = pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9], "b": [[e] for e in [4, 5, 6, 3, 2, 1, 0, 0, 0]]},
            index=np.random.rand(9),
        )
        psdf = ps.from_pandas(pdf)

        def identify3(x) -> ps.DataFrame[float, [int, List[int]]]:
            return x

        actual = psdf.pandas_on_spark.apply_batch(identify3)
        actual.columns = ["a", "b"]
        self.assert_eq(actual, pdf)

        # For NumPy typing, NumPy version should be 1.21+
        if LooseVersion(np.__version__) >= LooseVersion("1.21"):
            import numpy.typing as ntp

            psdf = ps.from_pandas(pdf)

            def identify4(
                x,
            ) -> ps.DataFrame[float, [int, ntp.NDArray[int]]]:
                return x

            actual = psdf.pandas_on_spark.apply_batch(identify4)
            actual.columns = ["a", "b"]
            self.assert_eq(actual, pdf)

        arrays = [[1, 2, 3, 4, 5, 6, 7, 8, 9], ["a", "b", "c", "d", "e", "f", "g", "h", "i"]]
        idx = pd.MultiIndex.from_arrays(arrays, names=("number", "color"))
        pdf = pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9], "b": [[e] for e in [4, 5, 6, 3, 2, 1, 0, 0, 0]]},
            index=idx,
        )
        psdf = ps.from_pandas(pdf)

        def identify4(x) -> ps.DataFrame[[int, str], [int, List[int]]]:
            return x

        actual = psdf.pandas_on_spark.apply_batch(identify4)
        actual.index.names = ["number", "color"]
        actual.columns = ["a", "b"]
        self.assert_eq(actual, pdf)

        def identify5(
            x,
        ) -> ps.DataFrame[
            [("number", int), ("color", str)], [("a", int), ("b", List[int])]  # noqa: F405
        ]:
            return x

        actual = psdf.pandas_on_spark.apply_batch(identify5)
        self.assert_eq(actual, pdf)

    def test_transform(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 2, 3, 4, 5, 6] * 100,
                "b": [1.0, 1.0, 2.0, 3.0, 5.0, 8.0] * 100,
                "c": [1, 4, 9, 16, 25, 36] * 100,
            },
            columns=["a", "b", "c"],
            index=np.random.rand(600),
        )
        psdf = ps.DataFrame(pdf)
        self.assert_eq(
            psdf.transform(lambda x: x + 1).sort_index(),
            pdf.transform(lambda x: x + 1).sort_index(),
        )
        self.assert_eq(
            psdf.transform(lambda x, y: x + y, y=2).sort_index(),
            pdf.transform(lambda x, y: x + y, y=2).sort_index(),
        )
        with option_context("compute.shortcut_limit", 500):
            self.assert_eq(
                psdf.transform(lambda x: x + 1).sort_index(),
                pdf.transform(lambda x: x + 1).sort_index(),
            )
            self.assert_eq(
                psdf.transform(lambda x, y: x + y, y=1).sort_index(),
                pdf.transform(lambda x, y: x + y, y=1).sort_index(),
            )

        with self.assertRaisesRegex(AssertionError, "the first argument should be a callable"):
            psdf.transform(1)
        with self.assertRaisesRegex(
            NotImplementedError, 'axis should be either 0 or "index" currently.'
        ):
            psdf.transform(lambda x: x + 1, axis=1)

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.transform(lambda x: x + 1).sort_index(),
            pdf.transform(lambda x: x + 1).sort_index(),
        )
        with option_context("compute.shortcut_limit", 500):
            self.assert_eq(
                psdf.transform(lambda x: x + 1).sort_index(),
                pdf.transform(lambda x: x + 1).sort_index(),
            )

    def test_transform_batch(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 2, 3, 4, 5, 6] * 100,
                "b": [1.0, 1.0, 2.0, 3.0, 5.0, 8.0] * 100,
                "c": [1, 4, 9, 16, 25, 36] * 100,
            },
            columns=["a", "b", "c"],
            index=np.random.rand(600),
        )
        psdf = ps.DataFrame(pdf)

        self.assert_eq(
            psdf.pandas_on_spark.transform_batch(lambda pdf: pdf.c + 1).sort_index(),
            (pdf.c + 1).sort_index(),
        )
        self.assert_eq(
            psdf.pandas_on_spark.transform_batch(lambda pdf, a: pdf + a, 1).sort_index(),
            (pdf + 1).sort_index(),
        )
        self.assert_eq(
            psdf.pandas_on_spark.transform_batch(lambda pdf, a: pdf.c + a, a=1).sort_index(),
            (pdf.c + 1).sort_index(),
        )

        with option_context("compute.shortcut_limit", 500):
            self.assert_eq(
                psdf.pandas_on_spark.transform_batch(lambda pdf: pdf + 1).sort_index(),
                (pdf + 1).sort_index(),
            )
            self.assert_eq(
                psdf.pandas_on_spark.transform_batch(lambda pdf: pdf.b + 1).sort_index(),
                (pdf.b + 1).sort_index(),
            )
            self.assert_eq(
                psdf.pandas_on_spark.transform_batch(lambda pdf, a: pdf + a, 1).sort_index(),
                (pdf + 1).sort_index(),
            )
            self.assert_eq(
                psdf.pandas_on_spark.transform_batch(lambda pdf, a: pdf.c + a, a=1).sort_index(),
                (pdf.c + 1).sort_index(),
            )

        with self.assertRaisesRegex(AssertionError, "the first argument should be a callable"):
            psdf.pandas_on_spark.transform_batch(1)

        with self.assertRaisesRegex(ValueError, "The given function should return a frame"):
            psdf.pandas_on_spark.transform_batch(lambda pdf: 1)

        with self.assertRaisesRegex(
            ValueError, "transform_batch cannot produce aggregated results"
        ):
            psdf.pandas_on_spark.transform_batch(lambda pdf: pd.Series(1))

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.pandas_on_spark.transform_batch(lambda x: x + 1).sort_index(),
            (pdf + 1).sort_index(),
        )
        with option_context("compute.shortcut_limit", 500):
            self.assert_eq(
                psdf.pandas_on_spark.transform_batch(lambda x: x + 1).sort_index(),
                (pdf + 1).sort_index(),
            )

    def test_transform_batch_with_type(self):
        pdf = self.pdf
        psdf = ps.from_pandas(pdf)

        def identify1(x) -> ps.DataFrame[int, int]:
            return x

        # Type hints set the default column names, and we use default index for
        # pandas API on Spark. Here we ignore both diff.
        actual = psdf.pandas_on_spark.transform_batch(identify1)
        expected = pdf
        self.assert_eq(sorted(actual["c0"].to_numpy()), sorted(expected["a"].to_numpy()))
        self.assert_eq(sorted(actual["c1"].to_numpy()), sorted(expected["b"].to_numpy()))

        def identify2(x) -> ps.DataFrame[slice("a", int), slice("b", int)]:  # noqa: F405
            return x

        actual = psdf.pandas_on_spark.transform_batch(identify2)
        expected = pdf
        self.assert_eq(sorted(actual["a"].to_numpy()), sorted(expected["a"].to_numpy()))
        self.assert_eq(sorted(actual["b"].to_numpy()), sorted(expected["b"].to_numpy()))

    def test_transform_batch_same_anchor(self):
        psdf = ps.range(10)
        psdf["d"] = psdf.pandas_on_spark.transform_batch(lambda pdf: pdf.id + 1)
        self.assert_eq(
            psdf,
            pd.DataFrame({"id": list(range(10)), "d": list(range(1, 11))}, columns=["id", "d"]),
        )

        psdf = ps.range(10)

        def plus_one(pdf) -> ps.Series[np.int64]:
            return pdf.id + 1

        psdf["d"] = psdf.pandas_on_spark.transform_batch(plus_one)
        self.assert_eq(
            psdf,
            pd.DataFrame({"id": list(range(10)), "d": list(range(1, 11))}, columns=["id", "d"]),
        )

        psdf = ps.range(10)

        def plus_one(ser) -> ps.Series[np.int64]:
            return ser + 1

        psdf["d"] = psdf.id.pandas_on_spark.transform_batch(plus_one)
        self.assert_eq(
            psdf,
            pd.DataFrame({"id": list(range(10)), "d": list(range(1, 11))}, columns=["id", "d"]),
        )

    def test_pipe(self):
        psdf = ps.DataFrame(
            {"category": ["A", "A", "B"], "col1": [1, 2, 3], "col2": [4, 5, 6]},
            columns=["category", "col1", "col2"],
        )

        self.assertRaisesRegex(
            ValueError,
            "arg is both the pipe target and a keyword argument",
            lambda: psdf.pipe((lambda x: x, "arg"), arg="1"),
        )

    def test_aggregate(self):
        pdf = pd.DataFrame(
            [[1, 2, 3], [4, 5, 6], [7, 8, 9], [np.nan, np.nan, np.nan]], columns=["A", "B", "C"]
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.agg(["sum", "min"])[["A", "B", "C"]].sort_index(),  # TODO?: fix column order
            pdf.agg(["sum", "min"])[["A", "B", "C"]].sort_index(),
        )
        self.assert_eq(
            psdf.agg({"A": ["sum", "min"], "B": ["min", "max"]})[["A", "B"]].sort_index(),
            pdf.agg({"A": ["sum", "min"], "B": ["min", "max"]})[["A", "B"]].sort_index(),
        )

        self.assertRaises(KeyError, lambda: psdf.agg({"A": ["sum", "min"], "X": ["min", "max"]}))

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("X", "A"), ("X", "B"), ("Y", "C")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.agg(["sum", "min"])[[("X", "A"), ("X", "B"), ("Y", "C")]].sort_index(),
            pdf.agg(["sum", "min"])[[("X", "A"), ("X", "B"), ("Y", "C")]].sort_index(),
        )
        self.assert_eq(
            psdf.agg({("X", "A"): ["sum", "min"], ("X", "B"): ["min", "max"]})[
                [("X", "A"), ("X", "B")]
            ].sort_index(),
            pdf.agg({("X", "A"): ["sum", "min"], ("X", "B"): ["min", "max"]})[
                [("X", "A"), ("X", "B")]
            ].sort_index(),
        )

        self.assertRaises(TypeError, lambda: psdf.agg({"X": ["sum", "min"], "Y": ["min", "max"]}))

        # non-string names
        pdf = pd.DataFrame(
            [[1, 2, 3], [4, 5, 6], [7, 8, 9], [np.nan, np.nan, np.nan]], columns=[10, 20, 30]
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.agg(["sum", "min"])[[10, 20, 30]].sort_index(),
            pdf.agg(["sum", "min"])[[10, 20, 30]].sort_index(),
        )
        self.assert_eq(
            psdf.agg({10: ["sum", "min"], 20: ["min", "max"]})[[10, 20]].sort_index(),
            pdf.agg({10: ["sum", "min"], 20: ["min", "max"]})[[10, 20]].sort_index(),
        )

        columns = pd.MultiIndex.from_tuples([("X", 10), ("X", 20), ("Y", 30)])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.agg(["sum", "min"])[[("X", 10), ("X", 20), ("Y", 30)]].sort_index(),
            pdf.agg(["sum", "min"])[[("X", 10), ("X", 20), ("Y", 30)]].sort_index(),
        )
        self.assert_eq(
            psdf.agg({("X", 10): ["sum", "min"], ("X", 20): ["min", "max"]})[
                [("X", 10), ("X", 20)]
            ].sort_index(),
            pdf.agg({("X", 10): ["sum", "min"], ("X", 20): ["min", "max"]})[
                [("X", 10), ("X", 20)]
            ].sort_index(),
        )

        pdf = pd.DataFrame(
            [datetime(2019, 2, 2, 0, 0, 0, 0), datetime(2019, 2, 3, 0, 0, 0, 0)],
            columns=["timestamp"],
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.timestamp.min(), pdf.timestamp.min())
        self.assert_eq(psdf.timestamp.max(), pdf.timestamp.max())

        self.assertRaises(ValueError, lambda: psdf.agg(("sum", "min")))


class FrameApplyFunctionTests(
    FrameApplyFunctionMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.computation.test_apply_func import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
