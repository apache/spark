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

import inspect
import unittest
from distutils.version import LooseVersion
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

import pyspark.pandas as ps
from pyspark.pandas.exceptions import PandasNotImplementedError
from pyspark.pandas.missing.indexes import (
    MissingPandasLikeDatetimeIndex,
    MissingPandasLikeIndex,
    MissingPandasLikeMultiIndex,
    MissingPandasLikeTimedeltaIndex,
)
from pyspark.testing.pandasutils import ComparisonTestBase, TestUtils, SPARK_CONF_ARROW_ENABLED


class IndexesTest(ComparisonTestBase, TestUtils):
    @property
    def pdf(self):
        return pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9], "b": [4, 5, 6, 3, 2, 1, 0, 0, 0]},
            index=[0, 1, 3, 5, 6, 8, 9, 9, 9],
        )

    def test_index_basic(self):
        for pdf in [
            pd.DataFrame(np.random.randn(10, 5), index=np.random.randint(100, size=10)),
            pd.DataFrame(
                np.random.randn(10, 5), index=np.random.randint(100, size=10).astype(np.int32)
            ),
            pd.DataFrame(np.random.randn(10, 5), index=np.random.randn(10)),
            pd.DataFrame(np.random.randn(10, 5), index=np.random.randn(10).astype(np.float32)),
            pd.DataFrame(np.random.randn(10, 5), index=list("abcdefghij")),
            pd.DataFrame(
                np.random.randn(10, 5), index=pd.date_range("2011-01-01", freq="D", periods=10)
            ),
            pd.DataFrame(np.random.randn(10, 5), index=pd.Categorical(list("abcdefghij"))),
            pd.DataFrame(np.random.randn(10, 5), columns=list("abcde")).set_index(["a", "b"]),
        ]:
            psdf = ps.from_pandas(pdf)
            self.assert_eq(psdf.index, pdf.index)
            self.assert_eq(type(psdf.index).__name__, type(pdf.index).__name__)

        self.assert_eq(ps.Index([])._summary(), "Index: 0 entries")
        with self.assertRaisesRegexp(ValueError, "The truth value of a Int64Index is ambiguous."):
            bool(ps.Index([1]))
        with self.assertRaisesRegexp(TypeError, "Index.name must be a hashable type"):
            ps.Int64Index([1, 2, 3], name=[(1, 2, 3)])
        with self.assertRaisesRegexp(TypeError, "Index.name must be a hashable type"):
            ps.Float64Index([1.0, 2.0, 3.0], name=[(1, 2, 3)])

    def test_index_from_series(self):
        pser = pd.Series([1, 2, 3], name="a", index=[10, 20, 30])
        psser = ps.from_pandas(pser)

        self.assert_eq(ps.Index(psser), pd.Index(pser))
        self.assert_eq(ps.Index(psser, dtype="float"), pd.Index(pser, dtype="float"))
        self.assert_eq(ps.Index(psser, name="x"), pd.Index(pser, name="x"))

        if LooseVersion(pd.__version__) >= LooseVersion("1.1"):
            self.assert_eq(ps.Int64Index(psser), pd.Int64Index(pser))
            self.assert_eq(ps.Float64Index(psser), pd.Float64Index(pser))
        else:
            self.assert_eq(ps.Int64Index(psser), pd.Int64Index(pser).rename("a"))
            self.assert_eq(ps.Float64Index(psser), pd.Float64Index(pser).rename("a"))

        pser = pd.Series([datetime(2021, 3, 1), datetime(2021, 3, 2)], name="x", index=[10, 20])
        psser = ps.from_pandas(pser)

        self.assert_eq(ps.Index(psser), pd.Index(pser))
        self.assert_eq(ps.DatetimeIndex(psser), pd.DatetimeIndex(pser))

    def test_index_from_index(self):
        pidx = pd.Index([1, 2, 3], name="a")
        psidx = ps.from_pandas(pidx)

        self.assert_eq(ps.Index(psidx), pd.Index(pidx))
        self.assert_eq(ps.Index(psidx, dtype="float"), pd.Index(pidx, dtype="float"))
        self.assert_eq(ps.Index(psidx, name="x"), pd.Index(pidx, name="x"))
        self.assert_eq(ps.Index(psidx, copy=True), pd.Index(pidx, copy=True))

        self.assert_eq(ps.Int64Index(psidx), pd.Int64Index(pidx))
        self.assert_eq(ps.Float64Index(psidx), pd.Float64Index(pidx))

        pidx = pd.DatetimeIndex(["2021-03-01", "2021-03-02"])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(ps.Index(psidx), pd.Index(pidx))
        self.assert_eq(ps.DatetimeIndex(psidx), pd.DatetimeIndex(pidx))

    def test_index_getattr(self):
        psidx = self.psdf.index
        item = "databricks"

        expected_error_message = "'.*Index' object has no attribute '{}'".format(item)
        with self.assertRaisesRegex(AttributeError, expected_error_message):
            psidx.__getattr__(item)
        with self.assertRaisesRegex(AttributeError, expected_error_message):
            ps.from_pandas(pd.date_range("2011-01-01", freq="D", periods=10)).__getattr__(item)

    def test_multi_index_getattr(self):
        arrays = [[1, 1, 2, 2], ["red", "blue", "red", "blue"]]
        idx = pd.MultiIndex.from_arrays(arrays, names=("number", "color"))
        pdf = pd.DataFrame(np.random.randn(4, 5), idx)
        psdf = ps.from_pandas(pdf)
        psidx = psdf.index
        item = "databricks"

        expected_error_message = "'MultiIndex' object has no attribute '{}'".format(item)
        with self.assertRaisesRegex(AttributeError, expected_error_message):
            psidx.__getattr__(item)

    def test_to_series(self):
        pidx = self.pdf.index
        psidx = self.psdf.index

        self.assert_eq(psidx.to_series(), pidx.to_series())
        self.assert_eq(psidx.to_series(name="a"), pidx.to_series(name="a"))

        # With name
        pidx.name = "Koalas"
        psidx.name = "Koalas"
        self.assert_eq(psidx.to_series(), pidx.to_series())
        self.assert_eq(psidx.to_series(name=("x", "a")), pidx.to_series(name=("x", "a")))

        # With tupled name
        pidx.name = ("x", "a")
        psidx.name = ("x", "a")
        self.assert_eq(psidx.to_series(), pidx.to_series())
        self.assert_eq(psidx.to_series(name="a"), pidx.to_series(name="a"))

        self.assert_eq((psidx + 1).to_series(), (pidx + 1).to_series())

        pidx = self.pdf.set_index("b", append=True).index
        psidx = self.psdf.set_index("b", append=True).index

        with self.sql_conf({SPARK_CONF_ARROW_ENABLED: False}):
            self.assert_eq(psidx.to_series(), pidx.to_series())
            self.assert_eq(psidx.to_series(name="a"), pidx.to_series(name="a"))

        expected_error_message = "Series.name must be a hashable type"
        with self.assertRaisesRegex(TypeError, expected_error_message):
            psidx.to_series(name=["x", "a"])

    def test_to_frame(self):
        pidx = self.pdf.index
        psidx = self.psdf.index

        self.assert_eq(psidx.to_frame(), pidx.to_frame())
        self.assert_eq(psidx.to_frame(index=False), pidx.to_frame(index=False))

        pidx.name = "a"
        psidx.name = "a"

        self.assert_eq(psidx.to_frame(), pidx.to_frame())
        self.assert_eq(psidx.to_frame(index=False), pidx.to_frame(index=False))

        self.assert_eq(psidx.to_frame(name="x"), pidx.to_frame(name="x"))
        self.assert_eq(psidx.to_frame(index=False, name="x"), pidx.to_frame(index=False, name="x"))

        self.assertRaises(TypeError, lambda: psidx.to_frame(name=["x"]))

        # non-string name
        self.assert_eq(psidx.to_frame(name=10), pidx.to_frame(name=10))
        self.assert_eq(psidx.to_frame(name=("x", 10)), pidx.to_frame(name=("x", 10)))

        pidx = self.pdf.set_index("b", append=True).index
        psidx = self.psdf.set_index("b", append=True).index

        self.assert_eq(psidx.to_frame(), pidx.to_frame())
        self.assert_eq(psidx.to_frame(index=False), pidx.to_frame(index=False))

        self.assert_eq(psidx.to_frame(name=["x", "y"]), pidx.to_frame(name=["x", "y"]))
        self.assert_eq(psidx.to_frame(name=("x", "y")), pidx.to_frame(name=("x", "y")))
        self.assert_eq(
            psidx.to_frame(index=False, name=["x", "y"]),
            pidx.to_frame(index=False, name=["x", "y"]),
        )

        self.assertRaises(TypeError, lambda: psidx.to_frame(name="x"))
        self.assertRaises(ValueError, lambda: psidx.to_frame(name=["x"]))

        # non-string names
        self.assert_eq(psidx.to_frame(name=[10, 20]), pidx.to_frame(name=[10, 20]))
        self.assert_eq(psidx.to_frame(name=("x", 10)), pidx.to_frame(name=("x", 10)))
        if LooseVersion(pd.__version__) < LooseVersion("1.5.0"):
            self.assert_eq(
                psidx.to_frame(name=[("x", 10), ("y", 20)]),
                pidx.to_frame(name=[("x", 10), ("y", 20)]),
            )
        else:
            # Since pandas 1.5.0, the result is changed as below:
            #      (x, 10)  (y, 20)
            #   b
            # 0 4        0        4
            # 1 5        1        5
            # 3 6        3        6
            # 5 3        5        3
            # 6 2        6        2
            # 8 1        8        1
            # 9 0        9        0
            #   0        9        0
            #   0        9        0
            #
            # The columns should be `Index([('x', 20), ('y', 20)], dtype='object')`,
            # but pandas API on Spark doesn't support such a way for creating Index.
            # So, we currently cannot follow the behavior of pandas.
            expected_result = ps.DataFrame(
                {("x", 10): [0, 1, 3, 5, 6, 8, 9, 9, 9], ("y", 20): [4, 5, 6, 3, 2, 1, 0, 0, 0]},
                index=ps.MultiIndex.from_tuples(
                    [(0, 4), (1, 5), (3, 6), (5, 3), (6, 2), (8, 1), (9, 0), (9, 0), (9, 0)],
                    names=[None, "b"],
                ),
            )
            self.assert_eq(psidx.to_frame(name=[("x", 10), ("y", 20)]), expected_result)

    def test_index_names(self):
        psdf = self.psdf
        self.assertIsNone(psdf.index.name)

        idx = pd.Index([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], name="x")
        pdf = pd.DataFrame(np.random.randn(10, 5), index=idx, columns=list("abcde"))
        psdf = ps.from_pandas(pdf)

        pser = pdf.a
        psser = psdf.a

        self.assertEqual(psdf.index.name, pdf.index.name)
        self.assertEqual(psdf.index.names, pdf.index.names)

        pidx = pdf.index
        psidx = psdf.index
        pidx.name = "renamed"
        psidx.name = "renamed"
        self.assertEqual(psidx.name, pidx.name)
        self.assertEqual(psidx.names, pidx.names)
        self.assert_eq(psidx, pidx)
        self.assertEqual(psdf.index.name, pdf.index.name)
        self.assertEqual(psdf.index.names, pdf.index.names)
        self.assertEqual(psser.index.names, pser.index.names)

        pidx.name = None
        psidx.name = None
        self.assertEqual(psidx.name, pidx.name)
        self.assertEqual(psidx.names, pidx.names)
        self.assert_eq(psidx, pidx)
        self.assertEqual(psdf.index.name, pdf.index.name)
        self.assertEqual(psdf.index.names, pdf.index.names)
        self.assertEqual(psser.index.names, pser.index.names)

        with self.assertRaisesRegex(ValueError, "Names must be a list-like"):
            psidx.names = "hi"

        expected_error_message = "Length of new names must be {}, got {}".format(
            psdf._internal.index_level, len(["0", "1"])
        )
        with self.assertRaisesRegex(ValueError, expected_error_message):
            psidx.names = ["0", "1"]

        expected_error_message = "Index.name must be a hashable type"
        with self.assertRaisesRegex(TypeError, expected_error_message):
            ps.Index([1, 2, 3], name=["0", "1"])
        with self.assertRaisesRegex(TypeError, expected_error_message):
            psidx.name = ["renamed"]
        with self.assertRaisesRegex(TypeError, expected_error_message):
            psidx.name = ["0", "1"]
        with self.assertRaisesRegex(TypeError, expected_error_message):
            ps.Index([(1, 2), (3, 4)], names=["a", ["b"]])

    def test_multi_index_names(self):
        arrays = [[1, 1, 2, 2], ["red", "blue", "red", "blue"]]
        idx = pd.MultiIndex.from_arrays(arrays, names=("number", "color"))
        pdf = pd.DataFrame(np.random.randn(4, 5), idx)
        psdf = ps.from_pandas(pdf)

        self.assertEqual(psdf.index.names, pdf.index.names)

        pidx = pdf.index
        psidx = psdf.index
        pidx.names = ["renamed_number", "renamed_color"]
        psidx.names = ["renamed_number", "renamed_color"]
        self.assertEqual(psidx.names, pidx.names)

        pidx.names = ["renamed_number", None]
        psidx.names = ["renamed_number", None]
        self.assertEqual(psidx.names, pidx.names)
        self.assert_eq(psidx, pidx)

        with self.assertRaises(PandasNotImplementedError):
            psidx.name
        with self.assertRaises(PandasNotImplementedError):
            psidx.name = "renamed"

    def test_index_rename(self):
        pdf = pd.DataFrame(
            np.random.randn(10, 5), index=pd.Index([0, 1, 2, 3, 4, 5, 6, 7, 8, 9], name="x")
        )
        psdf = ps.from_pandas(pdf)

        pidx = pdf.index
        psidx = psdf.index

        self.assert_eq(psidx.rename("y"), pidx.rename("y"))
        self.assert_eq(psdf.index.names, pdf.index.names)

        # non-string names
        self.assert_eq(psidx.rename(0), pidx.rename(0))
        self.assert_eq(psidx.rename(("y", 0)), pidx.rename(("y", 0)))

        psidx.rename("z", inplace=True)
        pidx.rename("z", inplace=True)

        self.assert_eq(psidx, pidx)
        self.assert_eq(psdf.index.names, pdf.index.names)

        self.assert_eq(psidx.rename(None), pidx.rename(None))
        self.assert_eq(psdf.index.names, pdf.index.names)

        self.assertRaises(TypeError, lambda: psidx.rename(["x", "y"]))

    def test_multi_index_rename(self):
        arrays = [[1, 1, 2, 2], ["red", "blue", "red", "blue"]]
        idx = pd.MultiIndex.from_arrays(arrays, names=("number", "color"))
        pdf = pd.DataFrame(np.random.randn(4, 5), idx)
        psdf = ps.from_pandas(pdf)

        pmidx = pdf.index
        psmidx = psdf.index

        self.assert_eq(psmidx.rename(["n", "c"]), pmidx.rename(["n", "c"]))
        self.assert_eq(psdf.index.names, pdf.index.names)

        # non-string names
        self.assert_eq(psmidx.rename([0, 1]), pmidx.rename([0, 1]))
        self.assert_eq(
            psmidx.rename([("x", "a"), ("y", "b")]), pmidx.rename([("x", "a"), ("y", "b")])
        )

        psmidx.rename(["num", "col"], inplace=True)
        pmidx.rename(["num", "col"], inplace=True)

        self.assert_eq(psmidx, pmidx)
        self.assert_eq(psdf.index.names, pdf.index.names)

        self.assert_eq(psmidx.rename([None, None]), pmidx.rename([None, None]))
        self.assert_eq(psdf.index.names, pdf.index.names)

        self.assertRaises(TypeError, lambda: psmidx.rename("number"))
        self.assertRaises(TypeError, lambda: psmidx.rename(None))
        self.assertRaises(ValueError, lambda: psmidx.rename(["number"]))

    def test_multi_index_levshape(self):
        pidx = pd.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2)])
        psidx = ps.from_pandas(pidx)
        self.assertEqual(pidx.levshape, psidx.levshape)

    def test_index_unique(self):
        psidx = self.psdf.index

        # here the output is different than pandas in terms of order
        expected = [0, 1, 3, 5, 6, 8, 9]

        self.assert_eq(expected, sorted(psidx.unique().to_pandas()))
        self.assert_eq(expected, sorted(psidx.unique(level=0).to_pandas()))

        expected = [1, 2, 4, 6, 7, 9, 10]
        self.assert_eq(expected, sorted((psidx + 1).unique().to_pandas()))

        with self.assertRaisesRegex(IndexError, "Too many levels*"):
            psidx.unique(level=1)

        with self.assertRaisesRegex(KeyError, "Requested level (hi)*"):
            psidx.unique(level="hi")

    def test_multi_index_copy(self):
        arrays = [[1, 1, 2, 2], ["red", "blue", "red", "blue"]]
        idx = pd.MultiIndex.from_arrays(arrays, names=("number", "color"))
        pdf = pd.DataFrame(np.random.randn(4, 5), idx)
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.index.copy(), pdf.index.copy())

    def test_drop_duplicates(self):
        pidx = pd.Index([4, 2, 4, 1, 4, 3])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(psidx.drop_duplicates(), pidx.drop_duplicates())
        self.assert_eq((psidx + 1).drop_duplicates(), (pidx + 1).drop_duplicates())

        self.assert_eq(psidx.drop_duplicates(keep="first"), pidx.drop_duplicates(keep="first"))
        self.assert_eq(psidx.drop_duplicates(keep="last"), pidx.drop_duplicates(keep="last"))
        self.assert_eq(psidx.drop_duplicates(keep=False), pidx.drop_duplicates(keep=False))

        arrays = [[1, 2, 3, 1, 2], ["red", "blue", "black", "red", "blue"]]
        pmidx = pd.MultiIndex.from_arrays(arrays, names=("number", "color"))
        psmidx = ps.from_pandas(pmidx)
        self.assert_eq(psmidx.drop_duplicates(), pmidx.drop_duplicates())
        self.assert_eq(psmidx.drop_duplicates(keep="first"), pmidx.drop_duplicates(keep="first"))
        self.assert_eq(psmidx.drop_duplicates(keep="last"), pmidx.drop_duplicates(keep="last"))
        self.assert_eq(psmidx.drop_duplicates(keep=False), pmidx.drop_duplicates(keep=False))

    def test_dropna(self):
        pidx = pd.Index([np.nan, 2, 4, 1, None, 3])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(psidx.dropna(), pidx.dropna())
        self.assert_eq((psidx + 1).dropna(), (pidx + 1).dropna())

        self.assert_eq(psidx.dropna(how="any"), pidx.dropna(how="any"))
        self.assert_eq(psidx.dropna(how="all"), pidx.dropna(how="all"))

        pmidx = pd.MultiIndex.from_tuples(
            [(np.nan, 1.0), (2.0, 2.0), (np.nan, None), (3.0, np.nan)]
        )
        psmidx = ps.from_pandas(pmidx)
        self.assert_eq(psmidx.dropna(), pmidx.dropna())
        self.assert_eq(psmidx.dropna(how="any"), pmidx.dropna(how="any"))
        self.assert_eq(psmidx.dropna(how="all"), pmidx.dropna(how="all"))

        invalid_how = "none"
        with self.assertRaisesRegex(ValueError, "invalid how option: %s" % invalid_how):
            psmidx.dropna(invalid_how)

    def test_index_symmetric_difference(self):
        pidx1 = pd.Index([1, 2, 3, 4])
        pidx2 = pd.Index([2, 3, 4, 5])
        psidx1 = ps.from_pandas(pidx1)
        psidx2 = ps.from_pandas(pidx2)

        self.assert_eq(
            psidx1.symmetric_difference(psidx2).sort_values(),
            pidx1.symmetric_difference(pidx2).sort_values(),
        )
        self.assert_eq(
            (psidx1 + 1).symmetric_difference(psidx2).sort_values(),
            (pidx1 + 1).symmetric_difference(pidx2).sort_values(),
        )
        self.assert_eq(
            (psidx1 ^ psidx2).sort_values(),
            (pidx1 ^ pidx2).sort_values(),
        )
        self.assert_eq(
            psidx1.symmetric_difference(psidx2, result_name="result").sort_values(),
            pidx1.symmetric_difference(pidx2, result_name="result").sort_values(),
        )

        pmidx1 = pd.MultiIndex(
            [["lama", "cow", "falcon"], ["speed", "weight", "length"]],
            [[0, 0, 0, 1, 1, 1, 2, 2, 2], [0, 0, 0, 0, 1, 2, 0, 1, 2]],
        )
        pmidx2 = pd.MultiIndex(
            [["koalas", "cow", "falcon"], ["speed", "weight", "length"]],
            [[0, 0, 0, 1, 1, 1, 2, 2, 2], [0, 0, 0, 0, 1, 2, 0, 1, 2]],
        )
        psmidx1 = ps.from_pandas(pmidx1)
        psmidx2 = ps.from_pandas(pmidx2)

        self.assert_eq(
            psmidx1.symmetric_difference(psmidx2).sort_values(),
            pmidx1.symmetric_difference(pmidx2).sort_values(),
        )

        # Pandas has a bug that raise TypeError when setting `result_name` for MultiIndex.
        pandas_result = pmidx1.symmetric_difference(pmidx2)
        pandas_result.names = ["a", "b"]
        self.assert_eq(
            psmidx1.symmetric_difference(psmidx2, result_name=["a", "b"]).sort_values(),
            pandas_result,
        )

        # Pandas sort the result by default, so doesn't provide the `True` for sort.
        self.assert_eq(
            psmidx1.symmetric_difference(psmidx2, sort=True),
            pmidx1.symmetric_difference(pmidx2),
        )

        idx = ps.Index(["a", "b", "c"])
        midx = ps.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])

        with self.assertRaisesRegex(NotImplementedError, "Doesn't support*"):
            idx.symmetric_difference(midx)

    def test_multi_index_symmetric_difference(self):
        idx = ps.Index(["a", "b", "c"])
        midx = ps.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        midx_ = ps.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])

        self.assert_eq(
            midx.symmetric_difference(midx_),
            midx.to_pandas().symmetric_difference(midx_.to_pandas()),
        )

        with self.assertRaisesRegex(NotImplementedError, "Doesn't support*"):
            midx.symmetric_difference(idx)

    def test_missing(self):
        psdf = ps.DataFrame(
            {
                "a": [1, 2, 3],
                "b": [4, 5, 6],
                "c": pd.date_range("2011-01-01", freq="D", periods=3),
                "d": pd.Categorical(["a", "b", "c"]),
                "e": [timedelta(1), timedelta(2), timedelta(3)],
            }
        )

        # Index functions
        missing_functions = inspect.getmembers(MissingPandasLikeIndex, inspect.isfunction)
        unsupported_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "unsupported_function"
        ]
        for name in unsupported_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "method.*Index.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.set_index("a").index, name)()

        deprecated_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "deprecated_function"
        ]
        for name in deprecated_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "method.*Index.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.set_index("a").index, name)()

        # MultiIndex functions
        missing_functions = inspect.getmembers(MissingPandasLikeMultiIndex, inspect.isfunction)
        unsupported_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "unsupported_function"
        ]
        for name in unsupported_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "method.*Index.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.set_index(["a", "b"]).index, name)()

        deprecated_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "deprecated_function"
        ]
        for name in deprecated_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "method.*Index.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.set_index(["a", "b"]).index, name)()

        # DatetimeIndex functions
        missing_functions = inspect.getmembers(MissingPandasLikeDatetimeIndex, inspect.isfunction)
        unsupported_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "unsupported_function"
        ]
        for name in unsupported_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "method.*Index.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.set_index("c").index, name)()

        deprecated_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "deprecated_function"
        ]
        for name in deprecated_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "method.*Index.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.set_index("c").index, name)()

        # TimedeltaIndex functions
        missing_functions = inspect.getmembers(MissingPandasLikeTimedeltaIndex, inspect.isfunction)
        unsupported_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "unsupported_function"
        ]
        for name in unsupported_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "method.*Index.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.set_index("e").index, name)()

        deprecated_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "deprecated_function"
        ]
        for name in deprecated_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "method.*Index.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.set_index("e").index, name)()

        # Index properties
        missing_properties = inspect.getmembers(
            MissingPandasLikeIndex, lambda o: isinstance(o, property)
        )
        unsupported_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "unsupported_property"
        ]
        for name in unsupported_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "property.*Index.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.set_index("a").index, name)

        deprecated_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "deprecated_property"
        ]
        for name in deprecated_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "property.*Index.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.set_index("a").index, name)

        # MultiIndex properties
        missing_properties = inspect.getmembers(
            MissingPandasLikeMultiIndex, lambda o: isinstance(o, property)
        )
        unsupported_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "unsupported_property"
        ]
        for name in unsupported_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "property.*Index.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.set_index(["a", "b"]).index, name)

        deprecated_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "deprecated_property"
        ]
        for name in deprecated_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "property.*Index.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.set_index(["a", "b"]).index, name)

        # DatetimeIndex properties
        missing_properties = inspect.getmembers(
            MissingPandasLikeDatetimeIndex, lambda o: isinstance(o, property)
        )
        unsupported_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "unsupported_property"
        ]
        for name in unsupported_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "property.*Index.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.set_index("c").index, name)

        # TimedeltaIndex properties
        missing_properties = inspect.getmembers(
            MissingPandasLikeDatetimeIndex, lambda o: isinstance(o, property)
        )
        unsupported_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "unsupported_property"
        ]
        for name in unsupported_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "property.*Index.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.set_index("c").index, name)

    def test_index_has_duplicates(self):
        indexes = [("a", "b", "c"), ("a", "a", "c"), (1, 3, 3), (1, 2, 3)]
        names = [None, "ks", "ks", None]
        has_dup = [False, True, True, False]

        for idx, name, expected in zip(indexes, names, has_dup):
            pdf = pd.DataFrame({"a": [1, 2, 3]}, index=pd.Index(idx, name=name))
            psdf = ps.from_pandas(pdf)

            self.assertEqual(psdf.index.has_duplicates, expected)

    def test_multiindex_has_duplicates(self):
        indexes = [
            [list("abc"), list("edf")],
            [list("aac"), list("edf")],
            [list("aac"), list("eef")],
            [[1, 4, 4], [4, 6, 6]],
        ]
        has_dup = [False, False, True, True]

        for idx, expected in zip(indexes, has_dup):
            pdf = pd.DataFrame({"a": [1, 2, 3]}, index=idx)
            psdf = ps.from_pandas(pdf)

            self.assertEqual(psdf.index.has_duplicates, expected)

    def test_multi_index_not_supported(self):
        psdf = ps.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]})

        with self.assertRaisesRegex(TypeError, "cannot perform any with this index type"):
            psdf.set_index(["a", "b"]).index.any()

        with self.assertRaisesRegex(TypeError, "cannot perform all with this index type"):
            psdf.set_index(["a", "b"]).index.all()

    def test_index_nlevels(self):
        pdf = pd.DataFrame({"a": [1, 2, 3]}, index=pd.Index(["a", "b", "c"]))
        psdf = ps.from_pandas(pdf)

        self.assertEqual(psdf.index.nlevels, 1)

    def test_multiindex_nlevel(self):
        pdf = pd.DataFrame({"a": [1, 2, 3]}, index=[list("abc"), list("def")])
        psdf = ps.from_pandas(pdf)

        self.assertEqual(psdf.index.nlevels, 2)

    def test_multiindex_from_arrays(self):
        arrays = [["a", "a", "b", "b"], ["red", "blue", "red", "blue"]]
        pidx = pd.MultiIndex.from_arrays(arrays)
        psidx = ps.MultiIndex.from_arrays(arrays)

        self.assert_eq(pidx, psidx)

    def test_multiindex_swaplevel(self):
        pidx = pd.MultiIndex.from_arrays([["a", "b"], [1, 2]])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.swaplevel(0, 1), psidx.swaplevel(0, 1))

        pidx = pd.MultiIndex.from_arrays([["a", "b"], [1, 2]], names=["word", "number"])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.swaplevel(0, 1), psidx.swaplevel(0, 1))

        pidx = pd.MultiIndex.from_arrays([["a", "b"], [1, 2]], names=["word", None])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.swaplevel(-2, -1), psidx.swaplevel(-2, -1))
        self.assert_eq(pidx.swaplevel(0, 1), psidx.swaplevel(0, 1))
        self.assert_eq(pidx.swaplevel("word", 1), psidx.swaplevel("word", 1))

        with self.assertRaisesRegex(IndexError, "Too many levels: Index"):
            psidx.swaplevel(-3, "word")
        with self.assertRaisesRegex(IndexError, "Too many levels: Index"):
            psidx.swaplevel(0, 2)
        with self.assertRaisesRegex(IndexError, "Too many levels: Index"):
            psidx.swaplevel(0, -3)
        with self.assertRaisesRegex(KeyError, "Level work not found"):
            psidx.swaplevel(0, "work")

    def test_multiindex_droplevel(self):
        pidx = pd.MultiIndex.from_tuples(
            [("a", "x", 1), ("b", "y", 2)], names=["level1", "level2", "level3"]
        )
        psidx = ps.from_pandas(pidx)
        with self.assertRaisesRegex(IndexError, "Too many levels: Index has only 3 levels, not 5"):
            psidx.droplevel(4)

        with self.assertRaisesRegex(KeyError, "Level level4 not found"):
            psidx.droplevel("level4")

        with self.assertRaisesRegex(KeyError, "Level.*level3.*level4.*not found"):
            psidx.droplevel([("level3", "level4")])

        with self.assertRaisesRegex(
            ValueError,
            "Cannot remove 4 levels from an index with 3 levels: at least one "
            "level must be left.",
        ):
            psidx.droplevel([0, 0, 1, 2])

        with self.assertRaisesRegex(
            ValueError,
            "Cannot remove 3 levels from an index with 3 levels: at least one "
            "level must be left.",
        ):
            psidx.droplevel([0, 1, 2])

        self.assert_eq(pidx.droplevel(0), psidx.droplevel(0))
        self.assert_eq(pidx.droplevel([0, 1]), psidx.droplevel([0, 1]))
        self.assert_eq(pidx.droplevel((0, 1)), psidx.droplevel((0, 1)))
        self.assert_eq(pidx.droplevel([0, "level2"]), psidx.droplevel([0, "level2"]))
        self.assert_eq(pidx.droplevel((0, "level2")), psidx.droplevel((0, "level2")))

        # non-string names
        pidx = pd.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2)], names=[1.0, 2.0, 3.0])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.droplevel(1.0), psidx.droplevel(1.0))
        self.assert_eq(pidx.droplevel([0, 2.0]), psidx.droplevel([0, 2.0]))

    def test_index_fillna(self):
        pidx = pd.Index([1, 2, None])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.fillna(0), psidx.fillna(0), almost=True)
        self.assert_eq(pidx.rename("name").fillna(0), psidx.rename("name").fillna(0), almost=True)

        with self.assertRaisesRegex(TypeError, "Unsupported type list"):
            psidx.fillna([1, 2])

    def test_index_drop(self):
        pidx = pd.Index([1, 2, 3])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.drop(1), psidx.drop(1))
        self.assert_eq(pidx.drop([1, 2]), psidx.drop([1, 2]))
        self.assert_eq((pidx + 1).drop([2, 3]), (psidx + 1).drop([2, 3]))

    def test_multiindex_drop(self):
        pidx = pd.MultiIndex.from_tuples(
            [("a", "x"), ("b", "y"), ("c", "z")], names=["level1", "level2"]
        )
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.drop("a"), psidx.drop("a"))
        self.assert_eq(pidx.drop(["a", "b"]), psidx.drop(["a", "b"]))
        self.assert_eq(pidx.drop(["x", "y"], level=1), psidx.drop(["x", "y"], level=1))
        self.assert_eq(
            pidx.drop(["x", "y"], level="level2"), psidx.drop(["x", "y"], level="level2")
        )

        pidx.names = ["lv1", "lv2"]
        psidx.names = ["lv1", "lv2"]
        self.assert_eq(pidx.drop(["x", "y"], level="lv2"), psidx.drop(["x", "y"], level="lv2"))

        self.assertRaises(IndexError, lambda: psidx.drop(["a", "b"], level=2))
        self.assertRaises(KeyError, lambda: psidx.drop(["a", "b"], level="level"))

        psidx.names = ["lv", "lv"]
        self.assertRaises(ValueError, lambda: psidx.drop(["x", "y"], level="lv"))

    def _test_sort_values(self, pidx, psidx):
        self.assert_eq(pidx.sort_values(), psidx.sort_values())
        # Parameter ascending
        self.assert_eq(pidx.sort_values(ascending=False), psidx.sort_values(ascending=False))
        # Parameter return_indexer
        p_sorted, p_indexer = pidx.sort_values(return_indexer=True)
        ps_sorted, ps_indexer = psidx.sort_values(return_indexer=True)
        self.assert_eq(p_sorted, ps_sorted)
        self.assert_eq(p_indexer, ps_indexer.to_list())
        self.assert_eq(
            pidx.sort_values(return_indexer=False), psidx.sort_values(return_indexer=False)
        )
        # Parameter return_indexer and ascending
        p_sorted, p_indexer = pidx.sort_values(return_indexer=True, ascending=False)
        ps_sorted, ps_indexer = psidx.sort_values(return_indexer=True, ascending=False)
        self.assert_eq(p_sorted, ps_sorted)
        self.assert_eq(p_indexer, ps_indexer.to_list())
        self.assert_eq(
            pidx.sort_values(return_indexer=False, ascending=False),
            psidx.sort_values(return_indexer=False, ascending=False),
        )

    def test_sort_values(self):
        pidx = pd.Index([-10, -100, 200, 100])
        psidx = ps.from_pandas(pidx)

        self._test_sort_values(pidx, psidx)

        pidx.name = "koalas"
        psidx.name = "koalas"

        self._test_sort_values(pidx, psidx)

        pidx = pd.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])
        psidx = ps.from_pandas(pidx)

        pidx.names = ["hello", "koalas", "goodbye"]
        psidx.names = ["hello", "koalas", "goodbye"]

        self._test_sort_values(pidx, psidx)

    def test_index_drop_duplicates(self):
        pidx = pd.Index([1, 1, 2])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.drop_duplicates().sort_values(), psidx.drop_duplicates().sort_values())

        pidx = pd.MultiIndex.from_tuples([(1, 1), (1, 1), (2, 2)], names=["level1", "level2"])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.drop_duplicates().sort_values(), psidx.drop_duplicates().sort_values())

    def test_index_sort(self):
        idx = ps.Index([1, 2, 3, 4, 5])
        midx = ps.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2)])

        with self.assertRaisesRegex(
            TypeError, "cannot sort an Index object in-place, use sort_values instead"
        ):
            idx.sort()
        with self.assertRaisesRegex(
            TypeError, "cannot sort an Index object in-place, use sort_values instead"
        ):
            midx.sort()

    def test_multiindex_isna(self):
        psidx = ps.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])

        with self.assertRaisesRegex(NotImplementedError, "isna is not defined for MultiIndex"):
            psidx.isna()

        with self.assertRaisesRegex(NotImplementedError, "isna is not defined for MultiIndex"):
            psidx.isnull()

        with self.assertRaisesRegex(NotImplementedError, "notna is not defined for MultiIndex"):
            psidx.notna()

        with self.assertRaisesRegex(NotImplementedError, "notna is not defined for MultiIndex"):
            psidx.notnull()

    def test_index_nunique(self):
        pidx = pd.Index([1, 1, 2, None])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.nunique(), psidx.nunique())
        self.assert_eq(pidx.nunique(dropna=True), psidx.nunique(dropna=True))

    def test_multiindex_nunique(self):
        psidx = ps.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])
        with self.assertRaisesRegex(NotImplementedError, "notna is not defined for MultiIndex"):
            psidx.notnull()

    def test_multiindex_rename(self):
        pidx = pd.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])
        psidx = ps.from_pandas(pidx)

        pidx = pidx.rename(list("ABC"))
        psidx = psidx.rename(list("ABC"))
        self.assert_eq(pidx, psidx)

        pidx = pidx.rename(["my", "name", "is"])
        psidx = psidx.rename(["my", "name", "is"])
        self.assert_eq(pidx, psidx)

    def test_multiindex_set_names(self):
        pidx = pd.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])
        psidx = ps.from_pandas(pidx)

        pidx = pidx.set_names(["set", "new", "names"])
        psidx = psidx.set_names(["set", "new", "names"])
        self.assert_eq(pidx, psidx)

        pidx.set_names(["set", "new", "names"], inplace=True)
        psidx.set_names(["set", "new", "names"], inplace=True)
        self.assert_eq(pidx, psidx)

        pidx = pidx.set_names("first", level=0)
        psidx = psidx.set_names("first", level=0)
        self.assert_eq(pidx, psidx)

        pidx = pidx.set_names("second", level=1)
        psidx = psidx.set_names("second", level=1)
        self.assert_eq(pidx, psidx)

        pidx = pidx.set_names("third", level=2)
        psidx = psidx.set_names("third", level=2)
        self.assert_eq(pidx, psidx)

        pidx.set_names("first", level=0, inplace=True)
        psidx.set_names("first", level=0, inplace=True)
        self.assert_eq(pidx, psidx)

        pidx.set_names("second", level=1, inplace=True)
        psidx.set_names("second", level=1, inplace=True)
        self.assert_eq(pidx, psidx)

        pidx.set_names("third", level=2, inplace=True)
        psidx.set_names("third", level=2, inplace=True)
        self.assert_eq(pidx, psidx)

    def test_multiindex_from_tuples(self):
        tuples = [(1, "red"), (1, "blue"), (2, "red"), (2, "blue")]
        pidx = pd.MultiIndex.from_tuples(tuples)
        psidx = ps.MultiIndex.from_tuples(tuples)

        self.assert_eq(pidx, psidx)

    def test_multiindex_from_product(self):
        iterables = [[0, 1, 2], ["green", "purple"]]
        pidx = pd.MultiIndex.from_product(iterables)
        psidx = ps.MultiIndex.from_product(iterables)

        self.assert_eq(pidx, psidx)

    def test_multiindex_tuple_column_name(self):
        column_labels = pd.MultiIndex.from_tuples([("a", "x"), ("a", "y"), ("b", "z")])
        pdf = pd.DataFrame([[1, 2, 3], [4, 5, 6], [7, 8, 9]], columns=column_labels)
        pdf.set_index(("a", "x"), append=True, inplace=True)
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf, psdf)

    def test_len(self):
        pidx = pd.Index(range(10000))
        psidx = ps.from_pandas(pidx)

        self.assert_eq(len(pidx), len(psidx))

        pidx = pd.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])
        psidx = ps.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])

        self.assert_eq(len(pidx), len(psidx))

    def test_delete(self):
        pidx = pd.Index([10, 9, 8, 7, 6, 7, 8, 9, 10])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.delete(8).sort_values(), psidx.delete(8).sort_values())
        self.assert_eq(pidx.delete(-9).sort_values(), psidx.delete(-9).sort_values())
        self.assert_eq(
            pidx.delete([-9, 0, 8]).sort_values(), psidx.delete([-9, 0, 8]).sort_values()
        )

        with self.assertRaisesRegex(IndexError, "index 9 is out of bounds for axis 0 with size 9"):
            psidx.delete([0, 9])
        with self.assertRaisesRegex(
            IndexError, "index -10 is out of bounds for axis 0 with size 9"
        ):
            psidx.delete([-10, 0])
        with self.assertRaisesRegex(IndexError, "index 9 is out of bounds for axis 0 with size 9"):
            psidx.delete(9)
        with self.assertRaisesRegex(
            IndexError, "index -10 is out of bounds for axis 0 with size 9"
        ):
            psidx.delete(-10)

        # MultiIndex
        pidx = pd.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])
        psidx = ps.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])

        self.assert_eq(pidx.delete(2).sort_values(), psidx.delete(2).sort_values())
        self.assert_eq(pidx.delete(-3).sort_values(), psidx.delete(-3).sort_values())
        self.assert_eq(
            pidx.delete([-3, 0, 2]).sort_values(), psidx.delete([-3, 0, 2]).sort_values()
        )

        with self.assertRaisesRegex(IndexError, "index 3 is out of bounds for axis 0 with size 3"):
            psidx.delete([0, 3])
        with self.assertRaisesRegex(IndexError, "index -4 is out of bounds for axis 0 with size 3"):
            psidx.delete([-4, 0])
        with self.assertRaisesRegex(IndexError, "index 3 is out of bounds for axis 0 with size 3"):
            psidx.delete(3)
        with self.assertRaisesRegex(IndexError, "index -4 is out of bounds for axis 0 with size 3"):
            psidx.delete(-4)

    def test_append(self):
        # Index
        pidx = pd.Index(range(10000))
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.append(pidx), psidx.append(psidx))

        # Index with name
        pidx1 = pd.Index(range(10000), name="a")
        pidx2 = pd.Index(range(10000), name="b")
        psidx1 = ps.from_pandas(pidx1)
        psidx2 = ps.from_pandas(pidx2)

        self.assert_eq(pidx1.append(pidx2), psidx1.append(psidx2))

        self.assert_eq(pidx2.append(pidx1), psidx2.append(psidx1))

        # Index from DataFrame
        pdf1 = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}, index=["a", "b", "c"])
        pdf2 = pd.DataFrame({"a": [7, 8, 9], "d": [10, 11, None]}, index=["x", "y", "z"])
        psdf1 = ps.from_pandas(pdf1)
        psdf2 = ps.from_pandas(pdf2)

        pidx1 = pdf1.set_index("a").index
        pidx2 = pdf2.set_index("d").index
        psidx1 = psdf1.set_index("a").index
        psidx2 = psdf2.set_index("d").index

        self.assert_eq(pidx1.append(pidx2), psidx1.append(psidx2))

        self.assert_eq(pidx2.append(pidx1), psidx2.append(psidx1))

        # Index from DataFrame with MultiIndex columns
        pdf1 = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        pdf2 = pd.DataFrame({"a": [7, 8, 9], "d": [10, 11, 12]})
        pdf1.columns = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y")])
        pdf2.columns = pd.MultiIndex.from_tuples([("a", "x"), ("d", "y")])
        psdf1 = ps.from_pandas(pdf1)
        psdf2 = ps.from_pandas(pdf2)

        pidx1 = pdf1.set_index(("a", "x")).index
        pidx2 = pdf2.set_index(("d", "y")).index
        psidx1 = psdf1.set_index(("a", "x")).index
        psidx2 = psdf2.set_index(("d", "y")).index

        self.assert_eq(pidx1.append(pidx2), psidx1.append(psidx2))

        self.assert_eq(pidx2.append(pidx1), psidx2.append(psidx1))

        # MultiIndex
        pmidx = pd.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])
        psmidx = ps.from_pandas(pmidx)

        self.assert_eq(pmidx.append(pmidx), psmidx.append(psmidx))

        # MultiIndex with names
        pmidx1 = pd.MultiIndex.from_tuples(
            [("a", "x", 1), ("b", "y", 2), ("c", "z", 3)], names=["x", "y", "z"]
        )
        pmidx2 = pd.MultiIndex.from_tuples(
            [("a", "x", 1), ("b", "y", 2), ("c", "z", 3)], names=["p", "q", "r"]
        )
        psmidx1 = ps.from_pandas(pmidx1)
        psmidx2 = ps.from_pandas(pmidx2)

        self.assert_eq(pmidx1.append(pmidx2), psmidx1.append(psmidx2))

        self.assert_eq(pmidx2.append(pmidx1), psmidx2.append(psmidx1))

        self.assert_eq(pmidx1.append(pmidx2).names, psmidx1.append(psmidx2).names)

        self.assert_eq(pmidx1.append(pmidx2).names, psmidx1.append(psmidx2).names)

        # Index & MultiIndex is currently not supported
        expected_error_message = r"append\(\) between Index & MultiIndex is currently not supported"
        with self.assertRaisesRegex(NotImplementedError, expected_error_message):
            psidx.append(psmidx)
        with self.assertRaisesRegex(NotImplementedError, expected_error_message):
            psmidx.append(psidx)

        # MultiIndexs with different levels is currently not supported
        psmidx3 = ps.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        expected_error_message = (
            r"append\(\) between MultiIndexs with different levels is currently not supported"
        )
        with self.assertRaisesRegex(NotImplementedError, expected_error_message):
            psmidx.append(psmidx3)

    def test_argmin(self):
        pidx = pd.Index([100, 50, 10, 20, 30, 60, 0, 50, 0, 100, 100, 100, 20, 0, 0])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.argmin(), psidx.argmin())

        # MultiIndex
        psidx = ps.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])
        with self.assertRaisesRegex(
            TypeError, "reduction operation 'argmin' not allowed for this dtype"
        ):
            psidx.argmin()

    def test_argmax(self):
        pidx = pd.Index([100, 50, 10, 20, 30, 60, 0, 50, 0, 100, 100, 100, 20, 0, 0])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.argmax(), psidx.argmax())

        # MultiIndex
        psidx = ps.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2), ("c", "z", 3)])
        with self.assertRaisesRegex(
            TypeError, "reduction operation 'argmax' not allowed for this dtype"
        ):
            psidx.argmax()

    def test_min(self):
        pidx = pd.Index([3, 2, 1])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.min(), psidx.min())

        # MultiIndex
        pmidx = pd.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2)])
        psmidx = ps.from_pandas(pmidx)

        self.assert_eq(pmidx.min(), psmidx.min())

        pidx = pd.DatetimeIndex(["2021-02-01", "2021-01-01", "2021-04-01", "2021-03-01"])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.min(), psidx.min())

    def test_max(self):
        pidx = pd.Index([3, 2, 1])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.max(), psidx.max())

        # MultiIndex
        pmidx = pd.MultiIndex.from_tuples([("a", "x", 1), ("b", "y", 2)])
        psmidx = ps.from_pandas(pmidx)

        self.assert_eq(pmidx.max(), psmidx.max())

        pidx = pd.DatetimeIndex(["2021-02-01", "2021-01-01", "2021-04-01", "2021-03-01"])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.max(), psidx.max())

    def test_monotonic(self):
        # test monotonic_increasing & monotonic_decreasing for MultiIndex.
        # Since the Behavior for null value was changed in pandas >= 1.0.0,
        # several cases are tested differently.
        datas = []

        # increasing / decreasing ordered each index level with string
        datas.append([("w", "a"), ("x", "b"), ("y", "c"), ("z", "d")])
        datas.append([("w", "d"), ("x", "c"), ("y", "b"), ("z", "a")])
        datas.append([("z", "a"), ("y", "b"), ("x", "c"), ("w", "d")])
        datas.append([("z", "d"), ("y", "c"), ("x", "b"), ("w", "a")])
        # mixed order each index level with string
        datas.append([("z", "a"), ("x", "b"), ("y", "c"), ("w", "d")])
        datas.append([("z", "a"), ("y", "c"), ("x", "b"), ("w", "d")])

        # increasing / decreasing ordered each index level with integer
        datas.append([(1, 100), (2, 200), (3, 300), (4, 400), (5, 500)])
        datas.append([(1, 500), (2, 400), (3, 300), (4, 200), (5, 100)])
        datas.append([(5, 100), (4, 200), (3, 300), (2, 400), (1, 500)])
        datas.append([(5, 500), (4, 400), (3, 300), (2, 200), (1, 100)])
        # mixed order each index level with integer
        datas.append([(1, 500), (3, 400), (2, 300), (4, 200), (5, 100)])
        datas.append([(1, 100), (2, 300), (3, 200), (4, 400), (5, 500)])

        # integer / negative mixed tests
        datas.append([("a", -500), ("b", -400), ("c", -300), ("d", -200), ("e", -100)])
        datas.append([("e", -500), ("d", -400), ("c", -300), ("b", -200), ("a", -100)])
        datas.append([(-5, "a"), (-4, "b"), (-3, "c"), (-2, "d"), (-1, "e")])
        datas.append([(-5, "e"), (-4, "d"), (-3, "c"), (-2, "b"), (-1, "a")])
        datas.append([(-5, "e"), (-3, "d"), (-2, "c"), (-4, "b"), (-1, "a")])
        datas.append([(-5, "e"), (-4, "c"), (-3, "b"), (-2, "d"), (-1, "a")])

        # boolean type tests
        datas.append([(True, True), (True, True)])
        datas.append([(True, True), (True, False)])
        datas.append([(True, False), (True, True)])
        datas.append([(False, True), (False, True)])
        datas.append([(False, True), (False, False)])
        datas.append([(False, False), (False, True)])
        datas.append([(True, True), (False, True)])
        datas.append([(True, True), (False, False)])
        datas.append([(True, False), (False, True)])
        datas.append([(False, True), (True, True)])
        datas.append([(False, True), (True, False)])
        datas.append([(False, False), (True, True)])

        # duplicated index value tests
        datas.append([("x", "d"), ("y", "c"), ("y", "b"), ("z", "a")])
        datas.append([("x", "d"), ("y", "b"), ("y", "c"), ("z", "a")])

        # more depth tests
        datas.append([("x", "d", "o"), ("y", "c", "p"), ("y", "c", "q"), ("z", "a", "r")])
        datas.append([("x", "d", "o"), ("y", "c", "q"), ("y", "c", "p"), ("z", "a", "r")])

        # None type tests (None type is treated as False from pandas >= 1.1.4)
        # Refer https://github.com/pandas-dev/pandas/issues/37220
        datas.append([(1, 100), (2, 200), (None, 300), (4, 400), (5, 500)])
        datas.append([(1, 100), (2, 200), (None, None), (4, 400), (5, 500)])
        datas.append([("x", "d"), ("y", "c"), ("y", None), ("z", "a")])
        datas.append([("x", "d"), ("y", "c"), ("y", "b"), (None, "a")])
        datas.append([("x", "d"), ("y", "b"), ("y", "c"), (None, "a")])
        datas.append([("x", "d", "o"), ("y", "c", "p"), ("y", "c", None), ("z", "a", "r")])

        for data in datas:
            with self.subTest(data=data):
                pmidx = pd.MultiIndex.from_tuples(data)
                psmidx = ps.from_pandas(pmidx)
                self.assert_eq(psmidx.is_monotonic_increasing, pmidx.is_monotonic_increasing)
                self.assert_eq(psmidx.is_monotonic_decreasing, pmidx.is_monotonic_decreasing)

        # datas below return different result depends on pandas version.
        # Because the behavior of handling null values is changed in pandas >= 1.1.4.
        # Since Koalas follows latest pandas, all of them should return `False`.
        datas = []
        datas.append([(1, 100), (2, 200), (3, None), (4, 400), (5, 500)])
        datas.append([(1, None), (2, 200), (3, 300), (4, 400), (5, 500)])
        datas.append([(1, 100), (2, 200), (3, 300), (4, 400), (5, None)])
        datas.append([(False, None), (True, True)])
        datas.append([(None, False), (True, True)])
        datas.append([(False, False), (True, None)])
        datas.append([(False, False), (None, True)])
        datas.append([("x", "d"), ("y", None), ("y", None), ("z", "a")])
        datas.append([("x", "d", "o"), ("y", "c", None), ("y", "c", None), ("z", "a", "r")])
        datas.append([(1, 100), (2, 200), (3, 300), (4, 400), (None, 500)])
        datas.append([(1, 100), (2, 200), (3, 300), (4, 400), (None, None)])
        datas.append([(5, 100), (4, 200), (3, None), (2, 400), (1, 500)])
        datas.append([(5, None), (4, 200), (3, 300), (2, 400), (1, 500)])
        datas.append([(5, 100), (4, 200), (3, None), (2, 400), (1, 500)])
        datas.append([(5, 100), (4, 200), (3, 300), (2, 400), (1, None)])
        datas.append([(True, None), (True, True)])
        datas.append([(None, True), (True, True)])
        datas.append([(True, True), (None, True)])
        datas.append([(True, True), (True, None)])
        datas.append([(None, 100), (2, 200), (3, 300), (4, 400), (5, 500)])
        datas.append([(None, None), (2, 200), (3, 300), (4, 400), (5, 500)])
        datas.append([("x", "d"), ("y", None), ("y", "c"), ("z", "a")])
        datas.append([("x", "d", "o"), ("y", "c", None), ("y", "c", "q"), ("z", "a", "r")])

        for data in datas:
            with self.subTest(data=data):
                pmidx = pd.MultiIndex.from_tuples(data)
                psmidx = ps.from_pandas(pmidx)
                if LooseVersion(pd.__version__) < LooseVersion("1.1.4"):
                    self.assert_eq(psmidx.is_monotonic_increasing, False)
                    self.assert_eq(psmidx.is_monotonic_decreasing, False)
                else:
                    self.assert_eq(psmidx.is_monotonic_increasing, pmidx.is_monotonic_increasing)
                    self.assert_eq(psmidx.is_monotonic_decreasing, pmidx.is_monotonic_decreasing)

        # The datas below are tested another way since they cannot be an arguments for
        # `MultiIndex.from_tuples` in pandas >= 1.1.0.
        # Refer https://github.com/databricks/koalas/pull/1688#issuecomment-667156560 for detail.
        if LooseVersion(pd.__version__) < LooseVersion("1.1.0"):
            pmidx = pd.MultiIndex.from_tuples(
                [(-5, None), (-4, None), (-3, None), (-2, None), (-1, None)]
            )
            psmidx = ps.from_pandas(pmidx)
            self.assert_eq(psmidx.is_monotonic_increasing, False)
            self.assert_eq(psmidx.is_monotonic_decreasing, False)

            pmidx = pd.MultiIndex.from_tuples(
                [(None, "e"), (None, "c"), (None, "b"), (None, "d"), (None, "a")]
            )
            psmidx = ps.from_pandas(pmidx)
            self.assert_eq(psmidx.is_monotonic_increasing, False)
            self.assert_eq(psmidx.is_monotonic_decreasing, False)

            pmidx = pd.MultiIndex.from_tuples(
                [(None, None), (None, None), (None, None), (None, None), (None, None)]
            )
            psmidx = ps.from_pandas(pmidx)
            self.assert_eq(psmidx.is_monotonic_increasing, False)
            self.assert_eq(psmidx.is_monotonic_decreasing, False)

            pmidx = pd.MultiIndex.from_tuples([(None, None)])
            psmidx = ps.from_pandas(pmidx)
            self.assert_eq(psmidx.is_monotonic_increasing, False)
            self.assert_eq(psmidx.is_monotonic_decreasing, False)

        else:
            # For [(-5, None), (-4, None), (-3, None), (-2, None), (-1, None)]
            psdf = ps.DataFrame({"a": [-5, -4, -3, -2, -1], "b": [1, 1, 1, 1, 1]})
            psdf["b"] = None
            psmidx = psdf.set_index(["a", "b"]).index
            pmidx = psmidx.to_pandas()
            self.assert_eq(psmidx.is_monotonic_increasing, pmidx.is_monotonic_increasing)
            self.assert_eq(psmidx.is_monotonic_decreasing, pmidx.is_monotonic_decreasing)

            # For [(None, "e"), (None, "c"), (None, "b"), (None, "d"), (None, "a")]
            psdf = ps.DataFrame({"a": [1, 1, 1, 1, 1], "b": ["e", "c", "b", "d", "a"]})
            psdf["a"] = None
            psmidx = psdf.set_index(["a", "b"]).index
            pmidx = psmidx.to_pandas()
            self.assert_eq(psmidx.is_monotonic_increasing, pmidx.is_monotonic_increasing)
            self.assert_eq(psmidx.is_monotonic_decreasing, pmidx.is_monotonic_decreasing)

            # For [(None, None), (None, None), (None, None), (None, None), (None, None)]
            psdf = ps.DataFrame({"a": [1, 1, 1, 1, 1], "b": [1, 1, 1, 1, 1]})
            psdf["a"] = None
            psdf["b"] = None
            psmidx = psdf.set_index(["a", "b"]).index
            pmidx = psmidx.to_pandas()
            self.assert_eq(psmidx.is_monotonic_increasing, pmidx.is_monotonic_increasing)
            self.assert_eq(psmidx.is_monotonic_decreasing, pmidx.is_monotonic_decreasing)

            # For [(None, None)]
            psdf = ps.DataFrame({"a": [1], "b": [1]})
            psdf["a"] = None
            psdf["b"] = None
            psmidx = psdf.set_index(["a", "b"]).index
            pmidx = psmidx.to_pandas()
            self.assert_eq(psmidx.is_monotonic_increasing, pmidx.is_monotonic_increasing)
            self.assert_eq(psmidx.is_monotonic_decreasing, pmidx.is_monotonic_decreasing)

    def test_difference(self):
        # Index
        pidx1 = pd.Index([1, 2, 3, 4], name="koalas")
        pidx2 = pd.Index([3, 4, 5, 6], name="koalas")
        psidx1 = ps.from_pandas(pidx1)
        psidx2 = ps.from_pandas(pidx2)
        # Series
        pser = pd.Series([3, 4, 5, 6], name="koalas")
        psser = ps.from_pandas(pser)

        self.assert_eq(
            psidx1.difference(psidx2).sort_values(), pidx1.difference(pidx2).sort_values()
        )
        self.assert_eq(psidx1.difference(psser).sort_values(), pidx1.difference(pser).sort_values())
        self.assert_eq(
            psidx1.difference([3, 4, 5, 6]).sort_values(),
            pidx1.difference([3, 4, 5, 6]).sort_values(),
        )
        self.assert_eq(
            psidx1.difference((3, 4, 5, 6)).sort_values(),
            pidx1.difference((3, 4, 5, 6)).sort_values(),
        )
        self.assert_eq(
            psidx1.difference({3, 4, 5, 6}).sort_values(),
            pidx1.difference({3, 4, 5, 6}).sort_values(),
        )
        self.assert_eq(
            psidx1.difference({3: 1, 4: 2, 5: 3, 6: 4}).sort_values(),
            pidx1.difference({3: 1, 4: 2, 5: 3, 6: 4}).sort_values(),
        )

        # Exceptions for Index
        with self.assertRaisesRegex(TypeError, "Input must be Index or array-like"):
            psidx1.difference("1234")
        with self.assertRaisesRegex(TypeError, "Input must be Index or array-like"):
            psidx1.difference(1234)
        with self.assertRaisesRegex(TypeError, "Input must be Index or array-like"):
            psidx1.difference(12.34)
        with self.assertRaisesRegex(TypeError, "Input must be Index or array-like"):
            psidx1.difference(None)
        with self.assertRaisesRegex(TypeError, "Input must be Index or array-like"):
            psidx1.difference(np.nan)
        with self.assertRaisesRegex(
            ValueError, "The 'sort' keyword only takes the values of None or True; 1 was passed."
        ):
            psidx1.difference(psidx2, sort=1)

        # MultiIndex
        pmidx1 = pd.MultiIndex.from_tuples(
            [("a", "x", 1), ("b", "y", 2), ("c", "z", 3)], names=["hello", "koalas", "world"]
        )
        pmidx2 = pd.MultiIndex.from_tuples(
            [("a", "x", 1), ("b", "z", 2), ("k", "z", 3)], names=["hello", "koalas", "world"]
        )
        psmidx1 = ps.from_pandas(pmidx1)
        psmidx2 = ps.from_pandas(pmidx2)

        self.assert_eq(
            psmidx1.difference(psmidx2).sort_values(), pmidx1.difference(pmidx2).sort_values()
        )
        self.assert_eq(
            psmidx1.difference(psidx1).sort_values(), pmidx1.difference(pidx1).sort_values()
        )
        self.assert_eq(
            psidx1.difference(psmidx1).sort_values(), pidx1.difference(pmidx1).sort_values()
        )
        self.assert_eq(psidx1.difference(psser).sort_values(), pidx1.difference(pser).sort_values())
        self.assert_eq(
            psmidx1.difference({("a", "x", 1)}).sort_values(),
            pmidx1.difference({("a", "x", 1)}).sort_values(),
        )
        self.assert_eq(
            psmidx1.difference({("a", "x", 1): [1, 2, 3]}).sort_values(),
            pmidx1.difference({("a", "x", 1): [1, 2, 3]}).sort_values(),
        )

        # Exceptions for MultiIndex
        with self.assertRaisesRegex(TypeError, "other must be a MultiIndex or a list of tuples"):
            psmidx1.difference(["b", "z", "2"])

    def test_repeat(self):
        pidx = pd.Index(["a", "b", "c"])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(psidx.repeat(3).sort_values(), pidx.repeat(3).sort_values())
        self.assert_eq(psidx.repeat(0).sort_values(), pidx.repeat(0).sort_values())
        self.assert_eq((psidx + "x").repeat(3).sort_values(), (pidx + "x").repeat(3).sort_values())

        self.assertRaises(ValueError, lambda: psidx.repeat(-1))
        self.assertRaises(TypeError, lambda: psidx.repeat("abc"))

        pmidx = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        psmidx = ps.from_pandas(pmidx)

        self.assert_eq(psmidx.repeat(3).sort_values(), pmidx.repeat(3).sort_values())
        self.assert_eq(psmidx.repeat(0).sort_values(), pmidx.repeat(0).sort_values(), almost=True)

        self.assertRaises(ValueError, lambda: psmidx.repeat(-1))
        self.assertRaises(TypeError, lambda: psmidx.repeat("abc"))

    def test_unique(self):
        pidx = pd.Index(["a", "b", "a"])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(psidx.unique().sort_values(), pidx.unique().sort_values())
        self.assert_eq(psidx.unique().sort_values(), pidx.unique().sort_values())

        pmidx = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("x", "a")])
        psmidx = ps.from_pandas(pmidx)

        self.assert_eq(psmidx.unique().sort_values(), pmidx.unique().sort_values())
        self.assert_eq(psmidx.unique().sort_values(), pmidx.unique().sort_values())

        with self.assertRaisesRegex(
            IndexError, "Too many levels: Index has only 1 level, -2 is not a valid level number"
        ):
            psidx.unique(level=-2)

    def test_asof(self):
        # Increasing values
        pidx = pd.Index(["2013-12-31", "2014-01-02", "2014-01-03"])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(psidx.asof("2014-01-01"), pidx.asof("2014-01-01"))
        self.assert_eq(psidx.asof("2014-01-02"), pidx.asof("2014-01-02"))
        self.assert_eq(repr(psidx.asof("1999-01-02")), repr(pidx.asof("1999-01-02")))
        self.assert_eq(psidx.asof("2014-01-04"), pidx.asof("2014-01-04"))

        pidx = pd.DatetimeIndex(["2013-12-31", "2014-01-02", "2014-01-03"])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(psidx.asof("2014-01-01"), pidx.asof("2014-01-01"))
        self.assert_eq(psidx.asof("2014-01-02"), pidx.asof("2014-01-02"))
        self.assert_eq(repr(psidx.asof("1999-01-02")), repr(pidx.asof("1999-01-02")))

        # Decreasing values
        pidx = pd.Index(["2014-01-03", "2014-01-02", "2013-12-31"])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(psidx.asof("2014-01-01"), pidx.asof("2014-01-01"))
        self.assert_eq(psidx.asof("2014-01-02"), pidx.asof("2014-01-02"))
        self.assert_eq(psidx.asof("1999-01-02"), pidx.asof("1999-01-02"))
        self.assert_eq(repr(psidx.asof("2015-01-02")), repr(pidx.asof("2015-01-02")))

        pidx = pd.DatetimeIndex(["2014-01-03", "2014-01-02", "2013-12-31"])
        psidx = ps.from_pandas(pidx)

        # TODO: a pandas bug?
        # self.assert_eq(psidx.asof("2014-01-01"), pidx.asof("2014-01-01"))
        # self.assert_eq(psidx.asof("2014-01-02"), pidx.asof("2014-01-02"))
        # self.assert_eq(psidx.asof("1999-01-02"), pidx.asof("1999-01-02"))
        # self.assert_eq(repr(psidx.asof("2015-01-02")), repr(pidx.asof("2015-01-02")))
        self.assert_eq(psidx.asof("2014-01-01"), pd.Timestamp("2014-01-02 00:00:00"))
        self.assert_eq(psidx.asof("2014-01-02"), pd.Timestamp("2014-01-02 00:00:00"))
        self.assert_eq(psidx.asof("1999-01-02"), pd.Timestamp("2013-12-31 00:00:00"))
        self.assert_eq(repr(psidx.asof("2015-01-02")), repr(pd.NaT))

        # Not increasing, neither decreasing (ValueError)
        psidx = ps.Index(["2013-12-31", "2015-01-02", "2014-01-03"])
        self.assertRaises(ValueError, lambda: psidx.asof("2013-12-31"))

        psmidx = ps.MultiIndex.from_tuples([("a", "a"), ("a", "b"), ("a", "c")])
        self.assertRaises(NotImplementedError, lambda: psmidx.asof(("a", "b")))

    def test_union(self):
        # Index
        pidx1 = pd.Index([1, 2, 3, 4])
        pidx2 = pd.Index([3, 4, 5, 6])
        pidx3 = pd.Index([7.0, 8.0, 9.0, 10.0])
        psidx1 = ps.from_pandas(pidx1)
        psidx2 = ps.from_pandas(pidx2)
        psidx3 = ps.from_pandas(pidx3)

        self.assert_eq(psidx1.union(psidx2), pidx1.union(pidx2))
        self.assert_eq(psidx2.union(psidx1), pidx2.union(pidx1))
        self.assert_eq(psidx1.union(psidx3), pidx1.union(pidx3))
        # Deprecated case, but adding to track if pandas stop supporting union
        # as a set operation. It should work fine until stop supporting anyway.
        self.assert_eq(pidx1 | pidx2, psidx1 | psidx2)

        self.assert_eq(psidx1.union([3, 4, 5, 6]), pidx1.union([3, 4, 5, 6]), almost=True)
        self.assert_eq(psidx2.union([1, 2, 3, 4]), pidx2.union([1, 2, 3, 4]), almost=True)
        self.assert_eq(
            psidx1.union(ps.Series([3, 4, 5, 6])), pidx1.union(pd.Series([3, 4, 5, 6])), almost=True
        )
        self.assert_eq(
            psidx2.union(ps.Series([1, 2, 3, 4])), pidx2.union(pd.Series([1, 2, 3, 4])), almost=True
        )

        # Testing if the result is correct after sort=False.
        self.assert_eq(
            psidx1.union(psidx2, sort=False).sort_values(),
            pidx1.union(pidx2, sort=False).sort_values(),
        )
        self.assert_eq(
            psidx2.union(psidx1, sort=False).sort_values(),
            pidx2.union(pidx1, sort=False).sort_values(),
        )
        self.assert_eq(
            psidx1.union([3, 4, 5, 6], sort=False).sort_values(),
            pidx1.union([3, 4, 5, 6], sort=False).sort_values(),
            almost=True,
        )
        self.assert_eq(
            psidx2.union([1, 2, 3, 4], sort=False).sort_values(),
            pidx2.union([1, 2, 3, 4], sort=False).sort_values(),
            almost=True,
        )
        self.assert_eq(
            psidx1.union(ps.Series([3, 4, 5, 6]), sort=False).sort_values(),
            pidx1.union(pd.Series([3, 4, 5, 6]), sort=False).sort_values(),
            almost=True,
        )
        self.assert_eq(
            psidx2.union(ps.Series([1, 2, 3, 4]), sort=False).sort_values(),
            pidx2.union(pd.Series([1, 2, 3, 4]), sort=False).sort_values(),
            almost=True,
        )

        pidx1 = pd.Index([1, 2, 3, 4, 3, 4, 3, 4])
        pidx2 = pd.Index([3, 4, 3, 4, 5, 6])
        psidx1 = ps.from_pandas(pidx1)
        psidx2 = ps.from_pandas(pidx2)

        self.assert_eq(psidx1.union(psidx2), pidx1.union(pidx2))
        self.assert_eq(
            psidx1.union([3, 4, 3, 3, 5, 6]), pidx1.union([3, 4, 3, 4, 5, 6]), almost=True
        )
        self.assert_eq(
            psidx1.union(ps.Series([3, 4, 3, 3, 5, 6])),
            pidx1.union(pd.Series([3, 4, 3, 4, 5, 6])),
            almost=True,
        )

        # Manually create the expected result here since there is a bug in Index.union
        # dropping duplicated values in pandas < 1.3.
        expected = pd.Index([1, 2, 3, 3, 3, 4, 4, 4, 5, 6])
        self.assert_eq(psidx2.union(psidx1), expected)
        self.assert_eq(
            psidx2.union([1, 2, 3, 4, 3, 4, 3, 4]),
            expected,
            almost=True,
        )
        self.assert_eq(
            psidx2.union(ps.Series([1, 2, 3, 4, 3, 4, 3, 4])),
            expected,
            almost=True,
        )

        # MultiIndex
        pmidx1 = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("x", "a"), ("x", "b")])
        pmidx2 = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("x", "c"), ("x", "d")])
        pmidx3 = pd.MultiIndex.from_tuples([(1, 1), (1, 2), (1, 3), (1, 4), (1, 3), (1, 4)])
        pmidx4 = pd.MultiIndex.from_tuples([(1, 3), (1, 4), (1, 5), (1, 6)])
        psmidx1 = ps.from_pandas(pmidx1)
        psmidx2 = ps.from_pandas(pmidx2)
        psmidx3 = ps.from_pandas(pmidx3)
        psmidx4 = ps.from_pandas(pmidx4)

        # Manually create the expected result here since there is a bug in MultiIndex.union
        # dropping duplicated values in pandas < 1.3.
        expected = pd.MultiIndex.from_tuples(
            [("x", "a"), ("x", "a"), ("x", "b"), ("x", "b"), ("x", "c"), ("x", "d")]
        )
        self.assert_eq(psmidx1.union(psmidx2), expected)
        self.assert_eq(psmidx2.union(psmidx1), expected)
        self.assert_eq(
            psmidx1.union([("x", "a"), ("x", "b"), ("x", "c"), ("x", "d")]),
            expected,
        )
        self.assert_eq(
            psmidx2.union([("x", "a"), ("x", "b"), ("x", "a"), ("x", "b")]),
            expected,
        )

        expected = pd.MultiIndex.from_tuples(
            [(1, 1), (1, 2), (1, 3), (1, 3), (1, 4), (1, 4), (1, 5), (1, 6)]
        )
        self.assert_eq(psmidx3.union(psmidx4), expected)
        self.assert_eq(psmidx4.union(psmidx3), expected)
        self.assert_eq(
            psmidx3.union([(1, 3), (1, 4), (1, 5), (1, 6)]),
            expected,
        )
        self.assert_eq(
            psmidx4.union([(1, 1), (1, 2), (1, 3), (1, 4), (1, 3), (1, 4)]),
            expected,
        )

        # Testing if the result is correct after sort=False.
        # Manually create the expected result here since there is a bug in MultiIndex.union
        # dropping duplicated values in pandas < 1.3.
        expected = pd.MultiIndex.from_tuples(
            [("x", "a"), ("x", "a"), ("x", "b"), ("x", "b"), ("x", "c"), ("x", "d")]
        )
        self.assert_eq(psmidx1.union(psmidx2, sort=False).sort_values(), expected)
        self.assert_eq(psmidx2.union(psmidx1, sort=False).sort_values(), expected)
        self.assert_eq(
            psmidx1.union(
                [("x", "a"), ("x", "b"), ("x", "c"), ("x", "d")], sort=False
            ).sort_values(),
            expected,
        )
        self.assert_eq(
            psmidx2.union(
                [("x", "a"), ("x", "b"), ("x", "a"), ("x", "b")], sort=False
            ).sort_values(),
            expected,
        )

        expected = pd.MultiIndex.from_tuples(
            [(1, 1), (1, 2), (1, 3), (1, 3), (1, 4), (1, 4), (1, 5), (1, 6)]
        )
        self.assert_eq(psmidx3.union(psmidx4, sort=False).sort_values(), expected)
        self.assert_eq(psmidx4.union(psmidx3, sort=False).sort_values(), expected)
        self.assert_eq(
            psmidx3.union([(1, 3), (1, 4), (1, 5), (1, 6)], sort=False).sort_values(), expected
        )
        self.assert_eq(
            psmidx4.union(
                [(1, 1), (1, 2), (1, 3), (1, 4), (1, 3), (1, 4)], sort=False
            ).sort_values(),
            expected,
        )

        self.assertRaises(NotImplementedError, lambda: psidx1.union(psmidx1))
        self.assertRaises(TypeError, lambda: psmidx1.union(psidx1))
        self.assertRaises(TypeError, lambda: psmidx1.union(["x", "a"]))
        self.assertRaises(ValueError, lambda: psidx1.union(ps.range(2)))

    def test_take(self):
        # Index
        pidx = pd.Index([100, 200, 300, 400, 500], name="Koalas")
        psidx = ps.from_pandas(pidx)

        self.assert_eq(psidx.take([0, 2, 4]).sort_values(), pidx.take([0, 2, 4]).sort_values())
        self.assert_eq(
            psidx.take(range(0, 5, 2)).sort_values(), pidx.take(range(0, 5, 2)).sort_values()
        )
        self.assert_eq(psidx.take([-4, -2, 0]).sort_values(), pidx.take([-4, -2, 0]).sort_values())
        self.assert_eq(
            psidx.take(range(-4, 1, 2)).sort_values(), pidx.take(range(-4, 1, 2)).sort_values()
        )

        # MultiIndex
        pmidx = pd.MultiIndex.from_tuples(
            [("x", "a"), ("x", "b"), ("x", "c")], names=["hello", "Koalas"]
        )
        psmidx = ps.from_pandas(pmidx)

        self.assert_eq(psmidx.take([0, 2]).sort_values(), pmidx.take([0, 2]).sort_values())
        self.assert_eq(
            psmidx.take(range(0, 4, 2)).sort_values(), pmidx.take(range(0, 4, 2)).sort_values()
        )
        self.assert_eq(psmidx.take([-2, 0]).sort_values(), pmidx.take([-2, 0]).sort_values())
        self.assert_eq(
            psmidx.take(range(-2, 1, 2)).sort_values(), pmidx.take(range(-2, 1, 2)).sort_values()
        )

        # Checking the type of indices.
        self.assertRaises(TypeError, lambda: psidx.take(1))
        self.assertRaises(TypeError, lambda: psidx.take("1"))
        self.assertRaises(TypeError, lambda: psidx.take({1, 2}))
        self.assertRaises(TypeError, lambda: psidx.take({1: None, 2: None}))
        self.assertRaises(TypeError, lambda: psmidx.take(1))
        self.assertRaises(TypeError, lambda: psmidx.take("1"))
        self.assertRaises(TypeError, lambda: psmidx.take({1, 2}))
        self.assertRaises(TypeError, lambda: psmidx.take({1: None, 2: None}))

    def test_index_get_level_values(self):
        pidx = pd.Index([1, 2, 3], name="ks")
        psidx = ps.from_pandas(pidx)

        for level in [0, "ks"]:
            self.assert_eq(psidx.get_level_values(level), pidx.get_level_values(level))

    def test_multiindex_get_level_values(self):
        pmidx = pd.MultiIndex.from_tuples([("a", "d"), ("b", "e"), ("c", "f")])
        pmidx.names = ["level_1", "level_2"]
        psmidx = ps.from_pandas(pmidx)

        for level in [0, 1, "level_1", "level_2"]:
            self.assert_eq(psmidx.get_level_values(level), pmidx.get_level_values(level))

    def test_index_get_level_number(self):
        # name of two levels are the same, which is None
        psdf = ps.DataFrame({"a": [1, 2, 3]}, index=[list("aac"), list("ddf")])
        with self.assertRaisesRegex(
            ValueError, "The name None occurs multiple times, use a level number"
        ):
            psdf.index._get_level_number(None)

        mi = pd.MultiIndex.from_arrays((list("abc"), list("def")))
        mi.names = ["level_1", "level_2"]
        psdf = ps.DataFrame({"a": [1, 2, 3]}, index=mi)

        # level is not int and not in the level name list
        with self.assertRaisesRegex(KeyError, "Level lv_3 not found"):
            psdf.index._get_level_number("lv_3")

        # level is int, but an invalid negative number
        with self.assertRaisesRegex(IndexError, "Too many levels: Index has only"):
            psdf.index._get_level_number(-3)

        # level is int, but an invalid positive number
        with self.assertRaisesRegex(IndexError, "Too many levels: Index has only"):
            psdf.index._get_level_number(3)

        # Correct and valid inputs in numbers
        level_number = [-2, -1, 0, 1]
        outputs = [0, 1, 0, 1]

        for lv, output in zip(level_number, outputs):
            self.assertEqual(output, psdf.index._get_level_number(lv))

        # Valid inputs as level names
        level_names = ["level_1", "level_2"]
        outputs = [0, 1]

        for lv, output in zip(level_names, outputs):
            self.assertEqual(output, psdf.index._get_level_number(lv))

    def test_holds_integer(self):
        pidx = pd.Index([1, 2, 3, 4])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.holds_integer(), psidx.holds_integer())

        pidx = pd.Index([1.1, 2.2, 3.3, 4.4])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.holds_integer(), psidx.holds_integer())

        pidx = pd.Index(["A", "B", "C", "D"])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.holds_integer(), psidx.holds_integer())

        # MultiIndex
        pmidx = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "a")])
        psmidx = ps.from_pandas(pmidx)
        self.assert_eq(pmidx.holds_integer(), psmidx.holds_integer())

        pmidx = pd.MultiIndex.from_tuples([(10, 1), (10, 2), (20, 1)])
        psmidx = ps.from_pandas(pmidx)
        self.assert_eq(pmidx.holds_integer(), psmidx.holds_integer())

    def test_abs(self):
        pidx = pd.Index([-2, -1, 0, 1])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(abs(pidx), abs(psidx))
        self.assert_eq(np.abs(pidx), np.abs(psidx))

        psidx = ps.MultiIndex.from_tuples([(1, 2)], names=["level1", "level2"])
        with self.assertRaisesRegex(TypeError, "perform __abs__ with this index"):
            abs(psidx)

    def test_hasnans(self):
        # BooleanType
        pidx = pd.Index([True, False, True, True])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.hasnans, psidx.hasnans)

        pidx = pd.Index([True, False, np.nan, True])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.hasnans, psidx.hasnans)

        # TimestampType
        pser = pd.Series([pd.Timestamp("2020-07-30") for _ in range(3)])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.hasnans, psser.hasnans)

        pser = pd.Series([pd.Timestamp("2020-07-30"), np.nan, pd.Timestamp("2020-07-30")])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.hasnans, psser.hasnans)

        # empty
        pidx = pd.Index([])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.hasnans, psidx.hasnans)

        # Not supported for MultiIndex
        psmidx = ps.Index([("a", 1), ("b", 2)])
        self.assertRaises(NotImplementedError, lambda: psmidx.hasnans())

    def test_intersection(self):
        pidx = pd.Index([1, 2, 3, 4], name="Koalas")
        psidx = ps.from_pandas(pidx)

        # other = Index
        pidx_other = pd.Index([3, 4, 5, 6], name="Koalas")
        psidx_other = ps.from_pandas(pidx_other)
        self.assert_eq(pidx.intersection(pidx_other), psidx.intersection(psidx_other).sort_values())
        self.assert_eq(
            (pidx + 1).intersection(pidx_other), (psidx + 1).intersection(psidx_other).sort_values()
        )
        # Deprecated case, but adding to track if pandas stop supporting intersection
        # as a set operation. It should work fine until stop supporting anyway.
        self.assert_eq(pidx & pidx_other, (psidx & psidx_other).sort_values())

        pidx_other_different_name = pd.Index([3, 4, 5, 6], name="Databricks")
        psidx_other_different_name = ps.from_pandas(pidx_other_different_name)
        self.assert_eq(
            pidx.intersection(pidx_other_different_name),
            psidx.intersection(psidx_other_different_name).sort_values(),
        )
        self.assert_eq(
            (pidx + 1).intersection(pidx_other_different_name),
            (psidx + 1).intersection(psidx_other_different_name).sort_values(),
        )

        pidx_other_from_frame = pd.DataFrame({"a": [3, 4, 5, 6]}).set_index("a").index
        psidx_other_from_frame = ps.from_pandas(pidx_other_from_frame)
        self.assert_eq(
            pidx.intersection(pidx_other_from_frame),
            psidx.intersection(psidx_other_from_frame).sort_values(),
        )
        self.assert_eq(
            (pidx + 1).intersection(pidx_other_from_frame),
            (psidx + 1).intersection(psidx_other_from_frame).sort_values(),
        )

        # other = MultiIndex
        pmidx = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        psmidx = ps.from_pandas(pmidx)
        if LooseVersion(pd.__version__) < LooseVersion("1.2.0"):
            self.assert_eq(
                psidx.intersection(psmidx).sort_values(),
                psidx._psdf.head(0).index.rename(None),
                almost=True,
            )
            self.assert_eq(
                (psidx + 1).intersection(psmidx).sort_values(),
                psidx._psdf.head(0).index.rename(None),
                almost=True,
            )
        else:
            self.assert_eq(
                pidx.intersection(pmidx), psidx.intersection(psmidx).sort_values(), almost=True
            )
            self.assert_eq(
                (pidx + 1).intersection(pmidx),
                (psidx + 1).intersection(psmidx).sort_values(),
                almost=True,
            )

        # other = Series
        pser = pd.Series([3, 4, 5, 6])
        psser = ps.from_pandas(pser)
        if LooseVersion(pd.__version__) < LooseVersion("1.2.0"):
            self.assert_eq(psidx.intersection(psser).sort_values(), ps.Index([3, 4], name="Koalas"))
            self.assert_eq(
                (psidx + 1).intersection(psser).sort_values(), ps.Index([3, 4, 5], name="Koalas")
            )
        else:
            self.assert_eq(pidx.intersection(pser), psidx.intersection(psser).sort_values())
            self.assert_eq(
                (pidx + 1).intersection(pser), (psidx + 1).intersection(psser).sort_values()
            )

        pser_different_name = pd.Series([3, 4, 5, 6], name="Databricks")
        psser_different_name = ps.from_pandas(pser_different_name)
        if LooseVersion(pd.__version__) < LooseVersion("1.2.0"):
            self.assert_eq(
                psidx.intersection(psser_different_name).sort_values(),
                ps.Index([3, 4], name="Koalas"),
            )
            self.assert_eq(
                (psidx + 1).intersection(psser_different_name).sort_values(),
                ps.Index([3, 4, 5], name="Koalas"),
            )
        else:
            self.assert_eq(
                pidx.intersection(pser_different_name),
                psidx.intersection(psser_different_name).sort_values(),
            )
            self.assert_eq(
                (pidx + 1).intersection(pser_different_name),
                (psidx + 1).intersection(psser_different_name).sort_values(),
            )

        others = ([3, 4, 5, 6], (3, 4, 5, 6), {3: None, 4: None, 5: None, 6: None})
        for other in others:
            if LooseVersion(pd.__version__) < LooseVersion("1.2.0"):
                self.assert_eq(
                    psidx.intersection(other).sort_values(), ps.Index([3, 4], name="Koalas")
                )
                self.assert_eq(
                    (psidx + 1).intersection(other).sort_values(),
                    ps.Index([3, 4, 5], name="Koalas"),
                )
            else:
                self.assert_eq(pidx.intersection(other), psidx.intersection(other).sort_values())
                self.assert_eq(
                    (pidx + 1).intersection(other), (psidx + 1).intersection(other).sort_values()
                )

        # MultiIndex / other = Index
        self.assert_eq(
            pmidx.intersection(pidx), psmidx.intersection(psidx).sort_values(), almost=True
        )
        self.assert_eq(
            pmidx.intersection(pidx_other_from_frame),
            psmidx.intersection(psidx_other_from_frame).sort_values(),
            almost=True,
        )

        # MultiIndex / other = MultiIndex
        pmidx_other = pd.MultiIndex.from_tuples([("c", "z"), ("d", "w")])
        psmidx_other = ps.from_pandas(pmidx_other)
        self.assert_eq(
            pmidx.intersection(pmidx_other), psmidx.intersection(psmidx_other).sort_values()
        )

        # MultiIndex / other = list
        other = [("c", "z"), ("d", "w")]
        self.assert_eq(pmidx.intersection(other), psmidx.intersection(other).sort_values())

        # MultiIndex / other = tuple
        other = (("c", "z"), ("d", "w"))
        self.assert_eq(pmidx.intersection(other), psmidx.intersection(other).sort_values())

        # MultiIndex / other = dict
        other = {("c", "z"): None, ("d", "w"): None}
        self.assert_eq(pmidx.intersection(other), psmidx.intersection(other).sort_values())

        # MultiIndex with different names.
        pmidx1 = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")], names=["X", "Y"])
        pmidx2 = pd.MultiIndex.from_tuples([("c", "z"), ("d", "w")], names=["A", "B"])
        psmidx1 = ps.from_pandas(pmidx1)
        psmidx2 = ps.from_pandas(pmidx2)
        self.assert_eq(pmidx1.intersection(pmidx2), psmidx1.intersection(psmidx2).sort_values())

        with self.assertRaisesRegex(TypeError, "Input must be Index or array-like"):
            psidx.intersection(4)
        with self.assertRaisesRegex(TypeError, "other must be a MultiIndex or a list of tuples"):
            psmidx.intersection(4)
        with self.assertRaisesRegex(TypeError, "other must be a MultiIndex or a list of tuples"):
            psmidx.intersection(ps.Series([3, 4, 5, 6]))
        with self.assertRaisesRegex(TypeError, "other must be a MultiIndex or a list of tuples"):
            psmidx.intersection([("c", "z"), ["d", "w"]])
        with self.assertRaisesRegex(ValueError, "Index data must be 1-dimensional"):
            psidx.intersection(ps.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]}))
        with self.assertRaisesRegex(ValueError, "Index data must be 1-dimensional"):
            psmidx.intersection(ps.DataFrame({"A": [1, 2, 3], "B": [4, 5, 6]}))
        # other = list of tuple
        with self.assertRaisesRegex(ValueError, "Names should be list-like for a MultiIndex"):
            psidx.intersection([(1, 2), (3, 4)])

    def test_item(self):
        pidx = pd.Index([10])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.item(), psidx.item())

        # with timestamp
        pidx = pd.Index([datetime(1990, 3, 9)])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.item(), psidx.item())

        # MultiIndex
        pmidx = pd.MultiIndex.from_tuples([("a", "x")])
        psmidx = ps.from_pandas(pmidx)

        self.assert_eq(pmidx.item(), psmidx.item())

        # MultiIndex with timestamp
        pmidx = pd.MultiIndex.from_tuples([(datetime(1990, 3, 9), datetime(2019, 8, 15))])
        psmidx = ps.from_pandas(pmidx)

        self.assert_eq(pidx.item(), psidx.item())

        err_msg = "can only convert an array of size 1 to a Python scalar"
        with self.assertRaisesRegex(ValueError, err_msg):
            ps.Index([10, 20]).item()
        with self.assertRaisesRegex(ValueError, err_msg):
            ps.MultiIndex.from_tuples([("a", "x"), ("b", "y")]).item()

    def test_inferred_type(self):
        # Integer
        pidx = pd.Index([1, 2, 3])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.inferred_type, psidx.inferred_type)

        # Floating
        pidx = pd.Index([1.0, 2.0, 3.0])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.inferred_type, psidx.inferred_type)

        # String
        pidx = pd.Index(["a", "b", "c"])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.inferred_type, psidx.inferred_type)

        # Boolean
        pidx = pd.Index([True, False, True, False])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.inferred_type, psidx.inferred_type)

        # MultiIndex
        pmidx = pd.MultiIndex.from_tuples([("a", "x")])
        psmidx = ps.from_pandas(pmidx)
        self.assert_eq(pmidx.inferred_type, psmidx.inferred_type)

    def test_multi_index_from_index(self):
        tuples = [(1, "red"), (1, "blue"), (2, "red"), (2, "blue")]
        pmidx = pd.Index(tuples)
        psmidx = ps.Index(tuples)

        self.assertTrue(isinstance(psmidx, ps.MultiIndex))
        self.assert_eq(pmidx, psmidx)

        # Specify the `names`
        pmidx = pd.Index(tuples, names=["Hello", "Koalas"])
        psmidx = ps.Index(tuples, names=["Hello", "Koalas"])

        self.assertTrue(isinstance(psmidx, ps.MultiIndex))
        self.assert_eq(pmidx, psmidx)

    def test_multiindex_from_frame(self):
        pdf = pd.DataFrame(
            [["HI", "Temp"], ["HI", "Precip"], ["NJ", "Temp"], ["NJ", "Precip"]], columns=["a", "b"]
        )
        psdf = ps.from_pandas(pdf)
        pidx = pd.MultiIndex.from_frame(pdf)
        psidx = ps.MultiIndex.from_frame(psdf)

        self.assert_eq(pidx, psidx)

        # Specify `names`
        pidx = pd.MultiIndex.from_frame(pdf, names=["state", "observation"])
        psidx = ps.MultiIndex.from_frame(psdf, names=["state", "observation"])
        self.assert_eq(pidx, psidx)

        pidx = pd.MultiIndex.from_frame(pdf, names=("state", "observation"))
        psidx = ps.MultiIndex.from_frame(psdf, names=("state", "observation"))
        self.assert_eq(pidx, psidx)

        # MultiIndex columns
        pidx = pd.MultiIndex.from_tuples([("a", "w"), ("b", "x")])
        pdf.columns = pidx
        psdf = ps.from_pandas(pdf)

        pidx = pd.MultiIndex.from_frame(pdf)
        psidx = ps.MultiIndex.from_frame(psdf)

        self.assert_eq(pidx, psidx)

        # tuples for names
        pidx = pd.MultiIndex.from_frame(pdf, names=[("a", "w"), ("b", "x")])
        psidx = ps.MultiIndex.from_frame(psdf, names=[("a", "w"), ("b", "x")])

        self.assert_eq(pidx, psidx)

        err_msg = "Input must be a DataFrame"
        with self.assertRaisesRegex(TypeError, err_msg):
            ps.MultiIndex.from_frame({"a": [1, 2, 3], "b": [4, 5, 6]})

        self.assertRaises(TypeError, lambda: ps.MultiIndex.from_frame(psdf, names="ab"))

        # non-string names
        self.assert_eq(
            ps.MultiIndex.from_frame(psdf, names=[0, 1]),
            pd.MultiIndex.from_frame(pdf, names=[0, 1]),
        )
        self.assert_eq(
            ps.MultiIndex.from_frame(psdf, names=[("x", 0), ("y", 1)]),
            pd.MultiIndex.from_frame(pdf, names=[("x", 0), ("y", 1)]),
        )

        pdf = pd.DataFrame([["HI", "Temp"], ["HI", "Precip"], ["NJ", "Temp"], ["NJ", "Precip"]])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(ps.MultiIndex.from_frame(psdf), pd.MultiIndex.from_frame(pdf))

    def test_is_type_compatible(self):
        data_types = ["integer", "floating", "string", "boolean"]
        # Integer
        pidx = pd.Index([1, 2, 3])
        psidx = ps.from_pandas(pidx)
        for data_type in data_types:
            self.assert_eq(pidx.is_type_compatible(data_type), psidx.is_type_compatible(data_type))

        # Floating
        pidx = pd.Index([1.0, 2.0, 3.0])
        psidx = ps.from_pandas(pidx)
        for data_type in data_types:
            self.assert_eq(pidx.is_type_compatible(data_type), psidx.is_type_compatible(data_type))

        # String
        pidx = pd.Index(["a", "b", "c"])
        psidx = ps.from_pandas(pidx)
        for data_type in data_types:
            self.assert_eq(pidx.is_type_compatible(data_type), psidx.is_type_compatible(data_type))

        # Boolean
        pidx = pd.Index([True, False, True, False])
        psidx = ps.from_pandas(pidx)
        for data_type in data_types:
            self.assert_eq(pidx.is_type_compatible(data_type), psidx.is_type_compatible(data_type))

        # MultiIndex
        pmidx = pd.MultiIndex.from_tuples([("a", "x")])
        psmidx = ps.from_pandas(pmidx)
        for data_type in data_types:
            self.assert_eq(
                pmidx.is_type_compatible(data_type), psmidx.is_type_compatible(data_type)
            )

    def test_asi8(self):
        # Integer
        pidx = pd.Index([1, 2, 3])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.asi8, psidx.asi8)
        self.assert_eq(pidx.astype("int").asi8, psidx.astype("int").asi8)
        self.assert_eq(pidx.astype("int16").asi8, psidx.astype("int16").asi8)
        self.assert_eq(pidx.astype("int8").asi8, psidx.astype("int8").asi8)

        # Integer with missing value
        pidx = pd.Index([1, 2, None, 4, 5])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.asi8, psidx.asi8)

        # Datetime
        pidx = pd.date_range(end="1/1/2018", periods=3)
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.asi8, psidx.asi8)

        # Floating
        pidx = pd.Index([1.0, 2.0, 3.0])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.asi8, psidx.asi8)

        # String
        pidx = pd.Index(["a", "b", "c"])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.asi8, psidx.asi8)

        # Boolean
        pidx = pd.Index([True, False, True, False])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.asi8, psidx.asi8)

        # MultiIndex
        pmidx = pd.MultiIndex.from_tuples([(1, 2)])
        psmidx = ps.from_pandas(pmidx)
        self.assert_eq(pmidx.asi8, psmidx.asi8)

    def test_index_is_unique(self):
        indexes = [("a", "b", "c"), ("a", "a", "c"), (1, 3, 3), (1, 2, 3)]
        names = [None, "ks", "ks", None]
        is_uniq = [True, False, False, True]

        for idx, name, expected in zip(indexes, names, is_uniq):
            pdf = pd.DataFrame({"a": [1, 2, 3]}, index=pd.Index(idx, name=name))
            psdf = ps.from_pandas(pdf)

            self.assertEqual(psdf.index.is_unique, expected)

    def test_multiindex_is_unique(self):
        indexes = [
            [list("abc"), list("edf")],
            [list("aac"), list("edf")],
            [list("aac"), list("eef")],
            [[1, 4, 4], [4, 6, 6]],
        ]
        is_uniq = [True, True, False, False]

        for idx, expected in zip(indexes, is_uniq):
            pdf = pd.DataFrame({"a": [1, 2, 3]}, index=idx)
            psdf = ps.from_pandas(pdf)

            self.assertEqual(psdf.index.is_unique, expected)

    def test_view(self):
        pidx = pd.Index([1, 2, 3, 4], name="Koalas")
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.view(), psidx.view())

        # MultiIndex
        pmidx = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        psmidx = ps.from_pandas(pmidx)

        self.assert_eq(pmidx.view(), psmidx.view())

    def test_insert(self):
        # Integer
        pidx = pd.Index([1, 2, 3], name="Koalas")
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.insert(1, 100), psidx.insert(1, 100))
        self.assert_eq(pidx.insert(-1, 100), psidx.insert(-1, 100))
        err_msg = "index 100 is out of bounds for axis 0 with size 3"
        with self.assertRaisesRegex(IndexError, err_msg):
            psidx.insert(100, 100)
        err_msg = "index -100 is out of bounds for axis 0 with size 3"
        with self.assertRaisesRegex(IndexError, err_msg):
            psidx.insert(-100, 100)

        # Floating
        pidx = pd.Index([1.0, 2.0, 3.0], name="Koalas")
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.insert(1, 100.0), psidx.insert(1, 100.0))
        self.assert_eq(pidx.insert(-1, 100.0), psidx.insert(-1, 100.0))
        err_msg = "index 100 is out of bounds for axis 0 with size 3"
        with self.assertRaisesRegex(IndexError, err_msg):
            psidx.insert(100, 100)
        err_msg = "index -100 is out of bounds for axis 0 with size 3"
        with self.assertRaisesRegex(IndexError, err_msg):
            psidx.insert(-100, 100)

        # String
        pidx = pd.Index(["a", "b", "c"], name="Koalas")
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.insert(1, "x"), psidx.insert(1, "x"))
        self.assert_eq(pidx.insert(-1, "x"), psidx.insert(-1, "x"))
        err_msg = "index 100 is out of bounds for axis 0 with size 3"
        with self.assertRaisesRegex(IndexError, err_msg):
            psidx.insert(100, "x")
        err_msg = "index -100 is out of bounds for axis 0 with size 3"
        with self.assertRaisesRegex(IndexError, err_msg):
            psidx.insert(-100, "x")

        # Boolean
        pidx = pd.Index([True, False, True, False], name="Koalas")
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.insert(1, True), psidx.insert(1, True))
        self.assert_eq(pidx.insert(-1, True), psidx.insert(-1, True))
        err_msg = "index 100 is out of bounds for axis 0 with size 4"
        with self.assertRaisesRegex(IndexError, err_msg):
            psidx.insert(100, True)
        err_msg = "index -100 is out of bounds for axis 0 with size 4"
        with self.assertRaisesRegex(IndexError, err_msg):
            psidx.insert(-100, True)

        # MultiIndex
        pmidx = pd.MultiIndex.from_tuples(
            [("a", "x"), ("b", "y"), ("c", "z")], names=["Hello", "Koalas"]
        )
        psmidx = ps.from_pandas(pmidx)
        self.assert_eq(pmidx.insert(2, ("h", "j")), psmidx.insert(2, ("h", "j")))
        self.assert_eq(pmidx.insert(-1, ("h", "j")), psmidx.insert(-1, ("h", "j")))

        err_msg = "index 4 is out of bounds for axis 0 with size 3"
        with self.assertRaisesRegex(IndexError, err_msg):
            psmidx.insert(4, ("b", "y"))

        err_msg = "index -4 is out of bounds for axis 0 with size 3"
        with self.assertRaisesRegex(IndexError, err_msg):
            psmidx.insert(-4, ("b", "y"))

    def test_astype(self):
        pidx = pd.Index([10, 20, 15, 30, 45], name="x")
        psidx = ps.Index(pidx)

        self.assert_eq(psidx.astype(int), pidx.astype(int))
        self.assert_eq(psidx.astype(np.int), pidx.astype(np.int))
        self.assert_eq(psidx.astype(np.int8), pidx.astype(np.int8))
        self.assert_eq(psidx.astype(np.int16), pidx.astype(np.int16))
        self.assert_eq(psidx.astype(np.int32), pidx.astype(np.int32))
        self.assert_eq(psidx.astype(np.int64), pidx.astype(np.int64))
        self.assert_eq(psidx.astype(np.byte), pidx.astype(np.byte))
        self.assert_eq(psidx.astype("int"), pidx.astype("int"))
        self.assert_eq(psidx.astype("int8"), pidx.astype("int8"))
        self.assert_eq(psidx.astype("int16"), pidx.astype("int16"))
        self.assert_eq(psidx.astype("int32"), pidx.astype("int32"))
        self.assert_eq(psidx.astype("int64"), pidx.astype("int64"))
        self.assert_eq(psidx.astype("b"), pidx.astype("b"))
        self.assert_eq(psidx.astype("byte"), pidx.astype("byte"))
        self.assert_eq(psidx.astype("i"), pidx.astype("i"))
        self.assert_eq(psidx.astype("long"), pidx.astype("long"))
        self.assert_eq(psidx.astype("short"), pidx.astype("short"))
        self.assert_eq(psidx.astype(np.float), pidx.astype(np.float))
        self.assert_eq(psidx.astype(np.float32), pidx.astype(np.float32))
        self.assert_eq(psidx.astype(np.float64), pidx.astype(np.float64))
        self.assert_eq(psidx.astype("float"), pidx.astype("float"))
        self.assert_eq(psidx.astype("float32"), pidx.astype("float32"))
        self.assert_eq(psidx.astype("float64"), pidx.astype("float64"))
        self.assert_eq(psidx.astype("double"), pidx.astype("double"))
        self.assert_eq(psidx.astype("f"), pidx.astype("f"))
        self.assert_eq(psidx.astype(bool), pidx.astype(bool))
        self.assert_eq(psidx.astype("bool"), pidx.astype("bool"))
        self.assert_eq(psidx.astype("?"), pidx.astype("?"))
        self.assert_eq(psidx.astype(np.unicode_), pidx.astype(np.unicode_))
        self.assert_eq(psidx.astype("str"), pidx.astype("str"))
        self.assert_eq(psidx.astype("U"), pidx.astype("U"))

        pidx = pd.Index([10, 20, 15, 30, 45, None], name="x")
        psidx = ps.Index(pidx)
        if LooseVersion(pd.__version__) >= LooseVersion("1.3"):
            self.assert_eq(psidx.astype(bool), pidx.astype(bool))
            self.assert_eq(psidx.astype(str), pidx.astype(str))
        else:
            self.assert_eq(
                psidx.astype(bool), ps.Index([True, True, True, True, True, True], name="x")
            )
            self.assert_eq(
                psidx.astype(str),
                ps.Index(["10.0", "20.0", "15.0", "30.0", "45.0", "nan"], name="x"),
            )

        pidx = pd.Index(["hi", "hi ", " ", " \t", "", None], name="x")
        psidx = ps.Index(pidx)

        self.assert_eq(psidx.astype(bool), pidx.astype(bool))
        self.assert_eq(psidx.astype(str), pidx.astype(str))

        pidx = pd.Index([True, False, None], name="x")
        psidx = ps.Index(pidx)

        self.assert_eq(psidx.astype(bool), pidx.astype(bool))

        pidx = pd.Index(["2020-10-27"], name="x")
        psidx = ps.Index(pidx)

        self.assert_eq(psidx.astype("datetime64[ns]"), pidx.astype("datetime64[ns]"))

        with self.assertRaisesRegex(TypeError, "not understood"):
            psidx.astype("int63")

    def test_to_list(self):
        # Index
        pidx = pd.Index([1, 2, 3, 4, 5])
        psidx = ps.from_pandas(pidx)
        # MultiIndex
        tuples = [(1, "red"), (1, "blue"), (2, "red"), (2, "green")]
        pmidx = pd.MultiIndex.from_tuples(tuples)
        psmidx = ps.from_pandas(pmidx)

        self.assert_eq(psidx.tolist(), pidx.tolist())
        self.assert_eq(psmidx.tolist(), pmidx.tolist())

    def test_index_ops(self):
        pidx = pd.Index([1, 2, 3, 4, 5])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(psidx * 100 + psidx * 10 + psidx, pidx * 100 + pidx * 10 + pidx)

        pidx = pd.Index([1, 2, 3, 4, 5], name="a")
        psidx = ps.from_pandas(pidx)

        self.assert_eq(psidx * 100 + psidx * 10 + psidx, pidx * 100 + pidx * 10 + pidx)

        pdf = pd.DataFrame(
            index=pd.MultiIndex.from_tuples([(1, 2), (3, 4), (5, 6)], names=["a", "b"])
        )
        psdf = ps.from_pandas(pdf)

        pidx1 = pdf.index.get_level_values(0)
        pidx2 = pdf.index.get_level_values(1)
        psidx1 = psdf.index.get_level_values(0)
        psidx2 = psdf.index.get_level_values(1)

        self.assert_eq(psidx1 * 10 + psidx2, pidx1 * 10 + pidx2)

    def test_factorize(self):
        pidx = pd.Index(["a", "b", "a", "b"])
        psidx = ps.from_pandas(pidx)
        pcodes, puniques = pidx.factorize(sort=True)
        kcodes, kuniques = psidx.factorize()
        self.assert_eq(pcodes.tolist(), kcodes.to_list())
        self.assert_eq(puniques, kuniques)

        pmidx = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        psmidx = ps.from_pandas(pmidx)

        self.assertRaises(PandasNotImplementedError, lambda: psmidx.factorize())

    def test_map(self):
        pidx = pd.Index([1, 2, 3])
        psidx = ps.from_pandas(pidx)

        # Apply dict
        self.assert_eq(
            pidx.map({1: "one", 2: "two", 3: "three"}),
            psidx.map({1: "one", 2: "two", 3: "three"}),
        )
        self.assert_eq(
            pidx.map({1: "one", 2: "two"}),
            psidx.map({1: "one", 2: "two"}),
        )
        self.assert_eq(
            pidx.map({1: "one", 2: "two"}, na_action="ignore"),
            psidx.map({1: "one", 2: "two"}, na_action="ignore"),
        )
        self.assert_eq(
            pidx.map({1: 10, 2: 20}),
            psidx.map({1: 10, 2: 20}),
        )
        self.assert_eq(
            (pidx + 1).map({1: 10, 2: 20}),
            (psidx + 1).map({1: 10, 2: 20}),
        )

        # Apply lambda
        self.assert_eq(
            pidx.map(lambda id: id + 1),
            psidx.map(lambda id: id + 1),
        )
        self.assert_eq(
            pidx.map(lambda id: id + 1.1),
            psidx.map(lambda id: id + 1.1),
        )
        self.assert_eq(
            pidx.map(lambda id: "{id} + 1".format(id=id)),
            psidx.map(lambda id: "{id} + 1".format(id=id)),
        )
        self.assert_eq(
            (pidx + 1).map(lambda id: "{id} + 1".format(id=id)),
            (psidx + 1).map(lambda id: "{id} + 1".format(id=id)),
        )

        # Apply series
        pser = pd.Series(["one", "two", "three"], index=[1, 2, 3])
        self.assert_eq(
            pidx.map(pser),
            psidx.map(pser),
        )
        pser = pd.Series(["one", "two", "three"])
        self.assert_eq(
            pidx.map(pser),
            psidx.map(pser),
        )
        self.assert_eq(
            pidx.map(pser, na_action="ignore"),
            psidx.map(pser, na_action="ignore"),
        )
        pser = pd.Series([1, 2, 3])
        self.assert_eq(
            pidx.map(pser),
            psidx.map(pser),
        )
        self.assert_eq(
            (pidx + 1).map(pser),
            (psidx + 1).map(pser),
        )

        self.assertRaises(
            TypeError,
            lambda: psidx.map({1: 1, 2: 2.0, 3: "three"}),
        )

    def test_multiindex_equal_levels(self):
        pmidx1 = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        pmidx2 = pd.MultiIndex.from_tuples([("b", "y"), ("a", "x"), ("c", "z")])
        psmidx1 = ps.from_pandas(pmidx1)
        psmidx2 = ps.from_pandas(pmidx2)
        self.assert_eq(pmidx1.equal_levels(pmidx2), psmidx1.equal_levels(psmidx2))

        pmidx2 = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "j")])
        psmidx2 = ps.from_pandas(pmidx2)
        self.assert_eq(pmidx1.equal_levels(pmidx2), psmidx1.equal_levels(psmidx2))

        pmidx2 = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("a", "x")])
        psmidx2 = ps.from_pandas(pmidx2)
        self.assert_eq(pmidx1.equal_levels(pmidx2), psmidx1.equal_levels(psmidx2))

        pmidx2 = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y")])
        psmidx2 = ps.from_pandas(pmidx2)
        self.assert_eq(pmidx1.equal_levels(pmidx2), psmidx1.equal_levels(psmidx2))

        pmidx2 = pd.MultiIndex.from_tuples([("a", "y"), ("b", "x"), ("c", "z")])
        psmidx2 = ps.from_pandas(pmidx2)
        self.assert_eq(pmidx1.equal_levels(pmidx2), psmidx1.equal_levels(psmidx2))

        pmidx1 = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z"), ("a", "y")])
        pmidx2 = pd.MultiIndex.from_tuples([("a", "y"), ("b", "x"), ("c", "z"), ("c", "x")])
        psmidx1 = ps.from_pandas(pmidx1)
        psmidx2 = ps.from_pandas(pmidx2)
        self.assert_eq(pmidx1.equal_levels(pmidx2), psmidx1.equal_levels(psmidx2))

        pmidx1 = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        pmidx2 = pd.MultiIndex.from_tuples([("a", "x", "q"), ("b", "y", "w"), ("c", "z", "e")])
        psmidx1 = ps.from_pandas(pmidx1)
        psmidx2 = ps.from_pandas(pmidx2)
        self.assert_eq(pmidx1.equal_levels(pmidx2), psmidx1.equal_levels(psmidx2))

    def test_to_numpy(self):
        pidx = pd.Index([1, 2, 3, 4])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.to_numpy(copy=True), psidx.to_numpy(copy=True))

    def test_drop_level(self):
        tuples = [(1, "red"), (1, "blue"), (2, "red"), (2, "green")]
        pmidx = pd.MultiIndex.from_tuples(tuples)
        psmidx = ps.from_pandas(pmidx)

        with self.assertRaisesRegex(
            IndexError, "Too many levels: Index has only 2 levels, -3 is not a valid level number"
        ):
            psmidx.droplevel(-3)

    def test_multi_index_nunique(self):
        tuples = [(1, "red"), (1, "blue"), (2, "red"), (2, "green")]
        pmidx = pd.MultiIndex.from_tuples(tuples)
        psmidx = ps.from_pandas(pmidx)

        with self.assertRaisesRegex(NotImplementedError, "nunique is not defined for MultiIndex"):
            psmidx.nunique()


if __name__ == "__main__":
    from pyspark.pandas.tests.indexes.test_base import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
