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
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

import pyspark.pandas as ps
from pyspark.loose_version import LooseVersion
from pyspark.pandas.exceptions import PandasNotImplementedError
from pyspark.pandas.missing.indexes import (
    MissingPandasLikeDatetimeIndex,
    MissingPandasLikeIndex,
    MissingPandasLikeMultiIndex,
    MissingPandasLikeTimedeltaIndex,
)
from pyspark.testing.pandasutils import ComparisonTestBase, TestUtils, SPARK_CONF_ARROW_ENABLED


class IndexesTestsMixin:
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
            self.assert_eq(psdf.index.dtype, pdf.index.dtype)

        self.assert_eq(ps.Index([])._summary(), "Index: 0 entries")
        with self.assertRaisesRegex(ValueError, "The truth value of a Index is ambiguous."):
            bool(ps.Index([1]))
        with self.assertRaisesRegex(TypeError, "Index.name must be a hashable type"):
            ps.Index([1, 2, 3], name=[(1, 2, 3)])
        with self.assertRaisesRegex(TypeError, "Index.name must be a hashable type"):
            ps.Index([1.0, 2.0, 3.0], name=[(1, 2, 3)])

    def test_index_from_series(self):
        pser = pd.Series([1, 2, 3], name="a", index=[10, 20, 30])
        psser = ps.from_pandas(pser)

        self.assert_eq(ps.Index(psser), pd.Index(pser))
        self.assert_eq(ps.Index(psser, dtype="float"), pd.Index(pser, dtype="float"))
        self.assert_eq(ps.Index(psser, name="x"), pd.Index(pser, name="x"))

        self.assert_eq(ps.Index(psser, dtype="int64"), pd.Index(pser, dtype="int64"))
        self.assert_eq(ps.Index(psser, dtype="float64"), pd.Index(pser, dtype="float64"))

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

        self.assert_eq(ps.Index(psidx, dtype="int64"), pd.Index(pidx, dtype="int64"))
        self.assert_eq(ps.Index(psidx, dtype="float64"), pd.Index(pidx, dtype="float64"))

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
        # Specifying `names` when creating Index is no longer supported from pandas 2.0.0.
        if LooseVersion(pd.__version__) >= LooseVersion("2.0.0"):
            pass
        else:
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

        self.assert_eq(expected, sorted(psidx.unique()._to_pandas()))
        self.assert_eq(expected, sorted(psidx.unique(level=0)._to_pandas()))

        expected = [1, 2, 4, 6, 7, 9, 10]
        self.assert_eq(expected, sorted((psidx + 1).unique()._to_pandas()))

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
        # No longer supported from pandas 2.0.0.
        if LooseVersion(pd.__version__) >= LooseVersion("2.0.0"):
            self.assert_eq(
                (psidx1 ^ psidx2).sort_values(),
                ps.Index([1, 5], dtype="int64"),
            )
        else:
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
            midx._to_pandas().symmetric_difference(midx_._to_pandas()),
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
        # Specify the `names` while Index creating is no longer supported from pandas 2.0.0.
        if LooseVersion(pd.__version__) >= LooseVersion("2.0.0"):
            pmidx = pd.Index(tuples)
            pmidx.names = ["Hello", "Koalas"]
            psmidx = ps.Index(tuples)
            psmidx.names = ["Hello", "Koalas"]
        else:
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


class IndexesTests(
    IndexesTestsMixin,
    ComparisonTestBase,
    TestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.indexes.test_base import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
