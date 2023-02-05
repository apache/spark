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
from collections import defaultdict
from distutils.version import LooseVersion
import inspect
from itertools import product
from datetime import datetime, timedelta
from decimal import Decimal

import numpy as np
import pandas as pd
from pyspark.ml.linalg import SparseVector

from pyspark import pandas as ps
from pyspark.testing.pandasutils import (
    have_tabulate,
    PandasOnSparkTestCase,
    SPARK_CONF_ARROW_ENABLED,
    tabulate_requirement_message,
)
from pyspark.testing.sqlutils import SQLTestUtils
from pyspark.pandas.exceptions import PandasNotImplementedError
from pyspark.pandas.missing.series import MissingPandasLikeSeries
from pyspark.pandas.typedef.typehints import (
    extension_dtypes,
    extension_dtypes_available,
    extension_float_dtypes_available,
    extension_object_dtypes_available,
)


class SeriesTest(PandasOnSparkTestCase, SQLTestUtils):
    @property
    def pser(self):
        return pd.Series([1, 2, 3, 4, 5, 6, 7], name="x")

    @property
    def psser(self):
        return ps.from_pandas(self.pser)

    def test_creation_index(self):
        err_msg = (
            "The given index cannot be a pandas-on-Spark index. Try pandas index or array-like."
        )
        with self.assertRaisesRegex(TypeError, err_msg):
            ps.Series([1, 2], index=ps.Index([1, 2]))

        with self.assertRaisesRegex(TypeError, err_msg):
            ps.Series([1, 2], index=ps.MultiIndex.from_tuples([(1, 3), (2, 4)]))

    def test_series_ops(self):
        pser = self.pser
        psser = self.psser

        self.assert_eq(psser + 1 + 10 * psser, pser + 1 + 10 * pser)
        self.assert_eq(psser + 1 + 10 * psser.index, pser + 1 + 10 * pser.index)
        self.assert_eq(psser.index + 1 + 10 * psser, pser.index + 1 + 10 * pser)

    def test_series_tuple_name(self):
        pser = self.pser
        pser.name = ("x", "a")

        psser = ps.from_pandas(pser)

        self.assert_eq(psser, pser)
        self.assert_eq(psser.name, pser.name)

        pser.name = ("y", "z")
        psser.name = ("y", "z")

        self.assert_eq(psser, pser)
        self.assert_eq(psser.name, pser.name)

    def test_repr_cache_invalidation(self):
        # If there is any cache, inplace operations should invalidate it.
        s = ps.range(10)["id"]
        s.__repr__()
        s.rename("a", inplace=True)
        self.assertEqual(s.__repr__(), s.rename("a").__repr__())

    def _check_extension(self, psser, pser):
        if LooseVersion("1.1") <= LooseVersion(pd.__version__) < LooseVersion("1.2.2"):
            self.assert_eq(psser, pser, check_exact=False)
            self.assertTrue(isinstance(psser.dtype, extension_dtypes))
        else:
            self.assert_eq(psser, pser)

    def test_empty_series(self):
        pser_a = pd.Series([], dtype="i1")
        pser_b = pd.Series([], dtype="str")

        self.assert_eq(ps.from_pandas(pser_a), pser_a)

        psser_b = ps.from_pandas(pser_b)
        self.assert_eq(psser_b, pser_b)

        with self.sql_conf({SPARK_CONF_ARROW_ENABLED: False}):
            self.assert_eq(ps.from_pandas(pser_a), pser_a)
            self.assert_eq(ps.from_pandas(pser_b), pser_b)

    def test_all_null_series(self):
        pser_a = pd.Series([None, None, None], dtype="float64")
        pser_b = pd.Series([None, None, None], dtype="str")

        self.assert_eq(ps.from_pandas(pser_a), pser_a)

        psser_b = ps.from_pandas(pser_b)
        self.assert_eq(psser_b, pser_b)

        with self.sql_conf({SPARK_CONF_ARROW_ENABLED: False}):
            self.assert_eq(ps.from_pandas(pser_a), pser_a)
            self.assert_eq(ps.from_pandas(pser_b), pser_b)

    def test_head(self):
        psser = self.psser
        pser = self.pser

        self.assert_eq(psser.head(3), pser.head(3))
        self.assert_eq(psser.head(0), pser.head(0))
        self.assert_eq(psser.head(-3), pser.head(-3))
        self.assert_eq(psser.head(-10), pser.head(-10))

    def test_last(self):
        with self.assertRaises(TypeError):
            self.psser.last("1D")

        index = pd.date_range("2018-04-09", periods=4, freq="2D")
        pser = pd.Series([1, 2, 3, 4], index=index)
        psser = ps.from_pandas(pser)
        self.assert_eq(psser.last("1D"), pser.last("1D"))

    def test_first(self):
        with self.assertRaises(TypeError):
            self.psser.first("1D")

        index = pd.date_range("2018-04-09", periods=4, freq="2D")
        pser = pd.Series([1, 2, 3, 4], index=index)
        psser = ps.from_pandas(pser)
        self.assert_eq(psser.first("1D"), pser.first("1D"))

    def test_rename(self):
        pser = pd.Series([1, 2, 3, 4, 5, 6, 7], name="x")
        psser = ps.from_pandas(pser)

        pser.name = "renamed"
        psser.name = "renamed"
        self.assertEqual(psser.name, "renamed")
        self.assert_eq(psser, pser)

        pser.name = None
        psser.name = None
        self.assertEqual(psser.name, None)
        self.assert_eq(psser, pser)

        pidx = pser.index
        psidx = psser.index
        pidx.name = "renamed"
        psidx.name = "renamed"
        self.assertEqual(psidx.name, "renamed")
        self.assert_eq(psidx, pidx)

        expected_error_message = "Series.name must be a hashable type"
        with self.assertRaisesRegex(TypeError, expected_error_message):
            psser.name = ["renamed"]
        with self.assertRaisesRegex(TypeError, expected_error_message):
            psser.name = ["0", "1"]
        with self.assertRaisesRegex(TypeError, expected_error_message):
            ps.Series([1, 2, 3], name=["0", "1"])

    def test_rename_method(self):
        # Series name
        pdf = pd.DataFrame({"x": [1, 2, 3, 4, 5, 6, 7]})
        psdf = ps.from_pandas(pdf)
        pser, psser = pdf.x, psdf.x

        self.assert_eq(psser.rename("y"), pser.rename("y"))
        self.assertEqual(psser.name, "x")  # no mutation
        self.assert_eq(psser.rename(), pser.rename())

        self.assert_eq((psser.rename("y") + 1).head(), (pser.rename("y") + 1).head())

        psser.rename("z", inplace=True)
        pser.rename("z", inplace=True)
        self.assertEqual(psser.name, "z")
        self.assert_eq(psser, pser)
        self.assert_eq(psdf, pdf)

        expected_error_message = "Series.name must be a hashable type"
        with self.assertRaisesRegex(TypeError, expected_error_message):
            psser.rename(["0", "1"])

        # Function index
        self.assert_eq(psser.rename(lambda x: x**2), pser.rename(lambda x: x**2))
        self.assert_eq((psser + 1).rename(lambda x: x**2), (pser + 1).rename(lambda x: x**2))

        expected_error_message = "inplace True is not supported yet for a function 'index'"
        with self.assertRaisesRegex(ValueError, expected_error_message):
            psser.rename(lambda x: x**2, inplace=True)

        unsupported_index_inputs = (pd.Series([2, 3, 4, 5, 6, 7, 8]), {0: "zero", 1: "one"})
        for index in unsupported_index_inputs:
            expected_error_message = (
                "'index' of %s type is not supported yet" % type(index).__name__
            )
            with self.assertRaisesRegex(ValueError, expected_error_message):
                psser.rename(index)

        # Series index
        # pser = pd.Series(['a', 'b', 'c', 'd', 'e', 'f', 'g'], name='x')
        # psser = ps.from_pandas(s)

        # res = psser.rename(pser)
        # self.assert_eq(res, pser.rename(pser))

        # res = psser.rename(psser)
        # self.assert_eq(res, pser.rename(pser))

        # res = psser.rename(lambda x: x**2, inplace=True)
        # self.assertis(res, psser)
        # s.rename(lambda x: x**2, inplace=True)
        # self.assert_eq(psser, pser)

    def test_rename_axis(self):
        index = pd.Index(["A", "B", "C"], name="index")
        pdf = pd.DataFrame({"x": [1.0, 2.0, 3.0]}, index=index)
        psdf = ps.from_pandas(pdf)
        pser, psser = pdf.x, psdf.x

        self.assert_eq(
            pser.rename_axis("index2").sort_index(),
            psser.rename_axis("index2").sort_index(),
        )

        self.assert_eq(
            (pser + 1).rename_axis("index2").sort_index(),
            (psser + 1).rename_axis("index2").sort_index(),
        )

        self.assertRaises(ValueError, lambda: psser.rename_axis(["index2", "index3"]))
        self.assertRaises(TypeError, lambda: psser.rename_axis(mapper=["index2"], index=["index3"]))

        self.assert_eq(
            pser.rename_axis(index={"index": "index2", "missing": "index4"}).sort_index(),
            psser.rename_axis(index={"index": "index2", "missing": "index4"}).sort_index(),
        )

        self.assert_eq(
            pser.rename_axis(index=str.upper).sort_index(),
            psser.rename_axis(index=str.upper).sort_index(),
        )

        pser.rename_axis("index2", inplace=True)
        psser.rename_axis("index2", inplace=True)
        self.assert_eq(pser.sort_index(), psser.sort_index())
        # Note: in pandas, pdf.x's index is renamed, whereas pdf's index isn't due to a bug.
        self.assert_eq(pdf, psdf)

        index = pd.MultiIndex.from_tuples(
            [("A", "B"), ("C", "D"), ("E", "F")], names=["index1", "index2"]
        )
        pser = pd.Series([1.0, 2.0, 3.0], index=index, name="name")
        psser = ps.from_pandas(pser)

        self.assert_eq(
            pser.rename_axis(["index3", "index4"]).sort_index(),
            psser.rename_axis(["index3", "index4"]).sort_index(),
        )

        self.assertRaises(ValueError, lambda: psser.rename_axis(["index3", "index4", "index5"]))

        self.assert_eq(
            pser.rename_axis(
                index={"index1": "index3", "index2": "index4", "missing": "index5"}
            ).sort_index(),
            psser.rename_axis(
                index={"index1": "index3", "index2": "index4", "missing": "index5"}
            ).sort_index(),
        )

        self.assert_eq(
            pser.rename_axis(index=str.upper).sort_index(),
            psser.rename_axis(index=str.upper).sort_index(),
        )

    def test_or(self):
        pdf = pd.DataFrame(
            {
                "left": [True, False, True, False, np.nan, np.nan, True, False, np.nan],
                "right": [True, False, False, True, True, False, np.nan, np.nan, np.nan],
            }
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf["left"] | psdf["right"], pdf["left"] | pdf["right"])
        self.assert_eq(psdf["left"] | True, pdf["left"] | True)
        self.assert_eq(psdf["left"] | False, pdf["left"] | False)
        self.assert_eq(psdf["left"] | None, pdf["left"] | None)
        self.assert_eq(True | psdf["right"], True | pdf["right"])
        self.assert_eq(False | psdf["right"], False | pdf["right"])
        self.assert_eq(None | psdf["right"], None | pdf["right"])

    @unittest.skipIf(
        not extension_object_dtypes_available, "pandas extension object dtypes are not available"
    )
    def test_or_extenstion_dtypes(self):
        pdf = pd.DataFrame(
            {
                "left": [True, False, True, False, np.nan, np.nan, True, False, np.nan],
                "right": [True, False, False, True, True, False, np.nan, np.nan, np.nan],
            }
        ).astype("boolean")
        psdf = ps.from_pandas(pdf)

        self._check_extension(psdf["left"] | psdf["right"], pdf["left"] | pdf["right"])
        self._check_extension(psdf["left"] | True, pdf["left"] | True)
        self._check_extension(psdf["left"] | False, pdf["left"] | False)
        self._check_extension(psdf["left"] | pd.NA, pdf["left"] | pd.NA)
        self._check_extension(True | psdf["right"], True | pdf["right"])
        self._check_extension(False | psdf["right"], False | pdf["right"])
        self._check_extension(pd.NA | psdf["right"], pd.NA | pdf["right"])

    def test_and(self):
        pdf = pd.DataFrame(
            {
                "left": [True, False, True, False, np.nan, np.nan, True, False, np.nan],
                "right": [True, False, False, True, True, False, np.nan, np.nan, np.nan],
            }
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf["left"] & psdf["right"], pdf["left"] & pdf["right"])
        self.assert_eq(psdf["left"] & True, pdf["left"] & True)
        self.assert_eq(psdf["left"] & False, pdf["left"] & False)
        self.assert_eq(psdf["left"] & None, pdf["left"] & None)
        self.assert_eq(True & psdf["right"], True & pdf["right"])
        self.assert_eq(False & psdf["right"], False & pdf["right"])
        self.assert_eq(None & psdf["right"], None & pdf["right"])

    @unittest.skipIf(
        not extension_object_dtypes_available, "pandas extension object dtypes are not available"
    )
    def test_and_extenstion_dtypes(self):
        pdf = pd.DataFrame(
            {
                "left": [True, False, True, False, np.nan, np.nan, True, False, np.nan],
                "right": [True, False, False, True, True, False, np.nan, np.nan, np.nan],
            }
        ).astype("boolean")
        psdf = ps.from_pandas(pdf)

        self._check_extension(psdf["left"] & psdf["right"], pdf["left"] & pdf["right"])
        self._check_extension(psdf["left"] & True, pdf["left"] & True)
        self._check_extension(psdf["left"] & False, pdf["left"] & False)
        self._check_extension(psdf["left"] & pd.NA, pdf["left"] & pd.NA)
        self._check_extension(True & psdf["right"], True & pdf["right"])
        self._check_extension(False & psdf["right"], False & pdf["right"])
        self._check_extension(pd.NA & psdf["right"], pd.NA & pdf["right"])

    def test_to_numpy(self):
        pser = pd.Series([1, 2, 3, 4, 5, 6, 7], name="x")

        psser = ps.from_pandas(pser)
        self.assert_eq(psser.to_numpy(), pser.values)

    def test_isin(self):
        pser = pd.Series(["lama", "cow", "lama", "beetle", "lama", "hippo"], name="animal")

        psser = ps.from_pandas(pser)

        self.assert_eq(psser.isin(["cow", "lama"]), pser.isin(["cow", "lama"]))
        self.assert_eq(psser.isin(np.array(["cow", "lama"])), pser.isin(np.array(["cow", "lama"])))
        self.assert_eq(psser.isin({"cow"}), pser.isin({"cow"}))

        pser = pd.Series([np.int64(1), np.int32(1), 1])
        psser = ps.from_pandas(pser)
        self.assert_eq(psser.isin([np.int64(1)]), pser.isin([np.int64(1)]))

        msg = "only list-like objects are allowed to be passed to isin()"
        with self.assertRaisesRegex(TypeError, msg):
            psser.isin(1)

        # when Series have NaN
        pser = pd.Series(["lama", "cow", None, "lama", "beetle", "lama", "hippo", None], name="a")
        psser = ps.from_pandas(pser)

        self.assert_eq(psser.isin(["cow", "lama"]), pser.isin(["cow", "lama"]))

        pser = pd.Series([None, 5, None, 3, 2, 1, None, 0, 0], name="a")
        psser = ps.from_pandas(pser)

        if LooseVersion(pd.__version__) >= LooseVersion("1.2"):
            self.assert_eq(psser.isin([1, 5, 0, None]), pser.isin([1, 5, 0, None]))
        else:
            expected = pd.Series(
                [False, True, False, False, False, True, False, True, True], name="a"
            )
            self.assert_eq(psser.isin([1, 5, 0, None]), expected)

    def test_duplicated(self):
        for pser in [
            pd.Series(["beetle", None, "beetle", None, "lama", "beetle"], name="objects"),
            pd.Series([1, np.nan, 1, np.nan], name="numbers"),
            pd.Series(
                [
                    pd.Timestamp("2022-01-01"),
                    pd.Timestamp("2022-02-02"),
                    pd.Timestamp("2022-01-01"),
                    pd.Timestamp("2022-02-02"),
                ],
                name="times",
            ),
        ]:
            psser = ps.from_pandas(pser)
            self.assert_eq(psser.duplicated().sort_index(), pser.duplicated())
            self.assert_eq(
                psser.duplicated(keep="first").sort_index(), pser.duplicated(keep="first")
            )
            self.assert_eq(psser.duplicated(keep="last").sort_index(), pser.duplicated(keep="last"))
            self.assert_eq(psser.duplicated(keep=False).sort_index(), pser.duplicated(keep=False))

        pser = pd.Series([1, 2, 1, 2, 3], name="numbers")
        psser = ps.from_pandas(pser)
        self.assert_eq((psser + 1).duplicated().sort_index(), (pser + 1).duplicated())

    def test_drop_duplicates(self):
        pdf = pd.DataFrame({"animal": ["lama", "cow", "lama", "beetle", "lama", "hippo"]})
        psdf = ps.from_pandas(pdf)

        pser = pdf.animal
        psser = psdf.animal

        self.assert_eq(psser.drop_duplicates().sort_index(), pser.drop_duplicates().sort_index())
        self.assert_eq(
            psser.drop_duplicates(keep="last").sort_index(),
            pser.drop_duplicates(keep="last").sort_index(),
        )

        # inplace
        psser.drop_duplicates(keep=False, inplace=True)
        pser.drop_duplicates(keep=False, inplace=True)
        self.assert_eq(psser.sort_index(), pser.sort_index())
        self.assert_eq(psdf, pdf)

    def test_reindex(self):
        index = ["A", "B", "C", "D", "E"]
        pser = pd.Series([1.0, 2.0, 3.0, 4.0, None], index=index, name="x")
        psser = ps.from_pandas(pser)

        self.assert_eq(pser, psser)

        self.assert_eq(
            pser.reindex(["A", "B"]).sort_index(),
            psser.reindex(["A", "B"]).sort_index(),
        )

        self.assert_eq(
            pser.reindex(["A", "B", "2", "3"]).sort_index(),
            psser.reindex(["A", "B", "2", "3"]).sort_index(),
        )

        self.assert_eq(
            pser.reindex(["A", "E", "2"], fill_value=0).sort_index(),
            psser.reindex(["A", "E", "2"], fill_value=0).sort_index(),
        )

        self.assertRaises(TypeError, lambda: psser.reindex(index=123))

    def test_reindex_like(self):
        data = [1.0, 2.0, None]
        index = pd.Index(["A", "B", "C"], name="index1")
        pser = pd.Series(data=data, index=index, name="name1")
        psser = ps.from_pandas(pser)

        # Reindexing single Index on single Index
        data2 = [3.0, None, 4.0]
        index2 = pd.Index(["A", "C", "D"], name="index2")
        pser2 = pd.Series(data=data2, index=index2, name="name2")
        psser2 = ps.from_pandas(pser2)

        self.assert_eq(
            pser.reindex_like(pser2).sort_index(),
            psser.reindex_like(psser2).sort_index(),
        )

        self.assert_eq(
            (pser + 1).reindex_like(pser2).sort_index(),
            (psser + 1).reindex_like(psser2).sort_index(),
        )

        # Reindexing MultiIndex on single Index
        index2 = pd.MultiIndex.from_tuples(
            [("A", "G"), ("C", "D"), ("I", "J")], names=["index3", "index4"]
        )
        pser2 = pd.Series(data=data2, index=index2, name="name2")
        psser2 = ps.from_pandas(pser2)

        self.assert_eq(
            pser.reindex_like(pser2).sort_index(),
            psser.reindex_like(psser2).sort_index(),
        )

        self.assertRaises(TypeError, lambda: psser.reindex_like(index2))
        self.assertRaises(AssertionError, lambda: psser2.reindex_like(psser))

        # Reindexing MultiIndex on MultiIndex
        index = pd.MultiIndex.from_tuples(
            [("A", "B"), ("C", "D"), ("E", "F")], names=["index1", "index2"]
        )
        pser = pd.Series(data=data, index=index, name="name1")
        psser = ps.from_pandas(pser)

        self.assert_eq(
            pser.reindex_like(pser2).sort_index(),
            psser.reindex_like(psser2).sort_index(),
        )

        # Reindexing with DataFrame
        index2 = pd.MultiIndex.from_tuples(
            [("A", "B"), ("C", "D"), ("E", "F")], names=["name3", "name4"]
        )
        pdf = pd.DataFrame(data=data, index=index2)
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            pser.reindex_like(pdf).sort_index(),
            psser.reindex_like(psdf).sort_index(),
        )

    def test_fillna(self):
        pdf = pd.DataFrame({"x": [np.nan, 2, 3, 4, np.nan, 6], "y": [np.nan, 2, 3, 4, np.nan, 6]})
        psdf = ps.from_pandas(pdf)

        pser = pdf.x
        psser = psdf.x

        self.assert_eq(psser.fillna(0), pser.fillna(0))
        self.assert_eq(psser.fillna(np.nan).fillna(0), pser.fillna(np.nan).fillna(0))

        psser.fillna(0, inplace=True)
        pser.fillna(0, inplace=True)
        self.assert_eq(psser, pser)
        self.assert_eq(psdf, pdf)

        # test considering series does not have NA/NaN values
        psser.fillna(0, inplace=True)
        pser.fillna(0, inplace=True)
        self.assert_eq(psser, pser)
        self.assert_eq(psdf, pdf)

        psser = psdf.x.rename("y")
        pser = pdf.x.rename("y")
        psser.fillna(0, inplace=True)
        pser.fillna(0, inplace=True)
        self.assert_eq(psser.head(), pser.head())
        self.assert_eq(psdf, pdf)

        pser = pd.Series([1, 2, 3, 4, 5, 6], name="x")
        psser = ps.from_pandas(pser)

        pser.loc[3] = np.nan
        psser.loc[3] = np.nan

        self.assert_eq(psser.fillna(0), pser.fillna(0))
        self.assert_eq(psser.fillna(method="ffill"), pser.fillna(method="ffill"))
        self.assert_eq(psser.fillna(method="bfill"), pser.fillna(method="bfill"))
        self.assert_eq(psser.fillna(method="backfill"), pser.fillna(method="backfill"))

        # inplace fillna on non-nullable column
        pdf = pd.DataFrame({"a": [1, 2, None], "b": [1, 2, 3]})
        psdf = ps.from_pandas(pdf)

        pser = pdf.b
        psser = psdf.b

        self.assert_eq(psser.fillna(0), pser.fillna(0))
        self.assert_eq(psser.fillna(np.nan).fillna(0), pser.fillna(np.nan).fillna(0))

        psser.fillna(0, inplace=True)
        pser.fillna(0, inplace=True)
        self.assert_eq(psser, pser)
        self.assert_eq(psdf, pdf)

        with self.assertRaisesRegex(
            ValueError, "Must specify a fillna 'value' or 'method' parameter."
        ):
            psser.fillna()
        with self.assertRaisesRegex(TypeError, "Unsupported type list"):
            psdf.a.fillna([0])
        with self.assertRaisesRegex(
            NotImplementedError, "fillna currently only works for axis=0 or axis='index'"
        ):
            psdf.a.fillna(0, axis=1)
        with self.assertRaisesRegex(
            NotImplementedError, "limit parameter for value is not support now"
        ):
            psdf.a.fillna(0, limit=1)

    def test_dropna(self):
        pdf = pd.DataFrame({"x": [np.nan, 2, 3, 4, np.nan, 6]})
        psdf = ps.from_pandas(pdf)

        pser = pdf.x
        psser = psdf.x

        self.assert_eq(psser.dropna(), pser.dropna())

        pser.dropna(inplace=True)
        psser.dropna(inplace=True)
        self.assert_eq(psser, pser)
        self.assert_eq(psdf, pdf)

    def test_nunique(self):
        pser = pd.Series([1, 2, 1, np.nan])
        psser = ps.from_pandas(pser)

        # Assert NaNs are dropped by default
        nunique_result = psser.nunique()
        self.assertEqual(nunique_result, 2)
        self.assert_eq(nunique_result, pser.nunique())

        # Assert including NaN values
        nunique_result = psser.nunique(dropna=False)
        self.assertEqual(nunique_result, 3)
        self.assert_eq(nunique_result, pser.nunique(dropna=False))

        # Assert approximate counts
        self.assertEqual(ps.Series(range(100)).nunique(approx=True), 103)
        self.assertEqual(ps.Series(range(100)).nunique(approx=True, rsd=0.01), 100)

    def test_value_counts(self):
        # this is also containing test for Index & MultiIndex
        pser = pd.Series(
            [1, 2, 1, 3, 3, np.nan, 1, 4, 2, np.nan, 3, np.nan, 3, 1, 3],
            index=[1, 2, 1, 3, 3, np.nan, 1, 4, 2, np.nan, 3, np.nan, 3, 1, 3],
            name="x",
        )
        psser = ps.from_pandas(pser)

        exp = pser.value_counts()
        res = psser.value_counts()
        self.assertEqual(res.name, exp.name)
        self.assert_eq(res, exp)

        self.assert_eq(psser.value_counts(normalize=True), pser.value_counts(normalize=True))
        self.assert_eq(psser.value_counts(ascending=True), pser.value_counts(ascending=True))
        self.assert_eq(
            psser.value_counts(normalize=True, dropna=False),
            pser.value_counts(normalize=True, dropna=False),
        )
        self.assert_eq(
            psser.value_counts(ascending=True, dropna=False),
            pser.value_counts(ascending=True, dropna=False),
        )

        self.assert_eq(
            psser.index.value_counts(normalize=True), pser.index.value_counts(normalize=True)
        )
        self.assert_eq(
            psser.index.value_counts(ascending=True), pser.index.value_counts(ascending=True)
        )
        self.assert_eq(
            psser.index.value_counts(normalize=True, dropna=False),
            pser.index.value_counts(normalize=True, dropna=False),
        )
        self.assert_eq(
            psser.index.value_counts(ascending=True, dropna=False),
            pser.index.value_counts(ascending=True, dropna=False),
        )

        with self.assertRaisesRegex(
            NotImplementedError, "value_counts currently does not support bins"
        ):
            psser.value_counts(bins=3)

        pser.name = "index"
        psser.name = "index"
        self.assert_eq(psser.value_counts(), pser.value_counts())

        # Series from DataFrame
        pdf = pd.DataFrame({"a": [2, 2, 3], "b": [None, 1, None]})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.a.value_counts(normalize=True), pdf.a.value_counts(normalize=True))
        self.assert_eq(psdf.a.value_counts(ascending=True), pdf.a.value_counts(ascending=True))
        self.assert_eq(
            psdf.a.value_counts(normalize=True, dropna=False),
            pdf.a.value_counts(normalize=True, dropna=False),
        )
        self.assert_eq(
            psdf.a.value_counts(ascending=True, dropna=False),
            pdf.a.value_counts(ascending=True, dropna=False),
        )

        self.assert_eq(
            psser.index.value_counts(normalize=True), pser.index.value_counts(normalize=True)
        )
        self.assert_eq(
            psser.index.value_counts(ascending=True), pser.index.value_counts(ascending=True)
        )
        self.assert_eq(
            psser.index.value_counts(normalize=True, dropna=False),
            pser.index.value_counts(normalize=True, dropna=False),
        )
        self.assert_eq(
            psser.index.value_counts(ascending=True, dropna=False),
            pser.index.value_counts(ascending=True, dropna=False),
        )

        # Series with NaN index
        pser = pd.Series([3, 2, 3, 1, 2, 3], index=[2.0, None, 5.0, 5.0, None, 5.0])
        psser = ps.from_pandas(pser)

        self.assert_eq(psser.value_counts(normalize=True), pser.value_counts(normalize=True))
        self.assert_eq(psser.value_counts(ascending=True), pser.value_counts(ascending=True))
        self.assert_eq(
            psser.value_counts(normalize=True, dropna=False),
            pser.value_counts(normalize=True, dropna=False),
        )
        self.assert_eq(
            psser.value_counts(ascending=True, dropna=False),
            pser.value_counts(ascending=True, dropna=False),
        )

        self.assert_eq(
            psser.index.value_counts(normalize=True), pser.index.value_counts(normalize=True)
        )
        self.assert_eq(
            psser.index.value_counts(ascending=True), pser.index.value_counts(ascending=True)
        )
        self.assert_eq(
            psser.index.value_counts(normalize=True, dropna=False),
            pser.index.value_counts(normalize=True, dropna=False),
        )
        self.assert_eq(
            psser.index.value_counts(ascending=True, dropna=False),
            pser.index.value_counts(ascending=True, dropna=False),
        )

        # Series with MultiIndex
        pser.index = pd.MultiIndex.from_tuples(
            [("x", "a"), ("x", "b"), ("y", "c"), ("x", "a"), ("y", "c"), ("x", "a")]
        )
        psser = ps.from_pandas(pser)

        self.assert_eq(psser.value_counts(normalize=True), pser.value_counts(normalize=True))
        self.assert_eq(psser.value_counts(ascending=True), pser.value_counts(ascending=True))
        self.assert_eq(
            psser.value_counts(normalize=True, dropna=False),
            pser.value_counts(normalize=True, dropna=False),
        )
        self.assert_eq(
            psser.value_counts(ascending=True, dropna=False),
            pser.value_counts(ascending=True, dropna=False),
        )

        # FIXME: MultiIndex.value_counts returns wrong indices.
        self.assert_eq(
            psser.index.value_counts(normalize=True),
            pser.index.value_counts(normalize=True),
            almost=True,
        )
        self.assert_eq(
            psser.index.value_counts(ascending=True),
            pser.index.value_counts(ascending=True),
            almost=True,
        )
        self.assert_eq(
            psser.index.value_counts(normalize=True, dropna=False),
            pser.index.value_counts(normalize=True, dropna=False),
            almost=True,
        )
        self.assert_eq(
            psser.index.value_counts(ascending=True, dropna=False),
            pser.index.value_counts(ascending=True, dropna=False),
            almost=True,
        )

        # Series with MultiIndex some of index has NaN
        pser.index = pd.MultiIndex.from_tuples(
            [("x", "a"), ("x", None), ("y", "c"), ("x", "a"), ("y", "c"), ("x", "a")]
        )
        psser = ps.from_pandas(pser)

        self.assert_eq(psser.value_counts(normalize=True), pser.value_counts(normalize=True))
        self.assert_eq(psser.value_counts(ascending=True), pser.value_counts(ascending=True))
        self.assert_eq(
            psser.value_counts(normalize=True, dropna=False),
            pser.value_counts(normalize=True, dropna=False),
        )
        self.assert_eq(
            psser.value_counts(ascending=True, dropna=False),
            pser.value_counts(ascending=True, dropna=False),
        )

        # FIXME: MultiIndex.value_counts returns wrong indices.
        self.assert_eq(
            psser.index.value_counts(normalize=True),
            pser.index.value_counts(normalize=True),
            almost=True,
        )
        self.assert_eq(
            psser.index.value_counts(ascending=True),
            pser.index.value_counts(ascending=True),
            almost=True,
        )
        self.assert_eq(
            psser.index.value_counts(normalize=True, dropna=False),
            pser.index.value_counts(normalize=True, dropna=False),
            almost=True,
        )
        self.assert_eq(
            psser.index.value_counts(ascending=True, dropna=False),
            pser.index.value_counts(ascending=True, dropna=False),
            almost=True,
        )

        # Series with MultiIndex some of index is NaN.
        pser.index = pd.MultiIndex.from_tuples(
            [("x", "a"), None, ("y", "c"), ("x", "a"), ("y", "c"), ("x", "a")]
        )
        psser = ps.from_pandas(pser)

        self.assert_eq(psser.value_counts(normalize=True), pser.value_counts(normalize=True))
        self.assert_eq(psser.value_counts(ascending=True), pser.value_counts(ascending=True))
        self.assert_eq(
            psser.value_counts(normalize=True, dropna=False),
            pser.value_counts(normalize=True, dropna=False),
        )
        self.assert_eq(
            psser.value_counts(ascending=True, dropna=False),
            pser.value_counts(ascending=True, dropna=False),
        )

        # FIXME: MultiIndex.value_counts returns wrong indices.
        self.assert_eq(
            psser.index.value_counts(normalize=True),
            pser.index.value_counts(normalize=True),
            almost=True,
        )
        self.assert_eq(
            psser.index.value_counts(ascending=True),
            pser.index.value_counts(ascending=True),
            almost=True,
        )
        self.assert_eq(
            psser.index.value_counts(normalize=True, dropna=False),
            pser.index.value_counts(normalize=True, dropna=False),
            almost=True,
        )
        self.assert_eq(
            psser.index.value_counts(ascending=True, dropna=False),
            pser.index.value_counts(ascending=True, dropna=False),
            almost=True,
        )

    def test_nsmallest(self):
        sample_lst = [1, 2, 3, 4, np.nan, 6]
        pser = pd.Series(sample_lst, name="x")
        psser = ps.Series(sample_lst, name="x")
        self.assert_eq(psser.nsmallest(n=3), pser.nsmallest(n=3))
        self.assert_eq(psser.nsmallest(), pser.nsmallest())
        self.assert_eq((psser + 1).nsmallest(), (pser + 1).nsmallest())

    def test_nlargest(self):
        sample_lst = [1, 2, 3, 4, np.nan, 6]
        pser = pd.Series(sample_lst, name="x")
        psser = ps.Series(sample_lst, name="x")
        self.assert_eq(psser.nlargest(n=3), pser.nlargest(n=3))
        self.assert_eq(psser.nlargest(), pser.nlargest())
        self.assert_eq((psser + 1).nlargest(), (pser + 1).nlargest())

    def test_notnull(self):
        pser = pd.Series([1, 2, 3, 4, np.nan, 6], name="x")
        psser = ps.from_pandas(pser)

        self.assert_eq(psser.notnull(), pser.notnull())

        pser = self.pser
        psser = self.psser

        self.assert_eq(psser.notnull(), pser.notnull())

    def test_all(self):
        for pser in [
            pd.Series([True, True], name="x"),
            pd.Series([True, False], name="x"),
            pd.Series([0, 1], name="x"),
            pd.Series([1, 2, 3], name="x"),
            pd.Series([np.nan, 0, 1], name="x"),
            pd.Series([np.nan, 1, 2, 3], name="x"),
            pd.Series([True, True, None], name="x"),
            pd.Series([True, False, None], name="x"),
            pd.Series([], name="x"),
            pd.Series([np.nan], name="x"),
            pd.Series([np.nan, np.nan], name="x"),
            pd.Series([None], name="x"),
            pd.Series([None, None], name="x"),
        ]:
            psser = ps.from_pandas(pser)
            self.assert_eq(psser.all(), pser.all())
            self.assert_eq(psser.all(skipna=False), pser.all(skipna=False))
            self.assert_eq(psser.all(skipna=True), pser.all(skipna=True))

        pser = pd.Series([1, 2, 3, 4], name="x")
        psser = ps.from_pandas(pser)

        self.assert_eq((psser % 2 == 0).all(), (pser % 2 == 0).all())

        with self.assertRaisesRegex(
            NotImplementedError, 'axis should be either 0 or "index" currently.'
        ):
            psser.all(axis=1)

    def test_any(self):
        for pser in [
            pd.Series([False, False], name="x"),
            pd.Series([True, False], name="x"),
            pd.Series([0, 1], name="x"),
            pd.Series([1, 2, 3], name="x"),
            pd.Series([True, True, None], name="x"),
            pd.Series([True, False, None], name="x"),
            pd.Series([], name="x"),
            pd.Series([np.nan], name="x"),
        ]:
            psser = ps.from_pandas(pser)
            self.assert_eq(psser.any(), pser.any())

        pser = pd.Series([1, 2, 3, 4], name="x")
        psser = ps.from_pandas(pser)

        self.assert_eq((psser % 2 == 0).any(), (pser % 2 == 0).any())

        with self.assertRaisesRegex(
            NotImplementedError, 'axis should be either 0 or "index" currently.'
        ):
            psser.any(axis=1)

    def test_reset_index(self):
        pdf = pd.DataFrame({"foo": [1, 2, 3, 4]}, index=pd.Index(["a", "b", "c", "d"], name="idx"))
        psdf = ps.from_pandas(pdf)

        pser = pdf.foo
        psser = psdf.foo

        self.assert_eq(psser.reset_index(), pser.reset_index())
        self.assert_eq(psser.reset_index(name="values"), pser.reset_index(name="values"))
        self.assert_eq(psser.reset_index(drop=True), pser.reset_index(drop=True))

        # inplace
        psser.reset_index(drop=True, inplace=True)
        pser.reset_index(drop=True, inplace=True)
        self.assert_eq(psser, pser)
        self.assert_eq(psdf, pdf)

    def test_reset_index_with_default_index_types(self):
        pser = pd.Series([1, 2, 3], name="0", index=np.random.rand(3))
        psser = ps.from_pandas(pser)

        with ps.option_context("compute.default_index_type", "sequence"):
            self.assert_eq(psser.reset_index(), pser.reset_index())

        with ps.option_context("compute.default_index_type", "distributed-sequence"):
            # the order might be changed.
            self.assert_eq(psser.reset_index().sort_index(), pser.reset_index())

        with ps.option_context("compute.default_index_type", "distributed"):
            # the index is different.
            self.assert_eq(
                psser.reset_index()._to_pandas().reset_index(drop=True), pser.reset_index()
            )

    def test_index_to_series_reset_index(self):
        def check(psser, pser):
            self.assert_eq(psser.reset_index(), pser.reset_index())
            self.assert_eq(psser.reset_index(drop=True), pser.reset_index(drop=True))

            pser.reset_index(drop=True, inplace=True)
            psser.reset_index(drop=True, inplace=True)
            self.assert_eq(psser, pser)

        pdf = pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9], "b": [4, 5, 6, 3, 2, 1, 0, 0, 0]},
            index=np.random.rand(9),
        )
        psdf = ps.from_pandas(pdf)
        check(psdf.index.to_series(), pdf.index.to_series())
        check(psdf.index.to_series(name="a"), pdf.index.to_series(name="a"))
        check(psdf.index.to_series(name=("x", "a")), pdf.index.to_series(name=("x", "a")))

    def test_sort_values(self):
        pdf = pd.DataFrame({"x": [1, 2, 3, 4, 5, None, 7]}, index=np.random.rand(7))
        psdf = ps.from_pandas(pdf)

        pser = pdf.x
        psser = psdf.x

        self.assert_eq(psser.sort_values(), pser.sort_values())
        self.assert_eq(psser.sort_values(ignore_index=True), pser.sort_values(ignore_index=True))
        self.assert_eq(psser.sort_values(ascending=False), pser.sort_values(ascending=False))
        self.assert_eq(
            psser.sort_values(na_position="first"), pser.sort_values(na_position="first")
        )

        self.assertRaises(ValueError, lambda: psser.sort_values(na_position="invalid"))

        # inplace
        # pandas raises an exception when the Series is derived from DataFrame
        psser.sort_values(inplace=True)
        self.assert_eq(psser, pser.sort_values())
        self.assert_eq(psdf, pdf)

        # pandas raises an exception when the Series is derived from DataFrame
        psser.sort_values(inplace=True, ascending=False, ignore_index=True)
        self.assert_eq(psser, pser.sort_values(ascending=False, ignore_index=True))
        self.assert_eq(psdf, pdf)

        pser = pdf.x.copy()
        psser = psdf.x.copy()

        psser.sort_values(inplace=True)
        pser.sort_values(inplace=True)
        self.assert_eq(psser, pser)
        self.assert_eq(psdf, pdf)

    def test_sort_index(self):
        pdf = pd.DataFrame({"x": [2, 1, np.nan]}, index=["b", "a", np.nan])
        psdf = ps.from_pandas(pdf)

        pser = pdf.x
        psser = psdf.x

        # Assert invalid parameters
        self.assertRaises(NotImplementedError, lambda: psser.sort_index(axis=1))
        self.assertRaises(NotImplementedError, lambda: psser.sort_index(kind="mergesort"))
        self.assertRaises(ValueError, lambda: psser.sort_index(na_position="invalid"))

        # Assert default behavior without parameters
        self.assert_eq(psser.sort_index(), pser.sort_index())
        # Assert sorting descending
        self.assert_eq(psser.sort_index(ascending=False), pser.sort_index(ascending=False))
        # Assert sorting NA indices first
        self.assert_eq(psser.sort_index(na_position="first"), pser.sort_index(na_position="first"))
        # Assert ignoring index
        self.assert_eq(psser.sort_index(ignore_index=True), pser.sort_index(ignore_index=True))

        # Assert sorting inplace
        # pandas sorts pdf.x by the index and update the column only
        # when the Series is derived from DataFrame.
        psser.sort_index(inplace=True)
        self.assert_eq(psser, pser.sort_index())
        self.assert_eq(psdf, pdf)

        # pandas sorts pdf.x by the index and update the column only
        # when the Series is derived from DataFrame.
        psser.sort_index(inplace=True, ascending=False, ignore_index=True)
        self.assert_eq(psser, pser.sort_index(ascending=False, ignore_index=True))
        self.assert_eq(psdf, pdf)

        pser = pdf.x.copy()
        psser = psdf.x.copy()

        psser.sort_index(inplace=True)
        pser.sort_index(inplace=True)
        self.assert_eq(psser, pser)
        self.assert_eq(psdf, pdf)

        # Assert multi-indices
        pser = pd.Series(range(4), index=[["b", "b", "a", "a"], [1, 0, 1, 0]], name="0")
        psser = ps.from_pandas(pser)
        self.assert_eq(psser.sort_index(), pser.sort_index())
        self.assert_eq(psser.sort_index(level=[1, 0]), pser.sort_index(level=[1, 0]))

        self.assert_eq(psser.reset_index().sort_index(), pser.reset_index().sort_index())

    def test_to_datetime(self):
        pser = pd.Series(["3/11/2000", "3/12/2000", "3/13/2000"] * 100)
        psser = ps.from_pandas(pser)

        self.assert_eq(
            pd.to_datetime(pser, infer_datetime_format=True),
            ps.to_datetime(psser, infer_datetime_format=True),
        )

    def test_missing(self):
        psser = self.psser

        missing_functions = inspect.getmembers(MissingPandasLikeSeries, inspect.isfunction)
        unsupported_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "unsupported_function"
        ]
        for name in unsupported_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "method.*Series.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psser, name)()

        deprecated_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "deprecated_function"
        ]
        for name in deprecated_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "method.*Series.*{}.*is deprecated".format(name)
            ):
                getattr(psser, name)()

        missing_properties = inspect.getmembers(
            MissingPandasLikeSeries, lambda o: isinstance(o, property)
        )
        unsupported_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "unsupported_property"
        ]
        for name in unsupported_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "property.*Series.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psser, name)
        deprecated_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "deprecated_property"
        ]
        for name in deprecated_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "property.*Series.*{}.*is deprecated".format(name)
            ):
                getattr(psser, name)

    def test_clip(self):
        pdf = pd.DataFrame({"x": [0, 2, 4]}, index=np.random.rand(3))
        psdf = ps.from_pandas(pdf)
        pser, psser = pdf.x, psdf.x

        # Assert list-like values are not accepted for 'lower' and 'upper'
        msg = "List-like value are not supported for 'lower' and 'upper' at the moment"
        with self.assertRaises(TypeError, msg=msg):
            psser.clip(lower=[1])
        with self.assertRaises(TypeError, msg=msg):
            psser.clip(upper=[1])

        # Assert no lower or upper
        self.assert_eq(psser.clip(), pser.clip())
        # Assert lower only
        self.assert_eq(psser.clip(1), pser.clip(1))
        # Assert upper only
        self.assert_eq(psser.clip(upper=3), pser.clip(upper=3))
        # Assert lower and upper
        self.assert_eq(psser.clip(1, 3), pser.clip(1, 3))
        self.assert_eq((psser + 1).clip(1, 3), (pser + 1).clip(1, 3))

        # Assert inplace is True
        pser.clip(1, 3, inplace=True)
        psser.clip(1, 3, inplace=True)
        self.assert_eq(psser, pser)
        self.assert_eq(psdf, pdf)

        # Assert behavior on string values
        str_psser = ps.Series(["a", "b", "c"])
        self.assert_eq(str_psser.clip(1, 3), str_psser)

    def test_compare(self):
        if LooseVersion(pd.__version__) >= LooseVersion("1.1"):
            pser = pd.Series([1, 2])
            psser = ps.from_pandas(pser)

            res_psdf = psser.compare(psser)
            self.assertTrue(res_psdf.empty)
            self.assert_eq(res_psdf.columns, pd.Index(["self", "other"]))

            self.assert_eq(
                pser.compare(pser + 1).sort_index(), psser.compare(psser + 1).sort_index()
            )

            pser = pd.Series([1, 2], index=["x", "y"])
            psser = ps.from_pandas(pser)
            self.assert_eq(
                pser.compare(pser + 1).sort_index(), psser.compare(psser + 1).sort_index()
            )
        else:
            psser = ps.Series([1, 2])
            res_psdf = psser.compare(psser)
            self.assertTrue(res_psdf.empty)
            self.assert_eq(res_psdf.columns, pd.Index(["self", "other"]))
            expected = ps.DataFrame([[1, 2], [2, 3]], columns=["self", "other"])
            self.assert_eq(expected, psser.compare(psser + 1).sort_index())

            psser = ps.Series([1, 2], index=["x", "y"])
            expected = ps.DataFrame([[1, 2], [2, 3]], index=["x", "y"], columns=["self", "other"])
            self.assert_eq(expected, psser.compare(psser + 1).sort_index())

    def test_is_unique(self):
        # We can't use pandas' is_unique for comparison. pandas 0.23 ignores None
        pser = pd.Series([1, 2, 2, None, None])
        psser = ps.from_pandas(pser)
        self.assertEqual(False, psser.is_unique)
        self.assertEqual(False, (psser + 1).is_unique)

        pser = pd.Series([1, None, None])
        psser = ps.from_pandas(pser)
        self.assertEqual(False, psser.is_unique)
        self.assertEqual(False, (psser + 1).is_unique)

        pser = pd.Series([1])
        psser = ps.from_pandas(pser)
        self.assertEqual(pser.is_unique, psser.is_unique)
        self.assertEqual((pser + 1).is_unique, (psser + 1).is_unique)

        pser = pd.Series([1, 1, 1])
        psser = ps.from_pandas(pser)
        self.assertEqual(pser.is_unique, psser.is_unique)
        self.assertEqual((pser + 1).is_unique, (psser + 1).is_unique)

    def test_to_list(self):
        self.assert_eq(self.psser.tolist(), self.pser.tolist())

    def test_append(self):
        pser1 = pd.Series([1, 2, 3], name="0")
        pser2 = pd.Series([4, 5, 6], name="0")
        pser3 = pd.Series([4, 5, 6], index=[3, 4, 5], name="0")
        psser1 = ps.from_pandas(pser1)
        psser2 = ps.from_pandas(pser2)
        psser3 = ps.from_pandas(pser3)

        self.assert_eq(psser1.append(psser2), pser1.append(pser2))
        self.assert_eq(psser1.append(psser3), pser1.append(pser3))
        self.assert_eq(
            psser1.append(psser2, ignore_index=True), pser1.append(pser2, ignore_index=True)
        )

        psser1.append(psser3, verify_integrity=True)
        msg = "Indices have overlapping values"
        with self.assertRaises(ValueError, msg=msg):
            psser1.append(psser2, verify_integrity=True)

    def test_map(self):
        pser = pd.Series(["cat", "dog", None, "rabbit"])
        psser = ps.from_pandas(pser)

        # dict correspondence
        # Currently pandas API on Spark doesn't return NaN as pandas does.
        self.assert_eq(psser.map({}), pser.map({}).replace({np.nan: None}))

        d = defaultdict(lambda: "abc")
        self.assertTrue("abc" in repr(psser.map(d)))
        self.assert_eq(psser.map(d), pser.map(d))

        # series correspondence
        pser_to_apply = pd.Series(["one", "two", "four"], index=["cat", "dog", "rabbit"])
        self.assert_eq(psser.map(pser_to_apply), pser.map(pser_to_apply))
        self.assert_eq(
            psser.map(pser_to_apply, na_action="ignore"),
            pser.map(pser_to_apply, na_action="ignore"),
        )

        # function correspondence
        self.assert_eq(
            psser.map(lambda x: x.upper(), na_action="ignore"),
            pser.map(lambda x: x.upper(), na_action="ignore"),
        )

        def to_upper(string) -> str:
            return string.upper() if string else ""

        self.assert_eq(psser.map(to_upper), pser.map(to_upper))

        def tomorrow(date) -> datetime:
            return date + timedelta(days=1)

        pser = pd.Series([datetime(2019, 10, 24)])
        psser = ps.from_pandas(pser)
        self.assert_eq(psser.map(tomorrow), pser.map(tomorrow))

    def test_add_prefix(self):
        pser = pd.Series([1, 2, 3, 4], name="0")
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.add_prefix("item_"), psser.add_prefix("item_"))

        pser = pd.Series(
            [1, 2, 3],
            name="0",
            index=pd.MultiIndex.from_tuples([("A", "X"), ("A", "Y"), ("B", "X")]),
        )
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.add_prefix("item_"), psser.add_prefix("item_"))

    def test_add_suffix(self):
        pser = pd.Series([1, 2, 3, 4], name="0")
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.add_suffix("_item"), psser.add_suffix("_item"))

        pser = pd.Series(
            [1, 2, 3],
            name="0",
            index=pd.MultiIndex.from_tuples([("A", "X"), ("A", "Y"), ("B", "X")]),
        )
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.add_suffix("_item"), psser.add_suffix("_item"))

    def test_cummin(self):
        pser = pd.Series([1.0, None, 0.0, 4.0, 9.0])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.cummin(), psser.cummin())
        self.assert_eq(pser.cummin(skipna=False), psser.cummin(skipna=False))
        self.assert_eq(pser.cummin().sum(), psser.cummin().sum())

        # with reversed index
        pser.index = [4, 3, 2, 1, 0]
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.cummin(), psser.cummin())
        self.assert_eq(pser.cummin(skipna=False), psser.cummin(skipna=False))

    def test_cummax(self):
        pser = pd.Series([1.0, None, 0.0, 4.0, 9.0])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.cummax(), psser.cummax())
        self.assert_eq(pser.cummax(skipna=False), psser.cummax(skipna=False))
        self.assert_eq(pser.cummax().sum(), psser.cummax().sum())

        # with reversed index
        pser.index = [4, 3, 2, 1, 0]
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.cummax(), psser.cummax())
        self.assert_eq(pser.cummax(skipna=False), psser.cummax(skipna=False))

    def test_cumsum(self):
        pser = pd.Series([1.0, None, 0.0, 4.0, 9.0])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.cumsum(), psser.cumsum())
        self.assert_eq(pser.cumsum(skipna=False), psser.cumsum(skipna=False))
        self.assert_eq(pser.cumsum().sum(), psser.cumsum().sum())

        # with reversed index
        pser.index = [4, 3, 2, 1, 0]
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.cumsum(), psser.cumsum())
        self.assert_eq(pser.cumsum(skipna=False), psser.cumsum(skipna=False))

        # bool
        pser = pd.Series([True, True, False, True])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.cumsum().astype(int), psser.cumsum())
        self.assert_eq(pser.cumsum(skipna=False).astype(int), psser.cumsum(skipna=False))

        with self.assertRaisesRegex(TypeError, r"Could not convert object \(string\) to numeric"):
            ps.Series(["a", "b", "c", "d"]).cumsum()

    def test_cumprod(self):
        pser = pd.Series([1.0, None, 1.0, 4.0, 9.0])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.cumprod(), psser.cumprod())
        self.assert_eq(pser.cumprod(skipna=False), psser.cumprod(skipna=False))
        self.assert_eq(pser.cumprod().sum(), psser.cumprod().sum())

        # with integer type
        pser = pd.Series([1, 10, 1, 4, 9])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.cumprod(), psser.cumprod())
        self.assert_eq(pser.cumprod(skipna=False), psser.cumprod(skipna=False))
        self.assert_eq(pser.cumprod().sum(), psser.cumprod().sum())

        # with reversed index
        pser.index = [4, 3, 2, 1, 0]
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.cumprod(), psser.cumprod())
        self.assert_eq(pser.cumprod(skipna=False), psser.cumprod(skipna=False))

        # including zero
        pser = pd.Series([1, 2, 0, 3])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.cumprod(), psser.cumprod())
        self.assert_eq(pser.cumprod(skipna=False), psser.cumprod(skipna=False))

        # including negative values
        pser = pd.Series([1, -1, -2])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.cumprod(), psser.cumprod())
        self.assert_eq(pser.cumprod(skipna=False), psser.cumprod(skipna=False))

        # bool
        pser = pd.Series([True, True, False, True])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.cumprod(), psser.cumprod())
        self.assert_eq(pser.cumprod(skipna=False).astype(int), psser.cumprod(skipna=False))

        with self.assertRaisesRegex(TypeError, r"Could not convert object \(string\) to numeric"):
            ps.Series(["a", "b", "c", "d"]).cumprod()

    def test_median(self):
        with self.assertRaisesRegex(TypeError, "accuracy must be an integer; however"):
            ps.Series([24.0, 21.0, 25.0, 33.0, 26.0]).median(accuracy="a")

    def test_rank(self):
        pser = pd.Series([1, 2, 3, 1], name="x")
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.rank(), psser.rank().sort_index())
        self.assert_eq(pser.rank().sum(), psser.rank().sum())
        self.assert_eq(pser.rank(ascending=False), psser.rank(ascending=False).sort_index())
        self.assert_eq(pser.rank(method="min"), psser.rank(method="min").sort_index())
        self.assert_eq(pser.rank(method="max"), psser.rank(method="max").sort_index())
        self.assert_eq(pser.rank(method="first"), psser.rank(method="first").sort_index())
        self.assert_eq(pser.rank(method="dense"), psser.rank(method="dense").sort_index())

        non_numeric_pser = pd.Series(["a", "c", "b", "d"], name="x", index=[10, 11, 12, 13])
        non_numeric_psser = ps.from_pandas(non_numeric_pser)
        self.assert_eq(
            non_numeric_pser.rank(numeric_only=True),
            non_numeric_psser.rank(numeric_only=True),
        )
        self.assert_eq(
            non_numeric_pser.rank(numeric_only=None),
            non_numeric_psser.rank(numeric_only=None).sort_index(),
        )
        self.assert_eq(
            non_numeric_pser.rank(numeric_only=False),
            non_numeric_psser.rank(numeric_only=False).sort_index(),
        )
        self.assert_eq(
            (non_numeric_pser + "x").rank(numeric_only=True),
            (non_numeric_psser + "x").rank(numeric_only=True),
        )

        msg = "method must be one of 'average', 'min', 'max', 'first', 'dense'"
        with self.assertRaisesRegex(ValueError, msg):
            psser.rank(method="nothing")

        msg = "method must be one of 'average', 'min', 'max', 'first', 'dense'"
        with self.assertRaisesRegex(ValueError, msg):
            psser.rank(method="nothing")

        midx = pd.MultiIndex.from_tuples([("a", "b"), ("a", "c"), ("b", "c"), ("c", "d")])
        pser.index = midx
        psser = ps.from_pandas(pser)
        msg = "rank do not support MultiIndex now"
        with self.assertRaisesRegex(NotImplementedError, msg):
            psser.rank(method="min")

    def test_round(self):
        pser = pd.Series([0.028208, 0.038683, 0.877076], name="x")
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.round(2), psser.round(2))
        msg = "decimals must be an integer"
        with self.assertRaisesRegex(TypeError, msg):
            psser.round(1.5)

    def test_quantile(self):
        pser = pd.Series([])
        psser = ps.from_pandas(pser)

        self.assert_eq(psser.quantile(0.5), pser.quantile(0.5))
        self.assert_eq(psser.quantile([0.25, 0.5, 0.75]), pser.quantile([0.25, 0.5, 0.75]))

        with self.assertRaisesRegex(TypeError, "accuracy must be an integer; however"):
            ps.Series([24.0, 21.0, 25.0, 33.0, 26.0]).quantile(accuracy="a")
        with self.assertRaisesRegex(TypeError, "q must be a float or an array of floats;"):
            ps.Series([24.0, 21.0, 25.0, 33.0, 26.0]).quantile(q=1)
        with self.assertRaisesRegex(TypeError, "q must be a float or an array of floats;"):
            ps.Series([24.0, 21.0, 25.0, 33.0, 26.0]).quantile(q=["a"])
        with self.assertRaisesRegex(
            ValueError, "percentiles should all be in the interval \\[0, 1\\]"
        ):
            ps.Series([24.0, 21.0, 25.0, 33.0, 26.0]).quantile(q=1.1)

        with self.assertRaisesRegex(TypeError, "Could not convert object \\(string\\) to numeric"):
            ps.Series(["a", "b", "c"]).quantile()
        with self.assertRaisesRegex(TypeError, "Could not convert object \\(string\\) to numeric"):
            ps.Series(["a", "b", "c"]).quantile([0.25, 0.5, 0.75])

    def test_idxmax(self):
        pser = pd.Series(data=[1, 4, 5], index=["A", "B", "C"])
        psser = ps.Series(pser)

        self.assertEqual(psser.idxmax(), pser.idxmax())
        self.assertEqual(psser.idxmax(skipna=False), pser.idxmax(skipna=False))

        index = pd.MultiIndex.from_arrays(
            [["a", "a", "b", "b"], ["c", "d", "e", "f"]], names=("first", "second")
        )
        pser = pd.Series(data=[1, 2, 4, 5], index=index)
        psser = ps.Series(pser)

        self.assertEqual(psser.idxmax(), pser.idxmax())
        self.assertEqual(psser.idxmax(skipna=False), pser.idxmax(skipna=False))

        psser = ps.Series([])
        with self.assertRaisesRegex(ValueError, "an empty sequence"):
            psser.idxmax()

        pser = pd.Series([1, 100, None, 100, 1, 100], index=[10, 3, 5, 2, 1, 8])
        psser = ps.Series(pser)

        self.assertEqual(psser.idxmax(), pser.idxmax())
        self.assertEqual(repr(psser.idxmax(skipna=False)), repr(pser.idxmax(skipna=False)))

    def test_idxmin(self):
        pser = pd.Series(data=[1, 4, 5], index=["A", "B", "C"])
        psser = ps.Series(pser)

        self.assertEqual(psser.idxmin(), pser.idxmin())
        self.assertEqual(psser.idxmin(skipna=False), pser.idxmin(skipna=False))

        index = pd.MultiIndex.from_arrays(
            [["a", "a", "b", "b"], ["c", "d", "e", "f"]], names=("first", "second")
        )
        pser = pd.Series(data=[1, 2, 4, 5], index=index)
        psser = ps.Series(pser)

        self.assertEqual(psser.idxmin(), pser.idxmin())
        self.assertEqual(psser.idxmin(skipna=False), pser.idxmin(skipna=False))

        psser = ps.Series([])
        with self.assertRaisesRegex(ValueError, "an empty sequence"):
            psser.idxmin()

        pser = pd.Series([1, 100, None, 100, 1, 100], index=[10, 3, 5, 2, 1, 8])
        psser = ps.Series(pser)

        self.assertEqual(psser.idxmin(), pser.idxmin())
        self.assertEqual(repr(psser.idxmin(skipna=False)), repr(pser.idxmin(skipna=False)))

    def test_shift(self):
        pser = pd.Series([10, 20, 15, 30, 45], name="x")
        psser = ps.Series(pser)

        self.assert_eq(psser.shift(2), pser.shift(2))
        self.assert_eq(psser.shift().shift(-1), pser.shift().shift(-1))
        self.assert_eq(psser.shift().sum(), pser.shift().sum())

        self.assert_eq(psser.shift(periods=2, fill_value=0), pser.shift(periods=2, fill_value=0))

        with self.assertRaisesRegex(TypeError, "periods should be an int; however"):
            psser.shift(periods=1.5)

        self.assert_eq(psser.shift(periods=0), pser.shift(periods=0))

    def test_diff(self):
        pser = pd.Series([10, 20, 15, 30, 45], name="x")
        psser = ps.Series(pser)

        self.assert_eq(psser.diff(2), pser.diff(2))
        self.assert_eq(psser.diff().diff(-1), pser.diff().diff(-1))
        self.assert_eq(psser.diff().sum(), pser.diff().sum())

    def _test_numeric_astype(self, pser):
        psser = ps.Series(pser)

        self.assert_eq(psser.astype(int), pser.astype(int))
        self.assert_eq(psser.astype(np.int), pser.astype(np.int))
        self.assert_eq(psser.astype(np.int8), pser.astype(np.int8))
        self.assert_eq(psser.astype(np.int16), pser.astype(np.int16))
        self.assert_eq(psser.astype(np.int32), pser.astype(np.int32))
        self.assert_eq(psser.astype(np.int64), pser.astype(np.int64))
        self.assert_eq(psser.astype(np.byte), pser.astype(np.byte))
        self.assert_eq(psser.astype("int"), pser.astype("int"))
        self.assert_eq(psser.astype("int8"), pser.astype("int8"))
        self.assert_eq(psser.astype("int16"), pser.astype("int16"))
        self.assert_eq(psser.astype("int32"), pser.astype("int32"))
        self.assert_eq(psser.astype("int64"), pser.astype("int64"))
        self.assert_eq(psser.astype("b"), pser.astype("b"))
        self.assert_eq(psser.astype("byte"), pser.astype("byte"))
        self.assert_eq(psser.astype("i"), pser.astype("i"))
        self.assert_eq(psser.astype("long"), pser.astype("long"))
        self.assert_eq(psser.astype("short"), pser.astype("short"))
        self.assert_eq(psser.astype(np.float), pser.astype(np.float))
        self.assert_eq(psser.astype(np.float32), pser.astype(np.float32))
        self.assert_eq(psser.astype(np.float64), pser.astype(np.float64))
        self.assert_eq(psser.astype("float"), pser.astype("float"))
        self.assert_eq(psser.astype("float32"), pser.astype("float32"))
        self.assert_eq(psser.astype("float64"), pser.astype("float64"))
        self.assert_eq(psser.astype("double"), pser.astype("double"))
        self.assert_eq(psser.astype("f"), pser.astype("f"))
        self.assert_eq(psser.astype(bool), pser.astype(bool))
        self.assert_eq(psser.astype("bool"), pser.astype("bool"))
        self.assert_eq(psser.astype("?"), pser.astype("?"))
        self.assert_eq(psser.astype(np.unicode_), pser.astype(np.unicode_))
        self.assert_eq(psser.astype("str"), pser.astype("str"))
        self.assert_eq(psser.astype("U"), pser.astype("U"))

        if extension_dtypes_available:
            from pandas import Int8Dtype, Int16Dtype, Int32Dtype, Int64Dtype

            self._check_extension(psser.astype("Int8"), pser.astype("Int8"))
            self._check_extension(psser.astype("Int16"), pser.astype("Int16"))
            self._check_extension(psser.astype("Int32"), pser.astype("Int32"))
            self._check_extension(psser.astype("Int64"), pser.astype("Int64"))
            self._check_extension(psser.astype(Int8Dtype()), pser.astype(Int8Dtype()))
            self._check_extension(psser.astype(Int16Dtype()), pser.astype(Int16Dtype()))
            self._check_extension(psser.astype(Int32Dtype()), pser.astype(Int32Dtype()))
            self._check_extension(psser.astype(Int64Dtype()), pser.astype(Int64Dtype()))

        if extension_object_dtypes_available:
            from pandas import StringDtype

            if LooseVersion(pd.__version__) >= LooseVersion("1.1"):
                self._check_extension(psser.astype("string"), pser.astype("string"))
                self._check_extension(psser.astype(StringDtype()), pser.astype(StringDtype()))
            else:
                self._check_extension(
                    psser.astype("string"),
                    pd.Series(["10", "20", "15", "30", "45"], name="x", dtype="string"),
                )
                self._check_extension(
                    psser.astype(StringDtype()),
                    pd.Series(["10", "20", "15", "30", "45"], name="x", dtype=StringDtype()),
                )

        if extension_float_dtypes_available:
            from pandas import Float32Dtype, Float64Dtype

            self._check_extension(psser.astype("Float32"), pser.astype("Float32"))
            self._check_extension(psser.astype("Float64"), pser.astype("Float64"))
            self._check_extension(psser.astype(Float32Dtype()), pser.astype(Float32Dtype()))
            self._check_extension(psser.astype(Float64Dtype()), pser.astype(Float64Dtype()))

    def test_astype(self):
        psers = [pd.Series([10, 20, 15, 30, 45], name="x")]

        if extension_dtypes_available:
            psers.append(pd.Series([10, 20, 15, 30, 45], name="x", dtype="Int64"))
        if extension_float_dtypes_available:
            psers.append(pd.Series([10, 20, 15, 30, 45], name="x", dtype="Float64"))

        for pser in psers:
            self._test_numeric_astype(pser)

        pser = pd.Series([10, 20, 15, 30, 45, None, np.nan], name="x")
        psser = ps.Series(pser)

        self.assert_eq(psser.astype(bool), pser.astype(bool))
        self.assert_eq(psser.astype(str), pser.astype(str))

        pser = pd.Series(["hi", "hi ", " ", " \t", "", None], name="x")
        psser = ps.Series(pser)

        self.assert_eq(psser.astype(bool), pser.astype(bool))
        if LooseVersion("1.1.1") <= LooseVersion(pd.__version__) < LooseVersion("1.1.4"):
            # a pandas bug: https://github.com/databricks/koalas/pull/1818#issuecomment-703961980
            self.assert_eq(psser.astype(str).tolist(), ["hi", "hi ", " ", " \t", "", "None"])
        else:
            self.assert_eq(psser.astype(str), pser.astype(str))
        self.assert_eq(psser.str.strip().astype(bool), pser.str.strip().astype(bool))

        if extension_object_dtypes_available:
            from pandas import StringDtype

            self._check_extension(psser.astype("string"), pser.astype("string"))
            self._check_extension(psser.astype(StringDtype()), pser.astype(StringDtype()))

        pser = pd.Series([True, False, None], name="x")
        psser = ps.Series(pser)

        self.assert_eq(psser.astype(bool), pser.astype(bool))
        self.assert_eq(psser.astype(str), pser.astype(str))

        if extension_object_dtypes_available:
            from pandas import BooleanDtype, StringDtype

            self._check_extension(psser.astype("boolean"), pser.astype("boolean"))
            self._check_extension(psser.astype(BooleanDtype()), pser.astype(BooleanDtype()))

            if LooseVersion(pd.__version__) >= LooseVersion("1.1"):
                self._check_extension(psser.astype("string"), pser.astype("string"))
                self._check_extension(psser.astype(StringDtype()), pser.astype(StringDtype()))
            else:
                self._check_extension(
                    psser.astype("string"),
                    pd.Series(["True", "False", None], name="x", dtype="string"),
                )
                self._check_extension(
                    psser.astype(StringDtype()),
                    pd.Series(["True", "False", None], name="x", dtype=StringDtype()),
                )

        pser = pd.Series(["2020-10-27 00:00:01", None], name="x")
        psser = ps.Series(pser)

        self.assert_eq(psser.astype(np.datetime64), pser.astype(np.datetime64))
        self.assert_eq(psser.astype("datetime64[ns]"), pser.astype("datetime64[ns]"))
        self.assert_eq(psser.astype("M"), pser.astype("M"))
        self.assert_eq(psser.astype("M").astype(str), pser.astype("M").astype(str))
        # Comment out the below test cause because pandas returns `NaT` or `nan` randomly
        # self.assert_eq(
        #     psser.astype("M").dt.date.astype(str), pser.astype("M").dt.date.astype(str)
        # )

        if extension_object_dtypes_available:
            from pandas import StringDtype

            # The behavior of casting datetime to nullable string is changed from pandas 1.3.
            if LooseVersion(pd.__version__) >= LooseVersion("1.3"):
                self._check_extension(
                    psser.astype("M").astype("string"), pser.astype("M").astype("string")
                )
                self._check_extension(
                    psser.astype("M").astype(StringDtype()), pser.astype("M").astype(StringDtype())
                )
            else:
                expected = ps.Series(["2020-10-27 00:00:01", None], name="x", dtype="string")
                self._check_extension(psser.astype("M").astype("string"), expected)
                self._check_extension(psser.astype("M").astype(StringDtype()), expected)

        with self.assertRaisesRegex(TypeError, "not understood"):
            psser.astype("int63")

    def test_aggregate(self):
        pser = pd.Series([10, 20, 15, 30, 45], name="x")
        psser = ps.Series(pser)
        msg = "func must be a string or list of strings"
        with self.assertRaisesRegex(TypeError, msg):
            psser.aggregate({"x": ["min", "max"]})
        msg = (
            "If the given function is a list, it " "should only contains function names as strings."
        )
        with self.assertRaisesRegex(ValueError, msg):
            psser.aggregate(["min", max])

    def test_drop(self):
        pdf = pd.DataFrame({"x": [10, 20, 15, 30, 45]})
        psdf = ps.from_pandas(pdf)
        pser, psser = pdf.x, psdf.x

        self.assert_eq(psser.drop(1), pser.drop(1))
        self.assert_eq(psser.drop([1, 4]), pser.drop([1, 4]))
        self.assert_eq(psser.drop(columns=1), pser.drop(columns=1))
        self.assert_eq(psser.drop(columns=[1, 4]), pser.drop(columns=[1, 4]))

        msg = "Need to specify at least one of 'labels', 'index' or 'columns'"
        with self.assertRaisesRegex(ValueError, msg):
            psser.drop()
        self.assertRaises(KeyError, lambda: psser.drop((0, 1)))

        psser.drop([2, 3], inplace=True)
        pser.drop([2, 3], inplace=True)
        self.assert_eq(psser, pser)
        self.assert_eq(psdf, pdf)

        n_pser, n_psser = pser + 1, psser + 1
        n_psser.drop([1, 4], inplace=True)
        n_pser.drop([1, 4], inplace=True)
        self.assert_eq(n_psser, n_pser)
        self.assert_eq(psser, pser)

        # For MultiIndex
        midx = pd.MultiIndex(
            [["lama", "cow", "falcon"], ["speed", "weight", "length"]],
            [[0, 0, 0, 1, 1, 1, 2, 2, 2], [0, 1, 2, 0, 1, 2, 0, 1, 2]],
        )

        pdf = pd.DataFrame({"x": [45, 200, 1.2, 30, 250, 1.5, 320, 1, 0.3]}, index=midx)
        psdf = ps.from_pandas(pdf)
        psser, pser = psdf.x, pdf.x

        self.assert_eq(psser.drop("lama"), pser.drop("lama"))
        self.assert_eq(psser.drop(labels="weight", level=1), pser.drop(labels="weight", level=1))
        self.assert_eq(psser.drop(("lama", "weight")), pser.drop(("lama", "weight")))
        self.assert_eq(
            psser.drop([("lama", "speed"), ("falcon", "weight")]),
            pser.drop([("lama", "speed"), ("falcon", "weight")]),
        )
        self.assert_eq(psser.drop({"lama": "speed"}), pser.drop({"lama": "speed"}))

        msg = "'level' should be less than the number of indexes"
        with self.assertRaisesRegex(ValueError, msg):
            psser.drop(labels="weight", level=2)

        msg = (
            "If the given index is a list, it "
            "should only contains names as all tuples or all non tuples "
            "that contain index names"
        )
        with self.assertRaisesRegex(ValueError, msg):
            psser.drop(["lama", ["cow", "falcon"]])

        msg = "Cannot specify both 'labels' and 'index'/'columns'"
        with self.assertRaisesRegex(ValueError, msg):
            psser.drop("lama", index="cow")

        with self.assertRaisesRegex(ValueError, msg):
            psser.drop("lama", columns="cow")

        msg = r"'Key length \(2\) exceeds index depth \(3\)'"
        with self.assertRaisesRegex(KeyError, msg):
            psser.drop(("lama", "speed", "x"))

        psser.drop({"lama": "speed"}, inplace=True)
        pser.drop({"lama": "speed"}, inplace=True)
        self.assert_eq(psser, pser)
        self.assert_eq(psdf, pdf)

    def test_pop(self):
        midx = pd.MultiIndex(
            [["lama", "cow", "falcon"], ["speed", "weight", "length"]],
            [[0, 0, 0, 1, 1, 1, 2, 2, 2], [0, 1, 2, 0, 1, 2, 0, 1, 2]],
        )
        pdf = pd.DataFrame({"x": [45, 200, 1.2, 30, 250, 1.5, 320, 1, 0.3]}, index=midx)
        psdf = ps.from_pandas(pdf)

        pser = pdf.x
        psser = psdf.x

        self.assert_eq(psser.pop(("lama", "speed")), pser.pop(("lama", "speed")))
        self.assert_eq(psser, pser)
        self.assert_eq(psdf, pdf)

        msg = r"'Key length \(3\) exceeds index depth \(2\)'"
        with self.assertRaisesRegex(KeyError, msg):
            psser.pop(("lama", "speed", "x"))

        msg = "'key' should be string or tuple that contains strings"
        with self.assertRaisesRegex(TypeError, msg):
            psser.pop(["lama", "speed"])

        pser = pd.Series(["a", "b", "c", "a"], dtype="category")
        psser = ps.from_pandas(pser)

        if LooseVersion(pd.__version__) >= LooseVersion("1.3.0"):
            self.assert_eq(psser.pop(0), pser.pop(0))
            self.assert_eq(psser, pser)

            self.assert_eq(psser.pop(3), pser.pop(3))
            self.assert_eq(psser, pser)
        else:
            # Before pandas 1.3.0, `pop` modifies the dtype of categorical series wrongly.
            self.assert_eq(psser.pop(0), "a")
            self.assert_eq(
                psser,
                pd.Series(
                    pd.Categorical(["b", "c", "a"], categories=["a", "b", "c"]), index=[1, 2, 3]
                ),
            )

            self.assert_eq(psser.pop(3), "a")
            self.assert_eq(
                psser,
                pd.Series(pd.Categorical(["b", "c"], categories=["a", "b", "c"]), index=[1, 2]),
            )

    def test_replace(self):
        pser = pd.Series([10, 20, 15, 30, np.nan], name="x")
        psser = ps.from_pandas(pser)

        self.assert_eq(psser.replace(), pser.replace())
        self.assert_eq(psser.replace({}), pser.replace({}))

        self.assert_eq(psser.replace(np.nan, 45), pser.replace(np.nan, 45))
        self.assert_eq(psser.replace([10, 15], 45), pser.replace([10, 15], 45))
        self.assert_eq(psser.replace((10, 15), 45), pser.replace((10, 15), 45))
        self.assert_eq(psser.replace([10, 15], [45, 50]), pser.replace([10, 15], [45, 50]))
        self.assert_eq(psser.replace((10, 15), (45, 50)), pser.replace((10, 15), (45, 50)))

        pser = pd.Series(["bat", "foo", "bait", "abc", "bar", "zoo"])
        psser = ps.from_pandas(pser)
        self.assert_eq(
            psser.replace(to_replace=r"^ba.$", value="new", regex=True),
            pser.replace(to_replace=r"^ba.$", value="new", regex=True),
        )
        self.assert_eq(
            psser.replace(regex=r"^.oo$", value="new"), pser.replace(regex=r"^.oo$", value="new")
        )
        self.assert_eq(
            (psser + "o").replace(regex=r"^.ooo$", value="new"),
            (pser + "o").replace(regex=r"^.ooo$", value="new"),
        )

        msg = "'to_replace' should be one of str, list, tuple, dict, int, float"
        with self.assertRaisesRegex(TypeError, msg):
            psser.replace(ps.range(5))
        msg = "Replacement lists must match in length. Expecting 3 got 2"
        with self.assertRaisesRegex(ValueError, msg):
            psser.replace(["bat", "foo", "bait"], ["a", "b"])
        msg = "'to_replace' must be 'None' if 'regex' is not a bool"
        with self.assertRaisesRegex(ValueError, msg):
            psser.replace(to_replace="foo", regex=r"^.oo$")
        msg = "If 'regex' is True then 'to_replace' must be a string"
        with self.assertRaisesRegex(AssertionError, msg):
            psser.replace(["bat", "foo", "bait"], regex=True)
        unsupported_regex = [r"^.oo$", r"^ba.$"]
        msg = "'regex' of %s type is not supported" % type(unsupported_regex).__name__
        with self.assertRaisesRegex(NotImplementedError, msg):
            psser.replace(regex=unsupported_regex, value="new")

    def test_xs(self):
        midx = pd.MultiIndex(
            [["a", "b", "c"], ["lama", "cow", "falcon"], ["speed", "weight", "length"]],
            [[0, 0, 0, 1, 1, 1, 2, 2, 2], [0, 0, 0, 1, 1, 1, 2, 2, 2], [0, 1, 2, 0, 1, 2, 0, 1, 2]],
        )
        pser = pd.Series([45, 200, 1.2, 30, 250, 1.5, 320, 1, 0.3], index=midx)
        psser = ps.from_pandas(pser)

        self.assert_eq(psser.xs(("a", "lama", "speed")), pser.xs(("a", "lama", "speed")))

    def test_duplicates(self):
        psers = {
            "test on texts": pd.Series(
                ["lama", "cow", "lama", "beetle", "lama", "hippo"], name="animal"
            ),
            "test on numbers": pd.Series([1, 1, 2, 4, 3]),
        }
        keeps = ["first", "last", False]

        for (msg, pser), keep in product(psers.items(), keeps):
            with self.subTest(msg, keep=keep):
                psser = ps.Series(pser)

                self.assert_eq(
                    pser.drop_duplicates(keep=keep).sort_values(),
                    psser.drop_duplicates(keep=keep).sort_values(),
                )

    def test_update(self):
        pser = pd.Series([10, 20, 15, 30, 45], name="x")
        psser = ps.Series(pser)

        msg = "'other' must be a Series"
        with self.assertRaisesRegex(TypeError, msg):
            psser.update(10)

        def _get_data():
            pdf = pd.DataFrame(
                {
                    "a": [None, 2, 3, 4, 5, 6, 7, 8, None],
                    "b": [None, 5, None, 3, 2, 1, None, 0, 0],
                    "c": [1, 5, 1, 3, 2, 1, 1, 0, 0],
                },
            )
            psdf = ps.from_pandas(pdf)
            return pdf, psdf

        pdf, psdf = _get_data()

        psdf.a.update(psdf.a)
        pdf.a.update(pdf.a)
        self.assert_eq(psdf, pdf)

        pdf, psdf = _get_data()

        psdf.a.update(psdf.b)
        pdf.a.update(pdf.b)
        self.assert_eq(psdf, pdf)

        pdf, psdf = _get_data()
        pser = pdf.a
        psser = psdf.a

        pser.update(pdf.b)
        psser.update(psdf.b)
        self.assert_eq(psser, pser)
        self.assert_eq(psdf, pdf)

    def test_where(self):
        pser1 = pd.Series([0, 1, 2, 3, 4])
        psser1 = ps.from_pandas(pser1)

        self.assert_eq(pser1.where(pser1 > 3), psser1.where(psser1 > 3).sort_index())

    def test_mask(self):
        pser1 = pd.Series([0, 1, 2, 3, 4])
        psser1 = ps.from_pandas(pser1)

        self.assert_eq(pser1.mask(pser1 > 3), psser1.mask(psser1 > 3).sort_index())

    def test_truncate(self):
        pser1 = pd.Series([10, 20, 30, 40, 50, 60, 70], index=[1, 2, 3, 4, 5, 6, 7])
        psser1 = ps.Series(pser1)
        pser2 = pd.Series([10, 20, 30, 40, 50, 60, 70], index=[7, 6, 5, 4, 3, 2, 1])
        psser2 = ps.Series(pser2)

        self.assert_eq(psser1.truncate(), pser1.truncate())
        self.assert_eq(psser1.truncate(before=2), pser1.truncate(before=2))
        self.assert_eq(psser1.truncate(after=5), pser1.truncate(after=5))
        self.assert_eq(psser1.truncate(copy=False), pser1.truncate(copy=False))
        self.assert_eq(psser1.truncate(2, 5, copy=False), pser1.truncate(2, 5, copy=False))
        # The bug for these tests has been fixed in pandas 1.1.0.
        if LooseVersion(pd.__version__) >= LooseVersion("1.1.0"):
            self.assert_eq(psser2.truncate(4, 6), pser2.truncate(4, 6))
            self.assert_eq(psser2.truncate(4, 6, copy=False), pser2.truncate(4, 6, copy=False))
        else:
            expected_psser = ps.Series([20, 30, 40], index=[6, 5, 4])
            self.assert_eq(psser2.truncate(4, 6), expected_psser)
            self.assert_eq(psser2.truncate(4, 6, copy=False), expected_psser)

        psser = ps.Series([10, 20, 30, 40, 50, 60, 70], index=[1, 2, 3, 4, 3, 2, 1])
        msg = "truncate requires a sorted index"
        with self.assertRaisesRegex(ValueError, msg):
            psser.truncate()

        psser = ps.Series([10, 20, 30, 40, 50, 60, 70], index=[1, 2, 3, 4, 5, 6, 7])
        msg = "Truncate: 2 must be after 5"
        with self.assertRaisesRegex(ValueError, msg):
            psser.truncate(5, 2)

    def test_getitem(self):
        pser = pd.Series([10, 20, 15, 30, 45], ["A", "A", "B", "C", "D"])
        psser = ps.Series(pser)

        self.assert_eq(psser["A"], pser["A"])
        self.assert_eq(psser["B"], pser["B"])
        self.assert_eq(psser[psser > 15], pser[pser > 15])

        # for MultiIndex
        midx = pd.MultiIndex(
            [["a", "b", "c"], ["lama", "cow", "falcon"], ["speed", "weight", "length"]],
            [[0, 0, 0, 0, 0, 0, 1, 1, 1], [0, 0, 0, 1, 1, 1, 2, 2, 2], [0, 0, 0, 0, 1, 2, 0, 1, 2]],
        )
        pser = pd.Series([45, 200, 1.2, 30, 250, 1.5, 320, 1, 0.3], name="0", index=midx)
        psser = ps.Series(pser)

        self.assert_eq(psser["a"], pser["a"])
        self.assert_eq(psser["a", "lama"], pser["a", "lama"])
        self.assert_eq(psser[psser > 1.5], pser[pser > 1.5])

        msg = r"'Key length \(4\) exceeds index depth \(3\)'"
        with self.assertRaisesRegex(KeyError, msg):
            psser[("a", "lama", "speed", "x")]

    def test_keys(self):
        midx = pd.MultiIndex(
            [["lama", "cow", "falcon"], ["speed", "weight", "length"]],
            [[0, 0, 0, 1, 1, 1, 2, 2, 2], [0, 1, 2, 0, 1, 2, 0, 1, 2]],
        )
        pser = pd.Series([45, 200, 1.2, 30, 250, 1.5, 320, 1, 0.3], index=midx)
        psser = ps.from_pandas(pser)

        self.assert_eq(psser.keys(), pser.keys())

    def test_index(self):
        # to check setting name of Index properly.
        idx = pd.Index([1, 2, 3, 4, 5, 6, 7, 8, 9])
        pser = pd.Series([45, 200, 1.2, 30, 250, 1.5, 320, 1, 0.3], index=idx)
        psser = ps.from_pandas(pser)

        psser.name = "koalas"
        pser.name = "koalas"
        self.assert_eq(psser.index.name, pser.index.name)

        # for check setting names of MultiIndex properly.
        psser.names = ["hello", "koalas"]
        pser.names = ["hello", "koalas"]
        self.assert_eq(psser.index.names, pser.index.names)

    def test_pct_change(self):
        pser = pd.Series([90, 91, 85], index=[2, 4, 1])
        psser = ps.from_pandas(pser)

        self.assert_eq(psser.pct_change(), pser.pct_change(), check_exact=False)
        self.assert_eq(psser.pct_change().sum(), pser.pct_change().sum(), almost=True)
        self.assert_eq(psser.pct_change(periods=2), pser.pct_change(periods=2), check_exact=False)
        self.assert_eq(psser.pct_change(periods=-1), pser.pct_change(periods=-1), check_exact=False)
        self.assert_eq(psser.pct_change(periods=-100000000), pser.pct_change(periods=-100000000))
        self.assert_eq(psser.pct_change(periods=100000000), pser.pct_change(periods=100000000))

        # for MultiIndex
        midx = pd.MultiIndex(
            [["lama", "cow", "falcon"], ["speed", "weight", "length"]],
            [[0, 0, 0, 1, 1, 1, 2, 2, 2], [0, 1, 2, 0, 1, 2, 0, 1, 2]],
        )
        pser = pd.Series([45, 200, 1.2, 30, 250, 1.5, 320, 1, 0.3], index=midx)
        psser = ps.from_pandas(pser)

        self.assert_eq(psser.pct_change(), pser.pct_change(), check_exact=False)
        self.assert_eq(psser.pct_change().sum(), pser.pct_change().sum(), almost=True)
        self.assert_eq(psser.pct_change(periods=2), pser.pct_change(periods=2), check_exact=False)
        self.assert_eq(psser.pct_change(periods=-1), pser.pct_change(periods=-1), check_exact=False)
        self.assert_eq(psser.pct_change(periods=-100000000), pser.pct_change(periods=-100000000))
        self.assert_eq(psser.pct_change(periods=100000000), pser.pct_change(periods=100000000))

    def test_axes(self):
        pser = pd.Series([90, 91, 85], index=[2, 4, 1])
        psser = ps.from_pandas(pser)
        self.assert_eq(psser.axes, pser.axes)

        # for MultiIndex
        midx = pd.MultiIndex(
            [["lama", "cow", "falcon"], ["speed", "weight", "length"]],
            [[0, 0, 0, 1, 1, 1, 2, 2, 2], [0, 1, 2, 0, 1, 2, 0, 1, 2]],
        )
        pser = pd.Series([45, 200, 1.2, 30, 250, 1.5, 320, 1, 0.3], index=midx)
        psser = ps.from_pandas(pser)
        self.assert_eq(psser.axes, pser.axes)

    def test_udt(self):
        sparse_values = {0: 0.1, 1: 1.1}
        sparse_vector = SparseVector(len(sparse_values), sparse_values)
        pser = pd.Series([sparse_vector])
        psser = ps.from_pandas(pser)
        self.assert_eq(psser, pser)

    def test_repeat(self):
        pser = pd.Series(["a", "b", "c"], name="0", index=np.random.rand(3))
        psser = ps.from_pandas(pser)

        self.assert_eq(psser.repeat(3).sort_index(), pser.repeat(3).sort_index())
        self.assert_eq(psser.repeat(0).sort_index(), pser.repeat(0).sort_index())

        self.assertRaises(ValueError, lambda: psser.repeat(-1))
        self.assertRaises(TypeError, lambda: psser.repeat("abc"))

        pdf = pd.DataFrame({"a": ["a", "b", "c"], "rep": [10, 20, 30]}, index=np.random.rand(3))
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.a.repeat(psdf.rep).sort_index(), pdf.a.repeat(pdf.rep).sort_index())

    def test_take(self):
        pser = pd.Series([100, 200, 300, 400, 500], name="Koalas")
        psser = ps.from_pandas(pser)

        self.assert_eq(psser.take([0, 2, 4]).sort_values(), pser.take([0, 2, 4]).sort_values())
        self.assert_eq(
            psser.take(range(0, 5, 2)).sort_values(), pser.take(range(0, 5, 2)).sort_values()
        )
        self.assert_eq(psser.take([-4, -2, 0]).sort_values(), pser.take([-4, -2, 0]).sort_values())
        self.assert_eq(
            psser.take(range(-2, 1, 2)).sort_values(), pser.take(range(-2, 1, 2)).sort_values()
        )

        # Checking the type of indices.
        self.assertRaises(TypeError, lambda: psser.take(1))
        self.assertRaises(TypeError, lambda: psser.take("1"))
        self.assertRaises(TypeError, lambda: psser.take({1, 2}))
        self.assertRaises(TypeError, lambda: psser.take({1: None, 2: None}))

    def test_divmod(self):
        pser = pd.Series([100, None, 300, None, 500], name="Koalas")
        psser = ps.from_pandas(pser)

        kdiv, kmod = psser.divmod(-100)
        pdiv, pmod = pser.divmod(-100)
        self.assert_eq(kdiv, pdiv)
        self.assert_eq(kmod, pmod)

        kdiv, kmod = psser.divmod(100)
        pdiv, pmod = pser.divmod(100)
        self.assert_eq(kdiv, pdiv)
        self.assert_eq(kmod, pmod)

    def test_rdivmod(self):
        pser = pd.Series([100, None, 300, None, 500])
        psser = ps.from_pandas(pser)

        krdiv, krmod = psser.rdivmod(-100)
        prdiv, prmod = pser.rdivmod(-100)
        self.assert_eq(krdiv, prdiv)
        self.assert_eq(krmod, prmod)

        krdiv, krmod = psser.rdivmod(100)
        prdiv, prmod = pser.rdivmod(100)
        self.assert_eq(krdiv, prdiv)
        self.assert_eq(krmod, prmod)

    def test_mod(self):
        pser = pd.Series([100, None, -300, None, 500, -700], name="Koalas")
        psser = ps.from_pandas(pser)

        self.assert_eq(psser.mod(-150), pser.mod(-150))
        self.assert_eq(psser.mod(0), pser.mod(0))
        self.assert_eq(psser.mod(150), pser.mod(150))

        pdf = pd.DataFrame({"a": [100, None, -300, None, 500, -700], "b": [150] * 6})
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psdf.a.mod(psdf.b), pdf.a.mod(pdf.b))

    def test_mode(self):
        pser = pd.Series([0, 0, 1, 1, 1, np.nan, np.nan, np.nan])
        psser = ps.from_pandas(pser)
        self.assert_eq(psser.mode(), pser.mode())
        self.assert_eq(
            psser.mode(dropna=False).sort_values().reset_index(drop=True),
            pser.mode(dropna=False).sort_values().reset_index(drop=True),
        )

        pser.name = "x"
        psser = ps.from_pandas(pser)
        if LooseVersion(pd.__version__) < LooseVersion("1.4"):
            # Due to pandas bug: https://github.com/pandas-dev/pandas/issues/46737
            psser.name = None
            self.assert_eq(psser.mode(), pser.mode())
        else:
            self.assert_eq(psser.mode(), pser.mode())
        self.assert_eq(
            psser.mode(dropna=False).sort_values().reset_index(drop=True),
            pser.mode(dropna=False).sort_values().reset_index(drop=True),
        )

    def test_rmod(self):
        pser = pd.Series([100, None, -300, None, 500, -700], name="Koalas")
        psser = ps.from_pandas(pser)

        self.assert_eq(psser.rmod(-150), pser.rmod(-150))
        self.assert_eq(psser.rmod(0), pser.rmod(0))
        self.assert_eq(psser.rmod(150), pser.rmod(150))

        pdf = pd.DataFrame({"a": [100, None, -300, None, 500, -700], "b": [150] * 6})
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psdf.a.rmod(psdf.b), pdf.a.rmod(pdf.b))

    def test_asof(self):
        pser = pd.Series([1, 2, np.nan, 4], index=[10, 20, 30, 40], name="Koalas")
        psser = ps.from_pandas(pser)

        self.assert_eq(psser.asof(20), pser.asof(20))
        self.assert_eq(psser.asof([5, 20]).sort_index(), pser.asof([5, 20]).sort_index())
        self.assert_eq(psser.asof(100), pser.asof(100))
        self.assert_eq(repr(psser.asof(-100)), repr(pser.asof(-100)))
        self.assert_eq(psser.asof([-100, 100]).sort_index(), pser.asof([-100, 100]).sort_index())

        # where cannot be an Index, Series or a DataFrame
        self.assertRaises(ValueError, lambda: psser.asof(ps.Index([-100, 100])))
        self.assertRaises(ValueError, lambda: psser.asof(ps.Series([-100, 100])))
        self.assertRaises(ValueError, lambda: psser.asof(ps.DataFrame({"A": [1, 2, 3]})))
        # asof is not supported for a MultiIndex
        pser.index = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c"), ("y", "d")])
        psser = ps.from_pandas(pser)
        self.assertRaises(ValueError, lambda: psser.asof(20))
        # asof requires a sorted index (More precisely, should be a monotonic increasing)
        psser = ps.Series([1, 2, np.nan, 4], index=[10, 30, 20, 40], name="Koalas")
        self.assertRaises(ValueError, lambda: psser.asof(20))
        psser = ps.Series([1, 2, np.nan, 4], index=[40, 30, 20, 10], name="Koalas")
        self.assertRaises(ValueError, lambda: psser.asof(20))

        pidx = pd.DatetimeIndex(["2013-12-31", "2014-01-02", "2014-01-03"])
        pser = pd.Series([1, 2, np.nan], index=pidx)
        psser = ps.from_pandas(pser)

        self.assert_eq(psser.asof("2014-01-01"), pser.asof("2014-01-01"))
        self.assert_eq(psser.asof("2014-01-02"), pser.asof("2014-01-02"))
        self.assert_eq(repr(psser.asof("1999-01-02")), repr(pser.asof("1999-01-02")))

        # SPARK-37482: Skip check monotonic increasing for Series.asof with 'compute.eager_check'
        pser = pd.Series([1, 2, np.nan, 4], index=[10, 30, 20, 40])
        psser = ps.from_pandas(pser)

        with ps.option_context("compute.eager_check", False):
            self.assert_eq(psser.asof(20), 1.0)

        pser = pd.Series([1, 2, np.nan, 4], index=[40, 30, 20, 10])
        psser = ps.from_pandas(pser)

        with ps.option_context("compute.eager_check", False):
            self.assert_eq(psser.asof(20), 4.0)

        pser = pd.Series([2, 1, np.nan, 4], index=[10, 20, 30, 40], name="Koalas")
        psser = ps.from_pandas(pser)
        self.assert_eq(psser.asof([5, 20]), pser.asof([5, 20]))

        pser = pd.Series([4, np.nan, np.nan, 2], index=[10, 20, 30, 40], name="Koalas")
        psser = ps.from_pandas(pser)
        self.assert_eq(psser.asof([5, 100]), pser.asof([5, 100]))

        pser = pd.Series([np.nan, 4, 1, 2], index=[10, 20, 30, 40], name="Koalas")
        psser = ps.from_pandas(pser)
        self.assert_eq(psser.asof([5, 35]), pser.asof([5, 35]))

        pser = pd.Series([2, 1, np.nan, 4], index=[10, 20, 30, 40], name="Koalas")
        psser = ps.from_pandas(pser)
        self.assert_eq(psser.asof([25, 25]), pser.asof([25, 25]))

        pser = pd.Series([2, 1, np.nan, 4], index=["a", "b", "c", "d"], name="Koalas")
        psser = ps.from_pandas(pser)
        self.assert_eq(psser.asof(["a", "d"]), pser.asof(["a", "d"]))

        pser = pd.Series(
            [2, 1, np.nan, 4],
            index=[
                pd.Timestamp(2020, 1, 1),
                pd.Timestamp(2020, 2, 2),
                pd.Timestamp(2020, 3, 3),
                pd.Timestamp(2020, 4, 4),
            ],
            name="Koalas",
        )
        psser = ps.from_pandas(pser)
        self.assert_eq(
            psser.asof([pd.Timestamp(2020, 1, 1)]),
            pser.asof([pd.Timestamp(2020, 1, 1)]),
        )

        pser = pd.Series([2, np.nan, 1, 4], index=[10, 20, 30, 40], name="Koalas")
        psser = ps.from_pandas(pser)
        self.assert_eq(psser.asof(np.nan), pser.asof(np.nan))
        self.assert_eq(psser.asof([np.nan, np.nan]), pser.asof([np.nan, np.nan]))
        self.assert_eq(psser.asof([10, np.nan]), pser.asof([10, np.nan]))

    def test_squeeze(self):
        # Single value
        pser = pd.Series([90])
        psser = ps.from_pandas(pser)
        self.assert_eq(psser.squeeze(), pser.squeeze())

        # Single value with MultiIndex
        midx = pd.MultiIndex.from_tuples([("a", "b", "c")])
        pser = pd.Series([90], index=midx)
        psser = ps.from_pandas(pser)
        self.assert_eq(psser.squeeze(), pser.squeeze())

        # Multiple values
        pser = pd.Series([90, 91, 85])
        psser = ps.from_pandas(pser)
        self.assert_eq(psser.squeeze(), pser.squeeze())

        # Multiple values with MultiIndex
        midx = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        pser = pd.Series([90, 91, 85], index=midx)
        psser = ps.from_pandas(pser)
        self.assert_eq(psser.squeeze(), pser.squeeze())

    def test_swaplevel(self):
        # MultiIndex with two levels
        arrays = [[1, 1, 2, 2], ["red", "blue", "red", "blue"]]
        pidx = pd.MultiIndex.from_arrays(arrays, names=("number", "color"))
        pser = pd.Series(["a", "b", "c", "d"], index=pidx)
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.swaplevel(), psser.swaplevel())
        self.assert_eq(pser.swaplevel(0, 1), psser.swaplevel(0, 1))
        self.assert_eq(pser.swaplevel(1, 1), psser.swaplevel(1, 1))
        self.assert_eq(pser.swaplevel("number", "color"), psser.swaplevel("number", "color"))

        # MultiIndex with more than two levels
        arrays = [[1, 1, 2, 2], ["red", "blue", "red", "blue"], ["l", "m", "s", "xs"]]
        pidx = pd.MultiIndex.from_arrays(arrays, names=("number", "color", "size"))
        pser = pd.Series(["a", "b", "c", "d"], index=pidx)
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.swaplevel(), psser.swaplevel())
        self.assert_eq(pser.swaplevel(0, 1), psser.swaplevel(0, 1))
        self.assert_eq(pser.swaplevel(0, 2), psser.swaplevel(0, 2))
        self.assert_eq(pser.swaplevel(1, 2), psser.swaplevel(1, 2))
        self.assert_eq(pser.swaplevel(1, 1), psser.swaplevel(1, 1))
        self.assert_eq(pser.swaplevel(-1, -2), psser.swaplevel(-1, -2))
        self.assert_eq(pser.swaplevel("number", "color"), psser.swaplevel("number", "color"))
        self.assert_eq(pser.swaplevel("number", "size"), psser.swaplevel("number", "size"))
        self.assert_eq(pser.swaplevel("color", "size"), psser.swaplevel("color", "size"))

        # Error conditions
        self.assertRaises(AssertionError, lambda: ps.Series([1, 2]).swaplevel())
        self.assertRaises(IndexError, lambda: psser.swaplevel(0, 9))
        self.assertRaises(KeyError, lambda: psser.swaplevel("not_number", "color"))
        self.assertRaises(AssertionError, lambda: psser.swaplevel(copy=False))

    def test_swapaxes(self):
        pser = pd.Series([1, 2, 3], index=["x", "y", "z"], name="ser")
        psser = ps.from_pandas(pser)

        self.assert_eq(psser.swapaxes(0, 0), pser.swapaxes(0, 0))
        self.assert_eq(psser.swapaxes("index", "index"), pser.swapaxes("index", "index"))
        self.assert_eq((psser + 1).swapaxes(0, 0), (pser + 1).swapaxes(0, 0))

        self.assertRaises(AssertionError, lambda: psser.swapaxes(0, 1, copy=False))
        self.assertRaises(ValueError, lambda: psser.swapaxes(0, 1))
        self.assertRaises(ValueError, lambda: psser.swapaxes("index", "columns"))

    def test_div_zero_and_nan(self):
        pser = pd.Series([100, None, -300, None, 500, -700, np.inf, -np.inf], name="Koalas")
        psser = ps.from_pandas(pser)

        self.assert_eq(pser.div(0), psser.div(0))
        self.assert_eq(pser.truediv(0), psser.truediv(0))
        self.assert_eq(pser / 0, psser / 0)
        self.assert_eq(pser.div(np.nan), psser.div(np.nan))
        self.assert_eq(pser.truediv(np.nan), psser.truediv(np.nan))
        self.assert_eq(pser / np.nan, psser / np.nan)

        self.assert_eq(pser.floordiv(0), psser.floordiv(0))
        self.assert_eq(pser // 0, psser // 0)
        self.assert_eq(pser.floordiv(np.nan), psser.floordiv(np.nan))

    def test_mad(self):
        pser = pd.Series([1, 2, 3, 4], name="Koalas")
        psser = ps.from_pandas(pser)

        self.assert_eq(pser.mad(), psser.mad())

        pser = pd.Series([None, -2, 5, 10, 50, np.nan, -20], name="Koalas")
        psser = ps.from_pandas(pser)

        self.assert_eq(pser.mad(), psser.mad())

        pmidx = pd.MultiIndex.from_tuples(
            [("a", "1"), ("a", "2"), ("b", "1"), ("b", "2"), ("c", "1")]
        )
        pser = pd.Series([1, 2, 3, 4, 5], name="Koalas")
        pser.index = pmidx
        psser = ps.from_pandas(pser)

        self.assert_eq(pser.mad(), psser.mad())

        pmidx = pd.MultiIndex.from_tuples(
            [("a", "1"), ("a", "2"), ("b", "1"), ("b", "2"), ("c", "1")]
        )
        pser = pd.Series([None, -2, 5, 50, np.nan], name="Koalas")
        pser.index = pmidx
        psser = ps.from_pandas(pser)

        # Mark almost as True to avoid precision issue like:
        # "21.555555555555554 != 21.555555555555557"
        self.assert_eq(pser.mad(), psser.mad(), almost=True)

    def test_to_frame(self):
        pser = pd.Series(["a", "b", "c"])
        psser = ps.from_pandas(pser)

        self.assert_eq(pser.to_frame(name="a"), psser.to_frame(name="a"))

        # for MultiIndex
        midx = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        pser = pd.Series(["a", "b", "c"], index=midx)
        psser = ps.from_pandas(pser)

        self.assert_eq(pser.to_frame(name="a"), psser.to_frame(name="a"))

    def test_shape(self):
        pser = pd.Series(["a", "b", "c"])
        psser = ps.from_pandas(pser)

        self.assert_eq(pser.shape, psser.shape)

        # for MultiIndex
        midx = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        pser = pd.Series(["a", "b", "c"], index=midx)
        psser = ps.from_pandas(pser)

        self.assert_eq(pser.shape, psser.shape)

    @unittest.skipIf(not have_tabulate, tabulate_requirement_message)
    def test_to_markdown(self):
        pser = pd.Series(["elk", "pig", "dog", "quetzal"], name="animal")
        psser = ps.from_pandas(pser)

        self.assert_eq(pser.to_markdown(), psser.to_markdown())

    def test_unstack(self):
        pser = pd.Series(
            [10, -2, 4, 7],
            index=pd.MultiIndex.from_tuples(
                [("one", "a", "z"), ("one", "b", "x"), ("two", "a", "c"), ("two", "b", "v")],
                names=["A", "B", "C"],
            ),
        )
        psser = ps.from_pandas(pser)

        levels = [-3, -2, -1, 0, 1, 2]
        for level in levels:
            pandas_result = pser.unstack(level=level)
            pandas_on_spark_result = psser.unstack(level=level).sort_index()
            self.assert_eq(pandas_result, pandas_on_spark_result)
            self.assert_eq(pandas_result.index.names, pandas_on_spark_result.index.names)
            self.assert_eq(pandas_result.columns.names, pandas_on_spark_result.columns.names)

        # non-numeric datatypes
        pser = pd.Series(
            list("abcd"), index=pd.MultiIndex.from_product([["one", "two"], ["a", "b"]])
        )
        psser = ps.from_pandas(pser)

        levels = [-2, -1, 0, 1]
        for level in levels:
            pandas_result = pser.unstack(level=level)
            pandas_on_spark_result = psser.unstack(level=level).sort_index()
            self.assert_eq(pandas_result, pandas_on_spark_result)
            self.assert_eq(pandas_result.index.names, pandas_on_spark_result.index.names)
            self.assert_eq(pandas_result.columns.names, pandas_on_spark_result.columns.names)

        # Exceeding the range of level
        self.assertRaises(IndexError, lambda: psser.unstack(level=3))
        self.assertRaises(IndexError, lambda: psser.unstack(level=-4))
        # Only support for MultiIndex
        psser = ps.Series([10, -2, 4, 7])
        self.assertRaises(ValueError, lambda: psser.unstack())

    def test_item(self):
        psser = ps.Series([10, 20])
        self.assertRaises(ValueError, lambda: psser.item())

    def test_filter(self):
        pser = pd.Series([0, 1, 2], index=["one", "two", "three"])
        psser = ps.from_pandas(pser)

        self.assert_eq(pser.filter(items=["one", "three"]), psser.filter(items=["one", "three"]))
        self.assert_eq(pser.filter(regex="e$"), psser.filter(regex="e$"))
        self.assert_eq(pser.filter(like="hre"), psser.filter(like="hre"))

        with self.assertRaisesRegex(ValueError, "Series does not support columns axis."):
            psser.filter(like="hre", axis=1)

        # for MultiIndex
        midx = pd.MultiIndex.from_tuples([("one", "x"), ("two", "y"), ("three", "z")])
        pser = pd.Series([0, 1, 2], index=midx)
        psser = ps.from_pandas(pser)

        self.assert_eq(
            pser.filter(items=[("one", "x"), ("three", "z")]),
            psser.filter(items=[("one", "x"), ("three", "z")]),
        )

        with self.assertRaisesRegex(TypeError, "Unsupported type list"):
            psser.filter(items=[["one", "x"], ("three", "z")])

        with self.assertRaisesRegex(ValueError, "The item should not be empty."):
            psser.filter(items=[(), ("three", "z")])

    def test_abs(self):
        pser = pd.Series([-2, -1, 0, 1])
        psser = ps.from_pandas(pser)

        self.assert_eq(abs(psser), abs(pser))
        self.assert_eq(np.abs(psser), np.abs(pser))

    def test_bfill(self):
        pdf = pd.DataFrame({"x": [np.nan, 2, 3, 4, np.nan, 6], "y": [np.nan, 2, 3, 4, np.nan, 6]})
        psdf = ps.from_pandas(pdf)

        pser = pdf.x
        psser = psdf.x

        self.assert_eq(psser.bfill(), pser.bfill())
        self.assert_eq(psser.bfill()[0], pser.bfill()[0])

        psser.bfill(inplace=True)
        pser.bfill(inplace=True)
        self.assert_eq(psser, pser)
        self.assert_eq(psser[0], pser[0])
        self.assert_eq(psdf, pdf)

    def test_ffill(self):
        pdf = pd.DataFrame({"x": [np.nan, 2, 3, 4, np.nan, 6], "y": [np.nan, 2, 3, 4, np.nan, 6]})
        psdf = ps.from_pandas(pdf)

        pser = pdf.x
        psser = psdf.x

        self.assert_eq(psser.ffill(), pser.ffill())
        self.assert_eq(psser.ffill()[4], pser.ffill()[4])

        psser.ffill(inplace=True)
        pser.ffill(inplace=True)
        self.assert_eq(psser, pser)
        self.assert_eq(psser[4], pser[4])
        self.assert_eq(psdf, pdf)

    def test_iteritems(self):
        pser = pd.Series(["A", "B", "C"])
        psser = ps.from_pandas(pser)

        for (p_name, p_items), (k_name, k_items) in zip(pser.iteritems(), psser.iteritems()):
            self.assert_eq(p_name, k_name)
            self.assert_eq(p_items, k_items)

    def test_droplevel(self):
        pser = pd.Series(
            [1, 2, 3],
            index=pd.MultiIndex.from_tuples(
                [("x", "a", "q"), ("x", "b", "w"), ("y", "c", "e")],
                names=["level_1", "level_2", "level_3"],
            ),
        )
        psser = ps.from_pandas(pser)

        self.assert_eq(pser.droplevel(0), psser.droplevel(0))
        self.assert_eq(pser.droplevel("level_1"), psser.droplevel("level_1"))
        self.assert_eq(pser.droplevel(-1), psser.droplevel(-1))
        self.assert_eq(pser.droplevel([0]), psser.droplevel([0]))
        self.assert_eq(pser.droplevel(["level_1"]), psser.droplevel(["level_1"]))
        self.assert_eq(pser.droplevel((0,)), psser.droplevel((0,)))
        self.assert_eq(pser.droplevel(("level_1",)), psser.droplevel(("level_1",)))
        self.assert_eq(pser.droplevel([0, 2]), psser.droplevel([0, 2]))
        self.assert_eq(
            pser.droplevel(["level_1", "level_3"]), psser.droplevel(["level_1", "level_3"])
        )
        self.assert_eq(pser.droplevel((1, 2)), psser.droplevel((1, 2)))
        self.assert_eq(
            pser.droplevel(("level_2", "level_3")), psser.droplevel(("level_2", "level_3"))
        )

        with self.assertRaisesRegex(KeyError, "Level {0, 1, 2} not found"):
            psser.droplevel({0, 1, 2})
        with self.assertRaisesRegex(KeyError, "Level level_100 not found"):
            psser.droplevel(["level_1", "level_100"])
        with self.assertRaisesRegex(IndexError, "Too many levels: Index has only 3 levels, not 11"):
            psser.droplevel(10)
        with self.assertRaisesRegex(
            IndexError,
            "Too many levels: Index has only 3 levels, -10 is not a valid level number",
        ):
            psser.droplevel(-10)
        with self.assertRaisesRegex(
            ValueError,
            "Cannot remove 3 levels from an index with 3 levels: "
            "at least one level must be left.",
        ):
            psser.droplevel([0, 1, 2])
        with self.assertRaisesRegex(
            ValueError,
            "Cannot remove 5 levels from an index with 3 levels: "
            "at least one level must be left.",
        ):
            psser.droplevel([1, 1, 1, 1, 1])

        # Tupled names
        pser.index.names = [("a", "1"), ("b", "2"), ("c", "3")]
        psser = ps.from_pandas(pser)

        self.assert_eq(
            pser.droplevel([("a", "1"), ("c", "3")]), psser.droplevel([("a", "1"), ("c", "3")])
        )

    def test_dot(self):
        pdf = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        psdf = ps.from_pandas(pdf)

        self.assert_eq((psdf["b"] * 10).dot(psdf["a"]), (pdf["b"] * 10).dot(pdf["a"]))
        self.assert_eq((psdf["b"] * 10).dot(psdf), (pdf["b"] * 10).dot(pdf))
        self.assert_eq((psdf["b"] * 10).dot(psdf + 1), (pdf["b"] * 10).dot(pdf + 1))

        psdf_other = ps.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}, index=["x", "y", "z"])
        with self.assertRaisesRegex(ValueError, "matrices are not aligned"):
            psdf["b"].dot(psdf_other)

    def test_tail(self):
        pser = pd.Series(range(1000), name="Koalas")
        psser = ps.from_pandas(pser)

        self.assert_eq(pser.tail(), psser.tail())
        self.assert_eq(pser.tail(10), psser.tail(10))
        self.assert_eq(pser.tail(-990), psser.tail(-990))
        self.assert_eq(pser.tail(0), psser.tail(0))
        self.assert_eq(pser.tail(1001), psser.tail(1001))
        self.assert_eq(pser.tail(-1001), psser.tail(-1001))
        self.assert_eq((pser + 1).tail(), (psser + 1).tail())
        self.assert_eq((pser + 1).tail(10), (psser + 1).tail(10))
        self.assert_eq((pser + 1).tail(-990), (psser + 1).tail(-990))
        self.assert_eq((pser + 1).tail(0), (psser + 1).tail(0))
        self.assert_eq((pser + 1).tail(1001), (psser + 1).tail(1001))
        self.assert_eq((pser + 1).tail(-1001), (psser + 1).tail(-1001))
        with self.assertRaisesRegex(TypeError, "bad operand type for unary -: 'str'"):
            psser.tail("10")

    def test_product(self):
        pser = pd.Series([10, 20, 30, 40, 50])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.prod(), psser.prod())

        # Containing NA values
        pser = pd.Series([10, np.nan, 30, np.nan, 50])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.prod(), psser.prod(), almost=True)

        # All-NA values
        pser = pd.Series([np.nan, np.nan, np.nan])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.prod(), psser.prod())

        # Empty Series
        pser = pd.Series([])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.prod(), psser.prod())

        # Boolean Series
        pser = pd.Series([True, True, True])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.prod(), psser.prod())

        pser = pd.Series([False, False, False])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.prod(), psser.prod())

        pser = pd.Series([True, False, True])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.prod(), psser.prod())

        # With `min_count` parameter
        pser = pd.Series([10, 20, 30, 40, 50])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.prod(min_count=5), psser.prod(min_count=5))
        self.assert_eq(pser.prod(min_count=6), psser.prod(min_count=6))

        pser = pd.Series([10, np.nan, 30, np.nan, 50])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.prod(min_count=3), psser.prod(min_count=3), almost=True)
        self.assert_eq(pser.prod(min_count=4), psser.prod(min_count=4))

        pser = pd.Series([np.nan, np.nan, np.nan])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.prod(min_count=1), psser.prod(min_count=1))

        pser = pd.Series([])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.prod(min_count=1), psser.prod(min_count=1))

        with self.assertRaisesRegex(TypeError, "Could not convert object \\(string\\) to numeric"):
            ps.Series(["a", "b", "c"]).prod()
        with self.assertRaisesRegex(
            TypeError, "Could not convert datetime64\\[ns\\] \\(timestamp.*\\) to numeric"
        ):
            ps.Series([pd.Timestamp("2016-01-01") for _ in range(3)]).prod()
        with self.assertRaisesRegex(NotImplementedError, "Series does not support columns axis."):
            psser.prod(axis=1)

    def test_hasnans(self):
        # BooleanType
        pser = pd.Series([True, False, True, True])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.hasnans, psser.hasnans)

        pser = pd.Series([True, False, np.nan, True])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.hasnans, psser.hasnans)

        # TimestampType
        pser = pd.Series([pd.Timestamp("2020-07-30") for _ in range(3)])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.hasnans, psser.hasnans)

        pser = pd.Series([pd.Timestamp("2020-07-30"), np.nan, pd.Timestamp("2020-07-30")])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.hasnans, psser.hasnans)

        # DecimalType
        pser = pd.Series([Decimal("0.1"), Decimal("NaN")])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.hasnans, psser.hasnans)

        # empty
        pser = pd.Series([])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.hasnans, psser.hasnans)

    def test_last_valid_index(self):
        pser = pd.Series([250, 1.5, 320, 1, 0.3, None, None, None, None])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.last_valid_index(), psser.last_valid_index())

        # MultiIndex columns
        midx = pd.MultiIndex(
            [["lama", "cow", "falcon"], ["speed", "weight", "length"]],
            [[0, 0, 0, 1, 1, 1, 2, 2, 2], [0, 1, 2, 0, 1, 2, 0, 1, 2]],
        )
        pser.index = midx
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.last_valid_index(), psser.last_valid_index())

        # Empty Series
        pser = pd.Series([])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.last_valid_index(), psser.last_valid_index())

    def test_first_valid_index(self):
        # Empty Series
        pser = pd.Series([])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.first_valid_index(), psser.first_valid_index())

    def test_factorize(self):
        pser = pd.Series(["a", "b", "a", "b"])
        psser = ps.from_pandas(pser)
        pcodes, puniques = pser.factorize(sort=True)
        kcodes, kuniques = psser.factorize()
        self.assert_eq(pcodes.tolist(), kcodes.to_list())
        self.assert_eq(puniques, kuniques)

        pser = pd.Series([5, 1, 5, 1])
        psser = ps.from_pandas(pser)
        pcodes, puniques = (pser + 1).factorize(sort=True)
        kcodes, kuniques = (psser + 1).factorize()
        self.assert_eq(pcodes.tolist(), kcodes.to_list())
        self.assert_eq(puniques, kuniques)

        pser = pd.Series(["a", "b", "a", "b"], name="ser", index=["w", "x", "y", "z"])
        psser = ps.from_pandas(pser)
        pcodes, puniques = pser.factorize(sort=True)
        kcodes, kuniques = psser.factorize()
        self.assert_eq(pcodes.tolist(), kcodes.to_list())
        self.assert_eq(puniques, kuniques)

        pser = pd.Series(
            ["a", "b", "a", "b"], index=pd.MultiIndex.from_arrays([[4, 3, 2, 1], [1, 2, 3, 4]])
        )
        psser = ps.from_pandas(pser)
        pcodes, puniques = pser.factorize(sort=True)
        kcodes, kuniques = psser.factorize()
        self.assert_eq(pcodes.tolist(), kcodes.to_list())
        self.assert_eq(puniques, kuniques)

        #
        # Deals with None and np.nan
        #
        pser = pd.Series(["a", "b", "a", np.nan])
        psser = ps.from_pandas(pser)
        pcodes, puniques = pser.factorize(sort=True)
        kcodes, kuniques = psser.factorize()
        self.assert_eq(pcodes.tolist(), kcodes.to_list())
        self.assert_eq(puniques, kuniques)

        pser = pd.Series([1, None, 3, 2, 1])
        psser = ps.from_pandas(pser)
        pcodes, puniques = pser.factorize(sort=True)
        kcodes, kuniques = psser.factorize()
        self.assert_eq(pcodes.tolist(), kcodes.to_list())
        self.assert_eq(puniques, kuniques)

        pser = pd.Series(["a", None, "a"])
        psser = ps.from_pandas(pser)
        pcodes, puniques = pser.factorize(sort=True)
        kcodes, kuniques = psser.factorize()
        self.assert_eq(pcodes.tolist(), kcodes.to_list())
        self.assert_eq(puniques, kuniques)

        pser = pd.Series([None, np.nan])
        psser = ps.from_pandas(pser)
        pcodes, puniques = pser.factorize()
        kcodes, kuniques = psser.factorize()
        self.assert_eq(pcodes, kcodes.to_list())
        # pandas: Float64Index([], dtype='float64')
        self.assert_eq(pd.Index([]), kuniques)

        pser = pd.Series([np.nan, np.nan])
        psser = ps.from_pandas(pser)
        pcodes, puniques = pser.factorize()
        kcodes, kuniques = psser.factorize()
        self.assert_eq(pcodes, kcodes.to_list())
        # pandas: Float64Index([], dtype='float64')
        self.assert_eq(pd.Index([]), kuniques)

        #
        # Deals with na_sentinel
        #
        # pandas >= 1.1.2 support na_sentinel=None
        #
        pd_below_1_1_2 = LooseVersion(pd.__version__) < LooseVersion("1.1.2")

        pser = pd.Series(["a", "b", "a", np.nan, None])
        psser = ps.from_pandas(pser)

        pcodes, puniques = pser.factorize(sort=True, na_sentinel=-2)
        kcodes, kuniques = psser.factorize(na_sentinel=-2)
        self.assert_eq(pcodes.tolist(), kcodes.to_list())
        self.assert_eq(puniques, kuniques)

        pcodes, puniques = pser.factorize(sort=True, na_sentinel=2)
        kcodes, kuniques = psser.factorize(na_sentinel=2)
        self.assert_eq(pcodes.tolist(), kcodes.to_list())
        self.assert_eq(puniques, kuniques)

        if not pd_below_1_1_2:
            pcodes, puniques = pser.factorize(sort=True, na_sentinel=None)
            kcodes, kuniques = psser.factorize(na_sentinel=None)
            self.assert_eq(pcodes.tolist(), kcodes.to_list())
            # puniques is Index(['a', 'b', nan], dtype='object')
            self.assert_eq(ps.Index(["a", "b", None]), kuniques)

            psser = ps.Series([1, 2, np.nan, 4, 5])  # Arrow takes np.nan as null
            psser.loc[3] = np.nan  # Spark takes np.nan as NaN
            kcodes, kuniques = psser.factorize(na_sentinel=None)
            pcodes, puniques = psser._to_pandas().factorize(sort=True, na_sentinel=None)
            self.assert_eq(pcodes.tolist(), kcodes.to_list())
            self.assert_eq(puniques, kuniques)

    def test_pad(self):
        pdf = pd.DataFrame({"x": [np.nan, 2, 3, 4, np.nan, 6]})
        psdf = ps.from_pandas(pdf)
        pser, psser = pdf.x, psdf.x

        if LooseVersion(pd.__version__) >= LooseVersion("1.1"):
            self.assert_eq(pser.pad(), psser.pad())

            # Test `inplace=True`
            pser.pad(inplace=True)
            psser.pad(inplace=True)
            self.assert_eq(pser, psser)
            self.assert_eq(pdf, psdf)
        else:
            expected = ps.Series([np.nan, 2, 3, 4, 4, 6], name="x")
            self.assert_eq(expected, psser.pad())

            # Test `inplace=True`
            psser.pad(inplace=True)
            self.assert_eq(expected, psser)

    def test_explode(self):
        pser = pd.Series([[1, 2, 3], [], None, [3, 4]])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.explode(), psser.explode(), almost=True)

        # MultiIndex
        pser.index = pd.MultiIndex.from_tuples([("a", "w"), ("b", "x"), ("c", "y"), ("d", "z")])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.explode(), psser.explode(), almost=True)

        # non-array type Series
        pser = pd.Series([1, 2, 3, 4])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.explode(), psser.explode())

    def test_argsort(self):
        # Without null values
        pser = pd.Series([0, -100, 50, 100, 20], index=["A", "B", "C", "D", "E"])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.argsort().sort_index(), psser.argsort().sort_index())
        self.assert_eq((-pser).argsort().sort_index(), (-psser).argsort().sort_index())

        # MultiIndex
        pser.index = pd.MultiIndex.from_tuples(
            [("a", "v"), ("b", "w"), ("c", "x"), ("d", "y"), ("e", "z")]
        )
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.argsort().sort_index(), psser.argsort().sort_index())
        self.assert_eq((-pser).argsort().sort_index(), (-psser).argsort().sort_index())

        # With name
        pser.name = "Koalas"
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.argsort().sort_index(), psser.argsort().sort_index())
        self.assert_eq((-pser).argsort().sort_index(), (-psser).argsort().sort_index())

        # Series from Index
        pidx = pd.Index([4.0, -6.0, 2.0, -100.0, 11.0, 20.0, 1.0, -99.0])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(
            pidx.to_series().argsort().sort_index(), psidx.to_series().argsort().sort_index()
        )
        self.assert_eq(
            (-pidx.to_series()).argsort().sort_index(), (-psidx.to_series()).argsort().sort_index()
        )

        # Series from Index with name
        pidx.name = "Koalas"
        psidx = ps.from_pandas(pidx)
        self.assert_eq(
            pidx.to_series().argsort().sort_index(), psidx.to_series().argsort().sort_index()
        )
        self.assert_eq(
            (-pidx.to_series()).argsort().sort_index(), (-psidx.to_series()).argsort().sort_index()
        )

        # Series from DataFrame
        pdf = pd.DataFrame({"A": [4.0, -6.0, 2.0, np.nan, -100.0, 11.0, 20.0, np.nan, 1.0, -99.0]})
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.A.argsort().sort_index(), psdf.A.argsort().sort_index())
        self.assert_eq((-pdf.A).argsort().sort_index(), (-psdf.A).argsort().sort_index())

        # With null values
        pser = pd.Series([0, -100, np.nan, 100, np.nan], index=["A", "B", "C", "D", "E"])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.argsort().sort_index(), psser.argsort().sort_index())
        self.assert_eq((-pser).argsort().sort_index(), (-psser).argsort().sort_index())

        # MultiIndex with null values
        pser.index = pd.MultiIndex.from_tuples(
            [("a", "v"), ("b", "w"), ("c", "x"), ("d", "y"), ("e", "z")]
        )
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.argsort().sort_index(), psser.argsort().sort_index())
        self.assert_eq((-pser).argsort().sort_index(), (-psser).argsort().sort_index())

        # With name with null values
        pser.name = "Koalas"
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.argsort().sort_index(), psser.argsort().sort_index())
        self.assert_eq((-pser).argsort().sort_index(), (-psser).argsort().sort_index())

        # Series from Index with null values
        pidx = pd.Index([4.0, -6.0, 2.0, np.nan, -100.0, 11.0, 20.0, np.nan, 1.0, -99.0])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(
            pidx.to_series().argsort().sort_index(), psidx.to_series().argsort().sort_index()
        )
        self.assert_eq(
            (-pidx.to_series()).argsort().sort_index(), (-psidx.to_series()).argsort().sort_index()
        )

        # Series from Index with name with null values
        pidx.name = "Koalas"
        psidx = ps.from_pandas(pidx)
        self.assert_eq(
            pidx.to_series().argsort().sort_index(), psidx.to_series().argsort().sort_index()
        )
        self.assert_eq(
            (-pidx.to_series()).argsort().sort_index(), (-psidx.to_series()).argsort().sort_index()
        )

        # Series from DataFrame with null values
        pdf = pd.DataFrame({"A": [4.0, -6.0, 2.0, np.nan, -100.0, 11.0, 20.0, np.nan, 1.0, -99.0]})
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.A.argsort().sort_index(), psdf.A.argsort().sort_index())
        self.assert_eq((-pdf.A).argsort().sort_index(), (-psdf.A).argsort().sort_index())

    def test_argmin_argmax(self):
        pser = pd.Series(
            {
                "Corn Flakes": 100.0,
                "Almond Delight": 110.0,
                "Cinnamon Toast Crunch": 120.0,
                "Cocoa Puff": 110.0,
                "Expensive Flakes": 120.0,
                "Cheap Flakes": 100.0,
            },
            name="Koalas",
        )
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.argmin(), psser.argmin())
        self.assert_eq(pser.argmax(), psser.argmax())
        self.assert_eq(pser.argmin(skipna=False), psser.argmin(skipna=False))
        self.assert_eq(pser.argmax(skipna=False), psser.argmax(skipna=False))
        self.assert_eq(pser.argmax(skipna=False), psser.argmax(skipna=False))
        self.assert_eq((pser + 1).argmax(skipna=False), (psser + 1).argmax(skipna=False))
        self.assert_eq(pser.argmin(skipna=False), psser.argmin(skipna=False))
        self.assert_eq((pser + 1).argmin(skipna=False), (psser + 1).argmin(skipna=False))

        # MultiIndex
        pser.index = pd.MultiIndex.from_tuples(
            [("a", "t"), ("b", "u"), ("c", "v"), ("d", "w"), ("e", "x"), ("f", "u")]
        )
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.argmin(), psser.argmin())
        self.assert_eq(pser.argmax(), psser.argmax())
        self.assert_eq(pser.argmax(skipna=False), psser.argmax(skipna=False))

        pser2 = pd.Series([np.NaN, 1.0, 2.0, np.NaN])
        psser2 = ps.from_pandas(pser2)
        self.assert_eq(pser2.argmin(), psser2.argmin())
        self.assert_eq(pser2.argmax(), psser2.argmax())
        self.assert_eq(pser2.argmin(skipna=False), psser2.argmin(skipna=False))
        self.assert_eq(pser2.argmax(skipna=False), psser2.argmax(skipna=False))

        # Null Series
        self.assert_eq(pd.Series([np.nan]).argmin(), ps.Series([np.nan]).argmin())
        self.assert_eq(pd.Series([np.nan]).argmax(), ps.Series([np.nan]).argmax())
        self.assert_eq(
            pd.Series([np.nan]).argmax(skipna=False), ps.Series([np.nan]).argmax(skipna=False)
        )

        with self.assertRaisesRegex(ValueError, "attempt to get argmin of an empty sequence"):
            ps.Series([]).argmin()
        with self.assertRaisesRegex(ValueError, "attempt to get argmax of an empty sequence"):
            ps.Series([]).argmax()
        with self.assertRaisesRegex(ValueError, "axis can only be 0 or 'index'"):
            psser.argmax(axis=1)
        with self.assertRaisesRegex(ValueError, "axis can only be 0 or 'index'"):
            psser.argmin(axis=1)

    def test_backfill(self):
        pdf = pd.DataFrame({"x": [np.nan, 2, 3, 4, np.nan, 6]})
        psdf = ps.from_pandas(pdf)
        pser, psser = pdf.x, psdf.x

        if LooseVersion(pd.__version__) >= LooseVersion("1.1"):
            self.assert_eq(pser.backfill(), psser.backfill())

            # Test `inplace=True`
            pser.backfill(inplace=True)
            psser.backfill(inplace=True)
            self.assert_eq(pser, psser)
            self.assert_eq(pdf, psdf)
        else:
            expected = ps.Series([2.0, 2.0, 3.0, 4.0, 6.0, 6.0], name="x")
            self.assert_eq(expected, psser.backfill())

            # Test `inplace=True`
            psser.backfill(inplace=True)
            self.assert_eq(expected, psser)

    def test_searchsorted(self):
        pser1 = pd.Series([1, 2, 2, 3])

        index2 = pd.date_range("2018-04-09", periods=4, freq="2D")
        pser2 = pd.Series([1, 2, 3, 4], index=index2)

        index3 = pd.MultiIndex.from_tuples(
            [("A", "B"), ("C", "D"), ("E", "F")], names=["index1", "index2"]
        )
        pser3 = pd.Series([1.0, 2.0, 3.0], index=index3, name="name")

        pser4 = pd.Series([])

        for pser in [pser1, pser2, pser3, pser4]:
            psser = ps.from_pandas(pser)
            for value in [0.5, 1, 2, 3.0, 4, 5]:
                for side in ["left", "right"]:
                    self.assert_eq(
                        pser.searchsorted(value, side=side),
                        psser.searchsorted(value, side=side),
                    )

        with self.assertRaisesRegex(ValueError, "Invalid side"):
            ps.from_pandas(pser1).searchsorted(1.1, side=[1, 2])
        with self.assertRaisesRegex(ValueError, "Invalid side"):
            ps.from_pandas(pser1).searchsorted(1.1, side="middle")

    def test_align(self):
        pdf = pd.DataFrame({"a": [1, 2, 3], "b": ["a", "b", "c"]})
        psdf = ps.from_pandas(pdf)

        for join in ["outer", "inner", "left", "right"]:
            for axis in [None, 0]:
                psser_l, psser_r = psdf.a.align(psdf.b, join=join, axis=axis)
                pser_l, pser_r = pdf.a.align(pdf.b, join=join, axis=axis)
                self.assert_eq(psser_l, pser_l)
                self.assert_eq(psser_r, pser_r)

                psser_l, psdf_r = psdf.b.align(psdf[["b", "a"]], join=join, axis=axis)
                pser_l, pdf_r = pdf.b.align(pdf[["b", "a"]], join=join, axis=axis)
                self.assert_eq(psser_l, pser_l)
                self.assert_eq(psdf_r, pdf_r)

        self.assertRaises(ValueError, lambda: psdf.a.align(psdf.b, axis=1))

    def test_pow_and_rpow(self):
        pser = pd.Series([1, 2, np.nan])
        psser = ps.from_pandas(pser)

        self.assert_eq(pser.pow(np.nan), psser.pow(np.nan))
        self.assert_eq(pser**np.nan, psser**np.nan)
        self.assert_eq(pser.rpow(np.nan), psser.rpow(np.nan))
        self.assert_eq(1**pser, 1**psser)

    def test_between(self):
        pser = pd.Series([np.nan, 1, 2, 3, 4])
        psser = ps.from_pandas(pser)
        self.assert_eq(psser.between(1, 4), pser.between(1, 4))
        self.assert_eq(psser.between(1, 4, inclusive="both"), pser.between(1, 4, inclusive="both"))
        self.assert_eq(
            psser.between(1, 4, inclusive="neither"), pser.between(1, 4, inclusive="neither")
        )
        self.assert_eq(psser.between(1, 4, inclusive="left"), pser.between(1, 4, inclusive="left"))
        self.assert_eq(
            psser.between(1, 4, inclusive="right"), pser.between(1, 4, inclusive="right")
        )
        expected_err_msg = (
            "Inclusive has to be either string of 'both'," "'left', 'right', or 'neither'"
        )
        with self.assertRaisesRegex(ValueError, expected_err_msg):
            psser.between(1, 4, inclusive="middle")

        # Test for backward compatibility
        self.assert_eq(psser.between(1, 4, inclusive=True), pser.between(1, 4, inclusive=True))
        self.assert_eq(psser.between(1, 4, inclusive=False), pser.between(1, 4, inclusive=False))
        with self.assertWarns(FutureWarning):
            psser.between(1, 4, inclusive=True)

    def test_between_time(self):
        idx = pd.date_range("2018-04-09", periods=4, freq="1D20min")
        pser = pd.Series([1, 2, 3, 4], index=idx)
        psser = ps.from_pandas(pser)
        self.assert_eq(
            pser.between_time("0:15", "0:45").sort_index(),
            psser.between_time("0:15", "0:45").sort_index(),
        )

        pser.index.name = "ts"
        psser = ps.from_pandas(pser)
        self.assert_eq(
            pser.between_time("0:15", "0:45").sort_index(),
            psser.between_time("0:15", "0:45").sort_index(),
        )

        pser.index.name = "index"
        psser = ps.from_pandas(pser)
        self.assert_eq(
            pser.between_time("0:15", "0:45").sort_index(),
            psser.between_time("0:15", "0:45").sort_index(),
        )

    def test_at_time(self):
        idx = pd.date_range("2018-04-09", periods=4, freq="1D20min")
        pser = pd.Series([1, 2, 3, 4], index=idx)
        psser = ps.from_pandas(pser)
        self.assert_eq(
            pser.at_time("0:20").sort_index(),
            psser.at_time("0:20").sort_index(),
        )

        pser.index.name = "ts"
        psser = ps.from_pandas(pser)
        self.assert_eq(
            pser.at_time("0:20").sort_index(),
            psser.at_time("0:20").sort_index(),
        )

        pser.index.name = "index"
        psser = ps.from_pandas(pser)
        self.assert_eq(
            pser.at_time("0:20").sort_index(),
            psser.at_time("0:20").sort_index(),
        )

    def test_apply(self):
        psser = self.psser

        def udf(col) -> ps.Series[int]:
            return col + 10

        with self.assertRaisesRegex(
            ValueError,
            r"Expected the return type of this function to be of scalar type, "
            r"but found type SeriesType\[LongType\(\)\]",
        ):
            psser.apply(udf)

    def test_combine_first(self):
        pdf = pd.DataFrame(
            {
                "A": {"falcon": 330.0, "eagle": 160.0},
                "B": {"falcon": 345.0, "eagle": 200.0, "duck": 30.0},
            }
        )
        pser1, pser2 = pdf.A, pdf.B
        psdf = ps.from_pandas(pdf)
        psser1, psser2 = psdf.A, psdf.B

        self.assert_eq(psser1.combine_first(psser2), pser1.combine_first(pser2))

        psser1.name = pser1.name = ("X", "A")
        psser2.name = pser2.name = ("Y", "B")

        self.assert_eq(psser1.combine_first(psser2), pser1.combine_first(pser2))

    def test_autocorr(self):
        pdf = pd.DataFrame({"s1": [0.90010907, 0.13484424, 0.62036035]})
        self._test_autocorr(pdf)

        pdf = pd.DataFrame({"s1": [0.90010907, np.nan, 0.13484424, 0.62036035]})
        self._test_autocorr(pdf)

        pdf = pd.DataFrame({"s1": [0.2, 0.0, 0.6, 0.2, np.nan, 0.5, 0.6]})
        self._test_autocorr(pdf)

        psser = ps.from_pandas(pdf["s1"])
        with self.assertRaisesRegex(TypeError, r"lag should be an int; however, got"):
            psser.autocorr(1.0)

    def _test_autocorr(self, pdf):
        psdf = ps.from_pandas(pdf)
        for lag in range(-10, 10):
            p_autocorr = pdf["s1"].autocorr(lag)
            ps_autocorr = psdf["s1"].autocorr(lag)
            self.assert_eq(p_autocorr, ps_autocorr, almost=True)

    def test_cov(self):
        pdf = pd.DataFrame(
            {
                "s1": ["a", "b", "c"],
                "s2": [0.12528585, 0.26962463, 0.51111198],
            },
            index=[0, 1, 2],
        )
        psdf = ps.from_pandas(pdf)
        with self.assertRaisesRegex(TypeError, "unsupported dtype: object"):
            psdf["s1"].cov(psdf["s2"])
        with self.assertRaisesRegex(TypeError, "unsupported dtype: object"):
            psdf["s2"].cov(psdf["s1"])
        with self.assertRaisesRegex(TypeError, "ddof must be integer"):
            psdf["s2"].cov(psdf["s2"], ddof="ddof")

        pdf = pd.DataFrame(
            {
                "s1": [0.90010907, 0.13484424, 0.62036035],
                "s2": [0.12528585, 0.26962463, 0.51111198],
            },
            index=[0, 1, 2],
        )
        self._test_cov(pdf)

        pdf = pd.DataFrame(
            {
                "s1": [0.90010907, np.nan, 0.13484424, 0.62036035],
                "s2": [0.12528585, 0.81131178, 0.26962463, 0.51111198],
            },
            index=[0, 1, 2, 3],
        )
        self._test_cov(pdf)

    def _test_cov(self, pdf):
        psdf = ps.from_pandas(pdf)

        self.assert_eq(pdf["s1"].cov(pdf["s2"]), psdf["s1"].cov(psdf["s2"]), almost=True)
        self.assert_eq(
            pdf["s1"].cov(pdf["s2"], ddof=2), psdf["s1"].cov(psdf["s2"], ddof=2), almost=True
        )

        self.assert_eq(
            pdf["s1"].cov(pdf["s2"], min_periods=3),
            psdf["s1"].cov(psdf["s2"], min_periods=3),
            almost=True,
        )
        self.assert_eq(
            pdf["s1"].cov(pdf["s2"], min_periods=3, ddof=-1),
            psdf["s1"].cov(psdf["s2"], min_periods=3, ddof=-1),
            almost=True,
        )

        self.assert_eq(
            pdf["s1"].cov(pdf["s2"], min_periods=4),
            psdf["s1"].cov(psdf["s2"], min_periods=4),
            almost=True,
        )
        self.assert_eq(
            pdf["s1"].cov(pdf["s2"], min_periods=4, ddof=3),
            psdf["s1"].cov(psdf["s2"], min_periods=4, ddof=3),
            almost=True,
        )

    def test_eq(self):
        pser = pd.Series([1, 2, 3, 4, 5, 6], name="x")
        psser = ps.from_pandas(pser)

        # other = Series
        self.assert_eq(pser.eq(pser), psser.eq(psser))
        self.assert_eq(pser == pser, psser == psser)

        # other = dict
        other = {1: None, 2: None, 3: None, 4: None, np.nan: None, 6: None}
        self.assert_eq(pser.eq(other), psser.eq(other))
        self.assert_eq(pser == other, psser == other)

        # other = set
        other = {1, 2, 3, 4, np.nan, 6}
        self.assert_eq(pser.eq(other), psser.eq(other))
        self.assert_eq(pser == other, psser == other)

        # other = list
        other = [np.nan, 1, 3, 4, np.nan, 6]
        if LooseVersion(pd.__version__) >= LooseVersion("1.2"):
            self.assert_eq(pser.eq(other), psser.eq(other).sort_index())
            self.assert_eq(pser == other, (psser == other).sort_index())
        else:
            self.assert_eq(pser.eq(other).rename("x"), psser.eq(other).sort_index())
            self.assert_eq((pser == other).rename("x"), (psser == other).sort_index())

        # other = tuple
        other = (np.nan, 1, 3, 4, np.nan, 6)
        if LooseVersion(pd.__version__) >= LooseVersion("1.2"):
            self.assert_eq(pser.eq(other), psser.eq(other).sort_index())
            self.assert_eq(pser == other, (psser == other).sort_index())
        else:
            self.assert_eq(pser.eq(other).rename("x"), psser.eq(other).sort_index())
            self.assert_eq((pser == other).rename("x"), (psser == other).sort_index())

        # other = list with the different length
        other = [np.nan, 1, 3, 4, np.nan]
        with self.assertRaisesRegex(ValueError, "Lengths must be equal"):
            psser.eq(other)
        with self.assertRaisesRegex(ValueError, "Lengths must be equal"):
            psser == other

        # other = tuple with the different length
        other = (np.nan, 1, 3, 4, np.nan)
        with self.assertRaisesRegex(ValueError, "Lengths must be equal"):
            psser.eq(other)
        with self.assertRaisesRegex(ValueError, "Lengths must be equal"):
            psser == other

    def test_transform(self):
        psser = self.psser
        with self.assertRaisesRegex(
            NotImplementedError, 'axis should be either 0 or "index" currently.'
        ):
            psser.transform(lambda x: x + 1, axis=1)

    def test_series_stat_fail(self):
        with self.assertRaisesRegex(TypeError, "Could not convert object"):
            ps.Series(["a", "b", "c"]).mean()
        with self.assertRaisesRegex(TypeError, "Could not convert object"):
            ps.Series(["a", "b", "c"]).skew()
        with self.assertRaisesRegex(TypeError, "Could not convert object"):
            ps.Series(["a", "b", "c"]).kurtosis()
        with self.assertRaisesRegex(TypeError, "Could not convert object"):
            ps.Series(["a", "b", "c"]).std()
        with self.assertRaisesRegex(TypeError, "Could not convert object"):
            ps.Series(["a", "b", "c"]).var()
        with self.assertRaisesRegex(TypeError, "Could not convert object"):
            ps.Series(["a", "b", "c"]).median()
        with self.assertRaisesRegex(TypeError, "Could not convert object"):
            ps.Series(["a", "b", "c"]).sem()


if __name__ == "__main__":
    from pyspark.pandas.tests.test_series import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
