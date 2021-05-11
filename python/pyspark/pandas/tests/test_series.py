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

import numpy as np
import pandas as pd
from pyspark.ml.linalg import SparseVector
from pyspark.sql import functions as F

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
    def kser(self):
        return ps.from_pandas(self.pser)

    def test_series_ops(self):
        pser = self.pser
        kser = self.kser

        self.assert_eq(kser + 1, pser + 1)
        self.assert_eq(1 + kser, 1 + pser)
        self.assert_eq(kser + 1 + 10 * kser, pser + 1 + 10 * pser)
        self.assert_eq(kser + 1 + 10 * kser.index, pser + 1 + 10 * pser.index)
        self.assert_eq(kser.index + 1 + 10 * kser, pser.index + 1 + 10 * pser)

    def test_series_tuple_name(self):
        pser = self.pser
        pser.name = ("x", "a")

        kser = ps.from_pandas(pser)

        self.assert_eq(kser, pser)
        self.assert_eq(kser.name, pser.name)

        pser.name = ("y", "z")
        kser.name = ("y", "z")

        self.assert_eq(kser, pser)
        self.assert_eq(kser.name, pser.name)

    def test_repr_cache_invalidation(self):
        # If there is any cache, inplace operations should invalidate it.
        s = ps.range(10)["id"]
        s.__repr__()
        s.rename("a", inplace=True)
        self.assertEqual(s.__repr__(), s.rename("a").__repr__())

    def _check_extension(self, kser, pser):
        if LooseVersion("1.1") <= LooseVersion(pd.__version__) < LooseVersion("1.2.2"):
            self.assert_eq(kser, pser, check_exact=False)
            self.assertTrue(isinstance(kser.dtype, extension_dtypes))
        else:
            self.assert_eq(kser, pser)

    @unittest.skipIf(not extension_dtypes_available, "pandas extension dtypes are not available")
    def test_extension_dtypes(self):
        for pser in [
            pd.Series([1, 2, None, 4], dtype="Int8"),
            pd.Series([1, 2, None, 4], dtype="Int16"),
            pd.Series([1, 2, None, 4], dtype="Int32"),
            pd.Series([1, 2, None, 4], dtype="Int64"),
        ]:
            kser = ps.from_pandas(pser)

            self._check_extension(kser, pser)
            self._check_extension(kser + F.lit(1).cast("byte"), pser + 1)
            self._check_extension(kser + kser, pser + pser)

    @unittest.skipIf(
        not extension_object_dtypes_available, "pandas extension object dtypes are not available"
    )
    def test_extension_object_dtypes(self):
        # string
        pser = pd.Series(["a", None, "c", "d"], dtype="string")
        kser = ps.from_pandas(pser)

        self._check_extension(kser, pser)

        # boolean
        pser = pd.Series([True, False, True, None], dtype="boolean")
        kser = ps.from_pandas(pser)

        self._check_extension(kser, pser)
        self._check_extension(kser & kser, pser & pser)
        self._check_extension(kser | kser, pser | pser)

    @unittest.skipIf(
        not extension_float_dtypes_available, "pandas extension float dtypes are not available"
    )
    def test_extension_float_dtypes(self):
        for pser in [
            pd.Series([1.0, 2.0, None, 4.0], dtype="Float32"),
            pd.Series([1.0, 2.0, None, 4.0], dtype="Float64"),
        ]:
            kser = ps.from_pandas(pser)

            self._check_extension(kser, pser)
            self._check_extension(kser + 1, pser + 1)
            self._check_extension(kser + kser, pser + pser)

    def test_empty_series(self):
        pser_a = pd.Series([], dtype="i1")
        pser_b = pd.Series([], dtype="str")

        self.assert_eq(ps.from_pandas(pser_a), pser_a)

        kser_b = ps.from_pandas(pser_b)
        self.assert_eq(kser_b, pser_b)

        with self.sql_conf({SPARK_CONF_ARROW_ENABLED: False}):
            self.assert_eq(ps.from_pandas(pser_a), pser_a)
            self.assert_eq(ps.from_pandas(pser_b), pser_b)

    def test_all_null_series(self):
        pser_a = pd.Series([None, None, None], dtype="float64")
        pser_b = pd.Series([None, None, None], dtype="str")

        self.assert_eq(ps.from_pandas(pser_a), pser_a)

        kser_b = ps.from_pandas(pser_b)
        self.assert_eq(kser_b, pser_b)

        with self.sql_conf({SPARK_CONF_ARROW_ENABLED: False}):
            self.assert_eq(ps.from_pandas(pser_a), pser_a)
            self.assert_eq(ps.from_pandas(pser_b), pser_b)

    def test_head(self):
        kser = self.kser
        pser = self.pser

        self.assert_eq(kser.head(3), pser.head(3))
        self.assert_eq(kser.head(0), pser.head(0))
        self.assert_eq(kser.head(-3), pser.head(-3))
        self.assert_eq(kser.head(-10), pser.head(-10))

    def test_last(self):
        with self.assertRaises(TypeError):
            self.kser.last("1D")

        index = pd.date_range("2018-04-09", periods=4, freq="2D")
        pser = pd.Series([1, 2, 3, 4], index=index)
        kser = ps.from_pandas(pser)
        self.assert_eq(kser.last("1D"), pser.last("1D"))

    def test_first(self):
        with self.assertRaises(TypeError):
            self.kser.first("1D")

        index = pd.date_range("2018-04-09", periods=4, freq="2D")
        pser = pd.Series([1, 2, 3, 4], index=index)
        kser = ps.from_pandas(pser)
        self.assert_eq(kser.first("1D"), pser.first("1D"))

    def test_rename(self):
        pser = pd.Series([1, 2, 3, 4, 5, 6, 7], name="x")
        kser = ps.from_pandas(pser)

        pser.name = "renamed"
        kser.name = "renamed"
        self.assertEqual(kser.name, "renamed")
        self.assert_eq(kser, pser)

        pser.name = None
        kser.name = None
        self.assertEqual(kser.name, None)
        self.assert_eq(kser, pser)

        pidx = pser.index
        kidx = kser.index
        pidx.name = "renamed"
        kidx.name = "renamed"
        self.assertEqual(kidx.name, "renamed")
        self.assert_eq(kidx, pidx)

        expected_error_message = "Series.name must be a hashable type"
        with self.assertRaisesRegex(TypeError, expected_error_message):
            kser.name = ["renamed"]
        with self.assertRaisesRegex(TypeError, expected_error_message):
            kser.name = ["0", "1"]
        with self.assertRaisesRegex(TypeError, expected_error_message):
            ps.Series([1, 2, 3], name=["0", "1"])

    def test_rename_method(self):
        # Series name
        pser = pd.Series([1, 2, 3, 4, 5, 6, 7], name="x")
        kser = ps.from_pandas(pser)

        self.assert_eq(kser.rename("y"), pser.rename("y"))
        self.assertEqual(kser.name, "x")  # no mutation
        self.assert_eq(kser.rename(), pser.rename())

        self.assert_eq((kser.rename("y") + 1).head(), (pser.rename("y") + 1).head())

        kser.rename("z", inplace=True)
        pser.rename("z", inplace=True)
        self.assertEqual(kser.name, "z")
        self.assert_eq(kser, pser)

        expected_error_message = "Series.name must be a hashable type"
        with self.assertRaisesRegex(TypeError, expected_error_message):
            kser.rename(["0", "1"])

        # Series index
        # pser = pd.Series(['a', 'b', 'c', 'd', 'e', 'f', 'g'], name='x')
        # kser = ps.from_pandas(s)

        # TODO: index
        # res = kser.rename(lambda x: x ** 2)
        # self.assert_eq(res, pser.rename(lambda x: x ** 2))

        # res = kser.rename(pser)
        # self.assert_eq(res, pser.rename(pser))

        # res = kser.rename(kser)
        # self.assert_eq(res, pser.rename(pser))

        # res = kser.rename(lambda x: x**2, inplace=True)
        # self.assertis(res, kser)
        # s.rename(lambda x: x**2, inplace=True)
        # self.assert_eq(kser, pser)

    def test_rename_axis(self):
        index = pd.Index(["A", "B", "C"], name="index")
        pser = pd.Series([1.0, 2.0, 3.0], index=index, name="name")
        kser = ps.from_pandas(pser)

        self.assert_eq(
            pser.rename_axis("index2").sort_index(), kser.rename_axis("index2").sort_index(),
        )

        self.assert_eq(
            (pser + 1).rename_axis("index2").sort_index(),
            (kser + 1).rename_axis("index2").sort_index(),
        )

        pser2 = pser.copy()
        kser2 = kser.copy()
        pser2.rename_axis("index2", inplace=True)
        kser2.rename_axis("index2", inplace=True)
        self.assert_eq(pser2.sort_index(), kser2.sort_index())

        self.assertRaises(ValueError, lambda: kser.rename_axis(["index2", "index3"]))
        self.assertRaises(TypeError, lambda: kser.rename_axis(mapper=["index2"], index=["index3"]))

        # index/columns parameters and dict_like/functions mappers introduced in pandas 0.24.0
        if LooseVersion(pd.__version__) >= LooseVersion("0.24.0"):
            self.assert_eq(
                pser.rename_axis(index={"index": "index2", "missing": "index4"}).sort_index(),
                kser.rename_axis(index={"index": "index2", "missing": "index4"}).sort_index(),
            )

            self.assert_eq(
                pser.rename_axis(index=str.upper).sort_index(),
                kser.rename_axis(index=str.upper).sort_index(),
            )
        else:
            expected = kser
            expected.index.name = "index2"
            result = kser.rename_axis(index={"index": "index2", "missing": "index4"}).sort_index()
            self.assert_eq(expected, result)

            expected = kser
            expected.index.name = "INDEX"
            result = kser.rename_axis(index=str.upper).sort_index()
            self.assert_eq(expected, result)

        index = pd.MultiIndex.from_tuples(
            [("A", "B"), ("C", "D"), ("E", "F")], names=["index1", "index2"]
        )
        pser = pd.Series([1.0, 2.0, 3.0], index=index, name="name")
        kser = ps.from_pandas(pser)

        self.assert_eq(
            pser.rename_axis(["index3", "index4"]).sort_index(),
            kser.rename_axis(["index3", "index4"]).sort_index(),
        )

        self.assertRaises(ValueError, lambda: kser.rename_axis(["index3", "index4", "index5"]))

        # index/columns parameters and dict_like/functions mappers introduced in pandas 0.24.0
        if LooseVersion(pd.__version__) >= LooseVersion("0.24.0"):
            self.assert_eq(
                pser.rename_axis(
                    index={"index1": "index3", "index2": "index4", "missing": "index5"}
                ).sort_index(),
                kser.rename_axis(
                    index={"index1": "index3", "index2": "index4", "missing": "index5"}
                ).sort_index(),
            )

            self.assert_eq(
                pser.rename_axis(index=str.upper).sort_index(),
                kser.rename_axis(index=str.upper).sort_index(),
            )
        else:
            expected = kser
            expected.index.names = ["index3", "index4"]
            result = kser.rename_axis(
                index={"index1": "index3", "index2": "index4", "missing": "index5"}
            ).sort_index()
            self.assert_eq(expected, result)

            expected.index.names = ["INDEX1", "INDEX2"]
            result = kser.rename_axis(index=str.upper).sort_index()
            self.assert_eq(expected, result)

    def test_or(self):
        pdf = pd.DataFrame(
            {
                "left": [True, False, True, False, np.nan, np.nan, True, False, np.nan],
                "right": [True, False, False, True, True, False, np.nan, np.nan, np.nan],
            }
        )
        kdf = ps.from_pandas(pdf)

        self.assert_eq(kdf["left"] | kdf["right"], pdf["left"] | pdf["right"])
        self.assert_eq(kdf["left"] | True, pdf["left"] | True)
        self.assert_eq(kdf["left"] | False, pdf["left"] | False)
        self.assert_eq(kdf["left"] | None, pdf["left"] | None)
        self.assert_eq(True | kdf["right"], True | pdf["right"])
        self.assert_eq(False | kdf["right"], False | pdf["right"])
        self.assert_eq(None | kdf["right"], None | pdf["right"])

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
        kdf = ps.from_pandas(pdf)

        self._check_extension(kdf["left"] | kdf["right"], pdf["left"] | pdf["right"])
        self._check_extension(kdf["left"] | True, pdf["left"] | True)
        self._check_extension(kdf["left"] | False, pdf["left"] | False)
        self._check_extension(kdf["left"] | pd.NA, pdf["left"] | pd.NA)
        self._check_extension(True | kdf["right"], True | pdf["right"])
        self._check_extension(False | kdf["right"], False | pdf["right"])
        self._check_extension(pd.NA | kdf["right"], pd.NA | pdf["right"])

    def test_and(self):
        pdf = pd.DataFrame(
            {
                "left": [True, False, True, False, np.nan, np.nan, True, False, np.nan],
                "right": [True, False, False, True, True, False, np.nan, np.nan, np.nan],
            }
        )
        kdf = ps.from_pandas(pdf)

        self.assert_eq(kdf["left"] & kdf["right"], pdf["left"] & pdf["right"])
        self.assert_eq(kdf["left"] & True, pdf["left"] & True)
        self.assert_eq(kdf["left"] & False, pdf["left"] & False)
        self.assert_eq(kdf["left"] & None, pdf["left"] & None)
        self.assert_eq(True & kdf["right"], True & pdf["right"])
        self.assert_eq(False & kdf["right"], False & pdf["right"])
        self.assert_eq(None & kdf["right"], None & pdf["right"])

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
        kdf = ps.from_pandas(pdf)

        self._check_extension(kdf["left"] & kdf["right"], pdf["left"] & pdf["right"])
        self._check_extension(kdf["left"] & True, pdf["left"] & True)
        self._check_extension(kdf["left"] & False, pdf["left"] & False)
        self._check_extension(kdf["left"] & pd.NA, pdf["left"] & pd.NA)
        self._check_extension(True & kdf["right"], True & pdf["right"])
        self._check_extension(False & kdf["right"], False & pdf["right"])
        self._check_extension(pd.NA & kdf["right"], pd.NA & pdf["right"])

    def test_to_numpy(self):
        pser = pd.Series([1, 2, 3, 4, 5, 6, 7], name="x")

        kser = ps.from_pandas(pser)
        self.assert_eq(kser.to_numpy(), pser.values)

    def test_isin(self):
        pser = pd.Series(["lama", "cow", "lama", "beetle", "lama", "hippo"], name="animal")

        kser = ps.from_pandas(pser)

        self.assert_eq(kser.isin(["cow", "lama"]), pser.isin(["cow", "lama"]))
        self.assert_eq(kser.isin(np.array(["cow", "lama"])), pser.isin(np.array(["cow", "lama"])))
        self.assert_eq(kser.isin({"cow"}), pser.isin({"cow"}))

        msg = "only list-like objects are allowed to be passed to isin()"
        with self.assertRaisesRegex(TypeError, msg):
            kser.isin(1)

    def test_drop_duplicates(self):
        pdf = pd.DataFrame({"animal": ["lama", "cow", "lama", "beetle", "lama", "hippo"]})
        kdf = ps.from_pandas(pdf)

        pser = pdf.animal
        kser = kdf.animal

        self.assert_eq(kser.drop_duplicates().sort_index(), pser.drop_duplicates().sort_index())
        self.assert_eq(
            kser.drop_duplicates(keep="last").sort_index(),
            pser.drop_duplicates(keep="last").sort_index(),
        )

        # inplace
        kser.drop_duplicates(keep=False, inplace=True)
        pser.drop_duplicates(keep=False, inplace=True)
        self.assert_eq(kser.sort_index(), pser.sort_index())
        self.assert_eq(kdf, pdf)

    def test_reindex(self):
        index = ["A", "B", "C", "D", "E"]
        pser = pd.Series([1.0, 2.0, 3.0, 4.0, None], index=index, name="x")
        kser = ps.from_pandas(pser)

        self.assert_eq(pser, kser)

        self.assert_eq(
            pser.reindex(["A", "B"]).sort_index(), kser.reindex(["A", "B"]).sort_index(),
        )

        self.assert_eq(
            pser.reindex(["A", "B", "2", "3"]).sort_index(),
            kser.reindex(["A", "B", "2", "3"]).sort_index(),
        )

        self.assert_eq(
            pser.reindex(["A", "E", "2"], fill_value=0).sort_index(),
            kser.reindex(["A", "E", "2"], fill_value=0).sort_index(),
        )

        self.assertRaises(TypeError, lambda: kser.reindex(index=123))

    def test_reindex_like(self):
        data = [1.0, 2.0, None]
        index = pd.Index(["A", "B", "C"], name="index1")
        pser = pd.Series(data=data, index=index, name="name1")
        kser = ps.from_pandas(pser)

        # Reindexing single Index on single Index
        data2 = [3.0, None, 4.0]
        index2 = pd.Index(["A", "C", "D"], name="index2")
        pser2 = pd.Series(data=data2, index=index2, name="name2")
        kser2 = ps.from_pandas(pser2)

        self.assert_eq(
            pser.reindex_like(pser2).sort_index(), kser.reindex_like(kser2).sort_index(),
        )

        self.assert_eq(
            (pser + 1).reindex_like(pser2).sort_index(),
            (kser + 1).reindex_like(kser2).sort_index(),
        )

        # Reindexing MultiIndex on single Index
        index2 = pd.MultiIndex.from_tuples(
            [("A", "G"), ("C", "D"), ("I", "J")], names=["index3", "index4"]
        )
        pser2 = pd.Series(data=data2, index=index2, name="name2")
        kser2 = ps.from_pandas(pser2)

        self.assert_eq(
            pser.reindex_like(pser2).sort_index(), kser.reindex_like(kser2).sort_index(),
        )

        self.assertRaises(TypeError, lambda: kser.reindex_like(index2))
        self.assertRaises(AssertionError, lambda: kser2.reindex_like(kser))

        # Reindexing MultiIndex on MultiIndex
        index = pd.MultiIndex.from_tuples(
            [("A", "B"), ("C", "D"), ("E", "F")], names=["index1", "index2"]
        )
        pser = pd.Series(data=data, index=index, name="name1")
        kser = ps.from_pandas(pser)

        self.assert_eq(
            pser.reindex_like(pser2).sort_index(), kser.reindex_like(kser2).sort_index(),
        )

        # Reindexing with DataFrame
        index2 = pd.MultiIndex.from_tuples(
            [("A", "B"), ("C", "D"), ("E", "F")], names=["name3", "name4"]
        )
        pdf = pd.DataFrame(data=data, index=index2)
        kdf = ps.from_pandas(pdf)

        self.assert_eq(
            pser.reindex_like(pdf).sort_index(), kser.reindex_like(kdf).sort_index(),
        )

    def test_fillna(self):
        pdf = pd.DataFrame({"x": [np.nan, 2, 3, 4, np.nan, 6], "y": [np.nan, 2, 3, 4, np.nan, 6]})
        kdf = ps.from_pandas(pdf)

        pser = pdf.x
        kser = kdf.x

        self.assert_eq(kser.fillna(0), pser.fillna(0))
        self.assert_eq(kser.fillna(np.nan).fillna(0), pser.fillna(np.nan).fillna(0))

        kser.fillna(0, inplace=True)
        pser.fillna(0, inplace=True)
        self.assert_eq(kser, pser)
        self.assert_eq(kdf, pdf)

        # test considering series does not have NA/NaN values
        kser.fillna(0, inplace=True)
        pser.fillna(0, inplace=True)
        self.assert_eq(kser, pser)

        kser = kdf.x.rename("y")
        pser = pdf.x.rename("y")
        kser.fillna(0, inplace=True)
        pser.fillna(0, inplace=True)
        self.assert_eq(kser.head(), pser.head())

        pser = pd.Series([1, 2, 3, 4, 5, 6], name="x")
        kser = ps.from_pandas(pser)

        pser.loc[3] = np.nan
        kser.loc[3] = np.nan

        self.assert_eq(kser.fillna(0), pser.fillna(0))
        self.assert_eq(kser.fillna(method="ffill"), pser.fillna(method="ffill"))
        self.assert_eq(kser.fillna(method="bfill"), pser.fillna(method="bfill"))

        # inplace fillna on non-nullable column
        pdf = pd.DataFrame({"a": [1, 2, None], "b": [1, 2, 3]})
        kdf = ps.from_pandas(pdf)

        pser = pdf.b
        kser = kdf.b

        self.assert_eq(kser.fillna(0), pser.fillna(0))
        self.assert_eq(kser.fillna(np.nan).fillna(0), pser.fillna(np.nan).fillna(0))

        kser.fillna(0, inplace=True)
        pser.fillna(0, inplace=True)
        self.assert_eq(kser, pser)
        self.assert_eq(kdf, pdf)

    def test_dropna(self):
        pdf = pd.DataFrame({"x": [np.nan, 2, 3, 4, np.nan, 6]})
        kdf = ps.from_pandas(pdf)

        pser = pdf.x
        kser = kdf.x

        self.assert_eq(kser.dropna(), pser.dropna())

        pser.dropna(inplace=True)
        kser.dropna(inplace=True)
        self.assert_eq(kser, pser)
        self.assert_eq(kdf, pdf)

    def test_nunique(self):
        pser = pd.Series([1, 2, 1, np.nan])
        kser = ps.from_pandas(pser)

        # Assert NaNs are dropped by default
        nunique_result = kser.nunique()
        self.assertEqual(nunique_result, 2)
        self.assert_eq(nunique_result, pser.nunique())

        # Assert including NaN values
        nunique_result = kser.nunique(dropna=False)
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
        kser = ps.from_pandas(pser)

        exp = pser.value_counts()
        res = kser.value_counts()
        self.assertEqual(res.name, exp.name)
        self.assert_eq(res, exp)

        self.assert_eq(kser.value_counts(normalize=True), pser.value_counts(normalize=True))
        self.assert_eq(kser.value_counts(ascending=True), pser.value_counts(ascending=True))
        self.assert_eq(
            kser.value_counts(normalize=True, dropna=False),
            pser.value_counts(normalize=True, dropna=False),
        )
        self.assert_eq(
            kser.value_counts(ascending=True, dropna=False),
            pser.value_counts(ascending=True, dropna=False),
        )

        self.assert_eq(
            kser.index.value_counts(normalize=True), pser.index.value_counts(normalize=True)
        )
        self.assert_eq(
            kser.index.value_counts(ascending=True), pser.index.value_counts(ascending=True)
        )
        self.assert_eq(
            kser.index.value_counts(normalize=True, dropna=False),
            pser.index.value_counts(normalize=True, dropna=False),
        )
        self.assert_eq(
            kser.index.value_counts(ascending=True, dropna=False),
            pser.index.value_counts(ascending=True, dropna=False),
        )

        with self.assertRaisesRegex(
            NotImplementedError, "value_counts currently does not support bins"
        ):
            kser.value_counts(bins=3)

        pser.name = "index"
        kser.name = "index"
        self.assert_eq(kser.value_counts(), pser.value_counts())

        # Series from DataFrame
        pdf = pd.DataFrame({"a": [2, 2, 3], "b": [None, 1, None]})
        kdf = ps.from_pandas(pdf)

        self.assert_eq(kdf.a.value_counts(normalize=True), pdf.a.value_counts(normalize=True))
        self.assert_eq(kdf.a.value_counts(ascending=True), pdf.a.value_counts(ascending=True))
        self.assert_eq(
            kdf.a.value_counts(normalize=True, dropna=False),
            pdf.a.value_counts(normalize=True, dropna=False),
        )
        self.assert_eq(
            kdf.a.value_counts(ascending=True, dropna=False),
            pdf.a.value_counts(ascending=True, dropna=False),
        )

        self.assert_eq(
            kser.index.value_counts(normalize=True), pser.index.value_counts(normalize=True)
        )
        self.assert_eq(
            kser.index.value_counts(ascending=True), pser.index.value_counts(ascending=True)
        )
        self.assert_eq(
            kser.index.value_counts(normalize=True, dropna=False),
            pser.index.value_counts(normalize=True, dropna=False),
        )
        self.assert_eq(
            kser.index.value_counts(ascending=True, dropna=False),
            pser.index.value_counts(ascending=True, dropna=False),
        )

        # Series with NaN index
        pser = pd.Series([3, 2, 3, 1, 2, 3], index=[2.0, None, 5.0, 5.0, None, 5.0])
        kser = ps.from_pandas(pser)

        self.assert_eq(kser.value_counts(normalize=True), pser.value_counts(normalize=True))
        self.assert_eq(kser.value_counts(ascending=True), pser.value_counts(ascending=True))
        self.assert_eq(
            kser.value_counts(normalize=True, dropna=False),
            pser.value_counts(normalize=True, dropna=False),
        )
        self.assert_eq(
            kser.value_counts(ascending=True, dropna=False),
            pser.value_counts(ascending=True, dropna=False),
        )

        self.assert_eq(
            kser.index.value_counts(normalize=True), pser.index.value_counts(normalize=True)
        )
        self.assert_eq(
            kser.index.value_counts(ascending=True), pser.index.value_counts(ascending=True)
        )
        self.assert_eq(
            kser.index.value_counts(normalize=True, dropna=False),
            pser.index.value_counts(normalize=True, dropna=False),
        )
        self.assert_eq(
            kser.index.value_counts(ascending=True, dropna=False),
            pser.index.value_counts(ascending=True, dropna=False),
        )

        # Series with MultiIndex
        pser.index = pd.MultiIndex.from_tuples(
            [("x", "a"), ("x", "b"), ("y", "c"), ("x", "a"), ("y", "c"), ("x", "a")]
        )
        kser = ps.from_pandas(pser)

        self.assert_eq(kser.value_counts(normalize=True), pser.value_counts(normalize=True))
        self.assert_eq(kser.value_counts(ascending=True), pser.value_counts(ascending=True))
        self.assert_eq(
            kser.value_counts(normalize=True, dropna=False),
            pser.value_counts(normalize=True, dropna=False),
        )
        self.assert_eq(
            kser.value_counts(ascending=True, dropna=False),
            pser.value_counts(ascending=True, dropna=False),
        )

        # FIXME: MultiIndex.value_counts returns wrong indices.
        self.assert_eq(
            kser.index.value_counts(normalize=True),
            pser.index.value_counts(normalize=True),
            almost=True,
        )
        self.assert_eq(
            kser.index.value_counts(ascending=True),
            pser.index.value_counts(ascending=True),
            almost=True,
        )
        self.assert_eq(
            kser.index.value_counts(normalize=True, dropna=False),
            pser.index.value_counts(normalize=True, dropna=False),
            almost=True,
        )
        self.assert_eq(
            kser.index.value_counts(ascending=True, dropna=False),
            pser.index.value_counts(ascending=True, dropna=False),
            almost=True,
        )

        # Series with MultiIndex some of index has NaN
        pser.index = pd.MultiIndex.from_tuples(
            [("x", "a"), ("x", None), ("y", "c"), ("x", "a"), ("y", "c"), ("x", "a")]
        )
        kser = ps.from_pandas(pser)

        self.assert_eq(kser.value_counts(normalize=True), pser.value_counts(normalize=True))
        self.assert_eq(kser.value_counts(ascending=True), pser.value_counts(ascending=True))
        self.assert_eq(
            kser.value_counts(normalize=True, dropna=False),
            pser.value_counts(normalize=True, dropna=False),
        )
        self.assert_eq(
            kser.value_counts(ascending=True, dropna=False),
            pser.value_counts(ascending=True, dropna=False),
        )

        # FIXME: MultiIndex.value_counts returns wrong indices.
        self.assert_eq(
            kser.index.value_counts(normalize=True),
            pser.index.value_counts(normalize=True),
            almost=True,
        )
        self.assert_eq(
            kser.index.value_counts(ascending=True),
            pser.index.value_counts(ascending=True),
            almost=True,
        )
        self.assert_eq(
            kser.index.value_counts(normalize=True, dropna=False),
            pser.index.value_counts(normalize=True, dropna=False),
            almost=True,
        )
        self.assert_eq(
            kser.index.value_counts(ascending=True, dropna=False),
            pser.index.value_counts(ascending=True, dropna=False),
            almost=True,
        )

        # Series with MultiIndex some of index is NaN.
        # This test only available for pandas >= 0.24.
        if LooseVersion(pd.__version__) >= LooseVersion("0.24"):
            pser.index = pd.MultiIndex.from_tuples(
                [("x", "a"), None, ("y", "c"), ("x", "a"), ("y", "c"), ("x", "a")]
            )
            kser = ps.from_pandas(pser)

            self.assert_eq(kser.value_counts(normalize=True), pser.value_counts(normalize=True))
            self.assert_eq(kser.value_counts(ascending=True), pser.value_counts(ascending=True))
            self.assert_eq(
                kser.value_counts(normalize=True, dropna=False),
                pser.value_counts(normalize=True, dropna=False),
            )
            self.assert_eq(
                kser.value_counts(ascending=True, dropna=False),
                pser.value_counts(ascending=True, dropna=False),
            )

            # FIXME: MultiIndex.value_counts returns wrong indices.
            self.assert_eq(
                kser.index.value_counts(normalize=True),
                pser.index.value_counts(normalize=True),
                almost=True,
            )
            self.assert_eq(
                kser.index.value_counts(ascending=True),
                pser.index.value_counts(ascending=True),
                almost=True,
            )
            self.assert_eq(
                kser.index.value_counts(normalize=True, dropna=False),
                pser.index.value_counts(normalize=True, dropna=False),
                almost=True,
            )
            self.assert_eq(
                kser.index.value_counts(ascending=True, dropna=False),
                pser.index.value_counts(ascending=True, dropna=False),
                almost=True,
            )

    def test_nsmallest(self):
        sample_lst = [1, 2, 3, 4, np.nan, 6]
        pser = pd.Series(sample_lst, name="x")
        kser = ps.Series(sample_lst, name="x")
        self.assert_eq(kser.nsmallest(n=3), pser.nsmallest(n=3))
        self.assert_eq(kser.nsmallest(), pser.nsmallest())
        self.assert_eq((kser + 1).nsmallest(), (pser + 1).nsmallest())

    def test_nlargest(self):
        sample_lst = [1, 2, 3, 4, np.nan, 6]
        pser = pd.Series(sample_lst, name="x")
        kser = ps.Series(sample_lst, name="x")
        self.assert_eq(kser.nlargest(n=3), pser.nlargest(n=3))
        self.assert_eq(kser.nlargest(), pser.nlargest())
        self.assert_eq((kser + 1).nlargest(), (pser + 1).nlargest())

    def test_isnull(self):
        pser = pd.Series([1, 2, 3, 4, np.nan, 6], name="x")
        kser = ps.from_pandas(pser)

        self.assert_eq(kser.notnull(), pser.notnull())
        self.assert_eq(kser.isnull(), pser.isnull())

        pser = self.pser
        kser = self.kser

        self.assert_eq(kser.notnull(), pser.notnull())
        self.assert_eq(kser.isnull(), pser.isnull())

    def test_all(self):
        for pser in [
            pd.Series([True, True], name="x"),
            pd.Series([True, False], name="x"),
            pd.Series([0, 1], name="x"),
            pd.Series([1, 2, 3], name="x"),
            pd.Series([True, True, None], name="x"),
            pd.Series([True, False, None], name="x"),
            pd.Series([], name="x"),
            pd.Series([np.nan], name="x"),
        ]:
            kser = ps.from_pandas(pser)
            self.assert_eq(kser.all(), pser.all())

        pser = pd.Series([1, 2, 3, 4], name="x")
        kser = ps.from_pandas(pser)

        self.assert_eq((kser % 2 == 0).all(), (pser % 2 == 0).all())

        with self.assertRaisesRegex(
            NotImplementedError, 'axis should be either 0 or "index" currently.'
        ):
            kser.all(axis=1)

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
            kser = ps.from_pandas(pser)
            self.assert_eq(kser.any(), pser.any())

        pser = pd.Series([1, 2, 3, 4], name="x")
        kser = ps.from_pandas(pser)

        self.assert_eq((kser % 2 == 0).any(), (pser % 2 == 0).any())

        with self.assertRaisesRegex(
            NotImplementedError, 'axis should be either 0 or "index" currently.'
        ):
            kser.any(axis=1)

    def test_reset_index(self):
        pdf = pd.DataFrame({"foo": [1, 2, 3, 4]}, index=pd.Index(["a", "b", "c", "d"], name="idx"))
        kdf = ps.from_pandas(pdf)

        pser = pdf.foo
        kser = kdf.foo

        self.assert_eq(kser.reset_index(), pser.reset_index())
        self.assert_eq(kser.reset_index(name="values"), pser.reset_index(name="values"))
        self.assert_eq(kser.reset_index(drop=True), pser.reset_index(drop=True))

        # inplace
        kser.reset_index(drop=True, inplace=True)
        pser.reset_index(drop=True, inplace=True)
        self.assert_eq(kser, pser)
        self.assert_eq(kdf, pdf)

    def test_reset_index_with_default_index_types(self):
        pser = pd.Series([1, 2, 3], name="0", index=np.random.rand(3))
        kser = ps.from_pandas(pser)

        with ps.option_context("compute.default_index_type", "sequence"):
            self.assert_eq(kser.reset_index(), pser.reset_index())

        with ps.option_context("compute.default_index_type", "distributed-sequence"):
            # the order might be changed.
            self.assert_eq(kser.reset_index().sort_index(), pser.reset_index())

        with ps.option_context("compute.default_index_type", "distributed"):
            # the index is different.
            self.assert_eq(
                kser.reset_index().to_pandas().reset_index(drop=True), pser.reset_index()
            )

    def test_index_to_series_reset_index(self):
        def check(kser, pser):
            self.assert_eq(kser.reset_index(), pser.reset_index())
            self.assert_eq(kser.reset_index(drop=True), pser.reset_index(drop=True))

            pser.reset_index(drop=True, inplace=True)
            kser.reset_index(drop=True, inplace=True)
            self.assert_eq(kser, pser)

        pdf = pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9], "b": [4, 5, 6, 3, 2, 1, 0, 0, 0]},
            index=np.random.rand(9),
        )
        kdf = ps.from_pandas(pdf)
        check(kdf.index.to_series(), pdf.index.to_series())
        check(kdf.index.to_series(name="a"), pdf.index.to_series(name="a"))
        check(kdf.index.to_series(name=("x", "a")), pdf.index.to_series(name=("x", "a")))

    def test_sort_values(self):
        pdf = pd.DataFrame({"x": [1, 2, 3, 4, 5, None, 7]})
        kdf = ps.from_pandas(pdf)

        pser = pdf.x
        kser = kdf.x

        self.assert_eq(kser.sort_values(), pser.sort_values())
        self.assert_eq(kser.sort_values(ascending=False), pser.sort_values(ascending=False))
        self.assert_eq(kser.sort_values(na_position="first"), pser.sort_values(na_position="first"))

        self.assertRaises(ValueError, lambda: kser.sort_values(na_position="invalid"))

        # inplace
        # pandas raises an exception when the Series is derived from DataFrame
        kser.sort_values(inplace=True)
        self.assert_eq(kser, pser.sort_values())
        self.assert_eq(kdf, pdf)

        pser = pdf.x.copy()
        kser = kdf.x.copy()

        kser.sort_values(inplace=True)
        pser.sort_values(inplace=True)
        self.assert_eq(kser, pser)
        self.assert_eq(kdf, pdf)

    def test_sort_index(self):
        pdf = pd.DataFrame({"x": [2, 1, np.nan]}, index=["b", "a", np.nan])
        kdf = ps.from_pandas(pdf)

        pser = pdf.x
        kser = kdf.x

        # Assert invalid parameters
        self.assertRaises(NotImplementedError, lambda: kser.sort_index(axis=1))
        self.assertRaises(NotImplementedError, lambda: kser.sort_index(kind="mergesort"))
        self.assertRaises(ValueError, lambda: kser.sort_index(na_position="invalid"))

        # Assert default behavior without parameters
        self.assert_eq(kser.sort_index(), pser.sort_index())
        # Assert sorting descending
        self.assert_eq(kser.sort_index(ascending=False), pser.sort_index(ascending=False))
        # Assert sorting NA indices first
        self.assert_eq(kser.sort_index(na_position="first"), pser.sort_index(na_position="first"))

        # Assert sorting inplace
        # pandas sorts pdf.x by the index and update the column only
        # when the Series is derived from DataFrame.
        kser.sort_index(inplace=True)
        self.assert_eq(kser, pser.sort_index())
        self.assert_eq(kdf, pdf)

        pser = pdf.x.copy()
        kser = kdf.x.copy()

        kser.sort_index(inplace=True)
        pser.sort_index(inplace=True)
        self.assert_eq(kser, pser)
        self.assert_eq(kdf, pdf)

        # Assert multi-indices
        pser = pd.Series(range(4), index=[["b", "b", "a", "a"], [1, 0, 1, 0]], name="0")
        kser = ps.from_pandas(pser)
        self.assert_eq(kser.sort_index(), pser.sort_index())
        self.assert_eq(kser.sort_index(level=[1, 0]), pser.sort_index(level=[1, 0]))

        self.assert_eq(kser.reset_index().sort_index(), pser.reset_index().sort_index())

    def test_to_datetime(self):
        pser = pd.Series(["3/11/2000", "3/12/2000", "3/13/2000"] * 100)
        kser = ps.from_pandas(pser)

        self.assert_eq(
            pd.to_datetime(pser, infer_datetime_format=True),
            ps.to_datetime(kser, infer_datetime_format=True),
        )

    def test_missing(self):
        kser = self.kser

        missing_functions = inspect.getmembers(MissingPandasLikeSeries, inspect.isfunction)
        unsupported_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "unsupported_function"
        ]
        for name in unsupported_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "method.*Series.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(kser, name)()

        deprecated_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "deprecated_function"
        ]
        for name in deprecated_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "method.*Series.*{}.*is deprecated".format(name)
            ):
                getattr(kser, name)()

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
                getattr(kser, name)
        deprecated_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "deprecated_property"
        ]
        for name in deprecated_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "property.*Series.*{}.*is deprecated".format(name)
            ):
                getattr(kser, name)

    def test_clip(self):
        pser = pd.Series([0, 2, 4], index=np.random.rand(3))
        kser = ps.from_pandas(pser)

        # Assert list-like values are not accepted for 'lower' and 'upper'
        msg = "List-like value are not supported for 'lower' and 'upper' at the moment"
        with self.assertRaises(TypeError, msg=msg):
            kser.clip(lower=[1])
        with self.assertRaises(TypeError, msg=msg):
            kser.clip(upper=[1])

        # Assert no lower or upper
        self.assert_eq(kser.clip(), pser.clip())
        # Assert lower only
        self.assert_eq(kser.clip(1), pser.clip(1))
        # Assert upper only
        self.assert_eq(kser.clip(upper=3), pser.clip(upper=3))
        # Assert lower and upper
        self.assert_eq(kser.clip(1, 3), pser.clip(1, 3))

        # Assert behavior on string values
        str_kser = ps.Series(["a", "b", "c"])
        self.assert_eq(str_kser.clip(1, 3), str_kser)

    def test_compare(self):
        if LooseVersion(pd.__version__) >= LooseVersion("1.1"):
            pser = pd.Series([1, 2])
            kser = ps.from_pandas(pser)

            res_kdf = kser.compare(kser)
            self.assertTrue(res_kdf.empty)
            self.assert_eq(res_kdf.columns, pd.Index(["self", "other"]))

            self.assert_eq(pser.compare(pser + 1).sort_index(), kser.compare(kser + 1).sort_index())

            pser = pd.Series([1, 2], index=["x", "y"])
            kser = ps.from_pandas(pser)
            self.assert_eq(pser.compare(pser + 1).sort_index(), kser.compare(kser + 1).sort_index())
        else:
            kser = ps.Series([1, 2])
            res_kdf = kser.compare(kser)
            self.assertTrue(res_kdf.empty)
            self.assert_eq(res_kdf.columns, pd.Index(["self", "other"]))
            expected = ps.DataFrame([[1, 2], [2, 3]], columns=["self", "other"])
            self.assert_eq(expected, kser.compare(kser + 1).sort_index())

            kser = ps.Series([1, 2], index=["x", "y"])
            expected = ps.DataFrame([[1, 2], [2, 3]], index=["x", "y"], columns=["self", "other"])
            self.assert_eq(expected, kser.compare(kser + 1).sort_index())

    def test_is_unique(self):
        # We can't use pandas' is_unique for comparison. pandas 0.23 ignores None
        pser = pd.Series([1, 2, 2, None, None])
        kser = ps.from_pandas(pser)
        self.assertEqual(False, kser.is_unique)
        self.assertEqual(False, (kser + 1).is_unique)

        pser = pd.Series([1, None, None])
        kser = ps.from_pandas(pser)
        self.assertEqual(False, kser.is_unique)
        self.assertEqual(False, (kser + 1).is_unique)

        pser = pd.Series([1])
        kser = ps.from_pandas(pser)
        self.assertEqual(pser.is_unique, kser.is_unique)
        self.assertEqual((pser + 1).is_unique, (kser + 1).is_unique)

        pser = pd.Series([1, 1, 1])
        kser = ps.from_pandas(pser)
        self.assertEqual(pser.is_unique, kser.is_unique)
        self.assertEqual((pser + 1).is_unique, (kser + 1).is_unique)

    def test_to_list(self):
        self.assert_eq(self.kser.tolist(), self.pser.tolist())

    def test_append(self):
        pser1 = pd.Series([1, 2, 3], name="0")
        pser2 = pd.Series([4, 5, 6], name="0")
        pser3 = pd.Series([4, 5, 6], index=[3, 4, 5], name="0")
        kser1 = ps.from_pandas(pser1)
        kser2 = ps.from_pandas(pser2)
        kser3 = ps.from_pandas(pser3)

        self.assert_eq(kser1.append(kser2), pser1.append(pser2))
        self.assert_eq(kser1.append(kser3), pser1.append(pser3))
        self.assert_eq(
            kser1.append(kser2, ignore_index=True), pser1.append(pser2, ignore_index=True)
        )

        kser1.append(kser3, verify_integrity=True)
        msg = "Indices have overlapping values"
        with self.assertRaises(ValueError, msg=msg):
            kser1.append(kser2, verify_integrity=True)

    def test_map(self):
        pser = pd.Series(["cat", "dog", None, "rabbit"])
        kser = ps.from_pandas(pser)
        # Currently Koalas doesn't return NaN as pandas does.
        self.assert_eq(kser.map({}), pser.map({}).replace({pd.np.nan: None}))

        d = defaultdict(lambda: "abc")
        self.assertTrue("abc" in repr(kser.map(d)))
        self.assert_eq(kser.map(d), pser.map(d))

        def tomorrow(date) -> datetime:
            return date + timedelta(days=1)

        pser = pd.Series([datetime(2019, 10, 24)])
        kser = ps.from_pandas(pser)
        self.assert_eq(kser.map(tomorrow), pser.map(tomorrow))

    def test_add_prefix(self):
        pser = pd.Series([1, 2, 3, 4], name="0")
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.add_prefix("item_"), kser.add_prefix("item_"))

        pser = pd.Series(
            [1, 2, 3],
            name="0",
            index=pd.MultiIndex.from_tuples([("A", "X"), ("A", "Y"), ("B", "X")]),
        )
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.add_prefix("item_"), kser.add_prefix("item_"))

    def test_add_suffix(self):
        pser = pd.Series([1, 2, 3, 4], name="0")
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.add_suffix("_item"), kser.add_suffix("_item"))

        pser = pd.Series(
            [1, 2, 3],
            name="0",
            index=pd.MultiIndex.from_tuples([("A", "X"), ("A", "Y"), ("B", "X")]),
        )
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.add_suffix("_item"), kser.add_suffix("_item"))

    def test_cummin(self):
        pser = pd.Series([1.0, None, 0.0, 4.0, 9.0])
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.cummin(), kser.cummin())
        self.assert_eq(pser.cummin(skipna=False), kser.cummin(skipna=False))
        self.assert_eq(pser.cummin().sum(), kser.cummin().sum())

        # with reversed index
        pser.index = [4, 3, 2, 1, 0]
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.cummin(), kser.cummin())
        self.assert_eq(pser.cummin(skipna=False), kser.cummin(skipna=False))

    def test_cummax(self):
        pser = pd.Series([1.0, None, 0.0, 4.0, 9.0])
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.cummax(), kser.cummax())
        self.assert_eq(pser.cummax(skipna=False), kser.cummax(skipna=False))
        self.assert_eq(pser.cummax().sum(), kser.cummax().sum())

        # with reversed index
        pser.index = [4, 3, 2, 1, 0]
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.cummax(), kser.cummax())
        self.assert_eq(pser.cummax(skipna=False), kser.cummax(skipna=False))

    def test_cumsum(self):
        pser = pd.Series([1.0, None, 0.0, 4.0, 9.0])
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.cumsum(), kser.cumsum())
        self.assert_eq(pser.cumsum(skipna=False), kser.cumsum(skipna=False))
        self.assert_eq(pser.cumsum().sum(), kser.cumsum().sum())

        # with reversed index
        pser.index = [4, 3, 2, 1, 0]
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.cumsum(), kser.cumsum())
        self.assert_eq(pser.cumsum(skipna=False), kser.cumsum(skipna=False))

        # bool
        pser = pd.Series([True, True, False, True])
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.cumsum().astype(int), kser.cumsum())
        self.assert_eq(pser.cumsum(skipna=False).astype(int), kser.cumsum(skipna=False))

    def test_cumprod(self):
        pser = pd.Series([1.0, None, 1.0, 4.0, 9.0])
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.cumprod(), kser.cumprod())
        self.assert_eq(pser.cumprod(skipna=False), kser.cumprod(skipna=False))
        self.assert_eq(pser.cumprod().sum(), kser.cumprod().sum())

        # with integer type
        pser = pd.Series([1, 10, 1, 4, 9])
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.cumprod(), kser.cumprod())
        self.assert_eq(pser.cumprod(skipna=False), kser.cumprod(skipna=False))
        self.assert_eq(pser.cumprod().sum(), kser.cumprod().sum())

        # with reversed index
        pser.index = [4, 3, 2, 1, 0]
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.cumprod(), kser.cumprod())
        self.assert_eq(pser.cumprod(skipna=False), kser.cumprod(skipna=False))

        # including zero
        pser = pd.Series([1, 2, 0, 3])
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.cumprod(), kser.cumprod())
        self.assert_eq(pser.cumprod(skipna=False), kser.cumprod(skipna=False))

        # including negative values
        pser = pd.Series([1, -1, -2])
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.cumprod(), kser.cumprod())
        self.assert_eq(pser.cumprod(skipna=False), kser.cumprod(skipna=False))

        # bool
        pser = pd.Series([True, True, False, True])
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.cumprod(), kser.cumprod())
        self.assert_eq(pser.cumprod(skipna=False).astype(int), kser.cumprod(skipna=False))

    def test_median(self):
        with self.assertRaisesRegex(TypeError, "accuracy must be an integer; however"):
            ps.Series([24.0, 21.0, 25.0, 33.0, 26.0]).median(accuracy="a")

    def test_rank(self):
        pser = pd.Series([1, 2, 3, 1], name="x")
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.rank(), kser.rank().sort_index())
        self.assert_eq(pser.rank().sum(), kser.rank().sum())
        self.assert_eq(pser.rank(ascending=False), kser.rank(ascending=False).sort_index())
        self.assert_eq(pser.rank(method="min"), kser.rank(method="min").sort_index())
        self.assert_eq(pser.rank(method="max"), kser.rank(method="max").sort_index())
        self.assert_eq(pser.rank(method="first"), kser.rank(method="first").sort_index())
        self.assert_eq(pser.rank(method="dense"), kser.rank(method="dense").sort_index())

        msg = "method must be one of 'average', 'min', 'max', 'first', 'dense'"
        with self.assertRaisesRegex(ValueError, msg):
            kser.rank(method="nothing")

    def test_round(self):
        pser = pd.Series([0.028208, 0.038683, 0.877076], name="x")
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.round(2), kser.round(2))
        msg = "decimals must be an integer"
        with self.assertRaisesRegex(TypeError, msg):
            kser.round(1.5)

    def test_quantile(self):
        pser = pd.Series([])
        kser = ps.from_pandas(pser)

        self.assert_eq(kser.quantile(0.5), pser.quantile(0.5))
        self.assert_eq(kser.quantile([0.25, 0.5, 0.75]), pser.quantile([0.25, 0.5, 0.75]))

        with self.assertRaisesRegex(TypeError, "accuracy must be an integer; however"):
            ps.Series([24.0, 21.0, 25.0, 33.0, 26.0]).quantile(accuracy="a")
        with self.assertRaisesRegex(TypeError, "q must be a float or an array of floats;"):
            ps.Series([24.0, 21.0, 25.0, 33.0, 26.0]).quantile(q="a")
        with self.assertRaisesRegex(TypeError, "q must be a float or an array of floats;"):
            ps.Series([24.0, 21.0, 25.0, 33.0, 26.0]).quantile(q=["a"])

        with self.assertRaisesRegex(TypeError, "Could not convert object \\(string\\) to numeric"):
            ps.Series(["a", "b", "c"]).quantile()
        with self.assertRaisesRegex(TypeError, "Could not convert object \\(string\\) to numeric"):
            ps.Series(["a", "b", "c"]).quantile([0.25, 0.5, 0.75])

    def test_idxmax(self):
        pser = pd.Series(data=[1, 4, 5], index=["A", "B", "C"])
        kser = ps.Series(pser)

        self.assertEqual(kser.idxmax(), pser.idxmax())
        self.assertEqual(kser.idxmax(skipna=False), pser.idxmax(skipna=False))

        index = pd.MultiIndex.from_arrays(
            [["a", "a", "b", "b"], ["c", "d", "e", "f"]], names=("first", "second")
        )
        pser = pd.Series(data=[1, 2, 4, 5], index=index)
        kser = ps.Series(pser)

        self.assertEqual(kser.idxmax(), pser.idxmax())
        self.assertEqual(kser.idxmax(skipna=False), pser.idxmax(skipna=False))

        kser = ps.Series([])
        with self.assertRaisesRegex(ValueError, "an empty sequence"):
            kser.idxmax()

        pser = pd.Series([1, 100, None, 100, 1, 100], index=[10, 3, 5, 2, 1, 8])
        kser = ps.Series(pser)

        self.assertEqual(kser.idxmax(), pser.idxmax())
        self.assertEqual(repr(kser.idxmax(skipna=False)), repr(pser.idxmax(skipna=False)))

    def test_idxmin(self):
        pser = pd.Series(data=[1, 4, 5], index=["A", "B", "C"])
        kser = ps.Series(pser)

        self.assertEqual(kser.idxmin(), pser.idxmin())
        self.assertEqual(kser.idxmin(skipna=False), pser.idxmin(skipna=False))

        index = pd.MultiIndex.from_arrays(
            [["a", "a", "b", "b"], ["c", "d", "e", "f"]], names=("first", "second")
        )
        pser = pd.Series(data=[1, 2, 4, 5], index=index)
        kser = ps.Series(pser)

        self.assertEqual(kser.idxmin(), pser.idxmin())
        self.assertEqual(kser.idxmin(skipna=False), pser.idxmin(skipna=False))

        kser = ps.Series([])
        with self.assertRaisesRegex(ValueError, "an empty sequence"):
            kser.idxmin()

        pser = pd.Series([1, 100, None, 100, 1, 100], index=[10, 3, 5, 2, 1, 8])
        kser = ps.Series(pser)

        self.assertEqual(kser.idxmin(), pser.idxmin())
        self.assertEqual(repr(kser.idxmin(skipna=False)), repr(pser.idxmin(skipna=False)))

    def test_shift(self):
        pser = pd.Series([10, 20, 15, 30, 45], name="x")
        kser = ps.Series(pser)

        self.assert_eq(kser.shift(2), pser.shift(2))
        self.assert_eq(kser.shift().shift(-1), pser.shift().shift(-1))
        self.assert_eq(kser.shift().sum(), pser.shift().sum())

        if LooseVersion(pd.__version__) < LooseVersion("0.24.2"):
            self.assert_eq(kser.shift(periods=2), pser.shift(periods=2))
        else:
            self.assert_eq(kser.shift(periods=2, fill_value=0), pser.shift(periods=2, fill_value=0))
        with self.assertRaisesRegex(TypeError, "periods should be an int; however"):
            kser.shift(periods=1.5)

    def test_diff(self):
        pser = pd.Series([10, 20, 15, 30, 45], name="x")
        kser = ps.Series(pser)

        self.assert_eq(kser.diff(2), pser.diff(2))
        self.assert_eq(kser.diff().diff(-1), pser.diff().diff(-1))
        self.assert_eq(kser.diff().sum(), pser.diff().sum())

    def _test_numeric_astype(self, pser):
        kser = ps.Series(pser)

        self.assert_eq(kser.astype(int), pser.astype(int))
        self.assert_eq(kser.astype(np.int), pser.astype(np.int))
        self.assert_eq(kser.astype(np.int8), pser.astype(np.int8))
        self.assert_eq(kser.astype(np.int16), pser.astype(np.int16))
        self.assert_eq(kser.astype(np.int32), pser.astype(np.int32))
        self.assert_eq(kser.astype(np.int64), pser.astype(np.int64))
        self.assert_eq(kser.astype(np.byte), pser.astype(np.byte))
        self.assert_eq(kser.astype("int"), pser.astype("int"))
        self.assert_eq(kser.astype("int8"), pser.astype("int8"))
        self.assert_eq(kser.astype("int16"), pser.astype("int16"))
        self.assert_eq(kser.astype("int32"), pser.astype("int32"))
        self.assert_eq(kser.astype("int64"), pser.astype("int64"))
        self.assert_eq(kser.astype("b"), pser.astype("b"))
        self.assert_eq(kser.astype("byte"), pser.astype("byte"))
        self.assert_eq(kser.astype("i"), pser.astype("i"))
        self.assert_eq(kser.astype("long"), pser.astype("long"))
        self.assert_eq(kser.astype("short"), pser.astype("short"))
        self.assert_eq(kser.astype(np.float), pser.astype(np.float))
        self.assert_eq(kser.astype(np.float32), pser.astype(np.float32))
        self.assert_eq(kser.astype(np.float64), pser.astype(np.float64))
        self.assert_eq(kser.astype("float"), pser.astype("float"))
        self.assert_eq(kser.astype("float32"), pser.astype("float32"))
        self.assert_eq(kser.astype("float64"), pser.astype("float64"))
        self.assert_eq(kser.astype("double"), pser.astype("double"))
        self.assert_eq(kser.astype("f"), pser.astype("f"))
        self.assert_eq(kser.astype(bool), pser.astype(bool))
        self.assert_eq(kser.astype("bool"), pser.astype("bool"))
        self.assert_eq(kser.astype("?"), pser.astype("?"))
        self.assert_eq(kser.astype(np.unicode_), pser.astype(np.unicode_))
        self.assert_eq(kser.astype("str"), pser.astype("str"))
        self.assert_eq(kser.astype("U"), pser.astype("U"))

        if extension_dtypes_available:
            from pandas import Int8Dtype, Int16Dtype, Int32Dtype, Int64Dtype

            self._check_extension(kser.astype("Int8"), pser.astype("Int8"))
            self._check_extension(kser.astype("Int16"), pser.astype("Int16"))
            self._check_extension(kser.astype("Int32"), pser.astype("Int32"))
            self._check_extension(kser.astype("Int64"), pser.astype("Int64"))
            self._check_extension(kser.astype(Int8Dtype()), pser.astype(Int8Dtype()))
            self._check_extension(kser.astype(Int16Dtype()), pser.astype(Int16Dtype()))
            self._check_extension(kser.astype(Int32Dtype()), pser.astype(Int32Dtype()))
            self._check_extension(kser.astype(Int64Dtype()), pser.astype(Int64Dtype()))

        if extension_object_dtypes_available:
            from pandas import StringDtype

            if LooseVersion(pd.__version__) >= LooseVersion("1.1"):
                self._check_extension(kser.astype("string"), pser.astype("string"))
                self._check_extension(kser.astype(StringDtype()), pser.astype(StringDtype()))
            else:
                self._check_extension(
                    kser.astype("string"),
                    pd.Series(["10", "20", "15", "30", "45"], name="x", dtype="string"),
                )
                self._check_extension(
                    kser.astype(StringDtype()),
                    pd.Series(["10", "20", "15", "30", "45"], name="x", dtype=StringDtype()),
                )

        if extension_float_dtypes_available:
            from pandas import Float32Dtype, Float64Dtype

            self._check_extension(kser.astype("Float32"), pser.astype("Float32"))
            self._check_extension(kser.astype("Float64"), pser.astype("Float64"))
            self._check_extension(kser.astype(Float32Dtype()), pser.astype(Float32Dtype()))
            self._check_extension(kser.astype(Float64Dtype()), pser.astype(Float64Dtype()))

    def test_astype(self):
        psers = [pd.Series([10, 20, 15, 30, 45], name="x")]

        if extension_dtypes_available:
            psers.append(pd.Series([10, 20, 15, 30, 45], name="x", dtype="Int64"))
        if extension_float_dtypes_available:
            psers.append(pd.Series([10, 20, 15, 30, 45], name="x", dtype="Float64"))

        for pser in psers:
            self._test_numeric_astype(pser)

        pser = pd.Series([10, 20, 15, 30, 45, None, np.nan], name="x")
        kser = ps.Series(pser)

        self.assert_eq(kser.astype(bool), pser.astype(bool))
        self.assert_eq(kser.astype(str), pser.astype(str))

        pser = pd.Series(["hi", "hi ", " ", " \t", "", None], name="x")
        kser = ps.Series(pser)

        self.assert_eq(kser.astype(bool), pser.astype(bool))
        if LooseVersion("1.1.1") <= LooseVersion(pd.__version__) < LooseVersion("1.1.4"):
            # a pandas bug: https://github.com/databricks/koalas/pull/1818#issuecomment-703961980
            self.assert_eq(kser.astype(str).tolist(), ["hi", "hi ", " ", " \t", "", "None"])
        else:
            self.assert_eq(kser.astype(str), pser.astype(str))
        self.assert_eq(kser.str.strip().astype(bool), pser.str.strip().astype(bool))

        if extension_object_dtypes_available:
            from pandas import StringDtype

            self._check_extension(kser.astype("string"), pser.astype("string"))
            self._check_extension(kser.astype(StringDtype()), pser.astype(StringDtype()))

        pser = pd.Series([True, False, None], name="x")
        kser = ps.Series(pser)

        self.assert_eq(kser.astype(bool), pser.astype(bool))
        # Comment out the below test cause because pandas returns `None` or `nan` randomly
        # self.assert_eq(kser.astype(str), pser.astype(str))

        if extension_object_dtypes_available:
            from pandas import BooleanDtype, StringDtype

            self._check_extension(kser.astype("boolean"), pser.astype("boolean"))
            self._check_extension(kser.astype(BooleanDtype()), pser.astype(BooleanDtype()))

            if LooseVersion(pd.__version__) >= LooseVersion("1.1"):
                self._check_extension(kser.astype("string"), pser.astype("string"))
                self._check_extension(kser.astype(StringDtype()), pser.astype(StringDtype()))
            else:
                self._check_extension(
                    kser.astype("string"),
                    pd.Series(["True", "False", None], name="x", dtype="string"),
                )
                self._check_extension(
                    kser.astype(StringDtype()),
                    pd.Series(["True", "False", None], name="x", dtype=StringDtype()),
                )

        pser = pd.Series(["2020-10-27 00:00:01", None], name="x")
        kser = ps.Series(pser)

        self.assert_eq(kser.astype(np.datetime64), pser.astype(np.datetime64))
        self.assert_eq(kser.astype("datetime64[ns]"), pser.astype("datetime64[ns]"))
        self.assert_eq(kser.astype("M"), pser.astype("M"))
        self.assert_eq(kser.astype("M").astype(str), pser.astype("M").astype(str))
        # Comment out the below test cause because pandas returns `NaT` or `nan` randomly
        # self.assert_eq(kser.astype("M").dt.date.astype(str), pser.astype("M").dt.date.astype(str))

        if extension_object_dtypes_available:
            from pandas import StringDtype

            self._check_extension(
                kser.astype("M").astype("string"), pser.astype("M").astype("string")
            )
            self._check_extension(
                kser.astype("M").astype(StringDtype()), pser.astype("M").astype(StringDtype())
            )

        with self.assertRaisesRegex(TypeError, "not understood"):
            kser.astype("int63")

    def test_aggregate(self):
        pser = pd.Series([10, 20, 15, 30, 45], name="x")
        kser = ps.Series(pser)
        msg = "func must be a string or list of strings"
        with self.assertRaisesRegex(TypeError, msg):
            kser.aggregate({"x": ["min", "max"]})
        msg = (
            "If the given function is a list, it " "should only contains function names as strings."
        )
        with self.assertRaisesRegex(ValueError, msg):
            kser.aggregate(["min", max])

    def test_drop(self):
        pser = pd.Series([10, 20, 15, 30, 45], name="x")
        kser = ps.Series(pser)

        self.assert_eq(kser.drop(1), pser.drop(1))
        self.assert_eq(kser.drop([1, 4]), pser.drop([1, 4]))

        msg = "Need to specify at least one of 'labels' or 'index'"
        with self.assertRaisesRegex(ValueError, msg):
            kser.drop()
        self.assertRaises(KeyError, lambda: kser.drop((0, 1)))

        # For MultiIndex
        midx = pd.MultiIndex(
            [["lama", "cow", "falcon"], ["speed", "weight", "length"]],
            [[0, 0, 0, 1, 1, 1, 2, 2, 2], [0, 1, 2, 0, 1, 2, 0, 1, 2]],
        )
        pser = pd.Series([45, 200, 1.2, 30, 250, 1.5, 320, 1, 0.3], index=midx)
        kser = ps.from_pandas(pser)

        self.assert_eq(kser.drop("lama"), pser.drop("lama"))
        self.assert_eq(kser.drop(labels="weight", level=1), pser.drop(labels="weight", level=1))
        self.assert_eq(kser.drop(("lama", "weight")), pser.drop(("lama", "weight")))
        self.assert_eq(
            kser.drop([("lama", "speed"), ("falcon", "weight")]),
            pser.drop([("lama", "speed"), ("falcon", "weight")]),
        )
        self.assert_eq(kser.drop({"lama": "speed"}), pser.drop({"lama": "speed"}))

        msg = "'level' should be less than the number of indexes"
        with self.assertRaisesRegex(ValueError, msg):
            kser.drop(labels="weight", level=2)

        msg = (
            "If the given index is a list, it "
            "should only contains names as all tuples or all non tuples "
            "that contain index names"
        )
        with self.assertRaisesRegex(ValueError, msg):
            kser.drop(["lama", ["cow", "falcon"]])

        msg = "Cannot specify both 'labels' and 'index'"
        with self.assertRaisesRegex(ValueError, msg):
            kser.drop("lama", index="cow")

        msg = r"'Key length \(2\) exceeds index depth \(3\)'"
        with self.assertRaisesRegex(KeyError, msg):
            kser.drop(("lama", "speed", "x"))

    def test_pop(self):
        midx = pd.MultiIndex(
            [["lama", "cow", "falcon"], ["speed", "weight", "length"]],
            [[0, 0, 0, 1, 1, 1, 2, 2, 2], [0, 1, 2, 0, 1, 2, 0, 1, 2]],
        )
        pdf = pd.DataFrame({"x": [45, 200, 1.2, 30, 250, 1.5, 320, 1, 0.3]}, index=midx)
        kdf = ps.from_pandas(pdf)

        pser = pdf.x
        kser = kdf.x

        self.assert_eq(kser.pop(("lama", "speed")), pser.pop(("lama", "speed")))
        self.assert_eq(kser, pser)
        self.assert_eq(kdf, pdf)

        msg = r"'Key length \(3\) exceeds index depth \(2\)'"
        with self.assertRaisesRegex(KeyError, msg):
            kser.pop(("lama", "speed", "x"))

    def test_replace(self):
        pser = pd.Series([10, 20, 15, 30, np.nan], name="x")
        kser = ps.Series(pser)

        self.assert_eq(kser.replace(), pser.replace())
        self.assert_eq(kser.replace({}), pser.replace({}))

        self.assert_eq(kser.replace(np.nan, 45), pser.replace(np.nan, 45))
        self.assert_eq(kser.replace([10, 15], 45), pser.replace([10, 15], 45))
        self.assert_eq(kser.replace((10, 15), 45), pser.replace((10, 15), 45))
        self.assert_eq(kser.replace([10, 15], [45, 50]), pser.replace([10, 15], [45, 50]))
        self.assert_eq(kser.replace((10, 15), (45, 50)), pser.replace((10, 15), (45, 50)))

        msg = "'to_replace' should be one of str, list, tuple, dict, int, float"
        with self.assertRaisesRegex(TypeError, msg):
            kser.replace(ps.range(5))
        msg = "Replacement lists must match in length. Expecting 3 got 2"
        with self.assertRaisesRegex(ValueError, msg):
            kser.replace([10, 20, 30], [1, 2])
        msg = "replace currently not support for regex"
        with self.assertRaisesRegex(NotImplementedError, msg):
            kser.replace(r"^1.$", regex=True)

    def test_xs(self):
        midx = pd.MultiIndex(
            [["a", "b", "c"], ["lama", "cow", "falcon"], ["speed", "weight", "length"]],
            [[0, 0, 0, 1, 1, 1, 2, 2, 2], [0, 0, 0, 1, 1, 1, 2, 2, 2], [0, 1, 2, 0, 1, 2, 0, 1, 2]],
        )
        pser = pd.Series([45, 200, 1.2, 30, 250, 1.5, 320, 1, 0.3], index=midx)
        kser = ps.from_pandas(pser)

        self.assert_eq(kser.xs(("a", "lama", "speed")), pser.xs(("a", "lama", "speed")))

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
                kser = ps.Series(pser)

                self.assert_eq(
                    pser.drop_duplicates(keep=keep).sort_values(),
                    kser.drop_duplicates(keep=keep).sort_values(),
                )

    def test_update(self):
        pser = pd.Series([10, 20, 15, 30, 45], name="x")
        kser = ps.Series(pser)

        msg = "'other' must be a Series"
        with self.assertRaisesRegex(TypeError, msg):
            kser.update(10)

    def test_where(self):
        pser1 = pd.Series([0, 1, 2, 3, 4])
        kser1 = ps.from_pandas(pser1)

        self.assert_eq(pser1.where(pser1 > 3), kser1.where(kser1 > 3).sort_index())

    def test_mask(self):
        pser1 = pd.Series([0, 1, 2, 3, 4])
        kser1 = ps.from_pandas(pser1)

        self.assert_eq(pser1.mask(pser1 > 3), kser1.mask(kser1 > 3).sort_index())

    def test_truncate(self):
        pser1 = pd.Series([10, 20, 30, 40, 50, 60, 70], index=[1, 2, 3, 4, 5, 6, 7])
        kser1 = ps.Series(pser1)
        pser2 = pd.Series([10, 20, 30, 40, 50, 60, 70], index=[7, 6, 5, 4, 3, 2, 1])
        kser2 = ps.Series(pser2)

        self.assert_eq(kser1.truncate(), pser1.truncate())
        self.assert_eq(kser1.truncate(before=2), pser1.truncate(before=2))
        self.assert_eq(kser1.truncate(after=5), pser1.truncate(after=5))
        self.assert_eq(kser1.truncate(copy=False), pser1.truncate(copy=False))
        self.assert_eq(kser1.truncate(2, 5, copy=False), pser1.truncate(2, 5, copy=False))
        # The bug for these tests has been fixed in pandas 1.1.0.
        if LooseVersion(pd.__version__) >= LooseVersion("1.1.0"):
            self.assert_eq(kser2.truncate(4, 6), pser2.truncate(4, 6))
            self.assert_eq(kser2.truncate(4, 6, copy=False), pser2.truncate(4, 6, copy=False))
        else:
            expected_kser = ps.Series([20, 30, 40], index=[6, 5, 4])
            self.assert_eq(kser2.truncate(4, 6), expected_kser)
            self.assert_eq(kser2.truncate(4, 6, copy=False), expected_kser)

        kser = ps.Series([10, 20, 30, 40, 50, 60, 70], index=[1, 2, 3, 4, 3, 2, 1])
        msg = "truncate requires a sorted index"
        with self.assertRaisesRegex(ValueError, msg):
            kser.truncate()

        kser = ps.Series([10, 20, 30, 40, 50, 60, 70], index=[1, 2, 3, 4, 5, 6, 7])
        msg = "Truncate: 2 must be after 5"
        with self.assertRaisesRegex(ValueError, msg):
            kser.truncate(5, 2)

    def test_getitem(self):
        pser = pd.Series([10, 20, 15, 30, 45], ["A", "A", "B", "C", "D"])
        kser = ps.Series(pser)

        self.assert_eq(kser["A"], pser["A"])
        self.assert_eq(kser["B"], pser["B"])
        self.assert_eq(kser[kser > 15], pser[pser > 15])

        # for MultiIndex
        midx = pd.MultiIndex(
            [["a", "b", "c"], ["lama", "cow", "falcon"], ["speed", "weight", "length"]],
            [[0, 0, 0, 0, 0, 0, 1, 1, 1], [0, 0, 0, 1, 1, 1, 2, 2, 2], [0, 0, 0, 0, 1, 2, 0, 1, 2]],
        )
        pser = pd.Series([45, 200, 1.2, 30, 250, 1.5, 320, 1, 0.3], name="0", index=midx)
        kser = ps.Series(pser)

        self.assert_eq(kser["a"], pser["a"])
        self.assert_eq(kser["a", "lama"], pser["a", "lama"])
        self.assert_eq(kser[kser > 1.5], pser[pser > 1.5])

        msg = r"'Key length \(4\) exceeds index depth \(3\)'"
        with self.assertRaisesRegex(KeyError, msg):
            kser[("a", "lama", "speed", "x")]

    def test_keys(self):
        midx = pd.MultiIndex(
            [["lama", "cow", "falcon"], ["speed", "weight", "length"]],
            [[0, 0, 0, 1, 1, 1, 2, 2, 2], [0, 1, 2, 0, 1, 2, 0, 1, 2]],
        )
        pser = pd.Series([45, 200, 1.2, 30, 250, 1.5, 320, 1, 0.3], index=midx)
        kser = ps.from_pandas(pser)

        self.assert_eq(kser.keys(), pser.keys())

    def test_index(self):
        # to check setting name of Index properly.
        idx = pd.Index([1, 2, 3, 4, 5, 6, 7, 8, 9])
        pser = pd.Series([45, 200, 1.2, 30, 250, 1.5, 320, 1, 0.3], index=idx)
        kser = ps.from_pandas(pser)

        kser.name = "koalas"
        pser.name = "koalas"
        self.assert_eq(kser.index.name, pser.index.name)

        # for check setting names of MultiIndex properly.
        kser.names = ["hello", "koalas"]
        pser.names = ["hello", "koalas"]
        self.assert_eq(kser.index.names, pser.index.names)

    def test_pct_change(self):
        pser = pd.Series([90, 91, 85], index=[2, 4, 1])
        kser = ps.from_pandas(pser)

        self.assert_eq(kser.pct_change(), pser.pct_change(), check_exact=False)
        self.assert_eq(kser.pct_change().sum(), pser.pct_change().sum(), almost=True)
        self.assert_eq(kser.pct_change(periods=2), pser.pct_change(periods=2), check_exact=False)
        self.assert_eq(kser.pct_change(periods=-1), pser.pct_change(periods=-1), check_exact=False)
        self.assert_eq(kser.pct_change(periods=-100000000), pser.pct_change(periods=-100000000))
        self.assert_eq(kser.pct_change(periods=100000000), pser.pct_change(periods=100000000))

        # for MultiIndex
        midx = pd.MultiIndex(
            [["lama", "cow", "falcon"], ["speed", "weight", "length"]],
            [[0, 0, 0, 1, 1, 1, 2, 2, 2], [0, 1, 2, 0, 1, 2, 0, 1, 2]],
        )
        pser = pd.Series([45, 200, 1.2, 30, 250, 1.5, 320, 1, 0.3], index=midx)
        kser = ps.from_pandas(pser)

        self.assert_eq(kser.pct_change(), pser.pct_change(), check_exact=False)
        self.assert_eq(kser.pct_change().sum(), pser.pct_change().sum(), almost=True)
        self.assert_eq(kser.pct_change(periods=2), pser.pct_change(periods=2), check_exact=False)
        self.assert_eq(kser.pct_change(periods=-1), pser.pct_change(periods=-1), check_exact=False)
        self.assert_eq(kser.pct_change(periods=-100000000), pser.pct_change(periods=-100000000))
        self.assert_eq(kser.pct_change(periods=100000000), pser.pct_change(periods=100000000))

    def test_axes(self):
        pser = pd.Series([90, 91, 85], index=[2, 4, 1])
        kser = ps.from_pandas(pser)
        self.assert_eq(kser.axes, pser.axes)

        # for MultiIndex
        midx = pd.MultiIndex(
            [["lama", "cow", "falcon"], ["speed", "weight", "length"]],
            [[0, 0, 0, 1, 1, 1, 2, 2, 2], [0, 1, 2, 0, 1, 2, 0, 1, 2]],
        )
        pser = pd.Series([45, 200, 1.2, 30, 250, 1.5, 320, 1, 0.3], index=midx)
        kser = ps.from_pandas(pser)
        self.assert_eq(kser.axes, pser.axes)

    def test_udt(self):
        sparse_values = {0: 0.1, 1: 1.1}
        sparse_vector = SparseVector(len(sparse_values), sparse_values)
        pser = pd.Series([sparse_vector])
        kser = ps.from_pandas(pser)
        self.assert_eq(kser, pser)

    def test_repeat(self):
        pser = pd.Series(["a", "b", "c"], name="0", index=np.random.rand(3))
        kser = ps.from_pandas(pser)

        self.assert_eq(kser.repeat(3).sort_index(), pser.repeat(3).sort_index())
        self.assert_eq(kser.repeat(0).sort_index(), pser.repeat(0).sort_index())

        self.assertRaises(ValueError, lambda: kser.repeat(-1))
        self.assertRaises(TypeError, lambda: kser.repeat("abc"))

        pdf = pd.DataFrame({"a": ["a", "b", "c"], "rep": [10, 20, 30]}, index=np.random.rand(3))
        kdf = ps.from_pandas(pdf)

        self.assert_eq(kdf.a.repeat(kdf.rep).sort_index(), pdf.a.repeat(pdf.rep).sort_index())

    def test_take(self):
        pser = pd.Series([100, 200, 300, 400, 500], name="Koalas")
        kser = ps.from_pandas(pser)

        self.assert_eq(kser.take([0, 2, 4]).sort_values(), pser.take([0, 2, 4]).sort_values())
        self.assert_eq(
            kser.take(range(0, 5, 2)).sort_values(), pser.take(range(0, 5, 2)).sort_values()
        )
        self.assert_eq(kser.take([-4, -2, 0]).sort_values(), pser.take([-4, -2, 0]).sort_values())
        self.assert_eq(
            kser.take(range(-2, 1, 2)).sort_values(), pser.take(range(-2, 1, 2)).sort_values()
        )

        # Checking the type of indices.
        self.assertRaises(TypeError, lambda: kser.take(1))
        self.assertRaises(TypeError, lambda: kser.take("1"))
        self.assertRaises(TypeError, lambda: kser.take({1, 2}))
        self.assertRaises(TypeError, lambda: kser.take({1: None, 2: None}))

    def test_divmod(self):
        pser = pd.Series([100, None, 300, None, 500], name="Koalas")
        kser = ps.from_pandas(pser)

        if LooseVersion(pd.__version__) >= LooseVersion("1.0.0"):
            kdiv, kmod = kser.divmod(-100)
            pdiv, pmod = pser.divmod(-100)
            self.assert_eq(kdiv, pdiv)
            self.assert_eq(kmod, pmod)

            kdiv, kmod = kser.divmod(100)
            pdiv, pmod = pser.divmod(100)
            self.assert_eq(kdiv, pdiv)
            self.assert_eq(kmod, pmod)
        elif LooseVersion(pd.__version__) < LooseVersion("1.0.0"):
            kdiv, kmod = kser.divmod(-100)
            pdiv, pmod = pser.floordiv(-100), pser.mod(-100)
            self.assert_eq(kdiv, pdiv)
            self.assert_eq(kmod, pmod)

            kdiv, kmod = kser.divmod(100)
            pdiv, pmod = pser.floordiv(100), pser.mod(100)
            self.assert_eq(kdiv, pdiv)
            self.assert_eq(kmod, pmod)

    def test_rdivmod(self):
        pser = pd.Series([100, None, 300, None, 500])
        kser = ps.from_pandas(pser)

        if LooseVersion(pd.__version__) >= LooseVersion("1.0.0"):
            krdiv, krmod = kser.rdivmod(-100)
            prdiv, prmod = pser.rdivmod(-100)
            self.assert_eq(krdiv, prdiv)
            self.assert_eq(krmod, prmod)

            krdiv, krmod = kser.rdivmod(100)
            prdiv, prmod = pser.rdivmod(100)
            self.assert_eq(krdiv, prdiv)
            self.assert_eq(krmod, prmod)
        elif LooseVersion(pd.__version__) < LooseVersion("1.0.0"):
            krdiv, krmod = kser.rdivmod(-100)
            prdiv, prmod = pser.rfloordiv(-100), pser.rmod(-100)
            self.assert_eq(krdiv, prdiv)
            self.assert_eq(krmod, prmod)

            krdiv, krmod = kser.rdivmod(100)
            prdiv, prmod = pser.rfloordiv(100), pser.rmod(100)
            self.assert_eq(krdiv, prdiv)
            self.assert_eq(krmod, prmod)

    def test_mod(self):
        pser = pd.Series([100, None, -300, None, 500, -700], name="Koalas")
        kser = ps.from_pandas(pser)

        self.assert_eq(kser.mod(-150), pser.mod(-150))
        self.assert_eq(kser.mod(0), pser.mod(0))
        self.assert_eq(kser.mod(150), pser.mod(150))

        pdf = pd.DataFrame({"a": [100, None, -300, None, 500, -700], "b": [150] * 6})
        kdf = ps.from_pandas(pdf)
        self.assert_eq(kdf.a.mod(kdf.b), pdf.a.mod(pdf.b))

    def test_mode(self):
        pser = pd.Series([0, 0, 1, 1, 1, np.nan, np.nan, np.nan])
        kser = ps.from_pandas(pser)
        self.assert_eq(kser.mode(), pser.mode())
        if LooseVersion(pd.__version__) >= LooseVersion("0.24"):
            # The `dropna` argument is added in pandas 0.24.
            self.assert_eq(
                kser.mode(dropna=False).sort_values().reset_index(drop=True),
                pser.mode(dropna=False).sort_values().reset_index(drop=True),
            )

        pser.name = "x"
        kser = ps.from_pandas(pser)
        self.assert_eq(kser.mode(), pser.mode())
        if LooseVersion(pd.__version__) >= LooseVersion("0.24"):
            # The `dropna` argument is added in pandas 0.24.
            self.assert_eq(
                kser.mode(dropna=False).sort_values().reset_index(drop=True),
                pser.mode(dropna=False).sort_values().reset_index(drop=True),
            )

    def test_rmod(self):
        pser = pd.Series([100, None, -300, None, 500, -700], name="Koalas")
        kser = ps.from_pandas(pser)

        self.assert_eq(kser.rmod(-150), pser.rmod(-150))
        self.assert_eq(kser.rmod(0), pser.rmod(0))
        self.assert_eq(kser.rmod(150), pser.rmod(150))

        pdf = pd.DataFrame({"a": [100, None, -300, None, 500, -700], "b": [150] * 6})
        kdf = ps.from_pandas(pdf)
        self.assert_eq(kdf.a.rmod(kdf.b), pdf.a.rmod(pdf.b))

    def test_asof(self):
        pser = pd.Series([1, 2, np.nan, 4], index=[10, 20, 30, 40], name="Koalas")
        kser = ps.from_pandas(pser)

        self.assert_eq(kser.asof(20), pser.asof(20))
        self.assert_eq(kser.asof([5, 20]).sort_index(), pser.asof([5, 20]).sort_index())
        self.assert_eq(kser.asof(100), pser.asof(100))
        self.assert_eq(repr(kser.asof(-100)), repr(pser.asof(-100)))
        self.assert_eq(kser.asof([-100, 100]).sort_index(), pser.asof([-100, 100]).sort_index())

        # where cannot be an Index, Series or a DataFrame
        self.assertRaises(ValueError, lambda: kser.asof(ps.Index([-100, 100])))
        self.assertRaises(ValueError, lambda: kser.asof(ps.Series([-100, 100])))
        self.assertRaises(ValueError, lambda: kser.asof(ps.DataFrame({"A": [1, 2, 3]})))
        # asof is not supported for a MultiIndex
        pser.index = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c"), ("y", "d")])
        kser = ps.from_pandas(pser)
        self.assertRaises(ValueError, lambda: kser.asof(20))
        # asof requires a sorted index (More precisely, should be a monotonic increasing)
        kser = ps.Series([1, 2, np.nan, 4], index=[10, 30, 20, 40], name="Koalas")
        self.assertRaises(ValueError, lambda: kser.asof(20))
        kser = ps.Series([1, 2, np.nan, 4], index=[40, 30, 20, 10], name="Koalas")
        self.assertRaises(ValueError, lambda: kser.asof(20))

        pidx = pd.DatetimeIndex(["2013-12-31", "2014-01-02", "2014-01-03"])
        pser = pd.Series([1, 2, np.nan], index=pidx)
        kser = ps.from_pandas(pser)

        self.assert_eq(kser.asof("2014-01-01"), pser.asof("2014-01-01"))
        self.assert_eq(kser.asof("2014-01-02"), pser.asof("2014-01-02"))
        self.assert_eq(repr(kser.asof("1999-01-02")), repr(pser.asof("1999-01-02")))

    def test_squeeze(self):
        # Single value
        pser = pd.Series([90])
        kser = ps.from_pandas(pser)
        self.assert_eq(kser.squeeze(), pser.squeeze())

        # Single value with MultiIndex
        midx = pd.MultiIndex.from_tuples([("a", "b", "c")])
        pser = pd.Series([90], index=midx)
        kser = ps.from_pandas(pser)
        self.assert_eq(kser.squeeze(), pser.squeeze())

        # Multiple values
        pser = pd.Series([90, 91, 85])
        kser = ps.from_pandas(pser)
        self.assert_eq(kser.squeeze(), pser.squeeze())

        # Multiple values with MultiIndex
        midx = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        pser = pd.Series([90, 91, 85], index=midx)
        kser = ps.from_pandas(pser)
        self.assert_eq(kser.squeeze(), pser.squeeze())

    def test_swaplevel(self):
        # MultiIndex with two levels
        arrays = [[1, 1, 2, 2], ["red", "blue", "red", "blue"]]
        pidx = pd.MultiIndex.from_arrays(arrays, names=("number", "color"))
        pser = pd.Series(["a", "b", "c", "d"], index=pidx)
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.swaplevel(), kser.swaplevel())
        self.assert_eq(pser.swaplevel(0, 1), kser.swaplevel(0, 1))
        self.assert_eq(pser.swaplevel(1, 1), kser.swaplevel(1, 1))
        self.assert_eq(pser.swaplevel("number", "color"), kser.swaplevel("number", "color"))

        # MultiIndex with more than two levels
        arrays = [[1, 1, 2, 2], ["red", "blue", "red", "blue"], ["l", "m", "s", "xs"]]
        pidx = pd.MultiIndex.from_arrays(arrays, names=("number", "color", "size"))
        pser = pd.Series(["a", "b", "c", "d"], index=pidx)
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.swaplevel(), kser.swaplevel())
        self.assert_eq(pser.swaplevel(0, 1), kser.swaplevel(0, 1))
        self.assert_eq(pser.swaplevel(0, 2), kser.swaplevel(0, 2))
        self.assert_eq(pser.swaplevel(1, 2), kser.swaplevel(1, 2))
        self.assert_eq(pser.swaplevel(1, 1), kser.swaplevel(1, 1))
        self.assert_eq(pser.swaplevel(-1, -2), kser.swaplevel(-1, -2))
        self.assert_eq(pser.swaplevel("number", "color"), kser.swaplevel("number", "color"))
        self.assert_eq(pser.swaplevel("number", "size"), kser.swaplevel("number", "size"))
        self.assert_eq(pser.swaplevel("color", "size"), kser.swaplevel("color", "size"))

        # Error conditions
        self.assertRaises(AssertionError, lambda: ps.Series([1, 2]).swaplevel())
        self.assertRaises(IndexError, lambda: kser.swaplevel(0, 9))
        self.assertRaises(KeyError, lambda: kser.swaplevel("not_number", "color"))
        self.assertRaises(AssertionError, lambda: kser.swaplevel(copy=False))

    def test_swapaxes(self):
        pser = pd.Series([1, 2, 3], index=["x", "y", "z"], name="ser")
        kser = ps.from_pandas(pser)

        self.assert_eq(kser.swapaxes(0, 0), pser.swapaxes(0, 0))
        self.assert_eq(kser.swapaxes("index", "index"), pser.swapaxes("index", "index"))
        self.assert_eq((kser + 1).swapaxes(0, 0), (pser + 1).swapaxes(0, 0))

        self.assertRaises(AssertionError, lambda: kser.swapaxes(0, 1, copy=False))
        self.assertRaises(ValueError, lambda: kser.swapaxes(0, 1))
        self.assertRaises(ValueError, lambda: kser.swapaxes("index", "columns"))

    def test_div_zero_and_nan(self):
        pser = pd.Series([100, None, -300, None, 500, -700, np.inf, -np.inf], name="Koalas")
        kser = ps.from_pandas(pser)

        self.assert_eq(pser.div(0), kser.div(0))
        self.assert_eq(pser.truediv(0), kser.truediv(0))
        self.assert_eq(pser / 0, kser / 0)
        self.assert_eq(pser.div(np.nan), kser.div(np.nan))
        self.assert_eq(pser.truediv(np.nan), kser.truediv(np.nan))
        self.assert_eq(pser / np.nan, kser / np.nan)

        # floordiv has different behavior in pandas > 1.0.0 when divide by 0
        if LooseVersion(pd.__version__) >= LooseVersion("1.0.0"):
            self.assert_eq(pser.floordiv(0), kser.floordiv(0))
            self.assert_eq(pser // 0, kser // 0)
        else:
            result = pd.Series(
                [np.inf, np.nan, -np.inf, np.nan, np.inf, -np.inf, np.inf, -np.inf], name="Koalas"
            )
            self.assert_eq(kser.floordiv(0), result)
            self.assert_eq(kser // 0, result)
        self.assert_eq(pser.floordiv(np.nan), kser.floordiv(np.nan))

    def test_mad(self):
        pser = pd.Series([1, 2, 3, 4], name="Koalas")
        kser = ps.from_pandas(pser)

        self.assert_eq(pser.mad(), kser.mad())

        pser = pd.Series([None, -2, 5, 10, 50, np.nan, -20], name="Koalas")
        kser = ps.from_pandas(pser)

        self.assert_eq(pser.mad(), kser.mad())

        pmidx = pd.MultiIndex.from_tuples(
            [("a", "1"), ("a", "2"), ("b", "1"), ("b", "2"), ("c", "1")]
        )
        pser = pd.Series([1, 2, 3, 4, 5], name="Koalas")
        pser.index = pmidx
        kser = ps.from_pandas(pser)

        self.assert_eq(pser.mad(), kser.mad())

        pmidx = pd.MultiIndex.from_tuples(
            [("a", "1"), ("a", "2"), ("b", "1"), ("b", "2"), ("c", "1")]
        )
        pser = pd.Series([None, -2, 5, 50, np.nan], name="Koalas")
        pser.index = pmidx
        kser = ps.from_pandas(pser)

        self.assert_eq(pser.mad(), kser.mad())

    def test_to_frame(self):
        pser = pd.Series(["a", "b", "c"])
        kser = ps.from_pandas(pser)

        self.assert_eq(pser.to_frame(name="a"), kser.to_frame(name="a"))

        # for MultiIndex
        midx = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        pser = pd.Series(["a", "b", "c"], index=midx)
        kser = ps.from_pandas(pser)

        self.assert_eq(pser.to_frame(name="a"), kser.to_frame(name="a"))

    def test_shape(self):
        pser = pd.Series(["a", "b", "c"])
        kser = ps.from_pandas(pser)

        self.assert_eq(pser.shape, kser.shape)

        # for MultiIndex
        midx = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        pser = pd.Series(["a", "b", "c"], index=midx)
        kser = ps.from_pandas(pser)

        self.assert_eq(pser.shape, kser.shape)

    @unittest.skipIf(not have_tabulate, tabulate_requirement_message)
    def test_to_markdown(self):
        pser = pd.Series(["elk", "pig", "dog", "quetzal"], name="animal")
        kser = ps.from_pandas(pser)

        # `to_markdown()` is supported in pandas >= 1.0.0 since it's newly added in pandas 1.0.0.
        if LooseVersion(pd.__version__) < LooseVersion("1.0.0"):
            self.assertRaises(NotImplementedError, lambda: kser.to_markdown())
        else:
            self.assert_eq(pser.to_markdown(), kser.to_markdown())

    def test_unstack(self):
        pser = pd.Series(
            [10, -2, 4, 7],
            index=pd.MultiIndex.from_tuples(
                [("one", "a", "z"), ("one", "b", "x"), ("two", "a", "c"), ("two", "b", "v")],
                names=["A", "B", "C"],
            ),
        )
        kser = ps.from_pandas(pser)

        levels = [-3, -2, -1, 0, 1, 2]
        for level in levels:
            pandas_result = pser.unstack(level=level)
            koalas_result = kser.unstack(level=level).sort_index()
            self.assert_eq(pandas_result, koalas_result)
            self.assert_eq(pandas_result.index.names, koalas_result.index.names)
            self.assert_eq(pandas_result.columns.names, koalas_result.columns.names)

        # non-numeric datatypes
        pser = pd.Series(
            list("abcd"), index=pd.MultiIndex.from_product([["one", "two"], ["a", "b"]])
        )
        kser = ps.from_pandas(pser)

        levels = [-2, -1, 0, 1]
        for level in levels:
            pandas_result = pser.unstack(level=level)
            koalas_result = kser.unstack(level=level).sort_index()
            self.assert_eq(pandas_result, koalas_result)
            self.assert_eq(pandas_result.index.names, koalas_result.index.names)
            self.assert_eq(pandas_result.columns.names, koalas_result.columns.names)

        # Exceeding the range of level
        self.assertRaises(IndexError, lambda: kser.unstack(level=3))
        self.assertRaises(IndexError, lambda: kser.unstack(level=-4))
        # Only support for MultiIndex
        kser = ps.Series([10, -2, 4, 7])
        self.assertRaises(ValueError, lambda: kser.unstack())

    def test_item(self):
        kser = ps.Series([10, 20])
        self.assertRaises(ValueError, lambda: kser.item())

    def test_filter(self):
        pser = pd.Series([0, 1, 2], index=["one", "two", "three"])
        kser = ps.from_pandas(pser)

        self.assert_eq(pser.filter(items=["one", "three"]), kser.filter(items=["one", "three"]))
        self.assert_eq(pser.filter(regex="e$"), kser.filter(regex="e$"))
        self.assert_eq(pser.filter(like="hre"), kser.filter(like="hre"))

        with self.assertRaisesRegex(ValueError, "Series does not support columns axis."):
            kser.filter(like="hre", axis=1)

        # for MultiIndex
        midx = pd.MultiIndex.from_tuples([("one", "x"), ("two", "y"), ("three", "z")])
        pser = pd.Series([0, 1, 2], index=midx)
        kser = ps.from_pandas(pser)

        self.assert_eq(
            pser.filter(items=[("one", "x"), ("three", "z")]),
            kser.filter(items=[("one", "x"), ("three", "z")]),
        )

        with self.assertRaisesRegex(TypeError, "Unsupported type list"):
            kser.filter(items=[["one", "x"], ("three", "z")])

        with self.assertRaisesRegex(ValueError, "The item should not be empty."):
            kser.filter(items=[(), ("three", "z")])

    def test_abs(self):
        pser = pd.Series([-2, -1, 0, 1])
        kser = ps.from_pandas(pser)

        self.assert_eq(abs(kser), abs(pser))
        self.assert_eq(np.abs(kser), np.abs(pser))

    def test_bfill(self):
        pdf = pd.DataFrame({"x": [np.nan, 2, 3, 4, np.nan, 6], "y": [np.nan, 2, 3, 4, np.nan, 6]})
        kdf = ps.from_pandas(pdf)

        pser = pdf.x
        kser = kdf.x

        self.assert_eq(kser.bfill(), pser.bfill())
        self.assert_eq(kser.bfill()[0], pser.bfill()[0])

        kser.bfill(inplace=True)
        pser.bfill(inplace=True)
        self.assert_eq(kser, pser)
        self.assert_eq(kser[0], pser[0])
        self.assert_eq(kdf, pdf)

    def test_ffill(self):
        pdf = pd.DataFrame({"x": [np.nan, 2, 3, 4, np.nan, 6], "y": [np.nan, 2, 3, 4, np.nan, 6]})
        kdf = ps.from_pandas(pdf)

        pser = pdf.x
        kser = kdf.x

        self.assert_eq(kser.ffill(), pser.ffill())
        self.assert_eq(kser.ffill()[4], pser.ffill()[4])

        kser.ffill(inplace=True)
        pser.ffill(inplace=True)
        self.assert_eq(kser, pser)
        self.assert_eq(kser[4], pser[4])
        self.assert_eq(kdf, pdf)

    def test_iteritems(self):
        pser = pd.Series(["A", "B", "C"])
        kser = ps.from_pandas(pser)

        for (p_name, p_items), (k_name, k_items) in zip(pser.iteritems(), kser.iteritems()):
            self.assert_eq(p_name, k_name)
            self.assert_eq(p_items, k_items)

    def test_droplevel(self):
        # droplevel is new in pandas 0.24.0
        if LooseVersion(pd.__version__) >= LooseVersion("0.24.0"):
            pser = pd.Series(
                [1, 2, 3],
                index=pd.MultiIndex.from_tuples(
                    [("x", "a", "q"), ("x", "b", "w"), ("y", "c", "e")],
                    names=["level_1", "level_2", "level_3"],
                ),
            )
            kser = ps.from_pandas(pser)

            self.assert_eq(pser.droplevel(0), kser.droplevel(0))
            self.assert_eq(pser.droplevel("level_1"), kser.droplevel("level_1"))
            self.assert_eq(pser.droplevel(-1), kser.droplevel(-1))
            self.assert_eq(pser.droplevel([0]), kser.droplevel([0]))
            self.assert_eq(pser.droplevel(["level_1"]), kser.droplevel(["level_1"]))
            self.assert_eq(pser.droplevel((0,)), kser.droplevel((0,)))
            self.assert_eq(pser.droplevel(("level_1",)), kser.droplevel(("level_1",)))
            self.assert_eq(pser.droplevel([0, 2]), kser.droplevel([0, 2]))
            self.assert_eq(
                pser.droplevel(["level_1", "level_3"]), kser.droplevel(["level_1", "level_3"])
            )
            self.assert_eq(pser.droplevel((1, 2)), kser.droplevel((1, 2)))
            self.assert_eq(
                pser.droplevel(("level_2", "level_3")), kser.droplevel(("level_2", "level_3"))
            )

            with self.assertRaisesRegex(KeyError, "Level {0, 1, 2} not found"):
                kser.droplevel({0, 1, 2})
            with self.assertRaisesRegex(KeyError, "Level level_100 not found"):
                kser.droplevel(["level_1", "level_100"])
            with self.assertRaisesRegex(
                IndexError, "Too many levels: Index has only 3 levels, not 11"
            ):
                kser.droplevel(10)
            with self.assertRaisesRegex(
                IndexError,
                "Too many levels: Index has only 3 levels, -10 is not a valid level number",
            ):
                kser.droplevel(-10)
            with self.assertRaisesRegex(
                ValueError,
                "Cannot remove 3 levels from an index with 3 levels: "
                "at least one level must be left.",
            ):
                kser.droplevel([0, 1, 2])
            with self.assertRaisesRegex(
                ValueError,
                "Cannot remove 5 levels from an index with 3 levels: "
                "at least one level must be left.",
            ):
                kser.droplevel([1, 1, 1, 1, 1])

            # Tupled names
            pser.index.names = [("a", "1"), ("b", "2"), ("c", "3")]
            kser = ps.from_pandas(pser)

            self.assert_eq(
                pser.droplevel([("a", "1"), ("c", "3")]), kser.droplevel([("a", "1"), ("c", "3")])
            )

    def test_dot(self):
        pdf = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        kdf = ps.from_pandas(pdf)

        self.assert_eq((kdf["b"] * 10).dot(kdf["a"]), (pdf["b"] * 10).dot(pdf["a"]))
        self.assert_eq((kdf["b"] * 10).dot(kdf), (pdf["b"] * 10).dot(pdf))
        self.assert_eq((kdf["b"] * 10).dot(kdf + 1), (pdf["b"] * 10).dot(pdf + 1))

    def test_tail(self):
        pser = pd.Series(range(1000), name="Koalas")
        kser = ps.from_pandas(pser)

        self.assert_eq(pser.tail(), kser.tail())
        self.assert_eq(pser.tail(10), kser.tail(10))
        self.assert_eq(pser.tail(-990), kser.tail(-990))
        self.assert_eq(pser.tail(0), kser.tail(0))
        self.assert_eq(pser.tail(1001), kser.tail(1001))
        self.assert_eq(pser.tail(-1001), kser.tail(-1001))
        self.assert_eq((pser + 1).tail(), (kser + 1).tail())
        self.assert_eq((pser + 1).tail(10), (kser + 1).tail(10))
        self.assert_eq((pser + 1).tail(-990), (kser + 1).tail(-990))
        self.assert_eq((pser + 1).tail(0), (kser + 1).tail(0))
        self.assert_eq((pser + 1).tail(1001), (kser + 1).tail(1001))
        self.assert_eq((pser + 1).tail(-1001), (kser + 1).tail(-1001))
        with self.assertRaisesRegex(TypeError, "bad operand type for unary -: 'str'"):
            kser.tail("10")

    def test_product(self):
        pser = pd.Series([10, 20, 30, 40, 50])
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.prod(), kser.prod())

        # Containing NA values
        pser = pd.Series([10, np.nan, 30, np.nan, 50])
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.prod(), kser.prod(), almost=True)

        # All-NA values
        pser = pd.Series([np.nan, np.nan, np.nan])
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.prod(), kser.prod())

        # Empty Series
        pser = pd.Series([])
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.prod(), kser.prod())

        # Boolean Series
        pser = pd.Series([True, True, True])
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.prod(), kser.prod())

        pser = pd.Series([False, False, False])
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.prod(), kser.prod())

        pser = pd.Series([True, False, True])
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.prod(), kser.prod())

        # With `min_count` parameter
        pser = pd.Series([10, 20, 30, 40, 50])
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.prod(min_count=5), kser.prod(min_count=5))
        self.assert_eq(pser.prod(min_count=6), kser.prod(min_count=6))

        pser = pd.Series([10, np.nan, 30, np.nan, 50])
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.prod(min_count=3), kser.prod(min_count=3), almost=True)
        self.assert_eq(pser.prod(min_count=4), kser.prod(min_count=4))

        pser = pd.Series([np.nan, np.nan, np.nan])
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.prod(min_count=1), kser.prod(min_count=1))

        pser = pd.Series([])
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.prod(min_count=1), kser.prod(min_count=1))

        with self.assertRaisesRegex(TypeError, "Could not convert object \\(string\\) to numeric"):
            ps.Series(["a", "b", "c"]).prod()
        with self.assertRaisesRegex(
            TypeError, "Could not convert datetime64\\[ns\\] \\(timestamp\\) to numeric"
        ):
            ps.Series([pd.Timestamp("2016-01-01") for _ in range(3)]).prod()

    def test_hasnans(self):
        # BooleanType
        pser = pd.Series([True, False, True, True])
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.hasnans, kser.hasnans)

        pser = pd.Series([True, False, np.nan, True])
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.hasnans, kser.hasnans)

        # TimestampType
        pser = pd.Series([pd.Timestamp("2020-07-30") for _ in range(3)])
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.hasnans, kser.hasnans)

        pser = pd.Series([pd.Timestamp("2020-07-30"), np.nan, pd.Timestamp("2020-07-30")])
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.hasnans, kser.hasnans)

    def test_last_valid_index(self):
        pser = pd.Series([250, 1.5, 320, 1, 0.3, None, None, None, None])
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.last_valid_index(), kser.last_valid_index())

        # MultiIndex columns
        midx = pd.MultiIndex(
            [["lama", "cow", "falcon"], ["speed", "weight", "length"]],
            [[0, 0, 0, 1, 1, 1, 2, 2, 2], [0, 1, 2, 0, 1, 2, 0, 1, 2]],
        )
        pser.index = midx
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.last_valid_index(), kser.last_valid_index())

        # Empty Series
        pser = pd.Series([])
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.last_valid_index(), kser.last_valid_index())

    def test_first_valid_index(self):
        # Empty Series
        pser = pd.Series([])
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.first_valid_index(), kser.first_valid_index())

    def test_factorize(self):
        pser = pd.Series(["a", "b", "a", "b"])
        kser = ps.from_pandas(pser)
        pcodes, puniques = pser.factorize(sort=True)
        kcodes, kuniques = kser.factorize()
        self.assert_eq(pcodes.tolist(), kcodes.to_list())
        self.assert_eq(puniques, kuniques)

        pser = pd.Series([5, 1, 5, 1])
        kser = ps.from_pandas(pser)
        pcodes, puniques = (pser + 1).factorize(sort=True)
        kcodes, kuniques = (kser + 1).factorize()
        self.assert_eq(pcodes.tolist(), kcodes.to_list())
        self.assert_eq(puniques, kuniques)

        pser = pd.Series(["a", "b", "a", "b"], name="ser", index=["w", "x", "y", "z"])
        kser = ps.from_pandas(pser)
        pcodes, puniques = pser.factorize(sort=True)
        kcodes, kuniques = kser.factorize()
        self.assert_eq(pcodes.tolist(), kcodes.to_list())
        self.assert_eq(puniques, kuniques)

        pser = pd.Series(
            ["a", "b", "a", "b"], index=pd.MultiIndex.from_arrays([[4, 3, 2, 1], [1, 2, 3, 4]])
        )
        kser = ps.from_pandas(pser)
        pcodes, puniques = pser.factorize(sort=True)
        kcodes, kuniques = kser.factorize()
        self.assert_eq(pcodes.tolist(), kcodes.to_list())
        self.assert_eq(puniques, kuniques)

        #
        # Deals with None and np.nan
        #
        pser = pd.Series(["a", "b", "a", np.nan])
        kser = ps.from_pandas(pser)
        pcodes, puniques = pser.factorize(sort=True)
        kcodes, kuniques = kser.factorize()
        self.assert_eq(pcodes.tolist(), kcodes.to_list())
        self.assert_eq(puniques, kuniques)

        pser = pd.Series([1, None, 3, 2, 1])
        kser = ps.from_pandas(pser)
        pcodes, puniques = pser.factorize(sort=True)
        kcodes, kuniques = kser.factorize()
        self.assert_eq(pcodes.tolist(), kcodes.to_list())
        self.assert_eq(puniques, kuniques)

        pser = pd.Series(["a", None, "a"])
        kser = ps.from_pandas(pser)
        pcodes, puniques = pser.factorize(sort=True)
        kcodes, kuniques = kser.factorize()
        self.assert_eq(pcodes.tolist(), kcodes.to_list())
        self.assert_eq(puniques, kuniques)

        pser = pd.Series([None, np.nan])
        kser = ps.from_pandas(pser)
        pcodes, puniques = pser.factorize()
        kcodes, kuniques = kser.factorize()
        self.assert_eq(pcodes, kcodes.to_list())
        # pandas: Float64Index([], dtype='float64')
        self.assert_eq(pd.Index([]), kuniques)

        pser = pd.Series([np.nan, np.nan])
        kser = ps.from_pandas(pser)
        pcodes, puniques = pser.factorize()
        kcodes, kuniques = kser.factorize()
        self.assert_eq(pcodes, kcodes.to_list())
        # pandas: Float64Index([], dtype='float64')
        self.assert_eq(pd.Index([]), kuniques)

        #
        # Deals with na_sentinel
        #
        # pandas >= 1.1.2 support na_sentinel=None
        # pandas >= 0.24 support na_sentinel not to be -1
        #
        pd_below_1_1_2 = LooseVersion(pd.__version__) < LooseVersion("1.1.2")
        pd_below_0_24 = LooseVersion(pd.__version__) < LooseVersion("0.24")

        pser = pd.Series(["a", "b", "a", np.nan, None])
        kser = ps.from_pandas(pser)

        pcodes, puniques = pser.factorize(sort=True, na_sentinel=-2)
        kcodes, kuniques = kser.factorize(na_sentinel=-2)
        self.assert_eq([0, 1, 0, -2, -2] if pd_below_0_24 else pcodes.tolist(), kcodes.to_list())
        self.assert_eq(puniques, kuniques)

        pcodes, puniques = pser.factorize(sort=True, na_sentinel=2)
        kcodes, kuniques = kser.factorize(na_sentinel=2)
        self.assert_eq([0, 1, 0, 2, 2] if pd_below_0_24 else pcodes.tolist(), kcodes.to_list())
        self.assert_eq(puniques, kuniques)

        if not pd_below_1_1_2:
            pcodes, puniques = pser.factorize(sort=True, na_sentinel=None)
            kcodes, kuniques = kser.factorize(na_sentinel=None)
            self.assert_eq(pcodes.tolist(), kcodes.to_list())
            # puniques is Index(['a', 'b', nan], dtype='object')
            self.assert_eq(ps.Index(["a", "b", None]), kuniques)

            kser = ps.Series([1, 2, np.nan, 4, 5])  # Arrow takes np.nan as null
            kser.loc[3] = np.nan  # Spark takes np.nan as NaN
            kcodes, kuniques = kser.factorize(na_sentinel=None)
            pcodes, puniques = kser.to_pandas().factorize(sort=True, na_sentinel=None)
            self.assert_eq(pcodes.tolist(), kcodes.to_list())
            self.assert_eq(puniques, kuniques)

    def test_pad(self):
        pser = pd.Series([np.nan, 2, 3, 4, np.nan, 6], name="x")
        kser = ps.from_pandas(pser)

        if LooseVersion(pd.__version__) >= LooseVersion("1.1"):
            self.assert_eq(pser.pad(), kser.pad())

            # Test `inplace=True`
            pser.pad(inplace=True)
            kser.pad(inplace=True)
            self.assert_eq(pser, kser)
        else:
            expected = ps.Series([np.nan, 2, 3, 4, 4, 6], name="x")
            self.assert_eq(expected, kser.pad())

            # Test `inplace=True`
            kser.pad(inplace=True)
            self.assert_eq(expected, kser)

    def test_explode(self):
        if LooseVersion(pd.__version__) >= LooseVersion("0.25"):
            pser = pd.Series([[1, 2, 3], [], None, [3, 4]])
            kser = ps.from_pandas(pser)
            self.assert_eq(pser.explode(), kser.explode(), almost=True)

            # MultiIndex
            pser.index = pd.MultiIndex.from_tuples([("a", "w"), ("b", "x"), ("c", "y"), ("d", "z")])
            kser = ps.from_pandas(pser)
            self.assert_eq(pser.explode(), kser.explode(), almost=True)

            # non-array type Series
            pser = pd.Series([1, 2, 3, 4])
            kser = ps.from_pandas(pser)
            self.assert_eq(pser.explode(), kser.explode())
        else:
            pser = pd.Series([[1, 2, 3], [], None, [3, 4]])
            kser = ps.from_pandas(pser)
            expected = pd.Series([1.0, 2.0, 3.0, None, None, 3.0, 4.0], index=[0, 0, 0, 1, 2, 3, 3])
            self.assert_eq(kser.explode(), expected)

            # MultiIndex
            pser.index = pd.MultiIndex.from_tuples([("a", "w"), ("b", "x"), ("c", "y"), ("d", "z")])
            kser = ps.from_pandas(pser)
            expected = pd.Series(
                [1.0, 2.0, 3.0, None, None, 3.0, 4.0],
                index=pd.MultiIndex.from_tuples(
                    [
                        ("a", "w"),
                        ("a", "w"),
                        ("a", "w"),
                        ("b", "x"),
                        ("c", "y"),
                        ("d", "z"),
                        ("d", "z"),
                    ]
                ),
            )
            self.assert_eq(kser.explode(), expected)

            # non-array type Series
            pser = pd.Series([1, 2, 3, 4])
            kser = ps.from_pandas(pser)
            expected = pser
            self.assert_eq(kser.explode(), expected)

    def test_argsort(self):
        # Without null values
        pser = pd.Series([0, -100, 50, 100, 20], index=["A", "B", "C", "D", "E"])
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.argsort().sort_index(), kser.argsort().sort_index())
        self.assert_eq((-pser).argsort().sort_index(), (-kser).argsort().sort_index())

        # MultiIndex
        pser.index = pd.MultiIndex.from_tuples(
            [("a", "v"), ("b", "w"), ("c", "x"), ("d", "y"), ("e", "z")]
        )
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.argsort().sort_index(), kser.argsort().sort_index())
        self.assert_eq((-pser).argsort().sort_index(), (-kser).argsort().sort_index())

        # With name
        pser.name = "Koalas"
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.argsort().sort_index(), kser.argsort().sort_index())
        self.assert_eq((-pser).argsort().sort_index(), (-kser).argsort().sort_index())

        # Series from Index
        pidx = pd.Index([4.0, -6.0, 2.0, -100.0, 11.0, 20.0, 1.0, -99.0])
        kidx = ps.from_pandas(pidx)
        self.assert_eq(
            pidx.to_series().argsort().sort_index(), kidx.to_series().argsort().sort_index()
        )
        self.assert_eq(
            (-pidx.to_series()).argsort().sort_index(), (-kidx.to_series()).argsort().sort_index()
        )

        # Series from Index with name
        pidx.name = "Koalas"
        kidx = ps.from_pandas(pidx)
        self.assert_eq(
            pidx.to_series().argsort().sort_index(), kidx.to_series().argsort().sort_index()
        )
        self.assert_eq(
            (-pidx.to_series()).argsort().sort_index(), (-kidx.to_series()).argsort().sort_index()
        )

        # Series from DataFrame
        pdf = pd.DataFrame({"A": [4.0, -6.0, 2.0, np.nan, -100.0, 11.0, 20.0, np.nan, 1.0, -99.0]})
        kdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.A.argsort().sort_index(), kdf.A.argsort().sort_index())
        self.assert_eq((-pdf.A).argsort().sort_index(), (-kdf.A).argsort().sort_index())

        # With null values
        pser = pd.Series([0, -100, np.nan, 100, np.nan], index=["A", "B", "C", "D", "E"])
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.argsort().sort_index(), kser.argsort().sort_index())
        self.assert_eq((-pser).argsort().sort_index(), (-kser).argsort().sort_index())

        # MultiIndex with null values
        pser.index = pd.MultiIndex.from_tuples(
            [("a", "v"), ("b", "w"), ("c", "x"), ("d", "y"), ("e", "z")]
        )
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.argsort().sort_index(), kser.argsort().sort_index())
        self.assert_eq((-pser).argsort().sort_index(), (-kser).argsort().sort_index())

        # With name with null values
        pser.name = "Koalas"
        kser = ps.from_pandas(pser)
        self.assert_eq(pser.argsort().sort_index(), kser.argsort().sort_index())
        self.assert_eq((-pser).argsort().sort_index(), (-kser).argsort().sort_index())

        # Series from Index with null values
        pidx = pd.Index([4.0, -6.0, 2.0, np.nan, -100.0, 11.0, 20.0, np.nan, 1.0, -99.0])
        kidx = ps.from_pandas(pidx)
        self.assert_eq(
            pidx.to_series().argsort().sort_index(), kidx.to_series().argsort().sort_index()
        )
        self.assert_eq(
            (-pidx.to_series()).argsort().sort_index(), (-kidx.to_series()).argsort().sort_index()
        )

        # Series from Index with name with null values
        pidx.name = "Koalas"
        kidx = ps.from_pandas(pidx)
        self.assert_eq(
            pidx.to_series().argsort().sort_index(), kidx.to_series().argsort().sort_index()
        )
        self.assert_eq(
            (-pidx.to_series()).argsort().sort_index(), (-kidx.to_series()).argsort().sort_index()
        )

        # Series from DataFrame with null values
        pdf = pd.DataFrame({"A": [4.0, -6.0, 2.0, np.nan, -100.0, 11.0, 20.0, np.nan, 1.0, -99.0]})
        kdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.A.argsort().sort_index(), kdf.A.argsort().sort_index())
        self.assert_eq((-pdf.A).argsort().sort_index(), (-kdf.A).argsort().sort_index())

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
        kser = ps.from_pandas(pser)

        if LooseVersion(pd.__version__) >= LooseVersion("1.0"):
            self.assert_eq(pser.argmin(), kser.argmin())
            self.assert_eq(pser.argmax(), kser.argmax())

            # MultiIndex
            pser.index = pd.MultiIndex.from_tuples(
                [("a", "t"), ("b", "u"), ("c", "v"), ("d", "w"), ("e", "x"), ("f", "u")]
            )
            kser = ps.from_pandas(pser)
            self.assert_eq(pser.argmin(), kser.argmin())
            self.assert_eq(pser.argmax(), kser.argmax())

            # Null Series
            self.assert_eq(pd.Series([np.nan]).argmin(), ps.Series([np.nan]).argmin())
            self.assert_eq(pd.Series([np.nan]).argmax(), ps.Series([np.nan]).argmax())
        else:
            self.assert_eq(pser.values.argmin(), kser.argmin())
            self.assert_eq(pser.values.argmax(), kser.argmax())

            # MultiIndex
            pser.index = pd.MultiIndex.from_tuples(
                [("a", "t"), ("b", "u"), ("c", "v"), ("d", "w"), ("e", "x"), ("f", "u")]
            )
            kser = ps.from_pandas(pser)
            self.assert_eq(pser.values.argmin(), kser.argmin())
            self.assert_eq(pser.values.argmax(), kser.argmax())

            # Null Series
            self.assert_eq(-1, ps.Series([np.nan]).argmin())
            self.assert_eq(-1, ps.Series([np.nan]).argmax())

        with self.assertRaisesRegex(ValueError, "attempt to get argmin of an empty sequence"):
            ps.Series([]).argmin()
        with self.assertRaisesRegex(ValueError, "attempt to get argmax of an empty sequence"):
            ps.Series([]).argmax()

    def test_backfill(self):
        pser = pd.Series([np.nan, 2, 3, 4, np.nan, 6], name="x")
        kser = ps.from_pandas(pser)

        if LooseVersion(pd.__version__) >= LooseVersion("1.1"):
            self.assert_eq(pser.backfill(), kser.backfill())

            # Test `inplace=True`
            pser.backfill(inplace=True)
            kser.backfill(inplace=True)
            self.assert_eq(pser, kser)
        else:
            expected = ps.Series([2.0, 2.0, 3.0, 4.0, 6.0, 6.0], name="x")
            self.assert_eq(expected, kser.backfill())

            # Test `inplace=True`
            kser.backfill(inplace=True)
            self.assert_eq(expected, kser)

    def test_align(self):
        pdf = pd.DataFrame({"a": [1, 2, 3], "b": ["a", "b", "c"]})
        kdf = ps.from_pandas(pdf)

        for join in ["outer", "inner", "left", "right"]:
            for axis in [None, 0]:
                kser_l, kser_r = kdf.a.align(kdf.b, join=join, axis=axis)
                pser_l, pser_r = pdf.a.align(pdf.b, join=join, axis=axis)
                self.assert_eq(kser_l, pser_l)
                self.assert_eq(kser_r, pser_r)

                kser_l, kdf_r = kdf.b.align(kdf[["b", "a"]], join=join, axis=axis)
                pser_l, pdf_r = pdf.b.align(pdf[["b", "a"]], join=join, axis=axis)
                self.assert_eq(kser_l, pser_l)
                self.assert_eq(kdf_r, pdf_r)

        self.assertRaises(ValueError, lambda: kdf.a.align(kdf.b, axis=1))

    def test_pow_and_rpow(self):
        pser = pd.Series([1, 2, np.nan])
        kser = ps.from_pandas(pser)

        self.assert_eq(pser.pow(np.nan), kser.pow(np.nan))
        self.assert_eq(pser ** np.nan, kser ** np.nan)
        self.assert_eq(pser.rpow(np.nan), kser.rpow(np.nan))
        self.assert_eq(1 ** pser, 1 ** kser)

    def test_between_time(self):
        idx = pd.date_range("2018-04-09", periods=4, freq="1D20min")
        pser = pd.Series([1, 2, 3, 4], index=idx)
        kser = ps.from_pandas(pser)
        self.assert_eq(
            pser.between_time("0:15", "0:45").sort_index(),
            kser.between_time("0:15", "0:45").sort_index(),
        )

        pser.index.name = "ts"
        kser = ps.from_pandas(pser)
        self.assert_eq(
            pser.between_time("0:15", "0:45").sort_index(),
            kser.between_time("0:15", "0:45").sort_index(),
        )

        pser.index.name = "index"
        kser = ps.from_pandas(pser)
        self.assert_eq(
            pser.between_time("0:15", "0:45").sort_index(),
            kser.between_time("0:15", "0:45").sort_index(),
        )

    def test_at_time(self):
        idx = pd.date_range("2018-04-09", periods=4, freq="1D20min")
        pser = pd.Series([1, 2, 3, 4], index=idx)
        kser = ps.from_pandas(pser)
        self.assert_eq(
            pser.at_time("0:20").sort_index(), kser.at_time("0:20").sort_index(),
        )

        pser.index.name = "ts"
        kser = ps.from_pandas(pser)
        self.assert_eq(
            pser.at_time("0:20").sort_index(), kser.at_time("0:20").sort_index(),
        )

        pser.index.name = "index"
        kser = ps.from_pandas(pser)
        self.assert_eq(
            pser.at_time("0:20").sort_index(), kser.at_time("0:20").sort_index(),
        )


if __name__ == "__main__":
    from pyspark.pandas.tests.test_series import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
