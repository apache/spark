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
import inspect
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from pyspark.ml.linalg import SparseVector

from pyspark import pandas as ps
from pyspark.testing.pandasutils import (
    PandasOnSparkTestCase,
    SPARK_CONF_ARROW_ENABLED,
)
from pyspark.testing.sqlutils import SQLTestUtils
from pyspark.pandas.exceptions import PandasNotImplementedError
from pyspark.pandas.missing.series import MissingPandasLikeSeries
from pyspark.pandas.typedef.typehints import extension_object_dtypes_available


class SeriesTestsMixin:
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

        self.assertTrue(pser_a.empty)

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

        self.assert_eq(psser.isin([1, 5, 0, None]), pser.isin([1, 5, 0, None]))

    def test_notnull(self):
        pser = pd.Series([1, 2, 3, 4, np.nan, 6], name="x")
        psser = ps.from_pandas(pser)

        self.assert_eq(psser.notnull(), pser.notnull())

        pser = self.pser
        psser = self.psser

        self.assert_eq(psser.notnull(), pser.notnull())

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

    def test_xs(self):
        midx = pd.MultiIndex(
            [["a", "b", "c"], ["lama", "cow", "falcon"], ["speed", "weight", "length"]],
            [[0, 0, 0, 1, 1, 1, 2, 2, 2], [0, 0, 0, 1, 1, 1, 2, 2, 2], [0, 1, 2, 0, 1, 2, 0, 1, 2]],
        )
        pser = pd.Series([45, 200, 1.2, 30, 250, 1.5, 320, 1, 0.3], index=midx)
        psser = ps.from_pandas(pser)

        self.assert_eq(psser.xs(("a", "lama", "speed")), pser.xs(("a", "lama", "speed")))

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

    def test_shape(self):
        pser = pd.Series(["a", "b", "c"])
        psser = ps.from_pandas(pser)

        self.assert_eq(pser.shape, psser.shape)

        # for MultiIndex
        midx = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        pser = pd.Series(["a", "b", "c"], index=midx)
        psser = ps.from_pandas(pser)

        self.assert_eq(pser.shape, psser.shape)

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

    def test_items(self):
        pser = pd.Series(["A", "B", "C"])
        psser = ps.from_pandas(pser)

        for (p_name, p_items), (k_name, k_items) in zip(pser.items(), psser.items()):
            self.assert_eq(p_name, k_name)
            self.assert_eq(p_items, k_items)

    def test_dot(self):
        pdf = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        psdf = ps.from_pandas(pdf)

        self.assert_eq((psdf["b"] * 10).dot(psdf["a"]), (pdf["b"] * 10).dot(pdf["a"]))
        self.assert_eq((psdf["b"] * 10).dot(psdf), (pdf["b"] * 10).dot(pdf))
        self.assert_eq((psdf["b"] * 10).dot(psdf + 1), (pdf["b"] * 10).dot(pdf + 1))

        psdf_other = ps.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}, index=["x", "y", "z"])
        with self.assertRaisesRegex(ValueError, "matrices are not aligned"):
            with ps.option_context("compute.ops_on_diff_frames", True):
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
        self.assert_eq(pser.eq(other), psser.eq(other).sort_index())
        self.assert_eq(pser == other, (psser == other).sort_index())

        # other = tuple
        other = (np.nan, 1, 3, 4, np.nan, 6)
        self.assert_eq(pser.eq(other), psser.eq(other).sort_index())
        self.assert_eq(pser == other, (psser == other).sort_index())

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


class SeriesTests(
    SeriesTestsMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.series.test_series import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
