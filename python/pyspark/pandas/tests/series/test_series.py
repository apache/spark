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
from collections import defaultdict
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from pyspark import pandas as ps
from pyspark.loose_version import LooseVersion
from pyspark.ml.linalg import SparseVector
from pyspark.pandas.exceptions import PandasNotImplementedError
from pyspark.pandas.missing.series import MissingPandasLikeSeries
from pyspark.pandas.typedef.typehints import extension_object_dtypes_available
from pyspark.testing.pandasutils import (
    SPARK_CONF_ARROW_ENABLED,
    PandasOnSparkTestCase,
)


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

    def test_series_from_series(self):
        psser = ps.Series([1, 2, 3, 4, 5, 6, 7], name="x")

        psser_from_psser = ps.Series(psser)
        self.assert_eq(psser_from_psser, psser)

        psser = ps.Series([1, 2, 3])

        # Specify new index
        psser_from_psser = ps.Series(psser, index=[1])
        self.assert_eq(psser_from_psser, ps.Series([2], index=[1]))

        psser_from_psser = ps.Series(psser, index=[1, 2])
        self.assert_eq(psser_from_psser, ps.Series([2, 3], index=[1, 2]))

        # Specify new out-of-order index
        psser_from_psser = ps.Series(psser, index=[1, 2, 0])
        self.assert_eq(psser_from_psser, ps.Series([2, 3, 1], index=[1, 2, 0]))

        # Specify new dtype and name
        psser_from_psser = ps.Series(psser, name="y", dtype=float)
        self.assert_eq(psser_from_psser, ps.Series([1, 2, 3], name="y", dtype=float))

        # Specify a pandas-on-Spark Index, e.g. the Series' own index
        pser = pd.Series([1, 2, 3], index=[10, 20, 30], name="x")
        psser = ps.from_pandas(pser)
        self.assert_eq(
            ps.Series(psser, index=psser.index),
            pd.Series(pser, index=pser.index),
        )
        self.assert_eq(
            ps.Series(psser, index=ps.Index([30, 10])),
            pd.Series(pser, index=pd.Index([30, 10])),
        )

        # Specify an empty index
        self.assert_eq(ps.Series(psser, index=[]), pd.Series(pser, index=[]))

    def _assert_no_index_join(self, psser):
        from pyspark.sql.utils import is_remote

        # Spark Connect's plan text is not available through the classic query execution.
        if is_remote():
            return
        plan = psser.to_frame().to_spark()._jdf.queryExecution().analyzed().toString()
        self.assertNotIn("Join", plan)

    def test_series_from_series_index_alignment(self):
        duplicate_message = "cannot reindex on an axis with duplicate labels"

        # The Series' own index is the same axis, so repeated labels and nulls stay put.
        pser = pd.Series([1, 2, 3], index=[10, 10, 20], name="x")
        psser = ps.from_pandas(pser)
        self.assert_eq(ps.Series(psser, index=psser.index), pd.Series(pser, index=pser.index))
        self.assert_eq(
            ps.Series(psser, index=psser.index.rename("k")),
            pd.Series(pser, index=pser.index.rename("k")),
        )
        self.assert_eq(
            ps.Series(psser, index=psser.index, name="y"),
            pd.Series(pser, index=pser.index, name="y"),
        )
        self._assert_no_index_join(ps.Series(psser, index=psser.index))

        pser = pd.Series([1, 2, 3], index=["a", None, "c"], name="x")
        psser = ps.from_pandas(pser)
        got = ps.Series(psser, index=psser.index)
        self.assert_eq(got, pd.Series(pser, index=pser.index))
        self._assert_no_index_join(got)

        # Any other index is refused when a label repeats, unless it is that same sequence.
        pser = pd.Series([1, 2, 3], index=[10, 10, 20], name="x")
        psser = ps.from_pandas(pser)
        other = ps.from_pandas(pd.Series([4, 5], index=[10, 30], name="y"))
        for target in (
            other.index,
            pd.Index([10, 30]),
            pd.Series([10, 30], name="s"),
            np.array([10, 30]),
        ):
            with self.assertRaisesRegex(ValueError, duplicate_message):
                ps.Series(psser, index=target)

        equal_index = pd.Index([10, 10, 20], name="k", dtype="int32")
        self.assert_eq(ps.Series(psser, index=equal_index), pd.Series(pser, index=equal_index))
        same_labels = np.array([10, 10, 20], dtype=np.int32)
        self.assert_eq(ps.Series(psser, index=same_labels), pd.Series(pser, index=same_labels))
        copied = ps.from_pandas(pd.Series([9, 8, 7], index=[10, 10, 20], name="z"))
        self.assert_eq(
            ps.Series(psser, index=copied.index.rename("k")),
            pd.Series(pser, index=pser.index.rename("k")),
        )

        # A pandas Index or Series keeps its name and dtype over a unique source index.
        pser = pd.Series([1, 2, 3], index=pd.Index([10, 20, 30], name="src"), name="x")
        psser = ps.from_pandas(pser)
        target = pd.Index([30, 10], dtype="int32", name="k")
        self.assert_eq(ps.Series(psser, index=target), pd.Series(pser, index=target))
        target_ser = pd.Series([30, 10], name="s")
        self.assert_eq(ps.Series(psser, index=target_ser), pd.Series(pser, index=target_ser))
        unnamed = pd.Index([30, 10])
        self.assert_eq(ps.Series(psser, index=unnamed), pd.Series(pser, index=unnamed))

        # Lists and one-element arrays keep the existing reindex path. A list does not
        # carry an index name, and reindex still keeps the source index name.
        dup = ps.from_pandas(pd.Series([1, 2, 3], index=[10, 10, 20], name="x"))
        self.assert_eq(ps.Series(dup, index=[10, 20]), dup.reindex([10, 20]))
        self.assert_eq(ps.Series(dup, index=np.array([10])), dup.reindex(np.array([10])))
        self.assert_eq(ps.Series(psser, index=[30, 10]), psser.reindex([30, 10]))
        self.assert_eq(ps.Series(psser, index=[]), psser.reindex([]))

        with self.assertRaisesRegex(TypeError, "collection of some kind"):
            ps.Series(psser, index=0)

    def test_series_from_series_duplicate_null_index(self):
        duplicate_message = "cannot reindex on an axis with duplicate labels"

        def assert_same_axis(pser, targets):
            psser = ps.from_pandas(pser)
            for target, pandas_target in targets:
                self.assert_eq(
                    ps.Series(psser, index=target),
                    pd.Series(pser, index=pandas_target),
                )
            self._assert_no_index_join(ps.Series(psser, index=psser.index))
            return psser

        def assert_different_axis(psser, targets):
            for target in targets:
                with self.assertRaisesRegex(ValueError, duplicate_message):
                    ps.Series(psser, index=target)

        # Repeated nulls are duplicate labels. The same axis keeps its values.
        pser = pd.Series([1, 2], index=[None, None], name="x")
        copied = pd.Index([None, None], dtype=object)
        named = pd.Index([None, None], dtype=object, name="k")
        psser = assert_same_axis(
            pser,
            [
                (ps.from_pandas(pser).index, pser.index),
                (copied, copied),
                (named, named),
                (pd.Series([None, None], name="k"), pd.Series([None, None], name="k")),
                (np.array([None, None], dtype=object), np.array([None, None], dtype=object)),
                (
                    ps.from_pandas(pd.Series([9, 8], index=[None, None])).index,
                    pd.Index([None, None], dtype=object),
                ),
            ],
        )
        # The first target above builds another Series, so compare the caller's own index too.
        self.assert_eq(ps.Series(psser, index=psser.index), pd.Series(pser, index=pser.index))
        copied_index = psser.index.to_pandas()
        self.assert_eq(
            ps.Series(psser, index=copied_index),
            pd.Series(pser, index=copied_index),
        )
        assert_different_axis(
            psser,
            [
                pd.Index([None], dtype=object),
                pd.Index(["a", "b"]),
                pd.Series([None, "a"]),
                np.array(["a", "b"], dtype=object),
                ps.from_pandas(pd.Series([4, 5], index=["a", "b"])).index,
            ],
        )

        # A repeated null alongside another label, and a repeated label alongside null.
        # Numeric labels with null become a float index; an object array is a different
        # dtype and is covered by the object cases above.
        for labels in (["a", None, None], ["a", None, "a"]):
            pser = pd.Series([1, 2, 3], index=labels, name="x")
            psser = assert_same_axis(
                pser,
                [
                    (pd.Index(labels), pd.Index(labels)),
                    (np.array(labels, dtype=object), np.array(labels, dtype=object)),
                    (
                        ps.from_pandas(pd.Series([9, 8, 7], index=labels)).index,
                        pd.Index(labels),
                    ),
                ],
            )
            self.assert_eq(
                ps.Series(psser, index=psser.index),
                pd.Series(pser, index=pser.index),
            )
            assert_different_axis(
                psser,
                [pd.Index(["b", "b"]), np.array(["z", None], dtype=object)],
            )

        labels = [10, 10, None]
        pser = pd.Series([1, 2, 3], index=labels, name="x")
        psser = assert_same_axis(
            pser,
            [
                (pd.Index(labels), pd.Index(labels)),
                (
                    ps.from_pandas(pd.Series([9, 8, 7], index=labels)).index,
                    pd.Index(labels),
                ),
            ],
        )
        self.assert_eq(ps.Series(psser, index=psser.index), pd.Series(pser, index=pser.index))
        assert_different_axis(psser, [pd.Index([10, None]), pd.Index([None, 10, 10])])

        # One null is not a duplicate, so a non-null target still aligns.
        pser = pd.Series([1, 2], index=["a", None], name="x")
        psser = ps.from_pandas(pser)
        self.assert_eq(
            ps.Series(psser, index=pd.Index(["a"])),
            pd.Series(pser, index=pd.Index(["a"])),
        )

        # Lists, tuples, and one-element arrays stay on reindex. That path can still
        # multiply a repeated non-null label and can fail to match a repeated null.
        # These assertions lock that existing behavior.
        dup = ps.from_pandas(pd.Series([1, 2], index=[None, None], name="x"))
        for target in ([None, None], (None, None), np.array([None], dtype=object)):
            self.assert_eq(ps.Series(dup, index=target), dup.reindex(target))
        repeated = ps.from_pandas(pd.Series([1, 2, 3], index=[10, 10, 20], name="x"))
        self.assert_eq(ps.Series(repeated, index=(10, 20)), repeated.reindex((10, 20)))

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
        index = pd.date_range("2018-04-09", periods=4, freq="2D")
        pser = pd.Series([1, 2, 3, 4], index=index)
        psser = ps.from_pandas(pser)

        if LooseVersion(pd.__version__) < "3.0.0":
            self.assert_eq(psser.last("1D"), pser.last("1D"))

            with self.assertRaises(TypeError):
                self.psser.last("1D")
        else:
            with self.assertRaises(AttributeError):
                psser.last("1D")
            with self.assertRaises(AttributeError):
                self.psser.last("1D")

    def test_first(self):
        index = pd.date_range("2018-04-09", periods=4, freq="2D")
        pser = pd.Series([1, 2, 3, 4], index=index)
        psser = ps.from_pandas(pser)

        if LooseVersion(pd.__version__) < "3.0.0":
            self.assert_eq(psser.first("1D"), pser.first("1D"))

            with self.assertRaises(TypeError):
                self.psser.first("1D")
        else:
            with self.assertRaises(AttributeError):
                psser.first("1D")
            with self.assertRaises(AttributeError):
                self.psser.first("1D")

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

        # Cross-type matching: string values against int column should not match
        pser = pd.Series([1, 2, 3], name="a")
        psser = ps.from_pandas(pser)
        self.assert_eq(psser.isin(["1", "2"]), pser.isin(["1", "2"]))

        # Numeric cross-type: float values against int column should match
        self.assert_eq(psser.isin([1.0, 2.0]), pser.isin([1.0, 2.0]))

        # String column with numeric values should not match
        pser = pd.Series(["1", "2", "3"], name="a")
        psser = ps.from_pandas(pser)
        self.assert_eq(psser.isin([1, 2]), pser.isin([1, 2]))

        # Bool column: int/float are compatible, string is not
        pser = pd.Series([True, False, True], name="a")
        psser = ps.from_pandas(pser)
        self.assert_eq(psser.isin([1]), pser.isin([1]))
        self.assert_eq(psser.isin([1.0]), pser.isin([1.0]))
        self.assert_eq(psser.isin(["True"]), pser.isin(["True"]))

        # Int column: bool/float are compatible, string is not
        pser = pd.Series([1, 2, 3], name="a")
        psser = ps.from_pandas(pser)
        self.assert_eq(psser.isin([True]), pser.isin([True]))
        self.assert_eq(psser.isin([1.0, 2.0]), pser.isin([1.0, 2.0]))
        self.assert_eq(psser.isin(["1"]), pser.isin(["1"]))

        # Date column: date/Timestamp are compatible, string/int are not
        pser = pd.Series([datetime(2023, 1, 1).date(), datetime(2023, 1, 2).date()], name="a")
        psser = ps.from_pandas(pser)
        self.assert_eq(
            psser.isin([datetime(2023, 1, 1).date()]),
            pser.isin([datetime(2023, 1, 1).date()]),
        )
        self.assert_eq(psser.isin(["2023-01-01"]), pser.isin(["2023-01-01"]))
        self.assert_eq(psser.isin([1]), pser.isin([1]))

        # Timestamp column: datetime is compatible, int/string are not
        pser = pd.Series(pd.to_datetime(["2023-01-01", "2023-01-02"]), name="a")
        psser = ps.from_pandas(pser)
        self.assert_eq(psser.isin([datetime(2023, 1, 1)]), pser.isin([datetime(2023, 1, 1)]))
        self.assert_eq(psser.isin([1]), pser.isin([1]))

        # Binary column: bytes is compatible, string is not
        pser = pd.Series([b"abc", b"def"], name="a")
        psser = ps.from_pandas(pser)
        self.assert_eq(psser.isin([b"abc"]), pser.isin([b"abc"]))
        self.assert_eq(psser.isin(["abc"]), pser.isin(["abc"]))

        # Timedelta column: timedelta is compatible, int/string are not
        pser = pd.Series([timedelta(days=1), timedelta(days=2)], name="a")
        psser = ps.from_pandas(pser)
        self.assert_eq(psser.isin([timedelta(days=1)]), pser.isin([timedelta(days=1)]))
        self.assert_eq(psser.isin([1]), pser.isin([1]))

        # None with incompatible types: None passes type filter but never matches via IN
        pser = pd.Series([1, 2, 3, None], name="a")
        psser = ps.from_pandas(pser)
        self.assert_eq(psser.isin(["a", "b", "c", None]), pser.isin(["a", "b", "c", None]))

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
        if LooseVersion(pd.__version__) < "3.0.0":
            # Currently pandas API on Spark doesn't return NaN as pandas does.
            self.assert_eq(psser.map({}), pser.map({}).replace({np.nan: None}))
        else:
            self.assert_eq(psser.map({}), pser.map({}))

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
            return string.upper() if isinstance(string, str) else ""

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
):
    pass


if __name__ == "__main__":
    from pyspark.testing import main

    main()
