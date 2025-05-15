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
from decimal import Decimal

import numpy as np
import pandas as pd

from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils
from pyspark.testing.utils import is_ansi_mode_test, ansi_mode_not_supported_message


class SeriesStatMixin:
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
            non_numeric_pser.rank(numeric_only=None),
            non_numeric_psser.rank(numeric_only=None).sort_index(),
        )
        self.assert_eq(
            non_numeric_pser.rank(numeric_only=False),
            non_numeric_psser.rank(numeric_only=False).sort_index(),
        )

        msg = "Series.rank does not allow numeric_only=True with non-numeric dtype."
        with self.assertRaisesRegex(TypeError, msg):
            non_numeric_psser.rank(numeric_only=True)

        msg = "Series.rank does not allow numeric_only=True with non-numeric dtype."
        with self.assertRaisesRegex(TypeError, msg):
            (non_numeric_psser + "x").rank(numeric_only=True)

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

    @unittest.skipIf(is_ansi_mode_test, ansi_mode_not_supported_message)
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

        # Boolean Series
        pser = pd.Series([True, True, True])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.prod(numeric_only=True), psser.prod())

        pser = pd.Series([False, False, False])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.prod(numeric_only=True), psser.prod())

        pser = pd.Series([True, False, True])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.prod(numeric_only=True), psser.prod())

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

        with self.assertRaisesRegex(TypeError, "Could not convert object \\(void\\) to numeric"):
            ps.Series([]).prod(numeric_only=True)
        with self.assertRaisesRegex(TypeError, "Could not convert object \\(void\\) to numeric"):
            ps.Series([]).prod(min_count=1)
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

    def test_pow_and_rpow(self):
        pser = pd.Series([1, 2, np.nan])
        psser = ps.from_pandas(pser)

        self.assert_eq(pser.pow(np.nan), psser.pow(np.nan))
        self.assert_eq(pser**np.nan, psser**np.nan)
        self.assert_eq(pser.rpow(np.nan), psser.rpow(np.nan))
        self.assert_eq(1**pser, 1**psser)

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


class SeriesStatTests(
    SeriesStatMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.series.test_stat import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
