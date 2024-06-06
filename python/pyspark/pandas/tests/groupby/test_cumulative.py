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

import numpy as np
import pandas as pd

from pyspark import pandas as ps
from pyspark.pandas.exceptions import DataError
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


class GroupbyCumulativeMixin:
    def test_cumcount(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 2, 3, 4, 5, 6] * 3,
                "b": [1, 1, 2, 3, 5, 8] * 3,
                "c": [1, 4, 9, 16, 25, 36] * 3,
            },
            index=np.random.rand(6 * 3),
        )
        psdf = ps.from_pandas(pdf)

        for ascending in [True, False]:
            self.assert_eq(
                psdf.groupby("b").cumcount(ascending=ascending).sort_index(),
                pdf.groupby("b").cumcount(ascending=ascending).sort_index(),
            )
            self.assert_eq(
                psdf.groupby(["a", "b"]).cumcount(ascending=ascending).sort_index(),
                pdf.groupby(["a", "b"]).cumcount(ascending=ascending).sort_index(),
            )
            self.assert_eq(
                psdf.groupby(["b"])["a"].cumcount(ascending=ascending).sort_index(),
                pdf.groupby(["b"])["a"].cumcount(ascending=ascending).sort_index(),
            )
            self.assert_eq(
                psdf.groupby(["b"])[["a", "c"]].cumcount(ascending=ascending).sort_index(),
                pdf.groupby(["b"])[["a", "c"]].cumcount(ascending=ascending).sort_index(),
            )
            self.assert_eq(
                psdf.groupby(psdf.b // 5).cumcount(ascending=ascending).sort_index(),
                pdf.groupby(pdf.b // 5).cumcount(ascending=ascending).sort_index(),
            )
            self.assert_eq(
                psdf.groupby(psdf.b // 5)["a"].cumcount(ascending=ascending).sort_index(),
                pdf.groupby(pdf.b // 5)["a"].cumcount(ascending=ascending).sort_index(),
            )
            self.assert_eq(
                psdf.groupby("b").cumcount(ascending=ascending).sum(),
                pdf.groupby("b").cumcount(ascending=ascending).sum(),
            )
            self.assert_eq(
                psdf.a.rename().groupby(psdf.b).cumcount(ascending=ascending).sort_index(),
                pdf.a.rename().groupby(pdf.b).cumcount(ascending=ascending).sort_index(),
            )
            self.assert_eq(
                psdf.a.groupby(psdf.b.rename()).cumcount(ascending=ascending).sort_index(),
                pdf.a.groupby(pdf.b.rename()).cumcount(ascending=ascending).sort_index(),
            )
            self.assert_eq(
                psdf.a.rename().groupby(psdf.b.rename()).cumcount(ascending=ascending).sort_index(),
                pdf.a.rename().groupby(pdf.b.rename()).cumcount(ascending=ascending).sort_index(),
            )

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        for ascending in [True, False]:
            self.assert_eq(
                psdf.groupby(("x", "b")).cumcount(ascending=ascending).sort_index(),
                pdf.groupby(("x", "b")).cumcount(ascending=ascending).sort_index(),
            )
            self.assert_eq(
                psdf.groupby([("x", "a"), ("x", "b")]).cumcount(ascending=ascending).sort_index(),
                pdf.groupby([("x", "a"), ("x", "b")]).cumcount(ascending=ascending).sort_index(),
            )

    def test_cummin(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 2, 3, 4, 5, 6] * 3,
                "b": [1, 1, 2, 3, 5, 8] * 3,
                "c": [1, 4, 9, 16, 25, 36] * 3,
            },
            index=np.random.rand(6 * 3),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby("b").cummin().sort_index(), pdf.groupby("b").cummin().sort_index()
        )
        self.assert_eq(
            psdf.groupby(["a", "b"]).cummin().sort_index(),
            pdf.groupby(["a", "b"]).cummin().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["b"])["a"].cummin().sort_index(),
            pdf.groupby(["b"])["a"].cummin().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["b"])[["a", "c"]].cummin().sort_index(),
            pdf.groupby(["b"])[["a", "c"]].cummin().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5).cummin().sort_index(),
            pdf.groupby(pdf.b // 5).cummin().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5)["a"].cummin().sort_index(),
            pdf.groupby(pdf.b // 5)["a"].cummin().sort_index(),
        )
        self.assert_eq(
            psdf.groupby("b").cummin().sum().sort_index(),
            pdf.groupby("b").cummin().sum().sort_index(),
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b).cummin().sort_index(),
            pdf.a.rename().groupby(pdf.b).cummin().sort_index(),
        )
        self.assert_eq(
            psdf.a.groupby(psdf.b.rename()).cummin().sort_index(),
            pdf.a.groupby(pdf.b.rename()).cummin().sort_index(),
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b.rename()).cummin().sort_index(),
            pdf.a.rename().groupby(pdf.b.rename()).cummin().sort_index(),
        )

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.groupby(("x", "b")).cummin().sort_index(),
            pdf.groupby(("x", "b")).cummin().sort_index(),
        )
        self.assert_eq(
            psdf.groupby([("x", "a"), ("x", "b")]).cummin().sort_index(),
            pdf.groupby([("x", "a"), ("x", "b")]).cummin().sort_index(),
        )

        psdf = ps.DataFrame([["a"], ["b"], ["c"]], columns=["A"])
        self.assertRaises(DataError, lambda: psdf.groupby(["A"]).cummin())
        psdf = ps.DataFrame([[1, "a"], [2, "b"], [3, "c"]], columns=["A", "B"])
        self.assertRaises(DataError, lambda: psdf.groupby(["A"])["B"].cummin())

    def test_cummax(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 2, 3, 4, 5, 6] * 3,
                "b": [1, 1, 2, 3, 5, 8] * 3,
                "c": [1, 4, 9, 16, 25, 36] * 3,
            },
            index=np.random.rand(6 * 3),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby("b").cummax().sort_index(), pdf.groupby("b").cummax().sort_index()
        )
        self.assert_eq(
            psdf.groupby(["a", "b"]).cummax().sort_index(),
            pdf.groupby(["a", "b"]).cummax().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["b"])["a"].cummax().sort_index(),
            pdf.groupby(["b"])["a"].cummax().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["b"])[["a", "c"]].cummax().sort_index(),
            pdf.groupby(["b"])[["a", "c"]].cummax().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5).cummax().sort_index(),
            pdf.groupby(pdf.b // 5).cummax().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5)["a"].cummax().sort_index(),
            pdf.groupby(pdf.b // 5)["a"].cummax().sort_index(),
        )
        self.assert_eq(
            psdf.groupby("b").cummax().sum().sort_index(),
            pdf.groupby("b").cummax().sum().sort_index(),
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b).cummax().sort_index(),
            pdf.a.rename().groupby(pdf.b).cummax().sort_index(),
        )
        self.assert_eq(
            psdf.a.groupby(psdf.b.rename()).cummax().sort_index(),
            pdf.a.groupby(pdf.b.rename()).cummax().sort_index(),
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b.rename()).cummax().sort_index(),
            pdf.a.rename().groupby(pdf.b.rename()).cummax().sort_index(),
        )

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.groupby(("x", "b")).cummax().sort_index(),
            pdf.groupby(("x", "b")).cummax().sort_index(),
        )
        self.assert_eq(
            psdf.groupby([("x", "a"), ("x", "b")]).cummax().sort_index(),
            pdf.groupby([("x", "a"), ("x", "b")]).cummax().sort_index(),
        )

        psdf = ps.DataFrame([["a"], ["b"], ["c"]], columns=["A"])
        self.assertRaises(DataError, lambda: psdf.groupby(["A"]).cummax())
        psdf = ps.DataFrame([[1, "a"], [2, "b"], [3, "c"]], columns=["A", "B"])
        self.assertRaises(DataError, lambda: psdf.groupby(["A"])["B"].cummax())

    def test_cumsum(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 2, 3, 4, 5, 6] * 3,
                "b": [1, 1, 2, 3, 5, 8] * 3,
                "c": [1, 4, 9, 16, 25, 36] * 3,
            },
            index=np.random.rand(6 * 3),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby("b").cumsum().sort_index(), pdf.groupby("b").cumsum().sort_index()
        )
        self.assert_eq(
            psdf.groupby(["a", "b"]).cumsum().sort_index(),
            pdf.groupby(["a", "b"]).cumsum().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["b"])["a"].cumsum().sort_index(),
            pdf.groupby(["b"])["a"].cumsum().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["b"])[["a", "c"]].cumsum().sort_index(),
            pdf.groupby(["b"])[["a", "c"]].cumsum().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5).cumsum().sort_index(),
            pdf.groupby(pdf.b // 5).cumsum().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5)["a"].cumsum().sort_index(),
            pdf.groupby(pdf.b // 5)["a"].cumsum().sort_index(),
        )
        self.assert_eq(
            psdf.groupby("b").cumsum().sum().sort_index(),
            pdf.groupby("b").cumsum().sum().sort_index(),
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b).cumsum().sort_index(),
            pdf.a.rename().groupby(pdf.b).cumsum().sort_index(),
        )
        self.assert_eq(
            psdf.a.groupby(psdf.b.rename()).cumsum().sort_index(),
            pdf.a.groupby(pdf.b.rename()).cumsum().sort_index(),
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b.rename()).cumsum().sort_index(),
            pdf.a.rename().groupby(pdf.b.rename()).cumsum().sort_index(),
        )

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.groupby(("x", "b")).cumsum().sort_index(),
            pdf.groupby(("x", "b")).cumsum().sort_index(),
        )
        self.assert_eq(
            psdf.groupby([("x", "a"), ("x", "b")]).cumsum().sort_index(),
            pdf.groupby([("x", "a"), ("x", "b")]).cumsum().sort_index(),
        )

        psdf = ps.DataFrame([["a"], ["b"], ["c"]], columns=["A"])
        self.assertRaises(DataError, lambda: psdf.groupby(["A"]).cumsum())
        psdf = ps.DataFrame([[1, "a"], [2, "b"], [3, "c"]], columns=["A", "B"])
        self.assertRaises(DataError, lambda: psdf.groupby(["A"])["B"].cumsum())

    def test_cumprod(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 2, -3, 4, -5, 6] * 3,
                "b": [1, 1, 2, 3, 5, 8] * 3,
                "c": [1, 0, 9, 16, 25, 36] * 3,
            },
            index=np.random.rand(6 * 3),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby("b").cumprod().sort_index(),
            pdf.groupby("b").cumprod().sort_index(),
            check_exact=False,
        )
        self.assert_eq(
            psdf.groupby(["a", "b"]).cumprod().sort_index(),
            pdf.groupby(["a", "b"]).cumprod().sort_index(),
            check_exact=False,
        )
        self.assert_eq(
            psdf.groupby(["b"])["a"].cumprod().sort_index(),
            pdf.groupby(["b"])["a"].cumprod().sort_index(),
            check_exact=False,
        )
        self.assert_eq(
            psdf.groupby(["b"])[["a", "c"]].cumprod().sort_index(),
            pdf.groupby(["b"])[["a", "c"]].cumprod().sort_index(),
            check_exact=False,
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 3).cumprod().sort_index(),
            pdf.groupby(pdf.b // 3).cumprod().sort_index(),
            check_exact=False,
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 3)["a"].cumprod().sort_index(),
            pdf.groupby(pdf.b // 3)["a"].cumprod().sort_index(),
            check_exact=False,
        )
        self.assert_eq(
            psdf.groupby("b").cumprod().sum().sort_index(),
            pdf.groupby("b").cumprod().sum().sort_index(),
            check_exact=False,
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b).cumprod().sort_index(),
            pdf.a.rename().groupby(pdf.b).cumprod().sort_index(),
            check_exact=False,
        )
        self.assert_eq(
            psdf.a.groupby(psdf.b.rename()).cumprod().sort_index(),
            pdf.a.groupby(pdf.b.rename()).cumprod().sort_index(),
            check_exact=False,
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b.rename()).cumprod().sort_index(),
            pdf.a.rename().groupby(pdf.b.rename()).cumprod().sort_index(),
            check_exact=False,
        )

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.groupby(("x", "b")).cumprod().sort_index(),
            pdf.groupby(("x", "b")).cumprod().sort_index(),
            check_exact=False,
        )
        self.assert_eq(
            psdf.groupby([("x", "a"), ("x", "b")]).cumprod().sort_index(),
            pdf.groupby([("x", "a"), ("x", "b")]).cumprod().sort_index(),
            check_exact=False,
        )

        psdf = ps.DataFrame([["a"], ["b"], ["c"]], columns=["A"])
        self.assertRaises(DataError, lambda: psdf.groupby(["A"]).cumprod())
        psdf = ps.DataFrame([[1, "a"], [2, "b"], [3, "c"]], columns=["A", "B"])
        self.assertRaises(DataError, lambda: psdf.groupby(["A"])["B"].cumprod())


class GroupbyCumulativeTests(
    GroupbyCumulativeMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.groupby.test_cumulative import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
