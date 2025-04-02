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
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


class SeriesArgOpsMixin:
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

        pser2 = pd.Series([np.nan, 1.0, 2.0, np.nan])
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


class SeriesArgOpsTests(
    SeriesArgOpsMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.series.test_arg_ops import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
