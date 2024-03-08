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


class SeriesMissingDataMixin:
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

    def test_add_and_radd_fill_value(self):
        pser = pd.Series([1, 2, None, 4])
        psser = ps.from_pandas(pser)
        values = [-10, -0.5, 0, None, 0.5, 10]
        for value in values:
            self.assert_eq(pser.add(10, fill_value=value), psser.add(10, fill_value=value))
            self.assert_eq(pser.radd(10, fill_value=value), psser.radd(10, fill_value=value))

        self.assertRaises(NotImplementedError, lambda: psser.add([1, 2, None, None], fill_value=10))
        self.assertRaises(
            NotImplementedError, lambda: psser.radd([1, 2, None, None], fill_value=10)
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

    def test_pad(self):
        pdf = pd.DataFrame({"x": [np.nan, 2, 3, 4, np.nan, 6]})
        psdf = ps.from_pandas(pdf)
        pser, psser = pdf.x, psdf.x

        self.assert_eq(pser.pad(), psser.pad())

        # Test `inplace=True`
        pser.pad(inplace=True)
        psser.pad(inplace=True)
        self.assert_eq(pser, psser)
        self.assert_eq(pdf, psdf)

    def test_backfill(self):
        pdf = pd.DataFrame({"x": [np.nan, 2, 3, 4, np.nan, 6]})
        psdf = ps.from_pandas(pdf)
        pser, psser = pdf.x, psdf.x

        self.assert_eq(pser.backfill(), psser.backfill())

        # Test `inplace=True`
        pser.backfill(inplace=True)
        psser.backfill(inplace=True)
        self.assert_eq(pser, psser)
        self.assert_eq(pdf, psdf)


class SeriesMissingDataTests(
    SeriesMissingDataMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.series.test_missing_data import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
