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
import numpy as np
import pandas as pd

import pyspark.pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase, TestUtils


class GenericFunctionsTest(PandasOnSparkTestCase, TestUtils):
    def test_interpolate_error(self):
        psdf = ps.range(10)

        with self.assertRaisesRegex(
            NotImplementedError, "interpolate currently works only for method='linear'"
        ):
            psdf.interpolate(method="quadratic")

        with self.assertRaisesRegex(ValueError, "limit must be > 0"):
            psdf.interpolate(limit=0)

    def _test_series_interpolate(self, pser):
        psser = ps.from_pandas(pser)
        self.assert_eq(psser.interpolate(), pser.interpolate())
        for l1 in range(1, 5):
            self.assert_eq(psser.interpolate(limit=l1), pser.interpolate(limit=l1))

    def _test_dataframe_interpolate(self, pdf):
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psdf.interpolate(), pdf.interpolate())
        for l2 in range(1, 5):
            self.assert_eq(psdf.interpolate(limit=l2), pdf.interpolate(limit=l2))

    def test_interpolate(self):
        pser = pd.Series(
            [
                1,
                np.nan,
                3,
            ],
            name="a",
        )
        self._test_series_interpolate(pser)

        pser = pd.Series(
            [
                np.nan,
                np.nan,
                np.nan,
            ],
            name="a",
        )
        self._test_series_interpolate(pser)

        pser = pd.Series(
            [
                np.nan,
                np.nan,
                np.nan,
                0,
                1,
                np.nan,
                np.nan,
                np.nan,
                np.nan,
                3,
                np.nan,
                np.nan,
                np.nan,
            ],
            name="a",
        )
        self._test_series_interpolate(pser)

        pdf = pd.DataFrame(
            [
                (1, 0.0, np.nan),
                (2, np.nan, 2.0),
                (3, 2.0, 3.0),
                (4, np.nan, 4.0),
                (5, np.nan, 1.0),
            ],
            columns=list("abc"),
        )
        self._test_dataframe_interpolate(pdf)

        pdf = pd.DataFrame(
            [
                (0.0, np.nan, -1.0, 1.0, np.nan),
                (np.nan, 2.0, np.nan, np.nan, np.nan),
                (2.0, 3.0, np.nan, 9.0, np.nan),
                (np.nan, 4.0, -4.0, 16.0, np.nan),
                (np.nan, 1.0, np.nan, 7.0, np.nan),
            ],
            columns=list("abcde"),
        )
        self._test_dataframe_interpolate(pdf)


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.test_generic_functions import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
