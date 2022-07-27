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

        with self.assertRaisesRegex(
            NotImplementedError, "interpolate currently works only for method='linear'"
        ):
            psdf.id.interpolate(method="quadratic")

        with self.assertRaisesRegex(ValueError, "limit must be > 0"):
            psdf.interpolate(limit=0)

        with self.assertRaisesRegex(ValueError, "limit must be > 0"):
            psdf.id.interpolate(limit=0)

        with self.assertRaisesRegex(ValueError, "invalid limit_direction"):
            psdf.interpolate(limit_direction="jump")

        with self.assertRaisesRegex(ValueError, "invalid limit_direction"):
            psdf.id.interpolate(limit_direction="jump")

        with self.assertRaisesRegex(ValueError, "invalid limit_area"):
            psdf.interpolate(limit_area="jump")

        with self.assertRaisesRegex(ValueError, "invalid limit_area"):
            psdf.id.interpolate(limit_area="jump")

    def _test_interpolate(self, pobj):
        psobj = ps.from_pandas(pobj)
        self.assert_eq(psobj.interpolate(), pobj.interpolate())
        for limit in range(1, 5):
            for limit_direction in [None, "forward", "backward", "both"]:
                for limit_area in [None, "inside", "outside"]:
                    self.assert_eq(
                        psobj.interpolate(
                            limit=limit, limit_direction=limit_direction, limit_area=limit_area
                        ),
                        pobj.interpolate(
                            limit=limit, limit_direction=limit_direction, limit_area=limit_area
                        ),
                    )

    def test_interpolate(self):
        pser = pd.Series(
            [
                1,
                np.nan,
                3,
            ],
            name="a",
        )
        self._test_interpolate(pser)

        pser = pd.Series(
            [
                np.nan,
                np.nan,
                np.nan,
            ],
            name="a",
        )
        self._test_interpolate(pser)

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
        self._test_interpolate(pser)

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
        self._test_interpolate(pdf)

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
        self._test_interpolate(pdf)

        pdf = pd.DataFrame(
            [
                (0.0, np.nan, -1.0, False, np.nan),
                (np.nan, 2.0, np.nan, True, np.nan),
                (2.0, 3.0, np.nan, True, np.nan),
                (np.nan, 4.0, -4.0, False, np.nan),
                (np.nan, 1.0, np.nan, True, np.nan),
            ],
            columns=list("abcde"),
        )
        self._test_interpolate(pdf)

    def _test_stat_functions(self, stat_func):
        pdf = pd.DataFrame({"a": [np.nan, np.nan, np.nan], "b": [1, np.nan, 2], "c": [1, 2, 3]})
        psdf = ps.from_pandas(pdf)
        self.assert_eq(stat_func(pdf.a), stat_func(psdf.a))
        self.assert_eq(stat_func(pdf.b), stat_func(psdf.b))
        self.assert_eq(stat_func(pdf), stat_func(psdf))

    # Fix skew and kurtosis and re-enable tests below
    def test_stat_functions(self):
        self._test_stat_functions(lambda x: x.sum())
        self._test_stat_functions(lambda x: x.sum(skipna=False))
        self._test_stat_functions(lambda x: x.mean())
        self._test_stat_functions(lambda x: x.mean(skipna=False))
        self._test_stat_functions(lambda x: x.product())
        self._test_stat_functions(lambda x: x.product(skipna=False))
        self._test_stat_functions(lambda x: x.min())
        self._test_stat_functions(lambda x: x.min(skipna=False))
        self._test_stat_functions(lambda x: x.max())
        self._test_stat_functions(lambda x: x.max(skipna=False))
        self._test_stat_functions(lambda x: x.std())
        self._test_stat_functions(lambda x: x.std(skipna=False))
        self._test_stat_functions(lambda x: x.sem())
        self._test_stat_functions(lambda x: x.sem(skipna=False))
        # self._test_stat_functions(lambda x: x.skew())
        self._test_stat_functions(lambda x: x.skew(skipna=False))

        # Test cases below return differently from pandas (either by design or to be fixed)
        pdf = pd.DataFrame({"a": [np.nan, np.nan, np.nan], "b": [1, np.nan, 2], "c": [1, 2, 3]})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(pdf.a.median(), psdf.a.median())
        self.assert_eq(pdf.a.median(skipna=False), psdf.a.median(skipna=False))
        self.assert_eq(1.0, psdf.b.median())
        self.assert_eq(pdf.b.median(skipna=False), psdf.b.median(skipna=False))
        self.assert_eq(pdf.c.median(), psdf.c.median())

        self.assert_eq(pdf.a.kurtosis(skipna=False), psdf.a.kurtosis(skipna=False))
        self.assert_eq(pdf.a.kurtosis(), psdf.a.kurtosis())
        self.assert_eq(pdf.b.kurtosis(skipna=False), psdf.b.kurtosis(skipna=False))
        self.assert_eq(pdf.b.kurtosis(), psdf.b.kurtosis())
        self.assert_eq(pdf.c.kurtosis(), psdf.c.kurtosis())


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.test_generic_functions import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
