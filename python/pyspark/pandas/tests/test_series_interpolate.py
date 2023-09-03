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


class SeriesInterpolateTestsMixin:
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


class SeriesInterpolateTests(SeriesInterpolateTestsMixin, PandasOnSparkTestCase, TestUtils):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.test_series_interpolate import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
