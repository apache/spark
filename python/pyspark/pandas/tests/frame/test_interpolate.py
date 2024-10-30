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


class FrameInterpolateMixin:
    def _test_interpolate(self, pobj):
        psobj = ps.from_pandas(pobj)
        self.assert_eq(
            psobj.interpolate().sort_index(),
            pobj.interpolate().sort_index(),
        )
        for limit, limit_direction, limit_area in [
            (1, None, None),
            (2, "forward", "inside"),
            (3, "backward", "outside"),
            (4, "backward", "inside"),
            (5, "both", "inside"),
        ]:
            self.assert_eq(
                psobj.interpolate(
                    limit=limit, limit_direction=limit_direction, limit_area=limit_area
                ).sort_index(),
                pobj.interpolate(
                    limit=limit, limit_direction=limit_direction, limit_area=limit_area
                ).sort_index(),
            )

    def test_interpolate(self):
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
                (0.0, np.nan, -1.0, False, np.nan),
                (np.nan, 2.0, np.nan, True, np.nan),
                (2.0, 3.0, np.nan, True, np.nan),
                (np.nan, 4.0, -4.0, False, np.nan),
                (np.nan, 1.0, np.nan, True, np.nan),
            ],
            columns=list("abcde"),
        )
        self._test_interpolate(pdf)


class FrameInterpolateTests(
    FrameInterpolateMixin,
    PandasOnSparkTestCase,
    TestUtils,
):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.frame.test_interpolate import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
