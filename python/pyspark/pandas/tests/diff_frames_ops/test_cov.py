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

import pandas as pd

from pyspark import pandas as ps
from pyspark.pandas.config import set_option, reset_option
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


class DiffFramesCovMixin:
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        set_option("compute.ops_on_diff_frames", True)

    @classmethod
    def tearDownClass(cls):
        reset_option("compute.ops_on_diff_frames")
        super().tearDownClass()

    def test_cov(self):
        pser1 = pd.Series([0.90010907, 0.13484424, 0.62036035], index=[0, 1, 2])
        pser2 = pd.Series([0.12528585, 0.26962463, 0.51111198], index=[1, 2, 3])
        self._test_cov(pser1, pser2)

        pser1 = pd.Series([0.90010907, 0.13484424, 0.62036035], index=[0, 1, 2])
        pser2 = pd.Series([0.12528585, 0.26962463, 0.51111198, 0.32076008], index=[1, 2, 3, 4])
        self._test_cov(pser1, pser2)

        pser1 = pd.Series([0.90010907, 0.13484424, 0.62036035, 0.32076008], index=[0, 1, 2, 3])
        pser2 = pd.Series([0.12528585, 0.26962463], index=[1, 2])
        self._test_cov(pser1, pser2)

        psser1 = ps.from_pandas(pser1)
        with self.assertRaisesRegex(TypeError, "unsupported type: <class 'list'>"):
            psser1.cov([0.12528585, 0.26962463, 0.51111198])
        with self.assertRaisesRegex(
            TypeError, "unsupported type: <class 'pandas.core.series.Series'>"
        ):
            psser1.cov(pser2)

    def _test_cov(self, pser1, pser2):
        psser1 = ps.from_pandas(pser1)
        psser2 = ps.from_pandas(pser2)

        pcov = pser1.cov(pser2)
        pscov = psser1.cov(psser2)
        self.assert_eq(pcov, pscov, almost=True)

        pcov = pser1.cov(pser2, min_periods=2)
        pscov = psser1.cov(psser2, min_periods=2)
        self.assert_eq(pcov, pscov, almost=True)

        pcov = pser1.cov(pser2, min_periods=3)
        pscov = psser1.cov(psser2, min_periods=3)
        self.assert_eq(pcov, pscov, almost=True)


class DiffFramesCovTests(
    DiffFramesCovMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.diff_frames_ops.test_cov import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
