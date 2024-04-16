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

from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase


class GetDummiesKWArgsMixin:
    def test_get_dummies_kwargs(self):
        # pser = pd.Series([1, 1, 1, 2, 2, 1, 3, 4], dtype='category')
        pser = pd.Series([1, 1, 1, 2, 2, 1, 3, 4])
        psser = ps.from_pandas(pser)
        self.assert_eq(
            ps.get_dummies(psser, prefix="X", prefix_sep="-"),
            pd.get_dummies(pser, prefix="X", prefix_sep="-", dtype=np.int8),
        )

        self.assert_eq(
            ps.get_dummies(psser, drop_first=True),
            pd.get_dummies(pser, drop_first=True, dtype=np.int8),
        )

        # nan
        # pser = pd.Series([1, 1, 1, 2, np.nan, 3, np.nan, 5], dtype='category')
        pser = pd.Series([1, 1, 1, 2, np.nan, 3, np.nan, 5])
        psser = ps.from_pandas(pser)
        self.assert_eq(ps.get_dummies(psser), pd.get_dummies(pser, dtype=np.int8), almost=True)

        # dummy_na
        self.assert_eq(
            ps.get_dummies(psser, dummy_na=True), pd.get_dummies(pser, dummy_na=True, dtype=np.int8)
        )


class GetDummiesKWArgsTests(
    GetDummiesKWArgsMixin,
    PandasOnSparkTestCase,
):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.reshape.test_get_dummies_kwargs import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
