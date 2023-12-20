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


class DdofTestsMixin:
    def test_ddof(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 1, 1, 1, 2, 2, 2, 3, 3, 3] * 3,
                "b": [2, 3, 1, 4, 6, 9, 8, 10, 7, 5] * 3,
                "c": [3, 5, 2, 5, 1, 2, 6, 4, 3, 6] * 3,
            },
            index=np.random.rand(10 * 3),
        )
        psdf = ps.from_pandas(pdf)

        for ddof in [-1, 0, 1, 2, 3]:
            # std
            self.assert_eq(
                pdf.groupby("a").std(ddof=ddof).sort_index(),
                psdf.groupby("a").std(ddof=ddof).sort_index(),
                check_exact=False,
            )
            self.assert_eq(
                pdf.groupby("a")["b"].std(ddof=ddof).sort_index(),
                psdf.groupby("a")["b"].std(ddof=ddof).sort_index(),
                check_exact=False,
            )
            # var
            self.assert_eq(
                pdf.groupby("a").var(ddof=ddof).sort_index(),
                psdf.groupby("a").var(ddof=ddof).sort_index(),
                check_exact=False,
            )
            self.assert_eq(
                pdf.groupby("a")["b"].var(ddof=ddof).sort_index(),
                psdf.groupby("a")["b"].var(ddof=ddof).sort_index(),
                check_exact=False,
            )
            # sem
            self.assert_eq(
                pdf.groupby("a").sem(ddof=ddof).sort_index(),
                psdf.groupby("a").sem(ddof=ddof).sort_index(),
                check_exact=False,
            )
            self.assert_eq(
                pdf.groupby("a")["b"].sem(ddof=ddof).sort_index(),
                psdf.groupby("a")["b"].sem(ddof=ddof).sort_index(),
                check_exact=False,
            )


class DdofTests(
    DdofTestsMixin,
    PandasOnSparkTestCase,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.groupby.test_stat_ddof import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
