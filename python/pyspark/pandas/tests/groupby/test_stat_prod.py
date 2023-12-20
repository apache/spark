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
from pyspark.pandas.tests.groupby.test_stat import GroupbyStatTestingFuncMixin


class ProdTestsMixin(GroupbyStatTestingFuncMixin):
    @property
    def pdf(self):
        return pd.DataFrame(
            {
                "A": [1, 2, 1, 2],
                "B": [3.1, 4.1, 4.1, 3.1],
                "C": ["a", "b", "b", "a"],
                "D": [True, False, False, True],
            }
        )

    @property
    def psdf(self):
        return ps.from_pandas(self.pdf)

    def test_prod(self):
        pdf = pd.DataFrame(
            {
                "A": [1, 2, 1, 2, 1],
                "B": [3.1, 4.1, 4.1, 3.1, 0.1],
                "C": ["a", "b", "b", "a", "c"],
                "D": [True, False, False, True, False],
                "E": [-1, -2, 3, -4, -2],
                "F": [-1.5, np.nan, -3.2, 0.1, 0],
                "G": [np.nan, np.nan, np.nan, np.nan, np.nan],
            }
        )
        psdf = ps.from_pandas(pdf)

        for n in [0, 1, 2, 128, -1, -2, -128]:
            self._test_stat_func(
                lambda groupby_obj: groupby_obj.prod(numeric_only=True, min_count=n),
                check_exact=False,
            )
            self.assert_eq(
                pdf.groupby("A").prod(min_count=n, numeric_only=True).sort_index(),
                psdf.groupby("A").prod(min_count=n).sort_index(),
                almost=True,
            )


class ProdTests(
    ProdTestsMixin,
    PandasOnSparkTestCase,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.groupby.test_stat_prod import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
