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


class GroupByDiffMixin:
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        set_option("compute.ops_on_diff_frames", True)

    @classmethod
    def tearDownClass(cls):
        reset_option("compute.ops_on_diff_frames")
        super().tearDownClass()

    def test_diff(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 2, 3, 4, 5, 6] * 3,
                "b": [1, 1, 2, 3, 5, 8] * 3,
                "c": [1, 4, 9, 16, 25, 36] * 3,
            }
        )
        pkey = pd.Series([1, 1, 2, 3, 5, 8] * 3)
        psdf = ps.from_pandas(pdf)
        kkey = ps.from_pandas(pkey)

        self.assert_eq(
            psdf.groupby(kkey).diff().sort_index(), pdf.groupby(pkey).diff().sort_index()
        )
        self.assert_eq(
            psdf.groupby(kkey)["a"].diff().sort_index(), pdf.groupby(pkey)["a"].diff().sort_index()
        )
        self.assert_eq(
            psdf.groupby(kkey)[["a"]].diff().sort_index(),
            pdf.groupby(pkey)[["a"]].diff().sort_index(),
        )

        self.assert_eq(psdf.groupby(kkey).diff().sum(), pdf.groupby(pkey).diff().sum().astype(int))
        self.assert_eq(psdf.groupby(kkey)["a"].diff().sum(), pdf.groupby(pkey)["a"].diff().sum())


class GroupByDiffTests(
    GroupByDiffMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.diff_frames_ops.test_groupby_diff import *  # noqa

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
