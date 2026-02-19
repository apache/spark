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
from pyspark.loose_version import LooseVersion
from pyspark.pandas.config import set_option, reset_option
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


class GroupByFillNAMixin:
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        set_option("compute.ops_on_diff_frames", True)

    @classmethod
    def tearDownClass(cls):
        reset_option("compute.ops_on_diff_frames")
        super().tearDownClass()

    def test_fillna(self):
        pdf = pd.DataFrame(
            {
                "A": [1, 1, 2, 2] * 3,
                "B": [2, 4, None, 3] * 3,
                "C": [None, None, None, 1] * 3,
                "D": [0, 1, 5, 4] * 3,
            }
        )
        pkey = pd.Series([1, 1, 2, 2] * 3)
        psdf = ps.from_pandas(pdf)
        kkey = ps.from_pandas(pkey)

        if LooseVersion(pd.__version__) < "3.0.0":
            self.assert_eq(
                psdf.groupby(kkey).fillna(0).sort_index(), pdf.groupby(pkey).fillna(0).sort_index()
            )
            self.assert_eq(
                psdf.groupby(kkey)["C"].fillna(0).sort_index(),
                pdf.groupby(pkey)["C"].fillna(0).sort_index(),
            )
            self.assert_eq(
                psdf.groupby(kkey)[["C"]].fillna(0).sort_index(),
                pdf.groupby(pkey)[["C"]].fillna(0).sort_index(),
            )
            self.assert_eq(
                psdf.groupby(kkey).fillna(method="bfill").sort_index(),
                pdf.groupby(pkey).fillna(method="bfill").sort_index(),
            )
            self.assert_eq(
                psdf.groupby(kkey)["C"].fillna(method="bfill").sort_index(),
                pdf.groupby(pkey)["C"].fillna(method="bfill").sort_index(),
            )
            self.assert_eq(
                psdf.groupby(kkey)[["C"]].fillna(method="bfill").sort_index(),
                pdf.groupby(pkey)[["C"]].fillna(method="bfill").sort_index(),
            )
            self.assert_eq(
                psdf.groupby(kkey).fillna(method="ffill").sort_index(),
                pdf.groupby(pkey).fillna(method="ffill").sort_index(),
            )
            self.assert_eq(
                psdf.groupby(kkey)["C"].fillna(method="ffill").sort_index(),
                pdf.groupby(pkey)["C"].fillna(method="ffill").sort_index(),
            )
            self.assert_eq(
                psdf.groupby(kkey)[["C"]].fillna(method="ffill").sort_index(),
                pdf.groupby(pkey)[["C"]].fillna(method="ffill").sort_index(),
            )
        else:
            with self.assertRaises(AttributeError):
                psdf.groupby(kkey).fillna(0)
            with self.assertRaises(AttributeError):
                psdf.groupby(kkey)["C"].fillna(0)
            with self.assertRaises(AttributeError):
                psdf.groupby(kkey)[["C"]].fillna(0)
            with self.assertRaises(AttributeError):
                psdf.groupby(kkey).fillna(method="bfill")
            with self.assertRaises(AttributeError):
                psdf.groupby(kkey)["C"].fillna(method="bfill")


class GroupByFillNATests(
    GroupByFillNAMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.testing import main

    main()
