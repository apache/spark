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


class GroupByMixin:
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        set_option("compute.ops_on_diff_frames", True)

    @classmethod
    def tearDownClass(cls):
        reset_option("compute.ops_on_diff_frames")
        super().tearDownClass()

    def test_groupby_multiindex_columns(self):
        pdf1 = pd.DataFrame(
            {("y", "c"): [4, 2, 7, 3, None, 1, 1, 1, 2], ("z", "d"): list("abcdefght")}
        )
        pdf2 = pd.DataFrame(
            {("x", "a"): [1, 2, 6, 4, 4, 6, 4, 3, 7], ("x", "b"): [4, 2, 7, 3, 3, 1, 1, 1, 2]}
        )
        psdf1 = ps.from_pandas(pdf1)
        psdf2 = ps.from_pandas(pdf2)

        self.assert_eq(
            psdf1.groupby(psdf2[("x", "a")]).sum().sort_index(),
            pdf1.groupby(pdf2[("x", "a")]).sum().sort_index(),
        )

        self.assert_eq(
            psdf1.groupby(psdf2[("x", "a")], as_index=False)
            .sum()
            .sort_values(("y", "c"))
            .reset_index(drop=True),
            pdf1.groupby(pdf2[("x", "a")], as_index=False)
            .sum()
            .sort_values(("y", "c"))
            .reset_index(drop=True),
        )
        self.assert_eq(
            psdf1.groupby(psdf2[("x", "a")])[[("y", "c")]].sum().sort_index(),
            pdf1.groupby(pdf2[("x", "a")])[[("y", "c")]].sum().sort_index(),
        )

    def test_duplicated_labels(self):
        pdf1 = pd.DataFrame({"A": [3, 2, 1]})
        pdf2 = pd.DataFrame({"A": [1, 2, 3]})
        psdf1 = ps.from_pandas(pdf1)
        psdf2 = ps.from_pandas(pdf2)

        self.assert_eq(
            psdf1.groupby(psdf2.A).sum().sort_index(), pdf1.groupby(pdf2.A).sum().sort_index()
        )
        self.assert_eq(
            psdf1.groupby(psdf2.A, as_index=False).sum().sort_values("A").reset_index(drop=True),
            pdf1.groupby(pdf2.A, as_index=False).sum().sort_values("A").reset_index(drop=True),
        )

    def test_head(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 1, 1, 1, 2, 2, 2, 3, 3, 3] * 3,
                "b": [2, 3, 1, 4, 6, 9, 8, 10, 7, 5] * 3,
                "c": [3, 5, 2, 5, 1, 2, 6, 4, 3, 6] * 3,
            },
        )
        pkey = pd.Series([1, 1, 1, 1, 2, 2, 2, 3, 3, 3] * 3)
        psdf = ps.from_pandas(pdf)
        kkey = ps.from_pandas(pkey)

        self.assert_eq(
            pdf.groupby(pkey).head(2).sort_index(), psdf.groupby(kkey).head(2).sort_index()
        )
        self.assert_eq(
            pdf.groupby("a")["b"].head(2).sort_index(), psdf.groupby("a")["b"].head(2).sort_index()
        )
        self.assert_eq(
            pdf.groupby("a")[["b"]].head(2).sort_index(),
            psdf.groupby("a")[["b"]].head(2).sort_index(),
        )
        self.assert_eq(
            pdf.groupby([pkey, "b"]).head(2).sort_index(),
            psdf.groupby([kkey, "b"]).head(2).sort_index(),
        )


class GroupByTests(
    GroupByMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.diff_frames_ops.test_groupby import *  # noqa

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
