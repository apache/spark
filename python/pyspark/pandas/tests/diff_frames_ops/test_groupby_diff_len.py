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


class GroupByDiffLenMixin:
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        set_option("compute.ops_on_diff_frames", True)

    @classmethod
    def tearDownClass(cls):
        reset_option("compute.ops_on_diff_frames")
        super().tearDownClass()

    def test_groupby_different_lengths(self):
        pdfs1 = [
            pd.DataFrame({"c": [4, 2, 7, 3, None, 1, 1, 1, 2], "d": list("abcdefght")}),
            pd.DataFrame({"c": [4, 2, 7, None, 1, 1, 2], "d": list("abcdefg")}),
            pd.DataFrame({"c": [4, 2, 7, 3, None, 1, 1, 1, 2, 2], "d": list("abcdefghti")}),
        ]
        pdfs2 = [
            pd.DataFrame({"a": [1, 2, 6, 4, 4, 6, 4, 3, 7], "b": [4, 2, 7, 3, 3, 1, 1, 1, 2]}),
            pd.DataFrame({"a": [1, 2, 6, 4, 4, 6, 4, 7], "b": [4, 2, 7, 3, 3, 1, 1, 2]}),
            pd.DataFrame({"a": [1, 2, 6, 4, 4, 6, 4, 3, 7], "b": [4, 2, 7, 3, 3, 1, 1, 1, 2]}),
        ]

        for pdf1, pdf2 in zip(pdfs1, pdfs2):
            psdf1 = ps.from_pandas(pdf1)
            psdf2 = ps.from_pandas(pdf2)

            for as_index in [True, False]:
                if as_index:

                    def sort(df):
                        return df.sort_index()

                else:

                    def sort(df):
                        return df.sort_values("c").reset_index(drop=True)

                self.assert_eq(
                    sort(psdf1.groupby(psdf2.a, as_index=as_index).sum()),
                    sort(pdf1.groupby(pdf2.a, as_index=as_index).sum()),
                    almost=as_index,
                )

                self.assert_eq(
                    sort(psdf1.groupby(psdf2.a, as_index=as_index).c.sum()),
                    sort(pdf1.groupby(pdf2.a, as_index=as_index).c.sum()),
                    almost=as_index,
                )
                self.assert_eq(
                    sort(psdf1.groupby(psdf2.a, as_index=as_index)["c"].sum()),
                    sort(pdf1.groupby(pdf2.a, as_index=as_index)["c"].sum()),
                    almost=as_index,
                )


class GroupByDiffLenTests(
    GroupByDiffLenMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.diff_frames_ops.test_groupby_diff_len import *  # noqa

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
