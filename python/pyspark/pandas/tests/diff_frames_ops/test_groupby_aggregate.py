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


class GroupByAggregateMixin:
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        set_option("compute.ops_on_diff_frames", True)

    @classmethod
    def tearDownClass(cls):
        reset_option("compute.ops_on_diff_frames")
        super().tearDownClass()

    def test_aggregate(self):
        pdf1 = pd.DataFrame({"C": [0.362, 0.227, 1.267, -0.562], "B": [1, 2, 3, 4]})
        pdf2 = pd.DataFrame({"A": [1, 1, 2, 2]})
        psdf1 = ps.from_pandas(pdf1)
        psdf2 = ps.from_pandas(pdf2)

        for as_index in [True, False]:
            if as_index:

                def sort(df):
                    return df.sort_index()

            else:

                def sort(df):
                    return df.sort_values(list(df.columns)).reset_index(drop=True)

            with self.subTest(as_index=as_index):
                self.assert_eq(
                    sort(psdf1.groupby(psdf2.A, as_index=as_index).agg("sum")),
                    sort(pdf1.groupby(pdf2.A, as_index=as_index).agg("sum")),
                )
                self.assert_eq(
                    sort(psdf1.groupby(psdf2.A, as_index=as_index).agg({"B": "min", "C": "sum"})),
                    sort(pdf1.groupby(pdf2.A, as_index=as_index).agg({"B": "min", "C": "sum"})),
                )
                self.assert_eq(
                    sort(
                        psdf1.groupby(psdf2.A, as_index=as_index).agg(
                            {"B": ["min", "max"], "C": "sum"}
                        )
                    ),
                    sort(
                        pdf1.groupby(pdf2.A, as_index=as_index).agg(
                            {"B": ["min", "max"], "C": "sum"}
                        )
                    ),
                )
                self.assert_eq(
                    sort(psdf1.groupby([psdf1.C, psdf2.A], as_index=as_index).agg("sum")),
                    sort(pdf1.groupby([pdf1.C, pdf2.A], as_index=as_index).agg("sum")),
                )
                self.assert_eq(
                    sort(psdf1.groupby([psdf1.C + 1, psdf2.A], as_index=as_index).agg("sum")),
                    sort(pdf1.groupby([pdf1.C + 1, pdf2.A], as_index=as_index).agg("sum")),
                )

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("Y", "C"), ("X", "B")])
        pdf1.columns = columns
        psdf1.columns = columns

        columns = pd.MultiIndex.from_tuples([("X", "A")])
        pdf2.columns = columns
        psdf2.columns = columns

        for as_index in [True, False]:
            stats_psdf = psdf1.groupby(psdf2[("X", "A")], as_index=as_index).agg(
                {("X", "B"): "min", ("Y", "C"): "sum"}
            )
            stats_pdf = pdf1.groupby(pdf2[("X", "A")], as_index=as_index).agg(
                {("X", "B"): "min", ("Y", "C"): "sum"}
            )
            self.assert_eq(
                stats_psdf.sort_values(by=[("X", "B"), ("Y", "C")]).reset_index(drop=True),
                stats_pdf.sort_values(by=[("X", "B"), ("Y", "C")]).reset_index(drop=True),
            )

        stats_psdf = psdf1.groupby(psdf2[("X", "A")]).agg(
            {("X", "B"): ["min", "max"], ("Y", "C"): "sum"}
        )
        stats_pdf = pdf1.groupby(pdf2[("X", "A")]).agg(
            {("X", "B"): ["min", "max"], ("Y", "C"): "sum"}
        )
        self.assert_eq(
            stats_psdf.sort_values(
                by=[("X", "B", "min"), ("X", "B", "max"), ("Y", "C", "sum")]
            ).reset_index(drop=True),
            stats_pdf.sort_values(
                by=[("X", "B", "min"), ("X", "B", "max"), ("Y", "C", "sum")]
            ).reset_index(drop=True),
        )


class GroupByAggregateTests(
    GroupByAggregateMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.diff_frames_ops.test_groupby_aggregate import *  # noqa

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
