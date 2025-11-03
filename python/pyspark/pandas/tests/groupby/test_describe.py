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

from itertools import product
import pandas as pd

from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


class GroupbyDescribeMixin:
    def test_describe(self):
        # support for numeric type, not support for string type yet
        datas = []
        datas.append({"a": [1, 1, 3], "b": [4, 5, 6], "c": [7, 8, 9]})
        datas.append({"a": [-1, -1, -3], "b": [-4, -5, -6], "c": [-7, -8, -9]})
        datas.append({"a": [0, 0, 0], "b": [0, 0, 0], "c": [0, 8, 0]})
        # it is okay if string type column as a group key
        datas.append({"a": ["a", "a", "c"], "b": [4, 5, 6], "c": [7, 8, 9]})

        percentiles = [0.25, 0.5, 0.75]
        formatted_percentiles = ["25%", "50%", "75%"]
        non_percentile_stats = ["count", "mean", "std", "min", "max"]

        for data in datas:
            pdf = pd.DataFrame(data)
            psdf = ps.from_pandas(pdf)

            describe_pdf = pdf.groupby("a").describe().sort_index()
            describe_psdf = psdf.groupby("a").describe().sort_index()

            # since the result of percentile columns are slightly difference from pandas,
            # we should check them separately: non-percentile columns & percentile columns

            # 1. Check that non-percentile columns are equal.
            agg_cols = [col.name for col in psdf.groupby("a")._agg_columns]
            self.assert_eq(
                describe_psdf.drop(columns=list(product(agg_cols, formatted_percentiles))),
                describe_pdf.drop(columns=formatted_percentiles, level=1),
                check_exact=False,
            )

            # 2. Check that percentile columns are equal.
            # The interpolation argument is yet to be implemented in Koalas.
            quantile_pdf = pdf.groupby("a").quantile(percentiles, interpolation="nearest")
            quantile_pdf = quantile_pdf.unstack(level=1).astype(float)
            self.assert_eq(
                describe_psdf.drop(columns=list(product(agg_cols, non_percentile_stats))),
                quantile_pdf.rename(columns="{:.0%}".format, level=1),
            )

        # not support for string type yet
        datas = []
        datas.append({"a": ["a", "a", "c"], "b": ["d", "e", "f"], "c": ["g", "h", "i"]})
        datas.append({"a": ["a", "a", "c"], "b": [4, 0, 1], "c": ["g", "h", "i"]})
        for data in datas:
            pdf = pd.DataFrame(data)
            psdf = ps.from_pandas(pdf)

            self.assertRaises(
                NotImplementedError, lambda: psdf.groupby("a").describe().sort_index()
            )

        # multi-index columns
        pdf = pd.DataFrame({("x", "a"): [1, 1, 3], ("x", "b"): [4, 5, 6], ("y", "c"): [7, 8, 9]})
        psdf = ps.from_pandas(pdf)

        describe_pdf = pdf.groupby(("x", "a")).describe().sort_index()
        describe_psdf = psdf.groupby(("x", "a")).describe().sort_index()

        # 1. Check that non-percentile columns are equal.
        agg_column_labels = [col._column_label for col in psdf.groupby(("x", "a"))._agg_columns]
        self.assert_eq(
            describe_psdf.drop(
                columns=[
                    tuple(list(label) + [s])
                    for label, s in product(agg_column_labels, formatted_percentiles)
                ]
            ),
            describe_pdf.drop(columns=formatted_percentiles, level=2),
            check_exact=False,
        )

        # 2. Check that percentile columns are equal.
        # The interpolation argument is yet to be implemented in Koalas.
        quantile_pdf = pdf.groupby(("x", "a")).quantile(percentiles, interpolation="nearest")
        quantile_pdf = quantile_pdf.unstack(level=1).astype(float)

        self.assert_eq(
            describe_psdf.drop(
                columns=[
                    tuple(list(label) + [s])
                    for label, s in product(agg_column_labels, non_percentile_stats)
                ]
            ),
            quantile_pdf.rename(columns="{:.0%}".format, level=2),
        )


class GroupbyDescribeTests(
    GroupbyDescribeMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.groupby.test_describe import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
