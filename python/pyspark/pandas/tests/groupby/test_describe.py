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

    def _check_series_groupby_describe(self, pdf, groupby_col, value_col):
        """Helper to check SeriesGroupBy.describe against pandas."""
        psdf = ps.from_pandas(pdf)

        describe_pdf = pdf.groupby(groupby_col)[value_col].describe().sort_index()
        describe_psdf = psdf.groupby(groupby_col)[value_col].describe().sort_index()

        non_percentile_stats = ["count", "mean", "std", "min", "max"]
        formatted_percentiles = ["25%", "50%", "75%"]
        percentiles = [0.25, 0.5, 0.75]

        # 1. Check non-percentile stats.
        self.assert_eq(
            describe_psdf[non_percentile_stats],
            describe_pdf[non_percentile_stats],
            check_exact=False,
        )

        # 2. Check percentile stats (approximate percentiles use nearest interpolation).
        quantile_pdf = (
            pdf.groupby(groupby_col)[value_col]
            .quantile(percentiles, interpolation="nearest")
            .unstack(level=1)
            .astype(float)
        )
        quantile_pdf.columns = ["{:.0%}".format(p) for p in percentiles]
        self.assert_eq(
            describe_psdf[formatted_percentiles],
            quantile_pdf,
        )

    def test_series_groupby_describe(self):
        # Basic numeric case
        self._check_series_groupby_describe(
            pd.DataFrame({"a": [1, 1, 3], "b": [4, 5, 6]}), "a", "b"
        )

        # Floats and negatives with larger groups
        self._check_series_groupby_describe(
            pd.DataFrame({"a": [1, 1, 1, 2, 2, 2], "b": [-1.5, 2.0, 3.5, 10.0, 20.0, 30.0]}),
            "a",
            "b",
        )

        # Same-value groups: std should be 0.0, not NaN
        pdf = pd.DataFrame({"a": [1, 1, 2], "b": [5, 5, 10]})
        psdf = ps.from_pandas(pdf)
        describe_psdf = psdf.groupby("a")["b"].describe().sort_index()
        describe_pdf = pdf.groupby("a")["b"].describe().sort_index()
        # Group a=1 has two identical values, so std must be 0.0
        self.assertEqual(describe_psdf.loc[1, "std"], 0.0)
        self.assert_eq(
            describe_psdf[["count", "mean", "std", "min", "max"]],
            describe_pdf[["count", "mean", "std", "min", "max"]],
            check_exact=False,
        )

        # String group key with numeric values -- check both non-percentile and percentile stats
        self._check_series_groupby_describe(
            pd.DataFrame({"a": ["x", "x", "y"], "b": [4, 5, 6]}), "a", "b"
        )

        # String type series should raise NotImplementedError with a descriptive message
        pdf = pd.DataFrame({"a": ["x", "x", "y"], "b": ["d", "e", "f"]})
        psdf = ps.from_pandas(pdf)
        self.assertRaisesRegex(
            NotImplementedError,
            "doesn't support for string type",
            lambda: psdf.groupby("a")["b"].describe(),
        )


class GroupbyDescribeTests(
    GroupbyDescribeMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.testing import main

    main()
