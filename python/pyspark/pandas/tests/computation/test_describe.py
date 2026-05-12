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

from pyspark.loose_version import LooseVersion
from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


class FrameDescribeMixin:
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # Some nanosecond->microsecond conversions throw loss of precision errors
        cls.spark.conf.set("spark.sql.execution.pandas.convertToArrowArraySafely", "false")

    @property
    def pdf(self):
        return pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9], "b": [4, 5, 6, 3, 2, 1, 0, 0, 0]},
            index=np.random.rand(9),
        )

    @property
    def df_pair(self):
        pdf = self.pdf
        psdf = ps.from_pandas(pdf)
        return pdf, psdf

    def test_describe(self):
        pdf, psdf = self.df_pair

        # numeric columns
        self.assert_eq(psdf.describe(), pdf.describe())
        psdf.a += psdf.a
        pdf.a += pdf.a
        self.assert_eq(psdf.describe(), pdf.describe())

        # string columns
        psdf = ps.DataFrame({"A": ["a", "b", "b", "c"], "B": ["d", "e", "f", "f"]})
        pdf = psdf._to_pandas()
        self.assert_eq(psdf.describe(), pdf.describe().astype(str))
        psdf.A += psdf.A
        pdf.A += pdf.A
        self.assert_eq(psdf.describe(), pdf.describe().astype(str))

        # timestamp columns
        psdf = ps.DataFrame(
            {
                "A": [
                    pd.Timestamp("2020-10-20"),
                    pd.Timestamp("2021-06-02"),
                    pd.Timestamp("2021-06-02"),
                    pd.Timestamp("2022-07-11"),
                ],
                "B": [
                    pd.Timestamp("2021-11-20"),
                    pd.Timestamp("2023-06-02"),
                    pd.Timestamp("2026-07-11"),
                    pd.Timestamp("2026-07-11"),
                ],
            }
        )
        pdf = psdf._to_pandas()
        self.assert_eq(
            psdf.describe().loc[["count", "mean", "min", "max"]],
            pdf.describe().astype(str).loc[["count", "mean", "min", "max"]],
        )

        # String & timestamp columns
        psdf = ps.DataFrame(
            {
                "A": ["a", "b", "b", "c"],
                "B": [
                    pd.Timestamp("2021-11-20"),
                    pd.Timestamp("2023-06-02"),
                    pd.Timestamp("2026-07-11"),
                    pd.Timestamp("2026-07-11"),
                ],
            }
        )
        pdf = psdf._to_pandas()
        self.assert_eq(
            psdf.describe().loc[["count", "mean", "min", "max"]],
            pdf.describe().astype(str).loc[["count", "mean", "min", "max"]],
        )
        psdf.A += psdf.A
        pdf.A += pdf.A
        self.assert_eq(
            psdf.describe().loc[["count", "mean", "min", "max"]],
            pdf.describe().astype(str).loc[["count", "mean", "min", "max"]],
        )

        # Numeric & timestamp columns
        psdf = ps.DataFrame(
            {
                "A": [1, 2, 2, 3],
                "B": [
                    pd.Timestamp("2021-11-20"),
                    pd.Timestamp("2023-06-02"),
                    pd.Timestamp("2026-07-11"),
                    pd.Timestamp("2026-07-11"),
                ],
            }
        )
        pdf = psdf._to_pandas()
        pandas_result = pdf.describe()
        pandas_result.B = pandas_result.B.astype(str)
        self.assert_eq(
            psdf.describe().loc[["count", "mean", "min", "max"]],
            pandas_result.loc[["count", "mean", "min", "max"]],
        )
        psdf.A += psdf.A
        pdf.A += pdf.A
        pandas_result = pdf.describe()
        pandas_result.B = pandas_result.B.astype(str)
        self.assert_eq(
            psdf.describe().loc[["count", "mean", "min", "max"]],
            pandas_result.loc[["count", "mean", "min", "max"]],
        )

        # Include None column
        psdf = ps.DataFrame(
            {
                "a": [1, 2, 3],
                "b": [pd.Timestamp(1), pd.Timestamp(1), pd.Timestamp(1)],
                "c": [None, None, None],
            }
        )
        pdf = psdf._to_pandas()
        pandas_result = pdf.describe()
        pandas_result.b = pandas_result.b.astype(str)
        self.assert_eq(
            psdf.describe().loc[["count", "mean", "min", "max"]],
            pandas_result.loc[["count", "mean", "min", "max"]],
        )

        msg = r"Percentiles should all be in the interval \[0, 1\]"
        with self.assertRaisesRegex(ValueError, msg):
            psdf.describe(percentiles=[1.1])

        psdf = ps.DataFrame()
        msg = "Cannot describe a DataFrame without columns"
        with self.assertRaisesRegex(ValueError, msg):
            psdf.describe()

    def test_describe_empty(self):
        # Empty DataFrame
        psdf = ps.DataFrame(columns=["A", "B"])
        pdf = psdf._to_pandas()
        self.assert_eq(
            psdf.describe(),
            pdf.describe().astype(float),
        )

        # Explicit empty DataFrame numeric only
        psdf = ps.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        pdf = psdf._to_pandas()
        self.assert_eq(
            psdf[psdf.a != psdf.a].describe(),
            pdf[pdf.a != pdf.a].describe(),
        )

        # Explicit empty DataFrame string only
        psdf = ps.DataFrame({"a": ["a", "b", "c"], "b": ["q", "w", "e"]})
        pdf = psdf._to_pandas()
        self.assert_eq(
            psdf[psdf.a != psdf.a].describe(),
            pdf[pdf.a != pdf.a].describe().astype(float),
        )

        # Explicit empty DataFrame timestamp only
        psdf = ps.DataFrame(
            {
                "a": [pd.Timestamp(1), pd.Timestamp(1), pd.Timestamp(1)],
                "b": [pd.Timestamp(1), pd.Timestamp(1), pd.Timestamp(1)],
            }
        )
        pdf = psdf._to_pandas()
        if LooseVersion(pd.__version__) < "3.0.0":
            # For timestamp type, we should convert NaT to None in pandas result
            # since pandas API on Spark doesn't support the NaT for object type.
            pdf_result = pdf[pdf.a != pdf.a].describe()
            pdf_result = pdf_result.where(pdf_result.notnull(), None).astype(str)
        else:
            # In pandas 3.0.0+, empty timestamp stats become missing values after astype(str),
            # and pandas API on Spark handles timestamp type as string type accordingly.
            pdf_result = pdf[pdf.a != pdf.a].describe().astype(str)
        self.assert_eq(psdf[psdf.a != psdf.a].describe(), pdf_result)

        # Explicit empty DataFrame numeric & timestamp
        psdf = ps.DataFrame(
            {"a": [1, 2, 3], "b": [pd.Timestamp(1), pd.Timestamp(1), pd.Timestamp(1)]}
        )
        pdf = psdf._to_pandas()
        if LooseVersion(pd.__version__) < "3.0.0":
            pdf_result = pdf[pdf.a != pdf.a].describe()
            pdf_result.b = pdf_result.b.where(pdf_result.b.notnull(), None).astype(str)
        else:
            pdf_result = pdf[pdf.a != pdf.a].describe()
            pdf_result.b = pdf_result.b.astype(str)
        self.assert_eq(psdf[psdf.a != psdf.a].describe(), pdf_result)

        # Explicit empty DataFrame numeric & string
        psdf = ps.DataFrame({"a": [1, 2, 3], "b": ["a", "b", "c"]})
        pdf = psdf._to_pandas()
        self.assert_eq(
            psdf[psdf.a != psdf.a].describe(),
            pdf[pdf.a != pdf.a].describe(),
        )

        # Explicit empty DataFrame string & timestamp
        psdf = ps.DataFrame(
            {"a": ["a", "b", "c"], "b": [pd.Timestamp(1), pd.Timestamp(1), pd.Timestamp(1)]}
        )
        pdf = psdf._to_pandas()
        if LooseVersion(pd.__version__) < "3.0.0":
            pdf_result = pdf[pdf.a != pdf.a].describe()
            pdf_result = pdf_result.where(pdf_result.notnull(), None).astype(str)
        else:
            pdf_result = pdf[pdf.a != pdf.a].describe()
            pdf_result.b = pdf_result.b.astype(str)
        self.assert_eq(psdf[psdf.a != psdf.a].describe(), pdf_result)

    def test_describe_include_exclude(self):
        # 9-row data is chosen so that `percentile_approx` (used by the numeric path) lands
        # on the same data points as pandas' linear interpolation, allowing direct
        # comparison.
        psdf = ps.DataFrame(
            {
                "num1": [1, 2, 3, 4, 5, 6, 7, 8, 9],
                "num2": [4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0],
                "str1": ["a", "b", "c", "d", "e", "f", "g", "h", "i"],
            }
        )
        pdf = psdf._to_pandas()

        self.assert_eq(
            psdf.describe(include=["int64", "float64"]),
            pdf.describe(include=["int64", "float64"]),
        )

        # pyspark.pandas returns string statistics as Python strings, so cast pandas'
        # object output to str before comparing.
        self.assert_eq(
            psdf.describe(exclude=["int64", "float64"]),
            pdf.describe(exclude=["int64", "float64"]).astype(str),
        )

        self.assert_eq(
            psdf.describe(include=["int64"], exclude=["float64"]),
            pdf.describe(include=["int64"], exclude=["float64"]),
        )

        with self.assertRaisesRegex(ValueError, "exclude must be None when include is 'all'"):
            psdf.describe(include="all", exclude=["int64"])

        with self.assertRaisesRegex(ValueError, "Cannot describe a DataFrame without columns"):
            psdf.describe(include=[np.datetime64])

    def test_describe_include_all_row_layout(self):
        # The mixed-type include="all" path must produce pandas' canonical row order.
        psdf = ps.DataFrame(
            {
                "num": [1, 2, 3, 4, 5, 6, 7, 8, 9],
                "str": ["a", "b", "c", "d", "e", "f", "g", "h", "i"],
            }
        )
        ps_result = psdf.describe(include="all")
        self.assertEqual(
            list(ps_result.index),
            ["count", "unique", "top", "freq", "mean", "min", "25%", "50%", "75%", "max", "std"],
        )
        # `unique`, `top`, `freq` are NaN for the numeric column; `mean`, `std`, `min`, `max`
        # are NaN for the string column.
        self.assertTrue(pd.isna(ps_result.loc["unique", "num"]))
        self.assertTrue(pd.isna(ps_result.loc["top", "num"]))
        self.assertTrue(pd.isna(ps_result.loc["freq", "num"]))
        self.assertTrue(pd.isna(ps_result.loc["mean", "str"]))
        self.assertTrue(pd.isna(ps_result.loc["std", "str"]))

    def test_describe_include_bool_only(self):
        # Bool was previously lumped into psser_string; promoting it to its own partition
        # must not change describe() output for bool-only frames.
        psdf = ps.DataFrame({"flag": [True, False, True, True, False]})
        pdf = psdf._to_pandas()
        self.assert_eq(
            psdf.describe(include=["bool"]),
            pdf.describe(include=["bool"]),
        )

    def test_describe_include_bool_with_others(self):
        psdf = ps.DataFrame(
            {
                "num": [1, 2, 3],
                "bool": [True, False, True],
                "str": ["a", "b", "c"],
            }
        )
        pdf = psdf._to_pandas()
        self.assert_eq(
            psdf.describe(include=["bool"]),
            pdf.describe(include=["bool"]),
        )

    def test_describe_include_all_with_timestamp(self):
        # 9-row data is chosen so that `percentile_approx` (used by both branches) lands
        # on the same data points as pandas' linear interpolation, allowing direct
        # comparison without skipping percentile rows.
        psdf = ps.DataFrame(
            {
                "num": [1, 2, 3, 4, 5, 6, 7, 8, 9],
                "str": ["a", "b", "c", "d", "e", "f", "g", "h", "i"],
                "bool": [True, False, True, False, True, False, True, False, True],
                "ts": [pd.Timestamp(f"202{i}-01-01") for i in range(9)],
            }
        )
        ps_result = psdf.describe(include="all")
        self.assertEqual(set(ps_result.columns), {"num", "str", "bool", "ts"})
        self.assertEqual(
            list(ps_result.index),
            ["count", "unique", "top", "freq", "mean", "min", "25%", "50%", "75%", "max", "std"],
        )
        # Object/timestamp columns expose count/unique/freq exactly.
        self.assertEqual(int(ps_result.loc["count", "str"]), 9)
        self.assertEqual(int(ps_result.loc["unique", "str"]), 9)
        self.assertEqual(int(ps_result.loc["count", "bool"]), 9)
        self.assertEqual(int(ps_result.loc["unique", "bool"]), 2)
        self.assertEqual(int(ps_result.loc["count", "ts"]), 9)
        self.assertEqual(int(ps_result.loc["unique", "ts"]), 9)
        # Numeric-only stats are populated for the numeric column.
        self.assertEqual(float(ps_result.loc["mean", "num"]), 5.0)
        self.assertEqual(float(ps_result.loc["std", "num"]), float(psdf["num"]._to_pandas().std()))


class FrameDescribeTests(
    FrameDescribeMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.testing import main

    main()
