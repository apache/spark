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
import decimal
from distutils.version import LooseVersion
import unittest

import numpy as np
import pandas as pd

from pyspark import pandas as ps
from pyspark.testing.pandasutils import ComparisonTestBase
from pyspark.testing.sqlutils import SQLTestUtils


# This file contains test cases for 'Computations / Descriptive Stats'
# https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/frame.html#computations-descriptive-stats
class FrameComputeMixin:
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

    def test_abs(self):
        pdf = pd.DataFrame({"a": [-2, -1, 0, 1]})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(abs(psdf), abs(pdf))
        self.assert_eq(np.abs(psdf), np.abs(pdf))

    def test_all(self):
        pdf = pd.DataFrame(
            {
                "col1": [False, False, False],
                "col2": [True, False, False],
                "col3": [0, 0, 1],
                "col4": [0, 1, 2],
                "col5": [False, False, None],
                "col6": [True, False, None],
            },
            index=np.random.rand(3),
        )
        pdf.name = "x"
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.all(), pdf.all())
        self.assert_eq(psdf.all(bool_only=True), pdf.all(bool_only=True))
        self.assert_eq(psdf.all(bool_only=False), pdf.all(bool_only=False))
        self.assert_eq(psdf[["col5"]].all(bool_only=True), pdf[["col5"]].all(bool_only=True))
        self.assert_eq(psdf[["col5"]].all(bool_only=False), pdf[["col5"]].all(bool_only=False))

        columns = pd.MultiIndex.from_tuples(
            [
                ("a", "col1"),
                ("a", "col2"),
                ("a", "col3"),
                ("b", "col4"),
                ("b", "col5"),
                ("c", "col6"),
            ]
        )
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(psdf.all(), pdf.all())
        self.assert_eq(psdf.all(bool_only=True), pdf.all(bool_only=True))
        self.assert_eq(psdf.all(bool_only=False), pdf.all(bool_only=False))

        columns.names = ["X", "Y"]
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(psdf.all(), pdf.all())
        self.assert_eq(psdf.all(bool_only=True), pdf.all(bool_only=True))
        self.assert_eq(psdf.all(bool_only=False), pdf.all(bool_only=False))

        with self.assertRaisesRegex(
            NotImplementedError, 'axis should be either 0 or "index" currently.'
        ):
            psdf.all(axis=1)

        # Test skipna
        pdf = pd.DataFrame({"A": [True, True], "B": [1, np.nan], "C": [True, None]})
        pdf.name = "x"
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psdf[["A", "B"]].all(skipna=False), pdf[["A", "B"]].all(skipna=False))
        self.assert_eq(psdf[["A", "C"]].all(skipna=False), pdf[["A", "C"]].all(skipna=False))
        self.assert_eq(psdf[["B", "C"]].all(skipna=False), pdf[["B", "C"]].all(skipna=False))
        self.assert_eq(psdf.all(skipna=False), pdf.all(skipna=False))
        self.assert_eq(psdf.all(skipna=True), pdf.all(skipna=True))
        self.assert_eq(psdf.all(), pdf.all())
        self.assert_eq(
            ps.DataFrame([np.nan]).all(skipna=False), pd.DataFrame([np.nan]).all(skipna=False)
        )
        self.assert_eq(ps.DataFrame([None]).all(skipna=True), pd.DataFrame([None]).all(skipna=True))

    def test_any(self):
        pdf = pd.DataFrame(
            {
                "col1": [False, False, False],
                "col2": [True, False, False],
                "col3": [0, 0, 1],
                "col4": [0, 1, 2],
                "col5": [False, False, None],
                "col6": [True, False, None],
            },
            index=np.random.rand(3),
        )
        pdf.name = "x"
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.any(), pdf.any())
        self.assert_eq(psdf.any(bool_only=True), pdf.any(bool_only=True))
        self.assert_eq(psdf.any(bool_only=False), pdf.any(bool_only=False))
        self.assert_eq(psdf[["col5"]].all(bool_only=True), pdf[["col5"]].all(bool_only=True))
        self.assert_eq(psdf[["col5"]].all(bool_only=False), pdf[["col5"]].all(bool_only=False))

        columns = pd.MultiIndex.from_tuples(
            [
                ("a", "col1"),
                ("a", "col2"),
                ("a", "col3"),
                ("b", "col4"),
                ("b", "col5"),
                ("c", "col6"),
            ]
        )
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(psdf.any(), pdf.any())
        self.assert_eq(psdf.any(bool_only=True), pdf.any(bool_only=True))
        self.assert_eq(psdf.any(bool_only=False), pdf.any(bool_only=False))

        columns.names = ["X", "Y"]
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(psdf.any(), pdf.any())
        self.assert_eq(psdf.any(bool_only=True), pdf.any(bool_only=True))
        self.assert_eq(psdf.any(bool_only=False), pdf.any(bool_only=False))

        with self.assertRaisesRegex(
            NotImplementedError, 'axis should be either 0 or "index" currently.'
        ):
            psdf.any(axis=1)

    def test_clip(self):
        pdf = pd.DataFrame(
            {"A": [0, 2, 4], "B": [4, 2, 0], "X": [-1, 10, 0]}, index=np.random.rand(3)
        )
        psdf = ps.from_pandas(pdf)

        # Assert list-like values are not accepted for 'lower' and 'upper'
        msg = "List-like value are not supported for 'lower' and 'upper' at the moment"
        with self.assertRaises(TypeError, msg=msg):
            psdf.clip(lower=[1])
        with self.assertRaises(TypeError, msg=msg):
            psdf.clip(upper=[1])

        # Assert no lower or upper
        self.assert_eq(psdf.clip(), pdf.clip())
        # Assert lower only
        self.assert_eq(psdf.clip(1), pdf.clip(1))
        # Assert upper only
        self.assert_eq(psdf.clip(upper=3), pdf.clip(upper=3))
        # Assert lower and upper
        self.assert_eq(psdf.clip(1, 3), pdf.clip(1, 3))

        pdf["clip"] = pdf.A.clip(lower=1, upper=3)
        psdf["clip"] = psdf.A.clip(lower=1, upper=3)
        self.assert_eq(psdf, pdf)

        # Assert behavior on string values
        str_psdf = ps.DataFrame({"A": ["a", "b", "c"]}, index=np.random.rand(3))
        self.assert_eq(str_psdf.clip(1, 3), str_psdf)

    def test_corrwith(self):
        df1 = ps.DataFrame(
            {"A": [1, np.nan, 7, 8], "B": [False, True, True, False], "C": [10, 4, 9, 3]}
        )
        df2 = df1[["A", "C"]]
        df3 = df1[["B", "C"]]
        self._test_corrwith(df1, df2)
        self._test_corrwith(df1, df3)
        self._test_corrwith((df1 + 1), df2.A)
        self._test_corrwith((df1 + 1), df3.B)
        self._test_corrwith((df1 + 1), (df2.C + 2))
        self._test_corrwith((df1 + 1), (df3.B + 2))

        with self.assertRaisesRegex(TypeError, "unsupported type"):
            df1.corrwith(123)
        with self.assertRaisesRegex(NotImplementedError, "only works for axis=0"):
            df1.corrwith(df1.A, axis=1)
        with self.assertRaisesRegex(ValueError, "Invalid method"):
            df1.corrwith(df1.A, method="cov")

        df_bool = ps.DataFrame({"A": [True, True, False, False], "B": [True, False, False, True]})
        self._test_corrwith(df_bool, df_bool.A)
        self._test_corrwith(df_bool, df_bool.B)

    def _test_corrwith(self, psdf, psobj):
        pdf = psdf._to_pandas()
        pobj = psobj._to_pandas()
        # There was a regression in pandas 1.5.0
        # when other is Series and method is "pearson" or "spearman", and fixed in pandas 1.5.1
        # Therefore, we only test the pandas 1.5.0 in different way.
        # See https://github.com/pandas-dev/pandas/issues/48826 for the reported issue,
        # and https://github.com/pandas-dev/pandas/pull/46174 for the initial PR that causes.
        if LooseVersion(pd.__version__) == LooseVersion("1.5.0") and isinstance(pobj, pd.Series):
            methods = ["kendall"]
        else:
            methods = ["pearson", "spearman", "kendall"]
        for method in methods:
            for drop in [True, False]:
                p_corr = pdf.corrwith(pobj, drop=drop, method=method)
                ps_corr = psdf.corrwith(psobj, drop=drop, method=method)
                self.assert_eq(p_corr.sort_index(), ps_corr.sort_index(), almost=True)

    def test_cov(self):
        # SPARK-36396: Implement DataFrame.cov

        # int
        pdf = pd.DataFrame([(1, 2), (0, 3), (2, 0), (1, 1)], columns=["a", "b"])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.cov(), psdf.cov(), almost=True)
        self.assert_eq(pdf.cov(min_periods=4), psdf.cov(min_periods=4), almost=True)
        self.assert_eq(pdf.cov(min_periods=5), psdf.cov(min_periods=5))

        # ddof
        with self.assertRaisesRegex(TypeError, "ddof must be integer"):
            psdf.cov(ddof="ddof")
        for ddof in [-1, 0, 2]:
            self.assert_eq(pdf.cov(ddof=ddof), psdf.cov(ddof=ddof), almost=True)
            self.assert_eq(
                pdf.cov(min_periods=4, ddof=ddof), psdf.cov(min_periods=4, ddof=ddof), almost=True
            )
            self.assert_eq(pdf.cov(min_periods=5, ddof=ddof), psdf.cov(min_periods=5, ddof=ddof))

        # bool
        pdf = pd.DataFrame(
            {
                "a": [1, np.nan, 3, 4],
                "b": [True, False, False, True],
                "c": [True, True, False, True],
            }
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.cov(), psdf.cov(), almost=True)
        self.assert_eq(pdf.cov(min_periods=4), psdf.cov(min_periods=4), almost=True)
        self.assert_eq(pdf.cov(min_periods=5), psdf.cov(min_periods=5))

        # extension dtype
        if LooseVersion(pd.__version__) >= LooseVersion("1.2"):
            numeric_dtypes = ["Int8", "Int16", "Int32", "Int64", "Float32", "Float64", "float"]
            boolean_dtypes = ["boolean", "bool"]
        else:
            numeric_dtypes = ["Int8", "Int16", "Int32", "Int64", "float"]
            boolean_dtypes = ["boolean", "bool"]

        sers = [pd.Series([1, 2, 3, None], dtype=dtype) for dtype in numeric_dtypes]
        sers += [pd.Series([True, False, True, None], dtype=dtype) for dtype in boolean_dtypes]
        sers.append(pd.Series([decimal.Decimal(1), decimal.Decimal(2), decimal.Decimal(3), None]))

        pdf = pd.concat(sers, axis=1)
        pdf.columns = [dtype for dtype in numeric_dtypes + boolean_dtypes] + ["decimal"]
        psdf = ps.from_pandas(pdf)

        if LooseVersion(pd.__version__) >= LooseVersion("1.2"):
            self.assert_eq(pdf.cov(), psdf.cov(), almost=True)
            self.assert_eq(pdf.cov(min_periods=3), psdf.cov(min_periods=3), almost=True)
            self.assert_eq(pdf.cov(min_periods=4), psdf.cov(min_periods=4))
        else:
            test_types = [
                "Int8",
                "Int16",
                "Int32",
                "Int64",
                "float",
                "boolean",
                "bool",
            ]
            expected = pd.DataFrame(
                data=[
                    [1.0, 1.0, 1.0, 1.0, 1.0, 0.0000000, 0.0000000],
                    [1.0, 1.0, 1.0, 1.0, 1.0, 0.0000000, 0.0000000],
                    [1.0, 1.0, 1.0, 1.0, 1.0, 0.0000000, 0.0000000],
                    [1.0, 1.0, 1.0, 1.0, 1.0, 0.0000000, 0.0000000],
                    [1.0, 1.0, 1.0, 1.0, 1.0, 0.0000000, 0.0000000],
                    [0.0, 0.0, 0.0, 0.0, 0.0, 0.3333333, 0.3333333],
                    [0.0, 0.0, 0.0, 0.0, 0.0, 0.3333333, 0.3333333],
                ],
                index=test_types,
                columns=test_types,
            )
            self.assert_eq(expected, psdf.cov(), almost=True)

        # string column
        pdf = pd.DataFrame(
            [(1, 2, "a", 1), (0, 3, "b", 1), (2, 0, "c", 9), (1, 1, "d", 1)],
            columns=["a", "b", "c", "d"],
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.cov(), psdf.cov(), almost=True)
        self.assert_eq(pdf.cov(min_periods=4), psdf.cov(min_periods=4), almost=True)
        self.assert_eq(pdf.cov(min_periods=5), psdf.cov(min_periods=5))

        # nan
        np.random.seed(42)
        pdf = pd.DataFrame(np.random.randn(20, 3), columns=["a", "b", "c"])
        pdf.loc[pdf.index[:5], "a"] = np.nan
        pdf.loc[pdf.index[5:10], "b"] = np.nan
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.cov(min_periods=11), psdf.cov(min_periods=11), almost=True)
        self.assert_eq(pdf.cov(min_periods=10), psdf.cov(min_periods=10), almost=True)

        # return empty DataFrame
        pdf = pd.DataFrame([("1", "2"), ("0", "3"), ("2", "0"), ("1", "1")], columns=["a", "b"])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.cov(), psdf.cov())

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
        # NOTE: Set `datetime_is_numeric=True` for pandas:
        # FutureWarning: Treating datetime data as categorical rather than numeric in
        # `.describe` is deprecated and will be removed in a future version of pandas.
        # Specify `datetime_is_numeric=True` to silence this
        # warning and adopt the future behavior now.
        # NOTE: Compare the result except percentiles, since we use approximate percentile
        # so the result is different from pandas.
        if LooseVersion(pd.__version__) >= LooseVersion("1.1.0"):
            self.assert_eq(
                psdf.describe().loc[["count", "mean", "min", "max"]],
                pdf.describe(datetime_is_numeric=True)
                .astype(str)
                .loc[["count", "mean", "min", "max"]],
            )
        else:
            self.assert_eq(
                psdf.describe(),
                ps.DataFrame(
                    {
                        "A": [
                            "4",
                            "2021-07-16 18:00:00",
                            "2020-10-20 00:00:00",
                            "2020-10-20 00:00:00",
                            "2021-06-02 00:00:00",
                            "2021-06-02 00:00:00",
                            "2022-07-11 00:00:00",
                        ],
                        "B": [
                            "4",
                            "2024-08-02 18:00:00",
                            "2021-11-20 00:00:00",
                            "2021-11-20 00:00:00",
                            "2023-06-02 00:00:00",
                            "2026-07-11 00:00:00",
                            "2026-07-11 00:00:00",
                        ],
                    },
                    index=["count", "mean", "min", "25%", "50%", "75%", "max"],
                ),
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
        if LooseVersion(pd.__version__) >= LooseVersion("1.1.0"):
            self.assert_eq(
                psdf.describe().loc[["count", "mean", "min", "max"]],
                pdf.describe(datetime_is_numeric=True)
                .astype(str)
                .loc[["count", "mean", "min", "max"]],
            )
            psdf.A += psdf.A
            pdf.A += pdf.A
            self.assert_eq(
                psdf.describe().loc[["count", "mean", "min", "max"]],
                pdf.describe(datetime_is_numeric=True)
                .astype(str)
                .loc[["count", "mean", "min", "max"]],
            )
        else:
            expected_result = ps.DataFrame(
                {
                    "B": [
                        "4",
                        "2024-08-02 18:00:00",
                        "2021-11-20 00:00:00",
                        "2021-11-20 00:00:00",
                        "2023-06-02 00:00:00",
                        "2026-07-11 00:00:00",
                        "2026-07-11 00:00:00",
                    ]
                },
                index=["count", "mean", "min", "25%", "50%", "75%", "max"],
            )
            self.assert_eq(
                psdf.describe(),
                expected_result,
            )
            psdf.A += psdf.A
            self.assert_eq(
                psdf.describe(),
                expected_result,
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
        if LooseVersion(pd.__version__) >= LooseVersion("1.1.0"):
            pandas_result = pdf.describe(datetime_is_numeric=True)
            pandas_result.B = pandas_result.B.astype(str)
            self.assert_eq(
                psdf.describe().loc[["count", "mean", "min", "max"]],
                pandas_result.loc[["count", "mean", "min", "max"]],
            )
            psdf.A += psdf.A
            pdf.A += pdf.A
            pandas_result = pdf.describe(datetime_is_numeric=True)
            pandas_result.B = pandas_result.B.astype(str)
            self.assert_eq(
                psdf.describe().loc[["count", "mean", "min", "max"]],
                pandas_result.loc[["count", "mean", "min", "max"]],
            )
        else:
            self.assert_eq(
                psdf.describe(),
                ps.DataFrame(
                    {
                        "A": [4, 2, 1, 1, 2, 2, 3, 0.816497],
                        "B": [
                            "4",
                            "2024-08-02 18:00:00",
                            "2021-11-20 00:00:00",
                            "2021-11-20 00:00:00",
                            "2023-06-02 00:00:00",
                            "2026-07-11 00:00:00",
                            "2026-07-11 00:00:00",
                            "None",
                        ],
                    },
                    index=["count", "mean", "min", "25%", "50%", "75%", "max", "std"],
                ),
            )
            psdf.A += psdf.A
            self.assert_eq(
                psdf.describe(),
                ps.DataFrame(
                    {
                        "A": [4, 4, 2, 2, 4, 4, 6, 1.632993],
                        "B": [
                            "4",
                            "2024-08-02 18:00:00",
                            "2021-11-20 00:00:00",
                            "2021-11-20 00:00:00",
                            "2023-06-02 00:00:00",
                            "2026-07-11 00:00:00",
                            "2026-07-11 00:00:00",
                            "None",
                        ],
                    },
                    index=["count", "mean", "min", "25%", "50%", "75%", "max", "std"],
                ),
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
        if LooseVersion(pd.__version__) >= LooseVersion("1.1.0"):
            pandas_result = pdf.describe(datetime_is_numeric=True)
            pandas_result.b = pandas_result.b.astype(str)
            self.assert_eq(
                psdf.describe().loc[["count", "mean", "min", "max"]],
                pandas_result.loc[["count", "mean", "min", "max"]],
            )
        else:
            self.assert_eq(
                psdf.describe(),
                ps.DataFrame(
                    {
                        "a": [3.0, 2.0, 1.0, 1.0, 2.0, 3.0, 3.0, 1.0],
                        "b": [
                            "3",
                            "1970-01-01 00:00:00.000001",
                            "1970-01-01 00:00:00.000001",
                            "1970-01-01 00:00:00.000001",
                            "1970-01-01 00:00:00.000001",
                            "1970-01-01 00:00:00.000001",
                            "1970-01-01 00:00:00.000001",
                            "None",
                        ],
                    },
                    index=["count", "mean", "min", "25%", "50%", "75%", "max", "std"],
                ),
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
        # For timestamp type, we should convert NaT to None in pandas result
        # since pandas API on Spark doesn't support the NaT for object type.
        if LooseVersion(pd.__version__) >= LooseVersion("1.1.0"):
            pdf_result = pdf[pdf.a != pdf.a].describe(datetime_is_numeric=True)
            self.assert_eq(
                psdf[psdf.a != psdf.a].describe(),
                pdf_result.where(pdf_result.notnull(), None).astype(str),
            )
        else:
            self.assert_eq(
                psdf[psdf.a != psdf.a].describe(),
                ps.DataFrame(
                    {
                        "a": [
                            "0",
                            "None",
                            "None",
                            "None",
                            "None",
                            "None",
                            "None",
                        ],
                        "b": [
                            "0",
                            "None",
                            "None",
                            "None",
                            "None",
                            "None",
                            "None",
                        ],
                    },
                    index=["count", "mean", "min", "25%", "50%", "75%", "max"],
                ),
            )

        # Explicit empty DataFrame numeric & timestamp
        psdf = ps.DataFrame(
            {"a": [1, 2, 3], "b": [pd.Timestamp(1), pd.Timestamp(1), pd.Timestamp(1)]}
        )
        pdf = psdf._to_pandas()
        if LooseVersion(pd.__version__) >= LooseVersion("1.1.0"):
            pdf_result = pdf[pdf.a != pdf.a].describe(datetime_is_numeric=True)
            pdf_result.b = pdf_result.b.where(pdf_result.b.notnull(), None).astype(str)
            self.assert_eq(
                psdf[psdf.a != psdf.a].describe(),
                pdf_result,
            )
        else:
            self.assert_eq(
                psdf[psdf.a != psdf.a].describe(),
                ps.DataFrame(
                    {
                        "a": [
                            0,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                        ],
                        "b": [
                            "0",
                            "None",
                            "None",
                            "None",
                            "None",
                            "None",
                            "None",
                            "None",
                        ],
                    },
                    index=["count", "mean", "min", "25%", "50%", "75%", "max", "std"],
                ),
            )

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
        if LooseVersion(pd.__version__) >= LooseVersion("1.1.0"):
            pdf_result = pdf[pdf.a != pdf.a].describe(datetime_is_numeric=True)
            self.assert_eq(
                psdf[psdf.a != psdf.a].describe(),
                pdf_result.where(pdf_result.notnull(), None).astype(str),
            )
        else:
            self.assert_eq(
                psdf[psdf.a != psdf.a].describe(),
                ps.DataFrame(
                    {
                        "b": [
                            "0",
                            "None",
                            "None",
                            "None",
                            "None",
                            "None",
                            "None",
                        ],
                    },
                    index=["count", "mean", "min", "25%", "50%", "75%", "max"],
                ),
            )

    def test_mad(self):
        pdf = pd.DataFrame(
            {
                "A": [1, 2, None, 4, np.nan],
                "B": [-0.1, 0.2, -0.3, np.nan, 0.5],
                "C": ["a", "b", "c", "d", "e"],
            }
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.mad(), pdf.mad())
        self.assert_eq(psdf.mad(axis=1), pdf.mad(axis=1))

        with self.assertRaises(ValueError):
            psdf.mad(axis=2)

        # MultiIndex columns
        columns = pd.MultiIndex.from_tuples([("A", "X"), ("A", "Y"), ("A", "Z")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(psdf.mad(), pdf.mad())
        self.assert_eq(psdf.mad(axis=1), pdf.mad(axis=1))

        pdf = pd.DataFrame({"A": [True, True, False, False], "B": [True, False, False, True]})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.mad(), pdf.mad())
        self.assert_eq(psdf.mad(axis=1), pdf.mad(axis=1))

    def test_mode(self):
        pdf = pd.DataFrame(
            {
                "A": [1, 2, None, 4, 5, 4, 2],
                "B": [-0.1, 0.2, -0.3, np.nan, 0.5, -0.1, -0.1],
                "C": ["d", "b", "c", "c", "e", "a", "a"],
                "D": [np.nan, np.nan, np.nan, np.nan, 0.1, -0.1, -0.1],
                "E": [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
            }
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.mode(), pdf.mode())
        self.assert_eq(psdf.mode(numeric_only=True), pdf.mode(numeric_only=True))
        self.assert_eq(psdf.mode(dropna=False), pdf.mode(dropna=False))

        # dataframe with single column
        for c in ["A", "B", "C", "D", "E"]:
            self.assert_eq(psdf[[c]].mode(), pdf[[c]].mode())

        with self.assertRaises(ValueError):
            psdf.mode(axis=2)

        def f(index, iterator):
            return ["3", "3", "3", "3", "4"] if index == 3 else ["0", "1", "2", "3", "4"]

        rdd = self.spark.sparkContext.parallelize(
            [
                1,
            ],
            4,
        ).mapPartitionsWithIndex(f)
        df = self.spark.createDataFrame(rdd, schema="string")
        psdf = df.pandas_api()
        self.assert_eq(psdf.mode(), psdf._to_pandas().mode())

    def _test_cummin(self, pdf, psdf):
        self.assert_eq(pdf.cummin(), psdf.cummin())
        self.assert_eq(pdf.cummin(skipna=False), psdf.cummin(skipna=False))
        self.assert_eq(pdf.cummin().sum(), psdf.cummin().sum())

    def test_cummin(self):
        pdf = pd.DataFrame(
            [[2.0, 1.0], [5, None], [1.0, 0.0], [2.0, 4.0], [4.0, 9.0]],
            columns=list("AB"),
            index=np.random.rand(5),
        )
        psdf = ps.from_pandas(pdf)
        self._test_cummin(pdf, psdf)

    def test_cummin_multiindex_columns(self):
        arrays = [np.array(["A", "A", "B", "B"]), np.array(["one", "two", "one", "two"])]
        pdf = pd.DataFrame(np.random.randn(3, 4), index=["A", "C", "B"], columns=arrays)
        pdf.at["C", ("A", "two")] = None
        psdf = ps.from_pandas(pdf)
        self._test_cummin(pdf, psdf)

    def _test_cummax(self, pdf, psdf):
        self.assert_eq(pdf.cummax(), psdf.cummax())
        self.assert_eq(pdf.cummax(skipna=False), psdf.cummax(skipna=False))
        self.assert_eq(pdf.cummax().sum(), psdf.cummax().sum())

    def test_cummax(self):
        pdf = pd.DataFrame(
            [[2.0, 1.0], [5, None], [1.0, 0.0], [2.0, 4.0], [4.0, 9.0]],
            columns=list("AB"),
            index=np.random.rand(5),
        )
        psdf = ps.from_pandas(pdf)
        self._test_cummax(pdf, psdf)

    def test_cummax_multiindex_columns(self):
        arrays = [np.array(["A", "A", "B", "B"]), np.array(["one", "two", "one", "two"])]
        pdf = pd.DataFrame(np.random.randn(3, 4), index=["A", "C", "B"], columns=arrays)
        pdf.at["C", ("A", "two")] = None
        psdf = ps.from_pandas(pdf)
        self._test_cummax(pdf, psdf)

    def _test_cumsum(self, pdf, psdf):
        self.assert_eq(pdf.cumsum(), psdf.cumsum())
        self.assert_eq(pdf.cumsum(skipna=False), psdf.cumsum(skipna=False))
        self.assert_eq(pdf.cumsum().sum(), psdf.cumsum().sum())

    def test_cumsum(self):
        pdf = pd.DataFrame(
            [[2.0, 1.0], [5, None], [1.0, 0.0], [2.0, 4.0], [4.0, 9.0]],
            columns=list("AB"),
            index=np.random.rand(5),
        )
        psdf = ps.from_pandas(pdf)
        self._test_cumsum(pdf, psdf)

    def test_cumsum_multiindex_columns(self):
        arrays = [np.array(["A", "A", "B", "B"]), np.array(["one", "two", "one", "two"])]
        pdf = pd.DataFrame(np.random.randn(3, 4), index=["A", "C", "B"], columns=arrays)
        pdf.at["C", ("A", "two")] = None
        psdf = ps.from_pandas(pdf)
        self._test_cumsum(pdf, psdf)

    def _test_cumprod(self, pdf, psdf):
        self.assert_eq(pdf.cumprod(), psdf.cumprod(), almost=True)
        self.assert_eq(pdf.cumprod(skipna=False), psdf.cumprod(skipna=False), almost=True)
        self.assert_eq(pdf.cumprod().sum(), psdf.cumprod().sum(), almost=True)

    def test_cumprod(self):
        pdf = pd.DataFrame(
            [[2.0, 1.0, 1], [5, None, 2], [1.0, -1.0, -3], [2.0, 0, 4], [4.0, 9.0, 5]],
            columns=list("ABC"),
            index=np.random.rand(5),
        )
        psdf = ps.from_pandas(pdf)
        self._test_cumprod(pdf, psdf)

    def test_cumprod_multiindex_columns(self):
        arrays = [np.array(["A", "A", "B", "B"]), np.array(["one", "two", "one", "two"])]
        pdf = pd.DataFrame(np.random.rand(3, 4), index=["A", "C", "B"], columns=arrays)
        pdf.at["C", ("A", "two")] = None
        psdf = ps.from_pandas(pdf)
        self._test_cumprod(pdf, psdf)

    def test_round(self):
        pdf = pd.DataFrame(
            {
                "A": [0.028208, 0.038683, 0.877076],
                "B": [0.992815, 0.645646, 0.149370],
                "C": [0.173891, 0.577595, 0.491027],
            },
            columns=["A", "B", "C"],
            index=np.random.rand(3),
        )
        psdf = ps.from_pandas(pdf)

        pser = pd.Series([1, 0, 2], index=["A", "B", "C"])
        psser = ps.Series([1, 0, 2], index=["A", "B", "C"])
        self.assert_eq(pdf.round(2), psdf.round(2))
        self.assert_eq(pdf.round({"A": 1, "C": 2}), psdf.round({"A": 1, "C": 2}))
        self.assert_eq(pdf.round({"A": 1, "D": 2}), psdf.round({"A": 1, "D": 2}))
        self.assert_eq(pdf.round(pser), psdf.round(psser))
        msg = "decimals must be an integer, a dict-like or a Series"
        with self.assertRaisesRegex(TypeError, msg):
            psdf.round(1.5)

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("X", "A"), ("X", "B"), ("Y", "C")])
        pdf.columns = columns
        psdf.columns = columns
        pser = pd.Series([1, 0, 2], index=columns)
        psser = ps.Series([1, 0, 2], index=columns)
        self.assert_eq(pdf.round(2), psdf.round(2))
        self.assert_eq(
            pdf.round({("X", "A"): 1, ("Y", "C"): 2}), psdf.round({("X", "A"): 1, ("Y", "C"): 2})
        )
        self.assert_eq(pdf.round({("X", "A"): 1, "Y": 2}), psdf.round({("X", "A"): 1, "Y": 2}))
        self.assert_eq(pdf.round(pser), psdf.round(psser))

        # non-string names
        pdf = pd.DataFrame(
            {
                10: [0.028208, 0.038683, 0.877076],
                20: [0.992815, 0.645646, 0.149370],
                30: [0.173891, 0.577595, 0.491027],
            },
            index=np.random.rand(3),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(pdf.round({10: 1, 30: 2}), psdf.round({10: 1, 30: 2}))

    def test_diff(self):
        pdf = pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6], "b": [1, 1, 2, 3, 5, 8], "c": [1, 4, 9, 16, 25, 36]},
            index=np.random.rand(6),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(pdf.diff(), psdf.diff())
        self.assert_eq(pdf.diff().diff(-1), psdf.diff().diff(-1))
        self.assert_eq(pdf.diff().sum().astype(int), psdf.diff().sum())

        msg = "should be an int"
        with self.assertRaisesRegex(TypeError, msg):
            psdf.diff(1.5)
        msg = 'axis should be either 0 or "index" currently.'
        with self.assertRaisesRegex(NotImplementedError, msg):
            psdf.diff(axis=1)

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "Col1"), ("x", "Col2"), ("y", "Col3")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(pdf.diff(), psdf.diff())

    def test_eval(self):
        pdf = pd.DataFrame({"A": range(1, 6), "B": range(10, 0, -2)})
        psdf = ps.from_pandas(pdf)

        # operation between columns (returns Series)
        self.assert_eq(pdf.eval("A + B"), psdf.eval("A + B"))
        self.assert_eq(pdf.eval("A + A"), psdf.eval("A + A"))
        # assignment (returns DataFrame)
        self.assert_eq(pdf.eval("C = A + B"), psdf.eval("C = A + B"))
        self.assert_eq(pdf.eval("A = A + A"), psdf.eval("A = A + A"))
        # operation between scalars (returns scalar)
        self.assert_eq(pdf.eval("1 + 1"), psdf.eval("1 + 1"))
        # complicated operations with assignment
        self.assert_eq(
            pdf.eval("B = A + B // (100 + 200) * (500 - B) - 10.5"),
            psdf.eval("B = A + B // (100 + 200) * (500 - B) - 10.5"),
        )

        # inplace=True (only support for assignment)
        pdf.eval("C = A + B", inplace=True)
        psdf.eval("C = A + B", inplace=True)
        self.assert_eq(pdf, psdf)
        pser = pdf.A
        psser = psdf.A
        pdf.eval("A = B + C", inplace=True)
        psdf.eval("A = B + C", inplace=True)
        self.assert_eq(pdf, psdf)
        # Skip due to pandas bug: https://github.com/pandas-dev/pandas/issues/47449
        if not (LooseVersion("1.4.0") <= LooseVersion(pd.__version__) <= LooseVersion("1.4.3")):
            self.assert_eq(pser, psser)

        # doesn't support for multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("y", "b"), ("z", "c")])
        psdf.columns = columns
        self.assertRaises(TypeError, lambda: psdf.eval("x.a + y.b"))

    def test_pct_change(self):
        pdf = pd.DataFrame(
            {"a": [1, 2, 3, 2], "b": [4.0, 2.0, 3.0, 1.0], "c": [300, 200, 400, 200]},
            index=np.random.rand(4),
        )
        pdf.columns = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.pct_change(2), pdf.pct_change(2), check_exact=False)
        self.assert_eq(psdf.pct_change().sum(), pdf.pct_change().sum(), check_exact=False)

    def test_rank(self):
        pdf = pd.DataFrame(
            data={"col1": [1, 2, 3, 1], "col2": [3, 4, 3, 1]},
            columns=["col1", "col2"],
            index=np.random.rand(4),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(pdf.rank().sort_index(), psdf.rank().sort_index())
        self.assert_eq(pdf.rank().sum(), psdf.rank().sum())
        self.assert_eq(
            pdf.rank(ascending=False).sort_index(), psdf.rank(ascending=False).sort_index()
        )
        self.assert_eq(pdf.rank(method="min").sort_index(), psdf.rank(method="min").sort_index())
        self.assert_eq(pdf.rank(method="max").sort_index(), psdf.rank(method="max").sort_index())
        self.assert_eq(
            pdf.rank(method="first").sort_index(), psdf.rank(method="first").sort_index()
        )
        self.assert_eq(
            pdf.rank(method="dense").sort_index(), psdf.rank(method="dense").sort_index()
        )

        msg = "method must be one of 'average', 'min', 'max', 'first', 'dense'"
        with self.assertRaisesRegex(ValueError, msg):
            psdf.rank(method="nothing")

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "col1"), ("y", "col2")])
        pdf.columns = columns
        psdf.columns = columns
        self.assert_eq(pdf.rank().sort_index(), psdf.rank().sort_index())

        # non-numeric columns
        pdf = pd.DataFrame(
            data={"col1": [1, 2, 3, 1], "col2": ["a", "b", "c", "d"]},
            index=np.random.rand(4),
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            pdf.rank(numeric_only=True).sort_index(), psdf.rank(numeric_only=True).sort_index()
        )
        self.assert_eq(
            pdf.rank(numeric_only=False).sort_index(), psdf.rank(numeric_only=False).sort_index()
        )
        self.assert_eq(
            pdf.rank(numeric_only=None).sort_index(), psdf.rank(numeric_only=None).sort_index()
        )
        self.assert_eq(
            pdf[["col2"]].rank(numeric_only=True),
            psdf[["col2"]].rank(numeric_only=True),
        )

    def test_nunique(self):
        pdf = pd.DataFrame({"A": [1, 2, 3], "B": [np.nan, 3, np.nan]}, index=np.random.rand(3))
        psdf = ps.from_pandas(pdf)

        # Assert NaNs are dropped by default
        self.assert_eq(psdf.nunique(), pdf.nunique())

        # Assert including NaN values
        self.assert_eq(psdf.nunique(dropna=False), pdf.nunique(dropna=False))

        # Assert approximate counts
        self.assert_eq(
            ps.DataFrame({"A": range(100)}).nunique(approx=True),
            pd.Series([103], index=["A"]),
        )
        self.assert_eq(
            ps.DataFrame({"A": range(100)}).nunique(approx=True, rsd=0.01),
            pd.Series([100], index=["A"]),
        )

        # Assert unsupported axis value yet
        msg = 'axis should be either 0 or "index" currently.'
        with self.assertRaisesRegex(NotImplementedError, msg):
            psdf.nunique(axis=1)

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("X", "A"), ("Y", "B")], names=["1", "2"])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(psdf.nunique(), pdf.nunique())
        self.assert_eq(psdf.nunique(dropna=False), pdf.nunique(dropna=False))

    def test_quantile(self):
        pdf, psdf = self.df_pair

        self.assert_eq(psdf.quantile(0.5), pdf.quantile(0.5))
        self.assert_eq(psdf.quantile([0.25, 0.5, 0.75]), pdf.quantile([0.25, 0.5, 0.75]))

        self.assert_eq(psdf.loc[[]].quantile(0.5), pdf.loc[[]].quantile(0.5))
        self.assert_eq(
            psdf.loc[[]].quantile([0.25, 0.5, 0.75]), pdf.loc[[]].quantile([0.25, 0.5, 0.75])
        )

        with self.assertRaisesRegex(
            NotImplementedError, 'axis should be either 0 or "index" currently.'
        ):
            psdf.quantile(0.5, axis=1)
        with self.assertRaisesRegex(TypeError, "accuracy must be an integer; however"):
            psdf.quantile(accuracy="a")
        with self.assertRaisesRegex(TypeError, "q must be a float or an array of floats;"):
            psdf.quantile(q="a")
        with self.assertRaisesRegex(TypeError, "q must be a float or an array of floats;"):
            psdf.quantile(q=["a"])
        with self.assertRaisesRegex(
            ValueError, r"percentiles should all be in the interval \[0, 1\]"
        ):
            psdf.quantile(q=[1.1])

        self.assert_eq(
            psdf.quantile(0.5, numeric_only=False), pdf.quantile(0.5, numeric_only=False)
        )
        self.assert_eq(
            psdf.quantile([0.25, 0.5, 0.75], numeric_only=False),
            pdf.quantile([0.25, 0.5, 0.75], numeric_only=False),
        )

        # multi-index column
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("y", "b")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(psdf.quantile(0.5), pdf.quantile(0.5))
        self.assert_eq(psdf.quantile([0.25, 0.5, 0.75]), pdf.quantile([0.25, 0.5, 0.75]))

        pdf = pd.DataFrame({"x": ["a", "b", "c"]})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.quantile(0.5), pdf.quantile(0.5))
        self.assert_eq(psdf.quantile([0.25, 0.5, 0.75]), pdf.quantile([0.25, 0.5, 0.75]))

        with self.assertRaisesRegex(TypeError, "Could not convert object \\(string\\) to numeric"):
            psdf.quantile(0.5, numeric_only=False)
        with self.assertRaisesRegex(TypeError, "Could not convert object \\(string\\) to numeric"):
            psdf.quantile([0.25, 0.5, 0.75], numeric_only=False)

    def test_product(self):
        pdf = pd.DataFrame(
            {"A": [1, 2, 3, 4, 5], "B": [10, 20, 30, 40, 50], "C": ["a", "b", "c", "d", "e"]}
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.prod(), psdf.prod().sort_index())

        # Named columns
        pdf.columns.name = "Koalas"
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.prod(), psdf.prod().sort_index())

        # MultiIndex columns
        pdf.columns = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.prod(), psdf.prod().sort_index())

        # Named MultiIndex columns
        pdf.columns.names = ["Hello", "Koalas"]
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.prod(), psdf.prod().sort_index())

        # No numeric columns
        pdf = pd.DataFrame({"key": ["a", "b", "c"], "val": ["x", "y", "z"]})
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.prod(), psdf.prod().sort_index())

        # No numeric named columns
        pdf.columns.name = "Koalas"
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.prod(), psdf.prod().sort_index(), almost=True)

        # No numeric MultiIndex columns
        pdf.columns = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y")])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.prod(), psdf.prod().sort_index(), almost=True)

        # No numeric named MultiIndex columns
        pdf.columns.names = ["Hello", "Koalas"]
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.prod(), psdf.prod().sort_index(), almost=True)

        # All NaN columns
        pdf = pd.DataFrame(
            {
                "A": [np.nan, np.nan, np.nan, np.nan, np.nan],
                "B": [10, 20, 30, 40, 50],
                "C": ["a", "b", "c", "d", "e"],
            }
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.prod(), psdf.prod().sort_index(), check_exact=False)

        # All NaN named columns
        pdf.columns.name = "Koalas"
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.prod(), psdf.prod().sort_index(), check_exact=False)

        # All NaN MultiIndex columns
        pdf.columns = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.prod(), psdf.prod().sort_index(), check_exact=False)

        # All NaN named MultiIndex columns
        pdf.columns.names = ["Hello", "Koalas"]
        psdf = ps.from_pandas(pdf)
        self.assert_eq(pdf.prod(), psdf.prod().sort_index(), check_exact=False)


class FrameComputeTests(FrameComputeMixin, ComparisonTestBase, SQLTestUtils):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.frame.test_frame_compute import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
