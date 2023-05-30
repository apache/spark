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
from distutils.version import LooseVersion
import unittest

import numpy as np
import pandas as pd

from pyspark import pandas as ps
from pyspark.testing.pandasutils import ComparisonTestBase
from pyspark.testing.sqlutils import SQLTestUtils


class FrameDescribeMixin:
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

    @unittest.skipIf(
        LooseVersion(pd.__version__) >= LooseVersion("2.0.0"),
        "TODO(SPARK-43556): Enable DataFrameSlowTests.test_describe for pandas 2.0.0.",
    )
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

    @unittest.skipIf(
        LooseVersion(pd.__version__) >= LooseVersion("2.0.0"),
        "TODO(SPARK-43556): Enable DataFrameSlowTests.test_describe for pandas 2.0.0.",
    )
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


class FrameDescribeTests(FrameDescribeMixin, ComparisonTestBase, SQLTestUtils):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.computation.test_describe import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
