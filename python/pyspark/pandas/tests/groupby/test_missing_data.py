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


class GroupbyMissingDataMixin:
    @property
    def pdf(self):
        return pd.DataFrame(
            {
                "A": [1, 2, 1, 2],
                "B": [3.1, 4.1, 4.1, 3.1],
                "C": ["a", "b", "b", "a"],
                "D": [True, False, False, True],
            }
        )

    @property
    def psdf(self):
        return ps.from_pandas(self.pdf)

    def test_fillna(self):
        pdf = pd.DataFrame(
            {
                "A": [1, 1, 2, 2] * 3,
                "B": [2, 4, None, 3] * 3,
                "C": [None, None, None, 1] * 3,
                "D": [0, 1, 5, 4] * 3,
            }
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby("A").fillna(0).sort_index(), pdf.groupby("A").fillna(0).sort_index()
        )
        self.assert_eq(
            psdf.groupby("A")["C"].fillna(0).sort_index(),
            pdf.groupby("A")["C"].fillna(0).sort_index(),
        )
        self.assert_eq(
            psdf.groupby("A")[["C"]].fillna(0).sort_index(),
            pdf.groupby("A")[["C"]].fillna(0).sort_index(),
        )
        self.assert_eq(
            psdf.groupby("A").fillna(method="bfill").sort_index(),
            pdf.groupby("A").fillna(method="bfill").sort_index(),
        )
        self.assert_eq(
            psdf.groupby("A")["C"].fillna(method="bfill").sort_index(),
            pdf.groupby("A")["C"].fillna(method="bfill").sort_index(),
        )
        self.assert_eq(
            psdf.groupby("A")[["C"]].fillna(method="bfill").sort_index(),
            pdf.groupby("A")[["C"]].fillna(method="bfill").sort_index(),
        )
        self.assert_eq(
            psdf.groupby("A").fillna(method="ffill").sort_index(),
            pdf.groupby("A").fillna(method="ffill").sort_index(),
        )
        self.assert_eq(
            psdf.groupby("A")["C"].fillna(method="ffill").sort_index(),
            pdf.groupby("A")["C"].fillna(method="ffill").sort_index(),
        )
        self.assert_eq(
            psdf.groupby("A")[["C"]].fillna(method="ffill").sort_index(),
            pdf.groupby("A")[["C"]].fillna(method="ffill").sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.A // 5).fillna(method="bfill").sort_index(),
            pdf.groupby(pdf.A // 5).fillna(method="bfill").sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.A // 5)["C"].fillna(method="bfill").sort_index(),
            pdf.groupby(pdf.A // 5)["C"].fillna(method="bfill").sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.A // 5)[["C"]].fillna(method="bfill").sort_index(),
            pdf.groupby(pdf.A // 5)[["C"]].fillna(method="bfill").sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.A // 5).fillna(method="ffill").sort_index(),
            pdf.groupby(pdf.A // 5).fillna(method="ffill").sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.A // 5)["C"].fillna(method="ffill").sort_index(),
            pdf.groupby(pdf.A // 5)["C"].fillna(method="ffill").sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.A // 5)[["C"]].fillna(method="ffill").sort_index(),
            pdf.groupby(pdf.A // 5)[["C"]].fillna(method="ffill").sort_index(),
        )
        self.assert_eq(
            psdf.C.rename().groupby(psdf.A).fillna(0).sort_index(),
            pdf.C.rename().groupby(pdf.A).fillna(0).sort_index(),
        )
        self.assert_eq(
            psdf.C.groupby(psdf.A.rename()).fillna(0).sort_index(),
            pdf.C.groupby(pdf.A.rename()).fillna(0).sort_index(),
        )
        self.assert_eq(
            psdf.C.rename().groupby(psdf.A.rename()).fillna(0).sort_index(),
            pdf.C.rename().groupby(pdf.A.rename()).fillna(0).sort_index(),
        )

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("X", "A"), ("X", "B"), ("Y", "C"), ("Z", "D")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.groupby(("X", "A")).fillna(0).sort_index(),
            pdf.groupby(("X", "A")).fillna(0).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(("X", "A")).fillna(method="bfill").sort_index(),
            pdf.groupby(("X", "A")).fillna(method="bfill").sort_index(),
        )
        self.assert_eq(
            psdf.groupby(("X", "A")).fillna(method="ffill").sort_index(),
            pdf.groupby(("X", "A")).fillna(method="ffill").sort_index(),
        )

    def test_ffill(self):
        idx = np.random.rand(4 * 3)
        pdf = pd.DataFrame(
            {
                "A": [1, 1, 2, 2] * 3,
                "B": [2, 4, None, 3] * 3,
                "C": [None, None, None, 1] * 3,
                "D": [0, 1, 5, 4] * 3,
            },
            index=idx,
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby("A").ffill().sort_index(), pdf.groupby("A").ffill().sort_index()
        )
        self.assert_eq(
            psdf.groupby("A")[["B"]].ffill().sort_index(),
            pdf.groupby("A")[["B"]].ffill().sort_index(),
        )
        self.assert_eq(
            psdf.groupby("A")["B"].ffill().sort_index(), pdf.groupby("A")["B"].ffill().sort_index()
        )
        self.assert_eq(
            psdf.groupby("A")["B"].ffill()[idx[6]], pdf.groupby("A")["B"].ffill()[idx[6]]
        )

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("X", "A"), ("X", "B"), ("Y", "C"), ("Z", "D")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.groupby(("X", "A")).ffill().sort_index(),
            pdf.groupby(("X", "A")).ffill().sort_index(),
        )

    def test_bfill(self):
        idx = np.random.rand(4 * 3)
        pdf = pd.DataFrame(
            {
                "A": [1, 1, 2, 2] * 3,
                "B": [2, 4, None, 3] * 3,
                "C": [None, None, None, 1] * 3,
                "D": [0, 1, 5, 4] * 3,
            },
            index=idx,
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby("A").bfill().sort_index(), pdf.groupby("A").bfill().sort_index()
        )
        self.assert_eq(
            psdf.groupby("A")[["B"]].bfill().sort_index(),
            pdf.groupby("A")[["B"]].bfill().sort_index(),
        )
        self.assert_eq(
            psdf.groupby("A")["B"].bfill().sort_index(),
            pdf.groupby("A")["B"].bfill().sort_index(),
        )
        self.assert_eq(
            psdf.groupby("A")["B"].bfill()[idx[6]], pdf.groupby("A")["B"].bfill()[idx[6]]
        )

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("X", "A"), ("X", "B"), ("Y", "C"), ("Z", "D")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.groupby(("X", "A")).bfill().sort_index(),
            pdf.groupby(("X", "A")).bfill().sort_index(),
        )

    def test_dropna(self):
        pdf = pd.DataFrame(
            {"A": [None, 1, None, 1, 2], "B": [1, 2, 3, None, None], "C": [4, 5, 6, 7, None]}
        )
        psdf = ps.from_pandas(pdf)

        # pd.DataFrame.groupby with dropna parameter is implemented since pandas 1.1.0
        if LooseVersion(pd.__version__) >= LooseVersion("1.1.0"):
            for dropna in [True, False]:
                for as_index in [True, False]:
                    if as_index:

                        def sort(df):
                            return df.sort_index()

                    else:

                        def sort(df):
                            return df.sort_values("A").reset_index(drop=True)

                    self.assert_eq(
                        sort(psdf.groupby("A", as_index=as_index, dropna=dropna).std()),
                        sort(pdf.groupby("A", as_index=as_index, dropna=dropna).std()),
                    )

                    self.assert_eq(
                        sort(psdf.groupby("A", as_index=as_index, dropna=dropna).B.std()),
                        sort(pdf.groupby("A", as_index=as_index, dropna=dropna).B.std()),
                    )
                    self.assert_eq(
                        sort(psdf.groupby("A", as_index=as_index, dropna=dropna)["B"].std()),
                        sort(pdf.groupby("A", as_index=as_index, dropna=dropna)["B"].std()),
                    )

                    self.assert_eq(
                        sort(
                            psdf.groupby("A", as_index=as_index, dropna=dropna).agg(
                                {"B": "min", "C": "std"}
                            )
                        ),
                        sort(
                            pdf.groupby("A", as_index=as_index, dropna=dropna).agg(
                                {"B": "min", "C": "std"}
                            )
                        ),
                    )

            for dropna in [True, False]:
                for as_index in [True, False]:
                    if as_index:

                        def sort(df):
                            return df.sort_index()

                    else:

                        def sort(df):
                            return df.sort_values(["A", "B"]).reset_index(drop=True)

                    self.assert_eq(
                        sort(
                            psdf.groupby(["A", "B"], as_index=as_index, dropna=dropna).agg(
                                {"C": ["min", "std"]}
                            )
                        ),
                        sort(
                            pdf.groupby(["A", "B"], as_index=as_index, dropna=dropna).agg(
                                {"C": ["min", "std"]}
                            )
                        ),
                        almost=True,
                    )

            # multi-index columns
            columns = pd.MultiIndex.from_tuples([("X", "A"), ("X", "B"), ("Y", "C")])
            pdf.columns = columns
            psdf.columns = columns

            for dropna in [True, False]:
                for as_index in [True, False]:
                    if as_index:

                        def sort(df):
                            return df.sort_index()

                    else:

                        def sort(df):
                            return df.sort_values(("X", "A")).reset_index(drop=True)

                    sorted_stats_psdf = sort(
                        psdf.groupby(("X", "A"), as_index=as_index, dropna=dropna).agg(
                            {("X", "B"): "min", ("Y", "C"): "std"}
                        )
                    )
                    sorted_stats_pdf = sort(
                        pdf.groupby(("X", "A"), as_index=as_index, dropna=dropna).agg(
                            {("X", "B"): "min", ("Y", "C"): "std"}
                        )
                    )
                    self.assert_eq(sorted_stats_psdf, sorted_stats_pdf)
        else:
            # Testing dropna=True (pandas default behavior)
            for as_index in [True, False]:
                if as_index:

                    def sort(df):
                        return df.sort_index()

                else:

                    def sort(df):
                        return df.sort_values("A").reset_index(drop=True)

                self.assert_eq(
                    sort(psdf.groupby("A", as_index=as_index, dropna=True)["B"].min()),
                    sort(pdf.groupby("A", as_index=as_index)["B"].min()),
                )

                if as_index:

                    def sort(df):
                        return df.sort_index()

                else:

                    def sort(df):
                        return df.sort_values(["A", "B"]).reset_index(drop=True)

                self.assert_eq(
                    sort(
                        psdf.groupby(["A", "B"], as_index=as_index, dropna=True).agg(
                            {"C": ["min", "std"]}
                        )
                    ),
                    sort(pdf.groupby(["A", "B"], as_index=as_index).agg({"C": ["min", "std"]})),
                    almost=True,
                )

            # Testing dropna=False
            index = pd.Index([1.0, 2.0, np.nan], name="A")
            expected = pd.Series([2.0, np.nan, 1.0], index=index, name="B")
            result = psdf.groupby("A", as_index=True, dropna=False)["B"].min().sort_index()
            self.assert_eq(expected, result)

            expected = pd.DataFrame({"A": [1.0, 2.0, np.nan], "B": [2.0, np.nan, 1.0]})
            result = (
                psdf.groupby("A", as_index=False, dropna=False)["B"]
                .min()
                .sort_values("A")
                .reset_index(drop=True)
            )
            self.assert_eq(expected, result)

            index = pd.MultiIndex.from_tuples(
                [(1.0, 2.0), (1.0, None), (2.0, None), (None, 1.0), (None, 3.0)], names=["A", "B"]
            )
            expected = pd.DataFrame(
                {
                    ("C", "min"): [5.0, 7.0, np.nan, 4.0, 6.0],
                    ("C", "std"): [np.nan, np.nan, np.nan, np.nan, np.nan],
                },
                index=index,
            )
            result = (
                psdf.groupby(["A", "B"], as_index=True, dropna=False)
                .agg({"C": ["min", "std"]})
                .sort_index()
            )
            self.assert_eq(expected, result)

            expected = pd.DataFrame(
                {
                    ("A", ""): [1.0, 1.0, 2.0, np.nan, np.nan],
                    ("B", ""): [2.0, np.nan, np.nan, 1.0, 3.0],
                    ("C", "min"): [5.0, 7.0, np.nan, 4.0, 6.0],
                    ("C", "std"): [np.nan, np.nan, np.nan, np.nan, np.nan],
                }
            )
            result = (
                psdf.groupby(["A", "B"], as_index=False, dropna=False)
                .agg({"C": ["min", "std"]})
                .sort_values(["A", "B"])
                .reset_index(drop=True)
            )
            self.assert_eq(expected, result)


class GroupbyMissingDataTests(GroupbyMissingDataMixin, ComparisonTestBase, SQLTestUtils):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.groupby.test_missing_data import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
