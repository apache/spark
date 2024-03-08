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

import pandas as pd

from pyspark import pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


class GroupbyAggregateMixin:
    def test_aggregate(self):
        pdf = pd.DataFrame(
            {"A": [1, 1, 2, 2], "B": [1, 2, 3, 4], "C": [0.362, 0.227, 1.267, -0.562]}
        )
        psdf = ps.from_pandas(pdf)

        for as_index in [True, False]:
            if as_index:

                def sort(df):
                    return df.sort_index()

            else:

                def sort(df):
                    return df.sort_values(list(df.columns)).reset_index(drop=True)

            for kkey, pkey in [("A", "A"), (psdf.A, pdf.A)]:
                with self.subTest(as_index=as_index, key=pkey):
                    self.assert_eq(
                        sort(psdf.groupby(kkey, as_index=as_index).agg("sum")),
                        sort(pdf.groupby(pkey, as_index=as_index).agg("sum")),
                    )
                    self.assert_eq(
                        sort(psdf.groupby(kkey, as_index=as_index).agg({"B": "min", "C": "sum"})),
                        sort(pdf.groupby(pkey, as_index=as_index).agg({"B": "min", "C": "sum"})),
                    )
                    self.assert_eq(
                        sort(
                            psdf.groupby(kkey, as_index=as_index).agg(
                                {"B": ["min", "max"], "C": "sum"}
                            )
                        ),
                        sort(
                            pdf.groupby(pkey, as_index=as_index).agg(
                                {"B": ["min", "max"], "C": "sum"}
                            )
                        ),
                    )

                    if as_index:
                        self.assert_eq(
                            sort(psdf.groupby(kkey, as_index=as_index).agg(["sum"])),
                            sort(pdf.groupby(pkey, as_index=as_index).agg(["sum"])),
                        )
                    else:
                        # seems like a pandas' bug for as_index=False and func_or_funcs is list?
                        self.assert_eq(
                            sort(psdf.groupby(kkey, as_index=as_index).agg(["sum"])),
                            sort(pdf.groupby(pkey, as_index=True).agg(["sum"]).reset_index()),
                        )

            for kkey, pkey in [(psdf.A + 1, pdf.A + 1), (psdf.copy().A, pdf.copy().A)]:
                with self.subTest(as_index=as_index, key=pkey):
                    self.assert_eq(
                        sort(psdf.groupby(kkey, as_index=as_index).agg("sum")),
                        sort(pdf.groupby(pkey, as_index=as_index).agg("sum")),
                    )
                    self.assert_eq(
                        sort(psdf.groupby(kkey, as_index=as_index).agg({"B": "min", "C": "sum"})),
                        sort(pdf.groupby(pkey, as_index=as_index).agg({"B": "min", "C": "sum"})),
                    )
                    self.assert_eq(
                        sort(
                            psdf.groupby(kkey, as_index=as_index).agg(
                                {"B": ["min", "max"], "C": "sum"}
                            )
                        ),
                        sort(
                            pdf.groupby(pkey, as_index=as_index).agg(
                                {"B": ["min", "max"], "C": "sum"}
                            )
                        ),
                    )
                    self.assert_eq(
                        sort(psdf.groupby(kkey, as_index=as_index).agg(["sum"])),
                        sort(pdf.groupby(pkey, as_index=as_index).agg(["sum"])),
                    )

        expected_error_message = (
            r"aggs must be a dict mapping from column name to aggregate functions "
            r"\(string or list of strings\)."
        )
        with self.assertRaisesRegex(ValueError, expected_error_message):
            psdf.groupby("A", as_index=as_index).agg(0)

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([(10, "A"), (10, "B"), (20, "C")])
        pdf.columns = columns
        psdf.columns = columns

        for as_index in [True, False]:
            stats_psdf = psdf.groupby((10, "A"), as_index=as_index).agg(
                {(10, "B"): "min", (20, "C"): "sum"}
            )
            stats_pdf = pdf.groupby((10, "A"), as_index=as_index).agg(
                {(10, "B"): "min", (20, "C"): "sum"}
            )
            self.assert_eq(
                stats_psdf.sort_values(by=[(10, "B"), (20, "C")]).reset_index(drop=True),
                stats_pdf.sort_values(by=[(10, "B"), (20, "C")]).reset_index(drop=True),
            )

        stats_psdf = psdf.groupby((10, "A")).agg({(10, "B"): ["min", "max"], (20, "C"): "sum"})
        stats_pdf = pdf.groupby((10, "A")).agg({(10, "B"): ["min", "max"], (20, "C"): "sum"})
        self.assert_eq(
            stats_psdf.sort_values(
                by=[(10, "B", "min"), (10, "B", "max"), (20, "C", "sum")]
            ).reset_index(drop=True),
            stats_pdf.sort_values(
                by=[(10, "B", "min"), (10, "B", "max"), (20, "C", "sum")]
            ).reset_index(drop=True),
        )

        # non-string names
        pdf.columns = [10, 20, 30]
        psdf.columns = [10, 20, 30]

        for as_index in [True, False]:
            stats_psdf = psdf.groupby(10, as_index=as_index).agg({20: "min", 30: "sum"})
            stats_pdf = pdf.groupby(10, as_index=as_index).agg({20: "min", 30: "sum"})
            self.assert_eq(
                stats_psdf.sort_values(by=[20, 30]).reset_index(drop=True),
                stats_pdf.sort_values(by=[20, 30]).reset_index(drop=True),
            )

        stats_psdf = psdf.groupby(10).agg({20: ["min", "max"], 30: "sum"})
        stats_pdf = pdf.groupby(10).agg({20: ["min", "max"], 30: "sum"})
        self.assert_eq(
            stats_psdf.reset_index(drop=True),
            stats_pdf.reset_index(drop=True),
        )

    def test_aggregate_func_str_list(self):
        # this is test for cases where only string or list is assigned
        pdf = pd.DataFrame(
            {
                "kind": ["cat", "dog", "cat", "dog"],
                "height": [9.1, 6.0, 9.5, 34.0],
                "weight": [7.9, 7.5, 9.9, 198.0],
            }
        )
        psdf = ps.from_pandas(pdf)

        agg_funcs = ["max", "min", ["min", "max"]]
        for aggfunc in agg_funcs:
            # Since in Koalas groupby, the order of rows might be different
            # so sort on index to ensure they have same output
            sorted_agg_psdf = psdf.groupby("kind").agg(aggfunc).sort_index()
            sorted_agg_pdf = pdf.groupby("kind").agg(aggfunc).sort_index()
            self.assert_eq(sorted_agg_psdf, sorted_agg_pdf)

        # test on multi index column case
        pdf = pd.DataFrame(
            {"A": [1, 1, 2, 2], "B": [1, 2, 3, 4], "C": [0.362, 0.227, 1.267, -0.562]}
        )
        psdf = ps.from_pandas(pdf)

        columns = pd.MultiIndex.from_tuples([("X", "A"), ("X", "B"), ("Y", "C")])
        pdf.columns = columns
        psdf.columns = columns

        for aggfunc in agg_funcs:
            sorted_agg_psdf = psdf.groupby(("X", "A")).agg(aggfunc).sort_index()
            sorted_agg_pdf = pdf.groupby(("X", "A")).agg(aggfunc).sort_index()
            self.assert_eq(sorted_agg_psdf, sorted_agg_pdf)

    def test_aggregate_relabel(self):
        # this is to test named aggregation in groupby
        pdf = pd.DataFrame({"group": ["a", "a", "b", "b"], "A": [0, 1, 2, 3], "B": [5, 6, 7, 8]})
        psdf = ps.from_pandas(pdf)

        # different agg column, same function
        agg_pdf = pdf.groupby("group").agg(a_max=("A", "max"), b_max=("B", "max")).sort_index()
        agg_psdf = psdf.groupby("group").agg(a_max=("A", "max"), b_max=("B", "max")).sort_index()
        self.assert_eq(agg_pdf, agg_psdf)

        # same agg column, different functions
        agg_pdf = pdf.groupby("group").agg(b_max=("B", "max"), b_min=("B", "min")).sort_index()
        agg_psdf = psdf.groupby("group").agg(b_max=("B", "max"), b_min=("B", "min")).sort_index()
        self.assert_eq(agg_pdf, agg_psdf)

        # test on NamedAgg
        agg_pdf = (
            pdf.groupby("group").agg(b_max=pd.NamedAgg(column="B", aggfunc="max")).sort_index()
        )
        agg_psdf = (
            psdf.groupby("group").agg(b_max=ps.NamedAgg(column="B", aggfunc="max")).sort_index()
        )
        self.assert_eq(agg_psdf, agg_pdf)

        # test on NamedAgg multi columns aggregation
        agg_pdf = (
            pdf.groupby("group")
            .agg(
                b_max=pd.NamedAgg(column="B", aggfunc="max"),
                b_min=pd.NamedAgg(column="B", aggfunc="min"),
            )
            .sort_index()
        )
        agg_psdf = (
            psdf.groupby("group")
            .agg(
                b_max=ps.NamedAgg(column="B", aggfunc="max"),
                b_min=ps.NamedAgg(column="B", aggfunc="min"),
            )
            .sort_index()
        )
        self.assert_eq(agg_psdf, agg_pdf)

    def test_aggregate_relabel_multiindex(self):
        pdf = pd.DataFrame({"A": [0, 1, 2, 3], "B": [5, 6, 7, 8], "group": ["a", "a", "b", "b"]})
        pdf.columns = pd.MultiIndex.from_tuples([("y", "A"), ("y", "B"), ("x", "group")])
        psdf = ps.from_pandas(pdf)

        agg_pdf = pdf.groupby(("x", "group")).agg(a_max=(("y", "A"), "max")).sort_index()
        agg_psdf = psdf.groupby(("x", "group")).agg(a_max=(("y", "A"), "max")).sort_index()
        self.assert_eq(agg_pdf, agg_psdf)

        # same column, different methods
        agg_pdf = (
            pdf.groupby(("x", "group"))
            .agg(a_max=(("y", "A"), "max"), a_min=(("y", "A"), "min"))
            .sort_index()
        )
        agg_psdf = (
            psdf.groupby(("x", "group"))
            .agg(a_max=(("y", "A"), "max"), a_min=(("y", "A"), "min"))
            .sort_index()
        )
        self.assert_eq(agg_pdf, agg_psdf)

        # different column, different methods
        agg_pdf = (
            pdf.groupby(("x", "group"))
            .agg(a_max=(("y", "B"), "max"), a_min=(("y", "A"), "min"))
            .sort_index()
        )
        agg_psdf = (
            psdf.groupby(("x", "group"))
            .agg(a_max=(("y", "B"), "max"), a_min=(("y", "A"), "min"))
            .sort_index()
        )
        self.assert_eq(agg_pdf, agg_psdf)


class GroupbyAggregateTests(
    GroupbyAggregateMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.groupby.test_aggregate import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
