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
import inspect
from distutils.version import LooseVersion
from itertools import product

import numpy as np
import pandas as pd

from pyspark import pandas as ps
from pyspark.pandas.config import option_context
from pyspark.pandas.exceptions import PandasNotImplementedError, DataError
from pyspark.pandas.missing.groupby import (
    MissingPandasLikeDataFrameGroupBy,
    MissingPandasLikeSeriesGroupBy,
)
from pyspark.pandas.groupby import is_multi_agg_with_relabel, SeriesGroupBy
from pyspark.testing.pandasutils import PandasOnSparkTestCase, TestUtils


class GroupByTest(PandasOnSparkTestCase, TestUtils):
    def test_groupby_simple(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 2, 6, 4, 4, 6, 4, 3, 7],
                "b": [4, 2, 7, 3, 3, 1, 1, 1, 2],
                "c": [4, 2, 7, 3, None, 1, 1, 1, 2],
                "d": list("abcdefght"),
            },
            index=[0, 1, 3, 5, 6, 8, 9, 9, 9],
        )
        psdf = ps.from_pandas(pdf)

        for as_index in [True, False]:
            if as_index:
                sort = lambda df: df.sort_index()
            else:
                sort = lambda df: df.sort_values("a").reset_index(drop=True)
            self.assert_eq(
                sort(psdf.groupby("a", as_index=as_index).sum()),
                sort(pdf.groupby("a", as_index=as_index).sum()),
            )
            self.assert_eq(
                sort(psdf.groupby("a", as_index=as_index).b.sum()),
                sort(pdf.groupby("a", as_index=as_index).b.sum()),
            )
            self.assert_eq(
                sort(psdf.groupby("a", as_index=as_index)["b"].sum()),
                sort(pdf.groupby("a", as_index=as_index)["b"].sum()),
            )
            self.assert_eq(
                sort(psdf.groupby("a", as_index=as_index)[["b", "c"]].sum()),
                sort(pdf.groupby("a", as_index=as_index)[["b", "c"]].sum()),
            )
            self.assert_eq(
                sort(psdf.groupby("a", as_index=as_index)[[]].sum()),
                sort(pdf.groupby("a", as_index=as_index)[[]].sum()),
            )
            self.assert_eq(
                sort(psdf.groupby("a", as_index=as_index)["c"].sum()),
                sort(pdf.groupby("a", as_index=as_index)["c"].sum()),
            )

        self.assert_eq(
            psdf.groupby("a").a.sum().sort_index(), pdf.groupby("a").a.sum().sort_index()
        )
        self.assert_eq(
            psdf.groupby("a")["a"].sum().sort_index(), pdf.groupby("a")["a"].sum().sort_index()
        )
        self.assert_eq(
            psdf.groupby("a")[["a"]].sum().sort_index(), pdf.groupby("a")[["a"]].sum().sort_index()
        )
        self.assert_eq(
            psdf.groupby("a")[["a", "c"]].sum().sort_index(),
            pdf.groupby("a")[["a", "c"]].sum().sort_index(),
        )

        self.assert_eq(
            psdf.a.groupby(psdf.b).sum().sort_index(), pdf.a.groupby(pdf.b).sum().sort_index()
        )

        for axis in [0, "index"]:
            self.assert_eq(
                psdf.groupby("a", axis=axis).a.sum().sort_index(),
                pdf.groupby("a", axis=axis).a.sum().sort_index(),
            )
            self.assert_eq(
                psdf.groupby("a", axis=axis)["a"].sum().sort_index(),
                pdf.groupby("a", axis=axis)["a"].sum().sort_index(),
            )
            self.assert_eq(
                psdf.groupby("a", axis=axis)[["a"]].sum().sort_index(),
                pdf.groupby("a", axis=axis)[["a"]].sum().sort_index(),
            )
            self.assert_eq(
                psdf.groupby("a", axis=axis)[["a", "c"]].sum().sort_index(),
                pdf.groupby("a", axis=axis)[["a", "c"]].sum().sort_index(),
            )

            self.assert_eq(
                psdf.a.groupby(psdf.b, axis=axis).sum().sort_index(),
                pdf.a.groupby(pdf.b, axis=axis).sum().sort_index(),
            )

        self.assertRaises(ValueError, lambda: psdf.groupby("a", as_index=False).a)
        self.assertRaises(ValueError, lambda: psdf.groupby("a", as_index=False)["a"])
        self.assertRaises(ValueError, lambda: psdf.groupby("a", as_index=False)[["a"]])
        self.assertRaises(ValueError, lambda: psdf.groupby("a", as_index=False)[["a", "c"]])
        self.assertRaises(KeyError, lambda: psdf.groupby("z", as_index=False)[["a", "c"]])
        self.assertRaises(KeyError, lambda: psdf.groupby(["z"], as_index=False)[["a", "c"]])

        self.assertRaises(TypeError, lambda: psdf.a.groupby(psdf.b, as_index=False))

        self.assertRaises(NotImplementedError, lambda: psdf.groupby("a", axis=1))
        self.assertRaises(NotImplementedError, lambda: psdf.groupby("a", axis="columns"))
        self.assertRaises(ValueError, lambda: psdf.groupby("a", "b"))
        self.assertRaises(TypeError, lambda: psdf.a.groupby(psdf.a, psdf.b))

        # we can't use column name/names as a parameter `by` for `SeriesGroupBy`.
        self.assertRaises(KeyError, lambda: psdf.a.groupby(by="a"))
        self.assertRaises(KeyError, lambda: psdf.a.groupby(by=["a", "b"]))
        self.assertRaises(KeyError, lambda: psdf.a.groupby(by=("a", "b")))

        # we can't use DataFrame as a parameter `by` for `DataFrameGroupBy`/`SeriesGroupBy`.
        self.assertRaises(ValueError, lambda: psdf.groupby(psdf))
        self.assertRaises(ValueError, lambda: psdf.a.groupby(psdf))
        self.assertRaises(ValueError, lambda: psdf.a.groupby((psdf,)))

        # non-string names
        pdf = pd.DataFrame(
            {
                10: [1, 2, 6, 4, 4, 6, 4, 3, 7],
                20: [4, 2, 7, 3, 3, 1, 1, 1, 2],
                30: [4, 2, 7, 3, None, 1, 1, 1, 2],
                40: list("abcdefght"),
            },
            index=[0, 1, 3, 5, 6, 8, 9, 9, 9],
        )
        psdf = ps.from_pandas(pdf)

        for as_index in [True, False]:
            if as_index:
                sort = lambda df: df.sort_index()
            else:
                sort = lambda df: df.sort_values(10).reset_index(drop=True)
            self.assert_eq(
                sort(psdf.groupby(10, as_index=as_index).sum()),
                sort(pdf.groupby(10, as_index=as_index).sum()),
            )
            self.assert_eq(
                sort(psdf.groupby(10, as_index=as_index)[20].sum()),
                sort(pdf.groupby(10, as_index=as_index)[20].sum()),
            )
            self.assert_eq(
                sort(psdf.groupby(10, as_index=as_index)[[20, 30]].sum()),
                sort(pdf.groupby(10, as_index=as_index)[[20, 30]].sum()),
            )

    def test_groupby_multiindex_columns(self):
        pdf = pd.DataFrame(
            {
                (10, "a"): [1, 2, 6, 4, 4, 6, 4, 3, 7],
                (10, "b"): [4, 2, 7, 3, 3, 1, 1, 1, 2],
                (20, "c"): [4, 2, 7, 3, None, 1, 1, 1, 2],
                (30, "d"): list("abcdefght"),
            },
            index=[0, 1, 3, 5, 6, 8, 9, 9, 9],
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby((10, "a")).sum().sort_index(), pdf.groupby((10, "a")).sum().sort_index()
        )
        self.assert_eq(
            psdf.groupby((10, "a"), as_index=False)
            .sum()
            .sort_values((10, "a"))
            .reset_index(drop=True),
            pdf.groupby((10, "a"), as_index=False)
            .sum()
            .sort_values((10, "a"))
            .reset_index(drop=True),
        )
        self.assert_eq(
            psdf.groupby((10, "a"))[[(20, "c")]].sum().sort_index(),
            pdf.groupby((10, "a"))[[(20, "c")]].sum().sort_index(),
        )

        # TODO: a pandas bug?
        #  expected = pdf.groupby((10, "a"))[(20, "c")].sum().sort_index()
        expected = pd.Series(
            [4.0, 2.0, 1.0, 4.0, 8.0, 2.0],
            name=(20, "c"),
            index=pd.Index([1, 2, 3, 4, 6, 7], name=(10, "a")),
        )

        self.assert_eq(psdf.groupby((10, "a"))[(20, "c")].sum().sort_index(), expected)

        if (
            LooseVersion(pd.__version__) >= LooseVersion("1.0.4")
            and LooseVersion(pd.__version__) != LooseVersion("1.1.3")
            and LooseVersion(pd.__version__) != LooseVersion("1.1.4")
        ):
            self.assert_eq(
                psdf[(20, "c")].groupby(psdf[(10, "a")]).sum().sort_index(),
                pdf[(20, "c")].groupby(pdf[(10, "a")]).sum().sort_index(),
            )
        else:
            # Due to pandas bugs resolved in 1.0.4, re-introduced in 1.1.3 and resolved in 1.1.5
            self.assert_eq(psdf[(20, "c")].groupby(psdf[(10, "a")]).sum().sort_index(), expected)

    def test_split_apply_combine_on_series(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 2, 6, 4, 4, 6, 4, 3, 7],
                "b": [4, 2, 7, 3, 3, 1, 1, 1, 2],
                "c": [4, 2, 7, 3, None, 1, 1, 1, 2],
                "d": list("abcdefght"),
            },
            index=[0, 1, 3, 5, 6, 8, 9, 9, 9],
        )
        psdf = ps.from_pandas(pdf)

        funcs = [
            ((True, False), ["sum", "min", "max", "count", "first", "last"]),
            ((True, True), ["mean"]),
            ((False, False), ["var", "std"]),
        ]
        funcs = [(check_exact, almost, f) for (check_exact, almost), fs in funcs for f in fs]

        for as_index in [True, False]:
            if as_index:
                sort = lambda df: df.sort_index()
            else:
                sort = lambda df: df.sort_values(list(df.columns)).reset_index(drop=True)

            for check_exact, almost, func in funcs:
                for kkey, pkey in [("b", "b"), (psdf.b, pdf.b)]:
                    with self.subTest(as_index=as_index, func=func, key=pkey):
                        if as_index is True or func != "std":
                            self.assert_eq(
                                sort(getattr(psdf.groupby(kkey, as_index=as_index).a, func)()),
                                sort(getattr(pdf.groupby(pkey, as_index=as_index).a, func)()),
                                check_exact=check_exact,
                                almost=almost,
                            )
                            self.assert_eq(
                                sort(getattr(psdf.groupby(kkey, as_index=as_index), func)()),
                                sort(getattr(pdf.groupby(pkey, as_index=as_index), func)()),
                                check_exact=check_exact,
                                almost=almost,
                            )
                        else:
                            # seems like a pandas' bug for as_index=False and func == "std"?
                            self.assert_eq(
                                sort(getattr(psdf.groupby(kkey, as_index=as_index).a, func)()),
                                sort(pdf.groupby(pkey, as_index=True).a.std().reset_index()),
                                check_exact=check_exact,
                                almost=almost,
                            )
                            self.assert_eq(
                                sort(getattr(psdf.groupby(kkey, as_index=as_index), func)()),
                                sort(pdf.groupby(pkey, as_index=True).std().reset_index()),
                                check_exact=check_exact,
                                almost=almost,
                            )

                for kkey, pkey in [(psdf.b + 1, pdf.b + 1), (psdf.copy().b, pdf.copy().b)]:
                    with self.subTest(as_index=as_index, func=func, key=pkey):
                        self.assert_eq(
                            sort(getattr(psdf.groupby(kkey, as_index=as_index).a, func)()),
                            sort(getattr(pdf.groupby(pkey, as_index=as_index).a, func)()),
                            check_exact=check_exact,
                            almost=almost,
                        )
                        self.assert_eq(
                            sort(getattr(psdf.groupby(kkey, as_index=as_index), func)()),
                            sort(getattr(pdf.groupby(pkey, as_index=as_index), func)()),
                            check_exact=check_exact,
                            almost=almost,
                        )

            for check_exact, almost, func in funcs:
                for i in [0, 4, 7]:
                    with self.subTest(as_index=as_index, func=func, i=i):
                        self.assert_eq(
                            sort(getattr(psdf.groupby(psdf.b > i, as_index=as_index).a, func)()),
                            sort(getattr(pdf.groupby(pdf.b > i, as_index=as_index).a, func)()),
                            check_exact=check_exact,
                            almost=almost,
                        )
                        self.assert_eq(
                            sort(getattr(psdf.groupby(psdf.b > i, as_index=as_index), func)()),
                            sort(getattr(pdf.groupby(pdf.b > i, as_index=as_index), func)()),
                            check_exact=check_exact,
                            almost=almost,
                        )

        for check_exact, almost, func in funcs:
            for kkey, pkey in [
                (psdf.b, pdf.b),
                (psdf.b + 1, pdf.b + 1),
                (psdf.copy().b, pdf.copy().b),
                (psdf.b.rename(), pdf.b.rename()),
            ]:
                with self.subTest(func=func, key=pkey):
                    self.assert_eq(
                        getattr(psdf.a.groupby(kkey), func)().sort_index(),
                        getattr(pdf.a.groupby(pkey), func)().sort_index(),
                        check_exact=check_exact,
                        almost=almost,
                    )
                    self.assert_eq(
                        getattr((psdf.a + 1).groupby(kkey), func)().sort_index(),
                        getattr((pdf.a + 1).groupby(pkey), func)().sort_index(),
                        check_exact=check_exact,
                        almost=almost,
                    )
                    self.assert_eq(
                        getattr((psdf.b + 1).groupby(kkey), func)().sort_index(),
                        getattr((pdf.b + 1).groupby(pkey), func)().sort_index(),
                        check_exact=check_exact,
                        almost=almost,
                    )
                    self.assert_eq(
                        getattr(psdf.a.rename().groupby(kkey), func)().sort_index(),
                        getattr(pdf.a.rename().groupby(pkey), func)().sort_index(),
                        check_exact=check_exact,
                        almost=almost,
                    )

    def test_aggregate(self):
        pdf = pd.DataFrame(
            {"A": [1, 1, 2, 2], "B": [1, 2, 3, 4], "C": [0.362, 0.227, 1.267, -0.562]}
        )
        psdf = ps.from_pandas(pdf)

        for as_index in [True, False]:
            if as_index:
                sort = lambda df: df.sort_index()
            else:
                sort = lambda df: df.sort_values(list(df.columns)).reset_index(drop=True)

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
            stats_psdf.sort_values(by=[(20, "min"), (20, "max"), (30, "sum")]).reset_index(
                drop=True
            ),
            stats_pdf.sort_values(by=[(20, "min"), (20, "max"), (30, "sum")]).reset_index(
                drop=True
            ),
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

    @unittest.skipIf(pd.__version__ < "0.25.0", "not supported before pandas 0.25.0")
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
                        sort = lambda df: df.sort_index()
                    else:
                        sort = lambda df: df.sort_values("A").reset_index(drop=True)

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
                        sort = lambda df: df.sort_index()
                    else:
                        sort = lambda df: df.sort_values(["A", "B"]).reset_index(drop=True)

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
                        sort = lambda df: df.sort_index()
                    else:
                        sort = lambda df: df.sort_values(("X", "A")).reset_index(drop=True)
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
                    sort = lambda df: df.sort_index()
                else:
                    sort = lambda df: df.sort_values("A").reset_index(drop=True)

                self.assert_eq(
                    sort(psdf.groupby("A", as_index=as_index, dropna=True)["B"].min()),
                    sort(pdf.groupby("A", as_index=as_index)["B"].min()),
                )

                if as_index:
                    sort = lambda df: df.sort_index()
                else:
                    sort = lambda df: df.sort_values(["A", "B"]).reset_index(drop=True)

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
                describe_psdf.drop(list(product(agg_cols, formatted_percentiles))),
                describe_pdf.drop(columns=formatted_percentiles, level=1),
                check_exact=False,
            )

            # 2. Check that percentile columns are equal.
            # The interpolation argument is yet to be implemented in Koalas.
            quantile_pdf = pdf.groupby("a").quantile(percentiles, interpolation="nearest")
            quantile_pdf = quantile_pdf.unstack(level=1).astype(float)
            self.assert_eq(
                describe_psdf.drop(list(product(agg_cols, non_percentile_stats))),
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
                [
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
                [
                    tuple(list(label) + [s])
                    for label, s in product(agg_column_labels, non_percentile_stats)
                ]
            ),
            quantile_pdf.rename(columns="{:.0%}".format, level=2),
        )

    def test_aggregate_relabel_multiindex(self):
        pdf = pd.DataFrame({"A": [0, 1, 2, 3], "B": [5, 6, 7, 8], "group": ["a", "a", "b", "b"]})
        pdf.columns = pd.MultiIndex.from_tuples([("y", "A"), ("y", "B"), ("x", "group")])
        psdf = ps.from_pandas(pdf)

        if LooseVersion(pd.__version__) < LooseVersion("1.0.0"):
            agg_pdf = pd.DataFrame(
                {"a_max": [1, 3]}, index=pd.Index(["a", "b"], name=("x", "group"))
            )
        elif LooseVersion(pd.__version__) >= LooseVersion("1.0.0"):
            agg_pdf = pdf.groupby(("x", "group")).agg(a_max=(("y", "A"), "max")).sort_index()
        agg_psdf = psdf.groupby(("x", "group")).agg(a_max=(("y", "A"), "max")).sort_index()
        self.assert_eq(agg_pdf, agg_psdf)

        # same column, different methods
        if LooseVersion(pd.__version__) < LooseVersion("1.0.0"):
            agg_pdf = pd.DataFrame(
                {"a_max": [1, 3], "a_min": [0, 2]}, index=pd.Index(["a", "b"], name=("x", "group"))
            )
        elif LooseVersion(pd.__version__) >= LooseVersion("1.0.0"):
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
        if LooseVersion(pd.__version__) < LooseVersion("1.0.0"):
            agg_pdf = pd.DataFrame(
                {"a_max": [6, 8], "a_min": [0, 2]}, index=pd.Index(["a", "b"], name=("x", "group"))
            )
        elif LooseVersion(pd.__version__) >= LooseVersion("1.0.0"):
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

    def test_all_any(self):
        pdf = pd.DataFrame(
            {
                "A": [1, 1, 2, 2, 3, 3, 4, 4, 5, 5],
                "B": [True, True, True, False, False, False, None, True, None, False],
            }
        )
        psdf = ps.from_pandas(pdf)

        for as_index in [True, False]:
            if as_index:
                sort = lambda df: df.sort_index()
            else:
                sort = lambda df: df.sort_values("A").reset_index(drop=True)
            self.assert_eq(
                sort(psdf.groupby("A", as_index=as_index).all()),
                sort(pdf.groupby("A", as_index=as_index).all()),
            )
            self.assert_eq(
                sort(psdf.groupby("A", as_index=as_index).any()),
                sort(pdf.groupby("A", as_index=as_index).any()),
            )

            self.assert_eq(
                sort(psdf.groupby("A", as_index=as_index).all()).B,
                sort(pdf.groupby("A", as_index=as_index).all()).B,
            )
            self.assert_eq(
                sort(psdf.groupby("A", as_index=as_index).any()).B,
                sort(pdf.groupby("A", as_index=as_index).any()).B,
            )

        self.assert_eq(
            psdf.B.groupby(psdf.A).all().sort_index(), pdf.B.groupby(pdf.A).all().sort_index()
        )
        self.assert_eq(
            psdf.B.groupby(psdf.A).any().sort_index(), pdf.B.groupby(pdf.A).any().sort_index()
        )

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("X", "A"), ("Y", "B")])
        pdf.columns = columns
        psdf.columns = columns

        for as_index in [True, False]:
            if as_index:
                sort = lambda df: df.sort_index()
            else:
                sort = lambda df: df.sort_values(("X", "A")).reset_index(drop=True)
            self.assert_eq(
                sort(psdf.groupby(("X", "A"), as_index=as_index).all()),
                sort(pdf.groupby(("X", "A"), as_index=as_index).all()),
            )
            self.assert_eq(
                sort(psdf.groupby(("X", "A"), as_index=as_index).any()),
                sort(pdf.groupby(("X", "A"), as_index=as_index).any()),
            )

    def test_raises(self):
        psdf = ps.DataFrame(
            {"a": [1, 2, 6, 4, 4, 6, 4, 3, 7], "b": [4, 2, 7, 3, 3, 1, 1, 1, 2]},
            index=[0, 1, 3, 5, 6, 8, 9, 9, 9],
        )
        # test raises with incorrect key
        self.assertRaises(ValueError, lambda: psdf.groupby([]))
        self.assertRaises(KeyError, lambda: psdf.groupby("x"))
        self.assertRaises(KeyError, lambda: psdf.groupby(["a", "x"]))
        self.assertRaises(KeyError, lambda: psdf.groupby("a")["x"])
        self.assertRaises(KeyError, lambda: psdf.groupby("a")["b", "x"])
        self.assertRaises(KeyError, lambda: psdf.groupby("a")[["b", "x"]])

    def test_nunique(self):
        pdf = pd.DataFrame(
            {"a": [1, 1, 1, 1, 1, 0, 0, 0, 0, 0], "b": [2, 2, 2, 3, 3, 4, 4, 5, 5, 5]}
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            psdf.groupby("a").agg({"b": "nunique"}).sort_index(),
            pdf.groupby("a").agg({"b": "nunique"}).sort_index(),
        )
        if LooseVersion(pd.__version__) < LooseVersion("1.1.0"):
            expected = ps.DataFrame({"b": [2, 2]}, index=pd.Index([0, 1], name="a"))
            self.assert_eq(psdf.groupby("a").nunique().sort_index(), expected)
            self.assert_eq(
                psdf.groupby("a").nunique(dropna=False).sort_index(),
                expected,
            )
        else:
            self.assert_eq(
                psdf.groupby("a").nunique().sort_index(), pdf.groupby("a").nunique().sort_index()
            )
            self.assert_eq(
                psdf.groupby("a").nunique(dropna=False).sort_index(),
                pdf.groupby("a").nunique(dropna=False).sort_index(),
            )
        self.assert_eq(
            psdf.groupby("a")["b"].nunique().sort_index(),
            pdf.groupby("a")["b"].nunique().sort_index(),
        )
        self.assert_eq(
            psdf.groupby("a")["b"].nunique(dropna=False).sort_index(),
            pdf.groupby("a")["b"].nunique(dropna=False).sort_index(),
        )

        nunique_psdf = psdf.groupby("a", as_index=False).agg({"b": "nunique"})
        nunique_pdf = pdf.groupby("a", as_index=False).agg({"b": "nunique"})
        self.assert_eq(
            nunique_psdf.sort_values(["a", "b"]).reset_index(drop=True),
            nunique_pdf.sort_values(["a", "b"]).reset_index(drop=True),
        )

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("y", "b")])
        pdf.columns = columns
        psdf.columns = columns

        if LooseVersion(pd.__version__) < LooseVersion("1.1.0"):
            expected = ps.DataFrame({("y", "b"): [2, 2]}, index=pd.Index([0, 1], name=("x", "a")))
            self.assert_eq(
                psdf.groupby(("x", "a")).nunique().sort_index(),
                expected,
            )
            self.assert_eq(
                psdf.groupby(("x", "a")).nunique(dropna=False).sort_index(),
                expected,
            )
        else:
            self.assert_eq(
                psdf.groupby(("x", "a")).nunique().sort_index(),
                pdf.groupby(("x", "a")).nunique().sort_index(),
            )
            self.assert_eq(
                psdf.groupby(("x", "a")).nunique(dropna=False).sort_index(),
                pdf.groupby(("x", "a")).nunique(dropna=False).sort_index(),
            )

    def test_unique(self):
        for pdf in [
            pd.DataFrame(
                {"a": [1, 1, 1, 1, 1, 0, 0, 0, 0, 0], "b": [2, 2, 2, 3, 3, 4, 4, 5, 5, 5]}
            ),
            pd.DataFrame(
                {
                    "a": [1, 1, 1, 1, 1, 0, 0, 0, 0, 0],
                    "b": ["w", "w", "w", "x", "x", "y", "y", "z", "z", "z"],
                }
            ),
        ]:
            with self.subTest(pdf=pdf):
                psdf = ps.from_pandas(pdf)

                actual = psdf.groupby("a")["b"].unique().sort_index().to_pandas()
                expect = pdf.groupby("a")["b"].unique().sort_index()
                self.assert_eq(len(actual), len(expect))
                for act, exp in zip(actual, expect):
                    self.assertTrue(sorted(act) == sorted(exp))

    def test_value_counts(self):
        pdf = pd.DataFrame({"A": [1, 2, 2, 3, 3, 3], "B": [1, 1, 2, 3, 3, 3]}, columns=["A", "B"])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            psdf.groupby("A")["B"].value_counts().sort_index(),
            pdf.groupby("A")["B"].value_counts().sort_index(),
        )
        self.assert_eq(
            psdf.groupby("A")["B"].value_counts(sort=True, ascending=False).sort_index(),
            pdf.groupby("A")["B"].value_counts(sort=True, ascending=False).sort_index(),
        )
        self.assert_eq(
            psdf.groupby("A")["B"].value_counts(sort=True, ascending=True).sort_index(),
            pdf.groupby("A")["B"].value_counts(sort=True, ascending=True).sort_index(),
        )
        self.assert_eq(
            psdf.B.rename().groupby(psdf.A).value_counts().sort_index(),
            pdf.B.rename().groupby(pdf.A).value_counts().sort_index(),
        )
        self.assert_eq(
            psdf.B.groupby(psdf.A.rename()).value_counts().sort_index(),
            pdf.B.groupby(pdf.A.rename()).value_counts().sort_index(),
        )
        self.assert_eq(
            psdf.B.rename().groupby(psdf.A.rename()).value_counts().sort_index(),
            pdf.B.rename().groupby(pdf.A.rename()).value_counts().sort_index(),
        )

    def test_size(self):
        pdf = pd.DataFrame({"A": [1, 2, 2, 3, 3, 3], "B": [1, 1, 2, 3, 3, 3]})
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psdf.groupby("A").size().sort_index(), pdf.groupby("A").size().sort_index())
        self.assert_eq(
            psdf.groupby("A")["B"].size().sort_index(), pdf.groupby("A")["B"].size().sort_index()
        )
        self.assert_eq(
            psdf.groupby("A")[["B"]].size().sort_index(),
            pdf.groupby("A")[["B"]].size().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["A", "B"]).size().sort_index(),
            pdf.groupby(["A", "B"]).size().sort_index(),
        )

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("X", "A"), ("Y", "B")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.groupby(("X", "A")).size().sort_index(),
            pdf.groupby(("X", "A")).size().sort_index(),
        )
        self.assert_eq(
            psdf.groupby([("X", "A"), ("Y", "B")]).size().sort_index(),
            pdf.groupby([("X", "A"), ("Y", "B")]).size().sort_index(),
        )

    def test_diff(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 2, 3, 4, 5, 6] * 3,
                "b": [1, 1, 2, 3, 5, 8] * 3,
                "c": [1, 4, 9, 16, 25, 36] * 3,
            }
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.groupby("b").diff().sort_index(), pdf.groupby("b").diff().sort_index())
        self.assert_eq(
            psdf.groupby(["a", "b"]).diff().sort_index(),
            pdf.groupby(["a", "b"]).diff().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["b"])["a"].diff().sort_index(),
            pdf.groupby(["b"])["a"].diff().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["b"])[["a", "b"]].diff().sort_index(),
            pdf.groupby(["b"])[["a", "b"]].diff().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5).diff().sort_index(),
            pdf.groupby(pdf.b // 5).diff().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5)["a"].diff().sort_index(),
            pdf.groupby(pdf.b // 5)["a"].diff().sort_index(),
        )

        self.assert_eq(psdf.groupby("b").diff().sum(), pdf.groupby("b").diff().sum().astype(int))
        self.assert_eq(psdf.groupby(["b"])["a"].diff().sum(), pdf.groupby(["b"])["a"].diff().sum())

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.groupby(("x", "b")).diff().sort_index(),
            pdf.groupby(("x", "b")).diff().sort_index(),
        )
        self.assert_eq(
            psdf.groupby([("x", "a"), ("x", "b")]).diff().sort_index(),
            pdf.groupby([("x", "a"), ("x", "b")]).diff().sort_index(),
        )

    def test_rank(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 2, 3, 4, 5, 6] * 3,
                "b": [1, 1, 2, 3, 5, 8] * 3,
                "c": [1, 4, 9, 16, 25, 36] * 3,
            },
            index=np.random.rand(6 * 3),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.groupby("b").rank().sort_index(), pdf.groupby("b").rank().sort_index())
        self.assert_eq(
            psdf.groupby(["a", "b"]).rank().sort_index(),
            pdf.groupby(["a", "b"]).rank().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["b"])["a"].rank().sort_index(),
            pdf.groupby(["b"])["a"].rank().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["b"])[["a", "c"]].rank().sort_index(),
            pdf.groupby(["b"])[["a", "c"]].rank().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5).rank().sort_index(),
            pdf.groupby(pdf.b // 5).rank().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5)["a"].rank().sort_index(),
            pdf.groupby(pdf.b // 5)["a"].rank().sort_index(),
        )

        self.assert_eq(psdf.groupby("b").rank().sum(), pdf.groupby("b").rank().sum())
        self.assert_eq(psdf.groupby(["b"])["a"].rank().sum(), pdf.groupby(["b"])["a"].rank().sum())

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.groupby(("x", "b")).rank().sort_index(),
            pdf.groupby(("x", "b")).rank().sort_index(),
        )
        self.assert_eq(
            psdf.groupby([("x", "a"), ("x", "b")]).rank().sort_index(),
            pdf.groupby([("x", "a"), ("x", "b")]).rank().sort_index(),
        )

    def test_cumcount(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 2, 3, 4, 5, 6] * 3,
                "b": [1, 1, 2, 3, 5, 8] * 3,
                "c": [1, 4, 9, 16, 25, 36] * 3,
            },
            index=np.random.rand(6 * 3),
        )
        psdf = ps.from_pandas(pdf)

        for ascending in [True, False]:
            self.assert_eq(
                psdf.groupby("b").cumcount(ascending=ascending).sort_index(),
                pdf.groupby("b").cumcount(ascending=ascending).sort_index(),
            )
            self.assert_eq(
                psdf.groupby(["a", "b"]).cumcount(ascending=ascending).sort_index(),
                pdf.groupby(["a", "b"]).cumcount(ascending=ascending).sort_index(),
            )
            self.assert_eq(
                psdf.groupby(["b"])["a"].cumcount(ascending=ascending).sort_index(),
                pdf.groupby(["b"])["a"].cumcount(ascending=ascending).sort_index(),
            )
            self.assert_eq(
                psdf.groupby(["b"])[["a", "c"]].cumcount(ascending=ascending).sort_index(),
                pdf.groupby(["b"])[["a", "c"]].cumcount(ascending=ascending).sort_index(),
            )
            self.assert_eq(
                psdf.groupby(psdf.b // 5).cumcount(ascending=ascending).sort_index(),
                pdf.groupby(pdf.b // 5).cumcount(ascending=ascending).sort_index(),
            )
            self.assert_eq(
                psdf.groupby(psdf.b // 5)["a"].cumcount(ascending=ascending).sort_index(),
                pdf.groupby(pdf.b // 5)["a"].cumcount(ascending=ascending).sort_index(),
            )
            self.assert_eq(
                psdf.groupby("b").cumcount(ascending=ascending).sum(),
                pdf.groupby("b").cumcount(ascending=ascending).sum(),
            )
            self.assert_eq(
                psdf.a.rename().groupby(psdf.b).cumcount(ascending=ascending).sort_index(),
                pdf.a.rename().groupby(pdf.b).cumcount(ascending=ascending).sort_index(),
            )
            self.assert_eq(
                psdf.a.groupby(psdf.b.rename()).cumcount(ascending=ascending).sort_index(),
                pdf.a.groupby(pdf.b.rename()).cumcount(ascending=ascending).sort_index(),
            )
            self.assert_eq(
                psdf.a.rename().groupby(psdf.b.rename()).cumcount(ascending=ascending).sort_index(),
                pdf.a.rename().groupby(pdf.b.rename()).cumcount(ascending=ascending).sort_index(),
            )

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        for ascending in [True, False]:
            self.assert_eq(
                psdf.groupby(("x", "b")).cumcount(ascending=ascending).sort_index(),
                pdf.groupby(("x", "b")).cumcount(ascending=ascending).sort_index(),
            )
            self.assert_eq(
                psdf.groupby([("x", "a"), ("x", "b")]).cumcount(ascending=ascending).sort_index(),
                pdf.groupby([("x", "a"), ("x", "b")]).cumcount(ascending=ascending).sort_index(),
            )

    def test_cummin(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 2, 3, 4, 5, 6] * 3,
                "b": [1, 1, 2, 3, 5, 8] * 3,
                "c": [1, 4, 9, 16, 25, 36] * 3,
            },
            index=np.random.rand(6 * 3),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby("b").cummin().sort_index(), pdf.groupby("b").cummin().sort_index()
        )
        self.assert_eq(
            psdf.groupby(["a", "b"]).cummin().sort_index(),
            pdf.groupby(["a", "b"]).cummin().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["b"])["a"].cummin().sort_index(),
            pdf.groupby(["b"])["a"].cummin().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["b"])[["a", "c"]].cummin().sort_index(),
            pdf.groupby(["b"])[["a", "c"]].cummin().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5).cummin().sort_index(),
            pdf.groupby(pdf.b // 5).cummin().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5)["a"].cummin().sort_index(),
            pdf.groupby(pdf.b // 5)["a"].cummin().sort_index(),
        )
        self.assert_eq(
            psdf.groupby("b").cummin().sum().sort_index(),
            pdf.groupby("b").cummin().sum().sort_index(),
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b).cummin().sort_index(),
            pdf.a.rename().groupby(pdf.b).cummin().sort_index(),
        )
        self.assert_eq(
            psdf.a.groupby(psdf.b.rename()).cummin().sort_index(),
            pdf.a.groupby(pdf.b.rename()).cummin().sort_index(),
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b.rename()).cummin().sort_index(),
            pdf.a.rename().groupby(pdf.b.rename()).cummin().sort_index(),
        )

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.groupby(("x", "b")).cummin().sort_index(),
            pdf.groupby(("x", "b")).cummin().sort_index(),
        )
        self.assert_eq(
            psdf.groupby([("x", "a"), ("x", "b")]).cummin().sort_index(),
            pdf.groupby([("x", "a"), ("x", "b")]).cummin().sort_index(),
        )

        psdf = ps.DataFrame([["a"], ["b"], ["c"]], columns=["A"])
        self.assertRaises(DataError, lambda: psdf.groupby(["A"]).cummin())
        psdf = ps.DataFrame([[1, "a"], [2, "b"], [3, "c"]], columns=["A", "B"])
        self.assertRaises(DataError, lambda: psdf.groupby(["A"])["B"].cummin())

    def test_cummax(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 2, 3, 4, 5, 6] * 3,
                "b": [1, 1, 2, 3, 5, 8] * 3,
                "c": [1, 4, 9, 16, 25, 36] * 3,
            },
            index=np.random.rand(6 * 3),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby("b").cummax().sort_index(), pdf.groupby("b").cummax().sort_index()
        )
        self.assert_eq(
            psdf.groupby(["a", "b"]).cummax().sort_index(),
            pdf.groupby(["a", "b"]).cummax().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["b"])["a"].cummax().sort_index(),
            pdf.groupby(["b"])["a"].cummax().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["b"])[["a", "c"]].cummax().sort_index(),
            pdf.groupby(["b"])[["a", "c"]].cummax().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5).cummax().sort_index(),
            pdf.groupby(pdf.b // 5).cummax().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5)["a"].cummax().sort_index(),
            pdf.groupby(pdf.b // 5)["a"].cummax().sort_index(),
        )
        self.assert_eq(
            psdf.groupby("b").cummax().sum().sort_index(),
            pdf.groupby("b").cummax().sum().sort_index(),
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b).cummax().sort_index(),
            pdf.a.rename().groupby(pdf.b).cummax().sort_index(),
        )
        self.assert_eq(
            psdf.a.groupby(psdf.b.rename()).cummax().sort_index(),
            pdf.a.groupby(pdf.b.rename()).cummax().sort_index(),
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b.rename()).cummax().sort_index(),
            pdf.a.rename().groupby(pdf.b.rename()).cummax().sort_index(),
        )

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.groupby(("x", "b")).cummax().sort_index(),
            pdf.groupby(("x", "b")).cummax().sort_index(),
        )
        self.assert_eq(
            psdf.groupby([("x", "a"), ("x", "b")]).cummax().sort_index(),
            pdf.groupby([("x", "a"), ("x", "b")]).cummax().sort_index(),
        )

        psdf = ps.DataFrame([["a"], ["b"], ["c"]], columns=["A"])
        self.assertRaises(DataError, lambda: psdf.groupby(["A"]).cummax())
        psdf = ps.DataFrame([[1, "a"], [2, "b"], [3, "c"]], columns=["A", "B"])
        self.assertRaises(DataError, lambda: psdf.groupby(["A"])["B"].cummax())

    def test_cumsum(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 2, 3, 4, 5, 6] * 3,
                "b": [1, 1, 2, 3, 5, 8] * 3,
                "c": [1, 4, 9, 16, 25, 36] * 3,
            },
            index=np.random.rand(6 * 3),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby("b").cumsum().sort_index(), pdf.groupby("b").cumsum().sort_index()
        )
        self.assert_eq(
            psdf.groupby(["a", "b"]).cumsum().sort_index(),
            pdf.groupby(["a", "b"]).cumsum().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["b"])["a"].cumsum().sort_index(),
            pdf.groupby(["b"])["a"].cumsum().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["b"])[["a", "c"]].cumsum().sort_index(),
            pdf.groupby(["b"])[["a", "c"]].cumsum().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5).cumsum().sort_index(),
            pdf.groupby(pdf.b // 5).cumsum().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5)["a"].cumsum().sort_index(),
            pdf.groupby(pdf.b // 5)["a"].cumsum().sort_index(),
        )
        self.assert_eq(
            psdf.groupby("b").cumsum().sum().sort_index(),
            pdf.groupby("b").cumsum().sum().sort_index(),
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b).cumsum().sort_index(),
            pdf.a.rename().groupby(pdf.b).cumsum().sort_index(),
        )
        self.assert_eq(
            psdf.a.groupby(psdf.b.rename()).cumsum().sort_index(),
            pdf.a.groupby(pdf.b.rename()).cumsum().sort_index(),
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b.rename()).cumsum().sort_index(),
            pdf.a.rename().groupby(pdf.b.rename()).cumsum().sort_index(),
        )

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.groupby(("x", "b")).cumsum().sort_index(),
            pdf.groupby(("x", "b")).cumsum().sort_index(),
        )
        self.assert_eq(
            psdf.groupby([("x", "a"), ("x", "b")]).cumsum().sort_index(),
            pdf.groupby([("x", "a"), ("x", "b")]).cumsum().sort_index(),
        )

        psdf = ps.DataFrame([["a"], ["b"], ["c"]], columns=["A"])
        self.assertRaises(DataError, lambda: psdf.groupby(["A"]).cumsum())
        psdf = ps.DataFrame([[1, "a"], [2, "b"], [3, "c"]], columns=["A", "B"])
        self.assertRaises(DataError, lambda: psdf.groupby(["A"])["B"].cumsum())

    def test_cumprod(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 2, -3, 4, -5, 6] * 3,
                "b": [1, 1, 2, 3, 5, 8] * 3,
                "c": [1, 0, 9, 16, 25, 36] * 3,
            },
            index=np.random.rand(6 * 3),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby("b").cumprod().sort_index(),
            pdf.groupby("b").cumprod().sort_index(),
            check_exact=False,
        )
        self.assert_eq(
            psdf.groupby(["a", "b"]).cumprod().sort_index(),
            pdf.groupby(["a", "b"]).cumprod().sort_index(),
            check_exact=False,
        )
        self.assert_eq(
            psdf.groupby(["b"])["a"].cumprod().sort_index(),
            pdf.groupby(["b"])["a"].cumprod().sort_index(),
            check_exact=False,
        )
        self.assert_eq(
            psdf.groupby(["b"])[["a", "c"]].cumprod().sort_index(),
            pdf.groupby(["b"])[["a", "c"]].cumprod().sort_index(),
            check_exact=False,
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 3).cumprod().sort_index(),
            pdf.groupby(pdf.b // 3).cumprod().sort_index(),
            check_exact=False,
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 3)["a"].cumprod().sort_index(),
            pdf.groupby(pdf.b // 3)["a"].cumprod().sort_index(),
            check_exact=False,
        )
        self.assert_eq(
            psdf.groupby("b").cumprod().sum().sort_index(),
            pdf.groupby("b").cumprod().sum().sort_index(),
            check_exact=False,
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b).cumprod().sort_index(),
            pdf.a.rename().groupby(pdf.b).cumprod().sort_index(),
            check_exact=False,
        )
        self.assert_eq(
            psdf.a.groupby(psdf.b.rename()).cumprod().sort_index(),
            pdf.a.groupby(pdf.b.rename()).cumprod().sort_index(),
            check_exact=False,
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b.rename()).cumprod().sort_index(),
            pdf.a.rename().groupby(pdf.b.rename()).cumprod().sort_index(),
            check_exact=False,
        )

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.groupby(("x", "b")).cumprod().sort_index(),
            pdf.groupby(("x", "b")).cumprod().sort_index(),
            check_exact=False,
        )
        self.assert_eq(
            psdf.groupby([("x", "a"), ("x", "b")]).cumprod().sort_index(),
            pdf.groupby([("x", "a"), ("x", "b")]).cumprod().sort_index(),
            check_exact=False,
        )

        psdf = ps.DataFrame([["a"], ["b"], ["c"]], columns=["A"])
        self.assertRaises(DataError, lambda: psdf.groupby(["A"]).cumprod())
        psdf = ps.DataFrame([[1, "a"], [2, "b"], [3, "c"]], columns=["A", "B"])
        self.assertRaises(DataError, lambda: psdf.groupby(["A"])["B"].cumprod())

    def test_nsmallest(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 1, 1, 2, 2, 2, 3, 3, 3] * 3,
                "b": [1, 2, 2, 2, 3, 3, 3, 4, 4] * 3,
                "c": [1, 2, 2, 2, 3, 3, 3, 4, 4] * 3,
                "d": [1, 2, 2, 2, 3, 3, 3, 4, 4] * 3,
            },
            index=np.random.rand(9 * 3),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby(["a"])["b"].nsmallest(1).sort_values(),
            pdf.groupby(["a"])["b"].nsmallest(1).sort_values(),
        )
        self.assert_eq(
            psdf.groupby(["a"])["b"].nsmallest(2).sort_index(),
            pdf.groupby(["a"])["b"].nsmallest(2).sort_index(),
        )
        self.assert_eq(
            (psdf.b * 10).groupby(psdf.a).nsmallest(2).sort_index(),
            (pdf.b * 10).groupby(pdf.a).nsmallest(2).sort_index(),
        )
        self.assert_eq(
            psdf.b.rename().groupby(psdf.a).nsmallest(2).sort_index(),
            pdf.b.rename().groupby(pdf.a).nsmallest(2).sort_index(),
        )
        self.assert_eq(
            psdf.b.groupby(psdf.a.rename()).nsmallest(2).sort_index(),
            pdf.b.groupby(pdf.a.rename()).nsmallest(2).sort_index(),
        )
        self.assert_eq(
            psdf.b.rename().groupby(psdf.a.rename()).nsmallest(2).sort_index(),
            pdf.b.rename().groupby(pdf.a.rename()).nsmallest(2).sort_index(),
        )
        with self.assertRaisesRegex(ValueError, "nsmallest do not support multi-index now"):
            psdf.set_index(["a", "b"]).groupby(["c"])["d"].nsmallest(1)

    def test_nlargest(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 1, 1, 2, 2, 2, 3, 3, 3] * 3,
                "b": [1, 2, 2, 2, 3, 3, 3, 4, 4] * 3,
                "c": [1, 2, 2, 2, 3, 3, 3, 4, 4] * 3,
                "d": [1, 2, 2, 2, 3, 3, 3, 4, 4] * 3,
            },
            index=np.random.rand(9 * 3),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby(["a"])["b"].nlargest(1).sort_values(),
            pdf.groupby(["a"])["b"].nlargest(1).sort_values(),
        )
        self.assert_eq(
            psdf.groupby(["a"])["b"].nlargest(2).sort_index(),
            pdf.groupby(["a"])["b"].nlargest(2).sort_index(),
        )
        self.assert_eq(
            (psdf.b * 10).groupby(psdf.a).nlargest(2).sort_index(),
            (pdf.b * 10).groupby(pdf.a).nlargest(2).sort_index(),
        )
        self.assert_eq(
            psdf.b.rename().groupby(psdf.a).nlargest(2).sort_index(),
            pdf.b.rename().groupby(pdf.a).nlargest(2).sort_index(),
        )
        self.assert_eq(
            psdf.b.groupby(psdf.a.rename()).nlargest(2).sort_index(),
            pdf.b.groupby(pdf.a.rename()).nlargest(2).sort_index(),
        )
        self.assert_eq(
            psdf.b.rename().groupby(psdf.a.rename()).nlargest(2).sort_index(),
            pdf.b.rename().groupby(pdf.a.rename()).nlargest(2).sort_index(),
        )
        with self.assertRaisesRegex(ValueError, "nlargest do not support multi-index now"):
            psdf.set_index(["a", "b"]).groupby(["c"])["d"].nlargest(1)

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

        if LooseVersion(pd.__version__) <= LooseVersion("0.24.2"):
            self.assert_eq(
                psdf.groupby("A").ffill().sort_index(),
                pdf.groupby("A").ffill().sort_index().drop("A", 1),
            )
            self.assert_eq(
                psdf.groupby("A")[["B"]].ffill().sort_index(),
                pdf.groupby("A")[["B"]].ffill().sort_index().drop("A", 1),
            )
        else:
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

        if LooseVersion(pd.__version__) <= LooseVersion("0.24.2"):
            self.assert_eq(
                psdf.groupby(("X", "A")).ffill().sort_index(),
                pdf.groupby(("X", "A")).ffill().sort_index().drop(("X", "A"), 1),
            )
        else:
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

        if LooseVersion(pd.__version__) <= LooseVersion("0.24.2"):
            self.assert_eq(
                psdf.groupby("A").bfill().sort_index(),
                pdf.groupby("A").bfill().sort_index().drop("A", 1),
            )
            self.assert_eq(
                psdf.groupby("A")[["B"]].bfill().sort_index(),
                pdf.groupby("A")[["B"]].bfill().sort_index().drop("A", 1),
            )
        else:
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

        if LooseVersion(pd.__version__) <= LooseVersion("0.24.2"):
            self.assert_eq(
                psdf.groupby(("X", "A")).bfill().sort_index(),
                pdf.groupby(("X", "A")).bfill().sort_index().drop(("X", "A"), 1),
            )
        else:
            self.assert_eq(
                psdf.groupby(("X", "A")).bfill().sort_index(),
                pdf.groupby(("X", "A")).bfill().sort_index(),
            )

    @unittest.skipIf(pd.__version__ < "0.24.0", "not supported before pandas 0.24.0")
    def test_shift(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 1, 2, 2, 3, 3] * 3,
                "b": [1, 1, 2, 2, 3, 4] * 3,
                "c": [1, 4, 9, 16, 25, 36] * 3,
            },
            index=np.random.rand(6 * 3),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby("a").shift().sort_index(), pdf.groupby("a").shift().sort_index()
        )
        # TODO: seems like a pandas' bug when fill_value is not None?
        # self.assert_eq(psdf.groupby(['a', 'b']).shift(periods=-1, fill_value=0).sort_index(),
        #                pdf.groupby(['a', 'b']).shift(periods=-1, fill_value=0).sort_index())
        self.assert_eq(
            psdf.groupby(["b"])["a"].shift().sort_index(),
            pdf.groupby(["b"])["a"].shift().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["a", "b"])["c"].shift().sort_index(),
            pdf.groupby(["a", "b"])["c"].shift().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5).shift().sort_index(),
            pdf.groupby(pdf.b // 5).shift().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5)["a"].shift().sort_index(),
            pdf.groupby(pdf.b // 5)["a"].shift().sort_index(),
        )
        # TODO: known pandas' bug when fill_value is not None pandas>=1.0.0
        # https://github.com/pandas-dev/pandas/issues/31971#issue-565171762
        if LooseVersion(pd.__version__) < LooseVersion("1.0.0"):
            self.assert_eq(
                psdf.groupby(["b"])[["a", "c"]].shift(periods=-1, fill_value=0).sort_index(),
                pdf.groupby(["b"])[["a", "c"]].shift(periods=-1, fill_value=0).sort_index(),
            )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b).shift().sort_index(),
            pdf.a.rename().groupby(pdf.b).shift().sort_index(),
        )
        self.assert_eq(
            psdf.a.groupby(psdf.b.rename()).shift().sort_index(),
            pdf.a.groupby(pdf.b.rename()).shift().sort_index(),
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b.rename()).shift().sort_index(),
            pdf.a.rename().groupby(pdf.b.rename()).shift().sort_index(),
        )

        self.assert_eq(psdf.groupby("a").shift().sum(), pdf.groupby("a").shift().sum().astype(int))
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b).shift().sum(),
            pdf.a.rename().groupby(pdf.b).shift().sum(),
        )

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.groupby(("x", "a")).shift().sort_index(),
            pdf.groupby(("x", "a")).shift().sort_index(),
        )
        # TODO: seems like a pandas' bug when fill_value is not None?
        # self.assert_eq(psdf.groupby([('x', 'a'), ('x', 'b')]).shift(periods=-1,
        #                                                            fill_value=0).sort_index(),
        #                pdf.groupby([('x', 'a'), ('x', 'b')]).shift(periods=-1,
        #                                                            fill_value=0).sort_index())

    def test_apply(self):
        pdf = pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6], "b": [1, 1, 2, 3, 5, 8], "c": [1, 4, 9, 16, 25, 36]},
            columns=["a", "b", "c"],
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            psdf.groupby("b").apply(lambda x: x + x.min()).sort_index(),
            pdf.groupby("b").apply(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.groupby("b").apply(len).sort_index(),
            pdf.groupby("b").apply(len).sort_index(),
        )
        self.assert_eq(
            psdf.groupby("b")["a"]
            .apply(lambda x, y, z: x + x.min() + y * z, 10, z=20)
            .sort_index(),
            pdf.groupby("b")["a"].apply(lambda x, y, z: x + x.min() + y * z, 10, z=20).sort_index(),
        )
        self.assert_eq(
            psdf.groupby("b")[["a"]].apply(lambda x: x + x.min()).sort_index(),
            pdf.groupby("b")[["a"]].apply(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["a", "b"])
            .apply(lambda x, y, z: x + x.min() + y + z, 1, z=2)
            .sort_index(),
            pdf.groupby(["a", "b"]).apply(lambda x, y, z: x + x.min() + y + z, 1, z=2).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["b"])["c"].apply(lambda x: 1).sort_index(),
            pdf.groupby(["b"])["c"].apply(lambda x: 1).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["b"])["c"].apply(len).sort_index(),
            pdf.groupby(["b"])["c"].apply(len).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5).apply(lambda x: x + x.min()).sort_index(),
            pdf.groupby(pdf.b // 5).apply(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5)["a"].apply(lambda x: x + x.min()).sort_index(),
            pdf.groupby(pdf.b // 5)["a"].apply(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5)[["a"]].apply(lambda x: x + x.min()).sort_index(),
            pdf.groupby(pdf.b // 5)[["a"]].apply(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5)[["a"]].apply(len).sort_index(),
            pdf.groupby(pdf.b // 5)[["a"]].apply(len).sort_index(),
            almost=True,
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b).apply(lambda x: x + x.min()).sort_index(),
            pdf.a.rename().groupby(pdf.b).apply(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.a.groupby(psdf.b.rename()).apply(lambda x: x + x.min()).sort_index(),
            pdf.a.groupby(pdf.b.rename()).apply(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b.rename()).apply(lambda x: x + x.min()).sort_index(),
            pdf.a.rename().groupby(pdf.b.rename()).apply(lambda x: x + x.min()).sort_index(),
        )

        with self.assertRaisesRegex(TypeError, "int object is not callable"):
            psdf.groupby("b").apply(1)

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.groupby(("x", "b")).apply(lambda x: 1).sort_index(),
            pdf.groupby(("x", "b")).apply(lambda x: 1).sort_index(),
        )
        self.assert_eq(
            psdf.groupby([("x", "a"), ("x", "b")]).apply(lambda x: x + x.min()).sort_index(),
            pdf.groupby([("x", "a"), ("x", "b")]).apply(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(("x", "b")).apply(len).sort_index(),
            pdf.groupby(("x", "b")).apply(len).sort_index(),
        )
        self.assert_eq(
            psdf.groupby([("x", "a"), ("x", "b")]).apply(len).sort_index(),
            pdf.groupby([("x", "a"), ("x", "b")]).apply(len).sort_index(),
        )

    def test_apply_without_shortcut(self):
        with option_context("compute.shortcut_limit", 0):
            self.test_apply()

    def test_apply_negative(self):
        def func(_) -> ps.Series[int]:
            return pd.Series([1])

        with self.assertRaisesRegex(TypeError, "Series as a return type hint at frame groupby"):
            ps.range(10).groupby("id").apply(func)

    def test_apply_with_new_dataframe(self):
        pdf = pd.DataFrame(
            {"timestamp": [0.0, 0.5, 1.0, 0.0, 0.5], "car_id": ["A", "A", "A", "B", "B"]}
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby("car_id").apply(lambda _: pd.DataFrame({"column": [0.0]})).sort_index(),
            pdf.groupby("car_id").apply(lambda _: pd.DataFrame({"column": [0.0]})).sort_index(),
        )

        self.assert_eq(
            psdf.groupby("car_id")
            .apply(lambda df: pd.DataFrame({"mean": [df["timestamp"].mean()]}))
            .sort_index(),
            pdf.groupby("car_id")
            .apply(lambda df: pd.DataFrame({"mean": [df["timestamp"].mean()]}))
            .sort_index(),
        )

        # dataframe with 1000+ records
        pdf = pd.DataFrame(
            {
                "timestamp": [0.0, 0.5, 1.0, 0.0, 0.5] * 300,
                "car_id": ["A", "A", "A", "B", "B"] * 300,
            }
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby("car_id").apply(lambda _: pd.DataFrame({"column": [0.0]})).sort_index(),
            pdf.groupby("car_id").apply(lambda _: pd.DataFrame({"column": [0.0]})).sort_index(),
        )

        self.assert_eq(
            psdf.groupby("car_id")
            .apply(lambda df: pd.DataFrame({"mean": [df["timestamp"].mean()]}))
            .sort_index(),
            pdf.groupby("car_id")
            .apply(lambda df: pd.DataFrame({"mean": [df["timestamp"].mean()]}))
            .sort_index(),
        )

    def test_apply_with_new_dataframe_without_shortcut(self):
        with option_context("compute.shortcut_limit", 0):
            self.test_apply_with_new_dataframe()

    def test_apply_key_handling(self):
        pdf = pd.DataFrame(
            {"d": [1.0, 1.0, 1.0, 2.0, 2.0, 2.0], "v": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]}
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby("d").apply(sum).sort_index(), pdf.groupby("d").apply(sum).sort_index()
        )

        with ps.option_context("compute.shortcut_limit", 1):
            self.assert_eq(
                psdf.groupby("d").apply(sum).sort_index(), pdf.groupby("d").apply(sum).sort_index()
            )

    def test_apply_with_side_effect(self):
        pdf = pd.DataFrame(
            {"d": [1.0, 1.0, 1.0, 2.0, 2.0, 2.0], "v": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]}
        )
        psdf = ps.from_pandas(pdf)

        acc = ps.utils.default_session().sparkContext.accumulator(0)

        def sum_with_acc_frame(x) -> ps.DataFrame[np.float64, np.float64]:
            nonlocal acc
            acc += 1
            return np.sum(x)

        actual = psdf.groupby("d").apply(sum_with_acc_frame).sort_index()
        actual.columns = ["d", "v"]
        self.assert_eq(actual, pdf.groupby("d").apply(sum).sort_index().reset_index(drop=True))
        self.assert_eq(acc.value, 2)

        def sum_with_acc_series(x) -> np.float64:
            nonlocal acc
            acc += 1
            return np.sum(x)

        self.assert_eq(
            psdf.groupby("d")["v"].apply(sum_with_acc_series).sort_index(),
            pdf.groupby("d")["v"].apply(sum).sort_index().reset_index(drop=True),
        )
        self.assert_eq(acc.value, 4)

    def test_transform(self):
        pdf = pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6], "b": [1, 1, 2, 3, 5, 8], "c": [1, 4, 9, 16, 25, 36]},
            columns=["a", "b", "c"],
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            psdf.groupby("b").transform(lambda x: x + x.min()).sort_index(),
            pdf.groupby("b").transform(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.groupby("b")["a"].transform(lambda x: x + x.min()).sort_index(),
            pdf.groupby("b")["a"].transform(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.groupby("b")[["a"]].transform(lambda x: x + x.min()).sort_index(),
            pdf.groupby("b")[["a"]].transform(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["a", "b"]).transform(lambda x: x + x.min()).sort_index(),
            pdf.groupby(["a", "b"]).transform(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["b"])["c"].transform(lambda x: x + x.min()).sort_index(),
            pdf.groupby(["b"])["c"].transform(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5).transform(lambda x: x + x.min()).sort_index(),
            pdf.groupby(pdf.b // 5).transform(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5)["a"].transform(lambda x: x + x.min()).sort_index(),
            pdf.groupby(pdf.b // 5)["a"].transform(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5)[["a"]].transform(lambda x: x + x.min()).sort_index(),
            pdf.groupby(pdf.b // 5)[["a"]].transform(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b).transform(lambda x: x + x.min()).sort_index(),
            pdf.a.rename().groupby(pdf.b).transform(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.a.groupby(psdf.b.rename()).transform(lambda x: x + x.min()).sort_index(),
            pdf.a.groupby(pdf.b.rename()).transform(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b.rename()).transform(lambda x: x + x.min()).sort_index(),
            pdf.a.rename().groupby(pdf.b.rename()).transform(lambda x: x + x.min()).sort_index(),
        )
        with self.assertRaisesRegex(TypeError, "str object is not callable"):
            psdf.groupby("a").transform("sum")

        def udf(col) -> int:
            return col + 10

        with self.assertRaisesRegex(
            TypeError,
            "Expected the return type of this function to be of Series type, "
            "but found type ScalarType\\[LongType\\]",
        ):
            psdf.groupby("a").transform(udf)

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.groupby(("x", "b")).transform(lambda x: x + x.min()).sort_index(),
            pdf.groupby(("x", "b")).transform(lambda x: x + x.min()).sort_index(),
        )
        self.assert_eq(
            psdf.groupby([("x", "a"), ("x", "b")]).transform(lambda x: x + x.min()).sort_index(),
            pdf.groupby([("x", "a"), ("x", "b")]).transform(lambda x: x + x.min()).sort_index(),
        )

    def test_transform_without_shortcut(self):
        with option_context("compute.shortcut_limit", 0):
            self.test_transform()

    def test_filter(self):
        pdf = pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6], "b": [1, 1, 2, 3, 5, 8], "c": [1, 4, 9, 16, 25, 36]},
            columns=["a", "b", "c"],
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby("b").filter(lambda x: any(x.a == 2)).sort_index(),
            pdf.groupby("b").filter(lambda x: any(x.a == 2)).sort_index(),
        )
        self.assert_eq(
            psdf.groupby("b")["a"].filter(lambda x: any(x == 2)).sort_index(),
            pdf.groupby("b")["a"].filter(lambda x: any(x == 2)).sort_index(),
        )
        self.assert_eq(
            psdf.groupby("b")[["a"]].filter(lambda x: any(x.a == 2)).sort_index(),
            pdf.groupby("b")[["a"]].filter(lambda x: any(x.a == 2)).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["a", "b"]).filter(lambda x: any(x.a == 2)).sort_index(),
            pdf.groupby(["a", "b"]).filter(lambda x: any(x.a == 2)).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf["b"] // 5).filter(lambda x: any(x.a == 2)).sort_index(),
            pdf.groupby(pdf["b"] // 5).filter(lambda x: any(x.a == 2)).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf["b"] // 5)["a"].filter(lambda x: any(x == 2)).sort_index(),
            pdf.groupby(pdf["b"] // 5)["a"].filter(lambda x: any(x == 2)).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf["b"] // 5)[["a"]].filter(lambda x: any(x.a == 2)).sort_index(),
            pdf.groupby(pdf["b"] // 5)[["a"]].filter(lambda x: any(x.a == 2)).sort_index(),
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b).filter(lambda x: any(x == 2)).sort_index(),
            pdf.a.rename().groupby(pdf.b).filter(lambda x: any(x == 2)).sort_index(),
        )
        self.assert_eq(
            psdf.a.groupby(psdf.b.rename()).filter(lambda x: any(x == 2)).sort_index(),
            pdf.a.groupby(pdf.b.rename()).filter(lambda x: any(x == 2)).sort_index(),
        )
        self.assert_eq(
            psdf.a.rename().groupby(psdf.b.rename()).filter(lambda x: any(x == 2)).sort_index(),
            pdf.a.rename().groupby(pdf.b.rename()).filter(lambda x: any(x == 2)).sort_index(),
        )

        with self.assertRaisesRegex(TypeError, "int object is not callable"):
            psdf.groupby("b").filter(1)

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.groupby(("x", "b")).filter(lambda x: any(x[("x", "a")] == 2)).sort_index(),
            pdf.groupby(("x", "b")).filter(lambda x: any(x[("x", "a")] == 2)).sort_index(),
        )
        self.assert_eq(
            psdf.groupby([("x", "a"), ("x", "b")])
            .filter(lambda x: any(x[("x", "a")] == 2))
            .sort_index(),
            pdf.groupby([("x", "a"), ("x", "b")])
            .filter(lambda x: any(x[("x", "a")] == 2))
            .sort_index(),
        )

    def test_idxmax(self):
        pdf = pd.DataFrame(
            {"a": [1, 1, 2, 2, 3] * 3, "b": [1, 2, 3, 4, 5] * 3, "c": [5, 4, 3, 2, 1] * 3}
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            pdf.groupby(["a"]).idxmax().sort_index(), psdf.groupby(["a"]).idxmax().sort_index()
        )
        self.assert_eq(
            pdf.groupby(["a"]).idxmax(skipna=False).sort_index(),
            psdf.groupby(["a"]).idxmax(skipna=False).sort_index(),
        )
        self.assert_eq(
            pdf.groupby(["a"])["b"].idxmax().sort_index(),
            psdf.groupby(["a"])["b"].idxmax().sort_index(),
        )
        self.assert_eq(
            pdf.b.rename().groupby(pdf.a).idxmax().sort_index(),
            psdf.b.rename().groupby(psdf.a).idxmax().sort_index(),
        )
        self.assert_eq(
            pdf.b.groupby(pdf.a.rename()).idxmax().sort_index(),
            psdf.b.groupby(psdf.a.rename()).idxmax().sort_index(),
        )
        self.assert_eq(
            pdf.b.rename().groupby(pdf.a.rename()).idxmax().sort_index(),
            psdf.b.rename().groupby(psdf.a.rename()).idxmax().sort_index(),
        )

        with self.assertRaisesRegex(ValueError, "idxmax only support one-level index now"):
            psdf.set_index(["a", "b"]).groupby(["c"]).idxmax()

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            pdf.groupby(("x", "a")).idxmax().sort_index(),
            psdf.groupby(("x", "a")).idxmax().sort_index(),
        )
        self.assert_eq(
            pdf.groupby(("x", "a")).idxmax(skipna=False).sort_index(),
            psdf.groupby(("x", "a")).idxmax(skipna=False).sort_index(),
        )

    def test_idxmin(self):
        pdf = pd.DataFrame(
            {"a": [1, 1, 2, 2, 3] * 3, "b": [1, 2, 3, 4, 5] * 3, "c": [5, 4, 3, 2, 1] * 3}
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            pdf.groupby(["a"]).idxmin().sort_index(), psdf.groupby(["a"]).idxmin().sort_index()
        )
        self.assert_eq(
            pdf.groupby(["a"]).idxmin(skipna=False).sort_index(),
            psdf.groupby(["a"]).idxmin(skipna=False).sort_index(),
        )
        self.assert_eq(
            pdf.groupby(["a"])["b"].idxmin().sort_index(),
            psdf.groupby(["a"])["b"].idxmin().sort_index(),
        )
        self.assert_eq(
            pdf.b.rename().groupby(pdf.a).idxmin().sort_index(),
            psdf.b.rename().groupby(psdf.a).idxmin().sort_index(),
        )
        self.assert_eq(
            pdf.b.groupby(pdf.a.rename()).idxmin().sort_index(),
            psdf.b.groupby(psdf.a.rename()).idxmin().sort_index(),
        )
        self.assert_eq(
            pdf.b.rename().groupby(pdf.a.rename()).idxmin().sort_index(),
            psdf.b.rename().groupby(psdf.a.rename()).idxmin().sort_index(),
        )

        with self.assertRaisesRegex(ValueError, "idxmin only support one-level index now"):
            psdf.set_index(["a", "b"]).groupby(["c"]).idxmin()

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            pdf.groupby(("x", "a")).idxmin().sort_index(),
            psdf.groupby(("x", "a")).idxmin().sort_index(),
        )
        self.assert_eq(
            pdf.groupby(("x", "a")).idxmin(skipna=False).sort_index(),
            psdf.groupby(("x", "a")).idxmin(skipna=False).sort_index(),
        )

    def test_head(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 1, 1, 1, 2, 2, 2, 3, 3, 3] * 3,
                "b": [2, 3, 1, 4, 6, 9, 8, 10, 7, 5] * 3,
                "c": [3, 5, 2, 5, 1, 2, 6, 4, 3, 6] * 3,
            },
            index=np.random.rand(10 * 3),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            pdf.groupby("a").head(2).sort_index(), psdf.groupby("a").head(2).sort_index()
        )
        self.assert_eq(
            pdf.groupby("a").head(-2).sort_index(), psdf.groupby("a").head(-2).sort_index()
        )
        self.assert_eq(
            pdf.groupby("a").head(100000).sort_index(), psdf.groupby("a").head(100000).sort_index()
        )

        self.assert_eq(
            pdf.groupby("a")["b"].head(2).sort_index(), psdf.groupby("a")["b"].head(2).sort_index()
        )
        self.assert_eq(
            pdf.groupby("a")["b"].head(-2).sort_index(),
            psdf.groupby("a")["b"].head(-2).sort_index(),
        )
        self.assert_eq(
            pdf.groupby("a")["b"].head(100000).sort_index(),
            psdf.groupby("a")["b"].head(100000).sort_index(),
        )

        self.assert_eq(
            pdf.groupby("a")[["b"]].head(2).sort_index(),
            psdf.groupby("a")[["b"]].head(2).sort_index(),
        )
        self.assert_eq(
            pdf.groupby("a")[["b"]].head(-2).sort_index(),
            psdf.groupby("a")[["b"]].head(-2).sort_index(),
        )
        self.assert_eq(
            pdf.groupby("a")[["b"]].head(100000).sort_index(),
            psdf.groupby("a")[["b"]].head(100000).sort_index(),
        )

        self.assert_eq(
            pdf.groupby(pdf.a // 2).head(2).sort_index(),
            psdf.groupby(psdf.a // 2).head(2).sort_index(),
        )
        self.assert_eq(
            pdf.groupby(pdf.a // 2)["b"].head(2).sort_index(),
            psdf.groupby(psdf.a // 2)["b"].head(2).sort_index(),
        )
        self.assert_eq(
            pdf.groupby(pdf.a // 2)[["b"]].head(2).sort_index(),
            psdf.groupby(psdf.a // 2)[["b"]].head(2).sort_index(),
        )

        self.assert_eq(
            pdf.b.rename().groupby(pdf.a).head(2).sort_index(),
            psdf.b.rename().groupby(psdf.a).head(2).sort_index(),
        )
        self.assert_eq(
            pdf.b.groupby(pdf.a.rename()).head(2).sort_index(),
            psdf.b.groupby(psdf.a.rename()).head(2).sort_index(),
        )
        self.assert_eq(
            pdf.b.rename().groupby(pdf.a.rename()).head(2).sort_index(),
            psdf.b.rename().groupby(psdf.a.rename()).head(2).sort_index(),
        )

        # multi-index
        midx = pd.MultiIndex(
            [["x", "y"], ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]],
            [[0, 0, 0, 0, 0, 1, 1, 1, 1, 1], [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]],
        )
        pdf = pd.DataFrame(
            {
                "a": [1, 1, 1, 1, 2, 2, 2, 3, 3, 3],
                "b": [2, 3, 1, 4, 6, 9, 8, 10, 7, 5],
                "c": [3, 5, 2, 5, 1, 2, 6, 4, 3, 6],
            },
            columns=["a", "b", "c"],
            index=midx,
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            pdf.groupby("a").head(2).sort_index(), psdf.groupby("a").head(2).sort_index()
        )
        self.assert_eq(
            pdf.groupby("a").head(-2).sort_index(), psdf.groupby("a").head(-2).sort_index()
        )
        self.assert_eq(
            pdf.groupby("a").head(100000).sort_index(), psdf.groupby("a").head(100000).sort_index()
        )

        self.assert_eq(
            pdf.groupby("a")["b"].head(2).sort_index(), psdf.groupby("a")["b"].head(2).sort_index()
        )
        self.assert_eq(
            pdf.groupby("a")["b"].head(-2).sort_index(),
            psdf.groupby("a")["b"].head(-2).sort_index(),
        )
        self.assert_eq(
            pdf.groupby("a")["b"].head(100000).sort_index(),
            psdf.groupby("a")["b"].head(100000).sort_index(),
        )

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            pdf.groupby(("x", "a")).head(2).sort_index(),
            psdf.groupby(("x", "a")).head(2).sort_index(),
        )
        self.assert_eq(
            pdf.groupby(("x", "a")).head(-2).sort_index(),
            psdf.groupby(("x", "a")).head(-2).sort_index(),
        )
        self.assert_eq(
            pdf.groupby(("x", "a")).head(100000).sort_index(),
            psdf.groupby(("x", "a")).head(100000).sort_index(),
        )

    def test_missing(self):
        psdf = ps.DataFrame({"a": [1, 2, 3, 4, 5, 6, 7, 8, 9]})

        # DataFrameGroupBy functions
        missing_functions = inspect.getmembers(
            MissingPandasLikeDataFrameGroupBy, inspect.isfunction
        )
        unsupported_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "unsupported_function"
        ]
        for name in unsupported_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "method.*GroupBy.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.groupby("a"), name)()

        deprecated_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "deprecated_function"
        ]
        for name in deprecated_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "method.*GroupBy.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.groupby("a"), name)()

        # SeriesGroupBy functions
        missing_functions = inspect.getmembers(MissingPandasLikeSeriesGroupBy, inspect.isfunction)
        unsupported_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "unsupported_function"
        ]
        for name in unsupported_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "method.*GroupBy.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.a.groupby(psdf.a), name)()

        deprecated_functions = [
            name for (name, type_) in missing_functions if type_.__name__ == "deprecated_function"
        ]
        for name in deprecated_functions:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "method.*GroupBy.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.a.groupby(psdf.a), name)()

        # DataFrameGroupBy properties
        missing_properties = inspect.getmembers(
            MissingPandasLikeDataFrameGroupBy, lambda o: isinstance(o, property)
        )
        unsupported_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "unsupported_property"
        ]
        for name in unsupported_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "property.*GroupBy.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.groupby("a"), name)
        deprecated_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "deprecated_property"
        ]
        for name in deprecated_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "property.*GroupBy.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.groupby("a"), name)

        # SeriesGroupBy properties
        missing_properties = inspect.getmembers(
            MissingPandasLikeSeriesGroupBy, lambda o: isinstance(o, property)
        )
        unsupported_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "unsupported_property"
        ]
        for name in unsupported_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError,
                "property.*GroupBy.*{}.*not implemented( yet\\.|\\. .+)".format(name),
            ):
                getattr(psdf.a.groupby(psdf.a), name)
        deprecated_properties = [
            name
            for (name, type_) in missing_properties
            if type_.fget.__name__ == "deprecated_property"
        ]
        for name in deprecated_properties:
            with self.assertRaisesRegex(
                PandasNotImplementedError, "property.*GroupBy.*{}.*is deprecated".format(name)
            ):
                getattr(psdf.a.groupby(psdf.a), name)

    @staticmethod
    def test_is_multi_agg_with_relabel():

        assert is_multi_agg_with_relabel(a="max") is False
        assert is_multi_agg_with_relabel(a_min=("a", "max"), a_max=("a", "min")) is True

    def test_get_group(self):
        pdf = pd.DataFrame(
            [
                ("falcon", "bird", 389.0),
                ("parrot", "bird", 24.0),
                ("lion", "mammal", 80.5),
                ("monkey", "mammal", np.nan),
            ],
            columns=["name", "class", "max_speed"],
            index=[0, 2, 3, 1],
        )
        pdf.columns.name = "Koalas"
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            psdf.groupby("class").get_group("bird"),
            pdf.groupby("class").get_group("bird"),
        )
        self.assert_eq(
            psdf.groupby("class")["name"].get_group("mammal"),
            pdf.groupby("class")["name"].get_group("mammal"),
        )
        self.assert_eq(
            psdf.groupby("class")[["name"]].get_group("mammal"),
            pdf.groupby("class")[["name"]].get_group("mammal"),
        )
        self.assert_eq(
            psdf.groupby(["class", "name"]).get_group(("mammal", "lion")),
            pdf.groupby(["class", "name"]).get_group(("mammal", "lion")),
        )
        self.assert_eq(
            psdf.groupby(["class", "name"])["max_speed"].get_group(("mammal", "lion")),
            pdf.groupby(["class", "name"])["max_speed"].get_group(("mammal", "lion")),
        )
        self.assert_eq(
            psdf.groupby(["class", "name"])[["max_speed"]].get_group(("mammal", "lion")),
            pdf.groupby(["class", "name"])[["max_speed"]].get_group(("mammal", "lion")),
        )
        self.assert_eq(
            (psdf.max_speed + 1).groupby(psdf["class"]).get_group("mammal"),
            (pdf.max_speed + 1).groupby(pdf["class"]).get_group("mammal"),
        )
        self.assert_eq(
            psdf.groupby("max_speed").get_group(80.5),
            pdf.groupby("max_speed").get_group(80.5),
        )

        self.assertRaises(KeyError, lambda: psdf.groupby("class").get_group("fish"))
        self.assertRaises(TypeError, lambda: psdf.groupby("class").get_group(["bird", "mammal"]))
        self.assertRaises(KeyError, lambda: psdf.groupby("class")["name"].get_group("fish"))
        self.assertRaises(
            TypeError, lambda: psdf.groupby("class")["name"].get_group(["bird", "mammal"])
        )
        self.assertRaises(
            KeyError, lambda: psdf.groupby(["class", "name"]).get_group(("lion", "mammal"))
        )
        self.assertRaises(ValueError, lambda: psdf.groupby(["class", "name"]).get_group(("lion",)))
        self.assertRaises(
            ValueError, lambda: psdf.groupby(["class", "name"]).get_group(("mammal",))
        )
        self.assertRaises(ValueError, lambda: psdf.groupby(["class", "name"]).get_group("mammal"))

        # MultiIndex columns
        pdf.columns = pd.MultiIndex.from_tuples([("A", "name"), ("B", "class"), ("C", "max_speed")])
        pdf.columns.names = ["Hello", "Koalas"]
        psdf = ps.from_pandas(pdf)
        self.assert_eq(
            psdf.groupby(("B", "class")).get_group("bird"),
            pdf.groupby(("B", "class")).get_group("bird"),
        )
        self.assert_eq(
            psdf.groupby(("B", "class"))[[("A", "name")]].get_group("mammal"),
            pdf.groupby(("B", "class"))[[("A", "name")]].get_group("mammal"),
        )
        self.assert_eq(
            psdf.groupby([("B", "class"), ("A", "name")]).get_group(("mammal", "lion")),
            pdf.groupby([("B", "class"), ("A", "name")]).get_group(("mammal", "lion")),
        )
        self.assert_eq(
            psdf.groupby([("B", "class"), ("A", "name")])[[("C", "max_speed")]].get_group(
                ("mammal", "lion")
            ),
            pdf.groupby([("B", "class"), ("A", "name")])[[("C", "max_speed")]].get_group(
                ("mammal", "lion")
            ),
        )
        self.assert_eq(
            (psdf[("C", "max_speed")] + 1).groupby(psdf[("B", "class")]).get_group("mammal"),
            (pdf[("C", "max_speed")] + 1).groupby(pdf[("B", "class")]).get_group("mammal"),
        )
        self.assert_eq(
            psdf.groupby(("C", "max_speed")).get_group(80.5),
            pdf.groupby(("C", "max_speed")).get_group(80.5),
        )

        self.assertRaises(KeyError, lambda: psdf.groupby(("B", "class")).get_group("fish"))
        self.assertRaises(
            TypeError, lambda: psdf.groupby(("B", "class")).get_group(["bird", "mammal"])
        )
        self.assertRaises(
            KeyError, lambda: psdf.groupby(("B", "class"))[("A", "name")].get_group("fish")
        )
        self.assertRaises(
            KeyError,
            lambda: psdf.groupby([("B", "class"), ("A", "name")]).get_group(("lion", "mammal")),
        )
        self.assertRaises(
            ValueError,
            lambda: psdf.groupby([("B", "class"), ("A", "name")]).get_group(("lion",)),
        )
        self.assertRaises(
            ValueError, lambda: psdf.groupby([("B", "class"), ("A", "name")]).get_group(("mammal",))
        )
        self.assertRaises(
            ValueError, lambda: psdf.groupby([("B", "class"), ("A", "name")]).get_group("mammal")
        )

    def test_median(self):
        psdf = ps.DataFrame(
            {
                "a": [1.0, 1.0, 1.0, 1.0, 2.0, 2.0, 2.0, 3.0, 3.0, 3.0],
                "b": [2.0, 3.0, 1.0, 4.0, 6.0, 9.0, 8.0, 10.0, 7.0, 5.0],
                "c": [3.0, 5.0, 2.0, 5.0, 1.0, 2.0, 6.0, 4.0, 3.0, 6.0],
            },
            columns=["a", "b", "c"],
            index=[7, 2, 4, 1, 3, 4, 9, 10, 5, 6],
        )
        # DataFrame
        expected_result = ps.DataFrame(
            {"b": [2.0, 8.0, 7.0], "c": [3.0, 2.0, 4.0]}, index=pd.Index([1.0, 2.0, 3.0], name="a")
        )
        self.assert_eq(expected_result, psdf.groupby("a").median().sort_index())
        # Series
        expected_result = ps.Series(
            [2.0, 8.0, 7.0], name="b", index=pd.Index([1.0, 2.0, 3.0], name="a")
        )
        self.assert_eq(expected_result, psdf.groupby("a")["b"].median().sort_index())

        with self.assertRaisesRegex(TypeError, "accuracy must be an integer; however"):
            psdf.groupby("a").median(accuracy="a")

    def test_tail(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 1, 1, 1, 2, 2, 2, 3, 3, 3] * 3,
                "b": [2, 3, 1, 4, 6, 9, 8, 10, 7, 5] * 3,
                "c": [3, 5, 2, 5, 1, 2, 6, 4, 3, 6] * 3,
            },
            index=np.random.rand(10 * 3),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            pdf.groupby("a").tail(2).sort_index(), psdf.groupby("a").tail(2).sort_index()
        )
        self.assert_eq(
            pdf.groupby("a").tail(-2).sort_index(), psdf.groupby("a").tail(-2).sort_index()
        )
        self.assert_eq(
            pdf.groupby("a").tail(100000).sort_index(), psdf.groupby("a").tail(100000).sort_index()
        )

        self.assert_eq(
            pdf.groupby("a")["b"].tail(2).sort_index(), psdf.groupby("a")["b"].tail(2).sort_index()
        )
        self.assert_eq(
            pdf.groupby("a")["b"].tail(-2).sort_index(),
            psdf.groupby("a")["b"].tail(-2).sort_index(),
        )
        self.assert_eq(
            pdf.groupby("a")["b"].tail(100000).sort_index(),
            psdf.groupby("a")["b"].tail(100000).sort_index(),
        )

        self.assert_eq(
            pdf.groupby("a")[["b"]].tail(2).sort_index(),
            psdf.groupby("a")[["b"]].tail(2).sort_index(),
        )
        self.assert_eq(
            pdf.groupby("a")[["b"]].tail(-2).sort_index(),
            psdf.groupby("a")[["b"]].tail(-2).sort_index(),
        )
        self.assert_eq(
            pdf.groupby("a")[["b"]].tail(100000).sort_index(),
            psdf.groupby("a")[["b"]].tail(100000).sort_index(),
        )

        self.assert_eq(
            pdf.groupby(pdf.a // 2).tail(2).sort_index(),
            psdf.groupby(psdf.a // 2).tail(2).sort_index(),
        )
        self.assert_eq(
            pdf.groupby(pdf.a // 2)["b"].tail(2).sort_index(),
            psdf.groupby(psdf.a // 2)["b"].tail(2).sort_index(),
        )
        self.assert_eq(
            pdf.groupby(pdf.a // 2)[["b"]].tail(2).sort_index(),
            psdf.groupby(psdf.a // 2)[["b"]].tail(2).sort_index(),
        )

        self.assert_eq(
            pdf.b.rename().groupby(pdf.a).tail(2).sort_index(),
            psdf.b.rename().groupby(psdf.a).tail(2).sort_index(),
        )
        self.assert_eq(
            pdf.b.groupby(pdf.a.rename()).tail(2).sort_index(),
            psdf.b.groupby(psdf.a.rename()).tail(2).sort_index(),
        )
        self.assert_eq(
            pdf.b.rename().groupby(pdf.a.rename()).tail(2).sort_index(),
            psdf.b.rename().groupby(psdf.a.rename()).tail(2).sort_index(),
        )

        # multi-index
        midx = pd.MultiIndex(
            [["x", "y"], ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]],
            [[0, 0, 0, 0, 0, 1, 1, 1, 1, 1], [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]],
        )
        pdf = pd.DataFrame(
            {
                "a": [1, 1, 1, 1, 2, 2, 2, 3, 3, 3],
                "b": [2, 3, 1, 4, 6, 9, 8, 10, 7, 5],
                "c": [3, 5, 2, 5, 1, 2, 6, 4, 3, 6],
            },
            columns=["a", "b", "c"],
            index=midx,
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            pdf.groupby("a").tail(2).sort_index(), psdf.groupby("a").tail(2).sort_index()
        )
        self.assert_eq(
            pdf.groupby("a").tail(-2).sort_index(), psdf.groupby("a").tail(-2).sort_index()
        )
        self.assert_eq(
            pdf.groupby("a").tail(100000).sort_index(), psdf.groupby("a").tail(100000).sort_index()
        )

        self.assert_eq(
            pdf.groupby("a")["b"].tail(2).sort_index(), psdf.groupby("a")["b"].tail(2).sort_index()
        )
        self.assert_eq(
            pdf.groupby("a")["b"].tail(-2).sort_index(),
            psdf.groupby("a")["b"].tail(-2).sort_index(),
        )
        self.assert_eq(
            pdf.groupby("a")["b"].tail(100000).sort_index(),
            psdf.groupby("a")["b"].tail(100000).sort_index(),
        )

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            pdf.groupby(("x", "a")).tail(2).sort_index(),
            psdf.groupby(("x", "a")).tail(2).sort_index(),
        )
        self.assert_eq(
            pdf.groupby(("x", "a")).tail(-2).sort_index(),
            psdf.groupby(("x", "a")).tail(-2).sort_index(),
        )
        self.assert_eq(
            pdf.groupby(("x", "a")).tail(100000).sort_index(),
            psdf.groupby(("x", "a")).tail(100000).sort_index(),
        )

    def test_ddof(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 1, 1, 1, 2, 2, 2, 3, 3, 3] * 3,
                "b": [2, 3, 1, 4, 6, 9, 8, 10, 7, 5] * 3,
                "c": [3, 5, 2, 5, 1, 2, 6, 4, 3, 6] * 3,
            },
            index=np.random.rand(10 * 3),
        )
        psdf = ps.from_pandas(pdf)

        for ddof in (0, 1):
            # std
            self.assert_eq(
                pdf.groupby("a").std(ddof=ddof).sort_index(),
                psdf.groupby("a").std(ddof=ddof).sort_index(),
                check_exact=False,
            )
            self.assert_eq(
                pdf.groupby("a")["b"].std(ddof=ddof).sort_index(),
                psdf.groupby("a")["b"].std(ddof=ddof).sort_index(),
                check_exact=False,
            )
            # var
            self.assert_eq(
                pdf.groupby("a").var(ddof=ddof).sort_index(),
                psdf.groupby("a").var(ddof=ddof).sort_index(),
                check_exact=False,
            )
            self.assert_eq(
                pdf.groupby("a")["b"].var(ddof=ddof).sort_index(),
                psdf.groupby("a")["b"].var(ddof=ddof).sort_index(),
                check_exact=False,
            )

    def test_getitem(self):
        psdf = ps.DataFrame(
            {
                "a": [1, 1, 1, 1, 2, 2, 2, 3, 3, 3] * 3,
                "b": [2, 3, 1, 4, 6, 9, 8, 10, 7, 5] * 3,
                "c": [3, 5, 2, 5, 1, 2, 6, 4, 3, 6] * 3,
            },
            index=np.random.rand(10 * 3),
        )

        self.assertTrue(isinstance(psdf.groupby("a")["b"], SeriesGroupBy))


if __name__ == "__main__":
    from pyspark.pandas.tests.test_groupby import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
