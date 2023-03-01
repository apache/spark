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

import numpy as np
import pandas as pd
from pandas.api.types import CategoricalDtype

import pyspark.pandas as ps
from pyspark.testing.pandasutils import ComparisonTestBase, TestUtils


class CategoricalTest(ComparisonTestBase, TestUtils):
    @property
    def pdf(self):
        return pd.DataFrame(
            {
                "a": pd.Categorical([1, 2, 3, 1, 2, 3]),
                "b": pd.Categorical(
                    ["b", "a", "c", "c", "b", "a"], categories=["c", "b", "d", "a"]
                ),
            },
        )

    @property
    def df_pair(self):
        return self.pdf, self.psdf

    def test_categorical_frame(self):
        pdf, psdf = self.df_pair

        self.assert_eq(psdf, pdf)
        self.assert_eq(psdf.a, pdf.a)
        self.assert_eq(psdf.b, pdf.b)
        self.assert_eq(psdf.index, pdf.index)

        self.assert_eq(psdf.sort_index(), pdf.sort_index())
        self.assert_eq(psdf.sort_values("b"), pdf.sort_values("b"))

    def test_categorical_series(self):
        pser = pd.Series([1, 2, 3], dtype="category")
        psser = ps.Series([1, 2, 3], dtype="category")

        self.assert_eq(psser, pser)
        self.assert_eq(psser.cat.categories, pser.cat.categories)
        self.assert_eq(psser.cat.codes, pser.cat.codes)
        self.assert_eq(psser.cat.ordered, pser.cat.ordered)

        with self.assertRaisesRegex(ValueError, "Cannot call CategoricalAccessor on type int64"):
            ps.Series([1, 2, 3]).cat

    def test_categories_setter(self):
        pdf, psdf = self.df_pair

        pser = pdf.a
        psser = psdf.a

        pser.cat.categories = ["z", "y", "x"]
        psser.cat.categories = ["z", "y", "x"]
        if LooseVersion(pd.__version__) >= LooseVersion("1.3"):
            # Bug in pandas 1.3. dtype is not updated properly with `inplace` argument.
            pser = pser.astype(CategoricalDtype(categories=["x", "y", "z"]))

        self.assert_eq(pser, psser)
        self.assert_eq(pdf, psdf)

        with self.assertRaises(ValueError):
            psser.cat.categories = [1, 2, 3, 4]

    def test_add_categories(self):
        pdf, psdf = self.df_pair

        pser = pdf.a
        psser = psdf.a

        self.assert_eq(pser.cat.add_categories(4), psser.cat.add_categories(4))
        self.assert_eq(pser.cat.add_categories([4, 5]), psser.cat.add_categories([4, 5]))
        self.assert_eq(pser.cat.add_categories([]), psser.cat.add_categories([]))

        pser = pser.cat.add_categories(4)
        psser = psser.cat.add_categories(4)

        self.assertRaises(ValueError, lambda: psser.cat.add_categories(4))
        self.assertRaises(ValueError, lambda: psser.cat.add_categories([5, 5]))

    def test_remove_categories(self):
        pdf, psdf = self.df_pair

        pser = pdf.a
        psser = psdf.a

        self.assert_eq(pser.cat.remove_categories(2), psser.cat.remove_categories(2))
        self.assert_eq(pser.cat.remove_categories([1, 3]), psser.cat.remove_categories([1, 3]))
        self.assert_eq(pser.cat.remove_categories([]), psser.cat.remove_categories([]))
        self.assert_eq(pser.cat.remove_categories([2, 2]), psser.cat.remove_categories([2, 2]))
        self.assert_eq(
            pser.cat.remove_categories([1, 2, 3]), psser.cat.remove_categories([1, 2, 3])
        )
        self.assert_eq(pser.cat.remove_categories(None), psser.cat.remove_categories(None))
        self.assert_eq(pser.cat.remove_categories([None]), psser.cat.remove_categories([None]))

        self.assertRaises(ValueError, lambda: psser.cat.remove_categories(4))
        self.assertRaises(ValueError, lambda: psser.cat.remove_categories([4, None]))

    def test_remove_unused_categories(self):
        pdf, psdf = self.df_pair

        pser = pdf.a
        psser = psdf.a

        self.assert_eq(pser.cat.remove_unused_categories(), psser.cat.remove_unused_categories())

        pser = pser.cat.add_categories(4)
        pser = pser.cat.remove_categories(2)
        psser = psser.cat.add_categories(4)
        psser = psser.cat.remove_categories(2)

        self.assert_eq(pser.cat.remove_unused_categories(), psser.cat.remove_unused_categories())

    def test_reorder_categories(self):
        pdf, psdf = self.df_pair

        pser = pdf.a
        psser = psdf.a

        self.assert_eq(
            pser.cat.reorder_categories([1, 2, 3]), psser.cat.reorder_categories([1, 2, 3])
        )
        self.assert_eq(
            pser.cat.reorder_categories([1, 2, 3], ordered=True),
            psser.cat.reorder_categories([1, 2, 3], ordered=True),
        )
        self.assert_eq(
            pser.cat.reorder_categories([3, 2, 1]), psser.cat.reorder_categories([3, 2, 1])
        )
        self.assert_eq(
            pser.cat.reorder_categories([3, 2, 1], ordered=True),
            psser.cat.reorder_categories([3, 2, 1], ordered=True),
        )

        self.assertRaises(ValueError, lambda: psser.cat.reorder_categories([1, 2]))
        self.assertRaises(ValueError, lambda: psser.cat.reorder_categories([1, 2, 4]))
        self.assertRaises(ValueError, lambda: psser.cat.reorder_categories([1, 2, 2]))
        self.assertRaises(TypeError, lambda: psser.cat.reorder_categories(1))
        self.assertRaises(TypeError, lambda: psdf.b.cat.reorder_categories("abcd"))

    def test_as_ordered_unordered(self):
        pdf, psdf = self.df_pair

        pser = pdf.a
        psser = psdf.a

        # as_ordered
        self.assert_eq(pser.cat.as_ordered(), psser.cat.as_ordered())

        pser.cat.as_ordered(inplace=True)
        psser.cat.as_ordered(inplace=True)
        if LooseVersion(pd.__version__) >= LooseVersion("1.3"):
            # Bug in pandas 1.3. dtype is not updated properly with `inplace` argument.
            pser = pser.astype(CategoricalDtype(categories=[1, 2, 3], ordered=True))

        self.assert_eq(pser, psser)
        self.assert_eq(pdf, psdf)

        # as_unordered
        self.assert_eq(pser.cat.as_unordered(), psser.cat.as_unordered())

        pser.cat.as_unordered(inplace=True)
        psser.cat.as_unordered(inplace=True)
        if LooseVersion(pd.__version__) >= LooseVersion("1.3"):
            # Bug in pandas 1.3. dtype is not updated properly with `inplace` argument.
            pser = pser.astype(CategoricalDtype(categories=[1, 2, 3], ordered=False))
            pdf.a = pser

        self.assert_eq(pser, psser)
        self.assert_eq(pdf, psdf)

    def test_astype(self):
        pser = pd.Series(["a", "b", "c"])
        psser = ps.from_pandas(pser)

        self.assert_eq(psser.astype("category"), pser.astype("category"))
        self.assert_eq(
            psser.astype(CategoricalDtype(["c", "a", "b"])),
            pser.astype(CategoricalDtype(["c", "a", "b"])),
        )

        pcser = pser.astype(CategoricalDtype(["c", "a", "b"]))
        pscser = psser.astype(CategoricalDtype(["c", "a", "b"]))

        self.assert_eq(pscser.astype("category"), pcser.astype("category"))

        # CategoricalDtype is not updated if the dtype is same from pandas 1.3.
        if LooseVersion(pd.__version__) >= LooseVersion("1.3"):
            self.assert_eq(
                pscser.astype(CategoricalDtype(["b", "c", "a"])),
                pcser.astype(CategoricalDtype(["b", "c", "a"])),
            )
        else:
            self.assert_eq(
                pscser.astype(CategoricalDtype(["b", "c", "a"])),
                pcser,
            )

        self.assert_eq(pscser.astype(str), pcser.astype(str))

    def test_factorize(self):
        pser = pd.Series(["a", "b", "c", None], dtype=CategoricalDtype(["c", "a", "d", "b"]))
        psser = ps.from_pandas(pser)

        pcodes, puniques = pser.factorize()
        kcodes, kuniques = psser.factorize()

        self.assert_eq(kcodes.tolist(), pcodes.tolist())
        self.assert_eq(kuniques, puniques)

        pcodes, puniques = pser.factorize(na_sentinel=-2)
        kcodes, kuniques = psser.factorize(na_sentinel=-2)

        self.assert_eq(kcodes.tolist(), pcodes.tolist())
        self.assert_eq(kuniques, puniques)

    def test_frame_apply(self):
        pdf, psdf = self.df_pair

        self.assert_eq(psdf.apply(lambda x: x).sort_index(), pdf.apply(lambda x: x).sort_index())
        self.assert_eq(
            psdf.apply(lambda x: x, axis=1).sort_index(),
            pdf.apply(lambda x: x, axis=1).sort_index(),
        )

    def test_frame_apply_without_shortcut(self):
        with ps.option_context("compute.shortcut_limit", 0):
            self.test_frame_apply()

        pdf = pd.DataFrame(
            {"a": ["a", "b", "c", "a", "b", "c"], "b": ["b", "a", "c", "c", "b", "a"]}
        )
        psdf = ps.from_pandas(pdf)

        dtype = CategoricalDtype(categories=["a", "b", "c"])

        def categorize(ser) -> ps.Series[dtype]:
            return ser.astype(dtype)

        self.assert_eq(
            psdf.apply(categorize).sort_values(["a", "b"]).reset_index(drop=True),
            pdf.apply(categorize).sort_values(["a", "b"]).reset_index(drop=True),
        )

    def test_frame_transform(self):
        pdf, psdf = self.df_pair

        self.assert_eq(psdf.transform(lambda x: x), pdf.transform(lambda x: x))
        self.assert_eq(psdf.transform(lambda x: x.cat.codes), pdf.transform(lambda x: x.cat.codes))

        pdf = pd.DataFrame(
            {"a": ["a", "b", "c", "a", "b", "c"], "b": ["b", "a", "c", "c", "b", "a"]}
        )
        psdf = ps.from_pandas(pdf)

        dtype = CategoricalDtype(categories=["a", "b", "c", "d"])

        self.assert_eq(
            psdf.transform(lambda x: x.astype(dtype)).sort_index(),
            pdf.transform(lambda x: x.astype(dtype)).sort_index(),
        )

    def test_frame_transform_without_shortcut(self):
        with ps.option_context("compute.shortcut_limit", 0):
            self.test_frame_transform()

        pdf, psdf = self.df_pair

        def codes(pser) -> ps.Series[np.int8]:
            return pser.cat.codes

        self.assert_eq(psdf.transform(codes), pdf.transform(codes))

        pdf = pd.DataFrame(
            {"a": ["a", "b", "c", "a", "b", "c"], "b": ["b", "a", "c", "c", "b", "a"]}
        )
        psdf = ps.from_pandas(pdf)

        dtype = CategoricalDtype(categories=["a", "b", "c", "d"])

        def to_category(pser) -> ps.Series[dtype]:
            return pser.astype(dtype)

        self.assert_eq(
            psdf.transform(to_category).sort_index(), pdf.transform(to_category).sort_index()
        )

    def test_series_apply(self):
        pdf, psdf = self.df_pair

        self.assert_eq(
            psdf.a.apply(lambda x: x).sort_index(), pdf.a.apply(lambda x: x).sort_index()
        )

    def test_series_apply_without_shortcut(self):
        with ps.option_context("compute.shortcut_limit", 0):
            self.test_series_apply()

        pdf, psdf = self.df_pair
        ret = psdf.a.dtype

        def identity(pser) -> ret:
            return pser

        self.assert_eq(psdf.a.apply(identity).sort_index(), pdf.a.apply(identity).sort_index())

        # TODO: The return type is still category.
        # def to_str(x) -> str:
        #     return str(x)
        #
        # self.assert_eq(
        #     psdf.a.apply(to_str).sort_index(), pdf.a.apply(to_str).sort_index()
        # )

    def test_groupby_apply(self):
        pdf, psdf = self.df_pair

        self.assert_eq(
            psdf.groupby("a").apply(lambda df: df).sort_index(),
            pdf.groupby("a").apply(lambda df: df).sort_index(),
        )
        self.assert_eq(
            psdf.groupby("b").apply(lambda df: df[["a"]]).sort_index(),
            pdf.groupby("b").apply(lambda df: df[["a"]]).sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["a", "b"]).apply(lambda df: df).sort_index(),
            pdf.groupby(["a", "b"]).apply(lambda df: df).sort_index(),
        )
        self.assert_eq(
            psdf.groupby("a").apply(lambda df: df.b.cat.codes).sort_index(),
            pdf.groupby("a").apply(lambda df: df.b.cat.codes).sort_index(),
        )
        self.assert_eq(
            psdf.groupby("a")["b"].apply(lambda b: b.cat.codes).sort_index(),
            pdf.groupby("a")["b"].apply(lambda b: b.cat.codes).sort_index(),
        )

        # TODO: grouping by a categorical type sometimes preserves unused categories.
        # self.assert_eq(
        #     psdf.groupby("a").apply(len).sort_index(), pdf.groupby("a").apply(len).sort_index(),
        # )

    def test_groupby_apply_without_shortcut(self):
        with ps.option_context("compute.shortcut_limit", 0):
            self.test_groupby_apply()

        pdf, psdf = self.df_pair

        def identity(df) -> ps.DataFrame[zip(psdf.columns, psdf.dtypes)]:
            return df

        self.assert_eq(
            psdf.groupby("a").apply(identity).sort_values(["a", "b"]).reset_index(drop=True),
            pdf.groupby("a").apply(identity).sort_values(["a", "b"]).reset_index(drop=True),
        )

    def test_groupby_transform(self):
        pdf, psdf = self.df_pair

        self.assert_eq(
            psdf.groupby("a").transform(lambda x: x).sort_index(),
            pdf.groupby("a").transform(lambda x: x).sort_index(),
        )

        dtype = CategoricalDtype(categories=["a", "b", "c", "d"])

        self.assert_eq(
            psdf.groupby("a").transform(lambda x: x.astype(dtype)).sort_index(),
            pdf.groupby("a").transform(lambda x: x.astype(dtype)).sort_index(),
        )

    def test_groupby_transform_without_shortcut(self):
        with ps.option_context("compute.shortcut_limit", 0):
            self.test_groupby_transform()

        pdf, psdf = self.df_pair

        def identity(x) -> ps.Series[psdf.b.dtype]:
            return x

        self.assert_eq(
            psdf.groupby("a").transform(identity).sort_values("b").reset_index(drop=True),
            pdf.groupby("a").transform(identity).sort_values("b").reset_index(drop=True),
        )

        dtype = CategoricalDtype(categories=["a", "b", "c", "d"])

        # The behavior for CategoricalDtype is changed from pandas 1.3
        if LooseVersion(pd.__version__) >= LooseVersion("1.3"):
            ret_dtype = pdf.b.dtype
        else:
            ret_dtype = dtype

        def astype(x) -> ps.Series[ret_dtype]:
            return x.astype(dtype)

        if LooseVersion(pd.__version__) >= LooseVersion("1.2"):
            self.assert_eq(
                psdf.groupby("a").transform(astype).sort_values("b").reset_index(drop=True),
                pdf.groupby("a").transform(astype).sort_values("b").reset_index(drop=True),
            )
        else:
            expected = pdf.groupby("a").transform(astype)
            expected["b"] = dtype.categories.take(expected["b"].cat.codes).astype(dtype)
            self.assert_eq(
                psdf.groupby("a").transform(astype).sort_values("b").reset_index(drop=True),
                expected.sort_values("b").reset_index(drop=True),
            )

    def test_frame_apply_batch(self):
        pdf, psdf = self.df_pair

        self.assert_eq(
            psdf.pandas_on_spark.apply_batch(lambda pdf: pdf.astype(str)).sort_index(),
            pdf.astype(str).sort_index(),
        )

        pdf = pd.DataFrame(
            {"a": ["a", "b", "c", "a", "b", "c"], "b": ["b", "a", "c", "c", "b", "a"]}
        )
        psdf = ps.from_pandas(pdf)

        dtype = CategoricalDtype(categories=["a", "b", "c", "d"])

        self.assert_eq(
            psdf.pandas_on_spark.apply_batch(lambda pdf: pdf.astype(dtype)).sort_index(),
            pdf.astype(dtype).sort_index(),
        )

    def test_frame_apply_batch_without_shortcut(self):
        with ps.option_context("compute.shortcut_limit", 0):
            self.test_frame_apply_batch()

        pdf, psdf = self.df_pair

        def to_str(pdf) -> 'ps.DataFrame["a":str, "b":str]':  # noqa: F405
            return pdf.astype(str)

        self.assert_eq(
            psdf.pandas_on_spark.apply_batch(to_str).sort_values(["a", "b"]).reset_index(drop=True),
            to_str(pdf).sort_values(["a", "b"]).reset_index(drop=True),
        )

        pdf = pd.DataFrame(
            {"a": ["a", "b", "c", "a", "b", "c"], "b": ["b", "a", "c", "c", "b", "a"]}
        )
        psdf = ps.from_pandas(pdf)

        dtype = CategoricalDtype(categories=["a", "b", "c", "d"])
        ret = ps.DataFrame["a":dtype, "b":dtype]

        def to_category(pdf) -> ret:
            return pdf.astype(dtype)

        self.assert_eq(
            psdf.pandas_on_spark.apply_batch(to_category)
            .sort_values(["a", "b"])
            .reset_index(drop=True),
            to_category(pdf).sort_values(["a", "b"]).reset_index(drop=True),
        )

    def test_frame_transform_batch(self):
        pdf, psdf = self.df_pair

        self.assert_eq(
            psdf.pandas_on_spark.transform_batch(lambda pdf: pdf.astype(str)).sort_index(),
            pdf.astype(str).sort_index(),
        )
        self.assert_eq(
            psdf.pandas_on_spark.transform_batch(lambda pdf: pdf.b.cat.codes).sort_index(),
            pdf.b.cat.codes.sort_index(),
        )

        pdf = pd.DataFrame(
            {"a": ["a", "b", "c", "a", "b", "c"], "b": ["b", "a", "c", "c", "b", "a"]}
        )
        psdf = ps.from_pandas(pdf)

        dtype = CategoricalDtype(categories=["a", "b", "c", "d"])

        self.assert_eq(
            psdf.pandas_on_spark.transform_batch(lambda pdf: pdf.astype(dtype)).sort_index(),
            pdf.astype(dtype).sort_index(),
        )
        self.assert_eq(
            psdf.pandas_on_spark.transform_batch(lambda pdf: pdf.b.astype(dtype)).sort_index(),
            pdf.b.astype(dtype).sort_index(),
        )

    def test_frame_transform_batch_without_shortcut(self):
        with ps.option_context("compute.shortcut_limit", 0):
            self.test_frame_transform_batch()

        pdf, psdf = self.df_pair

        def to_str(pdf) -> 'ps.DataFrame["a":str, "b":str]':  # noqa: F405
            return pdf.astype(str)

        self.assert_eq(
            psdf.pandas_on_spark.transform_batch(to_str).sort_index(),
            to_str(pdf).sort_index(),
        )

        def to_codes(pdf) -> ps.Series[np.int8]:
            return pdf.b.cat.codes

        self.assert_eq(
            psdf.pandas_on_spark.transform_batch(to_codes).sort_index(),
            to_codes(pdf).sort_index(),
        )

        pdf = pd.DataFrame(
            {"a": ["a", "b", "c", "a", "b", "c"], "b": ["b", "a", "c", "c", "b", "a"]}
        )
        psdf = ps.from_pandas(pdf)

        dtype = CategoricalDtype(categories=["a", "b", "c", "d"])
        ret = ps.DataFrame["a":dtype, "b":dtype]

        def to_category(pdf) -> ret:
            return pdf.astype(dtype)

        self.assert_eq(
            psdf.pandas_on_spark.transform_batch(to_category).sort_index(),
            to_category(pdf).sort_index(),
        )

        def to_category(pdf) -> ps.Series[dtype]:
            return pdf.b.astype(dtype)

        self.assert_eq(
            psdf.pandas_on_spark.transform_batch(to_category).sort_index(),
            to_category(pdf).rename().sort_index(),
        )

    def test_series_transform_batch(self):
        pdf, psdf = self.df_pair

        self.assert_eq(
            psdf.a.pandas_on_spark.transform_batch(lambda pser: pser.astype(str)).sort_index(),
            pdf.a.astype(str).sort_index(),
        )

        pdf = pd.DataFrame(
            {"a": ["a", "b", "c", "a", "b", "c"], "b": ["b", "a", "c", "c", "b", "a"]}
        )
        psdf = ps.from_pandas(pdf)

        dtype = CategoricalDtype(categories=["a", "b", "c", "d"])

        self.assert_eq(
            psdf.a.pandas_on_spark.transform_batch(lambda pser: pser.astype(dtype)).sort_index(),
            pdf.a.astype(dtype).sort_index(),
        )

    def test_series_transform_batch_without_shortcut(self):
        with ps.option_context("compute.shortcut_limit", 0):
            self.test_series_transform_batch()

        pdf, psdf = self.df_pair

        def to_str(pser) -> ps.Series[str]:
            return pser.astype(str)

        self.assert_eq(
            psdf.a.pandas_on_spark.transform_batch(to_str).sort_index(), to_str(pdf.a).sort_index()
        )

        pdf = pd.DataFrame(
            {"a": ["a", "b", "c", "a", "b", "c"], "b": ["b", "a", "c", "c", "b", "a"]}
        )
        psdf = ps.from_pandas(pdf)

        dtype = CategoricalDtype(categories=["a", "b", "c", "d"])

        def to_category(pser) -> ps.Series[dtype]:
            return pser.astype(dtype)

        self.assert_eq(
            psdf.a.pandas_on_spark.transform_batch(to_category).sort_index(),
            to_category(pdf.a).sort_index(),
        )

    def test_unstack(self):
        pdf = self.pdf
        index = pd.MultiIndex.from_tuples(
            [("x", "a"), ("x", "b"), ("x", "c"), ("y", "a"), ("y", "b"), ("y", "d")]
        )
        pdf.index = index
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.a.unstack().sort_index(), pdf.a.unstack().sort_index())
        self.assert_eq(psdf.b.unstack().sort_index(), pdf.b.unstack().sort_index())

    def test_rename_categories(self):
        pdf, psdf = self.df_pair

        pser = pdf.b
        psser = psdf.b

        self.assert_eq(
            pser.cat.rename_categories([0, 1, 3, 2]), psser.cat.rename_categories([0, 1, 3, 2])
        )
        self.assert_eq(
            pser.cat.rename_categories({"a": "A", "c": "C"}),
            psser.cat.rename_categories({"a": "A", "c": "C"}),
        )
        self.assert_eq(
            pser.cat.rename_categories(lambda x: x.upper()),
            psser.cat.rename_categories(lambda x: x.upper()),
        )

        self.assertRaisesRegex(
            ValueError,
            "new categories need to have the same number of items as the old categories",
            lambda: psser.cat.rename_categories([0, 1, 2]),
        )
        self.assertRaises(
            TypeError,
            lambda: psser.cat.rename_categories(None),
        )
        self.assertRaises(
            TypeError,
            lambda: psser.cat.rename_categories(1),
        )
        self.assertRaises(
            TypeError,
            lambda: psser.cat.rename_categories("x"),
        )

    def test_set_categories(self):
        pdf, psdf = self.df_pair

        pser = pdf.b
        psser = psdf.b

        self.assert_eq(
            pser.cat.set_categories(["a", "c", "b", "o"]),
            psser.cat.set_categories(["a", "c", "b", "o"]),
        )
        self.assert_eq(
            pser.cat.set_categories(["a", "c", "b"]),
            psser.cat.set_categories(["a", "c", "b"]),
        )
        self.assert_eq(
            pser.cat.set_categories(["a", "c", "b", "d", "e"]),
            psser.cat.set_categories(["a", "c", "b", "d", "e"]),
        )

        self.assert_eq(
            pser.cat.set_categories([0, 1, 3, 2], rename=True),
            psser.cat.set_categories([0, 1, 3, 2], rename=True),
        )
        self.assert_eq(
            pser.cat.set_categories([0, 1, 3], rename=True),
            psser.cat.set_categories([0, 1, 3], rename=True),
        )
        self.assert_eq(
            pser.cat.set_categories([0, 1, 3, 2, 4], rename=True),
            psser.cat.set_categories([0, 1, 3, 2, 4], rename=True),
        )

        self.assert_eq(
            pser.cat.set_categories(["a", "c", "b", "o"], ordered=True),
            psser.cat.set_categories(["a", "c", "b", "o"], ordered=True),
        )
        self.assert_eq(
            pser.cat.set_categories(["a", "c", "b"], ordered=True),
            psser.cat.set_categories(["a", "c", "b"], ordered=True),
        )
        self.assert_eq(
            pser.cat.set_categories(["a", "c", "b", "d", "e"], ordered=True),
            psser.cat.set_categories(["a", "c", "b", "d", "e"], ordered=True),
        )

        self.assertRaisesRegex(
            TypeError,
            "Parameter 'new_categories' must be list-like, was",
            lambda: psser.cat.set_categories(None),
        )


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.test_categorical import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
