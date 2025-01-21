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

import numpy as np
import pandas as pd

from pyspark import pandas as ps
from pyspark.pandas.config import option_context
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


# This file contains test cases for 'Indexing, Iteration'
# https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/frame.html#indexing-iteration
class FrameIndexingMixin:
    @property
    def pdf(self):
        return pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9], "b": [4, 5, 6, 3, 2, 1, 0, 0, 0]},
            index=np.random.rand(9),
        )

    @property
    def psdf(self):
        return ps.from_pandas(self.pdf)

    @property
    def df_pair(self):
        pdf = self.pdf
        psdf = ps.from_pandas(pdf)
        return pdf, psdf

    def test_head(self):
        pdf, psdf = self.df_pair

        self.assert_eq(psdf.head(2), pdf.head(2))
        self.assert_eq(psdf.head(3), pdf.head(3))
        self.assert_eq(psdf.head(0), pdf.head(0))
        self.assert_eq(psdf.head(-3), pdf.head(-3))
        self.assert_eq(psdf.head(-10), pdf.head(-10))
        with option_context("compute.ordered_head", True):
            self.assert_eq(psdf.head(), pdf.head())

    def test_items(self):
        pdf = pd.DataFrame(
            {"species": ["bear", "bear", "marsupial"], "population": [1864, 22000, 80000]},
            index=["panda", "polar", "koala"],
            columns=["species", "population"],
        )
        psdf = ps.from_pandas(pdf)

        for (p_name, p_items), (k_name, k_items) in zip(pdf.items(), psdf.items()):
            self.assert_eq(p_name, k_name)
            self.assert_eq(p_items, k_items)

    def test_keys(self):
        pdf = pd.DataFrame(
            [[1, 2], [4, 5], [7, 8]],
            index=["cobra", "viper", "sidewinder"],
            columns=["max_speed", "shield"],
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.keys(), pdf.keys())

    def test_tail(self):
        pdf = pd.DataFrame({"x": range(1000)})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(pdf.tail(), psdf.tail())
        self.assert_eq(pdf.tail(10), psdf.tail(10))
        self.assert_eq(pdf.tail(-990), psdf.tail(-990))
        self.assert_eq(pdf.tail(0), psdf.tail(0))
        self.assert_eq(pdf.tail(-1001), psdf.tail(-1001))
        self.assert_eq(pdf.tail(1001), psdf.tail(1001))
        self.assert_eq((pdf + 1).tail(), (psdf + 1).tail())
        self.assert_eq((pdf + 1).tail(10), (psdf + 1).tail(10))
        self.assert_eq((pdf + 1).tail(-990), (psdf + 1).tail(-990))
        self.assert_eq((pdf + 1).tail(0), (psdf + 1).tail(0))
        self.assert_eq((pdf + 1).tail(-1001), (psdf + 1).tail(-1001))
        self.assert_eq((pdf + 1).tail(1001), (psdf + 1).tail(1001))
        with self.assertRaisesRegex(TypeError, "bad operand type for unary -: 'str'"):
            psdf.tail("10")

    def test_xs(self):
        d = {
            "num_legs": [4, 4, 2, 2],
            "num_wings": [0, 0, 2, 2],
            "class": ["mammal", "mammal", "mammal", "bird"],
            "animal": ["cat", "dog", "bat", "penguin"],
            "locomotion": ["walks", "walks", "flies", "walks"],
        }
        pdf = pd.DataFrame(data=d)
        pdf = pdf.set_index(["class", "animal", "locomotion"])
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.xs("mammal"), pdf.xs("mammal"))
        self.assert_eq(psdf.xs(("mammal",)), pdf.xs(("mammal",)))
        self.assert_eq(psdf.xs(("mammal", "dog", "walks")), pdf.xs(("mammal", "dog", "walks")))
        self.assert_eq(
            ps.concat([psdf, psdf]).xs(("mammal", "dog", "walks")),
            pd.concat([pdf, pdf]).xs(("mammal", "dog", "walks")),
        )
        self.assert_eq(psdf.xs("cat", level=1), pdf.xs("cat", level=1))
        self.assert_eq(psdf.xs("flies", level=2), pdf.xs("flies", level=2))
        self.assert_eq(psdf.xs("mammal", level=-3), pdf.xs("mammal", level=-3))

        msg = 'axis should be either 0 or "index" currently.'
        with self.assertRaisesRegex(NotImplementedError, msg):
            psdf.xs("num_wings", axis=1)
        with self.assertRaises(KeyError):
            psdf.xs(("mammal", "dog", "walk"))
        msg = r"'Key length \(4\) exceeds index depth \(3\)'"
        with self.assertRaisesRegex(KeyError, msg):
            psdf.xs(("mammal", "dog", "walks", "foo"))
        msg = "'key' should be a scalar value or tuple that contains scalar values"
        with self.assertRaisesRegex(TypeError, msg):
            psdf.xs(["mammal", "dog", "walks", "foo"])

        self.assertRaises(IndexError, lambda: psdf.xs("foo", level=-4))
        self.assertRaises(IndexError, lambda: psdf.xs("foo", level=3))

        self.assertRaises(KeyError, lambda: psdf.xs(("dog", "walks"), level=1))

        # non-string names
        pdf = pd.DataFrame(data=d)
        pdf = pdf.set_index(["class", "animal", "num_legs", "num_wings"])
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.xs(("mammal", "dog", 4)), pdf.xs(("mammal", "dog", 4)))
        self.assert_eq(psdf.xs(2, level=2), pdf.xs(2, level=2))

        self.assert_eq((psdf + "a").xs(("mammal", "dog", 4)), (pdf + "a").xs(("mammal", "dog", 4)))
        self.assert_eq((psdf + "a").xs(2, level=2), (pdf + "a").xs(2, level=2))

    def test_where(self):
        pdf, psdf = self.df_pair

        # pandas requires `axis` argument when the `other` is Series.
        # `axis` is not fully supported yet in pandas-on-Spark.
        self.assert_eq(
            psdf.where(psdf > 2, psdf.a + 10, axis=0), pdf.where(pdf > 2, pdf.a + 10, axis=0)
        )

        with self.assertRaisesRegex(TypeError, "type of cond must be a DataFrame or Series"):
            psdf.where(1)
        with self.assertRaisesRegex(
            NotImplementedError, 'axis should be either 0 or "index" currently.'
        ):
            psdf.where(psdf > 2, psdf.a + 10, axis=1)

    def test_mask(self):
        psdf = ps.from_pandas(self.pdf)

        with self.assertRaisesRegex(TypeError, "type of cond must be a DataFrame or Series"):
            psdf.mask(1)

    def test_query(self):
        pdf = pd.DataFrame({"A": range(1, 6), "B": range(10, 0, -2), "C": range(10, 5, -1)})
        psdf = ps.from_pandas(pdf)

        exprs = ("A > B", "A < C", "C == B")
        for expr in exprs:
            self.assert_eq(psdf.query(expr), pdf.query(expr))

        # test `inplace=True`
        for expr in exprs:
            dummy_psdf = psdf.copy()
            dummy_pdf = pdf.copy()

            pser = dummy_pdf.A
            psser = dummy_psdf.A
            dummy_pdf.query(expr, inplace=True)
            dummy_psdf.query(expr, inplace=True)

            self.assert_eq(dummy_psdf, dummy_pdf)
            self.assert_eq(psser, pser)

        # invalid values for `expr`
        invalid_exprs = (1, 1.0, (exprs[0],), [exprs[0]])
        for expr in invalid_exprs:
            with self.assertRaisesRegex(
                TypeError,
                "expr must be a string to be evaluated, {} given".format(type(expr).__name__),
            ):
                psdf.query(expr)

        # invalid values for `inplace`
        invalid_inplaces = (1, 0, "True", "False")
        for inplace in invalid_inplaces:
            with self.assertRaisesRegex(
                TypeError,
                'For argument "inplace" expected type bool, received type {}.'.format(
                    type(inplace).__name__
                ),
            ):
                psdf.query("a < b", inplace=inplace)

        # doesn't support for MultiIndex columns
        columns = pd.MultiIndex.from_tuples([("A", "Z"), ("B", "X"), ("C", "C")])
        psdf.columns = columns
        with self.assertRaisesRegex(TypeError, "Doesn't support for MultiIndex columns"):
            psdf.query("('A', 'Z') > ('B', 'X')")

    def test_insert(self):
        #
        # Basic DataFrame
        #
        pdf = pd.DataFrame([1, 2, 3])
        psdf = ps.from_pandas(pdf)

        psdf.insert(1, "b", 10)
        pdf.insert(1, "b", 10)
        self.assert_eq(psdf.sort_index(), pdf.sort_index(), almost=True)
        psdf.insert(2, "c", 0.1)
        pdf.insert(2, "c", 0.1)
        self.assert_eq(psdf.sort_index(), pdf.sort_index(), almost=True)
        psdf.insert(3, "d", psdf.b + 1)
        pdf.insert(3, "d", pdf.b + 1)
        self.assert_eq(psdf.sort_index(), pdf.sort_index(), almost=True)

        psser = ps.Series([4, 5, 6])
        with ps.option_context("compute.ops_on_diff_frames", False):
            self.assertRaises(ValueError, lambda: psdf.insert(0, "y", psser))

        self.assertRaisesRegex(
            ValueError, "cannot insert b, already exists", lambda: psdf.insert(1, "b", 10)
        )
        self.assertRaisesRegex(
            TypeError,
            '"column" should be a scalar value or tuple that contains scalar values',
            lambda: psdf.insert(0, list("abc"), psser),
        )
        self.assertRaisesRegex(
            TypeError,
            "loc must be int",
            lambda: psdf.insert((1,), "b", 10),
        )
        self.assertRaisesRegex(
            NotImplementedError,
            "Assigning column name as tuple is only supported for MultiIndex columns for now.",
            lambda: psdf.insert(0, ("e",), 10),
        )

        self.assertRaises(ValueError, lambda: psdf.insert(0, "e", [7, 8, 9, 10]))
        with ps.option_context("compute.ops_on_diff_frames", False):
            self.assertRaises(ValueError, lambda: psdf.insert(0, "f", ps.Series([7, 8])))

        self.assertRaises(AssertionError, lambda: psdf.insert(100, "y", psser))
        self.assertRaises(AssertionError, lambda: psdf.insert(1, "y", psser, allow_duplicates=True))

        #
        # DataFrame with MultiIndex as columns
        #
        pdf = pd.DataFrame({("x", "a", "b"): [1, 2, 3]})
        psdf = ps.from_pandas(pdf)

        psdf.insert(1, "b", 10)
        pdf.insert(1, "b", 10)
        self.assert_eq(psdf.sort_index(), pdf.sort_index(), almost=True)
        psdf.insert(2, "c", 0.1)
        pdf.insert(2, "c", 0.1)
        self.assert_eq(psdf.sort_index(), pdf.sort_index(), almost=True)
        psdf.insert(3, "d", psdf.b + 1)
        pdf.insert(3, "d", pdf.b + 1)
        self.assert_eq(psdf.sort_index(), pdf.sort_index(), almost=True)

        self.assertRaisesRegex(
            ValueError, "cannot insert d, already exists", lambda: psdf.insert(4, "d", 11)
        )
        self.assertRaisesRegex(
            ValueError,
            r"cannot insert \('x', 'a', 'b'\), already exists",
            lambda: psdf.insert(4, ("x", "a", "b"), 11),
        )
        self.assertRaisesRegex(
            ValueError,
            '"column" must have length equal to number of column levels.',
            lambda: psdf.insert(4, ("e",), 11),
        )

    def test_itertuples(self):
        pdf = pd.DataFrame({"num_legs": [4, 2], "num_wings": [0, 2]}, index=["dog", "hawk"])
        psdf = ps.from_pandas(pdf)

        for ptuple, ktuple in zip(
            pdf.itertuples(index=False, name="Animal"), psdf.itertuples(index=False, name="Animal")
        ):
            self.assert_eq(ptuple, ktuple)
        for ptuple, ktuple in zip(pdf.itertuples(name=None), psdf.itertuples(name=None)):
            self.assert_eq(ptuple, ktuple)
        for ptuple, ktuple in zip(
            pdf.itertuples(index=False, name=None), psdf.itertuples(index=False, name=None)
        ):
            self.assert_eq(ptuple, ktuple)

        pdf.index = pd.MultiIndex.from_arrays(
            [[1, 2], ["black", "brown"]], names=("count", "color")
        )
        psdf = ps.from_pandas(pdf)
        for ptuple, ktuple in zip(pdf.itertuples(name="Animal"), psdf.itertuples(name="Animal")):
            self.assert_eq(ptuple, ktuple)

        pdf.columns = pd.MultiIndex.from_arrays(
            [["CA", "WA"], ["age", "children"]], names=("origin", "info")
        )
        psdf = ps.from_pandas(pdf)
        for ptuple, ktuple in zip(pdf.itertuples(name="Animal"), psdf.itertuples(name="Animal")):
            self.assert_eq(ptuple, ktuple)

        pdf = pd.DataFrame([1, 2, 3])
        psdf = ps.from_pandas(pdf)
        for ptuple, ktuple in zip(
            (pdf + 1).itertuples(name="num"), (psdf + 1).itertuples(name="num")
        ):
            self.assert_eq(ptuple, ktuple)

        # DataFrames with a large number of columns (>254)
        pdf = pd.DataFrame(np.random.random((1, 255)))
        psdf = ps.from_pandas(pdf)
        for ptuple, ktuple in zip(pdf.itertuples(name="num"), psdf.itertuples(name="num")):
            self.assert_eq(ptuple, ktuple)

    def test_iterrows(self):
        pdf = pd.DataFrame(
            {
                ("x", "a", "1"): [1, 2, 3],
                ("x", "b", "2"): [4, 5, 6],
                ("y.z", "c.d", "3"): [7, 8, 9],
                ("x", "b", "4"): [10, 11, 12],
            },
            index=np.random.rand(3),
        )
        psdf = ps.from_pandas(pdf)

        for (pdf_k, pdf_v), (psdf_k, psdf_v) in zip(pdf.iterrows(), psdf.iterrows()):
            self.assert_eq(pdf_k, psdf_k)
            self.assert_eq(pdf_v, psdf_v)

        # MultiIndex
        pmidx = pd.Index([(1, 2), (3, 4), (5, 6)])
        pdf.index = pmidx
        psdf = ps.from_pandas(pdf)

        for (pdf_k, pdf_v), (psdf_k, psdf_v) in zip(pdf.iterrows(), psdf.iterrows()):
            self.assert_eq(pdf_k, psdf_k)
            self.assert_eq(pdf_v, psdf_v)

    def test_multiindex_column_access(self):
        columns = pd.MultiIndex.from_tuples(
            [
                ("a", "", "", "b"),
                ("c", "", "d", ""),
                ("e", "", "f", ""),
                ("e", "g", "", ""),
                ("", "", "", "h"),
                ("i", "", "", ""),
            ]
        )

        pdf = pd.DataFrame(
            [
                (1, "a", "x", 10, 100, 1000),
                (2, "b", "y", 20, 200, 2000),
                (3, "c", "z", 30, 300, 3000),
            ],
            columns=columns,
            index=np.random.rand(3),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf, pdf)
        self.assert_eq(psdf["a"], pdf["a"])
        self.assert_eq(psdf["a"]["b"], pdf["a"]["b"])
        self.assert_eq(psdf["c"], pdf["c"])
        self.assert_eq(psdf["c"]["d"], pdf["c"]["d"])
        self.assert_eq(psdf["e"], pdf["e"])
        self.assert_eq(psdf["e"][""]["f"], pdf["e"][""]["f"])
        self.assert_eq(psdf["e"]["g"], pdf["e"]["g"])
        self.assert_eq(psdf[""], pdf[""])
        self.assert_eq(psdf[""]["h"], pdf[""]["h"])
        self.assert_eq(psdf["i"], pdf["i"])

        self.assert_eq(psdf[["a", "e"]], pdf[["a", "e"]])
        self.assert_eq(psdf[["e", "a"]], pdf[["e", "a"]])

        self.assert_eq(psdf[("a",)], pdf[("a",)])
        self.assert_eq(psdf[("e", "g")], pdf[("e", "g")])
        # self.assert_eq(psdf[("i",)], pdf[("i",)])
        self.assert_eq(psdf[("i", "")], pdf[("i", "")])

        self.assertRaises(KeyError, lambda: psdf[("a", "b")])

    def test_getitem_with_none_key(self):
        psdf = self.psdf

        with self.assertRaisesRegex(KeyError, "none key"):
            psdf[None]

    def test_iter_dataframe(self):
        pdf, psdf = self.df_pair

        for value_psdf, value_pdf in zip(psdf, pdf):
            self.assert_eq(value_psdf, value_pdf)


class FrameIndexingTests(
    FrameIndexingMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.indexes.test_indexing import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
