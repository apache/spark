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

import pandas as pd
import numpy as np

from pyspark import pandas as ps
from pyspark.pandas.config import set_option, reset_option
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


class OpsOnDiffFramesEnabledSlowTest(PandasOnSparkTestCase, SQLTestUtils):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        set_option("compute.ops_on_diff_frames", True)

    @classmethod
    def tearDownClass(cls):
        reset_option("compute.ops_on_diff_frames")
        super().tearDownClass()

    @property
    def pdf1(self):
        return pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9], "b": [4, 5, 6, 3, 2, 1, 0, 0, 0]},
            index=[0, 1, 3, 5, 6, 8, 9, 10, 11],
        )

    @property
    def pdf2(self):
        return pd.DataFrame(
            {"a": [9, 8, 7, 6, 5, 4, 3, 2, 1], "b": [0, 0, 0, 4, 5, 6, 1, 2, 3]},
            index=list(range(9)),
        )

    @property
    def pdf3(self):
        return pd.DataFrame(
            {"b": [1, 1, 1, 1, 1, 1, 1, 1, 1], "c": [1, 1, 1, 1, 1, 1, 1, 1, 1]},
            index=list(range(9)),
        )

    @property
    def pdf4(self):
        return pd.DataFrame(
            {"e": [2, 2, 2, 2, 2, 2, 2, 2, 2], "f": [2, 2, 2, 2, 2, 2, 2, 2, 2]},
            index=list(range(9)),
        )

    @property
    def pdf5(self):
        return pd.DataFrame(
            {
                "a": [1, 2, 3, 4, 5, 6, 7, 8, 9],
                "b": [4, 5, 6, 3, 2, 1, 0, 0, 0],
                "c": [4, 5, 6, 3, 2, 1, 0, 0, 0],
            },
            index=[0, 1, 3, 5, 6, 8, 9, 10, 11],
        ).set_index(["a", "b"])

    @property
    def pdf6(self):
        return pd.DataFrame(
            {
                "a": [9, 8, 7, 6, 5, 4, 3, 2, 1],
                "b": [0, 0, 0, 4, 5, 6, 1, 2, 3],
                "c": [9, 8, 7, 6, 5, 4, 3, 2, 1],
                "e": [4, 5, 6, 3, 2, 1, 0, 0, 0],
            },
            index=list(range(9)),
        ).set_index(["a", "b"])

    @property
    def pser1(self):
        midx = pd.MultiIndex(
            [["lama", "cow", "falcon", "koala"], ["speed", "weight", "length", "power"]],
            [[0, 3, 1, 1, 1, 2, 2, 2], [0, 2, 0, 3, 2, 0, 1, 3]],
        )
        return pd.Series([45, 200, 1.2, 30, 250, 1.5, 320, 1], index=midx)

    @property
    def pser2(self):
        midx = pd.MultiIndex(
            [["lama", "cow", "falcon"], ["speed", "weight", "length"]],
            [[0, 0, 0, 1, 1, 1, 2, 2, 2], [0, 1, 2, 0, 1, 2, 0, 1, 2]],
        )
        return pd.Series([-45, 200, -1.2, 30, -250, 1.5, 320, 1, -0.3], index=midx)

    @property
    def pser3(self):
        midx = pd.MultiIndex(
            [["koalas", "cow", "falcon"], ["speed", "weight", "length"]],
            [[0, 0, 0, 1, 1, 1, 2, 2, 2], [1, 1, 2, 0, 0, 2, 2, 2, 1]],
        )
        return pd.Series([45, 200, 1.2, 30, 250, 1.5, 320, 1, 0.3], index=midx)

    @property
    def psdf1(self):
        return ps.from_pandas(self.pdf1)

    @property
    def psdf2(self):
        return ps.from_pandas(self.pdf2)

    @property
    def psdf3(self):
        return ps.from_pandas(self.pdf3)

    @property
    def psdf4(self):
        return ps.from_pandas(self.pdf4)

    @property
    def psdf5(self):
        return ps.from_pandas(self.pdf5)

    @property
    def psdf6(self):
        return ps.from_pandas(self.pdf6)

    @property
    def psser1(self):
        return ps.from_pandas(self.pser1)

    @property
    def psser2(self):
        return ps.from_pandas(self.pser2)

    @property
    def psser3(self):
        return ps.from_pandas(self.pser3)

    def test_frame_loc_setitem(self):
        pdf_orig = pd.DataFrame(
            [[1, 2], [4, 5], [7, 8]],
            index=["cobra", "viper", "sidewinder"],
            columns=["max_speed", "shield"],
        )
        psdf_orig = ps.DataFrame(pdf_orig)

        pdf = pdf_orig.copy()
        psdf = psdf_orig.copy()
        pser1 = pdf.max_speed
        pser2 = pdf.shield
        psser1 = psdf.max_speed
        psser2 = psdf.shield

        another_psdf = ps.DataFrame(pdf_orig)

        psdf.loc[["viper", "sidewinder"], ["shield"]] = -another_psdf.max_speed
        pdf.loc[["viper", "sidewinder"], ["shield"]] = -pdf.max_speed
        self.assert_eq(psdf, pdf)
        self.assert_eq(psser1, pser1)
        self.assert_eq(psser2, pser2)

        pdf = pdf_orig.copy()
        psdf = psdf_orig.copy()
        pser1 = pdf.max_speed
        pser2 = pdf.shield
        psser1 = psdf.max_speed
        psser2 = psdf.shield
        psdf.loc[another_psdf.max_speed < 5, ["shield"]] = -psdf.max_speed
        pdf.loc[pdf.max_speed < 5, ["shield"]] = -pdf.max_speed
        self.assert_eq(psdf, pdf)
        self.assert_eq(psser1, pser1)
        self.assert_eq(psser2, pser2)

        pdf = pdf_orig.copy()
        psdf = psdf_orig.copy()
        pser1 = pdf.max_speed
        pser2 = pdf.shield
        psser1 = psdf.max_speed
        psser2 = psdf.shield
        psdf.loc[another_psdf.max_speed < 5, ["shield"]] = -another_psdf.max_speed
        pdf.loc[pdf.max_speed < 5, ["shield"]] = -pdf.max_speed
        self.assert_eq(psdf, pdf)
        self.assert_eq(psser1, pser1)
        self.assert_eq(psser2, pser2)

    def test_frame_iloc_setitem(self):
        pdf = pd.DataFrame(
            [[1, 2], [4, 5], [7, 8]],
            index=["cobra", "viper", "sidewinder"],
            columns=["max_speed", "shield"],
        )
        psdf = ps.DataFrame(pdf)
        another_psdf = ps.DataFrame(pdf)

        psdf.iloc[[0, 1, 2], 1] = -another_psdf.max_speed
        pdf.iloc[[0, 1, 2], 1] = -pdf.max_speed
        self.assert_eq(psdf, pdf)

        with self.assertRaisesRegex(
            ValueError,
            "shape mismatch",
        ):
            psdf.iloc[[1, 2], [1]] = -another_psdf.max_speed

        psdf.iloc[[0, 1, 2], 1] = 10 * another_psdf.max_speed
        pdf.iloc[[0, 1, 2], 1] = 10 * pdf.max_speed
        self.assert_eq(psdf, pdf)

        with self.assertRaisesRegex(ValueError, "shape mismatch"):
            psdf.iloc[[0], 1] = 10 * another_psdf.max_speed

    def test_series_loc_setitem(self):
        pdf = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]}, index=["cobra", "viper", "sidewinder"])
        psdf = ps.from_pandas(pdf)
        pser = pdf.x
        psery = pdf.y
        psser = psdf.x
        pssery = psdf.y

        pser_another = pd.Series([1, 2, 3], index=["cobra", "viper", "sidewinder"])
        psser_another = ps.from_pandas(pser_another)

        psser.loc[psser % 2 == 1] = -psser_another
        pser.loc[pser % 2 == 1] = -pser_another
        self.assert_eq(psser, pser)
        self.assert_eq(psdf, pdf)
        self.assert_eq(pssery, psery)

        pdf = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]}, index=["cobra", "viper", "sidewinder"])
        psdf = ps.from_pandas(pdf)
        pser = pdf.x
        psery = pdf.y
        psser = psdf.x
        pssery = psdf.y
        psser.loc[psser_another % 2 == 1] = -psser
        pser.loc[pser_another % 2 == 1] = -pser
        self.assert_eq(psser, pser)
        self.assert_eq(psdf, pdf)
        self.assert_eq(pssery, psery)

        pdf = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]}, index=["cobra", "viper", "sidewinder"])
        psdf = ps.from_pandas(pdf)
        pser = pdf.x
        psery = pdf.y
        psser = psdf.x
        pssery = psdf.y
        psser.loc[psser_another % 2 == 1] = -psser
        pser.loc[pser_another % 2 == 1] = -pser
        self.assert_eq(psser, pser)
        self.assert_eq(psdf, pdf)
        self.assert_eq(pssery, psery)

        pdf = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]}, index=["cobra", "viper", "sidewinder"])
        psdf = ps.from_pandas(pdf)
        pser = pdf.x
        psery = pdf.y
        psser = psdf.x
        pssery = psdf.y
        psser.loc[psser_another % 2 == 1] = -psser_another
        pser.loc[pser_another % 2 == 1] = -pser_another
        self.assert_eq(psser, pser)
        self.assert_eq(psdf, pdf)
        self.assert_eq(pssery, psery)

        pdf = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]}, index=["cobra", "viper", "sidewinder"])
        psdf = ps.from_pandas(pdf)
        pser = pdf.x
        psery = pdf.y
        psser = psdf.x
        pssery = psdf.y
        psser.loc[["viper", "sidewinder"]] = -psser_another
        pser.loc[["viper", "sidewinder"]] = -pser_another
        self.assert_eq(psser, pser)
        self.assert_eq(psdf, pdf)
        self.assert_eq(pssery, psery)

        pdf = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]}, index=["cobra", "viper", "sidewinder"])
        psdf = ps.from_pandas(pdf)
        pser = pdf.x
        psery = pdf.y
        psser = psdf.x
        pssery = psdf.y
        psser.loc[psser_another % 2 == 1] = 10
        pser.loc[pser_another % 2 == 1] = 10
        self.assert_eq(psser, pser)
        self.assert_eq(psdf, pdf)
        self.assert_eq(pssery, psery)

    def test_series_iloc_setitem(self):
        pdf = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]}, index=["cobra", "viper", "sidewinder"])
        psdf = ps.from_pandas(pdf)

        pser = pdf.x
        psery = pdf.y
        psser = psdf.x
        pssery = psdf.y

        pser1 = pser + 1
        psser1 = psser + 1

        pser_another = pd.Series([1, 2, 3], index=["cobra", "viper", "sidewinder"])
        psser_another = ps.from_pandas(pser_another)

        psser.iloc[[0, 1, 2]] = -psser_another
        pser.iloc[[0, 1, 2]] = -pser_another
        self.assert_eq(psser, pser)
        self.assert_eq(psdf, pdf)
        self.assert_eq(pssery, psery)

        with self.assertRaisesRegex(
            ValueError,
            "cannot set using a list-like indexer with a different length than the value",
        ):
            psser.iloc[[1, 2]] = -psser_another

        psser.iloc[[0, 1, 2]] = 10 * psser_another
        pser.iloc[[0, 1, 2]] = 10 * pser_another
        self.assert_eq(psser, pser)
        self.assert_eq(psdf, pdf)
        self.assert_eq(pssery, psery)

        with self.assertRaisesRegex(
            ValueError,
            "cannot set using a list-like indexer with a different length than the value",
        ):
            psser.iloc[[0]] = 10 * psser_another

        psser1.iloc[[0, 1, 2]] = -psser_another
        pser1.iloc[[0, 1, 2]] = -pser_another
        self.assert_eq(psser1, pser1)
        self.assert_eq(psdf, pdf)
        self.assert_eq(pssery, psery)

        with self.assertRaisesRegex(
            ValueError,
            "cannot set using a list-like indexer with a different length than the value",
        ):
            psser1.iloc[[1, 2]] = -psser_another

        pdf = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]}, index=["cobra", "viper", "sidewinder"])
        psdf = ps.from_pandas(pdf)

        pser = pdf.x
        psery = pdf.y
        psser = psdf.x
        pssery = psdf.y

        piloc = pser.iloc
        kiloc = psser.iloc

        kiloc[[0, 1, 2]] = -psser_another
        piloc[[0, 1, 2]] = -pser_another
        self.assert_eq(psser, pser)
        self.assert_eq(psdf, pdf)
        self.assert_eq(pssery, psery)

        with self.assertRaisesRegex(
            ValueError,
            "cannot set using a list-like indexer with a different length than the value",
        ):
            kiloc[[1, 2]] = -psser_another

        kiloc[[0, 1, 2]] = 10 * psser_another
        piloc[[0, 1, 2]] = 10 * pser_another
        self.assert_eq(psser, pser)
        self.assert_eq(psdf, pdf)
        self.assert_eq(pssery, psery)

        with self.assertRaisesRegex(
            ValueError,
            "cannot set using a list-like indexer with a different length than the value",
        ):
            kiloc[[0]] = 10 * psser_another

    def test_update(self):
        pdf = pd.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]})
        psdf = ps.from_pandas(pdf)

        pser = pdf.x
        psser = psdf.x
        pser.update(pd.Series([4, 5, 6]))
        psser.update(ps.Series([4, 5, 6]))
        self.assert_eq(psser.sort_index(), pser.sort_index())
        self.assert_eq(psdf.sort_index(), pdf.sort_index())

        pser1 = pd.Series([None, 2, 3, 4, 5, 6, 7, 8, None])
        pser2 = pd.Series([None, 5, None, 3, 2, 1, None, 0, 0])
        psser1 = ps.from_pandas(pser1)
        psser2 = ps.from_pandas(pser2)

        pser1.update(pser2)
        psser1.update(psser2)
        self.assert_eq(psser1.sort_index(), pser1)

    def test_where(self):
        pdf1 = pd.DataFrame({"A": [0, 1, 2, 3, 4], "B": [100, 200, 300, 400, 500]})
        pdf2 = pd.DataFrame({"A": [0, -1, -2, -3, -4], "B": [-100, -200, -300, -400, -500]})
        psdf1 = ps.from_pandas(pdf1)
        psdf2 = ps.from_pandas(pdf2)

        self.assert_eq(pdf1.where(pdf2 > 100), psdf1.where(psdf2 > 100).sort_index())

        pdf1 = pd.DataFrame({"A": [-1, -2, -3, -4, -5], "B": [-100, -200, -300, -400, -500]})
        pdf2 = pd.DataFrame({"A": [-10, -20, -30, -40, -50], "B": [-5, -4, -3, -2, -1]})
        psdf1 = ps.from_pandas(pdf1)
        psdf2 = ps.from_pandas(pdf2)

        self.assert_eq(pdf1.where(pdf2 < -250), psdf1.where(psdf2 < -250).sort_index())

        # multi-index columns
        pdf1 = pd.DataFrame({("X", "A"): [0, 1, 2, 3, 4], ("X", "B"): [100, 200, 300, 400, 500]})
        pdf2 = pd.DataFrame(
            {("X", "A"): [0, -1, -2, -3, -4], ("X", "B"): [-100, -200, -300, -400, -500]}
        )
        psdf1 = ps.from_pandas(pdf1)
        psdf2 = ps.from_pandas(pdf2)

        self.assert_eq(pdf1.where(pdf2 > 100), psdf1.where(psdf2 > 100).sort_index())

    def test_mask(self):
        pdf1 = pd.DataFrame({"A": [0, 1, 2, 3, 4], "B": [100, 200, 300, 400, 500]})
        pdf2 = pd.DataFrame({"A": [0, -1, -2, -3, -4], "B": [-100, -200, -300, -400, -500]})
        psdf1 = ps.from_pandas(pdf1)
        psdf2 = ps.from_pandas(pdf2)

        self.assert_eq(pdf1.mask(pdf2 < 100), psdf1.mask(psdf2 < 100).sort_index())

        pdf1 = pd.DataFrame({"A": [-1, -2, -3, -4, -5], "B": [-100, -200, -300, -400, -500]})
        pdf2 = pd.DataFrame({"A": [-10, -20, -30, -40, -50], "B": [-5, -4, -3, -2, -1]})
        psdf1 = ps.from_pandas(pdf1)
        psdf2 = ps.from_pandas(pdf2)

        self.assert_eq(pdf1.mask(pdf2 > -250), psdf1.mask(psdf2 > -250).sort_index())

        # multi-index columns
        pdf1 = pd.DataFrame({("X", "A"): [0, 1, 2, 3, 4], ("X", "B"): [100, 200, 300, 400, 500]})
        pdf2 = pd.DataFrame(
            {("X", "A"): [0, -1, -2, -3, -4], ("X", "B"): [-100, -200, -300, -400, -500]}
        )
        psdf1 = ps.from_pandas(pdf1)
        psdf2 = ps.from_pandas(pdf2)

        self.assert_eq(pdf1.mask(pdf2 < 100), psdf1.mask(psdf2 < 100).sort_index())

    def test_multi_index_column_assignment_frame(self):
        pdf = pd.DataFrame({"a": [1, 2, 3, 2], "b": [4.0, 2.0, 3.0, 1.0]})
        pdf.columns = pd.MultiIndex.from_tuples([("a", "x"), ("a", "y")])
        psdf = ps.DataFrame(pdf)

        psdf["c"] = ps.Series([10, 20, 30, 20])
        pdf["c"] = pd.Series([10, 20, 30, 20])

        psdf[("d", "x")] = ps.Series([100, 200, 300, 200], name="1")
        pdf[("d", "x")] = pd.Series([100, 200, 300, 200], name="1")

        psdf[("d", "y")] = ps.Series([1000, 2000, 3000, 2000], name=("1", "2"))
        pdf[("d", "y")] = pd.Series([1000, 2000, 3000, 2000], name=("1", "2"))

        psdf["e"] = ps.Series([10000, 20000, 30000, 20000], name=("1", "2", "3"))
        pdf["e"] = pd.Series([10000, 20000, 30000, 20000], name=("1", "2", "3"))

        psdf[[("f", "x"), ("f", "y")]] = ps.DataFrame(
            {"1": [100000, 200000, 300000, 200000], "2": [1000000, 2000000, 3000000, 2000000]}
        )
        pdf[[("f", "x"), ("f", "y")]] = pd.DataFrame(
            {"1": [100000, 200000, 300000, 200000], "2": [1000000, 2000000, 3000000, 2000000]}
        )

        self.assert_eq(repr(psdf.sort_index()), repr(pdf))

        with self.assertRaisesRegex(KeyError, "Key length \\(3\\) exceeds index depth \\(2\\)"):
            psdf[("1", "2", "3")] = ps.Series([100, 200, 300, 200])

    def test_series_dot(self):
        pser = pd.Series([90, 91, 85], index=[2, 4, 1])
        psser = ps.from_pandas(pser)
        pser_other = pd.Series([90, 91, 85], index=[2, 4, 1])
        psser_other = ps.from_pandas(pser_other)

        self.assert_eq(psser.dot(psser_other), pser.dot(pser_other))

        psser_other = ps.Series([90, 91, 85], index=[1, 2, 4])
        pser_other = pd.Series([90, 91, 85], index=[1, 2, 4])

        self.assert_eq(psser.dot(psser_other), pser.dot(pser_other))

        # length of index is different
        psser_other = ps.Series([90, 91, 85, 100], index=[2, 4, 1, 0])
        with self.assertRaisesRegex(ValueError, "matrices are not aligned"):
            psser.dot(psser_other)

        # for MultiIndex
        midx = pd.MultiIndex(
            [["lama", "cow", "falcon"], ["speed", "weight", "length"]],
            [[0, 0, 0, 1, 1, 1, 2, 2, 2], [0, 1, 2, 0, 1, 2, 0, 1, 2]],
        )
        pser = pd.Series([45, 200, 1.2, 30, 250, 1.5, 320, 1, 0.3], index=midx)
        psser = ps.from_pandas(pser)
        pser_other = pd.Series([-450, 20, 12, -30, -250, 15, -320, 100, 3], index=midx)
        psser_other = ps.from_pandas(pser_other)
        self.assert_eq(psser.dot(psser_other), pser.dot(pser_other))

        pser = pd.Series([0, 1, 2, 3])
        psser = ps.from_pandas(pser)

        # DataFrame "other" without Index/MultiIndex as columns
        pdf = pd.DataFrame([[0, 1], [-2, 3], [4, -5], [6, 7]])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psser.dot(psdf), pser.dot(pdf))

        # DataFrame "other" with Index as columns
        pdf.columns = pd.Index(["x", "y"])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psser.dot(psdf), pser.dot(pdf))
        pdf.columns = pd.Index(["x", "y"], name="cols_name")
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psser.dot(psdf), pser.dot(pdf))

        pdf = pdf.reindex([1, 0, 2, 3])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psser.dot(psdf), pser.dot(pdf))

        # DataFrame "other" with MultiIndex as columns
        pdf.columns = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y")])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psser.dot(psdf), pser.dot(pdf))
        pdf.columns = pd.MultiIndex.from_tuples(
            [("a", "x"), ("b", "y")], names=["cols_name1", "cols_name2"]
        )
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psser.dot(psdf), pser.dot(pdf))

        psser = ps.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}).b
        pser = psser._to_pandas()
        psdf = ps.DataFrame({"c": [7, 8, 9]})
        pdf = psdf._to_pandas()
        self.assert_eq(psser.dot(psdf), pser.dot(pdf))

        # SPARK-36968: ps.Series.dot raise "matrices are not aligned" if index is not same
        pser = pd.Series([90, 91, 85], index=[0, 1, 2])
        psser = ps.from_pandas(pser)
        pser_other = pd.Series([90, 91, 85], index=[0, 1, 3])
        psser_other = ps.from_pandas(pser_other)
        pser_other2 = pd.Series([90, 91, 85, 100], index=[0, 1, 3, 5])
        psser_other2 = ps.from_pandas(pser_other2)

        with self.assertRaisesRegex(ValueError, "matrices are not aligned"):
            psser.dot(psser_other)

        with ps.option_context("compute.eager_check", False), self.assertRaisesRegex(
            ValueError, "matrices are not aligned"
        ):
            psser.dot(psser_other2)

        with ps.option_context("compute.eager_check", True), self.assertRaisesRegex(
            ValueError, "matrices are not aligned"
        ):
            psser.dot(psser_other)

        with ps.option_context("compute.eager_check", False):
            self.assert_eq(psser.dot(psser_other), 16381)

    def test_frame_dot(self):
        pdf = pd.DataFrame([[0, 1, -2, -1], [1, 1, 1, 1]])
        psdf = ps.from_pandas(pdf)

        pser = pd.Series([1, 1, 2, 1])
        psser = ps.from_pandas(pser)
        self.assert_eq(psdf.dot(psser), pdf.dot(pser))

        # Index reorder
        pser = pser.reindex([1, 0, 2, 3])
        psser = ps.from_pandas(pser)
        self.assert_eq(psdf.dot(psser), pdf.dot(pser))

        # ser with name
        pser.name = "ser"
        psser = ps.from_pandas(pser)
        self.assert_eq(psdf.dot(psser), pdf.dot(pser))

        # df with MultiIndex as column (ser with MultiIndex)
        arrays = [[1, 1, 2, 2], ["red", "blue", "red", "blue"]]
        pidx = pd.MultiIndex.from_arrays(arrays, names=("number", "color"))
        pser = pd.Series([1, 1, 2, 1], index=pidx)
        pdf = pd.DataFrame([[0, 1, -2, -1], [1, 1, 1, 1]], columns=pidx)
        psdf = ps.from_pandas(pdf)
        psser = ps.from_pandas(pser)
        self.assert_eq(psdf.dot(psser), pdf.dot(pser))

        # df with Index as column (ser with Index)
        pidx = pd.Index([1, 2, 3, 4], name="number")
        pser = pd.Series([1, 1, 2, 1], index=pidx)
        pdf = pd.DataFrame([[0, 1, -2, -1], [1, 1, 1, 1]], columns=pidx)
        psdf = ps.from_pandas(pdf)
        psser = ps.from_pandas(pser)
        self.assert_eq(psdf.dot(psser), pdf.dot(pser))

        # df with Index
        pdf.index = pd.Index(["x", "y"], name="char")
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psdf.dot(psser), pdf.dot(pser))

        # df with MultiIndex
        pdf.index = pd.MultiIndex.from_arrays([[1, 1], ["red", "blue"]], names=("number", "color"))
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psdf.dot(psser), pdf.dot(pser))

        pdf = pd.DataFrame([[1, 2], [3, 4]])
        psdf = ps.from_pandas(pdf)
        self.assert_eq(psdf.dot(psdf[0]), pdf.dot(pdf[0]))
        self.assert_eq(psdf.dot(psdf[0] * 10), pdf.dot(pdf[0] * 10))
        self.assert_eq((psdf + 1).dot(psdf[0] * 10), (pdf + 1).dot(pdf[0] * 10))

    def test_to_series_comparison(self):
        psidx1 = ps.Index([1, 2, 3, 4, 5])
        psidx2 = ps.Index([1, 2, 3, 4, 5])

        self.assert_eq((psidx1.to_series() == psidx2.to_series()).all(), True)

        psidx1.name = "koalas"
        psidx2.name = "koalas"

        self.assert_eq((psidx1.to_series() == psidx2.to_series()).all(), True)

    def test_series_repeat(self):
        pser1 = pd.Series(["a", "b", "c"], name="a")
        pser2 = pd.Series([10, 20, 30], name="rep")
        psser1 = ps.from_pandas(pser1)
        psser2 = ps.from_pandas(pser2)

        self.assert_eq(psser1.repeat(psser2).sort_index(), pser1.repeat(pser2).sort_index())

    def test_series_ops(self):
        pser1 = pd.Series([1, 2, 3, 4, 5, 6, 7], name="x", index=[11, 12, 13, 14, 15, 16, 17])
        pser2 = pd.Series([1, 2, 3, 4, 5, 6, 7], name="x", index=[11, 12, 13, 14, 15, 16, 17])
        pidx1 = pd.Index([10, 11, 12, 13, 14, 15, 16], name="x")
        psser1 = ps.from_pandas(pser1)
        psser2 = ps.from_pandas(pser2)
        psidx1 = ps.from_pandas(pidx1)

        self.assert_eq(
            (psser1 + 1 + 10 * psser2).sort_index(), (pser1 + 1 + 10 * pser2).sort_index()
        )
        self.assert_eq(
            (psser1 + 1 + 10 * psser2.rename()).sort_index(),
            (pser1 + 1 + 10 * pser2.rename()).sort_index(),
        )
        self.assert_eq(
            (psser1.rename() + 1 + 10 * psser2).sort_index(),
            (pser1.rename() + 1 + 10 * pser2).sort_index(),
        )
        self.assert_eq(
            (psser1.rename() + 1 + 10 * psser2.rename()).sort_index(),
            (pser1.rename() + 1 + 10 * pser2.rename()).sort_index(),
        )

        self.assert_eq(psser1 + 1 + 10 * psidx1, pser1 + 1 + 10 * pidx1)
        self.assert_eq(psser1.rename() + 1 + 10 * psidx1, pser1.rename() + 1 + 10 * pidx1)
        self.assert_eq(psser1 + 1 + 10 * psidx1.rename(None), pser1 + 1 + 10 * pidx1.rename(None))
        self.assert_eq(
            psser1.rename() + 1 + 10 * psidx1.rename(None),
            pser1.rename() + 1 + 10 * pidx1.rename(None),
        )

        self.assert_eq(psidx1 + 1 + 10 * psser1, pidx1 + 1 + 10 * pser1)
        self.assert_eq(psidx1 + 1 + 10 * psser1.rename(), pidx1 + 1 + 10 * pser1.rename())
        self.assert_eq(psidx1.rename(None) + 1 + 10 * psser1, pidx1.rename(None) + 1 + 10 * pser1)
        self.assert_eq(
            psidx1.rename(None) + 1 + 10 * psser1.rename(),
            pidx1.rename(None) + 1 + 10 * pser1.rename(),
        )

        pidx2 = pd.Index([11, 12, 13])
        psidx2 = ps.from_pandas(pidx2)

        with self.assertRaisesRegex(
            ValueError, "operands could not be broadcast together with shapes"
        ):
            psser1 + psidx2

        with self.assertRaisesRegex(
            ValueError, "operands could not be broadcast together with shapes"
        ):
            psidx2 + psser1

    def test_index_ops(self):
        pidx1 = pd.Index([1, 2, 3, 4, 5], name="x")
        pidx2 = pd.Index([6, 7, 8, 9, 10], name="x")
        psidx1 = ps.from_pandas(pidx1)
        psidx2 = ps.from_pandas(pidx2)

        self.assert_eq(psidx1 * 10 + psidx2, pidx1 * 10 + pidx2)
        self.assert_eq(psidx1.rename(None) * 10 + psidx2, pidx1.rename(None) * 10 + pidx2)
        self.assert_eq(psidx1 * 10 + psidx2.rename(None), pidx1 * 10 + pidx2.rename(None))

        pidx3 = pd.Index([11, 12, 13])
        psidx3 = ps.from_pandas(pidx3)

        with self.assertRaisesRegex(
            ValueError, "operands could not be broadcast together with shapes"
        ):
            psidx1 + psidx3

        pidx1 = pd.Index([1, 2, 3, 4, 5], name="a")
        pidx2 = pd.Index([6, 7, 8, 9, 10], name="a")
        pidx3 = pd.Index([11, 12, 13, 14, 15], name="x")
        psidx1 = ps.from_pandas(pidx1)
        psidx2 = ps.from_pandas(pidx2)
        psidx3 = ps.from_pandas(pidx3)

        self.assert_eq(psidx1 * 10 + psidx2, pidx1 * 10 + pidx2)
        self.assert_eq(psidx1 * 10 + psidx3, pidx1 * 10 + pidx3)

    def test_align(self):
        pdf1 = pd.DataFrame({"a": [1, 2, 3], "b": ["a", "b", "c"]}, index=[10, 20, 30])
        pdf2 = pd.DataFrame({"a": [4, 5, 6], "c": ["d", "e", "f"]}, index=[10, 11, 12])
        psdf1 = ps.from_pandas(pdf1)
        psdf2 = ps.from_pandas(pdf2)

        for join in ["outer", "inner", "left", "right"]:
            for axis in [None, 0]:
                psdf_l, psdf_r = psdf1.align(psdf2, join=join, axis=axis)
                pdf_l, pdf_r = pdf1.align(pdf2, join=join, axis=axis)
                self.assert_eq(psdf_l.sort_index(), pdf_l.sort_index())
                self.assert_eq(psdf_r.sort_index(), pdf_r.sort_index())

        pser1 = pd.Series([7, 8, 9], index=[10, 11, 12])
        pser2 = pd.Series(["g", "h", "i"], index=[10, 20, 30])
        psser1 = ps.from_pandas(pser1)
        psser2 = ps.from_pandas(pser2)

        for join in ["outer", "inner", "left", "right"]:
            psser_l, psser_r = psser1.align(psser2, join=join)
            pser_l, pser_r = pser1.align(pser2, join=join)
            self.assert_eq(psser_l.sort_index(), pser_l.sort_index())
            self.assert_eq(psser_r.sort_index(), pser_r.sort_index())

            psdf_l, psser_r = psdf1.align(psser1, join=join, axis=0)
            pdf_l, pser_r = pdf1.align(pser1, join=join, axis=0)
            self.assert_eq(psdf_l.sort_index(), pdf_l.sort_index())
            self.assert_eq(psser_r.sort_index(), pser_r.sort_index())

            psser_l, psdf_r = psser1.align(psdf1, join=join)
            pser_l, pdf_r = pser1.align(pdf1, join=join)
            self.assert_eq(psser_l.sort_index(), pser_l.sort_index())
            self.assert_eq(psdf_r.sort_index(), pdf_r.sort_index())

        # multi-index columns
        pdf3 = pd.DataFrame(
            {("x", "a"): [4, 5, 6], ("y", "c"): ["d", "e", "f"]}, index=[10, 11, 12]
        )
        psdf3 = ps.from_pandas(pdf3)
        pser3 = pdf3[("y", "c")]
        psser3 = psdf3[("y", "c")]

        for join in ["outer", "inner", "left", "right"]:
            psdf_l, psdf_r = psdf1.align(psdf3, join=join, axis=0)
            pdf_l, pdf_r = pdf1.align(pdf3, join=join, axis=0)
            self.assert_eq(psdf_l.sort_index(), pdf_l.sort_index())
            self.assert_eq(psdf_r.sort_index(), pdf_r.sort_index())

            psser_l, psser_r = psser1.align(psser3, join=join)
            pser_l, pser_r = pser1.align(pser3, join=join)
            self.assert_eq(psser_l.sort_index(), pser_l.sort_index())
            self.assert_eq(psser_r.sort_index(), pser_r.sort_index())

            psdf_l, psser_r = psdf1.align(psser3, join=join, axis=0)
            pdf_l, pser_r = pdf1.align(pser3, join=join, axis=0)
            self.assert_eq(psdf_l.sort_index(), pdf_l.sort_index())
            self.assert_eq(psser_r.sort_index(), pser_r.sort_index())

            psser_l, psdf_r = psser3.align(psdf1, join=join)
            pser_l, pdf_r = pser3.align(pdf1, join=join)
            self.assert_eq(psser_l.sort_index(), pser_l.sort_index())
            self.assert_eq(psdf_r.sort_index(), pdf_r.sort_index())

        self.assertRaises(ValueError, lambda: psdf1.align(psdf3, axis=None))
        self.assertRaises(ValueError, lambda: psdf1.align(psdf3, axis=1))

    def test_pow_and_rpow(self):
        pser = pd.Series([1, 2, np.nan])
        psser = ps.from_pandas(pser)
        pser_other = pd.Series([np.nan, 2, 3])
        psser_other = ps.from_pandas(pser_other)

        self.assert_eq(pser.pow(pser_other), psser.pow(psser_other).sort_index())
        self.assert_eq(pser**pser_other, (psser**psser_other).sort_index())
        self.assert_eq(pser.rpow(pser_other), psser.rpow(psser_other).sort_index())

    def test_shift(self):
        pdf = pd.DataFrame(
            {
                "Col1": [10, 20, 15, 30, 45],
                "Col2": [13, 23, 18, 33, 48],
                "Col3": [17, 27, 22, 37, 52],
            },
            index=np.random.rand(5),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            pdf.shift().loc[pdf["Col1"] == 20].astype(int), psdf.shift().loc[psdf["Col1"] == 20]
        )
        self.assert_eq(
            pdf["Col2"].shift().loc[pdf["Col1"] == 20].astype(int),
            psdf["Col2"].shift().loc[psdf["Col1"] == 20],
        )

    def test_diff(self):
        pdf = pd.DataFrame(
            {
                "Col1": [10, 20, 15, 30, 45],
                "Col2": [13, 23, 18, 33, 48],
                "Col3": [17, 27, 22, 37, 52],
            },
            index=np.random.rand(5),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            pdf.diff().loc[pdf["Col1"] == 20].astype(int), psdf.diff().loc[psdf["Col1"] == 20]
        )
        self.assert_eq(
            pdf["Col2"].diff().loc[pdf["Col1"] == 20].astype(int),
            psdf["Col2"].diff().loc[psdf["Col1"] == 20],
        )

    def test_rank(self):
        pdf = pd.DataFrame(
            {
                "Col1": [10, 20, 15, 30, 45],
                "Col2": [13, 23, 18, 33, 48],
                "Col3": [17, 27, 22, 37, 52],
            },
            index=np.random.rand(5),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(pdf.rank().loc[pdf["Col1"] == 20], psdf.rank().loc[psdf["Col1"] == 20])
        self.assert_eq(
            pdf["Col2"].rank().loc[pdf["Col1"] == 20], psdf["Col2"].rank().loc[psdf["Col1"] == 20]
        )

    def test_cov(self):
        pser1 = pd.Series([0.90010907, 0.13484424, 0.62036035], index=[0, 1, 2])
        pser2 = pd.Series([0.12528585, 0.26962463, 0.51111198], index=[1, 2, 3])
        self._test_cov(pser1, pser2)

        pser1 = pd.Series([0.90010907, 0.13484424, 0.62036035], index=[0, 1, 2])
        pser2 = pd.Series([0.12528585, 0.26962463, 0.51111198, 0.32076008], index=[1, 2, 3, 4])
        self._test_cov(pser1, pser2)

        pser1 = pd.Series([0.90010907, 0.13484424, 0.62036035, 0.32076008], index=[0, 1, 2, 3])
        pser2 = pd.Series([0.12528585, 0.26962463], index=[1, 2])
        self._test_cov(pser1, pser2)

        psser1 = ps.from_pandas(pser1)
        with self.assertRaisesRegex(TypeError, "unsupported type: <class 'list'>"):
            psser1.cov([0.12528585, 0.26962463, 0.51111198])
        with self.assertRaisesRegex(
            TypeError, "unsupported type: <class 'pandas.core.series.Series'>"
        ):
            psser1.cov(pser2)

    def _test_cov(self, pser1, pser2):
        psser1 = ps.from_pandas(pser1)
        psser2 = ps.from_pandas(pser2)

        pcov = pser1.cov(pser2)
        pscov = psser1.cov(psser2)
        self.assert_eq(pcov, pscov, almost=True)

        pcov = pser1.cov(pser2, min_periods=2)
        pscov = psser1.cov(psser2, min_periods=2)
        self.assert_eq(pcov, pscov, almost=True)

        pcov = pser1.cov(pser2, min_periods=3)
        pscov = psser1.cov(psser2, min_periods=3)
        self.assert_eq(pcov, pscov, almost=True)

    def test_corrwith(self):
        df1 = ps.DataFrame({"A": [1, np.nan, 7, 8], "X": [5, 8, np.nan, 3], "C": [10, 4, 9, 3]})
        df2 = ps.DataFrame({"A": [5, 3, 6, 4], "B": [11, 2, 4, 3], "C": [4, 3, 8, np.nan]})
        self._test_corrwith(df1, df2)
        self._test_corrwith((df1 + 1), df2.B)
        self._test_corrwith((df1 + 1), (df2.B + 2))

        # There was a regression in pandas 1.5.0, and fixed in pandas 1.5.1.
        # Therefore, we only test the pandas 1.5.0 in different way.
        # See https://github.com/pandas-dev/pandas/issues/49141 for the reported issue,
        # and https://github.com/pandas-dev/pandas/pull/46174 for the initial PR that causes.
        df_bool = ps.DataFrame({"A": [True, True, False, False], "B": [True, False, False, True]})
        ser_bool = ps.Series([True, True, False, True])
        if LooseVersion(pd.__version__) == LooseVersion("1.5.0"):
            expected = ps.Series([0.5773502691896257, 0.5773502691896257], index=["B", "A"])
            self.assert_eq(df_bool.corrwith(ser_bool), expected, almost=True)
        else:
            self._test_corrwith(df_bool, ser_bool)

        self._test_corrwith(self.psdf1, self.psdf1)
        self._test_corrwith(self.psdf1, self.psdf2)
        self._test_corrwith(self.psdf2, self.psdf3)
        self._test_corrwith(self.psdf3, self.psdf4)

        self._test_corrwith(self.psdf1, self.psdf1.a)
        # There was a regression in pandas 1.5.0, and fixed in pandas 1.5.1.
        # Therefore, we only test the pandas 1.5.0 in different way.
        # See https://github.com/pandas-dev/pandas/issues/49141 for the reported issue,
        # and https://github.com/pandas-dev/pandas/pull/46174 for the initial PR that causes.
        if LooseVersion(pd.__version__) == LooseVersion("1.5.0"):
            expected = ps.Series([-0.08827348295047496, 0.4413674147523748], index=["b", "a"])
            self.assert_eq(self.psdf1.corrwith(self.psdf2.b), expected, almost=True)
        else:
            self._test_corrwith(self.psdf1, self.psdf2.b)

        self._test_corrwith(self.psdf2, self.psdf3.c)
        self._test_corrwith(self.psdf3, self.psdf4.f)

    def _test_corrwith(self, psdf, psobj):
        pdf = psdf._to_pandas()
        pobj = psobj._to_pandas()
        for drop in [True, False]:
            p_corr = pdf.corrwith(pobj, drop=drop)
            ps_corr = psdf.corrwith(psobj, drop=drop)
            self.assert_eq(p_corr.sort_index(), ps_corr.sort_index(), almost=True)

    def test_series_eq(self):
        pser = pd.Series([1, 2, 3, 4, 5, 6], name="x")
        psser = ps.from_pandas(pser)

        # other = Series
        pandas_other = pd.Series([np.nan, 1, 3, 4, np.nan, 6], name="x")
        pandas_on_spark_other = ps.from_pandas(pandas_other)
        self.assert_eq(pser.eq(pandas_other), psser.eq(pandas_on_spark_other).sort_index())
        self.assert_eq(pser == pandas_other, (psser == pandas_on_spark_other).sort_index())

        # other = Series with different Index
        pandas_other = pd.Series(
            [np.nan, 1, 3, 4, np.nan, 6], index=[10, 20, 30, 40, 50, 60], name="x"
        )
        pandas_on_spark_other = ps.from_pandas(pandas_other)
        self.assert_eq(pser.eq(pandas_other), psser.eq(pandas_on_spark_other).sort_index())

        # other = Index
        pandas_other = pd.Index([np.nan, 1, 3, 4, np.nan, 6], name="x")
        pandas_on_spark_other = ps.from_pandas(pandas_other)
        self.assert_eq(pser.eq(pandas_other), psser.eq(pandas_on_spark_other).sort_index())
        self.assert_eq(pser == pandas_other, (psser == pandas_on_spark_other).sort_index())


if __name__ == "__main__":
    from pyspark.pandas.tests.test_ops_on_diff_frames_slow import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
