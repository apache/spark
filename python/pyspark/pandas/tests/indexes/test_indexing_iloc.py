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
from pyspark.pandas.exceptions import SparkPandasIndexingError, SparkPandasNotImplementedError
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


class IndexingILocMixin:
    @property
    def pdf(self):
        return pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9], "b": [4, 5, 6, 3, 2, 1, 0, 0, 0]},
            index=[0, 1, 3, 5, 6, 8, 9, 9, 9],
        )

    @property
    def pdf2(self):
        return pd.DataFrame(
            {0: [1, 2, 3, 4, 5, 6, 7, 8, 9], 1: [4, 5, 6, 3, 2, 1, 0, 0, 0]},
            index=[0, 1, 3, 5, 6, 8, 9, 9, 9],
        )

    @property
    def psdf(self):
        return ps.from_pandas(self.pdf)

    @property
    def psdf2(self):
        return ps.from_pandas(self.pdf2)

    def test_iloc(self):
        pdf = pd.DataFrame({"A": [1, 2], "B": [3, 4], "C": [5, 6]})
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.iloc[0, 0], pdf.iloc[0, 0])
        for indexer in [0, [0], [0, 1], [1, 0], [False, True, True], slice(0, 1)]:
            self.assert_eq(psdf.iloc[:, indexer], pdf.iloc[:, indexer])
            self.assert_eq(psdf.iloc[:1, indexer], pdf.iloc[:1, indexer])
            self.assert_eq(psdf.iloc[:-1, indexer], pdf.iloc[:-1, indexer])
            # self.assert_eq(psdf.iloc[psdf.index == 2, indexer], pdf.iloc[pdf.index == 2, indexer])

        self.assertRaisesRegex(
            SparkPandasNotImplementedError,
            ".iloc requires numeric slice, conditional boolean",
            lambda: ps.range(10).iloc["a", :],
        )

    def test_iloc_multiindex_columns(self):
        arrays = [np.array(["bar", "bar", "baz", "baz"]), np.array(["one", "two", "one", "two"])]

        pdf = pd.DataFrame(np.random.randn(3, 4), index=["A", "B", "C"], columns=arrays)
        psdf = ps.from_pandas(pdf)

        for indexer in [0, [0], [0, 1], [1, 0], [False, True, True, True], slice(0, 1)]:
            self.assert_eq(psdf.iloc[:, indexer], pdf.iloc[:, indexer])
            self.assert_eq(psdf.iloc[:1, indexer], pdf.iloc[:1, indexer])
            self.assert_eq(psdf.iloc[:-1, indexer], pdf.iloc[:-1, indexer])
            # self.assert_eq(psdf.iloc[psdf.index == "B", indexer],
            #                pdf.iloc[pdf.index == "B", indexer])

    def test_iloc_series(self):
        pser = pd.Series([1, 2, 3])
        psser = ps.from_pandas(pser)

        self.assert_eq(psser.iloc[0], pser.iloc[0])
        self.assert_eq(psser.iloc[:], pser.iloc[:])
        self.assert_eq(psser.iloc[:1], pser.iloc[:1])
        self.assert_eq(psser.iloc[:-1], pser.iloc[:-1])

        self.assert_eq((psser + 1).iloc[0], (pser + 1).iloc[0])
        self.assert_eq((psser + 1).iloc[:], (pser + 1).iloc[:])
        self.assert_eq((psser + 1).iloc[:1], (pser + 1).iloc[:1])
        self.assert_eq((psser + 1).iloc[:-1], (pser + 1).iloc[:-1])

    def test_iloc_slice_rows_sel(self):
        pdf = pd.DataFrame({"A": [1, 2] * 5, "B": [3, 4] * 5, "C": [5, 6] * 5})
        psdf = ps.from_pandas(pdf)

        for rows_sel in [
            slice(None),
            slice(0, 1),
            slice(1, 2),
            slice(-3, None),
            slice(None, -3),
            slice(None, 0),
            slice(None, None, 3),
            slice(3, 8, 2),
            slice(None, None, -2),
            slice(8, 3, -2),
            slice(8, None, -2),
            slice(None, 3, -2),
        ]:
            with self.subTest(rows_sel=rows_sel):
                self.assert_eq(psdf.iloc[rows_sel].sort_index(), pdf.iloc[rows_sel].sort_index())
                self.assert_eq(
                    psdf.A.iloc[rows_sel].sort_index(), pdf.A.iloc[rows_sel].sort_index()
                )
                self.assert_eq(
                    (psdf.A + 1).iloc[rows_sel].sort_index(),
                    (pdf.A + 1).iloc[rows_sel].sort_index(),
                )

    def test_iloc_iterable_rows_sel(self):
        pdf = pd.DataFrame({"A": [1, 2] * 5, "B": [3, 4] * 5, "C": [5, 6] * 5})
        psdf = ps.from_pandas(pdf)

        for rows_sel in [
            [],
            np.array([0, 1]),
            [1, 2],
            np.array([-3]),
            [3],
            np.array([-2]),
            [8, 3, -5],
        ]:
            with self.subTest(rows_sel=rows_sel):
                self.assert_eq(psdf.iloc[rows_sel].sort_index(), pdf.iloc[rows_sel].sort_index())
                self.assert_eq(
                    psdf.A.iloc[rows_sel].sort_index(), pdf.A.iloc[rows_sel].sort_index()
                )
                self.assert_eq(
                    (psdf.A + 1).iloc[rows_sel].sort_index(),
                    (pdf.A + 1).iloc[rows_sel].sort_index(),
                )

            with self.subTest(rows_sel=rows_sel):
                self.assert_eq(
                    psdf.iloc[rows_sel, :].sort_index(), pdf.iloc[rows_sel, :].sort_index()
                )

            with self.subTest(rows_sel=rows_sel):
                self.assert_eq(
                    psdf.iloc[rows_sel, :1].sort_index(), pdf.iloc[rows_sel, :1].sort_index()
                )

    def test_frame_iloc_setitem(self):
        pdf = pd.DataFrame(
            [[1, 2], [4, 5], [7, 8]],
            index=["cobra", "viper", "sidewinder"],
            columns=["max_speed", "shield"],
        )
        psdf = ps.from_pandas(pdf)

        pdf.iloc[[1, 2], [1, 0]] = 10
        psdf.iloc[[1, 2], [1, 0]] = 10
        self.assert_eq(psdf, pdf)

        pdf.iloc[0, 1] = 50
        psdf.iloc[0, 1] = 50
        self.assert_eq(psdf, pdf)

        with self.assertRaisesRegex(ValueError, "setting an array element with a sequence."):
            psdf.iloc[0, 0] = -psdf.max_speed
        with self.assertRaisesRegex(ValueError, "shape mismatch"):
            psdf.iloc[:, [1, 0]] = -psdf.max_speed
        with self.assertRaisesRegex(ValueError, "Only a dataframe with one column can be assigned"):
            psdf.iloc[:, 0] = psdf

        pdf = pd.DataFrame(
            [[1], [4], [7]], index=["cobra", "viper", "sidewinder"], columns=["max_speed"]
        )
        psdf = ps.from_pandas(pdf)

        pdf.iloc[:, 0] = pdf
        psdf.iloc[:, 0] = psdf
        self.assert_eq(psdf, pdf)

    def test_series_iloc_setitem(self):
        pdf = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]}, index=["cobra", "viper", "sidewinder"])
        psdf = ps.from_pandas(pdf)

        pser = pdf.x
        psery = pdf.y
        psser = psdf.x
        pssery = psdf.y

        piloc = pser.iloc
        kiloc = psser.iloc

        pser1 = pser + 1
        psser1 = psser + 1

        for key, value in [
            ([1, 2], 10),
            (1, 50),
            (slice(None), 10),
            (slice(None, 1), 20),
            (slice(1, None), 30),
        ]:
            with self.subTest(key=key, value=value):
                pser.iloc[key] = value
                psser.iloc[key] = value
                self.assert_eq(psser, pser)
                self.assert_eq(psdf, pdf)
                self.assert_eq(pssery, psery)

                piloc[key] = -value
                kiloc[key] = -value
                self.assert_eq(psser, pser)
                self.assert_eq(psdf, pdf)
                self.assert_eq(pssery, psery)

                pser1.iloc[key] = value
                psser1.iloc[key] = value
                self.assert_eq(psser1, pser1)
                self.assert_eq(psdf, pdf)
                self.assert_eq(pssery, psery)

        with self.assertRaises(ValueError):
            psser.iloc[1] = -psser

        pser = pd.Index([1, 2, 3]).to_series()
        psser = ps.Index([1, 2, 3]).to_series()

        pser1 = pser + 1
        psser1 = psser + 1

        pser.iloc[0] = 10
        psser.iloc[0] = 10
        self.assert_eq(psser, pser)

        pser1.iloc[0] = 20
        psser1.iloc[0] = 20
        self.assert_eq(psser1, pser1)

        pdf = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        psdf = ps.from_pandas(pdf)

        pser = pdf.a
        psser = psdf.a

        pser.iloc[[0, 1, 2]] = -pdf.b
        psser.iloc[[0, 1, 2]] = -psdf.b
        self.assert_eq(psser, pser)
        self.assert_eq(psdf, pdf)

        with self.assertRaisesRegex(ValueError, "setting an array element with a sequence."):
            psser.iloc[1] = psdf[["b"]]

    def test_iloc_raises(self):
        pdf = pd.DataFrame({"A": [1, 2], "B": [3, 4], "C": [5, 6]})
        psdf = ps.from_pandas(pdf)

        with self.assertRaisesRegex(SparkPandasIndexingError, "Only accepts pairs of candidates"):
            psdf.iloc[[0, 1], [0, 1], [1, 2]]

        with self.assertRaisesRegex(SparkPandasIndexingError, "Too many indexers"):
            psdf.A.iloc[[0, 1], [0, 1]]

        with self.assertRaisesRegex(TypeError, "cannot do slice indexing with these indexers"):
            psdf.iloc[:"b", :]

        with self.assertRaisesRegex(TypeError, "cannot do slice indexing with these indexers"):
            psdf.iloc[:, :"b"]

        with self.assertRaisesRegex(TypeError, "cannot perform reduce with flexible type"):
            psdf.iloc[:, ["A"]]

        with self.assertRaisesRegex(ValueError, "Location based indexing can only have"):
            psdf.iloc[:, "A"]

        with self.assertRaisesRegex(IndexError, "out of range"):
            psdf.iloc[:, [5, 6]]


class IndexingILocTests(
    IndexingILocMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.indexes.test_indexing_iloc import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
