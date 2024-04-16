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
from pyspark.testing.pandasutils import PandasOnSparkTestCase
from pyspark.testing.sqlutils import SQLTestUtils


class SeriesIndexMixin:
    def test_align(self):
        pdf = pd.DataFrame({"a": [1, 2, 3], "b": ["a", "b", "c"]})
        psdf = ps.from_pandas(pdf)

        for join in ["outer", "inner", "left", "right"]:
            for axis in [None, 0]:
                psser_l, psser_r = psdf.a.align(psdf.b, join=join, axis=axis)
                pser_l, pser_r = pdf.a.align(pdf.b, join=join, axis=axis)
                self.assert_eq(psser_l, pser_l)
                self.assert_eq(psser_r, pser_r)

                psser_l, psdf_r = psdf.b.align(psdf[["b", "a"]], join=join, axis=axis)
                pser_l, pdf_r = pdf.b.align(pdf[["b", "a"]], join=join, axis=axis)
                self.assert_eq(psser_l, pser_l)
                self.assert_eq(psdf_r, pdf_r)

        self.assertRaises(ValueError, lambda: psdf.a.align(psdf.b, axis=1))

    def test_reindex(self):
        index = ["A", "B", "C", "D", "E"]
        pser = pd.Series([1.0, 2.0, 3.0, 4.0, None], index=index, name="x")
        psser = ps.from_pandas(pser)

        self.assert_eq(pser, psser)

        self.assert_eq(
            pser.reindex(["A", "B"]).sort_index(),
            psser.reindex(["A", "B"]).sort_index(),
        )

        self.assert_eq(
            pser.reindex(["A", "B", "2", "3"]).sort_index(),
            psser.reindex(["A", "B", "2", "3"]).sort_index(),
        )

        self.assert_eq(
            pser.reindex(["A", "E", "2"], fill_value=0).sort_index(),
            psser.reindex(["A", "E", "2"], fill_value=0).sort_index(),
        )

        self.assertRaises(TypeError, lambda: psser.reindex(index=123))

    def test_reindex_like(self):
        data = [1.0, 2.0, None]
        index = pd.Index(["A", "B", "C"], name="index1")
        pser = pd.Series(data=data, index=index, name="name1")
        psser = ps.from_pandas(pser)

        # Reindexing single Index on single Index
        data2 = [3.0, None, 4.0]
        index2 = pd.Index(["A", "C", "D"], name="index2")
        pser2 = pd.Series(data=data2, index=index2, name="name2")
        psser2 = ps.from_pandas(pser2)

        self.assert_eq(
            pser.reindex_like(pser2).sort_index(),
            psser.reindex_like(psser2).sort_index(),
        )

        self.assert_eq(
            (pser + 1).reindex_like(pser2).sort_index(),
            (psser + 1).reindex_like(psser2).sort_index(),
        )

        # Reindexing MultiIndex on single Index
        index2 = pd.MultiIndex.from_tuples(
            [("A", "G"), ("C", "D"), ("I", "J")], names=["index3", "index4"]
        )
        pser2 = pd.Series(data=data2, index=index2, name="name2")
        psser2 = ps.from_pandas(pser2)

        self.assert_eq(
            pser.reindex_like(pser2).sort_index(),
            psser.reindex_like(psser2).sort_index(),
        )

        self.assertRaises(TypeError, lambda: psser.reindex_like(index2))
        self.assertRaises(AssertionError, lambda: psser2.reindex_like(psser))

        # Reindexing MultiIndex on MultiIndex
        index = pd.MultiIndex.from_tuples(
            [("A", "B"), ("C", "D"), ("E", "F")], names=["index1", "index2"]
        )
        pser = pd.Series(data=data, index=index, name="name1")
        psser = ps.from_pandas(pser)

        self.assert_eq(
            pser.reindex_like(pser2).sort_index(),
            psser.reindex_like(psser2).sort_index(),
        )

        # Reindexing with DataFrame
        index2 = pd.MultiIndex.from_tuples(
            [("A", "B"), ("C", "D"), ("E", "F")], names=["name3", "name4"]
        )
        pdf = pd.DataFrame(data=data, index=index2)
        psdf = ps.from_pandas(pdf)

        self.assert_eq(
            pser.reindex_like(pdf).sort_index(),
            psser.reindex_like(psdf).sort_index(),
        )

    def test_reset_index(self):
        pdf = pd.DataFrame({"foo": [1, 2, 3, 4]}, index=pd.Index(["a", "b", "c", "d"], name="idx"))
        psdf = ps.from_pandas(pdf)

        pser = pdf.foo
        psser = psdf.foo

        self.assert_eq(psser.reset_index(), pser.reset_index())
        self.assert_eq(psser.reset_index(name="values"), pser.reset_index(name="values"))
        self.assert_eq(psser.reset_index(drop=True), pser.reset_index(drop=True))

        # inplace
        psser.reset_index(drop=True, inplace=True)
        pser.reset_index(drop=True, inplace=True)
        self.assert_eq(psser, pser)
        self.assert_eq(psdf, pdf)

    def test_reset_index_with_default_index_types(self):
        pser = pd.Series([1, 2, 3], name="0", index=np.random.rand(3))
        psser = ps.from_pandas(pser)

        with ps.option_context("compute.default_index_type", "sequence"):
            self.assert_eq(psser.reset_index(), pser.reset_index())

        with ps.option_context("compute.default_index_type", "distributed-sequence"):
            # the order might be changed.
            self.assert_eq(psser.reset_index().sort_index(), pser.reset_index())

        with ps.option_context("compute.default_index_type", "distributed"):
            # the index is different.
            self.assert_eq(
                psser.reset_index()._to_pandas().reset_index(drop=True), pser.reset_index()
            )

    def test_index_to_series_reset_index(self):
        def check(psser, pser):
            self.assert_eq(psser.reset_index(), pser.reset_index())
            self.assert_eq(psser.reset_index(drop=True), pser.reset_index(drop=True))

            pser.reset_index(drop=True, inplace=True)
            psser.reset_index(drop=True, inplace=True)
            self.assert_eq(psser, pser)

        pdf = pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9], "b": [4, 5, 6, 3, 2, 1, 0, 0, 0]},
            index=np.random.rand(9),
        )
        psdf = ps.from_pandas(pdf)
        check(psdf.index.to_series(), pdf.index.to_series())
        check(psdf.index.to_series(name="a"), pdf.index.to_series(name="a"))
        check(psdf.index.to_series(name=("x", "a")), pdf.index.to_series(name=("x", "a")))

    def test_idxmax(self):
        pser = pd.Series(data=[1, 4, 5], index=["A", "B", "C"])
        psser = ps.Series(pser)

        self.assertEqual(psser.idxmax(), pser.idxmax())
        self.assertEqual(psser.idxmax(skipna=False), pser.idxmax(skipna=False))

        index = pd.MultiIndex.from_arrays(
            [["a", "a", "b", "b"], ["c", "d", "e", "f"]], names=("first", "second")
        )
        pser = pd.Series(data=[1, 2, 4, 5], index=index)
        psser = ps.Series(pser)

        self.assertEqual(psser.idxmax(), pser.idxmax())
        self.assertEqual(psser.idxmax(skipna=False), pser.idxmax(skipna=False))

        psser = ps.Series([])
        with self.assertRaisesRegex(ValueError, "an empty sequence"):
            psser.idxmax()

        pser = pd.Series([1, 100, None, 100, 1, 100], index=[10, 3, 5, 2, 1, 8])
        psser = ps.Series(pser)

        self.assertEqual(psser.idxmax(), pser.idxmax())
        self.assertEqual(repr(psser.idxmax(skipna=False)), repr(pser.idxmax(skipna=False)))

    def test_idxmin(self):
        pser = pd.Series(data=[1, 4, 5], index=["A", "B", "C"])
        psser = ps.Series(pser)

        self.assertEqual(psser.idxmin(), pser.idxmin())
        self.assertEqual(psser.idxmin(skipna=False), pser.idxmin(skipna=False))

        index = pd.MultiIndex.from_arrays(
            [["a", "a", "b", "b"], ["c", "d", "e", "f"]], names=("first", "second")
        )
        pser = pd.Series(data=[1, 2, 4, 5], index=index)
        psser = ps.Series(pser)

        self.assertEqual(psser.idxmin(), pser.idxmin())
        self.assertEqual(psser.idxmin(skipna=False), pser.idxmin(skipna=False))

        psser = ps.Series([])
        with self.assertRaisesRegex(ValueError, "an empty sequence"):
            psser.idxmin()

        pser = pd.Series([1, 100, None, 100, 1, 100], index=[10, 3, 5, 2, 1, 8])
        psser = ps.Series(pser)

        self.assertEqual(psser.idxmin(), pser.idxmin())
        self.assertEqual(repr(psser.idxmin(skipna=False)), repr(pser.idxmin(skipna=False)))

    def test_index(self):
        # to check setting name of Index properly.
        idx = pd.Index([1, 2, 3, 4, 5, 6, 7, 8, 9])
        pser = pd.Series([45, 200, 1.2, 30, 250, 1.5, 320, 1, 0.3], index=idx)
        psser = ps.from_pandas(pser)

        psser.name = "koalas"
        pser.name = "koalas"
        self.assert_eq(psser.index.name, pser.index.name)

        # for check setting names of MultiIndex properly.
        psser.names = ["hello", "koalas"]
        pser.names = ["hello", "koalas"]
        self.assert_eq(psser.index.names, pser.index.names)

    def test_axes(self):
        pser = pd.Series([90, 91, 85], index=[2, 4, 1])
        psser = ps.from_pandas(pser)
        self.assert_eq(psser.axes, pser.axes)

        # for MultiIndex
        midx = pd.MultiIndex(
            [["lama", "cow", "falcon"], ["speed", "weight", "length"]],
            [[0, 0, 0, 1, 1, 1, 2, 2, 2], [0, 1, 2, 0, 1, 2, 0, 1, 2]],
        )
        pser = pd.Series([45, 200, 1.2, 30, 250, 1.5, 320, 1, 0.3], index=midx)
        psser = ps.from_pandas(pser)
        self.assert_eq(psser.axes, pser.axes)

    def test_swaplevel(self):
        # MultiIndex with two levels
        arrays = [[1, 1, 2, 2], ["red", "blue", "red", "blue"]]
        pidx = pd.MultiIndex.from_arrays(arrays, names=("number", "color"))
        pser = pd.Series(["a", "b", "c", "d"], index=pidx)
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.swaplevel(), psser.swaplevel())
        self.assert_eq(pser.swaplevel(0, 1), psser.swaplevel(0, 1))
        self.assert_eq(pser.swaplevel(1, 1), psser.swaplevel(1, 1))
        self.assert_eq(pser.swaplevel("number", "color"), psser.swaplevel("number", "color"))

        # MultiIndex with more than two levels
        arrays = [[1, 1, 2, 2], ["red", "blue", "red", "blue"], ["l", "m", "s", "xs"]]
        pidx = pd.MultiIndex.from_arrays(arrays, names=("number", "color", "size"))
        pser = pd.Series(["a", "b", "c", "d"], index=pidx)
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.swaplevel(), psser.swaplevel())
        self.assert_eq(pser.swaplevel(0, 1), psser.swaplevel(0, 1))
        self.assert_eq(pser.swaplevel(0, 2), psser.swaplevel(0, 2))
        self.assert_eq(pser.swaplevel(1, 2), psser.swaplevel(1, 2))
        self.assert_eq(pser.swaplevel(1, 1), psser.swaplevel(1, 1))
        self.assert_eq(pser.swaplevel(-1, -2), psser.swaplevel(-1, -2))
        self.assert_eq(pser.swaplevel("number", "color"), psser.swaplevel("number", "color"))
        self.assert_eq(pser.swaplevel("number", "size"), psser.swaplevel("number", "size"))
        self.assert_eq(pser.swaplevel("color", "size"), psser.swaplevel("color", "size"))

        # Error conditions
        self.assertRaises(AssertionError, lambda: ps.Series([1, 2]).swaplevel())
        self.assertRaises(IndexError, lambda: psser.swaplevel(0, 9))
        self.assertRaises(KeyError, lambda: psser.swaplevel("not_number", "color"))
        self.assertRaises(AssertionError, lambda: psser.swaplevel(copy=False))

    def test_swapaxes(self):
        pser = pd.Series([1, 2, 3], index=["x", "y", "z"], name="ser")
        psser = ps.from_pandas(pser)

        self.assert_eq(psser.swapaxes(0, 0), pser.swapaxes(0, 0))
        self.assert_eq(psser.swapaxes("index", "index"), pser.swapaxes("index", "index"))
        self.assert_eq((psser + 1).swapaxes(0, 0), (pser + 1).swapaxes(0, 0))

        self.assertRaises(AssertionError, lambda: psser.swapaxes(0, 1, copy=False))
        self.assertRaises(ValueError, lambda: psser.swapaxes(0, 1))
        self.assertRaises(ValueError, lambda: psser.swapaxes("index", "columns"))

    def test_droplevel(self):
        pser = pd.Series(
            [1, 2, 3],
            index=pd.MultiIndex.from_tuples(
                [("x", "a", "q"), ("x", "b", "w"), ("y", "c", "e")],
                names=["level_1", "level_2", "level_3"],
            ),
        )
        psser = ps.from_pandas(pser)

        self.assert_eq(pser.droplevel(0), psser.droplevel(0))
        self.assert_eq(pser.droplevel("level_1"), psser.droplevel("level_1"))
        self.assert_eq(pser.droplevel(-1), psser.droplevel(-1))
        self.assert_eq(pser.droplevel([0]), psser.droplevel([0]))
        self.assert_eq(pser.droplevel(["level_1"]), psser.droplevel(["level_1"]))
        self.assert_eq(pser.droplevel((0,)), psser.droplevel((0,)))
        self.assert_eq(pser.droplevel(("level_1",)), psser.droplevel(("level_1",)))
        self.assert_eq(pser.droplevel([0, 2]), psser.droplevel([0, 2]))
        self.assert_eq(
            pser.droplevel(["level_1", "level_3"]), psser.droplevel(["level_1", "level_3"])
        )
        self.assert_eq(pser.droplevel((1, 2)), psser.droplevel((1, 2)))
        self.assert_eq(
            pser.droplevel(("level_2", "level_3")), psser.droplevel(("level_2", "level_3"))
        )

        with self.assertRaisesRegex(KeyError, "Level {0, 1, 2} not found"):
            psser.droplevel({0, 1, 2})
        with self.assertRaisesRegex(KeyError, "Level level_100 not found"):
            psser.droplevel(["level_1", "level_100"])
        with self.assertRaisesRegex(IndexError, "Too many levels: Index has only 3 levels, not 11"):
            psser.droplevel(10)
        with self.assertRaisesRegex(
            IndexError,
            "Too many levels: Index has only 3 levels, -10 is not a valid level number",
        ):
            psser.droplevel(-10)
        with self.assertRaisesRegex(
            ValueError,
            "Cannot remove 3 levels from an index with 3 levels: "
            "at least one level must be left.",
        ):
            psser.droplevel([0, 1, 2])
        with self.assertRaisesRegex(
            ValueError,
            "Cannot remove 5 levels from an index with 3 levels: "
            "at least one level must be left.",
        ):
            psser.droplevel([1, 1, 1, 1, 1])

        # Tupled names
        pser.index.names = [("a", "1"), ("b", "2"), ("c", "3")]
        psser = ps.from_pandas(pser)

        self.assert_eq(
            pser.droplevel([("a", "1"), ("c", "3")]), psser.droplevel([("a", "1"), ("c", "3")])
        )

    def test_last_valid_index(self):
        pser = pd.Series([250, 1.5, 320, 1, 0.3, None, None, None, None])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.last_valid_index(), psser.last_valid_index())

        # MultiIndex columns
        midx = pd.MultiIndex(
            [["lama", "cow", "falcon"], ["speed", "weight", "length"]],
            [[0, 0, 0, 1, 1, 1, 2, 2, 2], [0, 1, 2, 0, 1, 2, 0, 1, 2]],
        )
        pser.index = midx
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.last_valid_index(), psser.last_valid_index())

        # Empty Series
        pser = pd.Series([])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.last_valid_index(), psser.last_valid_index())

    def test_first_valid_index(self):
        # Empty Series
        pser = pd.Series([])
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.first_valid_index(), psser.first_valid_index())

    def test_rename_axis(self):
        index = pd.Index(["A", "B", "C"], name="index")
        pdf = pd.DataFrame({"x": [1.0, 2.0, 3.0]}, index=index)
        psdf = ps.from_pandas(pdf)
        pser, psser = pdf.x, psdf.x

        self.assert_eq(
            pser.rename_axis("index2").sort_index(),
            psser.rename_axis("index2").sort_index(),
        )

        self.assert_eq(
            (pser + 1).rename_axis("index2").sort_index(),
            (psser + 1).rename_axis("index2").sort_index(),
        )

        self.assertRaises(ValueError, lambda: psser.rename_axis(["index2", "index3"]))
        self.assertRaises(TypeError, lambda: psser.rename_axis(mapper=["index2"], index=["index3"]))

        self.assert_eq(
            pser.rename_axis(index={"index": "index2", "missing": "index4"}).sort_index(),
            psser.rename_axis(index={"index": "index2", "missing": "index4"}).sort_index(),
        )

        self.assert_eq(
            pser.rename_axis(index=str.upper).sort_index(),
            psser.rename_axis(index=str.upper).sort_index(),
        )

        pser.rename_axis("index2", inplace=True)
        psser.rename_axis("index2", inplace=True)
        self.assert_eq(pser.sort_index(), psser.sort_index())
        # Note: in pandas, pdf.x's index is renamed, whereas pdf's index isn't due to a bug.
        self.assert_eq(pdf, psdf)

        index = pd.MultiIndex.from_tuples(
            [("A", "B"), ("C", "D"), ("E", "F")], names=["index1", "index2"]
        )
        pser = pd.Series([1.0, 2.0, 3.0], index=index, name="name")
        psser = ps.from_pandas(pser)

        self.assert_eq(
            pser.rename_axis(["index3", "index4"]).sort_index(),
            psser.rename_axis(["index3", "index4"]).sort_index(),
        )

        self.assertRaises(ValueError, lambda: psser.rename_axis(["index3", "index4", "index5"]))

        self.assert_eq(
            pser.rename_axis(
                index={"index1": "index3", "index2": "index4", "missing": "index5"}
            ).sort_index(),
            psser.rename_axis(
                index={"index1": "index3", "index2": "index4", "missing": "index5"}
            ).sort_index(),
        )

        self.assert_eq(
            pser.rename_axis(index=str.upper).sort_index(),
            psser.rename_axis(index=str.upper).sort_index(),
        )


class SeriesIndexTests(
    SeriesIndexMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.series.test_index import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
