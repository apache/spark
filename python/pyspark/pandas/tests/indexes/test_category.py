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

import pandas as pd
from pandas.api.types import CategoricalDtype

import pyspark.pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase, TestUtils


class CategoricalIndexTest(PandasOnSparkTestCase, TestUtils):
    def test_categorical_index(self):
        pidx = pd.CategoricalIndex([1, 2, 3])
        psidx = ps.CategoricalIndex([1, 2, 3])

        self.assert_eq(psidx, pidx)
        self.assert_eq(psidx.categories, pidx.categories)
        self.assert_eq(psidx.codes, pd.Index(pidx.codes))
        self.assert_eq(psidx.ordered, pidx.ordered)

        pidx = pd.Index([1, 2, 3], dtype="category")
        psidx = ps.Index([1, 2, 3], dtype="category")

        self.assert_eq(psidx, pidx)
        self.assert_eq(psidx.categories, pidx.categories)
        self.assert_eq(psidx.codes, pd.Index(pidx.codes))
        self.assert_eq(psidx.ordered, pidx.ordered)

        pdf = pd.DataFrame(
            {
                "a": pd.Categorical([1, 2, 3, 1, 2, 3]),
                "b": pd.Categorical(["a", "b", "c", "a", "b", "c"], categories=["c", "b", "a"]),
            },
            index=pd.Categorical([10, 20, 30, 20, 30, 10], categories=[30, 10, 20], ordered=True),
        )
        psdf = ps.from_pandas(pdf)

        pidx = pdf.set_index("b").index
        psidx = psdf.set_index("b").index

        self.assert_eq(psidx, pidx)
        self.assert_eq(psidx.categories, pidx.categories)
        self.assert_eq(psidx.codes, pd.Index(pidx.codes))
        self.assert_eq(psidx.ordered, pidx.ordered)

        pidx = pdf.set_index(["a", "b"]).index.get_level_values(0)
        psidx = psdf.set_index(["a", "b"]).index.get_level_values(0)

        self.assert_eq(psidx, pidx)
        self.assert_eq(psidx.categories, pidx.categories)
        self.assert_eq(psidx.codes, pd.Index(pidx.codes))
        self.assert_eq(psidx.ordered, pidx.ordered)

        with self.assertRaisesRegexp(TypeError, "Index.name must be a hashable type"):
            ps.CategoricalIndex([1, 2, 3], name=[(1, 2, 3)])
        with self.assertRaisesRegexp(
            TypeError, "Cannot perform 'all' with this index type: CategoricalIndex"
        ):
            ps.CategoricalIndex([1, 2, 3]).all()

    def test_categories_setter(self):
        pdf = pd.DataFrame(
            {
                "a": pd.Categorical([1, 2, 3, 1, 2, 3]),
                "b": pd.Categorical(["a", "b", "c", "a", "b", "c"], categories=["c", "b", "a"]),
            },
            index=pd.Categorical([10, 20, 30, 20, 30, 10], categories=[30, 10, 20], ordered=True),
        )
        psdf = ps.from_pandas(pdf)

        pidx = pdf.index
        psidx = psdf.index

        pidx.categories = ["z", "y", "x"]
        psidx.categories = ["z", "y", "x"]
        # Pandas deprecated all the in-place category-setting behaviors, dtypes also not be
        # refreshed in categories.setter since Pandas 1.4+, we should also consider to clean up
        # this test when in-place category-setting removed:
        # https://github.com/pandas-dev/pandas/issues/46820
        if LooseVersion("1.4") >= LooseVersion(pd.__version__) >= LooseVersion("1.1"):
            self.assert_eq(pidx, psidx)
            self.assert_eq(pdf, psdf)
        else:
            pidx = pidx.set_categories(pidx.categories)
            pdf.index = pidx
            self.assert_eq(pidx, psidx)
            self.assert_eq(pdf, psdf)

        with self.assertRaises(ValueError):
            psidx.categories = [1, 2, 3, 4]

    def test_add_categories(self):
        pidx = pd.CategoricalIndex([1, 2, 3], categories=[3, 2, 1])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.add_categories(4), psidx.add_categories(4))
        self.assert_eq(pidx.add_categories([4, 5]), psidx.add_categories([4, 5]))
        self.assert_eq(pidx.add_categories([]), psidx.add_categories([]))

        self.assertRaises(ValueError, lambda: psidx.add_categories(4, inplace=True))
        self.assertRaises(ValueError, lambda: psidx.add_categories(3))
        self.assertRaises(ValueError, lambda: psidx.add_categories([4, 4]))

    def test_remove_categories(self):
        pidx = pd.CategoricalIndex([1, 2, 3], categories=[3, 2, 1])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.remove_categories(2), psidx.remove_categories(2))
        self.assert_eq(pidx.remove_categories([1, 3]), psidx.remove_categories([1, 3]))
        self.assert_eq(pidx.remove_categories([]), psidx.remove_categories([]))
        self.assert_eq(pidx.remove_categories([2, 2]), psidx.remove_categories([2, 2]))
        self.assert_eq(pidx.remove_categories([1, 2, 3]), psidx.remove_categories([1, 2, 3]))
        self.assert_eq(pidx.remove_categories(None), psidx.remove_categories(None))
        self.assert_eq(pidx.remove_categories([None]), psidx.remove_categories([None]))

        self.assertRaises(ValueError, lambda: psidx.remove_categories(4, inplace=True))
        self.assertRaises(ValueError, lambda: psidx.remove_categories(4))
        self.assertRaises(ValueError, lambda: psidx.remove_categories([4, None]))

    def test_remove_unused_categories(self):
        pidx = pd.CategoricalIndex([1, 4, 5, 3], categories=[4, 3, 2, 1])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.remove_unused_categories(), psidx.remove_unused_categories())

        self.assertRaises(ValueError, lambda: psidx.remove_unused_categories(inplace=True))

    def test_reorder_categories(self):
        pidx = pd.CategoricalIndex([1, 2, 3])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.reorder_categories([1, 2, 3]), psidx.reorder_categories([1, 2, 3]))
        self.assert_eq(
            pidx.reorder_categories([1, 2, 3], ordered=True),
            psidx.reorder_categories([1, 2, 3], ordered=True),
        )
        self.assert_eq(pidx.reorder_categories([3, 2, 1]), psidx.reorder_categories([3, 2, 1]))
        self.assert_eq(
            pidx.reorder_categories([3, 2, 1], ordered=True),
            psidx.reorder_categories([3, 2, 1], ordered=True),
        )

        self.assertRaises(ValueError, lambda: psidx.reorder_categories([1, 2, 3], inplace=True))
        self.assertRaises(ValueError, lambda: psidx.reorder_categories([1, 2]))
        self.assertRaises(ValueError, lambda: psidx.reorder_categories([1, 2, 4]))
        self.assertRaises(ValueError, lambda: psidx.reorder_categories([1, 2, 2]))
        self.assertRaises(TypeError, lambda: psidx.reorder_categories(1))

    def test_as_ordered_unordered(self):
        pidx = pd.CategoricalIndex(["x", "y", "z"], categories=["z", "y", "x"])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.as_ordered(), psidx.as_ordered())
        self.assert_eq(pidx.as_unordered(), psidx.as_unordered())

        self.assertRaises(ValueError, lambda: psidx.as_ordered(inplace=True))
        self.assertRaises(ValueError, lambda: psidx.as_unordered(inplace=True))

    def test_astype(self):
        pidx = pd.Index(["a", "b", "c"])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(psidx.astype("category"), pidx.astype("category"))
        self.assert_eq(
            psidx.astype(CategoricalDtype(["c", "a", "b"])),
            pidx.astype(CategoricalDtype(["c", "a", "b"])),
        )

        pcidx = pidx.astype(CategoricalDtype(["c", "a", "b"]))
        pscidx = psidx.astype(CategoricalDtype(["c", "a", "b"]))

        self.assert_eq(pscidx.astype("category"), pcidx.astype("category"))

        # CategoricalDtype is not updated if the dtype is same from pandas 1.3.
        if LooseVersion(pd.__version__) >= LooseVersion("1.3"):
            self.assert_eq(
                pscidx.astype(CategoricalDtype(["b", "c", "a"])),
                pcidx.astype(CategoricalDtype(["b", "c", "a"])),
            )
        else:
            self.assert_eq(
                pscidx.astype(CategoricalDtype(["b", "c", "a"])),
                pcidx,
            )

        self.assert_eq(pscidx.astype(str), pcidx.astype(str))

    def test_factorize(self):
        pidx = pd.CategoricalIndex([1, 2, 3, None])
        psidx = ps.from_pandas(pidx)

        pcodes, puniques = pidx.factorize()
        kcodes, kuniques = psidx.factorize()

        self.assert_eq(kcodes.tolist(), pcodes.tolist())
        self.assert_eq(kuniques, puniques)

        pcodes, puniques = pidx.factorize(na_sentinel=-2)
        kcodes, kuniques = psidx.factorize(na_sentinel=-2)

        self.assert_eq(kcodes.tolist(), pcodes.tolist())
        self.assert_eq(kuniques, puniques)

    def test_append(self):
        pidx1 = pd.CategoricalIndex(["x", "y", "z"], categories=["z", "y", "x", "w"])
        pidx2 = pd.CategoricalIndex(["y", "x", "w"], categories=["z", "y", "x", "w"])
        pidx3 = pd.Index(["y", "x", "w", "z"])
        psidx1 = ps.from_pandas(pidx1)
        psidx2 = ps.from_pandas(pidx2)
        psidx3 = ps.from_pandas(pidx3)

        self.assert_eq(psidx1.append(psidx2), pidx1.append(pidx2))
        if LooseVersion(pd.__version__) >= LooseVersion("1.5.0"):
            self.assert_eq(
                psidx1.append(psidx3.astype("category")), pidx1.append(pidx3.astype("category"))
            )
        else:
            expected_result = ps.CategoricalIndex(
                ["x", "y", "z", "y", "x", "w", "z"],
                categories=["z", "y", "x", "w"],
                ordered=False,
                dtype="category",
            )
            self.assert_eq(psidx1.append(psidx3.astype("category")), expected_result)

        # TODO: append non-categorical or categorical with a different category
        self.assertRaises(NotImplementedError, lambda: psidx1.append(psidx3))

        pidx4 = pd.CategoricalIndex(["y", "x", "w"], categories=["z", "y", "x"])
        psidx4 = ps.from_pandas(pidx4)
        self.assertRaises(NotImplementedError, lambda: psidx1.append(psidx4))

    def test_union(self):
        pidx1 = pd.CategoricalIndex(["x", "y", "z"], categories=["z", "y", "x", "w"])
        pidx2 = pd.CategoricalIndex(["y", "x", "w"], categories=["z", "y", "x", "w"])
        pidx3 = pd.Index(["y", "x", "w", "z"])
        psidx1 = ps.from_pandas(pidx1)
        psidx2 = ps.from_pandas(pidx2)
        psidx3 = ps.from_pandas(pidx3)

        self.assert_eq(psidx1.union(psidx2), pidx1.union(pidx2))
        self.assert_eq(
            psidx1.union(psidx3.astype("category")), pidx1.union(pidx3.astype("category"))
        )

        # TODO: union non-categorical or categorical with a different category
        self.assertRaises(NotImplementedError, lambda: psidx1.union(psidx3))

        pidx4 = pd.CategoricalIndex(["y", "x", "w"], categories=["z", "y", "x"])
        psidx4 = ps.from_pandas(pidx4)
        self.assertRaises(NotImplementedError, lambda: psidx1.union(psidx4))

    def test_intersection(self):
        pidx1 = pd.CategoricalIndex(["x", "y", "z"], categories=["z", "y", "x", "w"])
        pidx2 = pd.CategoricalIndex(["y", "x", "w"], categories=["z", "y", "x", "w"])
        pidx3 = pd.Index(["y", "x", "w", "z"])
        psidx1 = ps.from_pandas(pidx1)
        psidx2 = ps.from_pandas(pidx2)
        psidx3 = ps.from_pandas(pidx3)

        if LooseVersion(pd.__version__) >= LooseVersion("1.2"):
            self.assert_eq(
                psidx1.intersection(psidx2).sort_values(), pidx1.intersection(pidx2).sort_values()
            )
            self.assert_eq(
                psidx1.intersection(psidx3.astype("category")).sort_values(),
                pidx1.intersection(pidx3.astype("category")).sort_values(),
            )
        else:
            self.assert_eq(
                psidx1.intersection(psidx2).sort_values(),
                pidx1.intersection(pidx2).set_categories(pidx1.categories).sort_values(),
            )
            self.assert_eq(
                psidx1.intersection(psidx3.astype("category")).sort_values(),
                pidx1.intersection(pidx3.astype("category"))
                .set_categories(pidx1.categories)
                .sort_values(),
            )

        # TODO: intersection non-categorical or categorical with a different category
        self.assertRaises(NotImplementedError, lambda: psidx1.intersection(psidx3))

        pidx4 = pd.CategoricalIndex(["y", "x", "w"], categories=["z", "y", "x"])
        psidx4 = ps.from_pandas(pidx4)
        self.assertRaises(NotImplementedError, lambda: psidx1.intersection(psidx4))

    def test_insert(self):
        pidx = pd.CategoricalIndex(["x", "y", "z"], categories=["z", "y", "x", "w"])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(psidx.insert(1, "w"), pidx.insert(1, "w"))

    def test_rename_categories(self):
        pidx = pd.CategoricalIndex(["a", "b", "c", "d"])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.rename_categories([0, 1, 3, 2]), psidx.rename_categories([0, 1, 3, 2]))
        self.assert_eq(
            pidx.rename_categories({"a": "A", "c": "C"}),
            psidx.rename_categories({"a": "A", "c": "C"}),
        )
        self.assert_eq(
            pidx.rename_categories(lambda x: x.upper()),
            psidx.rename_categories(lambda x: x.upper()),
        )
        self.assertRaises(
            TypeError,
            lambda: psidx.rename_categories(None),
        )
        self.assertRaises(
            TypeError,
            lambda: psidx.rename_categories(1),
        )
        self.assertRaises(
            TypeError,
            lambda: psidx.rename_categories("x"),
        )
        self.assertRaises(
            ValueError,
            lambda: psidx.rename_categories({"b": "B", "c": "C"}, inplace=True),
        )

    def test_set_categories(self):
        pidx = pd.CategoricalIndex(["a", "b", "c", "d"])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(
            pidx.set_categories(["a", "c", "b", "o"]),
            psidx.set_categories(["a", "c", "b", "o"]),
        )
        self.assert_eq(
            pidx.set_categories(["a", "c", "b"]),
            psidx.set_categories(["a", "c", "b"]),
        )
        self.assert_eq(
            pidx.set_categories(["a", "c", "b", "d", "e"]),
            psidx.set_categories(["a", "c", "b", "d", "e"]),
        )

        self.assert_eq(
            pidx.set_categories([0, 1, 3, 2], rename=True),
            psidx.set_categories([0, 1, 3, 2], rename=True),
        )
        self.assert_eq(
            pidx.set_categories([0, 1, 3], rename=True),
            psidx.set_categories([0, 1, 3], rename=True),
        )
        self.assert_eq(
            pidx.set_categories([0, 1, 3, 2, 4], rename=True),
            psidx.set_categories([0, 1, 3, 2, 4], rename=True),
        )

        self.assert_eq(
            pidx.set_categories(["a", "c", "b", "o"], ordered=True),
            psidx.set_categories(["a", "c", "b", "o"], ordered=True),
        )
        self.assert_eq(
            pidx.set_categories(["a", "c", "b"], ordered=True),
            psidx.set_categories(["a", "c", "b"], ordered=True),
        )
        self.assert_eq(
            pidx.set_categories(["a", "c", "b", "d", "e"], ordered=True),
            psidx.set_categories(["a", "c", "b", "d", "e"], ordered=True),
        )

        self.assertRaisesRegex(
            ValueError,
            "cannot use inplace with CategoricalIndex",
            lambda: psidx.set_categories(["a", "c", "b", "o"], inplace=True),
        )

    def test_map(self):
        pidxs = [pd.CategoricalIndex([1, 2, 3]), pd.CategoricalIndex([1, 2, 3], ordered=True)]
        psidxs = [ps.from_pandas(pidx) for pidx in pidxs]

        for pidx, psidx in zip(pidxs, psidxs):

            # Apply dict
            self.assert_eq(
                pidx.map({1: "one", 2: "two", 3: "three"}),
                psidx.map({1: "one", 2: "two", 3: "three"}),
            )
            self.assert_eq(
                pidx.map({1: "one", 2: "two", 3: "one"}),
                psidx.map({1: "one", 2: "two", 3: "one"}),
            )
            self.assert_eq(
                pidx.map({1: "one", 2: "two"}),
                psidx.map({1: "one", 2: "two"}),
            )
            self.assert_eq(
                pidx.map({1: "one", 2: "two"}),
                psidx.map({1: "one", 2: "two"}),
            )
            self.assert_eq(
                pidx.map({1: 10, 2: 20}),
                psidx.map({1: 10, 2: 20}),
            )

            # Apply lambda
            self.assert_eq(
                pidx.map(lambda id: id + 1),
                psidx.map(lambda id: id + 1),
            )
            self.assert_eq(
                pidx.map(lambda id: id + 1.1),
                psidx.map(lambda id: id + 1.1),
            )
            self.assert_eq(
                pidx.map(lambda id: "{id} + 1".format(id=id)),
                psidx.map(lambda id: "{id} + 1".format(id=id)),
            )

            # Apply series
            pser = pd.Series(["one", "two", "three"], index=[1, 2, 3])
            self.assert_eq(
                pidx.map(pser),
                psidx.map(pser),
            )
            pser = pd.Series(["one", "two", "three"])
            self.assert_eq(
                pidx.map(pser),
                psidx.map(pser),
            )
            self.assert_eq(
                pidx.map(pser),
                psidx.map(pser),
            )
            pser = pd.Series([1, 2, 3])
            self.assert_eq(
                pidx.map(pser),
                psidx.map(pser),
            )

            self.assertRaises(
                TypeError,
                lambda: psidx.map({1: 1, 2: 2.0, 3: "three"}),
            )


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.indexes.test_category import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
