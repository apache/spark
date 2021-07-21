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
        kcidx = psidx.astype(CategoricalDtype(["c", "a", "b"]))

        self.assert_eq(kcidx.astype("category"), pcidx.astype("category"))

        if LooseVersion(pd.__version__) >= LooseVersion("1.2"):
            self.assert_eq(
                kcidx.astype(CategoricalDtype(["b", "c", "a"])),
                pcidx.astype(CategoricalDtype(["b", "c", "a"])),
            )
        else:
            self.assert_eq(
                kcidx.astype(CategoricalDtype(["b", "c", "a"])),
                pidx.astype(CategoricalDtype(["b", "c", "a"])),
            )

        self.assert_eq(kcidx.astype(str), pcidx.astype(str))

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
        self.assert_eq(
            psidx1.append(psidx3.astype("category")), pidx1.append(pidx3.astype("category"))
        )

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


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.indexes.test_category import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
