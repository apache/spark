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
        kidx = ps.CategoricalIndex([1, 2, 3])

        self.assert_eq(kidx, pidx)
        self.assert_eq(kidx.categories, pidx.categories)
        self.assert_eq(kidx.codes, pd.Index(pidx.codes))
        self.assert_eq(kidx.ordered, pidx.ordered)

        pidx = pd.Index([1, 2, 3], dtype="category")
        kidx = ps.Index([1, 2, 3], dtype="category")

        self.assert_eq(kidx, pidx)
        self.assert_eq(kidx.categories, pidx.categories)
        self.assert_eq(kidx.codes, pd.Index(pidx.codes))
        self.assert_eq(kidx.ordered, pidx.ordered)

        pdf = pd.DataFrame(
            {
                "a": pd.Categorical([1, 2, 3, 1, 2, 3]),
                "b": pd.Categorical(["a", "b", "c", "a", "b", "c"], categories=["c", "b", "a"]),
            },
            index=pd.Categorical([10, 20, 30, 20, 30, 10], categories=[30, 10, 20], ordered=True),
        )
        kdf = ps.from_pandas(pdf)

        pidx = pdf.set_index("b").index
        kidx = kdf.set_index("b").index

        self.assert_eq(kidx, pidx)
        self.assert_eq(kidx.categories, pidx.categories)
        self.assert_eq(kidx.codes, pd.Index(pidx.codes))
        self.assert_eq(kidx.ordered, pidx.ordered)

        pidx = pdf.set_index(["a", "b"]).index.get_level_values(0)
        kidx = kdf.set_index(["a", "b"]).index.get_level_values(0)

        self.assert_eq(kidx, pidx)
        self.assert_eq(kidx.categories, pidx.categories)
        self.assert_eq(kidx.codes, pd.Index(pidx.codes))
        self.assert_eq(kidx.ordered, pidx.ordered)

    def test_astype(self):
        pidx = pd.Index(["a", "b", "c"])
        kidx = ps.from_pandas(pidx)

        self.assert_eq(kidx.astype("category"), pidx.astype("category"))
        self.assert_eq(
            kidx.astype(CategoricalDtype(["c", "a", "b"])),
            pidx.astype(CategoricalDtype(["c", "a", "b"])),
        )

        pcidx = pidx.astype(CategoricalDtype(["c", "a", "b"]))
        kcidx = kidx.astype(CategoricalDtype(["c", "a", "b"]))

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
        kidx = ps.from_pandas(pidx)

        pcodes, puniques = pidx.factorize()
        kcodes, kuniques = kidx.factorize()

        self.assert_eq(kcodes.tolist(), pcodes.tolist())
        self.assert_eq(kuniques, puniques)

        pcodes, puniques = pidx.factorize(na_sentinel=-2)
        kcodes, kuniques = kidx.factorize(na_sentinel=-2)

        self.assert_eq(kcodes.tolist(), pcodes.tolist())
        self.assert_eq(kuniques, puniques)


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.indexes.test_category import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
