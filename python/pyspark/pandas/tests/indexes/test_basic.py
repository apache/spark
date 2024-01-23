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
from datetime import datetime

import numpy as np
import pandas as pd

import pyspark.pandas as ps
from pyspark.pandas.exceptions import PandasNotImplementedError
from pyspark.testing.pandasutils import PandasOnSparkTestCase, TestUtils, SPARK_CONF_ARROW_ENABLED


class IndexBasicMixin:
    @property
    def pdf(self):
        return pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9], "b": [4, 5, 6, 3, 2, 1, 0, 0, 0]},
            index=[0, 1, 3, 5, 6, 8, 9, 9, 9],
        )

    @property
    def psdf(self):
        return ps.from_pandas(self.pdf)

    def test_index_basic(self):
        for pdf in [
            pd.DataFrame(np.random.randn(10, 5), index=np.random.randint(100, size=10)),
            pd.DataFrame(
                np.random.randn(10, 5), index=np.random.randint(100, size=10).astype(np.int32)
            ),
            pd.DataFrame(np.random.randn(10, 5), index=np.random.randn(10)),
            pd.DataFrame(np.random.randn(10, 5), index=np.random.randn(10).astype(np.float32)),
            pd.DataFrame(np.random.randn(10, 5), index=list("abcdefghij")),
            pd.DataFrame(
                np.random.randn(10, 5), index=pd.date_range("2011-01-01", freq="D", periods=10)
            ),
            pd.DataFrame(np.random.randn(10, 5), index=pd.Categorical(list("abcdefghij"))),
            pd.DataFrame(np.random.randn(10, 5), columns=list("abcde")).set_index(["a", "b"]),
        ]:
            psdf = ps.from_pandas(pdf)
            self.assert_eq(psdf.index, pdf.index)
            self.assert_eq(psdf.index.dtype, pdf.index.dtype)

        self.assert_eq(ps.Index([])._summary(), "Index: 0 entries")
        with self.assertRaisesRegex(ValueError, "The truth value of a Index is ambiguous."):
            bool(ps.Index([1]))
        with self.assertRaisesRegex(TypeError, "Index.name must be a hashable type"):
            ps.Index([1, 2, 3], name=[(1, 2, 3)])
        with self.assertRaisesRegex(TypeError, "Index.name must be a hashable type"):
            ps.Index([1.0, 2.0, 3.0], name=[(1, 2, 3)])

    def test_multi_index_copy(self):
        arrays = [[1, 1, 2, 2], ["red", "blue", "red", "blue"]]
        idx = pd.MultiIndex.from_arrays(arrays, names=("number", "color"))
        pdf = pd.DataFrame(np.random.randn(4, 5), idx)
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.index.copy(), pdf.index.copy())

    def test_holds_integer(self):
        pidx = pd.Index([1, 2, 3, 4])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.holds_integer(), psidx.holds_integer())

        pidx = pd.Index([1.1, 2.2, 3.3, 4.4])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.holds_integer(), psidx.holds_integer())

        pidx = pd.Index(["A", "B", "C", "D"])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.holds_integer(), psidx.holds_integer())

        # MultiIndex
        pmidx = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "a")])
        psmidx = ps.from_pandas(pmidx)
        self.assert_eq(pmidx.holds_integer(), psmidx.holds_integer())

        pmidx = pd.MultiIndex.from_tuples([(10, 1), (10, 2), (20, 1)])
        psmidx = ps.from_pandas(pmidx)
        self.assert_eq(pmidx.holds_integer(), psmidx.holds_integer())

    def test_item(self):
        pidx = pd.Index([10])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.item(), psidx.item())

        # with timestamp
        pidx = pd.Index([datetime(1990, 3, 9)])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.item(), psidx.item())

        # MultiIndex
        pmidx = pd.MultiIndex.from_tuples([("a", "x")])
        psmidx = ps.from_pandas(pmidx)

        self.assert_eq(pmidx.item(), psmidx.item())

        # MultiIndex with timestamp
        pmidx = pd.MultiIndex.from_tuples([(datetime(1990, 3, 9), datetime(2019, 8, 15))])
        psmidx = ps.from_pandas(pmidx)

        self.assert_eq(pidx.item(), psidx.item())

        err_msg = "can only convert an array of size 1 to a Python scalar"
        with self.assertRaisesRegex(ValueError, err_msg):
            ps.Index([10, 20]).item()
        with self.assertRaisesRegex(ValueError, err_msg):
            ps.MultiIndex.from_tuples([("a", "x"), ("b", "y")]).item()

    def test_inferred_type(self):
        # Integer
        pidx = pd.Index([1, 2, 3])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.inferred_type, psidx.inferred_type)

        # Floating
        pidx = pd.Index([1.0, 2.0, 3.0])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.inferred_type, psidx.inferred_type)

        # String
        pidx = pd.Index(["a", "b", "c"])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.inferred_type, psidx.inferred_type)

        # Boolean
        pidx = pd.Index([True, False, True, False])
        psidx = ps.from_pandas(pidx)
        self.assert_eq(pidx.inferred_type, psidx.inferred_type)

        # MultiIndex
        pmidx = pd.MultiIndex.from_tuples([("a", "x")])
        psmidx = ps.from_pandas(pmidx)
        self.assert_eq(pmidx.inferred_type, psmidx.inferred_type)

    def test_view(self):
        pidx = pd.Index([1, 2, 3, 4], name="Koalas")
        psidx = ps.from_pandas(pidx)

        self.assert_eq(pidx.view(), psidx.view())

        # MultiIndex
        pmidx = pd.MultiIndex.from_tuples([("a", "x"), ("b", "y"), ("c", "z")])
        psmidx = ps.from_pandas(pmidx)

        self.assert_eq(pmidx.view(), psmidx.view())

    def test_index_ops(self):
        pidx = pd.Index([1, 2, 3, 4, 5])
        psidx = ps.from_pandas(pidx)

        self.assert_eq(psidx * 100 + psidx * 10 + psidx, pidx * 100 + pidx * 10 + pidx)

        pidx = pd.Index([1, 2, 3, 4, 5], name="a")
        psidx = ps.from_pandas(pidx)

        self.assert_eq(psidx * 100 + psidx * 10 + psidx, pidx * 100 + pidx * 10 + pidx)

        pdf = pd.DataFrame(
            index=pd.MultiIndex.from_tuples([(1, 2), (3, 4), (5, 6)], names=["a", "b"])
        )
        psdf = ps.from_pandas(pdf)

        pidx1 = pdf.index.get_level_values(0)
        pidx2 = pdf.index.get_level_values(1)
        psidx1 = psdf.index.get_level_values(0)
        psidx2 = psdf.index.get_level_values(1)

        self.assert_eq(psidx1 * 10 + psidx2, pidx1 * 10 + pidx2)

    def test_factorize(self):
        pidx = pd.Index(["a", "b", "a", "b"])
        psidx = ps.from_pandas(pidx)
        pcodes, puniques = pidx.factorize(sort=True)
        kcodes, kuniques = psidx.factorize()
        self.assert_eq(pcodes.tolist(), kcodes.to_list())
        self.assert_eq(puniques, kuniques)

        pmidx = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        psmidx = ps.from_pandas(pmidx)

        self.assertRaises(PandasNotImplementedError, lambda: psmidx.factorize())


class IndexBasicTests(
    IndexBasicMixin,
    PandasOnSparkTestCase,
    TestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.indexes.test_basic import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
