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
import numpy as np
from pandas.api.types import CategoricalDtype

from pyspark import pandas as ps
from pyspark.pandas.config import option_context
from pyspark.pandas.tests.data_type_ops.testing_utils import TestCasesUtils
from pyspark.testing.pandasutils import PandasOnSparkTestCase


class CategoricalOpsTest(PandasOnSparkTestCase, TestCasesUtils):
    @property
    def pser(self):
        return pd.Series([1, "x", "y"], dtype="category")

    @property
    def psser(self):
        return ps.from_pandas(self.pser)

    @property
    def other_pser(self):
        return pd.Series(["y", "x", 1], dtype="category")

    @property
    def other_psser(self):
        return ps.from_pandas(self.other_pser)

    @property
    def ordered_pser(self):
        return pd.Series([1, 2, 3]).astype(CategoricalDtype([3, 2, 1], ordered=True))

    @property
    def ordered_psser(self):
        return ps.from_pandas(self.ordered_pser)

    @property
    def other_ordered_pser(self):
        return pd.Series([2, 1, 3]).astype(CategoricalDtype([3, 2, 1], ordered=True))

    @property
    def other_ordered_psser(self):
        return ps.from_pandas(self.other_ordered_pser)

    @property
    def unordered_psser(self):
        return ps.Series([1, 2, 3]).astype(CategoricalDtype([3, 2, 1]))

    def test_add(self):
        self.assertRaises(TypeError, lambda: self.psser + "x")
        self.assertRaises(TypeError, lambda: self.psser + 1)

        with option_context("compute.ops_on_diff_frames", True):
            for psser in self.pssers:
                self.assertRaises(TypeError, lambda: self.psser + psser)

    def test_sub(self):
        self.assertRaises(TypeError, lambda: self.psser - "x")
        self.assertRaises(TypeError, lambda: self.psser - 1)

        with option_context("compute.ops_on_diff_frames", True):
            for psser in self.pssers:
                self.assertRaises(TypeError, lambda: self.psser - psser)

    def test_mul(self):
        self.assertRaises(TypeError, lambda: self.psser * "x")
        self.assertRaises(TypeError, lambda: self.psser * 1)

        with option_context("compute.ops_on_diff_frames", True):
            for psser in self.pssers:
                self.assertRaises(TypeError, lambda: self.psser * psser)

    def test_truediv(self):
        self.assertRaises(TypeError, lambda: self.psser / "x")
        self.assertRaises(TypeError, lambda: self.psser / 1)

        with option_context("compute.ops_on_diff_frames", True):
            for psser in self.pssers:
                self.assertRaises(TypeError, lambda: self.psser / psser)

    def test_floordiv(self):
        self.assertRaises(TypeError, lambda: self.psser // "x")
        self.assertRaises(TypeError, lambda: self.psser // 1)

        with option_context("compute.ops_on_diff_frames", True):
            for psser in self.pssers:
                self.assertRaises(TypeError, lambda: self.psser // psser)

    def test_mod(self):
        self.assertRaises(TypeError, lambda: self.psser % "x")
        self.assertRaises(TypeError, lambda: self.psser % 1)

        with option_context("compute.ops_on_diff_frames", True):
            for psser in self.pssers:
                self.assertRaises(TypeError, lambda: self.psser % psser)

    def test_pow(self):
        self.assertRaises(TypeError, lambda: self.psser ** "x")
        self.assertRaises(TypeError, lambda: self.psser ** 1)

        with option_context("compute.ops_on_diff_frames", True):
            for psser in self.pssers:
                self.assertRaises(TypeError, lambda: self.psser ** psser)

    def test_radd(self):
        self.assertRaises(TypeError, lambda: "x" + self.psser)
        self.assertRaises(TypeError, lambda: 1 + self.psser)

    def test_rsub(self):
        self.assertRaises(TypeError, lambda: "x" - self.psser)
        self.assertRaises(TypeError, lambda: 1 - self.psser)

    def test_rmul(self):
        self.assertRaises(TypeError, lambda: "x" * self.psser)
        self.assertRaises(TypeError, lambda: 2 * self.psser)

    def test_rtruediv(self):
        self.assertRaises(TypeError, lambda: "x" / self.psser)
        self.assertRaises(TypeError, lambda: 1 / self.psser)

    def test_rfloordiv(self):
        self.assertRaises(TypeError, lambda: "x" // self.psser)
        self.assertRaises(TypeError, lambda: 1 // self.psser)

    def test_rmod(self):
        self.assertRaises(TypeError, lambda: 1 % self.psser)

    def test_rpow(self):
        self.assertRaises(TypeError, lambda: "x" ** self.psser)
        self.assertRaises(TypeError, lambda: 1 ** self.psser)

    def test_and(self):
        self.assertRaises(TypeError, lambda: self.psser & True)
        self.assertRaises(TypeError, lambda: self.psser & False)
        self.assertRaises(TypeError, lambda: self.psser & self.psser)

    def test_rand(self):
        self.assertRaises(TypeError, lambda: True & self.psser)
        self.assertRaises(TypeError, lambda: False & self.psser)

    def test_or(self):
        self.assertRaises(TypeError, lambda: self.psser | True)
        self.assertRaises(TypeError, lambda: self.psser | False)
        self.assertRaises(TypeError, lambda: self.psser | self.psser)

    def test_ror(self):
        self.assertRaises(TypeError, lambda: True | self.psser)
        self.assertRaises(TypeError, lambda: False | self.psser)

    def test_from_to_pandas(self):
        data = [1, "x", "y"]
        pser = pd.Series(data, dtype="category")
        psser = ps.Series(data, dtype="category")
        self.assert_eq(pser, psser.to_pandas())
        self.assert_eq(ps.from_pandas(pser), psser)

    def test_isnull(self):
        self.assert_eq(self.pser.isnull(), self.psser.isnull())

    def test_astype(self):
        data = [1, 2, 3]
        pser = pd.Series(data, dtype="category")
        psser = ps.from_pandas(pser)
        self.assert_eq(pser.astype(int), psser.astype(int))
        self.assert_eq(pser.astype(float), psser.astype(float))
        self.assert_eq(pser.astype(np.float32), psser.astype(np.float32))
        self.assert_eq(pser.astype(np.int32), psser.astype(np.int32))
        self.assert_eq(pser.astype(np.int16), psser.astype(np.int16))
        self.assert_eq(pser.astype(np.int8), psser.astype(np.int8))
        self.assert_eq(pser.astype(str), psser.astype(str))
        self.assert_eq(pser.astype(bool), psser.astype(bool))
        self.assert_eq(pser.astype("category"), psser.astype("category"))
        cat_type = CategoricalDtype(categories=[3, 1, 2])
        if LooseVersion(pd.__version__) >= LooseVersion("1.2"):
            self.assert_eq(pser.astype(cat_type), psser.astype(cat_type))
        else:
            self.assert_eq(pd.Series(data).astype(cat_type), psser.astype(cat_type))

    def test_neg(self):
        self.assertRaises(TypeError, lambda: -self.psser)

    def test_abs(self):
        self.assertRaises(TypeError, lambda: abs(self.psser))

    def test_invert(self):
        self.assertRaises(TypeError, lambda: ~self.psser)

    def test_eq(self):
        with option_context("compute.ops_on_diff_frames", True):
            self.assert_eq(
                self.pser == self.other_pser, (self.psser == self.other_psser).sort_index()
            )
            self.assert_eq(self.pser == self.pser, (self.psser == self.psser).sort_index())

    def test_ne(self):
        with option_context("compute.ops_on_diff_frames", True):
            self.assert_eq(
                self.pser != self.other_pser, (self.psser != self.other_psser).sort_index()
            )
            self.assert_eq(self.pser != self.pser, (self.psser != self.psser).sort_index())

    def test_lt(self):
        ordered_pser = self.ordered_pser
        ordered_psser = self.ordered_psser
        self.assert_eq(ordered_pser < ordered_pser, ordered_psser < ordered_psser)
        with option_context("compute.ops_on_diff_frames", True):
            self.assert_eq(
                ordered_pser < self.other_ordered_pser, ordered_psser < self.other_ordered_psser
            )
            self.assertRaisesRegex(
                TypeError,
                "Unordered Categoricals can only compare equality or not",
                lambda: self.unordered_psser < ordered_psser,
            )
            self.assertRaisesRegex(
                TypeError,
                "Categoricals can only be compared if 'categories' are the same",
                lambda: ordered_psser < self.unordered_psser,
            )
            self.assertRaisesRegex(
                TypeError,
                "Cannot compare a Categorical with the given type",
                lambda: ordered_psser < ps.Series([1, 2, 3]),
            )
        self.assertRaisesRegex(
            TypeError,
            "Cannot compare a Categorical with the given type",
            lambda: ordered_psser < [1, 2, 3],
        )
        self.assertRaisesRegex(
            TypeError, "Cannot compare a Categorical with the given type", lambda: ordered_psser < 1
        )

    def test_le(self):
        ordered_pser = self.ordered_pser
        ordered_psser = self.ordered_psser
        self.assert_eq(ordered_pser <= ordered_pser, ordered_psser <= ordered_psser)

        with option_context("compute.ops_on_diff_frames", True):
            self.assert_eq(
                ordered_pser <= self.other_ordered_pser, ordered_psser <= self.other_ordered_psser
            )
            self.assertRaisesRegex(
                TypeError,
                "Unordered Categoricals can only compare equality or not",
                lambda: self.unordered_psser <= ordered_psser,
            )
            self.assertRaisesRegex(
                TypeError,
                "Categoricals can only be compared if 'categories' are the same",
                lambda: ordered_psser <= self.unordered_psser,
            )
            self.assertRaisesRegex(
                TypeError,
                "Cannot compare a Categorical with the given type",
                lambda: ordered_psser <= ps.Series([1, 2, 3]),
            )
        self.assertRaisesRegex(
            TypeError,
            "Cannot compare a Categorical with the given type",
            lambda: ordered_psser <= [1, 2, 3],
        )
        self.assertRaisesRegex(
            TypeError,
            "Cannot compare a Categorical with the given type",
            lambda: ordered_psser <= 1,
        )

    def test_gt(self):
        ordered_pser = self.ordered_pser
        ordered_psser = self.ordered_psser
        self.assert_eq(ordered_pser > ordered_pser, ordered_psser > ordered_psser)
        with option_context("compute.ops_on_diff_frames", True):
            self.assert_eq(
                ordered_pser > self.other_ordered_pser, ordered_psser > self.other_ordered_psser
            )
            self.assertRaisesRegex(
                TypeError,
                "Unordered Categoricals can only compare equality or not",
                lambda: self.unordered_psser > ordered_psser,
            )
            self.assertRaisesRegex(
                TypeError,
                "Categoricals can only be compared if 'categories' are the same",
                lambda: ordered_psser > self.unordered_psser,
            )
            self.assertRaisesRegex(
                TypeError,
                "Cannot compare a Categorical with the given type",
                lambda: ordered_psser > ps.Series([1, 2, 3]),
            )
        self.assertRaisesRegex(
            TypeError,
            "Cannot compare a Categorical with the given type",
            lambda: ordered_psser > [1, 2, 3],
        )
        self.assertRaisesRegex(
            TypeError, "Cannot compare a Categorical with the given type", lambda: ordered_psser > 1
        )

    def test_ge(self):
        ordered_pser = self.ordered_pser
        ordered_psser = self.ordered_psser
        self.assert_eq(ordered_pser >= ordered_pser, ordered_psser >= ordered_psser)
        with option_context("compute.ops_on_diff_frames", True):
            self.assert_eq(
                ordered_pser >= self.other_ordered_pser, ordered_psser >= self.other_ordered_psser
            )
            self.assertRaisesRegex(
                TypeError,
                "Unordered Categoricals can only compare equality or not",
                lambda: self.unordered_psser >= ordered_psser,
            )
            self.assertRaisesRegex(
                TypeError,
                "Categoricals can only be compared if 'categories' are the same",
                lambda: ordered_psser >= self.unordered_psser,
            )
            self.assertRaisesRegex(
                TypeError,
                "Cannot compare a Categorical with the given type",
                lambda: ordered_psser >= ps.Series([1, 2, 3]),
            )
        self.assertRaisesRegex(
            TypeError,
            "Cannot compare a Categorical with the given type",
            lambda: ordered_psser >= [1, 2, 3],
        )
        self.assertRaisesRegex(
            TypeError,
            "Cannot compare a Categorical with the given type",
            lambda: ordered_psser >= 1,
        )


if __name__ == "__main__":
    import unittest
    from pyspark.pandas.tests.data_type_ops.test_categorical_ops import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
