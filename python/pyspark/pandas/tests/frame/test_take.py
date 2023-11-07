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
from pyspark.testing.pandasutils import ComparisonTestBase
from pyspark.testing.sqlutils import SQLTestUtils


class FrameTakeMixin:
    @property
    def pdf(self):
        return pd.DataFrame(
            {"a": [1, 2, 3, 4, 5, 6, 7, 8, 9], "b": [4, 5, 6, 3, 2, 1, 0, 0, 0]},
            index=np.random.rand(9),
        )

    @property
    def df_pair(self):
        pdf = self.pdf
        psdf = ps.from_pandas(pdf)
        return pdf, psdf

    def test_take(self):
        pdf = pd.DataFrame(
            {"A": range(0, 50000), "B": range(100000, 0, -2), "C": range(100000, 50000, -1)}
        )
        psdf = ps.from_pandas(pdf)

        # axis=0 (default)
        self.assert_eq(psdf.take([1, 2]).sort_index(), pdf.take([1, 2]).sort_index())
        self.assert_eq(psdf.take([-1, -2]).sort_index(), pdf.take([-1, -2]).sort_index())
        self.assert_eq(
            psdf.take(range(100, 110)).sort_index(), pdf.take(range(100, 110)).sort_index()
        )
        self.assert_eq(
            psdf.take(range(-110, -100)).sort_index(), pdf.take(range(-110, -100)).sort_index()
        )
        self.assert_eq(
            psdf.take([10, 100, 1000, 10000]).sort_index(),
            pdf.take([10, 100, 1000, 10000]).sort_index(),
        )
        self.assert_eq(
            psdf.take([-10, -100, -1000, -10000]).sort_index(),
            pdf.take([-10, -100, -1000, -10000]).sort_index(),
        )

        # axis=1
        self.assert_eq(
            psdf.take([1, 2], axis=1).sort_index(), pdf.take([1, 2], axis=1).sort_index()
        )
        self.assert_eq(
            psdf.take([-1, -2], axis=1).sort_index(), pdf.take([-1, -2], axis=1).sort_index()
        )
        self.assert_eq(
            psdf.take(range(1, 3), axis=1).sort_index(),
            pdf.take(range(1, 3), axis=1).sort_index(),
        )
        self.assert_eq(
            psdf.take(range(-1, -3), axis=1).sort_index(),
            pdf.take(range(-1, -3), axis=1).sort_index(),
        )
        self.assert_eq(
            psdf.take([2, 1], axis=1).sort_index(),
            pdf.take([2, 1], axis=1).sort_index(),
        )
        self.assert_eq(
            psdf.take([-1, -2], axis=1).sort_index(),
            pdf.take([-1, -2], axis=1).sort_index(),
        )

        # MultiIndex columns
        columns = pd.MultiIndex.from_tuples([("A", "Z"), ("B", "X"), ("C", "C")])
        psdf.columns = columns
        pdf.columns = columns

        # MultiIndex columns with axis=0 (default)
        self.assert_eq(psdf.take([1, 2]).sort_index(), pdf.take([1, 2]).sort_index())
        self.assert_eq(psdf.take([-1, -2]).sort_index(), pdf.take([-1, -2]).sort_index())
        self.assert_eq(
            psdf.take(range(100, 110)).sort_index(), pdf.take(range(100, 110)).sort_index()
        )
        self.assert_eq(
            psdf.take(range(-110, -100)).sort_index(), pdf.take(range(-110, -100)).sort_index()
        )
        self.assert_eq(
            psdf.take([10, 100, 1000, 10000]).sort_index(),
            pdf.take([10, 100, 1000, 10000]).sort_index(),
        )
        self.assert_eq(
            psdf.take([-10, -100, -1000, -10000]).sort_index(),
            pdf.take([-10, -100, -1000, -10000]).sort_index(),
        )

        # axis=1
        self.assert_eq(
            psdf.take([1, 2], axis=1).sort_index(), pdf.take([1, 2], axis=1).sort_index()
        )
        self.assert_eq(
            psdf.take([-1, -2], axis=1).sort_index(), pdf.take([-1, -2], axis=1).sort_index()
        )
        self.assert_eq(
            psdf.take(range(1, 3), axis=1).sort_index(),
            pdf.take(range(1, 3), axis=1).sort_index(),
        )
        self.assert_eq(
            psdf.take(range(-1, -3), axis=1).sort_index(),
            pdf.take(range(-1, -3), axis=1).sort_index(),
        )
        self.assert_eq(
            psdf.take([2, 1], axis=1).sort_index(),
            pdf.take([2, 1], axis=1).sort_index(),
        )
        self.assert_eq(
            psdf.take([-1, -2], axis=1).sort_index(),
            pdf.take([-1, -2], axis=1).sort_index(),
        )

        # Checking the type of indices.
        self.assertRaises(TypeError, lambda: psdf.take(1))
        self.assertRaises(TypeError, lambda: psdf.take("1"))
        self.assertRaises(TypeError, lambda: psdf.take({1, 2}))
        self.assertRaises(TypeError, lambda: psdf.take({1: None, 2: None}))


class FrameTakeTests(FrameTakeMixin, ComparisonTestBase, SQLTestUtils):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.frame.test_take import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
