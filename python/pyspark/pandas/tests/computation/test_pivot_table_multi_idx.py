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


class PivotTableMultiIdxMixin:
    def test_pivot_table(self):
        pdf = pd.DataFrame(
            {
                "a": [4, 2, 3, 4, 8, 6],
                "b": [1, 2, 2, 4, 2, 4],
                "e": [10, 20, 20, 40, 20, 40],
                "c": [1, 2, 9, 4, 7, 4],
                "d": [-1, -2, -3, -4, -5, -6],
            },
            index=np.random.rand(6),
        )
        psdf = ps.from_pandas(pdf)

        columns = pd.MultiIndex.from_tuples(
            [("x", "a"), ("x", "b"), ("y", "e"), ("z", "c"), ("w", "d")]
        )
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.pivot_table(columns=("x", "a"), values=("x", "b")).sort_index(),
            pdf.pivot_table(columns=[("x", "a")], values=[("x", "b")]).sort_index(),
            almost=True,
        )

        self.assert_eq(
            psdf.pivot_table(
                index=[("z", "c")], columns=("x", "a"), values=[("x", "b")]
            ).sort_index(),
            pdf.pivot_table(
                index=[("z", "c")], columns=[("x", "a")], values=[("x", "b")]
            ).sort_index(),
            almost=True,
        )

        self.assert_eq(
            psdf.pivot_table(
                index=[("z", "c")], columns=("x", "a"), values=[("x", "b"), ("y", "e")]
            ).sort_index(),
            pdf.pivot_table(
                index=[("z", "c")], columns=[("x", "a")], values=[("x", "b"), ("y", "e")]
            ).sort_index(),
            almost=True,
        )


class PivotTableMultiIdxTests(
    PivotTableMultiIdxMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.computation.test_pivot_table_multi_idx import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
