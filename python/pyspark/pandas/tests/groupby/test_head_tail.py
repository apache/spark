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


class GroupbyHeadTailMixin:
    def test_head(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 1, 1, 1, 2, 2, 2, 3, 3, 3] * 3,
                "b": [2, 3, 1, 4, 6, 9, 8, 10, 7, 5] * 3,
                "c": [3, 5, 2, 5, 1, 2, 6, 4, 3, 6] * 3,
            },
            index=np.random.rand(10 * 3),
        )
        psdf = ps.from_pandas(pdf)

        for limit in (2, 100000, -2, -100000, -1):
            self.assert_eq(
                pdf.groupby("a").head(limit).sort_index(),
                psdf.groupby("a").head(limit).sort_index(),
            )
            self.assert_eq(
                pdf.groupby("a")["b"].head(limit).sort_index(),
                psdf.groupby("a")["b"].head(limit).sort_index(),
            )
            self.assert_eq(
                pdf.groupby("a")[["b"]].head(limit).sort_index(),
                psdf.groupby("a")[["b"]].head(limit).sort_index(),
            )

        self.assert_eq(
            pdf.groupby(pdf.a // 2).head(2).sort_index(),
            psdf.groupby(psdf.a // 2).head(2).sort_index(),
        )
        self.assert_eq(
            pdf.groupby(pdf.a // 2)["b"].head(2).sort_index(),
            psdf.groupby(psdf.a // 2)["b"].head(2).sort_index(),
        )
        self.assert_eq(
            pdf.groupby(pdf.a // 2)[["b"]].head(2).sort_index(),
            psdf.groupby(psdf.a // 2)[["b"]].head(2).sort_index(),
        )

        self.assert_eq(
            pdf.b.rename().groupby(pdf.a).head(2).sort_index(),
            psdf.b.rename().groupby(psdf.a).head(2).sort_index(),
        )
        self.assert_eq(
            pdf.b.groupby(pdf.a.rename()).head(2).sort_index(),
            psdf.b.groupby(psdf.a.rename()).head(2).sort_index(),
        )
        self.assert_eq(
            pdf.b.rename().groupby(pdf.a.rename()).head(2).sort_index(),
            psdf.b.rename().groupby(psdf.a.rename()).head(2).sort_index(),
        )

        # multi-index
        midx = pd.MultiIndex(
            [["x", "y"], ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]],
            [[0, 0, 0, 0, 0, 1, 1, 1, 1, 1], [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]],
        )
        pdf = pd.DataFrame(
            {
                "a": [1, 1, 1, 1, 2, 2, 2, 3, 3, 3],
                "b": [2, 3, 1, 4, 6, 9, 8, 10, 7, 5],
                "c": [3, 5, 2, 5, 1, 2, 6, 4, 3, 6],
            },
            columns=["a", "b", "c"],
            index=midx,
        )
        psdf = ps.from_pandas(pdf)

        for limit in (2, 100000, -2, -100000, -1):
            self.assert_eq(
                pdf.groupby("a").head(limit).sort_index(),
                psdf.groupby("a").head(limit).sort_index(),
            )
            self.assert_eq(
                pdf.groupby("a")["b"].head(limit).sort_index(),
                psdf.groupby("a")["b"].head(limit).sort_index(),
            )

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        for limit in (2, 100000, -2, -100000, -1):
            self.assert_eq(
                pdf.groupby(("x", "a")).head(limit).sort_index(),
                psdf.groupby(("x", "a")).head(limit).sort_index(),
            )

    def test_tail(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 1, 1, 1, 2, 2, 2, 3, 3, 3] * 3,
                "b": [2, 3, 1, 4, 6, 9, 8, 10, 7, 5] * 3,
                "c": [3, 5, 2, 5, 1, 2, 6, 4, 3, 6] * 3,
            },
            index=np.random.rand(10 * 3),
        )
        psdf = ps.from_pandas(pdf)

        for limit in (2, 100000, -2, -100000, -1):
            self.assert_eq(
                pdf.groupby("a").tail(limit).sort_index(),
                psdf.groupby("a").tail(limit).sort_index(),
            )
            self.assert_eq(
                pdf.groupby("a")["b"].tail(limit).sort_index(),
                psdf.groupby("a")["b"].tail(limit).sort_index(),
            )
            self.assert_eq(
                pdf.groupby("a")[["b"]].tail(limit).sort_index(),
                psdf.groupby("a")[["b"]].tail(limit).sort_index(),
            )

        self.assert_eq(
            pdf.groupby(pdf.a // 2).tail(2).sort_index(),
            psdf.groupby(psdf.a // 2).tail(2).sort_index(),
        )
        self.assert_eq(
            pdf.groupby(pdf.a // 2)["b"].tail(2).sort_index(),
            psdf.groupby(psdf.a // 2)["b"].tail(2).sort_index(),
        )
        self.assert_eq(
            pdf.groupby(pdf.a // 2)[["b"]].tail(2).sort_index(),
            psdf.groupby(psdf.a // 2)[["b"]].tail(2).sort_index(),
        )

        self.assert_eq(
            pdf.b.rename().groupby(pdf.a).tail(2).sort_index(),
            psdf.b.rename().groupby(psdf.a).tail(2).sort_index(),
        )
        self.assert_eq(
            pdf.b.groupby(pdf.a.rename()).tail(2).sort_index(),
            psdf.b.groupby(psdf.a.rename()).tail(2).sort_index(),
        )
        self.assert_eq(
            pdf.b.rename().groupby(pdf.a.rename()).tail(2).sort_index(),
            psdf.b.rename().groupby(psdf.a.rename()).tail(2).sort_index(),
        )

        # multi-index
        midx = pd.MultiIndex(
            [["x", "y"], ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]],
            [[0, 0, 0, 0, 0, 1, 1, 1, 1, 1], [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]],
        )
        pdf = pd.DataFrame(
            {
                "a": [1, 1, 1, 1, 2, 2, 2, 3, 3, 3],
                "b": [2, 3, 1, 4, 6, 9, 8, 10, 7, 5],
                "c": [3, 5, 2, 5, 1, 2, 6, 4, 3, 6],
            },
            columns=["a", "b", "c"],
            index=midx,
        )
        psdf = ps.from_pandas(pdf)

        for limit in (2, 100000, -2, -100000, -1):
            self.assert_eq(
                pdf.groupby("a").tail(limit).sort_index(),
                psdf.groupby("a").tail(limit).sort_index(),
            )
            self.assert_eq(
                pdf.groupby("a")["b"].tail(limit).sort_index(),
                psdf.groupby("a")["b"].tail(limit).sort_index(),
            )

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        for limit in (2, 100000, -2, -100000, -1):
            self.assert_eq(
                pdf.groupby(("x", "a")).tail(limit).sort_index(),
                psdf.groupby(("x", "a")).tail(limit).sort_index(),
            )


class GroupbyHeadTailTests(
    GroupbyHeadTailMixin,
    PandasOnSparkTestCase,
    SQLTestUtils,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.groupby.test_head_tail import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
