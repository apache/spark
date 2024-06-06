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

import pandas as pd
import numpy as np
import pyspark.pandas as ps
from pyspark.testing.pandasutils import PandasOnSparkTestCase


class RankTestsMixin:
    def test_rank(self):
        pdf = pd.DataFrame(
            {
                "a": [1, 2, 3, 4, 5, 6] * 3,
                "b": [1, 1, 2, 3, 5, 8] * 3,
                "c": [1, 4, 9, 16, 25, 36] * 3,
            },
            index=np.random.rand(6 * 3),
        )
        psdf = ps.from_pandas(pdf)

        self.assert_eq(psdf.groupby("b").rank().sort_index(), pdf.groupby("b").rank().sort_index())
        self.assert_eq(
            psdf.groupby(["a", "b"]).rank().sort_index(),
            pdf.groupby(["a", "b"]).rank().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["b"])["a"].rank().sort_index(),
            pdf.groupby(["b"])["a"].rank().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(["b"])[["a", "c"]].rank().sort_index(),
            pdf.groupby(["b"])[["a", "c"]].rank().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5).rank().sort_index(),
            pdf.groupby(pdf.b // 5).rank().sort_index(),
        )
        self.assert_eq(
            psdf.groupby(psdf.b // 5)["a"].rank().sort_index(),
            pdf.groupby(pdf.b // 5)["a"].rank().sort_index(),
        )

        self.assert_eq(psdf.groupby("b").rank().sum(), pdf.groupby("b").rank().sum())
        self.assert_eq(psdf.groupby(["b"])["a"].rank().sum(), pdf.groupby(["b"])["a"].rank().sum())

        # multi-index columns
        columns = pd.MultiIndex.from_tuples([("x", "a"), ("x", "b"), ("y", "c")])
        pdf.columns = columns
        psdf.columns = columns

        self.assert_eq(
            psdf.groupby(("x", "b")).rank().sort_index(),
            pdf.groupby(("x", "b")).rank().sort_index(),
        )
        self.assert_eq(
            psdf.groupby([("x", "a"), ("x", "b")]).rank().sort_index(),
            pdf.groupby([("x", "a"), ("x", "b")]).rank().sort_index(),
        )


class RankTests(
    RankTestsMixin,
    PandasOnSparkTestCase,
):
    pass


if __name__ == "__main__":
    from pyspark.pandas.tests.groupby.test_rank import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
