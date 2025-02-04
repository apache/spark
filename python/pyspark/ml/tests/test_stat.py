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

import numpy as np
import unittest

from pyspark.ml.linalg import Vectors
from pyspark.ml.stat import ChiSquareTest, Correlation
from pyspark.sql import DataFrame
from pyspark.testing.sqlutils import ReusedSQLTestCase


class StatTestsMixin:
    def test_chisquaretest(self):
        spark = self.spark
        data = [
            [0, Vectors.dense([0, 1, 2])],
            [1, Vectors.dense([1, 1, 1])],
            [2, Vectors.dense([2, 1, 0])],
        ]
        df = spark.createDataFrame(data, ["label", "feat"])
        res = ChiSquareTest.test(df, "feat", "label")
        # This line is hitting the collect bug described in #17218, commented for now.
        # pValues = res.select("degreesOfFreedom").collect())
        self.assertIsInstance(res, DataFrame)
        fieldNames = set(field.name for field in res.schema.fields)
        expectedFields = ["pValues", "degreesOfFreedom", "statistics"]
        self.assertTrue(all(field in fieldNames for field in expectedFields))

        self.assertEqual(res.columns, ["pValues", "degreesOfFreedom", "statistics"])
        self.assertEqual(res.count(), 1)

        res2 = ChiSquareTest.test(df, "feat", "label", True)
        self.assertEqual(res2.columns, ["featureIndex", "pValue", "degreesOfFreedom", "statistic"])
        self.assertEqual(res2.count(), 3)

    def test_correlation(self):
        spark = self.spark
        data = [
            [Vectors.dense([1, 0, 0, -2])],
            [Vectors.dense([4, 5, 0, 3])],
            [Vectors.dense([6, 7, 0, 8])],
            [Vectors.dense([9, 0, 0, 1])],
        ]
        df = spark.createDataFrame(data, ["features"])

        res1 = Correlation.corr(df, "features")
        self.assertEqual(res1.columns, ["pearson(features)"])
        self.assertEqual(res1.count(), 1)
        corr1 = res1.head()[0]
        self.assertTrue(
            np.allclose(
                corr1.toArray(),
                [
                    [1.0, 0.05564149, float("nan"), 0.40047142],
                    [0.05564149, 1.0, float("nan"), 0.91359586],
                    [float("nan"), float("nan"), 1.0, float("nan")],
                    [0.40047142, 0.91359586, float("nan"), 1.0],
                ],
                atol=1e-4,
                equal_nan=True,
            ),
            corr1,
        )

        res2 = Correlation.corr(df, "features", "spearman")
        self.assertEqual(res2.columns, ["spearman(features)"])
        self.assertEqual(res2.count(), 1)
        corr2 = res2.head()[0]
        self.assertTrue(
            np.allclose(
                corr2.toArray(),
                [
                    [1.0, 0.10540926, float("nan"), 0.4],
                    [0.10540926, 1.0, float("nan"), 0.9486833],
                    [float("nan"), float("nan"), 1.0, float("nan")],
                    [0.4, 0.9486833, float("nan"), 1.0],
                ],
                atol=1e-4,
                equal_nan=True,
            ),
            corr2,
        )


class StatTests(StatTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.ml.tests.test_stat import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
