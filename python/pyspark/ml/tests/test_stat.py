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

from pyspark.ml.linalg import Vectors
from pyspark.ml.stat import ChiSquareTest
from pyspark.sql import DataFrame
from pyspark.testing.mlutils import SparkSessionTestCase


class ChiSquareTestTests(SparkSessionTestCase):
    def test_chisquaretest(self):
        data = [
            [0, Vectors.dense([0, 1, 2])],
            [1, Vectors.dense([1, 1, 1])],
            [2, Vectors.dense([2, 1, 0])],
        ]
        df = self.spark.createDataFrame(data, ["label", "feat"])
        res = ChiSquareTest.test(df, "feat", "label")
        # This line is hitting the collect bug described in #17218, commented for now.
        # pValues = res.select("degreesOfFreedom").collect())
        self.assertIsInstance(res, DataFrame)
        fieldNames = set(field.name for field in res.schema.fields)
        expectedFields = ["pValues", "degreesOfFreedom", "statistics"]
        self.assertTrue(all(field in fieldNames for field in expectedFields))


if __name__ == "__main__":
    from pyspark.ml.tests.test_stat import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
