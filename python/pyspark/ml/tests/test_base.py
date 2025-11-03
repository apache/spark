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

from pyspark.sql.types import DoubleType, IntegerType
from pyspark.testing.mlutils import (
    MockDataset,
    MockEstimator,
    MockUnaryTransformer,
    MockTransformer,
    SparkSessionTestCase,
)


class TransformerTests(unittest.TestCase):
    def test_transform_invalid_type(self):
        transformer = MockTransformer()
        data = MockDataset()
        self.assertRaises(TypeError, transformer.transform, data, "")


class UnaryTransformerTests(SparkSessionTestCase):
    def test_unary_transformer_validate_input_type(self):
        shiftVal = 3
        transformer = (
            MockUnaryTransformer(shiftVal=shiftVal).setInputCol("input").setOutputCol("output")
        )

        # should not raise any errors
        transformer.validateInputType(DoubleType())

        with self.assertRaises(TypeError):
            # passing the wrong input type should raise an error
            transformer.validateInputType(IntegerType())

    def test_unary_transformer_transform(self):
        shiftVal = 3
        transformer = (
            MockUnaryTransformer(shiftVal=shiftVal).setInputCol("input").setOutputCol("output")
        )

        df = self.spark.range(0, 10).toDF("input")
        df = df.withColumn("input", df.input.cast(dataType="double"))

        transformed_df = transformer.transform(df)
        results = transformed_df.select("input", "output").collect()

        for res in results:
            self.assertEqual(res.input + shiftVal, res.output)


class EstimatorTest(unittest.TestCase):
    def setUp(self):
        self.estimator = MockEstimator()
        self.data = MockDataset()

    def test_fit_invalid_params(self):
        invalid_type_parms = ""
        self.assertRaises(TypeError, self.estimator.fit, self.data, invalid_type_parms)

    def testDefaultFitMultiple(self):
        N = 4
        params = [{self.estimator.fake: i} for i in range(N)]
        modelIter = self.estimator.fitMultiple(self.data, params)
        indexList = []
        for index, model in modelIter:
            self.assertEqual(model.getFake(), index)
            indexList.append(index)
        self.assertEqual(sorted(indexList), list(range(N)))


if __name__ == "__main__":
    from pyspark.ml.tests.test_base import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
