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

from pyspark.ml.evaluation import ClusteringEvaluator, RegressionEvaluator
from pyspark.ml.linalg import Vectors
from pyspark.sql import Row
from pyspark.testing.mlutils import SparkSessionTestCase


class EvaluatorTests(SparkSessionTestCase):
    def test_evaluate_invalid_type(self):
        evaluator = RegressionEvaluator(metricName="r2")
        df = self.spark.createDataFrame([Row(label=1.0, prediction=1.1)])
        invalid_type = ""
        self.assertRaises(TypeError, evaluator.evaluate, df, invalid_type)

    def test_java_params(self):
        """
        This tests a bug fixed by SPARK-18274 which causes multiple copies
        of a Params instance in Python to be linked to the same Java instance.
        """
        evaluator = RegressionEvaluator(metricName="r2")
        df = self.spark.createDataFrame([Row(label=1.0, prediction=1.1)])
        evaluator.evaluate(df)
        self.assertEqual(evaluator._java_obj.getMetricName(), "r2")
        evaluatorCopy = evaluator.copy({evaluator.metricName: "mae"})
        evaluator.evaluate(df)
        evaluatorCopy.evaluate(df)
        self.assertEqual(evaluator._java_obj.getMetricName(), "r2")
        self.assertEqual(evaluatorCopy._java_obj.getMetricName(), "mae")

    def test_clustering_evaluator_with_cosine_distance(self):
        featureAndPredictions = map(
            lambda x: (Vectors.dense(x[0]), x[1]),
            [
                ([1.0, 1.0], 1.0),
                ([10.0, 10.0], 1.0),
                ([1.0, 0.5], 2.0),
                ([10.0, 4.4], 2.0),
                ([-1.0, 1.0], 3.0),
                ([-100.0, 90.0], 3.0),
            ],
        )
        dataset = self.spark.createDataFrame(featureAndPredictions, ["features", "prediction"])
        evaluator = ClusteringEvaluator(predictionCol="prediction", distanceMeasure="cosine")
        self.assertEqual(evaluator.getDistanceMeasure(), "cosine")
        self.assertTrue(np.isclose(evaluator.evaluate(dataset), 0.992671213, atol=1e-5))


if __name__ == "__main__":
    from pyspark.ml.tests.test_evaluation import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
