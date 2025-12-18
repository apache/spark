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

from pyspark.ml.linalg import Vectors
from pyspark.ml.classification import (
    LinearSVC,
    LinearSVCSummary,
    LinearSVCTrainingSummary,
)
from pyspark.ml.regression import (
    LinearRegression,
    LinearRegressionSummary,
    LinearRegressionTrainingSummary,
)
from pyspark.testing.connectutils import ReusedConnectTestCase


class ModelOffloadingTests(ReusedConnectTestCase):
    def test_linear_svc_offloading(self):
        # force clean up the ml cache
        self.spark.client._cleanup_ml_cache()

        df = (
            self.spark.createDataFrame(
                [
                    (1.0, 1.0, Vectors.dense(0.0, 5.0)),
                    (0.0, 2.0, Vectors.dense(1.0, 2.0)),
                    (1.0, 3.0, Vectors.dense(2.0, 1.0)),
                    (0.0, 4.0, Vectors.dense(3.0, 3.0)),
                ],
                ["label", "weight", "features"],
            )
            .coalesce(1)
            .sortWithinPartitions("weight")
        )
        vec = Vectors.dense(0.0, 5.0)

        svc = LinearSVC(maxIter=1, regParam=1.0)
        self.assertEqual(svc.getMaxIter(), 1)
        self.assertEqual(svc.getRegParam(), 1.0)

        model = svc.fit(df)

        # model is cached!
        # 'id: xxx, obj: class org.apache.spark.ml.classification.LinearSVCModel, size: xxx'
        cached = self.spark.client._get_ml_cache_info()
        self.assertEqual(len(cached), 1, cached)
        self.assertIn("class org.apache.spark.ml.classification.LinearSVCModel", cached[0])

        self.assertEqual(svc.uid, model.uid)
        self.assertEqual(model.numClasses, 2)
        self.assertEqual(model.predict(vec), 1.0)

        self.assertTrue(model.hasSummary)
        summary = model.summary()

        self.assertIsInstance(summary, LinearSVCSummary)
        self.assertIsInstance(summary, LinearSVCTrainingSummary)
        self.assertEqual(summary.labels, [0.0, 1.0])

        # model is offloaded!
        self.spark.client._delete_ml_cache([model._java_obj._ref_id], evict_only=True)

        cached = self.spark.client._get_ml_cache_info()
        self.assertEqual(len(cached), 0, cached)

        self.assertEqual(svc.uid, model.uid)
        self.assertEqual(model.numClasses, 2)
        self.assertEqual(model.predict(vec), 1.0)

        self.assertTrue(model.hasSummary)
        summary = model.summary()

        self.assertIsInstance(summary, LinearSVCSummary)
        self.assertIsInstance(summary, LinearSVCTrainingSummary)
        self.assertEqual(summary.labels, [0.0, 1.0])

    def test_linear_regression_offloading(self):
        # force clean up the ml cache
        self.spark.client._cleanup_ml_cache()

        df = (
            self.spark.createDataFrame(
                [
                    (1.0, 1.0, Vectors.dense(0.0, 5.0)),
                    (0.0, 2.0, Vectors.dense(1.0, 2.0)),
                    (1.5, 3.0, Vectors.dense(2.0, 1.0)),
                    (0.7, 4.0, Vectors.dense(1.5, 3.0)),
                ],
                ["label", "weight", "features"],
            )
            .coalesce(1)
            .sortWithinPartitions("weight")
        )
        vec = Vectors.dense(0.0, 5.0)

        lr = LinearRegression(
            regParam=0.0,
            maxIter=2,
            solver="normal",
            weightCol="weight",
        )
        self.assertEqual(lr.getRegParam(), 0)
        self.assertEqual(lr.getMaxIter(), 2)

        model = lr.fit(df)

        # model is cached!
        # 'id: xxx, obj: class org.apache.spark.ml.regression.LinearRegressionModel, size: xxx'
        cached = self.spark.client._get_ml_cache_info()
        self.assertEqual(len(cached), 1, cached)
        self.assertIn("class org.apache.spark.ml.regression.LinearRegressionModel", cached[0])

        self.assertEqual(lr.uid, model.uid)
        self.assertEqual(model.numFeatures, 2)
        self.assertTrue(np.allclose(model.predict(vec), 0.21249999999999963, atol=1e-4))

        summary = model.summary
        self.assertTrue(isinstance(summary, LinearRegressionSummary))
        self.assertTrue(isinstance(summary, LinearRegressionTrainingSummary))
        self.assertEqual(summary.predictions.count(), 4)

        # model is offloaded!
        self.spark.client._delete_ml_cache([model._java_obj._ref_id], evict_only=True)

        cached = self.spark.client._get_ml_cache_info()
        self.assertEqual(len(cached), 0, cached)

        self.assertEqual(lr.uid, model.uid)
        self.assertEqual(model.numFeatures, 2)
        self.assertTrue(np.allclose(model.predict(vec), 0.21249999999999963, atol=1e-4))

        summary = model.summary
        self.assertTrue(isinstance(summary, LinearRegressionSummary))
        self.assertTrue(isinstance(summary, LinearRegressionTrainingSummary))
        self.assertEqual(summary.predictions.count(), 4)


if __name__ == "__main__":
    from pyspark.ml.tests.connect.test_connect_model_offloading import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
