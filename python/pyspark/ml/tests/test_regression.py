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

import tempfile
import unittest

import numpy as np

from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession
from pyspark.ml.regression import (
    LinearRegression,
    LinearRegressionModel,
    LinearRegressionSummary,
    LinearRegressionTrainingSummary,
)


class RegressionTestsMixin:
    @property
    def df(self):
        return (
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

    def test_linear_regression(self):
        df = self.df
        lr = LinearRegression(
            regParam=0.0,
            maxIter=2,
            solver="normal",
            weightCol="weight",
        )
        self.assertEqual(lr.getRegParam(), 0)
        self.assertEqual(lr.getMaxIter(), 2)
        self.assertEqual(lr.getSolver(), "normal")
        self.assertEqual(lr.getWeightCol(), "weight")

        # Estimator save & load
        with tempfile.TemporaryDirectory(prefix="linear_regression") as d:
            lr.write().overwrite().save(d)
            lr2 = LinearRegression.load(d)
            self.assertEqual(str(lr), str(lr2))

        model = lr.fit(df)
        self.assertEqual(model.numFeatures, 2)
        self.assertTrue(np.allclose(model.scale, 1.0, atol=1e-4))
        self.assertTrue(np.allclose(model.intercept, -0.35, atol=1e-4))
        self.assertTrue(np.allclose(model.coefficients, [0.65, 0.1125], atol=1e-4))

        output = model.transform(df)
        expected_cols = [
            "label",
            "weight",
            "features",
            "prediction",
        ]
        self.assertEqual(output.columns, expected_cols)
        self.assertEqual(output.count(), 4)

        self.assertTrue(
            np.allclose(model.predict(Vectors.dense(0.0, 5.0)), 0.21249999999999963, atol=1e-4)
        )

        # Model summary
        summary = model.summary
        self.assertTrue(isinstance(summary, LinearRegressionSummary))
        self.assertTrue(isinstance(summary, LinearRegressionTrainingSummary))
        self.assertEqual(summary.predictions.columns, expected_cols)
        self.assertEqual(summary.predictions.count(), 4)
        self.assertEqual(summary.residuals.columns, ["residuals"])
        self.assertEqual(summary.residuals.count(), 4)

        self.assertEqual(summary.degreesOfFreedom, 1)
        self.assertEqual(summary.numInstances, 4)
        self.assertEqual(summary.objectiveHistory, [0.0])
        self.assertTrue(
            np.allclose(
                summary.coefficientStandardErrors,
                [1.2859821149611763, 0.6248749874975031, 3.1645497310044184],
                atol=1e-4,
            )
        )
        self.assertTrue(
            np.allclose(
                summary.devianceResiduals, [-0.7424621202458727, 0.7875000000000003], atol=1e-4
            )
        )
        self.assertTrue(
            np.allclose(
                summary.pValues,
                [0.7020630236843428, 0.8866003086182783, 0.9298746994547682],
                atol=1e-4,
            )
        )
        self.assertTrue(
            np.allclose(
                summary.tValues,
                [0.5054502643838291, 0.1800360108036021, -0.11060025272186746],
                atol=1e-4,
            )
        )
        self.assertTrue(np.allclose(summary.explainedVariance, 0.07997500000000031, atol=1e-4))
        self.assertTrue(np.allclose(summary.meanAbsoluteError, 0.4200000000000002, atol=1e-4))
        self.assertTrue(np.allclose(summary.meanSquaredError, 0.20212500000000005, atol=1e-4))
        self.assertTrue(np.allclose(summary.rootMeanSquaredError, 0.44958314025327956, atol=1e-4))
        self.assertTrue(np.allclose(summary.r2, 0.4427212572373862, atol=1e-4))
        self.assertTrue(np.allclose(summary.r2adj, -0.6718362282878414, atol=1e-4))

        summary2 = model.evaluate(df)
        self.assertTrue(isinstance(summary2, LinearRegressionSummary))
        self.assertFalse(isinstance(summary2, LinearRegressionTrainingSummary))
        self.assertEqual(summary2.predictions.columns, expected_cols)
        self.assertEqual(summary2.predictions.count(), 4)
        self.assertEqual(summary2.residuals.columns, ["residuals"])
        self.assertEqual(summary2.residuals.count(), 4)

        self.assertEqual(summary2.degreesOfFreedom, 1)
        self.assertEqual(summary2.numInstances, 4)
        self.assertTrue(
            np.allclose(
                summary2.devianceResiduals, [-0.7424621202458727, 0.7875000000000003], atol=1e-4
            )
        )
        self.assertTrue(np.allclose(summary2.explainedVariance, 0.07997500000000031, atol=1e-4))
        self.assertTrue(np.allclose(summary2.meanAbsoluteError, 0.4200000000000002, atol=1e-4))
        self.assertTrue(np.allclose(summary2.meanSquaredError, 0.20212500000000005, atol=1e-4))
        self.assertTrue(np.allclose(summary2.rootMeanSquaredError, 0.44958314025327956, atol=1e-4))
        self.assertTrue(np.allclose(summary2.r2, 0.4427212572373862, atol=1e-4))
        self.assertTrue(np.allclose(summary2.r2adj, -0.6718362282878414, atol=1e-4))

        # Model save & load
        with tempfile.TemporaryDirectory(prefix="linear_regression_model") as d:
            model.write().overwrite().save(d)
            model2 = LinearRegressionModel.load(d)
            self.assertEqual(str(model), str(model2))


class RegressionTests(RegressionTestsMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.spark = SparkSession.builder.master("local[4]").getOrCreate()

    def tearDown(self) -> None:
        self.spark.stop()


if __name__ == "__main__":
    from pyspark.ml.tests.test_regression import *  # noqa: F401,F403

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
