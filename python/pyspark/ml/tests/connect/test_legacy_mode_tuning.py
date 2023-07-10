# -*- coding: utf-8 -*-
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
import os
import tempfile
import unittest
import numpy as np
from pyspark.ml.param import Param, Params
from pyspark.ml.connect import Model, Estimator
from pyspark.ml.connect.feature import StandardScaler
from pyspark.ml.connect.classification import LogisticRegression as LORV2
from pyspark.ml.connect.pipeline import Pipeline
from pyspark.ml.connect.tuning import CrossValidator, ParamGridBuilder, CrossValidatorModel
from pyspark.ml.connect.evaluation import BinaryClassificationEvaluator, RegressionEvaluator
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand


have_torch = True
try:
    import torch  # noqa: F401
except ImportError:
    have_torch = False


class HasInducedError(Params):
    def __init__(self):
        super(HasInducedError, self).__init__()
        self.inducedError = Param(
            self, "inducedError", "Uniformly-distributed error added to feature"
        )

    def getInducedError(self):
        return self.getOrDefault(self.inducedError)


class InducedErrorModel(Model, HasInducedError):
    def __init__(self):
        super(InducedErrorModel, self).__init__()

    def _transform(self, dataset):
        return dataset.withColumn(
            "prediction", dataset.feature + (rand(0) * self.getInducedError())
        )


class InducedErrorEstimator(Estimator, HasInducedError):
    def __init__(self, inducedError=1.0):
        super(InducedErrorEstimator, self).__init__()
        self._set(inducedError=inducedError)

    def _fit(self, dataset):
        model = InducedErrorModel()
        self._copyValues(model)
        return model


class CrossValidatorTestsMixin:
    def test_gen_avg_and_std_metrics(self):
        metrics_all = [
            [1.0, 3.0, 2.0, 4.0],
            [3.0, 2.0, 2.0, 4.0],
            [3.0, 2.5, 2.1, 8.0],
        ]
        avg_metrics, std_metrics = CrossValidator._gen_avg_and_std_metrics(metrics_all)
        assert np.allclose(avg_metrics, [2.33333333, 2.5, 2.03333333, 5.33333333])
        assert np.allclose(std_metrics, [0.94280904, 0.40824829, 0.04714045, 1.88561808])
        assert isinstance(avg_metrics, list)
        assert isinstance(std_metrics, list)

    def test_copy(self):
        dataset = self.spark.createDataFrame(
            [(10, 10.0), (50, 50.0), (100, 100.0), (500, 500.0)] * 10, ["feature", "label"]
        )

        iee = InducedErrorEstimator()
        evaluator = RegressionEvaluator(metricName="rmse")

        grid = ParamGridBuilder().addGrid(iee.inducedError, [100.0, 0.0, 10000.0]).build()
        cv = CrossValidator(
            estimator=iee,
            estimatorParamMaps=grid,
            evaluator=evaluator,
            numFolds=2,
        )
        cvCopied = cv.copy()
        for param in [
            lambda x: x.getEstimator().uid,
            # SPARK-32092: CrossValidator.copy() needs to copy all existing params
            lambda x: x.getNumFolds(),
            lambda x: x.getFoldCol(),
            lambda x: x.getParallelism(),
            lambda x: x.getSeed(),
        ]:
            self.assertEqual(param(cv), param(cvCopied))

        cvModel = cv.fit(dataset)
        cvModelCopied = cvModel.copy()
        for index in range(len(cvModel.avgMetrics)):
            self.assertTrue(
                abs(cvModel.avgMetrics[index] - cvModelCopied.avgMetrics[index]) < 0.0001
            )
        self.assertTrue(np.allclose(cvModel.stdMetrics, cvModelCopied.stdMetrics))
        # SPARK-32092: CrossValidatorModel.copy() needs to copy all existing params
        for param in [lambda x: x.getNumFolds(), lambda x: x.getFoldCol(), lambda x: x.getSeed()]:
            self.assertEqual(param(cvModel), param(cvModelCopied))

        cvModel.avgMetrics[0] = "foo"
        self.assertNotEqual(
            cvModelCopied.avgMetrics[0],
            "foo",
            "Changing the original avgMetrics should not affect the copied model",
        )
        cvModel.stdMetrics[0] = "foo"
        self.assertNotEqual(
            cvModelCopied.stdMetrics[0],
            "foo",
            "Changing the original stdMetrics should not affect the copied model",
        )

    def test_fit_minimize_metric(self):
        dataset = self.spark.createDataFrame(
            [(10, 10.0), (50, 50.0), (100, 100.0), (500, 500.0)] * 10, ["feature", "label"]
        )

        iee = InducedErrorEstimator()
        evaluator = RegressionEvaluator(metricName="rmse")

        grid = ParamGridBuilder().addGrid(iee.inducedError, [100.0, 0.0, 10000.0]).build()
        cv = CrossValidator(estimator=iee, estimatorParamMaps=grid, evaluator=evaluator)
        cvModel = cv.fit(dataset)
        bestModel = cvModel.bestModel
        bestModelMetric = evaluator.evaluate(bestModel.transform(dataset))

        self.assertEqual(
            0.0, bestModel.getOrDefault("inducedError"), "Best model should have zero induced error"
        )
        self.assertEqual(0.0, bestModelMetric, "Best model has RMSE of 0")

    def test_fit_maximize_metric(self):
        dataset = self.spark.createDataFrame(
            [(10, 10.0), (50, 50.0), (100, 100.0), (500, 500.0)] * 10, ["feature", "label"]
        )

        iee = InducedErrorEstimator()
        evaluator = RegressionEvaluator(metricName="r2")

        grid = ParamGridBuilder().addGrid(iee.inducedError, [100.0, 0.0, 10000.0]).build()
        cv = CrossValidator(estimator=iee, estimatorParamMaps=grid, evaluator=evaluator)
        cvModel = cv.fit(dataset)
        bestModel = cvModel.bestModel
        bestModelMetric = evaluator.evaluate(bestModel.transform(dataset))

        self.assertEqual(
            0.0, bestModel.getOrDefault("inducedError"), "Best model should have zero induced error"
        )
        self.assertEqual(1.0, bestModelMetric, "Best model has R-squared of 1")

    @staticmethod
    def _check_result(result_dataframe, expected_predictions, expected_probabilities=None):
        np.testing.assert_array_equal(list(result_dataframe.prediction), expected_predictions)
        if "probability" in result_dataframe.columns:
            np.testing.assert_allclose(
                list(result_dataframe.probability),
                expected_probabilities,
                rtol=1e-1,
            )

    def test_crossvalidator_on_pipeline(self):
        train_dataset = self.spark.createDataFrame(
            [
                (1.0, [0.0, 5.0]),
                (0.0, [1.0, 2.0]),
                (1.0, [2.0, 1.0]),
                (0.0, [3.0, 3.0]),
            ]
            * 100,
            ["label", "features"],
            )
        eval_dataset = self.spark.createDataFrame(
            [
                ([0.0, 2.0],),
                ([3.5, 3.0],),
            ],
            ["features"],
        )

        lorv2 = LORV2()
        grid1 = ParamGridBuilder().addGrid(lorv2.maxIter, [2, 200]).build()

        cv1 = CrossValidator(
            estimator=LORV2(),
            estimatorParamMaps=grid1,
            parallelism=2,
            evaluator=BinaryClassificationEvaluator(),
        )
        cv1_model = cv1.fit(train_dataset)

        expected_predictions1 = [1, 0]
        expected_probabilities1 = [
            [0.247183, 0.752817],
            [0.837707, 0.162293],
        ]

        result1 = cv1_model.transform(eval_dataset).toPandas()
        self._check_result(result1, expected_predictions1, expected_probabilities1)
        local_transform_result1 = cv1_model.transform(eval_dataset.toPandas())
        self._check_result(local_transform_result1, expected_predictions1, expected_probabilities1)

        scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
        lorv2 = LORV2(
            numTrainWorkers=2, featuresCol="scaled_features"
        )
        pipeline = Pipeline(stages=[scaler, lorv2])

        grid2 = (
            ParamGridBuilder()
            .addGrid(lorv2.maxIter, [2, 200])
            .addGrid(lorv2.learningRate, [0.001, 0.00001])
            .build()
        )
        cv2 = CrossValidator(
            estimator=pipeline,
            estimatorParamMaps=grid2,
            parallelism=2,
            evaluator=BinaryClassificationEvaluator(),
        )
        cv2_model = cv2.fit(train_dataset)

        expected_predictions2 = [1, 0]
        expected_probabilities2 = [
            [0.117658, 0.882342],
            [0.878738, 0.121262],
        ]

        result2 = cv2_model.transform(eval_dataset).toPandas()
        self._check_result(result2, expected_predictions2, expected_probabilities2)
        local_transform_result2 = cv2_model.transform(eval_dataset.toPandas())
        self._check_result(local_transform_result2, expected_predictions2, expected_probabilities2)


class CrossValidatorTests(CrossValidatorTestsMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.spark = SparkSession.builder.master("local[2]").getOrCreate()

    def tearDown(self) -> None:
        self.spark.stop()


if __name__ == "__main__":
    from pyspark.ml.tests.connect.test_legacy_mode_tuning import *  # noqa: F401,F403

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
