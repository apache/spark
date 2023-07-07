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
from pyspark.ml.connect.feature import StandardScaler
from pyspark.ml.connect.classification import LogisticRegression as LORV2
from pyspark.ml.connect.pipeline import Pipeline
from pyspark.ml.connect.tuning import CrossValidator, ParamGridBuilder, CrossValidatorModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.sql import SparkSession


have_torch = True
try:
    import torch  # noqa: F401
except ImportError:
    have_torch = False


class CrossValidatorTestsMixin:
    @staticmethod
    def _check_result(result_dataframe, expected_predictions, expected_probabilities=None):
        np.testing.assert_array_equal(list(result_dataframe.prediction), expected_predictions)
        if "probability" in result_dataframe.columns:
            np.testing.assert_allclose(
                list(result_dataframe.probability),
                expected_probabilities,
                rtol=1e-1,
            )

    def test_crossvalidator(self):
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
            [0.117658, 0.882342],
            [0.878738, 0.121262],
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
            .addGrid(lorv2.learningRate, [0.001, 0.0001])
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
    from pyspark.ml.tests.connect.test_legacy_mode_crossvalidator import *  # noqa: F401,F403

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
