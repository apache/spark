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

import unittest
import numpy as np
from pyspark.mlv2.classification import LogisticRegression as LORV2
from pyspark.sql import SparkSession


have_torch = True
try:
    import torch  # noqa: F401
except ImportError:
    have_torch = False


class ClassificationTestsMixin:
    @staticmethod
    def _check_result(result_dataframe, expected_predictions, expected_probabilities=None):
        np.testing.assert_array_equal(list(result_dataframe.prediction), expected_predictions)
        if "probability" in result_dataframe.columns:
            np.testing.assert_allclose(
                list(result_dataframe.probability),
                expected_probabilities,
                rtol=1e-2,
            )

    def test_binary_classes_logistic_regression(self):
        df1 = self.spark.createDataFrame(
            [
                (1.0, [0.0, 5.0]),
                (0.0, [1.0, 2.0]),
                (1.0, [2.0, 1.0]),
                (0.0, [3.0, 3.0]),
            ]
            * 100,
            ["label", "features"],
        )
        eval_df1 = self.spark.createDataFrame(
            [
                ([0.0, 2.0],),
                ([3.5, 3.0],),
            ],
            ["features"],
        )

        lorv2 = LORV2(maxIter=200, numTrainWorkers=2, learningRate=0.001)
        assert lorv2.getMaxIter() == 200
        assert lorv2.getNumTrainWorkers() == 2
        assert lorv2.getOrDefault(lorv2.learningRate) == 0.001

        model = lorv2.fit(df1)
        assert model.uid == lorv2.uid

        expected_predictions = [1, 0]
        expected_probabilities = [
            [0.217875, 0.782125],
            [0.839615, 0.160385],
        ]

        result = model.transform(eval_df1).toPandas()
        self._check_result(result, expected_predictions, expected_probabilities)
        local_transform_result = model.transform(eval_df1.toPandas())
        self._check_result(local_transform_result, expected_predictions, expected_probabilities)

        model.set(model.probabilityCol, "")
        result_without_prob = model.transform(eval_df1).toPandas()
        assert "probability" not in result_without_prob.columns
        self._check_result(result_without_prob, expected_predictions, None)

    def test_multi_classes_logistic_regression(self):
        df1 = self.spark.createDataFrame(
            [
                (1.0, [1.0, 5.0]),
                (2.0, [1.0, -2.0]),
                (0.0, [-2.0, 1.5]),
            ]
            * 100,
            ["label", "features"],
        )
        eval_df1 = self.spark.createDataFrame(
            [
                ([1.5, 5.0],),
                ([1.0, -2.5],),
                ([-2.0, 1.0],),
            ],
            ["features"],
        )

        lorv2 = LORV2(maxIter=200, numTrainWorkers=2, learningRate=0.001)

        model = lorv2.fit(df1)

        expected_predictions = [1, 2, 0]
        expected_probabilities = [
            [5.526459e-03, 9.943553e-01, 1.183146e-04],
            [4.629959e-03, 8.141352e-03, 9.872288e-01],
            [9.624363e-01, 3.080821e-02, 6.755549e-03],
        ]

        result = model.transform(eval_df1).toPandas()
        self._check_result(result, expected_predictions, expected_probabilities)
        local_transform_result = model.transform(eval_df1.toPandas())
        self._check_result(local_transform_result, expected_predictions, expected_probabilities)


class ClassificationTests(ClassificationTestsMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.spark = SparkSession.builder.master("local[2]").getOrCreate()

    def tearDown(self) -> None:
        self.spark.stop()


if __name__ == "__main__":
    from pyspark.mlv2.tests.test_classification import *  # noqa: F401,F403

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
