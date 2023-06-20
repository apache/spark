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

from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.linalg import Vectors
from pyspark.ml.tuning import (
    ParamGridBuilder,
    TrainValidationSplit,
    TrainValidationSplitModel,
)
from pyspark.testing.mlutils import (
    DummyEvaluator,
    DummyLogisticRegression,
    DummyLogisticRegressionModel,
    SparkSessionTestCase,
)
from pyspark.ml.tests.tuning.test_tuning import ValidatorTestUtilsMixin


class TrainValidationSplitIOBasicTests(SparkSessionTestCase, ValidatorTestUtilsMixin):
    def _run_test_save_load_trained_model(self, LogisticRegressionCls, LogisticRegressionModelCls):
        # This tests saving and loading the trained model only.
        # Save/load for TrainValidationSplit will be added later: SPARK-13786
        temp_path = tempfile.mkdtemp()
        dataset = self.spark.createDataFrame(
            [
                (Vectors.dense([0.0]), 0.0),
                (Vectors.dense([0.4]), 1.0),
                (Vectors.dense([0.5]), 0.0),
                (Vectors.dense([0.6]), 1.0),
                (Vectors.dense([1.0]), 1.0),
            ]
            * 10,
            ["features", "label"],
        )
        lr = LogisticRegressionCls()
        grid = ParamGridBuilder().addGrid(lr.maxIter, [0, 1]).build()
        evaluator = BinaryClassificationEvaluator()
        tvs = TrainValidationSplit(
            estimator=lr,
            estimatorParamMaps=grid,
            evaluator=evaluator,
            collectSubModels=True,
            seed=42,
        )
        tvsModel = tvs.fit(dataset)
        lrModel = tvsModel.bestModel

        lrModelPath = temp_path + "/lrModel"
        lrModel.save(lrModelPath)
        loadedLrModel = LogisticRegressionModelCls.load(lrModelPath)
        self.assertEqual(loadedLrModel.uid, lrModel.uid)
        self.assertEqual(loadedLrModel.intercept, lrModel.intercept)

        tvsModelPath = temp_path + "/tvsModel"
        tvsModel.save(tvsModelPath)
        loadedTvsModel = TrainValidationSplitModel.load(tvsModelPath)
        for param in [
            lambda x: x.getSeed(),
            lambda x: x.getTrainRatio(),
        ]:
            self.assertEqual(param(tvsModel), param(loadedTvsModel))

        self.assertTrue(all(loadedTvsModel.isSet(param) for param in loadedTvsModel.params))

    def test_save_load_trained_model(self):
        self._run_test_save_load_trained_model(LogisticRegression, LogisticRegressionModel)
        self._run_test_save_load_trained_model(
            DummyLogisticRegression, DummyLogisticRegressionModel
        )

    def _run_test_save_load_simple_estimator(self, LogisticRegressionCls, evaluatorCls):
        # This tests saving and loading the trained model only.
        # Save/load for TrainValidationSplit will be added later: SPARK-13786
        temp_path = tempfile.mkdtemp()
        dataset = self.spark.createDataFrame(
            [
                (Vectors.dense([0.0]), 0.0),
                (Vectors.dense([0.4]), 1.0),
                (Vectors.dense([0.5]), 0.0),
                (Vectors.dense([0.6]), 1.0),
                (Vectors.dense([1.0]), 1.0),
            ]
            * 10,
            ["features", "label"],
        )
        lr = LogisticRegressionCls()
        grid = ParamGridBuilder().addGrid(lr.maxIter, [0, 1]).build()
        evaluator = evaluatorCls()
        tvs = TrainValidationSplit(estimator=lr, estimatorParamMaps=grid, evaluator=evaluator)
        tvsModel = tvs.fit(dataset)

        tvsPath = temp_path + "/tvs"
        tvs.save(tvsPath)
        loadedTvs = TrainValidationSplit.load(tvsPath)
        self.assertEqual(loadedTvs.getEstimator().uid, tvs.getEstimator().uid)
        self.assertEqual(loadedTvs.getEvaluator().uid, tvs.getEvaluator().uid)
        self.assert_param_maps_equal(loadedTvs.getEstimatorParamMaps(), tvs.getEstimatorParamMaps())

        tvsModelPath = temp_path + "/tvsModel"
        tvsModel.save(tvsModelPath)
        loadedModel = TrainValidationSplitModel.load(tvsModelPath)
        self.assertEqual(loadedModel.bestModel.uid, tvsModel.bestModel.uid)

    def test_save_load_simple_estimator(self):
        self._run_test_save_load_simple_estimator(LogisticRegression, BinaryClassificationEvaluator)
        self._run_test_save_load_simple_estimator(DummyLogisticRegression, DummyEvaluator)


if __name__ == "__main__":
    from pyspark.ml.tests.tuning.test_tvs_io_basic import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
