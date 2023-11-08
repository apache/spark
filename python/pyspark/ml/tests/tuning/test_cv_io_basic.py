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
    CrossValidator,
    CrossValidatorModel,
    ParamGridBuilder,
)
from pyspark.testing.mlutils import (
    DummyEvaluator,
    DummyLogisticRegression,
    DummyLogisticRegressionModel,
    SparkSessionTestCase,
)
from pyspark.ml.tests.tuning.test_tuning import ValidatorTestUtilsMixin


class CrossValidatorIOBasicTests(SparkSessionTestCase, ValidatorTestUtilsMixin):
    def _run_test_save_load_trained_model(self, LogisticRegressionCls, LogisticRegressionModelCls):
        # This tests saving and loading the trained model only.
        # Save/load for CrossValidator will be added later: SPARK-13786
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
        cv = CrossValidator(
            estimator=lr,
            estimatorParamMaps=grid,
            evaluator=evaluator,
            collectSubModels=True,
            numFolds=4,
            seed=42,
        )
        cvModel = cv.fit(dataset)
        lrModel = cvModel.bestModel

        lrModelPath = temp_path + "/lrModel"
        lrModel.save(lrModelPath)
        loadedLrModel = LogisticRegressionModelCls.load(lrModelPath)
        self.assertEqual(loadedLrModel.uid, lrModel.uid)
        self.assertEqual(loadedLrModel.intercept, lrModel.intercept)

        # SPARK-32092: Saving and then loading CrossValidatorModel should not change the params
        cvModelPath = temp_path + "/cvModel"
        cvModel.save(cvModelPath)
        loadedCvModel = CrossValidatorModel.load(cvModelPath)
        for param in [
            lambda x: x.getNumFolds(),
            lambda x: x.getFoldCol(),
            lambda x: x.getSeed(),
            lambda x: len(x.subModels),
        ]:
            self.assertEqual(param(cvModel), param(loadedCvModel))

        self.assertTrue(all(loadedCvModel.isSet(param) for param in loadedCvModel.params))

        # mimic old version CrossValidatorModel (without stdMetrics attribute)
        # test loading model backwards compatibility
        cvModel2 = cvModel.copy()
        cvModel2.stdMetrics = []
        cvModelPath2 = temp_path + "/cvModel2"
        cvModel2.save(cvModelPath2)
        loadedCvModel2 = CrossValidatorModel.load(cvModelPath2)
        assert loadedCvModel2.stdMetrics == []

    def test_save_load_trained_model(self):
        self._run_test_save_load_trained_model(LogisticRegression, LogisticRegressionModel)
        self._run_test_save_load_trained_model(
            DummyLogisticRegression, DummyLogisticRegressionModel
        )

    def _run_test_save_load_simple_estimator(self, LogisticRegressionCls, evaluatorCls):
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

        # test save/load of CrossValidator
        cv = CrossValidator(estimator=lr, estimatorParamMaps=grid, evaluator=evaluator)
        cvModel = cv.fit(dataset)
        cvPath = temp_path + "/cv"
        cv.save(cvPath)
        loadedCV = CrossValidator.load(cvPath)
        self.assertEqual(loadedCV.getEstimator().uid, cv.getEstimator().uid)
        self.assertEqual(loadedCV.getEvaluator().uid, cv.getEvaluator().uid)
        self.assert_param_maps_equal(loadedCV.getEstimatorParamMaps(), cv.getEstimatorParamMaps())

        # test save/load of CrossValidatorModel
        cvModelPath = temp_path + "/cvModel"
        cvModel.save(cvModelPath)
        loadedModel = CrossValidatorModel.load(cvModelPath)
        self.assertEqual(loadedModel.bestModel.uid, cvModel.bestModel.uid)

    def test_save_load_simple_estimator(self):
        self._run_test_save_load_simple_estimator(LogisticRegression, BinaryClassificationEvaluator)
        self._run_test_save_load_simple_estimator(DummyLogisticRegression, DummyEvaluator)


if __name__ == "__main__":
    from pyspark.ml.tests.tuning.test_cv_io_basic import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
