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

from pyspark.ml import Estimator, Model
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    RegressionEvaluator,
)
from pyspark.ml.linalg import Vectors
from pyspark.ml.param import Param, Params
from pyspark.ml.tuning import (
    CrossValidator,
    ParamGridBuilder,
    TrainValidationSplit,
    TrainValidationSplitModel,
)
from pyspark.sql.functions import rand
from pyspark.testing.mlutils import SparkSessionTestCase


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


class ParamGridBuilderTests(SparkSessionTestCase):
    def test_addGrid(self):
        with self.assertRaises(TypeError):
            (ParamGridBuilder().addGrid("must be an instance of Param", ["not", "string"]).build())


class ValidatorTestUtilsMixin:
    def assert_param_maps_equal(self, paramMaps1, paramMaps2):
        self.assertEqual(len(paramMaps1), len(paramMaps2))
        for paramMap1, paramMap2 in zip(paramMaps1, paramMaps2):
            self.assertEqual(set(paramMap1.keys()), set(paramMap2.keys()))
            for param in paramMap1.keys():
                v1 = paramMap1[param]
                v2 = paramMap2[param]
                if isinstance(v1, Params):
                    self.assertEqual(v1.uid, v2.uid)
                else:
                    self.assertEqual(v1, v2)


class CrossValidatorTests(SparkSessionTestCase, ValidatorTestUtilsMixin):
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
            collectSubModels=True,
            numFolds=2,
        )
        cvCopied = cv.copy()
        for param in [
            lambda x: x.getEstimator().uid,
            # SPARK-32092: CrossValidator.copy() needs to copy all existing params
            lambda x: x.getNumFolds(),
            lambda x: x.getFoldCol(),
            lambda x: x.getCollectSubModels(),
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
        cvModel.subModels[0][0].getInducedError = lambda: "foo"
        self.assertNotEqual(
            cvModelCopied.subModels[0][0].getInducedError(),
            "foo",
            "Changing the original subModels should not affect the copied model",
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

    def test_param_grid_type_coercion(self):
        lr = LogisticRegression(maxIter=10)
        paramGrid = ParamGridBuilder().addGrid(lr.regParam, [0.5, 1]).build()
        for param in paramGrid:
            for v in param.values():
                assert type(v) == float

    def test_parallel_evaluation(self):
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

        lr = LogisticRegression()
        grid = ParamGridBuilder().addGrid(lr.maxIter, [5, 6]).build()
        evaluator = BinaryClassificationEvaluator()

        # test save/load of CrossValidator
        cv = CrossValidator(estimator=lr, estimatorParamMaps=grid, evaluator=evaluator)
        cv.setParallelism(1)
        cvSerialModel = cv.fit(dataset)
        cv.setParallelism(2)
        cvParallelModel = cv.fit(dataset)
        self.assertEqual(cvSerialModel.avgMetrics, cvParallelModel.avgMetrics)
        self.assertEqual(cvSerialModel.stdMetrics, cvParallelModel.stdMetrics)


class TrainValidationSplitTests(SparkSessionTestCase, ValidatorTestUtilsMixin):
    def test_fit_minimize_metric(self):
        dataset = self.spark.createDataFrame(
            [(10, 10.0), (50, 50.0), (100, 100.0), (500, 500.0)] * 10, ["feature", "label"]
        )

        iee = InducedErrorEstimator()
        evaluator = RegressionEvaluator(metricName="rmse")

        grid = ParamGridBuilder().addGrid(iee.inducedError, [100.0, 0.0, 10000.0]).build()
        tvs = TrainValidationSplit(estimator=iee, estimatorParamMaps=grid, evaluator=evaluator)
        tvsModel = tvs.fit(dataset)
        bestModel = tvsModel.bestModel
        bestModelMetric = evaluator.evaluate(bestModel.transform(dataset))
        validationMetrics = tvsModel.validationMetrics

        self.assertEqual(
            0.0, bestModel.getOrDefault("inducedError"), "Best model should have zero induced error"
        )
        self.assertEqual(0.0, bestModelMetric, "Best model has RMSE of 0")
        self.assertEqual(
            len(grid),
            len(validationMetrics),
            "validationMetrics has the same size of grid parameter",
        )
        self.assertEqual(0.0, min(validationMetrics))

    def test_fit_maximize_metric(self):
        dataset = self.spark.createDataFrame(
            [(10, 10.0), (50, 50.0), (100, 100.0), (500, 500.0)] * 10, ["feature", "label"]
        )

        iee = InducedErrorEstimator()
        evaluator = RegressionEvaluator(metricName="r2")

        grid = ParamGridBuilder().addGrid(iee.inducedError, [100.0, 0.0, 10000.0]).build()
        tvs = TrainValidationSplit(estimator=iee, estimatorParamMaps=grid, evaluator=evaluator)
        tvsModel = tvs.fit(dataset)
        bestModel = tvsModel.bestModel
        bestModelMetric = evaluator.evaluate(bestModel.transform(dataset))
        validationMetrics = tvsModel.validationMetrics

        self.assertEqual(
            0.0, bestModel.getOrDefault("inducedError"), "Best model should have zero induced error"
        )
        self.assertEqual(1.0, bestModelMetric, "Best model has R-squared of 1")
        self.assertEqual(
            len(grid),
            len(validationMetrics),
            "validationMetrics has the same size of grid parameter",
        )
        self.assertEqual(1.0, max(validationMetrics))

    def test_parallel_evaluation(self):
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
        lr = LogisticRegression()
        grid = ParamGridBuilder().addGrid(lr.maxIter, [5, 6]).build()
        evaluator = BinaryClassificationEvaluator()
        tvs = TrainValidationSplit(estimator=lr, estimatorParamMaps=grid, evaluator=evaluator)
        tvs.setParallelism(1)
        tvsSerialModel = tvs.fit(dataset)
        tvs.setParallelism(2)
        tvsParallelModel = tvs.fit(dataset)
        self.assertEqual(tvsSerialModel.validationMetrics, tvsParallelModel.validationMetrics)

    def test_expose_sub_models(self):
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
        lr = LogisticRegression()
        grid = ParamGridBuilder().addGrid(lr.maxIter, [0, 1]).build()
        evaluator = BinaryClassificationEvaluator()
        tvs = TrainValidationSplit(
            estimator=lr, estimatorParamMaps=grid, evaluator=evaluator, collectSubModels=True
        )
        tvsModel = tvs.fit(dataset)
        self.assertEqual(len(tvsModel.subModels), len(grid))

        # Test the default value for option "persistSubModel" to be "true"
        testSubPath = temp_path + "/testTrainValidationSplitSubModels"
        savingPathWithSubModels = testSubPath + "cvModel3"
        tvsModel.save(savingPathWithSubModels)
        tvsModel3 = TrainValidationSplitModel.load(savingPathWithSubModels)
        self.assertEqual(len(tvsModel3.subModels), len(grid))
        tvsModel4 = tvsModel3.copy()
        self.assertEqual(len(tvsModel4.subModels), len(grid))

        savingPathWithoutSubModels = testSubPath + "cvModel2"
        tvsModel.write().option("persistSubModels", "false").save(savingPathWithoutSubModels)
        tvsModel2 = TrainValidationSplitModel.load(savingPathWithoutSubModels)
        self.assertEqual(tvsModel2.subModels, None)

        for i in range(len(grid)):
            self.assertEqual(tvsModel.subModels[i].uid, tvsModel3.subModels[i].uid)

    def test_copy(self):
        dataset = self.spark.createDataFrame(
            [(10, 10.0), (50, 50.0), (100, 100.0), (500, 500.0)] * 10, ["feature", "label"]
        )

        iee = InducedErrorEstimator()
        evaluator = RegressionEvaluator(metricName="r2")

        grid = ParamGridBuilder().addGrid(iee.inducedError, [100.0, 0.0, 10000.0]).build()
        tvs = TrainValidationSplit(
            estimator=iee, estimatorParamMaps=grid, evaluator=evaluator, collectSubModels=True
        )
        tvsModel = tvs.fit(dataset)
        tvsCopied = tvs.copy()
        tvsModelCopied = tvsModel.copy()

        for param in [
            lambda x: x.getCollectSubModels(),
            lambda x: x.getParallelism(),
            lambda x: x.getSeed(),
            lambda x: x.getTrainRatio(),
        ]:
            self.assertEqual(param(tvs), param(tvsCopied))

        for param in [
            lambda x: x.getSeed(),
            lambda x: x.getTrainRatio(),
        ]:
            self.assertEqual(param(tvsModel), param(tvsModelCopied))

        self.assertEqual(
            tvs.getEstimator().uid,
            tvsCopied.getEstimator().uid,
            "Copied TrainValidationSplit has the same uid of Estimator",
        )

        self.assertEqual(tvsModel.bestModel.uid, tvsModelCopied.bestModel.uid)
        self.assertEqual(
            len(tvsModel.validationMetrics),
            len(tvsModelCopied.validationMetrics),
            "Copied validationMetrics has the same size of the original",
        )
        for index in range(len(tvsModel.validationMetrics)):
            self.assertEqual(
                tvsModel.validationMetrics[index], tvsModelCopied.validationMetrics[index]
            )

        tvsModel.validationMetrics[0] = "foo"
        self.assertNotEqual(
            tvsModelCopied.validationMetrics[0],
            "foo",
            "Changing the original validationMetrics should not affect the copied model",
        )
        tvsModel.subModels[0].getInducedError = lambda: "foo"
        self.assertNotEqual(
            tvsModelCopied.subModels[0].getInducedError(),
            "foo",
            "Changing the original subModels should not affect the copied model",
        )


if __name__ == "__main__":
    from pyspark.ml.tests.tuning.test_tuning import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
