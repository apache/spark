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

from pyspark.ml import Estimator, Model
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel, OneVsRest
from pyspark.ml.evaluation import BinaryClassificationEvaluator, \
    MulticlassClassificationEvaluator, RegressionEvaluator
from pyspark.ml.linalg import Vectors
from pyspark.ml.param import Param, Params
from pyspark.ml.tuning import CrossValidator, CrossValidatorModel, ParamGridBuilder, \
    TrainValidationSplit, TrainValidationSplitModel
from pyspark.sql.functions import rand
from pyspark.testing.mlutils import SparkSessionTestCase


class HasInducedError(Params):

    def __init__(self):
        super(HasInducedError, self).__init__()
        self.inducedError = Param(self, "inducedError",
                                  "Uniformly-distributed error added to feature")

    def getInducedError(self):
        return self.getOrDefault(self.inducedError)


class InducedErrorModel(Model, HasInducedError):

    def __init__(self):
        super(InducedErrorModel, self).__init__()

    def _transform(self, dataset):
        return dataset.withColumn("prediction",
                                  dataset.feature + (rand(0) * self.getInducedError()))


class InducedErrorEstimator(Estimator, HasInducedError):

    def __init__(self, inducedError=1.0):
        super(InducedErrorEstimator, self).__init__()
        self._set(inducedError=inducedError)

    def _fit(self, dataset):
        model = InducedErrorModel()
        self._copyValues(model)
        return model


class CrossValidatorTests(SparkSessionTestCase):

    def test_copy(self):
        dataset = self.spark.createDataFrame([
            (10, 10.0),
            (50, 50.0),
            (100, 100.0),
            (500, 500.0)] * 10,
            ["feature", "label"])

        iee = InducedErrorEstimator()
        evaluator = RegressionEvaluator(metricName="rmse")

        grid = (ParamGridBuilder()
                .addGrid(iee.inducedError, [100.0, 0.0, 10000.0])
                .build())
        cv = CrossValidator(estimator=iee, estimatorParamMaps=grid, evaluator=evaluator)
        cvCopied = cv.copy()
        self.assertEqual(cv.getEstimator().uid, cvCopied.getEstimator().uid)

        cvModel = cv.fit(dataset)
        cvModelCopied = cvModel.copy()
        for index in range(len(cvModel.avgMetrics)):
            self.assertTrue(abs(cvModel.avgMetrics[index] - cvModelCopied.avgMetrics[index])
                            < 0.0001)

    def test_fit_minimize_metric(self):
        dataset = self.spark.createDataFrame([
            (10, 10.0),
            (50, 50.0),
            (100, 100.0),
            (500, 500.0)] * 10,
            ["feature", "label"])

        iee = InducedErrorEstimator()
        evaluator = RegressionEvaluator(metricName="rmse")

        grid = (ParamGridBuilder()
                .addGrid(iee.inducedError, [100.0, 0.0, 10000.0])
                .build())
        cv = CrossValidator(estimator=iee, estimatorParamMaps=grid, evaluator=evaluator)
        cvModel = cv.fit(dataset)
        bestModel = cvModel.bestModel
        bestModelMetric = evaluator.evaluate(bestModel.transform(dataset))

        self.assertEqual(0.0, bestModel.getOrDefault('inducedError'),
                         "Best model should have zero induced error")
        self.assertEqual(0.0, bestModelMetric, "Best model has RMSE of 0")

    def test_fit_maximize_metric(self):
        dataset = self.spark.createDataFrame([
            (10, 10.0),
            (50, 50.0),
            (100, 100.0),
            (500, 500.0)] * 10,
            ["feature", "label"])

        iee = InducedErrorEstimator()
        evaluator = RegressionEvaluator(metricName="r2")

        grid = (ParamGridBuilder()
                .addGrid(iee.inducedError, [100.0, 0.0, 10000.0])
                .build())
        cv = CrossValidator(estimator=iee, estimatorParamMaps=grid, evaluator=evaluator)
        cvModel = cv.fit(dataset)
        bestModel = cvModel.bestModel
        bestModelMetric = evaluator.evaluate(bestModel.transform(dataset))

        self.assertEqual(0.0, bestModel.getOrDefault('inducedError'),
                         "Best model should have zero induced error")
        self.assertEqual(1.0, bestModelMetric, "Best model has R-squared of 1")

    def test_param_grid_type_coercion(self):
        lr = LogisticRegression(maxIter=10)
        paramGrid = ParamGridBuilder().addGrid(lr.regParam, [0.5, 1]).build()
        for param in paramGrid:
            for v in param.values():
                assert(type(v) == float)

    def test_save_load_trained_model(self):
        # This tests saving and loading the trained model only.
        # Save/load for CrossValidator will be added later: SPARK-13786
        temp_path = tempfile.mkdtemp()
        dataset = self.spark.createDataFrame(
            [(Vectors.dense([0.0]), 0.0),
             (Vectors.dense([0.4]), 1.0),
             (Vectors.dense([0.5]), 0.0),
             (Vectors.dense([0.6]), 1.0),
             (Vectors.dense([1.0]), 1.0)] * 10,
            ["features", "label"])
        lr = LogisticRegression()
        grid = ParamGridBuilder().addGrid(lr.maxIter, [0, 1]).build()
        evaluator = BinaryClassificationEvaluator()
        cv = CrossValidator(estimator=lr, estimatorParamMaps=grid, evaluator=evaluator)
        cvModel = cv.fit(dataset)
        lrModel = cvModel.bestModel

        cvModelPath = temp_path + "/cvModel"
        lrModel.save(cvModelPath)
        loadedLrModel = LogisticRegressionModel.load(cvModelPath)
        self.assertEqual(loadedLrModel.uid, lrModel.uid)
        self.assertEqual(loadedLrModel.intercept, lrModel.intercept)

    def test_save_load_simple_estimator(self):
        temp_path = tempfile.mkdtemp()
        dataset = self.spark.createDataFrame(
            [(Vectors.dense([0.0]), 0.0),
             (Vectors.dense([0.4]), 1.0),
             (Vectors.dense([0.5]), 0.0),
             (Vectors.dense([0.6]), 1.0),
             (Vectors.dense([1.0]), 1.0)] * 10,
            ["features", "label"])

        lr = LogisticRegression()
        grid = ParamGridBuilder().addGrid(lr.maxIter, [0, 1]).build()
        evaluator = BinaryClassificationEvaluator()

        # test save/load of CrossValidator
        cv = CrossValidator(estimator=lr, estimatorParamMaps=grid, evaluator=evaluator)
        cvModel = cv.fit(dataset)
        cvPath = temp_path + "/cv"
        cv.save(cvPath)
        loadedCV = CrossValidator.load(cvPath)
        self.assertEqual(loadedCV.getEstimator().uid, cv.getEstimator().uid)
        self.assertEqual(loadedCV.getEvaluator().uid, cv.getEvaluator().uid)
        self.assertEqual(loadedCV.getEstimatorParamMaps(), cv.getEstimatorParamMaps())

        # test save/load of CrossValidatorModel
        cvModelPath = temp_path + "/cvModel"
        cvModel.save(cvModelPath)
        loadedModel = CrossValidatorModel.load(cvModelPath)
        self.assertEqual(loadedModel.bestModel.uid, cvModel.bestModel.uid)

    def test_parallel_evaluation(self):
        dataset = self.spark.createDataFrame(
            [(Vectors.dense([0.0]), 0.0),
             (Vectors.dense([0.4]), 1.0),
             (Vectors.dense([0.5]), 0.0),
             (Vectors.dense([0.6]), 1.0),
             (Vectors.dense([1.0]), 1.0)] * 10,
            ["features", "label"])

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

    def test_expose_sub_models(self):
        temp_path = tempfile.mkdtemp()
        dataset = self.spark.createDataFrame(
            [(Vectors.dense([0.0]), 0.0),
             (Vectors.dense([0.4]), 1.0),
             (Vectors.dense([0.5]), 0.0),
             (Vectors.dense([0.6]), 1.0),
             (Vectors.dense([1.0]), 1.0)] * 10,
            ["features", "label"])

        lr = LogisticRegression()
        grid = ParamGridBuilder().addGrid(lr.maxIter, [0, 1]).build()
        evaluator = BinaryClassificationEvaluator()

        numFolds = 3
        cv = CrossValidator(estimator=lr, estimatorParamMaps=grid, evaluator=evaluator,
                            numFolds=numFolds, collectSubModels=True)

        def checkSubModels(subModels):
            self.assertEqual(len(subModels), numFolds)
            for i in range(numFolds):
                self.assertEqual(len(subModels[i]), len(grid))

        cvModel = cv.fit(dataset)
        checkSubModels(cvModel.subModels)

        # Test the default value for option "persistSubModel" to be "true"
        testSubPath = temp_path + "/testCrossValidatorSubModels"
        savingPathWithSubModels = testSubPath + "cvModel3"
        cvModel.save(savingPathWithSubModels)
        cvModel3 = CrossValidatorModel.load(savingPathWithSubModels)
        checkSubModels(cvModel3.subModels)
        cvModel4 = cvModel3.copy()
        checkSubModels(cvModel4.subModels)

        savingPathWithoutSubModels = testSubPath + "cvModel2"
        cvModel.write().option("persistSubModels", "false").save(savingPathWithoutSubModels)
        cvModel2 = CrossValidatorModel.load(savingPathWithoutSubModels)
        self.assertEqual(cvModel2.subModels, None)

        for i in range(numFolds):
            for j in range(len(grid)):
                self.assertEqual(cvModel.subModels[i][j].uid, cvModel3.subModels[i][j].uid)

    def test_save_load_nested_estimator(self):
        temp_path = tempfile.mkdtemp()
        dataset = self.spark.createDataFrame(
            [(Vectors.dense([0.0]), 0.0),
             (Vectors.dense([0.4]), 1.0),
             (Vectors.dense([0.5]), 0.0),
             (Vectors.dense([0.6]), 1.0),
             (Vectors.dense([1.0]), 1.0)] * 10,
            ["features", "label"])

        ova = OneVsRest(classifier=LogisticRegression())
        lr1 = LogisticRegression().setMaxIter(100)
        lr2 = LogisticRegression().setMaxIter(150)
        grid = ParamGridBuilder().addGrid(ova.classifier, [lr1, lr2]).build()
        evaluator = MulticlassClassificationEvaluator()

        # test save/load of CrossValidator
        cv = CrossValidator(estimator=ova, estimatorParamMaps=grid, evaluator=evaluator)
        cvModel = cv.fit(dataset)
        cvPath = temp_path + "/cv"
        cv.save(cvPath)
        loadedCV = CrossValidator.load(cvPath)
        self.assertEqual(loadedCV.getEstimator().uid, cv.getEstimator().uid)
        self.assertEqual(loadedCV.getEvaluator().uid, cv.getEvaluator().uid)

        originalParamMap = cv.getEstimatorParamMaps()
        loadedParamMap = loadedCV.getEstimatorParamMaps()
        for i, param in enumerate(loadedParamMap):
            for p in param:
                if p.name == "classifier":
                    self.assertEqual(param[p].uid, originalParamMap[i][p].uid)
                else:
                    self.assertEqual(param[p], originalParamMap[i][p])

        # test save/load of CrossValidatorModel
        cvModelPath = temp_path + "/cvModel"
        cvModel.save(cvModelPath)
        loadedModel = CrossValidatorModel.load(cvModelPath)
        self.assertEqual(loadedModel.bestModel.uid, cvModel.bestModel.uid)


class TrainValidationSplitTests(SparkSessionTestCase):

    def test_fit_minimize_metric(self):
        dataset = self.spark.createDataFrame([
            (10, 10.0),
            (50, 50.0),
            (100, 100.0),
            (500, 500.0)] * 10,
            ["feature", "label"])

        iee = InducedErrorEstimator()
        evaluator = RegressionEvaluator(metricName="rmse")

        grid = ParamGridBuilder() \
            .addGrid(iee.inducedError, [100.0, 0.0, 10000.0]) \
            .build()
        tvs = TrainValidationSplit(estimator=iee, estimatorParamMaps=grid, evaluator=evaluator)
        tvsModel = tvs.fit(dataset)
        bestModel = tvsModel.bestModel
        bestModelMetric = evaluator.evaluate(bestModel.transform(dataset))
        validationMetrics = tvsModel.validationMetrics

        self.assertEqual(0.0, bestModel.getOrDefault('inducedError'),
                         "Best model should have zero induced error")
        self.assertEqual(0.0, bestModelMetric, "Best model has RMSE of 0")
        self.assertEqual(len(grid), len(validationMetrics),
                         "validationMetrics has the same size of grid parameter")
        self.assertEqual(0.0, min(validationMetrics))

    def test_fit_maximize_metric(self):
        dataset = self.spark.createDataFrame([
            (10, 10.0),
            (50, 50.0),
            (100, 100.0),
            (500, 500.0)] * 10,
            ["feature", "label"])

        iee = InducedErrorEstimator()
        evaluator = RegressionEvaluator(metricName="r2")

        grid = ParamGridBuilder() \
            .addGrid(iee.inducedError, [100.0, 0.0, 10000.0]) \
            .build()
        tvs = TrainValidationSplit(estimator=iee, estimatorParamMaps=grid, evaluator=evaluator)
        tvsModel = tvs.fit(dataset)
        bestModel = tvsModel.bestModel
        bestModelMetric = evaluator.evaluate(bestModel.transform(dataset))
        validationMetrics = tvsModel.validationMetrics

        self.assertEqual(0.0, bestModel.getOrDefault('inducedError'),
                         "Best model should have zero induced error")
        self.assertEqual(1.0, bestModelMetric, "Best model has R-squared of 1")
        self.assertEqual(len(grid), len(validationMetrics),
                         "validationMetrics has the same size of grid parameter")
        self.assertEqual(1.0, max(validationMetrics))

    def test_save_load_trained_model(self):
        # This tests saving and loading the trained model only.
        # Save/load for TrainValidationSplit will be added later: SPARK-13786
        temp_path = tempfile.mkdtemp()
        dataset = self.spark.createDataFrame(
            [(Vectors.dense([0.0]), 0.0),
             (Vectors.dense([0.4]), 1.0),
             (Vectors.dense([0.5]), 0.0),
             (Vectors.dense([0.6]), 1.0),
             (Vectors.dense([1.0]), 1.0)] * 10,
            ["features", "label"])
        lr = LogisticRegression()
        grid = ParamGridBuilder().addGrid(lr.maxIter, [0, 1]).build()
        evaluator = BinaryClassificationEvaluator()
        tvs = TrainValidationSplit(estimator=lr, estimatorParamMaps=grid, evaluator=evaluator)
        tvsModel = tvs.fit(dataset)
        lrModel = tvsModel.bestModel

        tvsModelPath = temp_path + "/tvsModel"
        lrModel.save(tvsModelPath)
        loadedLrModel = LogisticRegressionModel.load(tvsModelPath)
        self.assertEqual(loadedLrModel.uid, lrModel.uid)
        self.assertEqual(loadedLrModel.intercept, lrModel.intercept)

    def test_save_load_simple_estimator(self):
        # This tests saving and loading the trained model only.
        # Save/load for TrainValidationSplit will be added later: SPARK-13786
        temp_path = tempfile.mkdtemp()
        dataset = self.spark.createDataFrame(
            [(Vectors.dense([0.0]), 0.0),
             (Vectors.dense([0.4]), 1.0),
             (Vectors.dense([0.5]), 0.0),
             (Vectors.dense([0.6]), 1.0),
             (Vectors.dense([1.0]), 1.0)] * 10,
            ["features", "label"])
        lr = LogisticRegression()
        grid = ParamGridBuilder().addGrid(lr.maxIter, [0, 1]).build()
        evaluator = BinaryClassificationEvaluator()
        tvs = TrainValidationSplit(estimator=lr, estimatorParamMaps=grid, evaluator=evaluator)
        tvsModel = tvs.fit(dataset)

        tvsPath = temp_path + "/tvs"
        tvs.save(tvsPath)
        loadedTvs = TrainValidationSplit.load(tvsPath)
        self.assertEqual(loadedTvs.getEstimator().uid, tvs.getEstimator().uid)
        self.assertEqual(loadedTvs.getEvaluator().uid, tvs.getEvaluator().uid)
        self.assertEqual(loadedTvs.getEstimatorParamMaps(), tvs.getEstimatorParamMaps())

        tvsModelPath = temp_path + "/tvsModel"
        tvsModel.save(tvsModelPath)
        loadedModel = TrainValidationSplitModel.load(tvsModelPath)
        self.assertEqual(loadedModel.bestModel.uid, tvsModel.bestModel.uid)

    def test_parallel_evaluation(self):
        dataset = self.spark.createDataFrame(
            [(Vectors.dense([0.0]), 0.0),
             (Vectors.dense([0.4]), 1.0),
             (Vectors.dense([0.5]), 0.0),
             (Vectors.dense([0.6]), 1.0),
             (Vectors.dense([1.0]), 1.0)] * 10,
            ["features", "label"])
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
            [(Vectors.dense([0.0]), 0.0),
             (Vectors.dense([0.4]), 1.0),
             (Vectors.dense([0.5]), 0.0),
             (Vectors.dense([0.6]), 1.0),
             (Vectors.dense([1.0]), 1.0)] * 10,
            ["features", "label"])
        lr = LogisticRegression()
        grid = ParamGridBuilder().addGrid(lr.maxIter, [0, 1]).build()
        evaluator = BinaryClassificationEvaluator()
        tvs = TrainValidationSplit(estimator=lr, estimatorParamMaps=grid, evaluator=evaluator,
                                   collectSubModels=True)
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

    def test_save_load_nested_estimator(self):
        # This tests saving and loading the trained model only.
        # Save/load for TrainValidationSplit will be added later: SPARK-13786
        temp_path = tempfile.mkdtemp()
        dataset = self.spark.createDataFrame(
            [(Vectors.dense([0.0]), 0.0),
             (Vectors.dense([0.4]), 1.0),
             (Vectors.dense([0.5]), 0.0),
             (Vectors.dense([0.6]), 1.0),
             (Vectors.dense([1.0]), 1.0)] * 10,
            ["features", "label"])
        ova = OneVsRest(classifier=LogisticRegression())
        lr1 = LogisticRegression().setMaxIter(100)
        lr2 = LogisticRegression().setMaxIter(150)
        grid = ParamGridBuilder().addGrid(ova.classifier, [lr1, lr2]).build()
        evaluator = MulticlassClassificationEvaluator()

        tvs = TrainValidationSplit(estimator=ova, estimatorParamMaps=grid, evaluator=evaluator)
        tvsModel = tvs.fit(dataset)
        tvsPath = temp_path + "/tvs"
        tvs.save(tvsPath)
        loadedTvs = TrainValidationSplit.load(tvsPath)
        self.assertEqual(loadedTvs.getEstimator().uid, tvs.getEstimator().uid)
        self.assertEqual(loadedTvs.getEvaluator().uid, tvs.getEvaluator().uid)

        originalParamMap = tvs.getEstimatorParamMaps()
        loadedParamMap = loadedTvs.getEstimatorParamMaps()
        for i, param in enumerate(loadedParamMap):
            for p in param:
                if p.name == "classifier":
                    self.assertEqual(param[p].uid, originalParamMap[i][p].uid)
                else:
                    self.assertEqual(param[p], originalParamMap[i][p])

        tvsModelPath = temp_path + "/tvsModel"
        tvsModel.save(tvsModelPath)
        loadedModel = TrainValidationSplitModel.load(tvsModelPath)
        self.assertEqual(loadedModel.bestModel.uid, tvsModel.bestModel.uid)

    def test_copy(self):
        dataset = self.spark.createDataFrame([
            (10, 10.0),
            (50, 50.0),
            (100, 100.0),
            (500, 500.0)] * 10,
            ["feature", "label"])

        iee = InducedErrorEstimator()
        evaluator = RegressionEvaluator(metricName="r2")

        grid = ParamGridBuilder() \
            .addGrid(iee.inducedError, [100.0, 0.0, 10000.0]) \
            .build()
        tvs = TrainValidationSplit(estimator=iee, estimatorParamMaps=grid, evaluator=evaluator)
        tvsModel = tvs.fit(dataset)
        tvsCopied = tvs.copy()
        tvsModelCopied = tvsModel.copy()

        self.assertEqual(tvs.getEstimator().uid, tvsCopied.getEstimator().uid,
                         "Copied TrainValidationSplit has the same uid of Estimator")

        self.assertEqual(tvsModel.bestModel.uid, tvsModelCopied.bestModel.uid)
        self.assertEqual(len(tvsModel.validationMetrics),
                         len(tvsModelCopied.validationMetrics),
                         "Copied validationMetrics has the same size of the original")
        for index in range(len(tvsModel.validationMetrics)):
            self.assertEqual(tvsModel.validationMetrics[index],
                             tvsModelCopied.validationMetrics[index])


if __name__ == "__main__":
    from pyspark.ml.tests.test_tuning import *

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
