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
import math
import unittest

import numpy as np
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.ml import Estimator, Pipeline, Model
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel, OneVsRest
from pyspark.ml.evaluation import BinaryClassificationEvaluator, \
    MulticlassClassificationEvaluator, RegressionEvaluator
from pyspark.ml.linalg import Vectors
from pyspark.ml.param import Param, Params
from pyspark.ml.tuning import CrossValidator, CrossValidatorModel, ParamGridBuilder, \
    TrainValidationSplit, TrainValidationSplitModel, ParamRandomBuilder
from pyspark.sql.functions import rand
from pyspark.testing.mlutils import DummyEvaluator, DummyLogisticRegression, \
    DummyLogisticRegressionModel, SparkSessionTestCase


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


class DummyParams(Params):

    def __init__(self):
        super(DummyParams, self).__init__()
        self.test_param = Param(self, "test_param", "dummy parameter for testing")
        self.another_test_param = Param(self, "another_test_param", "second parameter for testing")


class ParamRandomBuilderTests(unittest.TestCase):

    def __init__(self, methodName):
        super(ParamRandomBuilderTests, self).__init__(methodName=methodName)
        self.dummy_params = DummyParams()
        self.to_test = ParamRandomBuilder()
        self.n = 100

    def check_ranges(self, params, lowest, highest, expected_type):
        self.assertEqual(self.n, len(params))
        for param in params:
            for v in param.values():
                self.assertGreaterEqual(v, lowest)
                self.assertLessEqual(v, highest)
                self.assertEqual(type(v), expected_type)

    def check_addRandom_ranges(self, x, y, expected_type):
        params = self.to_test.addRandom(self.dummy_params.test_param, x, y, self.n).build()
        self.check_ranges(params, x, y, expected_type)

    def check_addLog10Random_ranges(self, x, y, expected_type):
        params = self.to_test.addLog10Random(self.dummy_params.test_param, x, y, self.n).build()
        self.check_ranges(params, x, y, expected_type)

    @staticmethod
    def counts(xs):
        key_to_count = {}
        for v in xs:
            k = int(v)
            if key_to_count.get(k) is None:
                key_to_count[k] = 1
            else:
                key_to_count[k] = key_to_count[k] + 1
        return key_to_count

    @staticmethod
    def raw_values_of(params):
        values = []
        for param in params:
            for v in param.values():
                values.append(v)
        return values

    def check_even_distribution(self, vs, bin_function):
        binned = map(lambda x: bin_function(x), vs)
        histogram = self.counts(binned)
        values = list(histogram.values())
        sd = np.std(values)
        mu = np.mean(values)
        for k, v in histogram.items():
            self.assertLess(abs(v - mu), 5 * sd, "{} values for bucket {} is unlikely "
                                                 "when the mean is {} and standard deviation {}"
                            .format(v, k, mu, sd))

    def test_distribution(self):
        params = self.to_test.addRandom(self.dummy_params.test_param, 0, 20000, 10000).build()
        values = self.raw_values_of(params)
        self.check_even_distribution(values, lambda x: x // 1000)

    def test_logarithmic_distribution(self):
        params = self.to_test.addLog10Random(self.dummy_params.test_param, 1, 1e10, 10000).build()
        values = self.raw_values_of(params)
        self.check_even_distribution(values, lambda x: math.log10(x))

    def test_param_cardinality(self):
        num_random_params = 7
        values = [1, 2, 3]
        self.to_test.addRandom(self.dummy_params.test_param, 1, 10, num_random_params)
        self.to_test.addGrid(self.dummy_params.another_test_param, values)
        self.assertEqual(len(self.to_test.build()), num_random_params * len(values))

    def test_add_random_integer_logarithmic_range(self):
        self.check_addLog10Random_ranges(100, 200, int)

    def test_add_logarithmic_random_float_and_integer_yields_floats(self):
        self.check_addLog10Random_ranges(100, 200., float)

    def test_add_random_float_logarithmic_range(self):
        self.check_addLog10Random_ranges(100., 200., float)

    def test_add_random_integer_range(self):
        self.check_addRandom_ranges(100, 200, int)

    def test_add_random_float_and_integer_yields_floats(self):
        self.check_addRandom_ranges(100, 200., float)

    def test_add_random_float_range(self):
        self.check_addRandom_ranges(100., 200., float)

    def test_unexpected_type(self):
        with self.assertRaises(TypeError):
            self.to_test.addRandom(self.dummy_params.test_param, 1, "wrong type", 1).build()


class ParamGridBuilderTests(SparkSessionTestCase):

    def test_addGrid(self):
        with self.assertRaises(TypeError):
            grid = (ParamGridBuilder()
                    .addGrid("must be an instance of Param", ["not", "string"])
                    .build())


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
        cv = CrossValidator(
            estimator=iee,
            estimatorParamMaps=grid,
            evaluator=evaluator,
            collectSubModels=True,
            numFolds=2
        )
        cvCopied = cv.copy()
        for param in [
            lambda x: x.getEstimator().uid,
            # SPARK-32092: CrossValidator.copy() needs to copy all existing params
            lambda x: x.getNumFolds(),
            lambda x: x.getFoldCol(),
            lambda x: x.getCollectSubModels(),
            lambda x: x.getParallelism(),
            lambda x: x.getSeed()
        ]:
            self.assertEqual(param(cv), param(cvCopied))

        cvModel = cv.fit(dataset)
        cvModelCopied = cvModel.copy()
        for index in range(len(cvModel.avgMetrics)):
            self.assertTrue(abs(cvModel.avgMetrics[index] - cvModelCopied.avgMetrics[index])
                            < 0.0001)
        self.assertTrue(np.allclose(cvModel.stdMetrics, cvModelCopied.stdMetrics))
        # SPARK-32092: CrossValidatorModel.copy() needs to copy all existing params
        for param in [
            lambda x: x.getNumFolds(),
            lambda x: x.getFoldCol(),
            lambda x: x.getSeed()
        ]:
            self.assertEqual(param(cvModel), param(cvModelCopied))

        cvModel.avgMetrics[0] = 'foo'
        self.assertNotEqual(
            cvModelCopied.avgMetrics[0],
            'foo',
            "Changing the original avgMetrics should not affect the copied model"
        )
        cvModel.stdMetrics[0] = 'foo'
        self.assertNotEqual(
            cvModelCopied.stdMetrics[0],
            'foo',
            "Changing the original stdMetrics should not affect the copied model"
        )
        cvModel.subModels[0][0].getInducedError = lambda: 'foo'
        self.assertNotEqual(
            cvModelCopied.subModels[0][0].getInducedError(),
            'foo',
            "Changing the original subModels should not affect the copied model"
        )

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

    def _run_test_save_load_trained_model(self, LogisticRegressionCls, LogisticRegressionModelCls):
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
        lr = LogisticRegressionCls()
        grid = ParamGridBuilder().addGrid(lr.maxIter, [0, 1]).build()
        evaluator = BinaryClassificationEvaluator()
        cv = CrossValidator(
            estimator=lr,
            estimatorParamMaps=grid,
            evaluator=evaluator,
            collectSubModels=True,
            numFolds=4,
            seed=42
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
            lambda x: len(x.subModels)
        ]:
            self.assertEqual(param(cvModel), param(loadedCvModel))

        self.assertTrue(all(
            loadedCvModel.isSet(param) for param in loadedCvModel.params
        ))

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
        self._run_test_save_load_trained_model(DummyLogisticRegression,
                                               DummyLogisticRegressionModel)

    def _run_test_save_load_simple_estimator(self, LogisticRegressionCls, evaluatorCls):
        temp_path = tempfile.mkdtemp()
        dataset = self.spark.createDataFrame(
            [(Vectors.dense([0.0]), 0.0),
             (Vectors.dense([0.4]), 1.0),
             (Vectors.dense([0.5]), 0.0),
             (Vectors.dense([0.6]), 1.0),
             (Vectors.dense([1.0]), 1.0)] * 10,
            ["features", "label"])

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
        self._run_test_save_load_simple_estimator(
            LogisticRegression, BinaryClassificationEvaluator)
        self._run_test_save_load_simple_estimator(
            DummyLogisticRegression, DummyEvaluator)

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
        self.assertEqual(cvSerialModel.stdMetrics, cvParallelModel.stdMetrics)

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

    def _run_test_save_load_nested_estimator(self, LogisticRegressionCls):
        temp_path = tempfile.mkdtemp()
        dataset = self.spark.createDataFrame(
            [(Vectors.dense([0.0]), 0.0),
             (Vectors.dense([0.4]), 1.0),
             (Vectors.dense([0.5]), 0.0),
             (Vectors.dense([0.6]), 1.0),
             (Vectors.dense([1.0]), 1.0)] * 10,
            ["features", "label"])

        ova = OneVsRest(classifier=LogisticRegressionCls())
        lr1 = LogisticRegressionCls().setMaxIter(100)
        lr2 = LogisticRegressionCls().setMaxIter(150)
        grid = ParamGridBuilder().addGrid(ova.classifier, [lr1, lr2]).build()
        evaluator = MulticlassClassificationEvaluator()

        # test save/load of CrossValidator
        cv = CrossValidator(estimator=ova, estimatorParamMaps=grid, evaluator=evaluator)
        cvModel = cv.fit(dataset)
        cvPath = temp_path + "/cv"
        cv.save(cvPath)
        loadedCV = CrossValidator.load(cvPath)
        self.assert_param_maps_equal(loadedCV.getEstimatorParamMaps(), grid)
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
        self.assert_param_maps_equal(loadedModel.getEstimatorParamMaps(), grid)
        self.assertEqual(loadedModel.bestModel.uid, cvModel.bestModel.uid)

    def test_save_load_nested_estimator(self):
        self._run_test_save_load_nested_estimator(LogisticRegression)
        self._run_test_save_load_nested_estimator(DummyLogisticRegression)

    def _run_test_save_load_pipeline_estimator(self, LogisticRegressionCls):
        temp_path = tempfile.mkdtemp()
        training = self.spark.createDataFrame([
            (0, "a b c d e spark", 1.0),
            (1, "b d", 0.0),
            (2, "spark f g h", 1.0),
            (3, "hadoop mapreduce", 0.0),
            (4, "b spark who", 1.0),
            (5, "g d a y", 0.0),
            (6, "spark fly", 1.0),
            (7, "was mapreduce", 0.0),
        ], ["id", "text", "label"])

        # Configure an ML pipeline, which consists of tree stages: tokenizer, hashingTF, and lr.
        tokenizer = Tokenizer(inputCol="text", outputCol="words")
        hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="features")

        ova = OneVsRest(classifier=LogisticRegressionCls())
        lr1 = LogisticRegressionCls().setMaxIter(5)
        lr2 = LogisticRegressionCls().setMaxIter(10)

        pipeline = Pipeline(stages=[tokenizer, hashingTF, ova])

        paramGrid = ParamGridBuilder() \
            .addGrid(hashingTF.numFeatures, [10, 100]) \
            .addGrid(ova.classifier, [lr1, lr2]) \
            .build()

        crossval = CrossValidator(estimator=pipeline,
                                  estimatorParamMaps=paramGrid,
                                  evaluator=MulticlassClassificationEvaluator(),
                                  numFolds=2)  # use 3+ folds in practice
        cvPath = temp_path + "/cv"
        crossval.save(cvPath)
        loadedCV = CrossValidator.load(cvPath)
        self.assert_param_maps_equal(loadedCV.getEstimatorParamMaps(), paramGrid)
        self.assertEqual(loadedCV.getEstimator().uid, crossval.getEstimator().uid)

        # Run cross-validation, and choose the best set of parameters.
        cvModel = crossval.fit(training)

        # test save/load of CrossValidatorModel
        cvModelPath = temp_path + "/cvModel"
        cvModel.save(cvModelPath)
        loadedModel = CrossValidatorModel.load(cvModelPath)
        self.assertEqual(loadedModel.bestModel.uid, cvModel.bestModel.uid)
        self.assertEqual(len(loadedModel.bestModel.stages), len(cvModel.bestModel.stages))
        for loadedStage, originalStage in zip(loadedModel.bestModel.stages,
                                              cvModel.bestModel.stages):
            self.assertEqual(loadedStage.uid, originalStage.uid)

        # Test nested pipeline
        nested_pipeline = Pipeline(stages=[tokenizer, Pipeline(stages=[hashingTF, ova])])
        crossval2 = CrossValidator(estimator=nested_pipeline,
                                   estimatorParamMaps=paramGrid,
                                   evaluator=MulticlassClassificationEvaluator(),
                                   numFolds=2)  # use 3+ folds in practice
        cv2Path = temp_path + "/cv2"
        crossval2.save(cv2Path)
        loadedCV2 = CrossValidator.load(cv2Path)
        self.assert_param_maps_equal(loadedCV2.getEstimatorParamMaps(), paramGrid)
        self.assertEqual(loadedCV2.getEstimator().uid, crossval2.getEstimator().uid)

        # Run cross-validation, and choose the best set of parameters.
        cvModel2 = crossval2.fit(training)
        # test save/load of CrossValidatorModel
        cvModelPath2 = temp_path + "/cvModel2"
        cvModel2.save(cvModelPath2)
        loadedModel2 = CrossValidatorModel.load(cvModelPath2)
        self.assertEqual(loadedModel2.bestModel.uid, cvModel2.bestModel.uid)
        loaded_nested_pipeline_model = loadedModel2.bestModel.stages[1]
        original_nested_pipeline_model = cvModel2.bestModel.stages[1]
        self.assertEqual(loaded_nested_pipeline_model.uid, original_nested_pipeline_model.uid)
        self.assertEqual(len(loaded_nested_pipeline_model.stages),
                         len(original_nested_pipeline_model.stages))
        for loadedStage, originalStage in zip(loaded_nested_pipeline_model.stages,
                                              original_nested_pipeline_model.stages):
            self.assertEqual(loadedStage.uid, originalStage.uid)

    def test_save_load_pipeline_estimator(self):
        self._run_test_save_load_pipeline_estimator(LogisticRegression)
        self._run_test_save_load_pipeline_estimator(DummyLogisticRegression)

    def test_user_specified_folds(self):
        from pyspark.sql import functions as F

        dataset = self.spark.createDataFrame(
            [(Vectors.dense([0.0]), 0.0),
             (Vectors.dense([0.4]), 1.0),
             (Vectors.dense([0.5]), 0.0),
             (Vectors.dense([0.6]), 1.0),
             (Vectors.dense([1.0]), 1.0)] * 10,
            ["features", "label"]).repartition(2, "features")

        dataset_with_folds = dataset.repartition(1).withColumn("random", rand(100)) \
            .withColumn("fold", F.when(F.col("random") < 0.33, 0)
                        .when(F.col("random") < 0.66, 1)
                        .otherwise(2)).repartition(2, "features")

        lr = LogisticRegression()
        grid = ParamGridBuilder().addGrid(lr.maxIter, [20]).build()
        evaluator = BinaryClassificationEvaluator()

        cv = CrossValidator(estimator=lr, estimatorParamMaps=grid, evaluator=evaluator, numFolds=3)
        cv_with_user_folds = CrossValidator(estimator=lr,
                                            estimatorParamMaps=grid,
                                            evaluator=evaluator,
                                            numFolds=3,
                                            foldCol="fold")

        self.assertEqual(cv.getEstimator().uid, cv_with_user_folds.getEstimator().uid)

        cvModel1 = cv.fit(dataset)
        cvModel2 = cv_with_user_folds.fit(dataset_with_folds)
        for index in range(len(cvModel1.avgMetrics)):
            print(abs(cvModel1.avgMetrics[index] - cvModel2.avgMetrics[index]))
            self.assertTrue(abs(cvModel1.avgMetrics[index] - cvModel2.avgMetrics[index])
                            < 0.1)

        # test save/load of CrossValidator
        temp_path = tempfile.mkdtemp()
        cvPath = temp_path + "/cv"
        cv_with_user_folds.save(cvPath)
        loadedCV = CrossValidator.load(cvPath)
        self.assertEqual(loadedCV.getFoldCol(), cv_with_user_folds.getFoldCol())

    def test_invalid_user_specified_folds(self):
        dataset_with_folds = self.spark.createDataFrame(
            [(Vectors.dense([0.0]), 0.0, 0),
             (Vectors.dense([0.4]), 1.0, 1),
             (Vectors.dense([0.5]), 0.0, 2),
             (Vectors.dense([0.6]), 1.0, 0),
             (Vectors.dense([1.0]), 1.0, 1)] * 10,
            ["features", "label", "fold"])

        lr = LogisticRegression()
        grid = ParamGridBuilder().addGrid(lr.maxIter, [20]).build()
        evaluator = BinaryClassificationEvaluator()

        cv = CrossValidator(estimator=lr,
                            estimatorParamMaps=grid,
                            evaluator=evaluator,
                            numFolds=2,
                            foldCol="fold")
        with self.assertRaisesRegex(Exception, "Fold number must be in range"):
            cv.fit(dataset_with_folds)

        cv = CrossValidator(estimator=lr,
                            estimatorParamMaps=grid,
                            evaluator=evaluator,
                            numFolds=4,
                            foldCol="fold")
        with self.assertRaisesRegex(Exception, "The validation data at fold 3 is empty"):
            cv.fit(dataset_with_folds)


class TrainValidationSplitTests(SparkSessionTestCase, ValidatorTestUtilsMixin):

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

    def _run_test_save_load_trained_model(self, LogisticRegressionCls, LogisticRegressionModelCls):
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
        lr = LogisticRegressionCls()
        grid = ParamGridBuilder().addGrid(lr.maxIter, [0, 1]).build()
        evaluator = BinaryClassificationEvaluator()
        tvs = TrainValidationSplit(
            estimator=lr,
            estimatorParamMaps=grid,
            evaluator=evaluator,
            collectSubModels=True,
            seed=42
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

        self.assertTrue(all(
            loadedTvsModel.isSet(param) for param in loadedTvsModel.params
        ))

    def test_save_load_trained_model(self):
        self._run_test_save_load_trained_model(LogisticRegression, LogisticRegressionModel)
        self._run_test_save_load_trained_model(DummyLogisticRegression,
                                               DummyLogisticRegressionModel)

    def _run_test_save_load_simple_estimator(self, LogisticRegressionCls, evaluatorCls):
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
        self.assert_param_maps_equal(
            loadedTvs.getEstimatorParamMaps(), tvs.getEstimatorParamMaps())

        tvsModelPath = temp_path + "/tvsModel"
        tvsModel.save(tvsModelPath)
        loadedModel = TrainValidationSplitModel.load(tvsModelPath)
        self.assertEqual(loadedModel.bestModel.uid, tvsModel.bestModel.uid)

    def test_save_load_simple_estimator(self):
        self._run_test_save_load_simple_estimator(
            LogisticRegression, BinaryClassificationEvaluator)
        self._run_test_save_load_simple_estimator(
            DummyLogisticRegression, DummyEvaluator)

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

    def _run_test_save_load_nested_estimator(self, LogisticRegressionCls):
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
        ova = OneVsRest(classifier=LogisticRegressionCls())
        lr1 = LogisticRegressionCls().setMaxIter(100)
        lr2 = LogisticRegressionCls().setMaxIter(150)
        grid = ParamGridBuilder().addGrid(ova.classifier, [lr1, lr2]).build()
        evaluator = MulticlassClassificationEvaluator()

        tvs = TrainValidationSplit(estimator=ova, estimatorParamMaps=grid, evaluator=evaluator)
        tvsModel = tvs.fit(dataset)
        tvsPath = temp_path + "/tvs"
        tvs.save(tvsPath)
        loadedTvs = TrainValidationSplit.load(tvsPath)
        self.assert_param_maps_equal(loadedTvs.getEstimatorParamMaps(), grid)
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
        self.assert_param_maps_equal(loadedModel.getEstimatorParamMaps(), grid)
        self.assertEqual(loadedModel.bestModel.uid, tvsModel.bestModel.uid)

    def test_save_load_nested_estimator(self):
        self._run_test_save_load_nested_estimator(LogisticRegression)
        self._run_test_save_load_nested_estimator(DummyLogisticRegression)

    def _run_test_save_load_pipeline_estimator(self, LogisticRegressionCls):
        temp_path = tempfile.mkdtemp()
        training = self.spark.createDataFrame([
            (0, "a b c d e spark", 1.0),
            (1, "b d", 0.0),
            (2, "spark f g h", 1.0),
            (3, "hadoop mapreduce", 0.0),
            (4, "b spark who", 1.0),
            (5, "g d a y", 0.0),
            (6, "spark fly", 1.0),
            (7, "was mapreduce", 0.0),
        ], ["id", "text", "label"])

        # Configure an ML pipeline, which consists of tree stages: tokenizer, hashingTF, and lr.
        tokenizer = Tokenizer(inputCol="text", outputCol="words")
        hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="features")

        ova = OneVsRest(classifier=LogisticRegressionCls())
        lr1 = LogisticRegressionCls().setMaxIter(5)
        lr2 = LogisticRegressionCls().setMaxIter(10)

        pipeline = Pipeline(stages=[tokenizer, hashingTF, ova])

        paramGrid = ParamGridBuilder() \
            .addGrid(hashingTF.numFeatures, [10, 100]) \
            .addGrid(ova.classifier, [lr1, lr2]) \
            .build()

        tvs = TrainValidationSplit(estimator=pipeline,
                                   estimatorParamMaps=paramGrid,
                                   evaluator=MulticlassClassificationEvaluator())
        tvsPath = temp_path + "/tvs"
        tvs.save(tvsPath)
        loadedTvs = TrainValidationSplit.load(tvsPath)
        self.assert_param_maps_equal(loadedTvs.getEstimatorParamMaps(), paramGrid)
        self.assertEqual(loadedTvs.getEstimator().uid, tvs.getEstimator().uid)

        # Run train validation split, and choose the best set of parameters.
        tvsModel = tvs.fit(training)

        # test save/load of CrossValidatorModel
        tvsModelPath = temp_path + "/tvsModel"
        tvsModel.save(tvsModelPath)
        loadedModel = TrainValidationSplitModel.load(tvsModelPath)
        self.assertEqual(loadedModel.bestModel.uid, tvsModel.bestModel.uid)
        self.assertEqual(len(loadedModel.bestModel.stages), len(tvsModel.bestModel.stages))
        for loadedStage, originalStage in zip(loadedModel.bestModel.stages,
                                              tvsModel.bestModel.stages):
            self.assertEqual(loadedStage.uid, originalStage.uid)

        # Test nested pipeline
        nested_pipeline = Pipeline(stages=[tokenizer, Pipeline(stages=[hashingTF, ova])])
        tvs2 = TrainValidationSplit(estimator=nested_pipeline,
                                    estimatorParamMaps=paramGrid,
                                    evaluator=MulticlassClassificationEvaluator())
        tvs2Path = temp_path + "/tvs2"
        tvs2.save(tvs2Path)
        loadedTvs2 = TrainValidationSplit.load(tvs2Path)
        self.assert_param_maps_equal(loadedTvs2.getEstimatorParamMaps(), paramGrid)
        self.assertEqual(loadedTvs2.getEstimator().uid, tvs2.getEstimator().uid)

        # Run train validation split, and choose the best set of parameters.
        tvsModel2 = tvs2.fit(training)
        # test save/load of CrossValidatorModel
        tvsModelPath2 = temp_path + "/tvsModel2"
        tvsModel2.save(tvsModelPath2)
        loadedModel2 = TrainValidationSplitModel.load(tvsModelPath2)
        self.assertEqual(loadedModel2.bestModel.uid, tvsModel2.bestModel.uid)
        loaded_nested_pipeline_model = loadedModel2.bestModel.stages[1]
        original_nested_pipeline_model = tvsModel2.bestModel.stages[1]
        self.assertEqual(loaded_nested_pipeline_model.uid, original_nested_pipeline_model.uid)
        self.assertEqual(len(loaded_nested_pipeline_model.stages),
                         len(original_nested_pipeline_model.stages))
        for loadedStage, originalStage in zip(loaded_nested_pipeline_model.stages,
                                              original_nested_pipeline_model.stages):
            self.assertEqual(loadedStage.uid, originalStage.uid)

    def test_save_load_pipeline_estimator(self):
        self._run_test_save_load_pipeline_estimator(LogisticRegression)
        self._run_test_save_load_pipeline_estimator(DummyLogisticRegression)

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
        tvs = TrainValidationSplit(
            estimator=iee,
            estimatorParamMaps=grid,
            evaluator=evaluator,
            collectSubModels=True
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

        self.assertEqual(tvs.getEstimator().uid, tvsCopied.getEstimator().uid,
                         "Copied TrainValidationSplit has the same uid of Estimator")

        self.assertEqual(tvsModel.bestModel.uid, tvsModelCopied.bestModel.uid)
        self.assertEqual(len(tvsModel.validationMetrics),
                         len(tvsModelCopied.validationMetrics),
                         "Copied validationMetrics has the same size of the original")
        for index in range(len(tvsModel.validationMetrics)):
            self.assertEqual(tvsModel.validationMetrics[index],
                             tvsModelCopied.validationMetrics[index])

        tvsModel.validationMetrics[0] = 'foo'
        self.assertNotEqual(
            tvsModelCopied.validationMetrics[0],
            'foo',
            "Changing the original validationMetrics should not affect the copied model"
        )
        tvsModel.subModels[0].getInducedError = lambda: 'foo'
        self.assertNotEqual(
            tvsModelCopied.subModels[0].getInducedError(),
            'foo',
            "Changing the original subModels should not affect the copied model"
        )


if __name__ == "__main__":
    from pyspark.ml.tests.test_tuning import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
