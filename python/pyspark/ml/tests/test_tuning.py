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

from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.linalg import Vectors
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.tuning import (
    ParamGridBuilder,
    CrossValidator,
    CrossValidatorModel,
    TrainValidationSplit,
    TrainValidationSplitModel,
)
from pyspark.ml.util import _SPARKML_TEMP_DFS_PATH
from pyspark.sql.functions import rand
from pyspark.testing.sqlutils import ReusedSQLTestCase


class TuningTestsMixin:
    def test_train_validation_split(self):
        dataset = self.spark.createDataFrame(
            [
                (Vectors.dense([0.0]), 0.0),
                (Vectors.dense([0.4]), 1.0),
                (Vectors.dense([0.5]), 0.0),
                (Vectors.dense([0.6]), 1.0),
                (Vectors.dense([1.0]), 1.0),
            ]
            * 10,  # Repeat the data 10 times
            ["features", "label"],
        ).repartition(1)

        lr = LogisticRegression()
        grid = ParamGridBuilder().addGrid(lr.maxIter, [0, 1]).build()
        evaluator = BinaryClassificationEvaluator()

        tvs = TrainValidationSplit(
            estimator=lr, estimatorParamMaps=grid, evaluator=evaluator, parallelism=1, seed=42
        )
        self.assertEqual(tvs.getEstimator(), lr)
        self.assertEqual(tvs.getEvaluator(), evaluator)
        self.assertEqual(tvs.getParallelism(), 1)
        self.assertEqual(tvs.getEstimatorParamMaps(), grid)

        tvs_model = tvs.fit(dataset)

        # Access the train ratio
        self.assertEqual(tvs_model.getTrainRatio(), 0.75)
        self.assertTrue(np.isclose(tvs_model.validationMetrics[0], 0.5, atol=1e-4))
        self.assertTrue(np.isclose(tvs_model.validationMetrics[1], 0.8857142857142857, atol=1e-4))

        evaluation_score = evaluator.evaluate(tvs_model.transform(dataset))
        self.assertTrue(np.isclose(evaluation_score, 0.8333333333333333, atol=1e-4))

        with tempfile.TemporaryDirectory(prefix="ml_tmp_dir") as d:
            os.environ[_SPARKML_TEMP_DFS_PATH] = d
            try:
                tvs_model2 = tvs.fit(dataset)
                assert len(os.listdir(d)) == 0
                self.assertTrue(np.isclose(tvs_model2.validationMetrics[0], 0.5, atol=1e-4))
                self.assertTrue(
                    np.isclose(tvs_model2.validationMetrics[1], 0.8857142857142857, atol=1e-4)
                )
            finally:
                os.environ.pop(_SPARKML_TEMP_DFS_PATH, None)

        # save & load
        with tempfile.TemporaryDirectory(prefix="train_validation_split") as d:
            tvs.write().overwrite().save(d)
            tvs2 = TrainValidationSplit.load(d)
            self.assertEqual(str(tvs), str(tvs2))
            self.assertEqual(str(tvs.getEstimator()), str(tvs2.getEstimator()))
            self.assertEqual(str(tvs.getEvaluator()), str(tvs2.getEvaluator()))

            tvs_model.write().overwrite().save(d)
            model2 = TrainValidationSplitModel.load(d)
            self.assertEqual(str(tvs_model), str(model2))
            self.assertEqual(str(tvs_model.getEstimator()), str(model2.getEstimator()))
            self.assertEqual(str(tvs_model.getEvaluator()), str(model2.getEvaluator()))

    def test_cross_validator(self):
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
        cv = CrossValidator(
            estimator=lr,
            estimatorParamMaps=grid,
            evaluator=evaluator,
            parallelism=1,
            collectSubModels=True,
        )

        self.assertEqual(cv.getEstimator(), lr)
        self.assertEqual(cv.getEvaluator(), evaluator)
        self.assertEqual(cv.getParallelism(), 1)
        self.assertEqual(cv.getEstimatorParamMaps(), grid)

        model = cv.fit(dataset)
        self.assertEqual(model.getEstimator(), lr)
        self.assertEqual(model.getEvaluator(), evaluator)
        self.assertEqual(model.getEstimatorParamMaps(), grid)
        self.assertTrue(np.isclose(model.avgMetrics[0], 0.5, atol=1e-4))

        with tempfile.TemporaryDirectory(prefix="ml_tmp_dir") as d:
            os.environ[_SPARKML_TEMP_DFS_PATH] = d
            try:
                model2 = cv.fit(dataset)
                assert len(os.listdir(d)) == 0
                self.assertTrue(np.isclose(model2.avgMetrics[0], 0.5, atol=1e-4))
            finally:
                os.environ.pop(_SPARKML_TEMP_DFS_PATH, None)

        output = model.transform(dataset)
        self.assertEqual(
            output.columns, ["features", "label", "rawPrediction", "probability", "prediction"]
        )
        self.assertEqual(output.count(), 50)

        numFolds = cv.getNumFolds()

        def checkSubModels(subModels):
            self.assertEqual(len(subModels), numFolds)
            for i in range(numFolds):
                self.assertEqual(len(subModels[i]), len(grid))

        checkSubModels(model.subModels)

        for i in range(numFolds):
            for j in range(len(grid)):
                self.assertEqual(model.subModels[i][j].uid, model.subModels[i][j].uid)

        # save & load
        with tempfile.TemporaryDirectory(prefix="cv") as d:
            cv.write().overwrite().save(d)
            cv2 = CrossValidator.load(d)
            self.assertEqual(str(cv), str(cv2))
            self.assertEqual(str(cv.getEstimator()), str(cv2.getEstimator()))
            self.assertEqual(str(cv.getEvaluator()), str(cv2.getEvaluator()))

            model.write().overwrite().save(d)
            model2 = CrossValidatorModel.load(d)
            checkSubModels(model2.subModels)
            self.assertEqual(str(model), str(model2))
            self.assertEqual(str(model.getEstimator()), str(model2.getEstimator()))
            self.assertEqual(str(model.getEvaluator()), str(model2.getEvaluator()))

            model.write().overwrite().option("persistSubModels", "false").save(d)
            cvModel2 = CrossValidatorModel.load(d)
            self.assertEqual(cvModel2.subModels, None)

            model3 = model2.copy()
            checkSubModels(model3.subModels)

    def test_cv_user_specified_folds(self):
        from pyspark.sql import functions as F

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
        ).repartition(2, "features")

        dataset_with_folds = (
            dataset.repartition(1)
            .withColumn("random", rand(100))
            .withColumn(
                "fold",
                F.when(F.col("random") < 0.33, 0).when(F.col("random") < 0.66, 1).otherwise(2),
            )
            .repartition(2, "features")
        )

        lr = LogisticRegression()
        grid = ParamGridBuilder().addGrid(lr.maxIter, [20]).build()
        evaluator = BinaryClassificationEvaluator()

        cv = CrossValidator(estimator=lr, estimatorParamMaps=grid, evaluator=evaluator, numFolds=3)
        cv_with_user_folds = CrossValidator(
            estimator=lr, estimatorParamMaps=grid, evaluator=evaluator, numFolds=3, foldCol="fold"
        )

        self.assertEqual(cv.getEstimator().uid, cv_with_user_folds.getEstimator().uid)

        cvModel1 = cv.fit(dataset)
        cvModel2 = cv_with_user_folds.fit(dataset_with_folds)
        for index in range(len(cvModel1.avgMetrics)):
            print(abs(cvModel1.avgMetrics[index] - cvModel2.avgMetrics[index]))
            self.assertTrue(abs(cvModel1.avgMetrics[index] - cvModel2.avgMetrics[index]) < 0.1)

    def test_cv_invalid_user_specified_folds(self):
        dataset_with_folds = self.spark.createDataFrame(
            [
                (Vectors.dense([0.0]), 0.0, 0),
                (Vectors.dense([0.4]), 1.0, 1),
                (Vectors.dense([0.5]), 0.0, 2),
                (Vectors.dense([0.6]), 1.0, 0),
                (Vectors.dense([1.0]), 1.0, 1),
            ]
            * 10,
            ["features", "label", "fold"],
        )

        lr = LogisticRegression()
        grid = ParamGridBuilder().addGrid(lr.maxIter, [20]).build()
        evaluator = BinaryClassificationEvaluator()

        cv = CrossValidator(
            estimator=lr, estimatorParamMaps=grid, evaluator=evaluator, numFolds=2, foldCol="fold"
        )
        with self.assertRaisesRegex(Exception, "Fold number must be in range"):
            cv.fit(dataset_with_folds)

        cv = CrossValidator(
            estimator=lr, estimatorParamMaps=grid, evaluator=evaluator, numFolds=4, foldCol="fold"
        )
        with self.assertRaisesRegex(Exception, "The validation data at fold 3 is empty"):
            cv.fit(dataset_with_folds)

    def test_crossvalidator_with_random_forest_classifier(self):
        dataset = self.spark.createDataFrame(
            [
                (Vectors.dense(1.0, 2.0), 0),
                (Vectors.dense(2.0, 3.0), 1),
                (Vectors.dense(1.5, 2.5), 0),
                (Vectors.dense(3.0, 4.0), 1),
                (Vectors.dense(1.1, 2.1), 0),
                (Vectors.dense(2.5, 3.5), 1),
            ],
            ["features", "label"],
        )
        rf = RandomForestClassifier(labelCol="label", featuresCol="features")
        evaluator = BinaryClassificationEvaluator(labelCol="label")
        paramGrid = (
            ParamGridBuilder().addGrid(rf.maxDepth, [2]).addGrid(rf.numTrees, [5, 10]).build()
        )
        cv = CrossValidator(
            estimator=rf, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=3
        )
        cv.fit(dataset)


class TuningTests(TuningTestsMixin, ReusedSQLTestCase):
    pass


if __name__ == "__main__":
    from pyspark.ml.tests.test_tuning import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
