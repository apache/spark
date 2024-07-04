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
from shutil import rmtree

import numpy as np

from pyspark.ml.linalg import Vectors, Matrices
from pyspark.sql import SparkSession, DataFrame
from pyspark.ml.classification import (
    LogisticRegression,
    LogisticRegressionModel,
    LogisticRegressionSummary,
    BinaryLogisticRegressionSummary,
)


class ClassificationTestsMixin:
    def test_binomial_logistic_regression_with_bound(self):
        df = self.spark.createDataFrame(
            [
                (1.0, 1.0, Vectors.dense(0.0, 5.0)),
                (0.0, 2.0, Vectors.dense(1.0, 2.0)),
                (1.0, 3.0, Vectors.dense(2.0, 1.0)),
                (0.0, 4.0, Vectors.dense(3.0, 3.0)),
            ],
            ["label", "weight", "features"],
        )

        lor = LogisticRegression(
            regParam=0.01,
            weightCol="weight",
            lowerBoundsOnCoefficients=Matrices.dense(1, 2, [-1.0, -1.0]),
            upperBoundsOnIntercepts=Vectors.dense(0.0),
        )
        lor_model = lor.fit(df)

        def check_result(model: LogisticRegressionModel) -> None:
            self.assertTrue(
                np.allclose(model.coefficients.toArray(), [-0.2944, -0.0484], atol=1e-4)
            )
            self.assertTrue(np.isclose(model.intercept, 0.0, atol=1e-4))

        check_result(lor_model)

        # Model save
        with tempfile.TemporaryDirectory(prefix="model_save") as tmp_dir:
            local_path = os.path.join(tmp_dir, "model")
            lor_model.write().save(local_path)
            loaded_model = LogisticRegressionModel.load(local_path)
            check_result(loaded_model)

    def test_multinomial_logistic_regression_with_bound(self):
        data_path = "data/mllib/sample_multiclass_classification_data.txt"
        df = self.spark.read.format("libsvm").load(data_path)

        lor = LogisticRegression(
            regParam=0.01,
            lowerBoundsOnCoefficients=Matrices.dense(3, 4, range(12)),
            upperBoundsOnIntercepts=Vectors.dense(0.0, 0.0, 0.0),
        )
        lor_model = lor.fit(df)

        def check_result(model: LogisticRegressionModel) -> None:
            expected = [
                [4.593, 4.5516, 9.0099, 12.2904],
                [1.0, 8.1093, 7.0, 10.0],
                [3.041, 5.0, 8.0, 11.0],
            ]
            for i in range(0, len(expected)):
                self.assertTrue(
                    np.allclose(model.coefficientMatrix.toArray()[i], expected[i], atol=1e-4)
                )
            self.assertTrue(
                np.allclose(model.interceptVector.toArray(), [-0.9057, -1.1392, -0.0033], atol=1e-4)
            )

        check_result(lor_model)

        # Model save
        with tempfile.TemporaryDirectory(prefix="model_save") as tmp_dir:
            local_path = os.path.join(tmp_dir, "model")
            lor_model.write().save(local_path)
            loaded_model = LogisticRegressionModel.load(local_path)
            check_result(loaded_model)

    def test_logistic_regression_with_threshold(self):
        df = self.spark.createDataFrame(
            [
                (1.0, 1.0, Vectors.dense(0.0, 5.0)),
                (0.0, 2.0, Vectors.dense(1.0, 2.0)),
                (1.0, 3.0, Vectors.dense(2.0, 1.0)),
                (0.0, 4.0, Vectors.dense(3.0, 3.0)),
            ],
            ["label", "weight", "features"],
        )

        lor = LogisticRegression(weightCol="weight")
        model = lor.fit(df)

        # status changes 1
        for t in [0.0, 0.1, 0.2, 0.5, 1.0]:
            model.setThreshold(t).transform(df)

        # status changes 2
        [model.setThreshold(t).predict(Vectors.dense(0.0, 5.0)) for t in [0.0, 0.1, 0.2, 0.5, 1.0]]

        self.assertEqual(
            [row.prediction for row in model.setThreshold(0.0).transform(df).collect()],
            [1.0, 1.0, 1.0, 1.0],
        )
        self.assertEqual(
            [row.prediction for row in model.setThreshold(0.5).transform(df).collect()],
            [0.0, 1.0, 1.0, 0.0],
        )
        self.assertEqual(
            [row.prediction for row in model.setThreshold(1.0).transform(df).collect()],
            [0.0, 0.0, 0.0, 0.0],
        )

    def check_binary_logistic_regression_summary(self, check_evaluation):
        df = self.spark.createDataFrame(
            [(1.0, 2.0, Vectors.dense(1.0)), (0.0, 2.0, Vectors.sparse(1, [], []))],
            ["label", "weight", "features"],
        )
        lr = LogisticRegression(maxIter=5, regParam=0.01, weightCol="weight", fitIntercept=False)
        model = lr.fit(df)
        self.assertTrue(model.hasSummary)
        s = model.summary
        # test that api is callable and returns expected types
        self.assertTrue(isinstance(s.predictions, DataFrame))
        self.assertEqual(s.probabilityCol, "probability")
        self.assertEqual(s.labelCol, "label")
        self.assertEqual(s.featuresCol, "features")
        self.assertEqual(s.predictionCol, "prediction")
        objHist = s.objectiveHistory
        self.assertTrue(isinstance(objHist, list) and isinstance(objHist[0], float))
        self.assertGreater(s.totalIterations, 0)
        self.assertTrue(isinstance(s.labels, list))
        self.assertTrue(isinstance(s.truePositiveRateByLabel, list))
        self.assertTrue(isinstance(s.falsePositiveRateByLabel, list))
        self.assertTrue(isinstance(s.precisionByLabel, list))
        self.assertTrue(isinstance(s.recallByLabel, list))
        self.assertTrue(isinstance(s.fMeasureByLabel(), list))
        self.assertTrue(isinstance(s.fMeasureByLabel(1.0), list))
        self.assertTrue(isinstance(s.roc, DataFrame))
        self.assertAlmostEqual(s.areaUnderROC, 1.0, 2)
        self.assertTrue(isinstance(s.pr, DataFrame))
        self.assertTrue(isinstance(s.fMeasureByThreshold, DataFrame))
        self.assertTrue(isinstance(s.precisionByThreshold, DataFrame))
        self.assertTrue(isinstance(s.recallByThreshold, DataFrame))
        self.assertAlmostEqual(s.accuracy, 1.0, 2)
        self.assertAlmostEqual(s.weightedTruePositiveRate, 1.0, 2)
        self.assertAlmostEqual(s.weightedFalsePositiveRate, 0.0, 2)
        self.assertAlmostEqual(s.weightedRecall, 1.0, 2)
        self.assertAlmostEqual(s.weightedPrecision, 1.0, 2)
        self.assertAlmostEqual(s.weightedFMeasure(), 1.0, 2)
        self.assertAlmostEqual(s.weightedFMeasure(1.0), 1.0, 2)

        if check_evaluation:
            # test evaluation (with training dataset) produces a summary with same values
            # one check is enough to verify a summary is returned, Scala version runs full test
            sameSummary = model.evaluate(df)
            self.assertTrue(isinstance(sameSummary, BinaryLogisticRegressionSummary))
            self.assertAlmostEqual(sameSummary.areaUnderROC, s.areaUnderROC)

    def test_binary_logistic_regression_summary(self):
        self.check_binary_logistic_regression_summary(True)

    def check_multiclass_logistic_regression_summary(self, check_evaluation: bool):
        df = self.spark.createDataFrame(
            [
                (1.0, 2.0, Vectors.dense(1.0)),
                (0.0, 2.0, Vectors.sparse(1, [], [])),
                (2.0, 2.0, Vectors.dense(2.0)),
                (2.0, 2.0, Vectors.dense(1.9)),
            ],
            ["label", "weight", "features"],
        )
        lr = LogisticRegression(maxIter=5, regParam=0.01, weightCol="weight", fitIntercept=False)
        model = lr.fit(df)
        self.assertTrue(model.hasSummary)
        s = model.summary
        # test that api is callable and returns expected types
        self.assertTrue(isinstance(s.predictions, DataFrame))
        self.assertEqual(s.probabilityCol, "probability")
        self.assertEqual(s.labelCol, "label")
        self.assertEqual(s.featuresCol, "features")
        self.assertEqual(s.predictionCol, "prediction")
        objHist = s.objectiveHistory
        self.assertTrue(isinstance(objHist, list) and isinstance(objHist[0], float))
        self.assertGreater(s.totalIterations, 0)
        self.assertTrue(isinstance(s.labels, list))
        self.assertTrue(isinstance(s.truePositiveRateByLabel, list))
        self.assertTrue(isinstance(s.falsePositiveRateByLabel, list))
        self.assertTrue(isinstance(s.precisionByLabel, list))
        self.assertTrue(isinstance(s.recallByLabel, list))
        self.assertTrue(isinstance(s.fMeasureByLabel(), list))
        self.assertTrue(isinstance(s.fMeasureByLabel(1.0), list))
        self.assertAlmostEqual(s.accuracy, 0.75, 2)
        self.assertAlmostEqual(s.weightedTruePositiveRate, 0.75, 2)
        self.assertAlmostEqual(s.weightedFalsePositiveRate, 0.25, 2)
        self.assertAlmostEqual(s.weightedRecall, 0.75, 2)
        self.assertAlmostEqual(s.weightedPrecision, 0.583, 2)
        self.assertAlmostEqual(s.weightedFMeasure(), 0.65, 2)
        self.assertAlmostEqual(s.weightedFMeasure(1.0), 0.65, 2)

        if check_evaluation:
            # test evaluation (with training dataset) produces a summary with same values
            # one check is enough to verify a summary is returned, Scala version runs full test
            sameSummary = model.evaluate(df)
            self.assertTrue(isinstance(sameSummary, LogisticRegressionSummary))
            self.assertFalse(isinstance(sameSummary, BinaryLogisticRegressionSummary))
            self.assertAlmostEqual(sameSummary.accuracy, s.accuracy)

    def test_multiclass_logistic_regression_summary(self):
        self.check_multiclass_logistic_regression_summary(True)

    def test_logistic_regression(self):
        lr = LogisticRegression(maxIter=1)
        path = tempfile.mkdtemp()
        lr_path = path + "/logreg"
        lr.save(lr_path)
        lr2 = LogisticRegression.load(lr_path)
        self.assertEqual(
            lr2.uid,
            lr2.maxIter.parent,
            "Loaded LogisticRegression instance uid (%s) "
            "did not match Param's uid (%s)" % (lr2.uid, lr2.maxIter.parent),
        )
        self.assertEqual(
            lr._defaultParamMap[lr.maxIter],
            lr2._defaultParamMap[lr2.maxIter],
            "Loaded LogisticRegression instance default params did not match "
            + "original defaults",
        )
        try:
            rmtree(path)
        except OSError:
            pass


class ClassificationTests(ClassificationTestsMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.spark = SparkSession.builder.master("local[4]").getOrCreate()

    def tearDown(self) -> None:
        self.spark.stop()


if __name__ == "__main__":
    from pyspark.ml.tests.test_classification import *  # noqa: F401,F403

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
