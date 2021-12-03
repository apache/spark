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

from pyspark.ml.classification import (
    BinaryLogisticRegressionSummary,
    BinaryRandomForestClassificationSummary,
    FMClassifier,
    FMClassificationSummary,
    LinearSVC,
    LinearSVCSummary,
    LogisticRegression,
    LogisticRegressionSummary,
    MultilayerPerceptronClassifier,
    MultilayerPerceptronClassificationSummary,
    RandomForestClassificationSummary,
    RandomForestClassifier,
)
from pyspark.ml.clustering import BisectingKMeans, GaussianMixture, KMeans
from pyspark.ml.linalg import Vectors
from pyspark.ml.regression import GeneralizedLinearRegression, LinearRegression
from pyspark.sql import DataFrame
from pyspark.testing.mlutils import SparkSessionTestCase


class TrainingSummaryTest(SparkSessionTestCase):
    def test_linear_regression_summary(self):
        df = self.spark.createDataFrame(
            [(1.0, 2.0, Vectors.dense(1.0)), (0.0, 2.0, Vectors.sparse(1, [], []))],
            ["label", "weight", "features"],
        )
        lr = LinearRegression(
            maxIter=5, regParam=0.0, solver="normal", weightCol="weight", fitIntercept=False
        )
        model = lr.fit(df)
        self.assertTrue(model.hasSummary)
        s = model.summary
        # test that api is callable and returns expected types
        self.assertEqual(s.totalIterations, 0)
        self.assertTrue(isinstance(s.predictions, DataFrame))
        self.assertEqual(s.predictionCol, "prediction")
        self.assertEqual(s.labelCol, "label")
        self.assertEqual(s.featuresCol, "features")
        objHist = s.objectiveHistory
        self.assertTrue(isinstance(objHist, list) and isinstance(objHist[0], float))
        self.assertAlmostEqual(s.explainedVariance, 0.25, 2)
        self.assertAlmostEqual(s.meanAbsoluteError, 0.0)
        self.assertAlmostEqual(s.meanSquaredError, 0.0)
        self.assertAlmostEqual(s.rootMeanSquaredError, 0.0)
        self.assertAlmostEqual(s.r2, 1.0, 2)
        self.assertAlmostEqual(s.r2adj, 1.0, 2)
        self.assertTrue(isinstance(s.residuals, DataFrame))
        self.assertEqual(s.numInstances, 2)
        self.assertEqual(s.degreesOfFreedom, 1)
        devResiduals = s.devianceResiduals
        self.assertTrue(isinstance(devResiduals, list) and isinstance(devResiduals[0], float))
        coefStdErr = s.coefficientStandardErrors
        self.assertTrue(isinstance(coefStdErr, list) and isinstance(coefStdErr[0], float))
        tValues = s.tValues
        self.assertTrue(isinstance(tValues, list) and isinstance(tValues[0], float))
        pValues = s.pValues
        self.assertTrue(isinstance(pValues, list) and isinstance(pValues[0], float))
        # test evaluation (with training dataset) produces a summary with same values
        # one check is enough to verify a summary is returned
        # The child class LinearRegressionTrainingSummary runs full test
        sameSummary = model.evaluate(df)
        self.assertAlmostEqual(sameSummary.explainedVariance, s.explainedVariance)

    def test_glr_summary(self):
        from pyspark.ml.linalg import Vectors

        df = self.spark.createDataFrame(
            [(1.0, 2.0, Vectors.dense(1.0)), (0.0, 2.0, Vectors.sparse(1, [], []))],
            ["label", "weight", "features"],
        )
        glr = GeneralizedLinearRegression(
            family="gaussian", link="identity", weightCol="weight", fitIntercept=False
        )
        model = glr.fit(df)
        self.assertTrue(model.hasSummary)
        s = model.summary
        # test that api is callable and returns expected types
        self.assertEqual(s.numIterations, 1)  # this should default to a single iteration of WLS
        self.assertTrue(isinstance(s.predictions, DataFrame))
        self.assertEqual(s.predictionCol, "prediction")
        self.assertEqual(s.numInstances, 2)
        self.assertTrue(isinstance(s.residuals(), DataFrame))
        self.assertTrue(isinstance(s.residuals("pearson"), DataFrame))
        coefStdErr = s.coefficientStandardErrors
        self.assertTrue(isinstance(coefStdErr, list) and isinstance(coefStdErr[0], float))
        tValues = s.tValues
        self.assertTrue(isinstance(tValues, list) and isinstance(tValues[0], float))
        pValues = s.pValues
        self.assertTrue(isinstance(pValues, list) and isinstance(pValues[0], float))
        self.assertEqual(s.degreesOfFreedom, 1)
        self.assertEqual(s.residualDegreeOfFreedom, 1)
        self.assertEqual(s.residualDegreeOfFreedomNull, 2)
        self.assertEqual(s.rank, 1)
        self.assertTrue(isinstance(s.solver, str))
        self.assertTrue(isinstance(s.aic, float))
        self.assertTrue(isinstance(s.deviance, float))
        self.assertTrue(isinstance(s.nullDeviance, float))
        self.assertTrue(isinstance(s.dispersion, float))
        # test evaluation (with training dataset) produces a summary with same values
        # one check is enough to verify a summary is returned
        # The child class GeneralizedLinearRegressionTrainingSummary runs full test
        sameSummary = model.evaluate(df)
        self.assertAlmostEqual(sameSummary.deviance, s.deviance)

    def test_binary_logistic_regression_summary(self):
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
        # test evaluation (with training dataset) produces a summary with same values
        # one check is enough to verify a summary is returned, Scala version runs full test
        sameSummary = model.evaluate(df)
        self.assertTrue(isinstance(sameSummary, BinaryLogisticRegressionSummary))
        self.assertAlmostEqual(sameSummary.areaUnderROC, s.areaUnderROC)

    def test_multiclass_logistic_regression_summary(self):
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
        # test evaluation (with training dataset) produces a summary with same values
        # one check is enough to verify a summary is returned, Scala version runs full test
        sameSummary = model.evaluate(df)
        self.assertTrue(isinstance(sameSummary, LogisticRegressionSummary))
        self.assertFalse(isinstance(sameSummary, BinaryLogisticRegressionSummary))
        self.assertAlmostEqual(sameSummary.accuracy, s.accuracy)

    def test_linear_svc_summary(self):
        df = self.spark.createDataFrame(
            [(1.0, 2.0, Vectors.dense(1.0, 1.0, 1.0)), (0.0, 2.0, Vectors.dense(1.0, 2.0, 3.0))],
            ["label", "weight", "features"],
        )
        svc = LinearSVC(maxIter=5, weightCol="weight")
        model = svc.fit(df)
        self.assertTrue(model.hasSummary)
        s = model.summary()
        # test that api is callable and returns expected types
        self.assertTrue(isinstance(s.predictions, DataFrame))
        self.assertEqual(s.scoreCol, "rawPrediction")
        self.assertEqual(s.labelCol, "label")
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
        print(s.weightedTruePositiveRate)
        self.assertAlmostEqual(s.weightedTruePositiveRate, 1.0, 2)
        self.assertAlmostEqual(s.weightedFalsePositiveRate, 0.0, 2)
        self.assertAlmostEqual(s.weightedRecall, 1.0, 2)
        self.assertAlmostEqual(s.weightedPrecision, 1.0, 2)
        self.assertAlmostEqual(s.weightedFMeasure(), 1.0, 2)
        self.assertAlmostEqual(s.weightedFMeasure(1.0), 1.0, 2)
        # test evaluation (with training dataset) produces a summary with same values
        # one check is enough to verify a summary is returned, Scala version runs full test
        sameSummary = model.evaluate(df)
        self.assertTrue(isinstance(sameSummary, LinearSVCSummary))
        self.assertAlmostEqual(sameSummary.areaUnderROC, s.areaUnderROC)

    def test_binary_randomforest_classification_summary(self):
        df = self.spark.createDataFrame(
            [(1.0, 2.0, Vectors.dense(1.0)), (0.0, 2.0, Vectors.sparse(1, [], []))],
            ["label", "weight", "features"],
        )
        rf = RandomForestClassifier(weightCol="weight")
        model = rf.fit(df)
        self.assertTrue(model.hasSummary)
        s = model.summary
        # test that api is callable and returns expected types
        self.assertTrue(isinstance(s.predictions, DataFrame))
        self.assertEqual(s.labelCol, "label")
        self.assertEqual(s.predictionCol, "prediction")
        self.assertEqual(s.totalIterations, 0)
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
        # test evaluation (with training dataset) produces a summary with same values
        # one check is enough to verify a summary is returned, Scala version runs full test
        sameSummary = model.evaluate(df)
        self.assertTrue(isinstance(sameSummary, BinaryRandomForestClassificationSummary))
        self.assertAlmostEqual(sameSummary.areaUnderROC, s.areaUnderROC)

    def test_multiclass_randomforest_classification_summary(self):
        df = self.spark.createDataFrame(
            [
                (1.0, 2.0, Vectors.dense(1.0)),
                (0.0, 2.0, Vectors.sparse(1, [], [])),
                (2.0, 2.0, Vectors.dense(2.0)),
                (2.0, 2.0, Vectors.dense(1.9)),
            ],
            ["label", "weight", "features"],
        )
        rf = RandomForestClassifier(weightCol="weight")
        model = rf.fit(df)
        self.assertTrue(model.hasSummary)
        s = model.summary
        # test that api is callable and returns expected types
        self.assertTrue(isinstance(s.predictions, DataFrame))
        self.assertEqual(s.labelCol, "label")
        self.assertEqual(s.predictionCol, "prediction")
        self.assertEqual(s.totalIterations, 0)
        self.assertTrue(isinstance(s.labels, list))
        self.assertTrue(isinstance(s.truePositiveRateByLabel, list))
        self.assertTrue(isinstance(s.falsePositiveRateByLabel, list))
        self.assertTrue(isinstance(s.precisionByLabel, list))
        self.assertTrue(isinstance(s.recallByLabel, list))
        self.assertTrue(isinstance(s.fMeasureByLabel(), list))
        self.assertTrue(isinstance(s.fMeasureByLabel(1.0), list))
        self.assertAlmostEqual(s.accuracy, 1.0, 2)
        self.assertAlmostEqual(s.weightedTruePositiveRate, 1.0, 2)
        self.assertAlmostEqual(s.weightedFalsePositiveRate, 0.0, 2)
        self.assertAlmostEqual(s.weightedRecall, 1.0, 2)
        self.assertAlmostEqual(s.weightedPrecision, 1.0, 2)
        self.assertAlmostEqual(s.weightedFMeasure(), 1.0, 2)
        self.assertAlmostEqual(s.weightedFMeasure(1.0), 1.0, 2)
        # test evaluation (with training dataset) produces a summary with same values
        # one check is enough to verify a summary is returned, Scala version runs full test
        sameSummary = model.evaluate(df)
        self.assertTrue(isinstance(sameSummary, RandomForestClassificationSummary))
        self.assertFalse(isinstance(sameSummary, BinaryRandomForestClassificationSummary))
        self.assertAlmostEqual(sameSummary.accuracy, s.accuracy)

    def test_fm_classification_summary(self):
        df = self.spark.createDataFrame(
            [
                (1.0, Vectors.dense(2.0)),
                (0.0, Vectors.dense(2.0)),
                (0.0, Vectors.dense(6.0)),
                (1.0, Vectors.dense(3.0)),
            ],
            ["label", "features"],
        )
        fm = FMClassifier(maxIter=5)
        model = fm.fit(df)
        self.assertTrue(model.hasSummary)
        s = model.summary()
        # test that api is callable and returns expected types
        self.assertTrue(isinstance(s.predictions, DataFrame))
        self.assertEqual(s.scoreCol, "probability")
        self.assertEqual(s.labelCol, "label")
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
        self.assertAlmostEqual(s.areaUnderROC, 0.625, 2)
        self.assertTrue(isinstance(s.pr, DataFrame))
        self.assertTrue(isinstance(s.fMeasureByThreshold, DataFrame))
        self.assertTrue(isinstance(s.precisionByThreshold, DataFrame))
        self.assertTrue(isinstance(s.recallByThreshold, DataFrame))
        self.assertAlmostEqual(s.weightedTruePositiveRate, 0.75, 2)
        self.assertAlmostEqual(s.weightedFalsePositiveRate, 0.25, 2)
        self.assertAlmostEqual(s.weightedRecall, 0.75, 2)
        self.assertAlmostEqual(s.weightedPrecision, 0.8333333333333333, 2)
        self.assertAlmostEqual(s.weightedFMeasure(), 0.7333333333333334, 2)
        self.assertAlmostEqual(s.weightedFMeasure(1.0), 0.7333333333333334, 2)
        # test evaluation (with training dataset) produces a summary with same values
        # one check is enough to verify a summary is returned, Scala version runs full test
        sameSummary = model.evaluate(df)
        self.assertTrue(isinstance(sameSummary, FMClassificationSummary))
        self.assertAlmostEqual(sameSummary.areaUnderROC, s.areaUnderROC)

    def test_mlp_classification_summary(self):
        df = self.spark.createDataFrame(
            [
                (0.0, Vectors.dense([0.0, 0.0])),
                (1.0, Vectors.dense([0.0, 1.0])),
                (1.0, Vectors.dense([1.0, 0.0])),
                (0.0, Vectors.dense([1.0, 1.0])),
            ],
            ["label", "features"],
        )
        mlp = MultilayerPerceptronClassifier(layers=[2, 2, 2], seed=123)
        model = mlp.fit(df)
        self.assertTrue(model.hasSummary)
        s = model.summary()
        # test that api is callable and returns expected types
        self.assertTrue(isinstance(s.predictions, DataFrame))
        self.assertEqual(s.labelCol, "label")
        self.assertEqual(s.predictionCol, "prediction")
        self.assertGreater(s.totalIterations, 0)
        self.assertTrue(isinstance(s.labels, list))
        self.assertTrue(isinstance(s.truePositiveRateByLabel, list))
        self.assertTrue(isinstance(s.falsePositiveRateByLabel, list))
        self.assertTrue(isinstance(s.precisionByLabel, list))
        self.assertTrue(isinstance(s.recallByLabel, list))
        self.assertTrue(isinstance(s.fMeasureByLabel(), list))
        self.assertTrue(isinstance(s.fMeasureByLabel(1.0), list))
        self.assertAlmostEqual(s.accuracy, 1.0, 2)
        self.assertAlmostEqual(s.weightedTruePositiveRate, 1.0, 2)
        self.assertAlmostEqual(s.weightedFalsePositiveRate, 0.0, 2)
        self.assertAlmostEqual(s.weightedRecall, 1.0, 2)
        self.assertAlmostEqual(s.weightedPrecision, 1.0, 2)
        self.assertAlmostEqual(s.weightedFMeasure(), 1.0, 2)
        self.assertAlmostEqual(s.weightedFMeasure(1.0), 1.0, 2)
        # test evaluation (with training dataset) produces a summary with same values
        # one check is enough to verify a summary is returned, Scala version runs full test
        sameSummary = model.evaluate(df)
        self.assertTrue(isinstance(sameSummary, MultilayerPerceptronClassificationSummary))
        self.assertAlmostEqual(sameSummary.accuracy, s.accuracy)

    def test_gaussian_mixture_summary(self):
        data = [
            (Vectors.dense(1.0),),
            (Vectors.dense(5.0),),
            (Vectors.dense(10.0),),
            (Vectors.sparse(1, [], []),),
        ]
        df = self.spark.createDataFrame(data, ["features"])
        gmm = GaussianMixture(k=2)
        model = gmm.fit(df)
        self.assertTrue(model.hasSummary)
        s = model.summary
        self.assertTrue(isinstance(s.predictions, DataFrame))
        self.assertEqual(s.probabilityCol, "probability")
        self.assertTrue(isinstance(s.probability, DataFrame))
        self.assertEqual(s.featuresCol, "features")
        self.assertEqual(s.predictionCol, "prediction")
        self.assertTrue(isinstance(s.cluster, DataFrame))
        self.assertEqual(len(s.clusterSizes), 2)
        self.assertEqual(s.k, 2)
        self.assertEqual(s.numIter, 3)

    def test_bisecting_kmeans_summary(self):
        data = [
            (Vectors.dense(1.0),),
            (Vectors.dense(5.0),),
            (Vectors.dense(10.0),),
            (Vectors.sparse(1, [], []),),
        ]
        df = self.spark.createDataFrame(data, ["features"])
        bkm = BisectingKMeans(k=2)
        model = bkm.fit(df)
        self.assertTrue(model.hasSummary)
        s = model.summary
        self.assertTrue(isinstance(s.predictions, DataFrame))
        self.assertEqual(s.featuresCol, "features")
        self.assertEqual(s.predictionCol, "prediction")
        self.assertTrue(isinstance(s.cluster, DataFrame))
        self.assertEqual(len(s.clusterSizes), 2)
        self.assertEqual(s.k, 2)
        self.assertEqual(s.numIter, 20)

    def test_kmeans_summary(self):
        data = [
            (Vectors.dense([0.0, 0.0]),),
            (Vectors.dense([1.0, 1.0]),),
            (Vectors.dense([9.0, 8.0]),),
            (Vectors.dense([8.0, 9.0]),),
        ]
        df = self.spark.createDataFrame(data, ["features"])
        kmeans = KMeans(k=2, seed=1)
        model = kmeans.fit(df)
        self.assertTrue(model.hasSummary)
        s = model.summary
        self.assertTrue(isinstance(s.predictions, DataFrame))
        self.assertEqual(s.featuresCol, "features")
        self.assertEqual(s.predictionCol, "prediction")
        self.assertTrue(isinstance(s.cluster, DataFrame))
        self.assertEqual(len(s.clusterSizes), 2)
        self.assertEqual(s.k, 2)
        self.assertEqual(s.numIter, 1)


if __name__ == "__main__":
    from pyspark.ml.tests.test_training_summary import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
