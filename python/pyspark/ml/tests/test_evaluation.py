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

from pyspark.ml.evaluation import (
    ClusteringEvaluator,
    RegressionEvaluator,
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
    MultilabelClassificationEvaluator,
    RankingEvaluator,
)
from pyspark.ml.linalg import Vectors
from pyspark.sql import Row, SparkSession


class EvaluatorTestsMixin:
    def test_ranking_evaluator(self):
        scoreAndLabels = [
            ([1.0, 6.0, 2.0, 7.0, 8.0, 3.0, 9.0, 10.0, 4.0, 5.0], [1.0, 2.0, 3.0, 4.0, 5.0]),
            ([4.0, 1.0, 5.0, 6.0, 2.0, 7.0, 3.0, 8.0, 9.0, 10.0], [1.0, 2.0, 3.0]),
            ([1.0, 2.0, 3.0, 4.0, 5.0], []),
        ]
        dataset = self.spark.createDataFrame(scoreAndLabels, ["prediction", "label"])

        # Initialize RankingEvaluator
        evaluator = RankingEvaluator().setPredictionCol("prediction")
        self.assertTrue(evaluator.isLargerBetter())

        # Evaluate the dataset using the default metric (mean average precision)
        mean_average_precision = evaluator.evaluate(dataset)
        self.assertTrue(np.allclose(mean_average_precision, 0.3550, atol=1e-4))

        # Evaluate the dataset using precisionAtK for k=2
        precision_at_k = evaluator.evaluate(
            dataset, {evaluator.metricName: "precisionAtK", evaluator.k: 2}
        )
        self.assertTrue(np.allclose(precision_at_k, 0.3333, atol=1e-4))

        # read/write
        with tempfile.TemporaryDirectory(prefix="ranking_evaluator") as tmp_dir:
            # Save the evaluator
            evaluator.write().overwrite().save(tmp_dir)
            # Load the saved evaluator
            evaluator2 = RankingEvaluator.load(tmp_dir)
            self.assertEqual(evaluator2.getPredictionCol(), "prediction")
            self.assertEqual(str(evaluator), str(evaluator2))

    def test_multilabel_classification_evaluator(self):
        dataset = self.spark.createDataFrame(
            [
                ([0.0, 1.0], [0.0, 2.0]),
                ([0.0, 2.0], [0.0, 1.0]),
                ([], [0.0]),
                ([2.0], [2.0]),
                ([2.0, 0.0], [2.0, 0.0]),
                ([0.0, 1.0, 2.0], [0.0, 1.0]),
                ([1.0], [1.0, 2.0]),
            ],
            ["prediction", "label"],
        )

        evaluator = MultilabelClassificationEvaluator().setPredictionCol("prediction")

        # Evaluate the dataset using the default metric (f1 measure by default)
        f1_score = evaluator.evaluate(dataset)
        self.assertTrue(np.allclose(f1_score, 0.6380, atol=1e-4))
        # Evaluate the dataset using accuracy
        accuracy = evaluator.evaluate(dataset, {evaluator.metricName: "accuracy"})
        self.assertTrue(np.allclose(accuracy, 0.5476, atol=1e-4))

        # read/write
        with tempfile.TemporaryDirectory(prefix="multi_label_class_eval") as tmp_dir:
            # Save the evaluator
            evaluator.write().overwrite().save(tmp_dir)
            # Load the saved evaluator
            evaluator2 = MultilabelClassificationEvaluator.load(tmp_dir)
            self.assertEqual(evaluator2.getPredictionCol(), "prediction")
            self.assertEqual(str(evaluator), str(evaluator2))

        for metric in [
            "subsetAccuracy",
            "accuracy",
            "precision",
            "recall",
            "f1Measure",
            "precisionByLabel",
            "recallByLabel",
            "f1MeasureByLabel",
            "microPrecision",
            "microRecall",
            "microF1Measure",
        ]:
            evaluator.setMetricName(metric)
            self.assertTrue(evaluator.isLargerBetter())

        evaluator.setMetricName("hammingLoss")
        self.assertTrue(not evaluator.isLargerBetter())

    def test_multiclass_classification_evaluator(self):
        dataset = self.spark.createDataFrame(
            [
                (0.0, 0.0, 1.0, [0.1, 0.8, 0.1]),
                (0.0, 1.0, 1.0, [0.3, 0.4, 0.3]),
                (0.0, 0.0, 1.0, [0.9, 0.05, 0.05]),
                (1.0, 0.0, 1.0, [0.5, 0.2, 0.3]),
                (1.0, 1.0, 1.0, [0.2, 0.7, 0.1]),
                (1.0, 1.0, 1.0, [0.1, 0.3, 0.6]),
                (1.0, 1.0, 1.0, [0.2, 0.1, 0.7]),
                (2.0, 2.0, 1.0, [0.3, 0.2, 0.5]),
                (2.0, 0.0, 1.0, [0.6, 0.2, 0.2]),
            ],
            ["prediction", "label", "weight", "probability"],
        )

        evaluator = MulticlassClassificationEvaluator().setPredictionCol("prediction")

        f1_score = evaluator.evaluate(dataset)
        self.assertTrue(np.allclose(f1_score, 0.6613, atol=1e-4))

        # Evaluate the dataset using accuracy
        accuracy = evaluator.evaluate(dataset, {evaluator.metricName: "accuracy"})
        self.assertTrue(np.allclose(accuracy, 0.6666, atol=1e-4))

        # Evaluate the true positive rate for label 1.0
        true_positive_rate_label_1 = evaluator.evaluate(
            dataset, {evaluator.metricName: "truePositiveRateByLabel", evaluator.metricLabel: 1.0}
        )
        self.assertEqual(true_positive_rate_label_1, 0.75)

        # Set the metric to Hamming loss
        evaluator.setMetricName("hammingLoss")

        # Evaluate the dataset using Hamming loss
        hamming_loss = evaluator.evaluate(dataset)
        self.assertTrue(np.allclose(hamming_loss, 0.3333, atol=1e-4))

        # read/write
        with tempfile.TemporaryDirectory(prefix="multi_class_classification_evaluator") as tmp_dir:
            # Save the evaluator
            evaluator.write().overwrite().save(tmp_dir)
            # Load the saved evaluator
            evaluator2 = MulticlassClassificationEvaluator.load(tmp_dir)
            self.assertEqual(evaluator2.getPredictionCol(), "prediction")
            self.assertEqual(str(evaluator), str(evaluator2))

        # Initialize MulticlassClassificationEvaluator with weight column
        evaluator = MulticlassClassificationEvaluator(
            predictionCol="prediction", weightCol="weight"
        )

        # Evaluate the dataset with weights using default metric (f1 score)
        weighted_f1_score = evaluator.evaluate(dataset)
        self.assertTrue(np.allclose(weighted_f1_score, 0.6613, atol=1e-4))

        # Evaluate the dataset with weights using accuracy
        weighted_accuracy = evaluator.evaluate(dataset, {evaluator.metricName: "accuracy"})
        self.assertTrue(np.allclose(weighted_accuracy, 0.6666, atol=1e-4))

        evaluator = MulticlassClassificationEvaluator(
            predictionCol="prediction", probabilityCol="probability"
        )
        # Set the metric to log loss
        evaluator.setMetricName("logLoss")
        # Evaluate the dataset using log loss
        log_loss = evaluator.evaluate(dataset)
        self.assertTrue(np.allclose(log_loss, 1.0093, atol=1e-4))

        for metric in [
            "f1",
            "accuracy",
            "weightedPrecision",
            "weightedRecall",
            "weightedTruePositiveRate",
            "weightedFMeasure",
            "truePositiveRateByLabel",
            "precisionByLabel",
            "recallByLabel",
            "fMeasureByLabel",
        ]:
            evaluator.setMetricName(metric)
            self.assertTrue(evaluator.isLargerBetter())
        for metric in [
            "weightedFalsePositiveRate",
            "falsePositiveRateByLabel",
            "logLoss",
            "hammingLoss",
        ]:
            evaluator.setMetricName(metric)
            self.assertTrue(not evaluator.isLargerBetter())

    def test_binary_classification_evaluator(self):
        # Define score and labels data
        data = map(
            lambda x: (Vectors.dense([1.0 - x[0], x[0]]), x[1], x[2]),
            [
                (0.1, 0.0, 1.0),
                (0.1, 1.0, 0.9),
                (0.4, 0.0, 0.7),
                (0.6, 0.0, 0.9),
                (0.6, 1.0, 1.0),
                (0.6, 1.0, 0.3),
                (0.8, 1.0, 1.0),
            ],
        )
        dataset = self.spark.createDataFrame(data, ["raw", "label", "weight"])

        evaluator = BinaryClassificationEvaluator().setRawPredictionCol("raw")
        self.assertTrue(evaluator.isLargerBetter())

        auc_roc = evaluator.evaluate(dataset)
        self.assertTrue(np.allclose(auc_roc, 0.7083, atol=1e-4))

        # Evaluate the dataset using the areaUnderPR metric
        auc_pr = evaluator.evaluate(dataset, {evaluator.metricName: "areaUnderPR"})
        self.assertTrue(np.allclose(auc_pr, 0.8339, atol=1e-4))

        # read/write
        with tempfile.TemporaryDirectory(prefix="binary_classification_evaluator") as tmp_dir:
            # Save the evaluator
            evaluator.write().overwrite().save(tmp_dir)
            # Load the saved evaluator
            evaluator2 = BinaryClassificationEvaluator.load(tmp_dir)
            self.assertEqual(evaluator2.getRawPredictionCol(), "raw")
            self.assertEqual(str(evaluator), str(evaluator2))

        evaluator = BinaryClassificationEvaluator(rawPredictionCol="raw", weightCol="weight")

        # Evaluate the dataset with weights using the default metric (areaUnderROC)
        auc_roc_weighted = evaluator.evaluate(dataset)
        self.assertTrue(np.allclose(auc_roc_weighted, 0.7025, atol=1e-4))

        # Evaluate the dataset with weights using the areaUnderPR metric
        auc_pr_weighted = evaluator.evaluate(dataset, {evaluator.metricName: "areaUnderPR"})
        self.assertTrue(np.allclose(auc_pr_weighted, 0.8221, atol=1e-4))

        # Get the number of bins used to compute areaUnderROC
        num_bins = evaluator.getNumBins()
        self.assertEqual(num_bins, 1000)

    def test_clustering_evaluator(self):
        # Define feature and predictions data
        data = map(
            lambda x: (Vectors.dense(x[0]), x[1], x[2]),
            [
                ([0.0, 0.5], 0.0, 2.5),
                ([0.5, 0.0], 0.0, 2.5),
                ([10.0, 11.0], 1.0, 2.5),
                ([10.5, 11.5], 1.0, 2.5),
                ([1.0, 1.0], 0.0, 2.5),
                ([8.0, 6.0], 1.0, 2.5),
            ],
        )
        dataset = self.spark.createDataFrame(data, ["features", "prediction", "weight"])

        evaluator = ClusteringEvaluator().setPredictionCol("prediction")
        self.assertTrue(evaluator.isLargerBetter())

        score = evaluator.evaluate(dataset)
        self.assertTrue(np.allclose(score, 0.9079, atol=1e-4))

        evaluator.setWeightCol("weight")

        # Evaluate the dataset with weights
        score_with_weight = evaluator.evaluate(dataset)
        self.assertTrue(np.allclose(score_with_weight, 0.9079, atol=1e-4))

        # read/write
        with tempfile.TemporaryDirectory(prefix="clustering_evaluator") as tmp_dir:
            # Save the evaluator
            evaluator.write().overwrite().save(tmp_dir)
            # Load the saved evaluator
            evaluator2 = ClusteringEvaluator.load(tmp_dir)
            self.assertEqual(evaluator2.getPredictionCol(), "prediction")
            self.assertTrue(str(evaluator) == str(evaluator2))

    def test_clustering_evaluator_with_cosine_distance(self):
        featureAndPredictions = map(
            lambda x: (Vectors.dense(x[0]), x[1]),
            [
                ([1.0, 1.0], 1.0),
                ([10.0, 10.0], 1.0),
                ([1.0, 0.5], 2.0),
                ([10.0, 4.4], 2.0),
                ([-1.0, 1.0], 3.0),
                ([-100.0, 90.0], 3.0),
            ],
        )
        dataset = self.spark.createDataFrame(featureAndPredictions, ["features", "prediction"])
        evaluator = ClusteringEvaluator(predictionCol="prediction", distanceMeasure="cosine")
        self.assertEqual(evaluator.getDistanceMeasure(), "cosine")
        self.assertTrue(np.isclose(evaluator.evaluate(dataset), 0.992671213, atol=1e-5))

    def test_regression_evaluator(self):
        dataset = self.spark.createDataFrame(
            [
                (-28.98343821, -27.0, 1.0),
                (20.21491975, 21.5, 0.8),
                (-25.98418959, -22.0, 1.0),
                (30.69731842, 33.0, 0.6),
                (74.69283752, 71.0, 0.2),
            ],
            ["raw", "label", "weight"],
        )

        evaluator = RegressionEvaluator()
        evaluator.setPredictionCol("raw")

        # Evaluate dataset with default metric (RMSE)
        rmse = evaluator.evaluate(dataset)
        self.assertTrue(np.allclose(rmse, 2.8424, atol=1e-4))
        # Evaluate dataset with R2 metric
        r2 = evaluator.evaluate(dataset, {evaluator.metricName: "r2"})
        self.assertTrue(np.allclose(r2, 0.9939, atol=1e-4))
        # Evaluate dataset with MAE metric
        mae = evaluator.evaluate(dataset, {evaluator.metricName: "mae"})
        self.assertTrue(np.allclose(mae, 2.6496, atol=1e-4))
        # read/write
        with tempfile.TemporaryDirectory(prefix="save") as tmp_dir:
            # Save the evaluator
            evaluator.write().overwrite().save(tmp_dir)
            # Load the saved evaluator
            evaluator2 = RegressionEvaluator.load(tmp_dir)
            self.assertEqual(evaluator2.getPredictionCol(), "raw")
            self.assertTrue(str(evaluator) == str(evaluator2))

        evaluator_with_weights = RegressionEvaluator(predictionCol="raw", weightCol="weight")
        weighted_rmse = evaluator_with_weights.evaluate(dataset)
        self.assertTrue(np.allclose(weighted_rmse, 2.7405, atol=1e-4))
        through_origin = evaluator_with_weights.getThroughOrigin()
        self.assertEqual(through_origin, False)

        for metric in ["mse", "rmse", "mae"]:
            evaluator.setMetricName(metric)
            self.assertTrue(not evaluator.isLargerBetter())
        for metric in ["r2", "var"]:
            evaluator.setMetricName(metric)
            self.assertTrue(evaluator.isLargerBetter())


class EvaluatorTests(EvaluatorTestsMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.spark = SparkSession.builder.master("local[4]").getOrCreate()

    def tearDown(self) -> None:
        self.spark.stop()

    def test_evaluate_invalid_type(self):
        evaluator = RegressionEvaluator(metricName="r2")
        df = self.spark.createDataFrame([Row(label=1.0, prediction=1.1)])
        invalid_type = ""
        self.assertRaises(TypeError, evaluator.evaluate, df, invalid_type)

    def test_java_params(self):
        """
        This tests a bug fixed by SPARK-18274 which causes multiple copies
        of a Params instance in Python to be linked to the same Java instance.
        """
        evaluator = RegressionEvaluator(metricName="r2")
        df = self.spark.createDataFrame([Row(label=1.0, prediction=1.1)])
        evaluator.evaluate(df)
        self.assertEqual(evaluator._java_obj.getMetricName(), "r2")
        evaluatorCopy = evaluator.copy({evaluator.metricName: "mae"})
        evaluator.evaluate(df)
        evaluatorCopy.evaluate(df)
        self.assertEqual(evaluator._java_obj.getMetricName(), "r2")
        self.assertEqual(evaluatorCopy._java_obj.getMetricName(), "mae")


if __name__ == "__main__":
    from pyspark.ml.tests.test_evaluation import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
