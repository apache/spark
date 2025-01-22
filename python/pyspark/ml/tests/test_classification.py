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
    DecisionTreeClassifier,
    DecisionTreeClassificationModel,
    RandomForestClassifier,
    RandomForestClassificationModel,
    RandomForestClassificationSummary,
    RandomForestClassificationTrainingSummary,
    BinaryRandomForestClassificationSummary,
    BinaryRandomForestClassificationTrainingSummary,
    GBTClassifier,
    GBTClassificationModel,
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
        self.assertEqual(sorted(sameSummary.predictions.collect()), sorted(s.predictions.collect()))

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

        # We can't use sorted(s.predictions.collect()), since the DenseVector doesn't support "<"
        self.assertEqual(
            sameSummary.predictions.coalesce(1).sort("label", "weight", "prediction").collect(),
            s.predictions.coalesce(1).sort("label", "weight", "prediction").collect(),
        )

    def test_logistic_regression(self):
        # test sparse/dense vector and matrix
        lower_intercepts = Vectors.dense([1, 2, 3, 4])
        upper_intercepts = Vectors.sparse(4, [(1, 1.0), (3, 5.5)])
        lower_coefficients = Matrices.dense(3, 2, [0, 1, 4, 5, 9, 10])
        upper_coefficients = Matrices.sparse(1, 1, [0, 1], [0], [2.0])

        lr = LogisticRegression(
            maxIter=1,
            lowerBoundsOnIntercepts=lower_intercepts,
            upperBoundsOnIntercepts=upper_intercepts,
            lowerBoundsOnCoefficients=lower_coefficients,
            upperBoundsOnCoefficients=upper_coefficients,
        )
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
        self.assertEqual(
            lr.getLowerBoundsOnIntercepts(),
            lr2.getLowerBoundsOnIntercepts(),
        )
        self.assertEqual(
            lr.getUpperBoundsOnIntercepts(),
            lr2.getUpperBoundsOnIntercepts(),
        )
        self.assertEqual(
            lr.getLowerBoundsOnCoefficients(),
            lr2.getLowerBoundsOnCoefficients(),
        )
        self.assertEqual(
            lr.getUpperBoundsOnCoefficients(),
            lr2.getUpperBoundsOnCoefficients(),
        )
        try:
            rmtree(path)
        except OSError:
            pass

    def test_decision_tree_classifier(self):
        df = (
            self.spark.createDataFrame(
                [
                    (1.0, 1.0, Vectors.dense(0.0, 5.0)),
                    (0.0, 2.0, Vectors.dense(1.0, 2.0)),
                    (1.0, 3.0, Vectors.dense(2.0, 1.0)),
                    (0.0, 4.0, Vectors.dense(3.0, 3.0)),
                ],
                ["label", "weight", "features"],
            )
            .coalesce(1)
            .sortWithinPartitions("weight")
        )

        dt = DecisionTreeClassifier(
            maxDepth=2,
            labelCol="label",
            leafCol="leaf",
            seed=1,
        )
        self.assertEqual(dt.getMaxDepth(), 2)
        self.assertEqual(dt.getSeed(), 1)
        self.assertEqual(dt.getLabelCol(), "label")
        self.assertEqual(dt.getLeafCol(), "leaf")

        # Estimator save & load
        with tempfile.TemporaryDirectory(prefix="decision_tree_classifier") as d:
            dt.write().overwrite().save(d)
            dt2 = DecisionTreeClassifier.load(d)
            self.assertEqual(str(dt), str(dt2))

        model = dt.fit(df)
        self.assertEqual(model.numClasses, 2)
        self.assertEqual(model.numFeatures, 2)
        self.assertEqual(model.depth, 2)
        self.assertEqual(model.numNodes, 5)
        self.assertEqual(model.featureImportances, Vectors.sparse(2, [0], [1.0]))
        self.assertTrue("depth=2, numNodes=5, numClasses=2, numFeatures=2" in model.toDebugString)
        self.assertTrue("If (feature 0 <= 2.5)" in model.toDebugString)

        vec = Vectors.dense(0.0, 5.0)
        self.assertEqual(model.predict(vec), 1.0)
        self.assertEqual(model.predictRaw(vec), Vectors.dense(0.0, 1.0))
        self.assertEqual(model.predictProbability(vec), Vectors.dense(0.0, 1.0))
        self.assertEqual(model.predictLeaf(vec), 0.0)

        output = model.transform(df)
        expected_cols = [
            "label",
            "weight",
            "features",
            "rawPrediction",
            "probability",
            "prediction",
            "leaf",
        ]
        self.assertEqual(output.columns, expected_cols)
        self.assertEqual(output.count(), 4)

        # Model save & load
        with tempfile.TemporaryDirectory(prefix="decision_tree_model") as d:
            model.write().overwrite().save(d)
            model2 = DecisionTreeClassificationModel.load(d)
            self.assertEqual(str(model), str(model2))
            self.assertEqual(model.toDebugString, model2.toDebugString)

    def test_gbt_classifier(self):
        df = (
            self.spark.createDataFrame(
                [
                    (1.0, 1.0, Vectors.dense(0.0, 5.0)),
                    (0.0, 2.0, Vectors.dense(1.0, 2.0)),
                    (1.0, 3.0, Vectors.dense(2.0, 1.0)),
                    (0.0, 4.0, Vectors.dense(3.0, 3.0)),
                ],
                ["label", "weight", "features"],
            )
            .coalesce(1)
            .sortWithinPartitions("weight")
        )

        gbt = GBTClassifier(
            maxIter=3,
            maxDepth=2,
            labelCol="label",
            leafCol="leaf",
            seed=1,
        )
        self.assertEqual(gbt.getMaxIter(), 3)
        self.assertEqual(gbt.getMaxDepth(), 2)
        self.assertEqual(gbt.getSeed(), 1)
        self.assertEqual(gbt.getLabelCol(), "label")
        self.assertEqual(gbt.getLeafCol(), "leaf")

        # Estimator save & load
        with tempfile.TemporaryDirectory(prefix="gbt_classifier") as d:
            gbt.write().overwrite().save(d)
            gbt2 = GBTClassifier.load(d)
            self.assertEqual(str(gbt), str(gbt2))

        model = gbt.fit(df)
        self.assertEqual(model.numClasses, 2)
        self.assertEqual(model.numFeatures, 2)
        # TODO(SPARK-50843): Support access submodel in TreeEnsembleModel
        # model.trees
        self.assertEqual(model.treeWeights, [1.0, 0.1, 0.1])
        self.assertEqual(model.totalNumNodes, 15)
        self.assertTrue(np.allclose(model.featureImportances, [0.3333, 0.6667], atol=1e-4))
        self.assertTrue("numTrees=3, numClasses=2, numFeatures=2" in model.toDebugString)
        self.assertTrue("If (feature 0 <= 0.5)" in model.toDebugString)

        vec = Vectors.dense(0.0, 5.0)
        self.assertEqual(model.predict(vec), 1.0)
        self.assertTrue(np.allclose(model.predictRaw(vec), [-1.0915, 1.0915], atol=1e-4))
        self.assertTrue(np.allclose(model.predictProbability(vec), [0.1013, 0.8987], atol=1e-4))
        self.assertEqual(model.predictLeaf(vec), Vectors.dense(0.0, 0.0, 0.0))

        # GBT-specific method: evaluateEachIteration
        self.assertTrue(
            np.allclose(
                model.evaluateEachIteration(df),
                [0.253856022085945, 0.23205304779013333, 0.21358401299568353],
                atol=1e-4,
            )
        )

        output = model.transform(df)
        expected_cols = [
            "label",
            "weight",
            "features",
            "rawPrediction",
            "probability",
            "prediction",
            "leaf",
        ]
        self.assertEqual(output.columns, expected_cols)
        self.assertEqual(output.count(), 4)

        # Model save & load
        with tempfile.TemporaryDirectory(prefix="gbt_classification_model") as d:
            model.write().overwrite().save(d)
            model2 = GBTClassificationModel.load(d)
            self.assertEqual(str(model), str(model2))
            self.assertEqual(model.toDebugString, model2.toDebugString)

    def test_binary_random_forest_classifier(self):
        df = (
            self.spark.createDataFrame(
                [
                    (1.0, 1.0, Vectors.dense(0.0, 5.0)),
                    (0.0, 2.0, Vectors.dense(1.0, 2.0)),
                    (1.0, 3.0, Vectors.dense(2.0, 1.0)),
                    (0.0, 4.0, Vectors.dense(3.0, 3.0)),
                ],
                ["label", "weight", "features"],
            )
            .coalesce(1)
            .sortWithinPartitions("weight")
        )

        rf = RandomForestClassifier(
            numTrees=3,
            maxDepth=2,
            labelCol="label",
            leafCol="leaf",
            seed=1,
        )
        self.assertEqual(rf.getNumTrees(), 3)
        self.assertEqual(rf.getMaxDepth(), 2)
        self.assertEqual(rf.getSeed(), 1)
        self.assertEqual(rf.getLabelCol(), "label")
        self.assertEqual(rf.getLeafCol(), "leaf")

        # Estimator save & load
        with tempfile.TemporaryDirectory(prefix="binary_random_forest_classifier") as d:
            rf.write().overwrite().save(d)
            rf2 = RandomForestClassifier.load(d)
            self.assertEqual(str(rf), str(rf2))

        model = rf.fit(df)
        self.assertEqual(model.numClasses, 2)
        self.assertEqual(model.numFeatures, 2)
        # TODO(SPARK-50843): Support access submodel in TreeEnsembleModel
        # model.trees
        self.assertEqual(model.treeWeights, [1.0, 1.0, 1.0])
        self.assertEqual(model.totalNumNodes, 9)
        self.assertTrue(np.allclose(model.featureImportances, [0.7778, 0.2222], atol=1e-4))
        self.assertTrue("numTrees=3, numClasses=2, numFeatures=2" in model.toDebugString)
        self.assertTrue("If (feature 0 <= 0.5)" in model.toDebugString)

        vec = Vectors.dense(0.0, 5.0)
        self.assertEqual(model.predict(vec), 1.0)
        self.assertEqual(model.predictRaw(vec), Vectors.dense(1.0, 2.0))
        self.assertTrue(np.allclose(model.predictProbability(vec), [0.3333, 0.6667], atol=1e-4))
        self.assertEqual(model.predictLeaf(vec), Vectors.dense(0.0, 0.0, 1.0))

        output = model.transform(df)
        expected_cols = [
            "label",
            "weight",
            "features",
            "rawPrediction",
            "probability",
            "prediction",
            "leaf",
        ]
        self.assertEqual(output.columns, expected_cols)
        self.assertEqual(output.count(), 4)

        # model summary
        summary = model.summary
        self.assertTrue(isinstance(summary, BinaryRandomForestClassificationSummary))
        self.assertTrue(isinstance(summary, BinaryRandomForestClassificationTrainingSummary))
        self.assertEqual(summary.labels, [0.0, 1.0])
        self.assertEqual(summary.accuracy, 0.75)
        self.assertEqual(summary.areaUnderROC, 0.875)
        self.assertEqual(summary.predictions.columns, expected_cols)

        summary2 = model.evaluate(df)
        self.assertTrue(isinstance(summary2, BinaryRandomForestClassificationSummary))
        self.assertFalse(isinstance(summary2, BinaryRandomForestClassificationTrainingSummary))
        self.assertEqual(summary2.labels, [0.0, 1.0])
        self.assertEqual(summary2.accuracy, 0.75)
        self.assertEqual(summary.areaUnderROC, 0.875)
        self.assertEqual(summary2.predictions.columns, expected_cols)

        # Model save & load
        with tempfile.TemporaryDirectory(prefix="binary_random_forest_model") as d:
            model.write().overwrite().save(d)
            model2 = RandomForestClassificationModel.load(d)
            self.assertEqual(str(model), str(model2))
            self.assertEqual(model.toDebugString, model2.toDebugString)

    def test_multiclass_random_forest_classifier(self):
        df = (
            self.spark.createDataFrame(
                [
                    (1.0, 1.0, Vectors.dense(0.0, 5.0)),
                    (0.0, 2.0, Vectors.dense(1.0, 2.0)),
                    (1.0, 3.0, Vectors.dense(2.0, 1.0)),
                    (2.0, 4.0, Vectors.dense(3.0, 3.0)),
                ],
                ["label", "weight", "features"],
            )
            .coalesce(1)
            .sortWithinPartitions("weight")
        )

        rf = RandomForestClassifier(
            numTrees=3,
            maxDepth=2,
            labelCol="label",
            leafCol="leaf",
            seed=1,
        )
        self.assertEqual(rf.getNumTrees(), 3)
        self.assertEqual(rf.getMaxDepth(), 2)
        self.assertEqual(rf.getSeed(), 1)
        self.assertEqual(rf.getLabelCol(), "label")
        self.assertEqual(rf.getLeafCol(), "leaf")

        # Estimator save & load
        with tempfile.TemporaryDirectory(prefix="binary_random_forest_classifier") as d:
            rf.write().overwrite().save(d)
            rf2 = RandomForestClassifier.load(d)
            self.assertEqual(str(rf), str(rf2))

        model = rf.fit(df)
        self.assertEqual(model.numClasses, 3)
        self.assertEqual(model.numFeatures, 2)
        # TODO(SPARK-50843): Support access submodel in TreeEnsembleModel
        # model.trees
        self.assertEqual(model.treeWeights, [1.0, 1.0, 1.0])
        self.assertEqual(model.totalNumNodes, 9)
        self.assertEqual(model.featureImportances, Vectors.sparse(2, [0], [1.0]))
        self.assertTrue("numTrees=3, numClasses=3, numFeatures=2" in model.toDebugString)
        self.assertTrue("If (feature 0 <= 1.5)" in model.toDebugString)

        vec = Vectors.dense(0.0, 5.0)
        self.assertEqual(model.predict(vec), 0.0)
        self.assertEqual(model.predictRaw(vec), Vectors.dense(1.5, 1.5, 0.0))
        self.assertTrue(np.allclose(model.predictProbability(vec), [0.5, 0.5, 0.0], atol=1e-4))
        self.assertEqual(model.predictLeaf(vec), Vectors.dense(0.0, 0.0, 0.0))

        output = model.transform(df)
        expected_cols = [
            "label",
            "weight",
            "features",
            "rawPrediction",
            "probability",
            "prediction",
            "leaf",
        ]
        self.assertEqual(output.columns, expected_cols)
        self.assertEqual(output.count(), 4)

        # model summary
        summary = model.summary
        self.assertTrue(isinstance(summary, RandomForestClassificationSummary))
        self.assertTrue(isinstance(summary, RandomForestClassificationTrainingSummary))
        self.assertEqual(summary.labels, [0.0, 1.0, 2.0])
        self.assertEqual(summary.accuracy, 0.5)
        self.assertEqual(summary.predictions.columns, expected_cols)

        summary2 = model.evaluate(df)
        self.assertTrue(isinstance(summary2, RandomForestClassificationSummary))
        self.assertFalse(isinstance(summary2, RandomForestClassificationTrainingSummary))
        self.assertEqual(summary2.labels, [0.0, 1.0, 2.0])
        self.assertEqual(summary2.accuracy, 0.5)
        self.assertEqual(summary2.predictions.columns, expected_cols)

        # Model save & load
        with tempfile.TemporaryDirectory(prefix="multiclass_random_forest_model") as d:
            model.write().overwrite().save(d)
            model2 = RandomForestClassificationModel.load(d)
            self.assertEqual(str(model), str(model2))
            self.assertEqual(model.toDebugString, model2.toDebugString)


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
