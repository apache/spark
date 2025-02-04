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

from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession
from pyspark.ml.regression import (
    AFTSurvivalRegression,
    AFTSurvivalRegressionModel,
    IsotonicRegression,
    IsotonicRegressionModel,
    LinearRegression,
    LinearRegressionModel,
    GeneralizedLinearRegression,
    GeneralizedLinearRegressionModel,
    GeneralizedLinearRegressionSummary,
    GeneralizedLinearRegressionTrainingSummary,
    LinearRegressionSummary,
    LinearRegressionTrainingSummary,
    FMRegressor,
    FMRegressionModel,
    DecisionTreeRegressor,
    DecisionTreeRegressionModel,
    RandomForestRegressor,
    RandomForestRegressionModel,
    GBTRegressor,
    GBTRegressionModel,
)


class RegressionTestsMixin:
    @property
    def df(self):
        return (
            self.spark.createDataFrame(
                [
                    (1.0, 1.0, Vectors.dense(0.0, 5.0)),
                    (0.0, 2.0, Vectors.dense(1.0, 2.0)),
                    (1.5, 3.0, Vectors.dense(2.0, 1.0)),
                    (0.7, 4.0, Vectors.dense(1.5, 3.0)),
                ],
                ["label", "weight", "features"],
            )
            .coalesce(1)
            .sortWithinPartitions("weight")
        )

    def test_aft_survival(self):
        spark = self.spark
        df = spark.createDataFrame(
            [(1.0, Vectors.dense(1.0), 1.0), (1e-40, Vectors.sparse(1, [], []), 0.0)],
            ["label", "features", "censor"],
        )

        aft = AFTSurvivalRegression()
        aft.setMaxIter(1)
        self.assertEqual(aft.getMaxIter(), 1)

        model = aft.fit(df)
        self.assertEqual(aft.uid, model.uid)
        self.assertEqual(model.numFeatures, 1)
        self.assertTrue(np.allclose(model.intercept, 0.0, atol=1e-4), model.intercept)
        self.assertTrue(
            np.allclose(model.coefficients.toArray(), [0.0], atol=1e-4), model.coefficients
        )
        self.assertTrue(np.allclose(model.scale, 1.0, atol=1e-4), model.scale)

        vec = Vectors.dense(6.3)
        pred = model.predict(vec)
        self.assertEqual(pred, 1.0)
        pred = model.predictQuantiles(vec)
        self.assertTrue(
            np.allclose(
                pred,
                [
                    0.010050335853501444,
                    0.051293294387550536,
                    0.1053605156578263,
                    0.2876820724517809,
                    0.6931471805599453,
                    1.3862943611198906,
                    2.302585092994046,
                    2.9957322735539895,
                    4.60517018598809,
                ],
                atol=1e-4,
            ),
            pred,
        )

        output = model.transform(df)
        expected_cols = ["label", "features", "censor", "prediction"]
        self.assertEqual(output.columns, expected_cols)
        self.assertEqual(output.count(), 2)

        # Model save & load
        with tempfile.TemporaryDirectory(prefix="aft_survival") as d:
            aft.write().overwrite().save(d)
            aft2 = AFTSurvivalRegression.load(d)
            self.assertEqual(str(aft), str(aft2))

            model.write().overwrite().save(d)
            model2 = AFTSurvivalRegressionModel.load(d)
            self.assertEqual(str(model), str(model2))

    def test_isotonic_regression(self):
        spark = self.spark
        df = spark.createDataFrame(
            [(1.0, Vectors.dense(1.0)), (0.0, Vectors.sparse(1, [], []))], ["label", "features"]
        )

        ir = IsotonicRegression(
            isotonic=True,
            featureIndex=0,
        )
        self.assertTrue(ir.getIsotonic())
        self.assertEqual(ir.getFeatureIndex(), 0)

        model = ir.fit(df)
        self.assertEqual(model.numFeatures, 1)
        self.assertTrue(
            np.allclose(model.boundaries.toArray(), [0.0, 1.0], atol=1e-4), model.boundaries
        )
        self.assertTrue(
            np.allclose(model.predictions.toArray(), [0.0, 1.0], atol=1e-4), model.predictions
        )

        pred = model.predict(1.0)
        self.assertTrue(np.allclose(pred, 1.0, atol=1e-4), pred)

        output = model.transform(df)
        expected_cols = ["label", "features", "prediction"]
        self.assertEqual(output.columns, expected_cols)
        self.assertEqual(output.count(), 2)

        # Model save & load
        with tempfile.TemporaryDirectory(prefix="isotonic_regression") as d:
            ir.write().overwrite().save(d)
            ir2 = IsotonicRegression.load(d)
            self.assertEqual(str(ir), str(ir2))

            model.write().overwrite().save(d)
            model2 = IsotonicRegressionModel.load(d)
            self.assertEqual(str(model), str(model2))

    def test_linear_regression(self):
        df = self.df
        lr = LinearRegression(
            regParam=0.0,
            maxIter=2,
            solver="normal",
            weightCol="weight",
        )
        self.assertEqual(lr.getRegParam(), 0)
        self.assertEqual(lr.getMaxIter(), 2)
        self.assertEqual(lr.getSolver(), "normal")
        self.assertEqual(lr.getWeightCol(), "weight")

        model = lr.fit(df)
        self.assertEqual(lr.uid, model.uid)
        self.assertEqual(model.numFeatures, 2)
        self.assertTrue(np.allclose(model.scale, 1.0, atol=1e-4))
        self.assertTrue(np.allclose(model.intercept, -0.35, atol=1e-4))
        self.assertTrue(np.allclose(model.coefficients, [0.65, 0.1125], atol=1e-4))

        output = model.transform(df)
        expected_cols = [
            "label",
            "weight",
            "features",
            "prediction",
        ]
        self.assertEqual(output.columns, expected_cols)
        self.assertEqual(output.count(), 4)

        self.assertTrue(
            np.allclose(model.predict(Vectors.dense(0.0, 5.0)), 0.21249999999999963, atol=1e-4)
        )

        # Model summary
        summary = model.summary
        self.assertTrue(isinstance(summary, LinearRegressionSummary))
        self.assertTrue(isinstance(summary, LinearRegressionTrainingSummary))
        self.assertEqual(summary.predictions.columns, expected_cols)
        self.assertEqual(summary.predictions.count(), 4)
        self.assertEqual(summary.residuals.columns, ["residuals"])
        self.assertEqual(summary.residuals.count(), 4)

        self.assertEqual(summary.degreesOfFreedom, 1)
        self.assertEqual(summary.numInstances, 4)
        self.assertEqual(summary.objectiveHistory, [0.0])
        self.assertTrue(
            np.allclose(
                summary.coefficientStandardErrors,
                [1.2859821149611763, 0.6248749874975031, 3.1645497310044184],
                atol=1e-4,
            )
        )
        self.assertTrue(
            np.allclose(
                summary.devianceResiduals, [-0.7424621202458727, 0.7875000000000003], atol=1e-4
            )
        )
        self.assertTrue(
            np.allclose(
                summary.pValues,
                [0.7020630236843428, 0.8866003086182783, 0.9298746994547682],
                atol=1e-4,
            )
        )
        self.assertTrue(
            np.allclose(
                summary.tValues,
                [0.5054502643838291, 0.1800360108036021, -0.11060025272186746],
                atol=1e-4,
            )
        )
        self.assertTrue(np.allclose(summary.explainedVariance, 0.07997500000000031, atol=1e-4))
        self.assertTrue(np.allclose(summary.meanAbsoluteError, 0.4200000000000002, atol=1e-4))
        self.assertTrue(np.allclose(summary.meanSquaredError, 0.20212500000000005, atol=1e-4))
        self.assertTrue(np.allclose(summary.rootMeanSquaredError, 0.44958314025327956, atol=1e-4))
        self.assertTrue(np.allclose(summary.r2, 0.4427212572373862, atol=1e-4))
        self.assertTrue(np.allclose(summary.r2adj, -0.6718362282878414, atol=1e-4))

        summary2 = model.evaluate(df)
        self.assertTrue(isinstance(summary2, LinearRegressionSummary))
        self.assertFalse(isinstance(summary2, LinearRegressionTrainingSummary))
        self.assertEqual(summary2.predictions.columns, expected_cols)
        self.assertEqual(summary2.predictions.count(), 4)
        self.assertEqual(summary2.residuals.columns, ["residuals"])
        self.assertEqual(summary2.residuals.count(), 4)

        self.assertEqual(summary2.degreesOfFreedom, 1)
        self.assertEqual(summary2.numInstances, 4)
        self.assertTrue(
            np.allclose(
                summary2.devianceResiduals, [-0.7424621202458727, 0.7875000000000003], atol=1e-4
            )
        )
        self.assertTrue(np.allclose(summary2.explainedVariance, 0.07997500000000031, atol=1e-4))
        self.assertTrue(np.allclose(summary2.meanAbsoluteError, 0.4200000000000002, atol=1e-4))
        self.assertTrue(np.allclose(summary2.meanSquaredError, 0.20212500000000005, atol=1e-4))
        self.assertTrue(np.allclose(summary2.rootMeanSquaredError, 0.44958314025327956, atol=1e-4))
        self.assertTrue(np.allclose(summary2.r2, 0.4427212572373862, atol=1e-4))
        self.assertTrue(np.allclose(summary2.r2adj, -0.6718362282878414, atol=1e-4))

        # Model save & load
        with tempfile.TemporaryDirectory(prefix="linear_regression") as d:
            lr.write().overwrite().save(d)
            lr2 = LinearRegression.load(d)
            self.assertEqual(str(lr), str(lr2))

            model.write().overwrite().save(d)
            model2 = LinearRegressionModel.load(d)
            self.assertEqual(str(model), str(model2))

    def test_generalized_linear_regression(self):
        spark = self.spark
        df = (
            spark.createDataFrame(
                [
                    (1, 1.0, Vectors.dense(0.0, 0.0)),
                    (2, 1.0, Vectors.dense(1.0, 2.0)),
                    (3, 2.0, Vectors.dense(0.0, 0.0)),
                    (4, 2.0, Vectors.dense(1.0, 1.0)),
                ],
                ["index", "label", "features"],
            )
            .coalesce(1)
            .sortWithinPartitions("index")
            .select("label", "features")
        )

        glr = GeneralizedLinearRegression(
            family="gaussian",
            link="identity",
            linkPredictionCol="p",
        )
        glr.setRegParam(0.1)
        glr.setMaxIter(1)
        self.assertEqual(glr.getFamily(), "gaussian")
        self.assertEqual(glr.getLink(), "identity")
        self.assertEqual(glr.getLinkPredictionCol(), "p")
        self.assertEqual(glr.getRegParam(), 0.1)
        self.assertEqual(glr.getMaxIter(), 1)

        model = glr.fit(df)
        self.assertTrue(np.allclose(model.intercept, 1.543859649122807, atol=1e-4), model.intercept)
        self.assertTrue(
            np.allclose(model.coefficients.toArray(), [0.43859649, -0.35087719], atol=1e-4),
            model.coefficients,
        )
        self.assertEqual(model.numFeatures, 2)

        vec = Vectors.dense(1.0, 2.0)
        pred = model.predict(vec)
        self.assertTrue(np.allclose(pred, 1.280701754385965, atol=1e-4), pred)

        expected_cols = ["label", "features", "p", "prediction"]

        output = model.transform(df)
        self.assertEqual(output.columns, expected_cols)
        self.assertEqual(output.count(), 4)

        # Model summary
        self.assertTrue(model.hasSummary)

        summary = model.summary
        self.assertIsInstance(summary, GeneralizedLinearRegressionSummary)
        self.assertIsInstance(summary, GeneralizedLinearRegressionTrainingSummary)
        self.assertEqual(summary.numIterations, 1)
        self.assertEqual(summary.numInstances, 4)
        self.assertEqual(summary.rank, 3)
        self.assertTrue(
            np.allclose(
                summary.tValues,
                [0.3725037662281711, -0.49418209022924164, 2.6589353685797654],
                atol=1e-4,
            ),
            summary.tValues,
        )
        self.assertTrue(
            np.allclose(
                summary.pValues,
                [0.7729938686180984, 0.707802691825973, 0.22900885781807023],
                atol=1e-4,
            ),
            summary.pValues,
        )
        self.assertEqual(summary.predictions.columns, expected_cols)
        self.assertEqual(summary.predictions.count(), 4)
        self.assertEqual(summary.residuals().columns, ["devianceResiduals"])
        self.assertEqual(summary.residuals().count(), 4)

        summary2 = model.evaluate(df)
        self.assertIsInstance(summary2, GeneralizedLinearRegressionSummary)
        self.assertNotIsInstance(summary2, GeneralizedLinearRegressionTrainingSummary)
        self.assertEqual(summary2.numInstances, 4)
        self.assertEqual(summary2.rank, 3)
        self.assertEqual(summary.predictions.columns, expected_cols)
        self.assertEqual(summary.predictions.count(), 4)
        self.assertEqual(summary2.residuals().columns, ["devianceResiduals"])
        self.assertEqual(summary2.residuals().count(), 4)

        # Model save & load
        with tempfile.TemporaryDirectory(prefix="generalized_linear_regression") as d:
            glr.write().overwrite().save(d)
            glr2 = GeneralizedLinearRegression.load(d)
            self.assertEqual(str(glr), str(glr2))

            model.write().overwrite().save(d)
            model2 = GeneralizedLinearRegressionModel.load(d)
            self.assertEqual(str(model), str(model2))

    def test_factorization_machine(self):
        spark = self.spark
        df = (
            spark.createDataFrame(
                [
                    (1, 1.0, Vectors.dense(0.0, 0.0)),
                    (2, 1.0, Vectors.dense(1.0, 2.0)),
                    (3, 2.0, Vectors.dense(0.0, 0.0)),
                    (4, 2.0, Vectors.dense(1.0, 1.0)),
                ],
                ["index", "label", "features"],
            )
            .coalesce(1)
            .sortWithinPartitions("index")
            .select("label", "features")
        )

        fm = FMRegressor(factorSize=2, maxIter=1, regParam=1.0, seed=1)
        self.assertEqual(fm.getFactorSize(), 2)
        self.assertEqual(fm.getMaxIter(), 1)
        self.assertEqual(fm.getRegParam(), 1.0)
        self.assertEqual(fm.getSeed(), 1)

        model = fm.fit(df)
        self.assertEqual(fm.uid, model.uid)
        self.assertEqual(model.numFeatures, 2)
        self.assertTrue(
            np.allclose(model.intercept, 0.9999999966668874, atol=1e-4), model.intercept
        )
        self.assertTrue(
            np.allclose(
                model.linear.toArray(), [0.9999999933342161, 0.9999999950008276], atol=1e-4
            ),
            model.linear,
        )
        self.assertTrue(
            np.allclose(
                model.factors.toArray(),
                [[-0.99999954, -0.9999992], [0.99999968, -0.99999918]],
                atol=1e-4,
            ),
            model.factors,
        )

        vec = Vectors.dense(0.0, 5.0)
        pred = model.predict(vec)
        self.assertTrue(np.allclose(pred, 5.999999971671025, atol=1e-4), pred)

        output = model.transform(df)
        expected_cols = ["label", "features", "prediction"]
        self.assertEqual(output.columns, expected_cols)
        self.assertEqual(output.count(), 4)

        # Model save & load
        with tempfile.TemporaryDirectory(prefix="factorization_machine") as d:
            fm.write().overwrite().save(d)
            fm2 = FMRegressor.load(d)
            self.assertEqual(str(fm), str(fm2))

            model.write().overwrite().save(d)
            model2 = FMRegressionModel.load(d)
            self.assertEqual(str(model), str(model2))

    def test_decision_tree_regressor(self):
        df = self.df

        dt = DecisionTreeRegressor(
            maxDepth=2,
            labelCol="label",
            leafCol="leaf",
            seed=1,
        )
        self.assertEqual(dt.getMaxDepth(), 2)
        self.assertEqual(dt.getSeed(), 1)
        self.assertEqual(dt.getLabelCol(), "label")
        self.assertEqual(dt.getLeafCol(), "leaf")

        model = dt.fit(df)
        self.assertEqual(dt.uid, model.uid)
        self.assertEqual(model.numFeatures, 2)
        self.assertEqual(model.depth, 2)
        self.assertEqual(model.numNodes, 5)

        featureImportances = model.featureImportances
        self.assertTrue(
            np.allclose(featureImportances, [0.5756, 0.4244], atol=1e-4),
            featureImportances,
        )

        debugString = model.toDebugString
        self.assertTrue("depth=2, numNodes=5, numFeatures=2" in debugString, debugString)
        self.assertTrue("If (feature 0 <= 1.75)" in debugString, debugString)

        vec = Vectors.dense(0.0, 5.0)
        self.assertTrue(np.allclose(model.predict(vec), 0.85, atol=1e-4))
        self.assertEqual(model.predictLeaf(vec), 1.0)

        output = model.transform(df)
        expected_cols = [
            "label",
            "weight",
            "features",
            "prediction",
            "leaf",
        ]
        self.assertEqual(output.columns, expected_cols)
        self.assertEqual(output.count(), 4)

        # Model save & load
        with tempfile.TemporaryDirectory(prefix="decision_tree_regression") as d:
            dt.write().overwrite().save(d)
            dt2 = DecisionTreeRegressor.load(d)
            self.assertEqual(str(dt), str(dt2))

            model.write().overwrite().save(d)
            model2 = DecisionTreeRegressionModel.load(d)
            self.assertEqual(str(model), str(model2))
            self.assertEqual(model.toDebugString, model2.toDebugString)

    def test_gbt_regressor(self):
        df = self.df

        gbt = GBTRegressor(
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

        model = gbt.fit(df)
        self.assertEqual(gbt.uid, model.uid)
        self.assertEqual(model.numFeatures, 2)
        self.assertEqual(model.treeWeights, [1.0, 0.1, 0.1])
        self.assertEqual(model.totalNumNodes, 15)

        featureImportances = model.featureImportances
        self.assertTrue(
            np.allclose(featureImportances, [0.5944156994359766, 0.4055843005640234], atol=1e-4),
            featureImportances,
        )

        debugString = model.toDebugString
        self.assertTrue("numTrees=3, numFeatures=2" in debugString, debugString)
        self.assertTrue("If (feature 0 <= 1.75)" in debugString, debugString)

        vec = Vectors.dense(0.0, 5.0)
        self.assertTrue(np.allclose(model.predict(vec), 0.904, atol=1e-4))
        self.assertEqual(model.predictLeaf(vec), Vectors.dense(1.0, 0.0, 0.0))

        # GBT-specific method: evaluateEachIteration
        self.assertTrue(
            np.allclose(
                model.evaluateEachIteration(df, "squared"),
                [0.011250000000000003, 0.0072, 0.0046079999999999975],
                atol=1e-4,
            )
        )
        self.assertTrue(
            np.allclose(
                model.evaluateEachIteration(df, "absolute"),
                [0.07500000000000007, 0.06000000000000006, 0.048000000000000057],
                atol=1e-4,
            )
        )

        output = model.transform(df)
        expected_cols = [
            "label",
            "weight",
            "features",
            "prediction",
            "leaf",
        ]
        self.assertEqual(output.columns, expected_cols)
        self.assertEqual(output.count(), 4)

        trees = model.trees
        self.assertEqual(len(trees), 3)
        for tree in trees:
            self.assertIsInstance(tree, DecisionTreeRegressionModel)
            self.assertTrue(tree.predict(vec) > -10)
            self.assertEqual(tree.transform(df).count(), 4)
            self.assertEqual(
                tree.transform(df).columns,
                ["label", "weight", "features", "prediction", "leaf"],
            )

        # save & load
        with tempfile.TemporaryDirectory(prefix="gbt_regression") as d:
            gbt.write().overwrite().save(d)
            gbt2 = GBTRegressor.load(d)
            self.assertEqual(str(gbt), str(gbt2))

            model.write().overwrite().save(d)
            model2 = GBTRegressionModel.load(d)
            self.assertEqual(str(model), str(model2))
            self.assertEqual(model.toDebugString, model2.toDebugString)

    def test_random_forest_regressor(self):
        df = self.df

        rf = RandomForestRegressor(
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

        model = rf.fit(df)
        self.assertEqual(rf.uid, model.uid)
        self.assertEqual(model.numFeatures, 2)
        self.assertEqual(model.treeWeights, [1.0, 1.0, 1.0])
        self.assertEqual(model.totalNumNodes, 11)

        featureImportances = model.featureImportances
        self.assertTrue(
            np.allclose(featureImportances, [0.5615222294986538, 0.43847777050134623], atol=1e-4),
            featureImportances,
        )

        debugString = model.toDebugString
        self.assertTrue("numTrees=3, numFeatures=2" in debugString, debugString)
        self.assertTrue("If (feature 0 <= 1.75)" in debugString, debugString)

        vec = Vectors.dense(0.0, 5.0)
        self.assertTrue(np.allclose(model.predict(vec), 0.6166666666666667, atol=1e-4))
        self.assertEqual(model.predictLeaf(vec), Vectors.dense(1.0, 0.0, 1.0))

        output = model.transform(df)
        expected_cols = [
            "label",
            "weight",
            "features",
            "prediction",
            "leaf",
        ]
        self.assertEqual(output.columns, expected_cols)
        self.assertEqual(output.count(), 4)

        trees = model.trees
        self.assertEqual(len(trees), 3)
        for tree in trees:
            self.assertIsInstance(tree, DecisionTreeRegressionModel)
            self.assertTrue(tree.predict(vec) > -10)
            self.assertEqual(tree.transform(df).count(), 4)
            self.assertEqual(
                tree.transform(df).columns,
                ["label", "weight", "features", "prediction", "leaf"],
            )

        with tempfile.TemporaryDirectory(prefix="random_forest_regression") as d:
            rf.write().overwrite().save(d)
            rf2 = RandomForestRegressor.load(d)
            self.assertEqual(str(rf), str(rf2))

            model.write().overwrite().save(d)
            model2 = RandomForestRegressionModel.load(d)
            self.assertEqual(str(model), str(model2))
            self.assertEqual(model.toDebugString, model2.toDebugString)


class RegressionTests(RegressionTestsMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.spark = SparkSession.builder.master("local[4]").getOrCreate()

    def tearDown(self) -> None:
        self.spark.stop()


if __name__ == "__main__":
    from pyspark.ml.tests.test_regression import *  # noqa: F401,F403

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
