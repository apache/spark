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
from shutil import rmtree
import unittest

from numpy import array, array_equal
from py4j.protocol import Py4JJavaError

from pyspark.mllib.fpm import FPGrowth
from pyspark.mllib.recommendation import Rating
from pyspark.mllib.regression import LabeledPoint
from pyspark.serializers import PickleSerializer
from pyspark.testing.mllibutils import MLlibTestCase


class ListTests(MLlibTestCase):

    """
    Test MLlib algorithms on plain lists, to make sure they're passed through
    as NumPy arrays.
    """

    def test_bisecting_kmeans(self):
        from pyspark.mllib.clustering import BisectingKMeans

        data = array([0.0, 0.0, 1.0, 1.0, 9.0, 8.0, 8.0, 9.0]).reshape(4, 2)
        bskm = BisectingKMeans()
        model = bskm.train(self.sc.parallelize(data, 2), k=4)
        p = array([0.0, 0.0])
        rdd_p = self.sc.parallelize([p])
        self.assertEqual(model.predict(p), model.predict(rdd_p).first())
        self.assertEqual(model.computeCost(p), model.computeCost(rdd_p))
        self.assertEqual(model.k, len(model.clusterCenters))

    def test_kmeans(self):
        from pyspark.mllib.clustering import KMeans

        data = [
            [0, 1.1],
            [0, 1.2],
            [1.1, 0],
            [1.2, 0],
        ]
        clusters = KMeans.train(
            self.sc.parallelize(data),
            2,
            initializationMode="k-means||",
            initializationSteps=7,
            epsilon=1e-4,
        )
        self.assertEqual(clusters.predict(data[0]), clusters.predict(data[1]))
        self.assertEqual(clusters.predict(data[2]), clusters.predict(data[3]))

    def test_kmeans_deterministic(self):
        from pyspark.mllib.clustering import KMeans

        X = range(0, 100, 10)
        Y = range(0, 100, 10)
        data = [[x, y] for x, y in zip(X, Y)]
        clusters1 = KMeans.train(
            self.sc.parallelize(data),
            3,
            initializationMode="k-means||",
            seed=42,
            initializationSteps=7,
            epsilon=1e-4,
        )
        clusters2 = KMeans.train(
            self.sc.parallelize(data),
            3,
            initializationMode="k-means||",
            seed=42,
            initializationSteps=7,
            epsilon=1e-4,
        )
        centers1 = clusters1.centers
        centers2 = clusters2.centers
        for c1, c2 in zip(centers1, centers2):
            # TODO: Allow small numeric difference.
            self.assertTrue(array_equal(c1, c2))

    def test_gmm(self):
        from pyspark.mllib.clustering import GaussianMixture

        data = self.sc.parallelize(
            [
                [1, 2],
                [8, 9],
                [-4, -3],
                [-6, -7],
            ]
        )
        clusters = GaussianMixture.train(data, 2, convergenceTol=0.001, maxIterations=10, seed=1)
        labels = clusters.predict(data).collect()
        self.assertEqual(labels[0], labels[1])
        self.assertEqual(labels[2], labels[3])

    def test_gmm_deterministic(self):
        from pyspark.mllib.clustering import GaussianMixture

        x = range(0, 100, 10)
        y = range(0, 100, 10)
        data = self.sc.parallelize([[a, b] for a, b in zip(x, y)])
        clusters1 = GaussianMixture.train(data, 5, convergenceTol=0.001, maxIterations=10, seed=63)
        clusters2 = GaussianMixture.train(data, 5, convergenceTol=0.001, maxIterations=10, seed=63)
        for c1, c2 in zip(clusters1.weights, clusters2.weights):
            self.assertEqual(round(c1, 7), round(c2, 7))

    def test_gmm_with_initial_model(self):
        from pyspark.mllib.clustering import GaussianMixture

        data = self.sc.parallelize([(-10, -5), (-9, -4), (10, 5), (9, 4)])

        gmm1 = GaussianMixture.train(data, 2, convergenceTol=0.001, maxIterations=10, seed=63)
        gmm2 = GaussianMixture.train(
            data, 2, convergenceTol=0.001, maxIterations=10, seed=63, initialModel=gmm1
        )
        self.assertAlmostEqual((gmm1.weights - gmm2.weights).sum(), 0.0)

    def test_classification(self):
        from pyspark.mllib.classification import LogisticRegressionWithSGD, SVMWithSGD, NaiveBayes
        from pyspark.mllib.tree import (
            DecisionTree,
            DecisionTreeModel,
            RandomForest,
            RandomForestModel,
            GradientBoostedTrees,
            GradientBoostedTreesModel,
        )

        data = [
            LabeledPoint(0.0, [1, 0, 0]),
            LabeledPoint(1.0, [0, 1, 1]),
            LabeledPoint(0.0, [2, 0, 0]),
            LabeledPoint(1.0, [0, 2, 1]),
        ]
        rdd = self.sc.parallelize(data)
        features = [p.features.tolist() for p in data]

        temp_dir = tempfile.mkdtemp()

        lr_model = LogisticRegressionWithSGD.train(rdd, iterations=10)
        self.assertTrue(lr_model.predict(features[0]) <= 0)
        self.assertTrue(lr_model.predict(features[1]) > 0)
        self.assertTrue(lr_model.predict(features[2]) <= 0)
        self.assertTrue(lr_model.predict(features[3]) > 0)

        svm_model = SVMWithSGD.train(rdd, iterations=10)
        self.assertTrue(svm_model.predict(features[0]) <= 0)
        self.assertTrue(svm_model.predict(features[1]) > 0)
        self.assertTrue(svm_model.predict(features[2]) <= 0)
        self.assertTrue(svm_model.predict(features[3]) > 0)

        nb_model = NaiveBayes.train(rdd)
        self.assertTrue(nb_model.predict(features[0]) <= 0)
        self.assertTrue(nb_model.predict(features[1]) > 0)
        self.assertTrue(nb_model.predict(features[2]) <= 0)
        self.assertTrue(nb_model.predict(features[3]) > 0)

        categoricalFeaturesInfo = {0: 3}  # feature 0 has 3 categories
        dt_model = DecisionTree.trainClassifier(
            rdd, numClasses=2, categoricalFeaturesInfo=categoricalFeaturesInfo, maxBins=4
        )
        self.assertTrue(dt_model.predict(features[0]) <= 0)
        self.assertTrue(dt_model.predict(features[1]) > 0)
        self.assertTrue(dt_model.predict(features[2]) <= 0)
        self.assertTrue(dt_model.predict(features[3]) > 0)

        dt_model_dir = os.path.join(temp_dir, "dt")
        dt_model.save(self.sc, dt_model_dir)
        same_dt_model = DecisionTreeModel.load(self.sc, dt_model_dir)
        self.assertEqual(same_dt_model.toDebugString(), dt_model.toDebugString())

        rf_model = RandomForest.trainClassifier(
            rdd,
            numClasses=2,
            categoricalFeaturesInfo=categoricalFeaturesInfo,
            numTrees=10,
            maxBins=4,
            seed=1,
        )
        self.assertTrue(rf_model.predict(features[0]) <= 0)
        self.assertTrue(rf_model.predict(features[1]) > 0)
        self.assertTrue(rf_model.predict(features[2]) <= 0)
        self.assertTrue(rf_model.predict(features[3]) > 0)

        rf_model_dir = os.path.join(temp_dir, "rf")
        rf_model.save(self.sc, rf_model_dir)
        same_rf_model = RandomForestModel.load(self.sc, rf_model_dir)
        self.assertEqual(same_rf_model.toDebugString(), rf_model.toDebugString())

        gbt_model = GradientBoostedTrees.trainClassifier(
            rdd, categoricalFeaturesInfo=categoricalFeaturesInfo, numIterations=4
        )
        self.assertTrue(gbt_model.predict(features[0]) <= 0)
        self.assertTrue(gbt_model.predict(features[1]) > 0)
        self.assertTrue(gbt_model.predict(features[2]) <= 0)
        self.assertTrue(gbt_model.predict(features[3]) > 0)

        gbt_model_dir = os.path.join(temp_dir, "gbt")
        gbt_model.save(self.sc, gbt_model_dir)
        same_gbt_model = GradientBoostedTreesModel.load(self.sc, gbt_model_dir)
        self.assertEqual(same_gbt_model.toDebugString(), gbt_model.toDebugString())

        try:
            rmtree(temp_dir)
        except OSError:
            pass

    def test_regression(self):
        from pyspark.mllib.regression import (
            LinearRegressionWithSGD,
            LassoWithSGD,
            RidgeRegressionWithSGD,
        )
        from pyspark.mllib.tree import DecisionTree, RandomForest, GradientBoostedTrees

        data = [
            LabeledPoint(-1.0, [0, -1]),
            LabeledPoint(1.0, [0, 1]),
            LabeledPoint(-1.0, [0, -2]),
            LabeledPoint(1.0, [0, 2]),
        ]
        rdd = self.sc.parallelize(data)
        features = [p.features.tolist() for p in data]

        lr_model = LinearRegressionWithSGD.train(rdd, iterations=10)
        self.assertTrue(lr_model.predict(features[0]) <= 0)
        self.assertTrue(lr_model.predict(features[1]) > 0)
        self.assertTrue(lr_model.predict(features[2]) <= 0)
        self.assertTrue(lr_model.predict(features[3]) > 0)

        lasso_model = LassoWithSGD.train(rdd, iterations=10)
        self.assertTrue(lasso_model.predict(features[0]) <= 0)
        self.assertTrue(lasso_model.predict(features[1]) > 0)
        self.assertTrue(lasso_model.predict(features[2]) <= 0)
        self.assertTrue(lasso_model.predict(features[3]) > 0)

        rr_model = RidgeRegressionWithSGD.train(rdd, iterations=10)
        self.assertTrue(rr_model.predict(features[0]) <= 0)
        self.assertTrue(rr_model.predict(features[1]) > 0)
        self.assertTrue(rr_model.predict(features[2]) <= 0)
        self.assertTrue(rr_model.predict(features[3]) > 0)

        categoricalFeaturesInfo = {0: 2}  # feature 0 has 2 categories
        dt_model = DecisionTree.trainRegressor(
            rdd, categoricalFeaturesInfo=categoricalFeaturesInfo, maxBins=4
        )
        self.assertTrue(dt_model.predict(features[0]) <= 0)
        self.assertTrue(dt_model.predict(features[1]) > 0)
        self.assertTrue(dt_model.predict(features[2]) <= 0)
        self.assertTrue(dt_model.predict(features[3]) > 0)

        rf_model = RandomForest.trainRegressor(
            rdd, categoricalFeaturesInfo=categoricalFeaturesInfo, numTrees=10, maxBins=4, seed=1
        )
        self.assertTrue(rf_model.predict(features[0]) <= 0)
        self.assertTrue(rf_model.predict(features[1]) > 0)
        self.assertTrue(rf_model.predict(features[2]) <= 0)
        self.assertTrue(rf_model.predict(features[3]) > 0)

        gbt_model = GradientBoostedTrees.trainRegressor(
            rdd, categoricalFeaturesInfo=categoricalFeaturesInfo, numIterations=4
        )
        self.assertTrue(gbt_model.predict(features[0]) <= 0)
        self.assertTrue(gbt_model.predict(features[1]) > 0)
        self.assertTrue(gbt_model.predict(features[2]) <= 0)
        self.assertTrue(gbt_model.predict(features[3]) > 0)

        try:
            LinearRegressionWithSGD.train(rdd, initialWeights=array([1.0, 1.0]), iterations=10)
            LassoWithSGD.train(rdd, initialWeights=array([1.0, 1.0]), iterations=10)
            RidgeRegressionWithSGD.train(rdd, initialWeights=array([1.0, 1.0]), iterations=10)
        except ValueError:
            self.fail()

        # Verify that maxBins is being passed through
        GradientBoostedTrees.trainRegressor(
            rdd, categoricalFeaturesInfo=categoricalFeaturesInfo, numIterations=4, maxBins=32
        )
        with self.assertRaises(Exception) as cm:
            GradientBoostedTrees.trainRegressor(
                rdd, categoricalFeaturesInfo=categoricalFeaturesInfo, numIterations=4, maxBins=1
            )


class ALSTests(MLlibTestCase):
    def test_als_ratings_serialize(self):
        ser = PickleSerializer()
        r = Rating(7, 1123, 3.14)
        jr = self.sc._jvm.org.apache.spark.mllib.api.python.SerDe.loads(bytearray(ser.dumps(r)))
        nr = ser.loads(bytes(self.sc._jvm.org.apache.spark.mllib.api.python.SerDe.dumps(jr)))
        self.assertEqual(r.user, nr.user)
        self.assertEqual(r.product, nr.product)
        self.assertAlmostEqual(r.rating, nr.rating, 2)

    def test_als_ratings_id_long_error(self):
        ser = PickleSerializer()
        r = Rating(1205640308657491975, 50233468418, 1.0)
        # rating user id exceeds max int value, should fail when pickled
        self.assertRaises(
            Py4JJavaError,
            self.sc._jvm.org.apache.spark.mllib.api.python.SerDe.loads,
            bytearray(ser.dumps(r)),
        )


class FPGrowthTest(MLlibTestCase):
    def test_fpgrowth(self):
        data = [["a", "b", "c"], ["a", "b", "d", "e"], ["a", "c", "e"], ["a", "c", "f"]]
        rdd = self.sc.parallelize(data, 2)
        model1 = FPGrowth.train(rdd, 0.6, 2)
        # use default data partition number when numPartitions is not specified
        model2 = FPGrowth.train(rdd, 0.6)
        self.assertEqual(
            sorted(model1.freqItemsets().collect()), sorted(model2.freqItemsets().collect())
        )


if __name__ == "__main__":
    from pyspark.mllib.tests.test_algorithms import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
