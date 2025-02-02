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

from pyspark.ml.linalg import Vectors, SparseVector
from pyspark.sql import SparkSession
from pyspark.ml.clustering import (
    KMeans,
    KMeansModel,
    KMeansSummary,
    BisectingKMeans,
    BisectingKMeansModel,
    BisectingKMeansSummary,
    GaussianMixture,
    GaussianMixtureModel,
    GaussianMixtureSummary,
    LDA,
    LDAModel,
    LocalLDAModel,
    DistributedLDAModel,
)


class ClusteringTestsMixin:
    def test_kmeans(self):
        df = (
            self.spark.createDataFrame(
                [
                    (1, 1.0, Vectors.dense([-0.1, -0.05])),
                    (2, 2.0, Vectors.dense([-0.01, -0.1])),
                    (3, 3.0, Vectors.dense([0.9, 0.8])),
                    (4, 1.0, Vectors.dense([0.75, 0.935])),
                    (5, 1.0, Vectors.dense([-0.83, -0.68])),
                    (6, 1.0, Vectors.dense([-0.91, -0.76])),
                ],
                ["index", "weight", "features"],
            )
            .coalesce(1)
            .sortWithinPartitions("index")
            .select("weight", "features")
        )

        km = KMeans(
            k=2,
            maxIter=2,
            weightCol="weight",
        )
        self.assertEqual(km.getK(), 2)
        self.assertEqual(km.getMaxIter(), 2)
        self.assertEqual(km.getWeightCol(), "weight")

        model = km.fit(df)
        self.assertEqual(km.uid, model.uid)

        centers = model.clusterCenters()
        self.assertEqual(len(centers), 2)
        self.assertTrue(np.allclose(centers[0], [-0.372, -0.338], atol=1e-3), centers[0])
        self.assertTrue(np.allclose(centers[1], [0.8625, 0.83375], atol=1e-3), centers[1])

        # TODO: support KMeansModel.numFeatures in Python
        # self.assertEqual(model.numFeatures, 2)

        output = model.transform(df)
        expected_cols = ["weight", "features", "prediction"]
        self.assertEqual(output.columns, expected_cols)
        self.assertEqual(output.count(), 6)

        self.assertTrue(np.allclose(model.predict(Vectors.dense(0.0, 5.0)), 1, atol=1e-4))

        # Model summary
        self.assertTrue(model.hasSummary)
        summary = model.summary
        self.assertTrue(isinstance(summary, KMeansSummary))
        self.assertEqual(summary.k, 2)
        self.assertEqual(summary.numIter, 2)
        self.assertEqual(summary.clusterSizes, [4, 2])
        self.assertTrue(np.allclose(summary.trainingCost, 1.35710375, atol=1e-4))

        self.assertEqual(summary.featuresCol, "features")
        self.assertEqual(summary.predictionCol, "prediction")

        self.assertEqual(summary.cluster.columns, ["prediction"])
        self.assertEqual(summary.cluster.count(), 6)

        self.assertEqual(summary.predictions.columns, expected_cols)
        self.assertEqual(summary.predictions.count(), 6)

        # save & load
        with tempfile.TemporaryDirectory(prefix="kmeans_model") as d:
            km.write().overwrite().save(d)
            km2 = KMeans.load(d)
            self.assertEqual(str(km), str(km2))

            model.write().overwrite().save(d)
            model2 = KMeansModel.load(d)
            self.assertEqual(str(model), str(model2))

    def test_bisecting_kmeans(self):
        df = (
            self.spark.createDataFrame(
                [
                    (1, 1.0, Vectors.dense([-0.1, -0.05])),
                    (2, 2.0, Vectors.dense([-0.01, -0.1])),
                    (3, 3.0, Vectors.dense([0.9, 0.8])),
                    (4, 1.0, Vectors.dense([0.75, 0.935])),
                    (5, 1.0, Vectors.dense([-0.83, -0.68])),
                    (6, 1.0, Vectors.dense([-0.91, -0.76])),
                ],
                ["index", "weight", "features"],
            )
            .coalesce(1)
            .sortWithinPartitions("index")
            .select("weight", "features")
        )

        bkm = BisectingKMeans(
            k=2,
            maxIter=2,
            minDivisibleClusterSize=1.0,
            weightCol="weight",
        )
        self.assertEqual(bkm.getK(), 2)
        self.assertEqual(bkm.getMaxIter(), 2)
        self.assertEqual(bkm.getMinDivisibleClusterSize(), 1.0)
        self.assertEqual(bkm.getWeightCol(), "weight")

        model = bkm.fit(df)
        self.assertEqual(bkm.uid, model.uid)

        centers = model.clusterCenters()
        self.assertEqual(len(centers), 2)
        self.assertTrue(np.allclose(centers[0], [-0.372, -0.338], atol=1e-3), centers[0])
        self.assertTrue(np.allclose(centers[1], [0.8625, 0.83375], atol=1e-3), centers[1])

        # TODO: support KMeansModel.numFeatures in Python
        # self.assertEqual(model.numFeatures, 2)

        output = model.transform(df)
        expected_cols = ["weight", "features", "prediction"]
        self.assertEqual(output.columns, expected_cols)
        self.assertEqual(output.count(), 6)

        self.assertTrue(np.allclose(model.predict(Vectors.dense(0.0, 5.0)), 1, atol=1e-4))

        # BisectingKMeans-specific method: computeCost
        self.assertTrue(np.allclose(model.computeCost(df), 1.164325125, atol=1e-4))

        # Model summary
        self.assertTrue(model.hasSummary)
        summary = model.summary
        self.assertTrue(isinstance(summary, BisectingKMeansSummary))
        self.assertEqual(summary.k, 2)
        self.assertEqual(summary.numIter, 2)
        self.assertEqual(summary.clusterSizes, [4, 2])
        self.assertTrue(np.allclose(summary.trainingCost, 1.3571037499999998, atol=1e-4))

        self.assertEqual(summary.featuresCol, "features")
        self.assertEqual(summary.predictionCol, "prediction")

        self.assertEqual(summary.cluster.columns, ["prediction"])
        self.assertEqual(summary.cluster.count(), 6)

        self.assertEqual(summary.predictions.columns, expected_cols)
        self.assertEqual(summary.predictions.count(), 6)

        # save & load
        with tempfile.TemporaryDirectory(prefix="bisecting_kmeans") as d:
            bkm.write().overwrite().save(d)
            bkm2 = BisectingKMeans.load(d)
            self.assertEqual(str(bkm), str(bkm2))

            model.write().overwrite().save(d)
            model2 = BisectingKMeansModel.load(d)
            self.assertEqual(str(model), str(model2))

    def test_gaussian_mixture(self):
        df = (
            self.spark.createDataFrame(
                [
                    (1, 1.0, Vectors.dense([-0.1, -0.05])),
                    (2, 2.0, Vectors.dense([-0.01, -0.1])),
                    (3, 3.0, Vectors.dense([0.9, 0.8])),
                    (4, 1.0, Vectors.dense([0.75, 0.935])),
                    (5, 1.0, Vectors.dense([-0.83, -0.68])),
                    (6, 1.0, Vectors.dense([-0.91, -0.76])),
                ],
                ["index", "weight", "features"],
            )
            .coalesce(1)
            .sortWithinPartitions("index")
            .select("weight", "features")
        )

        gmm = GaussianMixture(
            k=2,
            maxIter=2,
            weightCol="weight",
            seed=1,
        )
        self.assertEqual(gmm.getK(), 2)
        self.assertEqual(gmm.getMaxIter(), 2)
        self.assertEqual(gmm.getWeightCol(), "weight")
        self.assertEqual(gmm.getSeed(), 1)

        model = gmm.fit(df)
        self.assertEqual(gmm.uid, model.uid)
        # TODO: support GMM.numFeatures in Python
        # self.assertEqual(model.numFeatures, 2)
        self.assertEqual(len(model.weights), 2)
        self.assertTrue(
            np.allclose(model.weights, [0.541014115744985, 0.4589858842550149], atol=1e-4),
            model.weights,
        )
        # TODO: support GMM.gaussians on connect
        # self.assertEqual(model.gaussians, xxx)
        self.assertEqual(model.gaussiansDF.columns, ["mean", "cov"])
        self.assertEqual(model.gaussiansDF.count(), 2)

        vec = Vectors.dense(0.0, 5.0)
        pred = model.predict(vec)
        self.assertTrue(np.allclose(pred, 0, atol=1e-4), pred)
        pred = model.predictProbability(vec)
        self.assertTrue(np.allclose(pred.toArray(), [0.5, 0.5], atol=1e-4), pred)

        output = model.transform(df)
        expected_cols = ["weight", "features", "probability", "prediction"]
        self.assertEqual(output.columns, expected_cols)
        self.assertEqual(output.count(), 6)

        # Model summary
        self.assertTrue(model.hasSummary)
        summary = model.summary
        self.assertTrue(isinstance(summary, GaussianMixtureSummary))
        self.assertEqual(summary.k, 2)
        self.assertEqual(summary.numIter, 2)
        self.assertEqual(len(summary.clusterSizes), 2)
        self.assertEqual(summary.clusterSizes, [3, 3])
        ll = summary.logLikelihood
        self.assertTrue(ll < 0, ll)
        self.assertTrue(np.allclose(ll, -1.311264553744033, atol=1e-4), ll)

        self.assertEqual(summary.featuresCol, "features")
        self.assertEqual(summary.predictionCol, "prediction")
        self.assertEqual(summary.probabilityCol, "probability")

        self.assertEqual(summary.cluster.columns, ["prediction"])
        self.assertEqual(summary.cluster.count(), 6)

        self.assertEqual(summary.predictions.columns, expected_cols)
        self.assertEqual(summary.predictions.count(), 6)

        self.assertEqual(summary.probability.columns, ["probability"])
        self.assertEqual(summary.predictions.count(), 6)

        # save & load
        with tempfile.TemporaryDirectory(prefix="gaussian_mixture") as d:
            gmm.write().overwrite().save(d)
            gmm2 = GaussianMixture.load(d)
            self.assertEqual(str(gmm), str(gmm2))

            model.write().overwrite().save(d)
            model2 = GaussianMixtureModel.load(d)
            self.assertEqual(str(model), str(model2))

    def test_local_lda(self):
        spark = self.spark
        df = (
            spark.createDataFrame(
                [
                    [1, Vectors.dense([0.0, 1.0])],
                    [2, SparseVector(2, {0: 1.0})],
                ],
                ["id", "features"],
            )
            .coalesce(1)
            .sortWithinPartitions("id")
        )

        lda = LDA(k=2, optimizer="online", seed=1)
        lda.setMaxIter(1)
        self.assertEqual(lda.getK(), 2)
        self.assertEqual(lda.getOptimizer(), "online")
        self.assertEqual(lda.getMaxIter(), 1)
        self.assertEqual(lda.getSeed(), 1)

        model = lda.fit(df)
        self.assertEqual(lda.uid, model.uid)
        self.assertIsInstance(model, LDAModel)
        self.assertIsInstance(model, LocalLDAModel)
        self.assertNotIsInstance(model, DistributedLDAModel)
        self.assertFalse(model.isDistributed())

        dc = model.estimatedDocConcentration()
        self.assertTrue(np.allclose(dc.toArray(), [0.5, 0.5], atol=1e-4), dc)
        topics = model.topicsMatrix()
        self.assertTrue(
            np.allclose(
                topics.toArray(), [[1.20296728, 1.15740442], [0.99357675, 1.02993164]], atol=1e-4
            ),
            topics,
        )

        ll = model.logLikelihood(df)
        self.assertTrue(np.allclose(ll, -3.2125122434040088, atol=1e-4), ll)
        lp = model.logPerplexity(df)
        self.assertTrue(np.allclose(lp, 1.6062561217020044, atol=1e-4), lp)
        dt = model.describeTopics()
        self.assertEqual(dt.columns, ["topic", "termIndices", "termWeights"])
        self.assertEqual(dt.count(), 2)

        # LocalLDAModel specific methods
        self.assertEqual(model.vocabSize(), 2)

        output = model.transform(df)
        expected_cols = ["id", "features", "topicDistribution"]
        self.assertEqual(output.columns, expected_cols)
        self.assertEqual(output.count(), 2)

        # save & load
        with tempfile.TemporaryDirectory(prefix="local_lda") as d:
            lda.write().overwrite().save(d)
            lda2 = LDA.load(d)
            self.assertEqual(str(lda), str(lda2))

            model.write().overwrite().save(d)
            model2 = LocalLDAModel.load(d)
            self.assertEqual(str(model), str(model2))

    def test_distributed_lda(self):
        spark = self.spark
        df = (
            spark.createDataFrame(
                [
                    [1, Vectors.dense([0.0, 1.0])],
                    [2, SparseVector(2, {0: 1.0})],
                ],
                ["id", "features"],
            )
            .coalesce(1)
            .sortWithinPartitions("id")
        )

        lda = LDA(k=2, optimizer="em", seed=1)
        lda.setMaxIter(1)

        self.assertEqual(lda.getK(), 2)
        self.assertEqual(lda.getOptimizer(), "em")
        self.assertEqual(lda.getMaxIter(), 1)
        self.assertEqual(lda.getSeed(), 1)

        model = lda.fit(df)
        self.assertEqual(lda.uid, model.uid)
        self.assertIsInstance(model, LDAModel)
        self.assertNotIsInstance(model, LocalLDAModel)
        self.assertIsInstance(model, DistributedLDAModel)
        self.assertTrue(model.isDistributed())

        dc = model.estimatedDocConcentration()
        self.assertTrue(np.allclose(dc.toArray(), [26.0, 26.0], atol=1e-4), dc)
        topics = model.topicsMatrix()
        self.assertTrue(
            np.allclose(
                topics.toArray(), [[0.39149926, 0.60850074], [0.60991237, 0.39008763]], atol=1e-4
            ),
            topics,
        )

        ll = model.logLikelihood(df)
        self.assertTrue(np.allclose(ll, -3.719138517085772, atol=1e-4), ll)
        lp = model.logPerplexity(df)
        self.assertTrue(np.allclose(lp, 1.859569258542886, atol=1e-4), lp)

        dt = model.describeTopics()
        self.assertEqual(dt.columns, ["topic", "termIndices", "termWeights"])
        self.assertEqual(dt.count(), 2)

        # DistributedLDAModel specific methods
        ll = model.trainingLogLikelihood()
        self.assertTrue(np.allclose(ll, -1.3847360462201639, atol=1e-4), ll)
        lp = model.logPrior()
        self.assertTrue(np.allclose(lp, -69.59963186898915, atol=1e-4), lp)
        model.getCheckpointFiles()

        output = model.transform(df)
        expected_cols = ["id", "features", "topicDistribution"]
        self.assertEqual(output.columns, expected_cols)
        self.assertEqual(output.count(), 2)

        # Test toLocal()
        localModel = model.toLocal()
        self.assertIsInstance(localModel, LDAModel)
        self.assertIsInstance(localModel, LocalLDAModel)
        self.assertNotIsInstance(localModel, DistributedLDAModel)
        self.assertFalse(localModel.isDistributed())
        output = localModel.transform(df)
        expected_cols = ["id", "features", "topicDistribution"]
        self.assertEqual(output.columns, expected_cols)
        self.assertEqual(output.count(), 2)

        # save & load
        with tempfile.TemporaryDirectory(prefix="distributed_lda") as d:
            lda.write().overwrite().save(d)
            lda2 = LDA.load(d)
            self.assertEqual(str(lda), str(lda2))

            model.write().overwrite().save(d)
            model2 = DistributedLDAModel.load(d)
            self.assertEqual(str(model), str(model2))


class ClusteringTests(ClusteringTestsMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.spark = SparkSession.builder.master("local[4]").getOrCreate()

    def tearDown(self) -> None:
        self.spark.stop()


if __name__ == "__main__":
    from pyspark.ml.tests.test_clustering import *  # noqa: F401,F403

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
