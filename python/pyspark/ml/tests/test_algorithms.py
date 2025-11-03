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
from shutil import rmtree
import tempfile
import unittest

import numpy as np

from pyspark.ml.classification import (
    FMClassifier,
    LogisticRegression,
    MultilayerPerceptronClassifier,
    OneVsRest,
)
from pyspark.ml.clustering import DistributedLDAModel, KMeans, LocalLDAModel, LDA, LDAModel
from pyspark.ml.fpm import FPGrowth
from pyspark.ml.linalg import Vectors, DenseVector
from pyspark.ml.recommendation import ALS
from pyspark.ml.regression import GeneralizedLinearRegression, LinearRegression
from pyspark.sql import Row
from pyspark.testing.mlutils import SparkSessionTestCase


class MultilayerPerceptronClassifierTest(SparkSessionTestCase):
    def test_raw_and_probability_prediction(self):
        data_path = "data/mllib/sample_multiclass_classification_data.txt"
        df = self.spark.read.format("libsvm").load(data_path)

        mlp = MultilayerPerceptronClassifier(
            maxIter=100, layers=[4, 5, 4, 3], blockSize=128, seed=123
        )
        model = mlp.fit(df)
        test = self.sc.parallelize([Row(features=Vectors.dense(0.1, 0.1, 0.25, 0.25))]).toDF()
        result = model.transform(test).head()
        expected_prediction = 2.0
        expected_probability = [0.0, 0.0, 1.0]
        expected_rawPrediction = [-11.6081922998, -8.15827998691, 22.17757045]
        self.assertTrue(result.prediction, expected_prediction)
        self.assertTrue(np.allclose(result.probability, expected_probability, atol=1e-4))
        # Use `assert_allclose` to show the value of `result.rawPrediction` in the assertion error
        # message
        np.testing.assert_allclose(
            result.rawPrediction,
            expected_rawPrediction,
            rtol=0.3,
            # Use the same default value as `np.allclose`
            atol=1e-08,
        )


class OneVsRestTests(SparkSessionTestCase):
    def test_copy(self):
        df = self.spark.createDataFrame(
            [
                (0.0, Vectors.dense(1.0, 0.8)),
                (1.0, Vectors.sparse(2, [], [])),
                (2.0, Vectors.dense(0.5, 0.5)),
            ],
            ["label", "features"],
        )
        lr = LogisticRegression(maxIter=5, regParam=0.01)
        ovr = OneVsRest(classifier=lr)
        ovr1 = ovr.copy({lr.maxIter: 10})
        self.assertEqual(ovr.getClassifier().getMaxIter(), 5)
        self.assertEqual(ovr1.getClassifier().getMaxIter(), 10)
        model = ovr.fit(df)
        model1 = model.copy({model.predictionCol: "indexed"})
        self.assertEqual(model1.getPredictionCol(), "indexed")

    def test_output_columns(self):
        df = self.spark.createDataFrame(
            [
                (0.0, Vectors.dense(1.0, 0.8)),
                (1.0, Vectors.sparse(2, [], [])),
                (2.0, Vectors.dense(0.5, 0.5)),
            ],
            ["label", "features"],
        )
        lr = LogisticRegression(maxIter=5, regParam=0.01)
        ovr = OneVsRest(classifier=lr, parallelism=1)
        model = ovr.fit(df)
        output = model.transform(df)
        self.assertEqual(output.columns, ["label", "features", "rawPrediction", "prediction"])

    def test_raw_prediction_column_is_of_vector_type(self):
        # SPARK-35142: `OneVsRestModel` outputs raw prediction as a string column
        df = self.spark.createDataFrame(
            [
                (0.0, Vectors.dense(1.0, 0.8)),
                (1.0, Vectors.sparse(2, [], [])),
                (2.0, Vectors.dense(0.5, 0.5)),
            ],
            ["label", "features"],
        )
        lr = LogisticRegression(maxIter=5, regParam=0.01)
        ovr = OneVsRest(classifier=lr, parallelism=1)
        model = ovr.fit(df)
        row = model.transform(df).head()
        self.assertIsInstance(row["rawPrediction"], DenseVector)

    def test_parallelism_does_not_change_output(self):
        df = self.spark.createDataFrame(
            [
                (0.0, Vectors.dense(1.0, 0.8)),
                (1.0, Vectors.sparse(2, [], [])),
                (2.0, Vectors.dense(0.5, 0.5)),
            ],
            ["label", "features"],
        )
        ovrPar1 = OneVsRest(classifier=LogisticRegression(maxIter=5, regParam=0.01), parallelism=1)
        modelPar1 = ovrPar1.fit(df)
        ovrPar2 = OneVsRest(classifier=LogisticRegression(maxIter=5, regParam=0.01), parallelism=2)
        modelPar2 = ovrPar2.fit(df)
        for i, model in enumerate(modelPar1.models):
            self.assertTrue(
                np.allclose(
                    model.coefficients.toArray(),
                    modelPar2.models[i].coefficients.toArray(),
                    atol=1e-4,
                )
            )
            self.assertTrue(np.allclose(model.intercept, modelPar2.models[i].intercept, atol=1e-4))

    def test_support_for_weightCol(self):
        df = self.spark.createDataFrame(
            [
                (0.0, Vectors.dense(1.0, 0.8), 1.0),
                (1.0, Vectors.sparse(2, [], []), 1.0),
                (2.0, Vectors.dense(0.5, 0.5), 1.0),
            ],
            ["label", "features", "weight"],
        )
        # classifier inherits hasWeightCol
        lr = LogisticRegression(maxIter=5, regParam=0.01)
        ovr = OneVsRest(classifier=lr, weightCol="weight")
        self.assertIsNotNone(ovr.fit(df))
        # classifier doesn't inherit hasWeightCol
        dt = FMClassifier()
        ovr2 = OneVsRest(classifier=dt, weightCol="weight")
        self.assertIsNotNone(ovr2.fit(df))

    def test_tmp_dfs_cache(self):
        from pyspark.ml.util import _SPARKML_TEMP_DFS_PATH

        with tempfile.TemporaryDirectory(prefix="ml_tmp_dir") as d:
            os.environ[_SPARKML_TEMP_DFS_PATH] = d
            try:
                df = self.spark.createDataFrame(
                    [
                        (0.0, Vectors.dense(1.0, 0.8)),
                        (1.0, Vectors.sparse(2, [], [])),
                        (2.0, Vectors.dense(0.5, 0.5)),
                    ],
                    ["label", "features"],
                )
                lr = LogisticRegression(maxIter=5, regParam=0.01)
                ovr = OneVsRest(classifier=lr, parallelism=1)
                model = ovr.fit(df)
                model.transform(df)
                assert len(os.listdir(d)) == 0
            finally:
                os.environ.pop(_SPARKML_TEMP_DFS_PATH, None)


class KMeansTests(SparkSessionTestCase):
    def test_kmeans_cosine_distance(self):
        data = [
            (Vectors.dense([1.0, 1.0]),),
            (Vectors.dense([10.0, 10.0]),),
            (Vectors.dense([1.0, 0.5]),),
            (Vectors.dense([10.0, 4.4]),),
            (Vectors.dense([-1.0, 1.0]),),
            (Vectors.dense([-100.0, 90.0]),),
        ]
        df = self.spark.createDataFrame(data, ["features"])
        kmeans = KMeans(k=3, seed=1, distanceMeasure="cosine")
        model = kmeans.fit(df)
        result = model.transform(df).collect()
        self.assertTrue(result[0].prediction == result[1].prediction)
        self.assertTrue(result[2].prediction == result[3].prediction)
        self.assertTrue(result[4].prediction == result[5].prediction)


class LDATest(SparkSessionTestCase):
    def _compare(self, m1, m2):
        """
        Temp method for comparing instances.
        TODO: Replace with generic implementation once SPARK-14706 is merged.
        """
        self.assertEqual(m1.uid, m2.uid)
        self.assertEqual(type(m1), type(m2))
        self.assertEqual(len(m1.params), len(m2.params))
        for p in m1.params:
            if m1.isDefined(p):
                self.assertEqual(m1.getOrDefault(p), m2.getOrDefault(p))
                self.assertEqual(p.parent, m2.getParam(p.name).parent)
        if isinstance(m1, LDAModel):
            self.assertEqual(m1.vocabSize(), m2.vocabSize())
            self.assertEqual(m1.topicsMatrix(), m2.topicsMatrix())

    def test_persistence(self):
        # Test save/load for LDA, LocalLDAModel, DistributedLDAModel.
        df = self.spark.createDataFrame(
            [
                [1, Vectors.dense([0.0, 1.0])],
                [2, Vectors.sparse(2, {0: 1.0})],
            ],
            ["id", "features"],
        )
        # Fit model
        lda = LDA(k=2, seed=1, optimizer="em")
        distributedModel = lda.fit(df)
        self.assertTrue(distributedModel.isDistributed())
        localModel = distributedModel.toLocal()
        self.assertFalse(localModel.isDistributed())
        # Define paths
        path = tempfile.mkdtemp()
        lda_path = path + "/lda"
        dist_model_path = path + "/distLDAModel"
        local_model_path = path + "/localLDAModel"
        # Test LDA
        lda.save(lda_path)
        lda2 = LDA.load(lda_path)
        self._compare(lda, lda2)
        # Test DistributedLDAModel
        distributedModel.save(dist_model_path)
        distributedModel2 = DistributedLDAModel.load(dist_model_path)
        self._compare(distributedModel, distributedModel2)
        # Test LocalLDAModel
        localModel.save(local_model_path)
        localModel2 = LocalLDAModel.load(local_model_path)
        self._compare(localModel, localModel2)
        # Clean up
        try:
            rmtree(path)
        except OSError:
            pass


class FPGrowthTests(SparkSessionTestCase):
    def setUp(self):
        super(FPGrowthTests, self).setUp()
        self.data = self.spark.createDataFrame(
            [([1, 2],), ([1, 2],), ([1, 2, 3],), ([1, 3],)], ["items"]
        )

    def test_association_rules(self):
        fp = FPGrowth()
        fpm = fp.fit(self.data)

        expected_association_rules = self.spark.createDataFrame(
            [([3], [1], 1.0, 1.0, 0.5), ([2], [1], 1.0, 1.0, 0.75)],
            ["antecedent", "consequent", "confidence", "lift", "support"],
        )
        actual_association_rules = fpm.associationRules

        self.assertEqual(actual_association_rules.subtract(expected_association_rules).count(), 0)
        self.assertEqual(expected_association_rules.subtract(actual_association_rules).count(), 0)

    def test_freq_itemsets(self):
        fp = FPGrowth()
        fpm = fp.fit(self.data)

        expected_freq_itemsets = self.spark.createDataFrame(
            [([1], 4), ([2], 3), ([2, 1], 3), ([3], 2), ([3, 1], 2)], ["items", "freq"]
        )
        actual_freq_itemsets = fpm.freqItemsets

        self.assertEqual(actual_freq_itemsets.subtract(expected_freq_itemsets).count(), 0)
        self.assertEqual(expected_freq_itemsets.subtract(actual_freq_itemsets).count(), 0)

    def tearDown(self):
        del self.data


class ALSTest(SparkSessionTestCase):
    def test_storage_levels(self):
        df = self.spark.createDataFrame(
            [(0, 0, 4.0), (0, 1, 2.0), (1, 1, 3.0), (1, 2, 4.0), (2, 1, 1.0), (2, 2, 5.0)],
            ["user", "item", "rating"],
        )
        als = ALS().setMaxIter(1).setRank(1)
        # test default params
        als.fit(df)
        self.assertEqual(als.getIntermediateStorageLevel(), "MEMORY_AND_DISK")
        self.assertEqual(als._java_obj.getIntermediateStorageLevel(), "MEMORY_AND_DISK")
        self.assertEqual(als.getFinalStorageLevel(), "MEMORY_AND_DISK")
        self.assertEqual(als._java_obj.getFinalStorageLevel(), "MEMORY_AND_DISK")
        # test non-default params
        als.setIntermediateStorageLevel("MEMORY_ONLY_2")
        als.setFinalStorageLevel("DISK_ONLY")
        als.fit(df)
        self.assertEqual(als.getIntermediateStorageLevel(), "MEMORY_ONLY_2")
        self.assertEqual(als._java_obj.getIntermediateStorageLevel(), "MEMORY_ONLY_2")
        self.assertEqual(als.getFinalStorageLevel(), "DISK_ONLY")
        self.assertEqual(als._java_obj.getFinalStorageLevel(), "DISK_ONLY")


class GeneralizedLinearRegressionTest(SparkSessionTestCase):
    def test_tweedie_distribution(self):
        df = self.spark.createDataFrame(
            [
                (1.0, Vectors.dense(0.0, 0.0)),
                (1.0, Vectors.dense(1.0, 2.0)),
                (2.0, Vectors.dense(0.0, 0.0)),
                (2.0, Vectors.dense(1.0, 1.0)),
            ],
            ["label", "features"],
        )

        glr = GeneralizedLinearRegression(family="tweedie", variancePower=1.6)
        model = glr.fit(df)
        self.assertTrue(np.allclose(model.coefficients.toArray(), [-0.4645, 0.3402], atol=1e-4))
        self.assertTrue(np.isclose(model.intercept, 0.7841, atol=1e-4))

        model2 = glr.setLinkPower(-1.0).fit(df)
        self.assertTrue(np.allclose(model2.coefficients.toArray(), [-0.6667, 0.5], atol=1e-4))
        self.assertTrue(np.isclose(model2.intercept, 0.6667, atol=1e-4))

    def test_offset(self):
        df = self.spark.createDataFrame(
            [
                (0.2, 1.0, 2.0, Vectors.dense(0.0, 5.0)),
                (0.5, 2.1, 0.5, Vectors.dense(1.0, 2.0)),
                (0.9, 0.4, 1.0, Vectors.dense(2.0, 1.0)),
                (0.7, 0.7, 0.0, Vectors.dense(3.0, 3.0)),
            ],
            ["label", "weight", "offset", "features"],
        )

        glr = GeneralizedLinearRegression(family="poisson", weightCol="weight", offsetCol="offset")
        model = glr.fit(df)
        self.assertTrue(
            np.allclose(model.coefficients.toArray(), [0.664647, -0.3192581], atol=1e-4)
        )
        self.assertTrue(np.isclose(model.intercept, -1.561613, atol=1e-4))


class LinearRegressionTest(SparkSessionTestCase):
    def test_linear_regression_with_huber_loss(self):
        data_path = "data/mllib/sample_linear_regression_data.txt"
        df = self.spark.read.format("libsvm").load(data_path)

        lir = LinearRegression(loss="huber", epsilon=2.0)
        model = lir.fit(df)

        expectedCoefficients = [
            0.136,
            0.7648,
            -0.7761,
            2.4236,
            0.537,
            1.2612,
            -0.333,
            -0.5694,
            -0.6311,
            0.6053,
        ]
        expectedIntercept = 0.1607
        expectedScale = 9.758

        self.assertTrue(np.allclose(model.coefficients.toArray(), expectedCoefficients, atol=1e-3))
        self.assertTrue(np.isclose(model.intercept, expectedIntercept, atol=1e-3))
        self.assertTrue(np.isclose(model.scale, expectedScale, atol=1e-3))


if __name__ == "__main__":
    from pyspark.ml.tests.test_algorithms import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
