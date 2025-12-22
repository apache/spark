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

import json
import unittest

import numpy as np

from pyspark.sql import functions as sf
from pyspark.ml.linalg import Vectors
from pyspark.ml.classification import (
    LinearSVC,
    LinearSVCSummary,
    LinearSVCTrainingSummary,
)
from pyspark.ml.regression import (
    LinearRegression,
    LinearRegressionSummary,
    LinearRegressionTrainingSummary,
)
from pyspark.ml.clustering import (
    LDA,
    LDAModel,
    LocalLDAModel,
    DistributedLDAModel,
)
from pyspark.ml.fpm import (
    FPGrowth,
    FPGrowthModel,
)
from pyspark.testing.connectutils import ReusedConnectTestCase


class ModelOffloadingTests(ReusedConnectTestCase):
    def test_linear_svc_offloading(self):
        # force clean up the ml cache
        self.spark.client._cleanup_ml_cache()

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
        vec = Vectors.dense(0.0, 5.0)

        svc = LinearSVC(maxIter=1, regParam=1.0)
        self.assertEqual(svc.getMaxIter(), 1)
        self.assertEqual(svc.getRegParam(), 1.0)

        model = svc.fit(df)

        def check_model(m):
            self.assertEqual(svc.uid, m.uid)
            self.assertEqual(m.numClasses, 2)
            self.assertEqual(m.predict(vec), 1.0)

            self.assertTrue(m.hasSummary)
            summary = m.summary()

            self.assertIsInstance(summary, LinearSVCSummary)
            self.assertIsInstance(summary, LinearSVCTrainingSummary)
            self.assertEqual(summary.labels, [0.0, 1.0])

        # model is cached!
        # '{"id":"xxx","class":"org.apache.spark.ml.classification.LinearSVCModel","size":xxx}'
        cached = self.spark.client._get_ml_cache_info()
        self.assertEqual(len(cached), 1, cached)
        self.assertEqual(
            json.loads(cached[0])["class"],
            "org.apache.spark.ml.classification.LinearSVCModel",
            cached,
        )

        check_model(model)

        # model is offloaded!
        self.spark.client._delete_ml_cache([model._java_obj._ref_id], evict_only=True)
        cached = self.spark.client._get_ml_cache_info()
        self.assertEqual(len(cached), 0, cached)

        check_model(model)

    def test_linear_regression_offloading(self):
        # force clean up the ml cache
        self.spark.client._cleanup_ml_cache()

        df = (
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
        vec = Vectors.dense(0.0, 5.0)

        lr = LinearRegression(
            regParam=0.0,
            maxIter=2,
            solver="normal",
            weightCol="weight",
        )
        self.assertEqual(lr.getRegParam(), 0)
        self.assertEqual(lr.getMaxIter(), 2)

        model = lr.fit(df)

        def check_model(m):
            self.assertEqual(lr.uid, m.uid)
            self.assertEqual(m.numFeatures, 2)
            self.assertTrue(np.allclose(m.predict(vec), 0.21249999999999963, atol=1e-4))

            summary = m.summary
            self.assertTrue(isinstance(summary, LinearRegressionSummary))
            self.assertTrue(isinstance(summary, LinearRegressionTrainingSummary))
            self.assertEqual(summary.predictions.count(), 4)

        # model is cached!
        # '{"id":"xxx","class":"org.apache.spark.ml.regression.LinearRegressionModel","size":xxx}'
        cached = self.spark.client._get_ml_cache_info()
        self.assertEqual(len(cached), 1, cached)
        self.assertEqual(
            json.loads(cached[0])["class"],
            "org.apache.spark.ml.regression.LinearRegressionModel",
            cached,
        )

        check_model(model)

        # model is offloaded!
        self.spark.client._delete_ml_cache([model._java_obj._ref_id], evict_only=True)
        cached = self.spark.client._get_ml_cache_info()
        self.assertEqual(len(cached), 0, cached)

        check_model(model)

    def test_lda_offloading(self):
        # force clean up the ml cache
        self.spark.client._cleanup_ml_cache()

        df = (
            self.spark.createDataFrame(
                [
                    [1, Vectors.dense([0.0, 1.0])],
                    [2, Vectors.sparse(2, {0: 1.0})],
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

        def check_model(m):
            self.assertEqual(lda.uid, m.uid)
            self.assertIsInstance(m, LDAModel)
            self.assertNotIsInstance(m, LocalLDAModel)
            self.assertIsInstance(m, DistributedLDAModel)
            self.assertTrue(m.isDistributed())
            self.assertEqual(m.vocabSize(), 2)

            output = m.transform(df)
            expected_cols = ["id", "features", "topicDistribution"]
            self.assertEqual(output.columns, expected_cols)
            self.assertEqual(output.count(), 2)

        # model is cached!
        # '{"id":"xxx","class":"org.apache.spark.ml.clustering.DistributedLDAModel","size":xxx}'
        cached = self.spark.client._get_ml_cache_info()
        self.assertEqual(len(cached), 1, cached)
        self.assertEqual(
            json.loads(cached[0])["class"],
            "org.apache.spark.ml.clustering.DistributedLDAModel",
            cached,
        )

        check_model(model)

        # both model and local_model are is cached!
        local_model = model.toLocal()
        # '{"id":"xxx","class":"org.apache.spark.ml.clustering.LocalLDAModel","size":xxx}'
        # '{"id":"xxx","class":"org.apache.spark.ml.clustering.DistributedLDAModel","size":xxx}'
        cached = self.spark.client._get_ml_cache_info()
        self.assertEqual(len(cached), 2, cached)
        self.assertEqual(
            sorted([json.loads(c)["class"] for c in cached]),
            [
                "org.apache.spark.ml.clustering.DistributedLDAModel",
                "org.apache.spark.ml.clustering.LocalLDAModel",
            ],
            cached,
        )

        def check_local_model(m):
            self.assertIsInstance(m, LDAModel)
            self.assertIsInstance(m, LocalLDAModel)
            self.assertNotIsInstance(m, DistributedLDAModel)
            self.assertFalse(m.isDistributed())
            self.assertEqual(m.vocabSize(), 2)

            output = m.transform(df)
            expected_cols = ["id", "features", "topicDistribution"]
            self.assertEqual(output.columns, expected_cols)
            self.assertEqual(output.count(), 2)

        check_local_model(local_model)

        # both model and local_model are offloaded!
        self.spark.client._delete_ml_cache([model._java_obj._ref_id], evict_only=True)
        self.spark.client._delete_ml_cache([local_model._java_obj._ref_id], evict_only=True)
        cached = self.spark.client._get_ml_cache_info()
        self.assertEqual(len(cached), 0, cached)

        check_model(model)
        check_local_model(local_model)

    def test_fp_growth_offloading(self):
        # force clean up the ml cache
        self.spark.client._cleanup_ml_cache()

        df = self.spark.createDataFrame(
            [
                ["r z h k p"],
                ["z y x w v u t s"],
                ["s x o n r"],
                ["x z y m t s q e"],
                ["z"],
                ["x z y r q t p"],
            ],
            ["items"],
        ).select(sf.split("items", " ").alias("items"))

        fp = FPGrowth(minSupport=0.2, minConfidence=0.7)
        fp.setNumPartitions(1)

        model = fp.fit(df)

        def check_model(m):
            self.assertIsInstance(m, FPGrowthModel)
            self.assertEqual(fp.uid, m.uid)
            self.assertEqual(m.freqItemsets.columns, ["items", "freq"])
            self.assertEqual(m.freqItemsets.count(), 54)

            self.assertEqual(
                m.associationRules.columns,
                ["antecedent", "consequent", "confidence", "lift", "support"],
            )
            self.assertEqual(m.associationRules.count(), 89)

            output = m.transform(df)
            self.assertEqual(output.columns, ["items", "prediction"])
            self.assertEqual(output.count(), 6)

        # model is cached!
        # '{"id":"xxx","class":"org.apache.spark.ml.fpm.FPGrowthModel","size":xxx}'
        cached = self.spark.client._get_ml_cache_info()
        self.assertEqual(len(cached), 1, cached)
        self.assertEqual(
            json.loads(cached[0])["class"],
            "org.apache.spark.ml.fpm.FPGrowthModel",
            cached,
        )

        check_model(model)

        # model is offloaded!
        self.spark.client._delete_ml_cache([model._java_obj._ref_id], evict_only=True)
        cached = self.spark.client._get_ml_cache_info()
        self.assertEqual(len(cached), 0, cached)

        check_model(model)


if __name__ == "__main__":
    from pyspark.ml.tests.connect.test_connect_model_offloading import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
