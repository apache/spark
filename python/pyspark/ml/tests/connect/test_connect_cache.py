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
import unittest

import numpy as np

from pyspark.errors import AnalysisException, PySparkTypeError, PySparkException
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.ml.linalg import Vectors, Matrices, DenseMatrix
from pyspark.ml.classification import LogisticRegression


class ConnectCacheTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = (
            SparkSession.builder.remote(os.environ.get("SPARK_CONNECT_TESTING_REMOTE", "local[2]"))
            .config("spark.connect.session.mlCache.singleItemSize", "2MB")
            .config("spark.connect.session.mlCache.totalItemSize", "10MB")
            .getOrCreate()
        )

    def test_assert_remote_mode(self):
        from pyspark.sql import is_remote

        self.assertTrue(is_remote())

    def test_ml_cache_config(self):
        spark = self.spark
        self.assertEqual(
            spark.conf.get("spark.connect.session.mlCache.singleItemSize"),
            "2MB",
        )
        self.assertEqual(
            spark.conf.get("spark.connect.session.mlCache.totalItemSize"),
            "10MB",
        )

    def test_large_single_model(self):
        spark = self.spark
        df = spark.createDataFrame(
            [
                (1.0, 1.0, Vectors.sparse(1000000000, [(1, 1.0), (3, 5.5)])),
                (0.0, 2.0, Vectors.sparse(1000000000, [(1, 1.0), (3, 5.5)])),
            ],
            ["label", "weight", "features"],
        )

        lor = LogisticRegression(
            maxIter=0,
            regParam=0.0,
            weightCol="weight",
        )

        with self.assertRaisesRegex(PySparkException, "CONNECT_ML.CACHE_ITEM_EXCEEDED") as e:
            lor.fit(df)

    def test_model_eviction(self):
        spark = self.spark

        rng = np.random.default_rng(seed=1)
        df = spark.createDataFrame(
            [
                (1.0, 1.0, Vectors.dense(rng.random(100000))),
                (0.0, 1.0, Vectors.dense(rng.random(100000))),
            ],
            ["label", "weight", "features"],
        )

        lor = LogisticRegression(
            maxIter=1,
            regParam=0.0,
            weightCol="weight",
        )
        first_model = lor.fit(df)

        # The model coefficients should be a dense matrix
        # 100000 double values, 800KB, plus other overhead
        self.assertIsInstance(first_model.coefficientMatrix, DenseMatrix)
        self.assertEqual(first_model.coefficientMatrix.toArray().shape, (1, 100000))

        # The first model works
        self.assertEqual(first_model.transform(df).count(), 2)

        # cache more and more models
        models = []
        for _ in range(20):
            model = lor.fit(df)
            self.assertIsInstance(model.coefficientMatrix, DenseMatrix)
            models.append(model)

        # The first model should have been evicted
        with self.assertRaisesRegex(PySparkException, "CONNECT_ML.CACHE_INVALID") as e:
            first_model.transform(df).count()


if __name__ == "__main__":
    from pyspark.ml.tests.connect.test_connect_cache import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
