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

from pyspark.ml.linalg import Vectors
from pyspark.ml.classification import LinearSVC
from pyspark.testing.connectutils import ReusedConnectTestCase


class MLConnectCacheTests(ReusedConnectTestCase):
    def test_delete_model(self):
        spark = self.spark
        df = (
            spark.createDataFrame(
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
        svc = LinearSVC(maxIter=1, regParam=1.0)

        model = svc.fit(df)

        cache_info = spark.client._get_ml_cache_info()
        self.assertEqual(len(cache_info), 1)
        self.assertTrue(
            "obj: class org.apache.spark.ml.classification.LinearSVCModel" in cache_info[0],
            cache_info,
        )
        assert model._java_obj._ref_count == 1

        model2 = model.copy()
        cache_info = spark.client._get_ml_cache_info()
        self.assertEqual(len(cache_info), 1)
        assert model._java_obj._ref_count == 2
        assert model2._java_obj._ref_count == 2

        # explicitly delete the model
        del model

        cache_info = spark.client._get_ml_cache_info()
        self.assertEqual(len(cache_info), 1)
        assert model2._java_obj._ref_count == 1

        del model2
        cache_info = spark.client._get_ml_cache_info()
        self.assertEqual(len(cache_info), 0)

    def test_cleanup_ml_cache(self):
        spark = self.spark
        df = (
            spark.createDataFrame(
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

        svc = LinearSVC(maxIter=1, regParam=1.0)
        model1 = svc.fit(df)
        model2 = svc.fit(df)
        model3 = svc.fit(df)
        self.assertEqual(len([model1, model2, model3]), 3)

        cache_info = spark.client._get_ml_cache_info()
        self.assertEqual(len(cache_info), 3)
        self.assertTrue(
            all(
                "obj: class org.apache.spark.ml.classification.LinearSVCModel" in c
                for c in cache_info
            ),
            cache_info,
        )

        # explicitly delete the model1
        del model1

        cache_info = spark.client._get_ml_cache_info()
        self.assertEqual(len(cache_info), 2)

        spark.client._cleanup_ml_cache()

        cache_info = spark.client._get_ml_cache_info()
        self.assertEqual(len(cache_info), 0)


if __name__ == "__main__":
    from pyspark.ml.tests.connect.test_connect_cache import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
