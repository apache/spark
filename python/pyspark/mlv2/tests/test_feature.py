# -*- coding: utf-8 -*-
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
import numpy as np

from pyspark.ml.linalg import DenseVector, SparseVector, Vectors
from pyspark.sql import Row
from pyspark.testing.utils import QuietTest
from pyspark.testing.mlutils import check_params, SparkSessionTestCase

from pyspark.mlv2.feature import MaxAbsScaler, StandardScaler


class FeatureTests(SparkSessionTestCase):

    @classmethod
    def conf(cls):
        """
        Override this in subclasses to supply a more specific conf
        """
        base_conf = super().conf()
        base_conf.set("spark.task.maxFailures", "1")
        return base_conf

    def test_max_abs_scaler(self):
        df1 = self.spark.createDataFrame([
            (Vectors.dense([2.0, 3.5, 1.5]),),
            (Vectors.dense([-3.0, -0.5, -2.5]),),
        ], schema=['features'])

        scaler = MaxAbsScaler(inputCol='features', outputCol='scaled_features')
        model = scaler.fit(df1)
        result = model.transform(df1).toPandas()

        expected_result = [
            [2.0 / 3, 1.0, 0.6],
            [-1.0, -1.0 / 7, -1.0]
        ]

        np.testing.assert_allclose(list(result.scaled_features), expected_result)

        local_df1 = df1.toPandas()
        local_fit_model = scaler.fit(local_df1)
        local_transform_result = local_fit_model.transform(local_df1)

        np.testing.assert_allclose(
            list(local_transform_result.scaled_features), expected_result
        )

    def test_standard_scaler(self):
        df1 = self.spark.createDataFrame([
            (Vectors.dense([2.0, 3.5, 1.5]),),
            (Vectors.dense([-3.0, -0.5, -2.5]),),
            (Vectors.dense([1.0, -1.5, 0.5]),),
        ], schema=['features'])

        scaler = StandardScaler(inputCol='features', outputCol='scaled_features')
        model = scaler.fit(df1)
        result = model.transform(df1).toPandas()

        expected_result = [
            [0.7559289460184544, 1.1338934190276817, 0.8006407690254358],
            [-1.1338934190276817, -0.3779644730092272, -1.1208970766356101],
            [0.3779644730092272, -0.7559289460184544, 0.32025630761017426]
        ]

        np.testing.assert_allclose(list(result.scaled_features), expected_result)

        local_df1 = df1.toPandas()
        local_fit_model = scaler.fit(local_df1)
        local_transform_result = local_fit_model.transform(local_df1)

        np.testing.assert_allclose(
            list(local_transform_result.scaled_features), expected_result
        )


if __name__ == "__main__":
    from pyspark.mlv2.tests.test_feature import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
