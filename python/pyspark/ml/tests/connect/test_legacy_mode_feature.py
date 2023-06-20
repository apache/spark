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

import os
import pickle
import numpy as np
import tempfile
import unittest

from pyspark.ml.connect.feature import (
    MaxAbsScaler,
    MaxAbsScalerModel,
    StandardScaler,
    StandardScalerModel,
)
from pyspark.sql import SparkSession


class FeatureTestsMixin:
    def test_max_abs_scaler(self):
        df1 = self.spark.createDataFrame(
            [
                ([2.0, 3.5, 1.5],),
                ([-3.0, -0.5, -2.5],),
            ],
            schema=["features"],
        )

        scaler = MaxAbsScaler(inputCol="features", outputCol="scaled_features")

        model = scaler.fit(df1)
        assert model.uid == scaler.uid
        result = model.transform(df1).toPandas()
        assert list(result.columns) == ["features", "scaled_features"]

        expected_result = [[2.0 / 3, 1.0, 0.6], [-1.0, -1.0 / 7, -1.0]]

        np.testing.assert_allclose(list(result.scaled_features), expected_result)

        local_df1 = df1.toPandas()
        local_fit_model = scaler.fit(local_df1)
        local_transform_result = local_fit_model.transform(local_df1)
        assert id(local_transform_result) == id(local_df1)
        assert list(local_transform_result.columns) == ["features", "scaled_features"]

        np.testing.assert_allclose(list(local_transform_result.scaled_features), expected_result)

        with tempfile.TemporaryDirectory() as tmp_dir:
            estimator_path = os.path.join(tmp_dir, "estimator")
            scaler.saveToLocal(estimator_path)
            loaded_scaler = MaxAbsScaler.loadFromLocal(estimator_path)
            assert loaded_scaler.getInputCol() == "features"
            assert loaded_scaler.getOutputCol() == "scaled_features"

            model_path = os.path.join(tmp_dir, "model")
            model.saveToLocal(model_path)
            loaded_model = MaxAbsScalerModel.loadFromLocal(model_path)

            np.testing.assert_allclose(model.scale_values, loaded_model.scale_values)
            np.testing.assert_allclose(model.max_abs_values, loaded_model.max_abs_values)
            assert model.n_samples_seen == loaded_model.n_samples_seen

            # Test loading core model as scikit-learn model
            with open(os.path.join(model_path, "MaxAbsScalerModel.sklearn.pkl"), "rb") as f:
                sk_model = pickle.load(f)
                sk_result = sk_model.transform(np.stack(list(local_df1.features)))
                np.testing.assert_allclose(sk_result, expected_result)

    def test_standard_scaler(self):
        df1 = self.spark.createDataFrame(
            [
                ([2.0, 3.5, 1.5],),
                ([-3.0, -0.5, -2.5],),
                ([1.0, -1.5, 0.5],),
            ],
            schema=["features"],
        )

        scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
        model = scaler.fit(df1)
        assert model.uid == scaler.uid
        result = model.transform(df1).toPandas()
        assert list(result.columns) == ["features", "scaled_features"]

        expected_result = [
            [0.7559289460184544, 1.1338934190276817, 0.8006407690254358],
            [-1.1338934190276817, -0.3779644730092272, -1.1208970766356101],
            [0.3779644730092272, -0.7559289460184544, 0.32025630761017426],
        ]

        np.testing.assert_allclose(list(result.scaled_features), expected_result)

        local_df1 = df1.toPandas()
        local_fit_model = scaler.fit(local_df1)
        local_transform_result = local_fit_model.transform(local_df1)
        assert id(local_transform_result) == id(local_df1)
        assert list(local_transform_result.columns) == ["features", "scaled_features"]

        np.testing.assert_allclose(list(local_transform_result.scaled_features), expected_result)

        with tempfile.TemporaryDirectory() as tmp_dir:
            estimator_path = os.path.join(tmp_dir, "estimator")
            scaler.saveToLocal(estimator_path)
            loaded_scaler = StandardScaler.loadFromLocal(estimator_path)
            assert loaded_scaler.getInputCol() == "features"
            assert loaded_scaler.getOutputCol() == "scaled_features"

            model_path = os.path.join(tmp_dir, "model")
            model.saveToLocal(model_path)
            loaded_model = StandardScalerModel.loadFromLocal(model_path)

            np.testing.assert_allclose(model.std_values, loaded_model.std_values)
            np.testing.assert_allclose(model.mean_values, loaded_model.mean_values)
            np.testing.assert_allclose(model.scale_values, loaded_model.scale_values)
            assert model.n_samples_seen == loaded_model.n_samples_seen

            # Test loading core model as scikit-learn model
            with open(os.path.join(model_path, "StandardScalerModel.sklearn.pkl"), "rb") as f:
                sk_model = pickle.load(f)
                sk_result = sk_model.transform(np.stack(list(local_df1.features)))
                np.testing.assert_allclose(sk_result, expected_result)


class FeatureTests(FeatureTestsMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.spark = SparkSession.builder.master("local[2]").getOrCreate()

    def tearDown(self) -> None:
        self.spark.stop()


if __name__ == "__main__":
    from pyspark.ml.tests.connect.test_legacy_mode_feature import *  # noqa: F401,F403

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
