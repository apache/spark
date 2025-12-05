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
import tempfile
import unittest

import numpy as np

from pyspark.util import is_remote_only
from pyspark.testing.connectutils import should_test_connect, connect_requirement_message
from pyspark.testing.utils import have_torch, torch_requirement_message
from pyspark.testing.sqlutils import ReusedSQLTestCase

if should_test_connect:
    from pyspark.ml.connect.feature import (
        MaxAbsScaler,
        MaxAbsScalerModel,
        StandardScaler,
        StandardScalerModel,
        ArrayAssembler,
    )
    import pandas as pd


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
        local_df1_copy = local_df1.copy()
        local_transform_result = local_fit_model.transform(local_df1)
        # assert that `transform` doesn't mutate the input dataframe.
        pd.testing.assert_frame_equal(local_df1, local_df1_copy)
        assert list(local_transform_result.columns) == ["features", "scaled_features"]

        np.testing.assert_allclose(list(local_transform_result.scaled_features), expected_result)

        with tempfile.TemporaryDirectory(prefix="test_max_abs_scaler") as tmp_dir:
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
        local_df1_copy = local_df1.copy()
        local_transform_result = local_fit_model.transform(local_df1)
        # assert that `transform` doesn't mutate the input dataframe.
        pd.testing.assert_frame_equal(local_df1, local_df1_copy)
        assert list(local_transform_result.columns) == ["features", "scaled_features"]

        np.testing.assert_allclose(list(local_transform_result.scaled_features), expected_result)

        with tempfile.TemporaryDirectory(prefix="test_standard_scaler") as tmp_dir:
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

    def test_array_assembler(self):
        spark_df = self.spark.createDataFrame(
            [
                ([2.0, 3.5, 1.5], 3.0, True, 1),
                ([-3.0, np.nan, -2.5], 4.0, False, 2),
            ],
            schema=["f1", "f2", "f3", "f4"],
        )
        pandas_df = spark_df.toPandas()

        assembler1 = ArrayAssembler(
            inputCols=["f1", "f2", "f3", "f4"],
            outputCol="out",
            featureSizes=[3, 1, 1, 1],
            handleInvalid="keep",
        )
        expected_result = [
            [2.0, 3.5, 1.5, 3.0, 1.0, 1.0],
            [-3.0, np.nan, -2.5, 4.0, 0.0, 2.0],
        ]
        result1 = assembler1.transform(pandas_df)["out"].tolist()
        np.testing.assert_allclose(result1, expected_result)

        result2 = assembler1.transform(spark_df).toPandas()["out"].tolist()
        # For spark UDF, if output is a array type, 'NaN' values in UDF output array
        # are converted to 'None' value.
        if result2[1][1] is None:
            result2[1][1] = np.nan
        np.testing.assert_allclose(result2, expected_result)

        with tempfile.TemporaryDirectory(prefix="test_array_assembler") as tmp_dir:
            save_path = os.path.join(tmp_dir, "assembler")
            assembler1.saveToLocal(save_path)
            loaded_assembler = ArrayAssembler.loadFromLocal(save_path)
            assert loaded_assembler.getInputCols() == ["f1", "f2", "f3", "f4"]
            assert loaded_assembler.getFeatureSizes() == [3, 1, 1, 1]

        assembler2 = ArrayAssembler(
            inputCols=["f1", "f2", "f3", "f4"],
            outputCol="out",
            featureSizes=[3, 1, 1, 1],
            handleInvalid="error",
        )

        with self.assertRaisesRegex(Exception, "The input features contains invalid value"):
            assembler2.transform(pandas_df)["out"].tolist()


@unittest.skipIf(
    not should_test_connect or not have_torch or is_remote_only(),
    connect_requirement_message
    or torch_requirement_message
    or "pyspark-connect cannot test classic Spark",
)
class FeatureTests(FeatureTestsMixin, ReusedSQLTestCase):
    @classmethod
    def master(cls):
        return "local[2]"


if __name__ == "__main__":
    from pyspark.ml.tests.connect.test_legacy_mode_feature import *  # noqa: F401,F403

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
