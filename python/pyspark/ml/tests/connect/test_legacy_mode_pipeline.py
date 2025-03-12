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
import tempfile
import unittest

import numpy as np

from pyspark.util import is_remote_only
from pyspark.sql import SparkSession
from pyspark.testing.connectutils import should_test_connect, connect_requirement_message
from pyspark.testing.utils import have_torch, torch_requirement_message

if should_test_connect:
    from pyspark.ml.connect.feature import StandardScaler
    from pyspark.ml.connect.classification import LogisticRegression as LORV2
    from pyspark.ml.connect.pipeline import Pipeline
    import pandas as pd


class PipelineTestsMixin:
    @staticmethod
    def _check_result(result_dataframe, expected_predictions, expected_probabilities=None):
        np.testing.assert_array_equal(list(result_dataframe.prediction), expected_predictions)
        if "probability" in result_dataframe.columns:
            np.testing.assert_allclose(
                list(result_dataframe.probability),
                expected_probabilities,
                rtol=1e-1,
            )

    def test_pipeline(self):
        train_dataset = self.spark.createDataFrame(
            [
                (1.0, [0.0, 5.0]),
                (0.0, [1.0, 2.0]),
                (1.0, [2.0, 1.0]),
                (0.0, [3.0, 3.0]),
            ]
            * 100,
            ["label", "features"],
        )
        eval_dataset = self.spark.createDataFrame(
            [
                ([0.0, 2.0],),
                ([3.5, 3.0],),
            ],
            ["features"],
        )
        scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
        lorv2 = LORV2(
            maxIter=200, numTrainWorkers=2, learningRate=0.001, featuresCol="scaled_features"
        )

        pipeline = Pipeline(stages=[scaler, lorv2])
        model = pipeline.fit(train_dataset)
        assert model.uid == pipeline.uid

        expected_predictions = [1, 0]
        expected_probabilities = [
            [0.117658, 0.882342],
            [0.878738, 0.121262],
        ]

        result = model.transform(eval_dataset).toPandas()
        self._check_result(result, expected_predictions, expected_probabilities)
        local_transform_result = model.transform(eval_dataset.toPandas())
        self._check_result(local_transform_result, expected_predictions, expected_probabilities)

        pipeline2 = Pipeline(stages=[pipeline])
        model2 = pipeline2.fit(train_dataset)
        result2 = model2.transform(eval_dataset).toPandas()
        self._check_result(result2, expected_predictions, expected_probabilities)
        local_eval_dataset = eval_dataset.toPandas()
        local_eval_dataset_copy = local_eval_dataset.copy()
        local_transform_result2 = model2.transform(local_eval_dataset)
        # assert that `transform` doesn't mutate the input dataframe.
        pd.testing.assert_frame_equal(local_eval_dataset, local_eval_dataset_copy)
        self._check_result(local_transform_result2, expected_predictions, expected_probabilities)

        with tempfile.TemporaryDirectory(prefix="test_pipeline") as tmp_dir:
            pipeline_local_path = os.path.join(tmp_dir, "pipeline")
            pipeline.saveToLocal(pipeline_local_path)
            loaded_pipeline = Pipeline.loadFromLocal(pipeline_local_path)

            assert pipeline.uid == loaded_pipeline.uid
            assert loaded_pipeline.getStages()[1].getMaxIter() == 200

            pipeline_model_local_path = os.path.join(tmp_dir, "pipeline_model")
            model.saveToLocal(pipeline_model_local_path)
            loaded_model = Pipeline.loadFromLocal(pipeline_model_local_path)

            assert model.uid == loaded_model.uid
            assert loaded_model.stages[1].getMaxIter() == 200

            loaded_model_transform_result = loaded_model.transform(eval_dataset).toPandas()
            self._check_result(
                loaded_model_transform_result, expected_predictions, expected_probabilities
            )

            pipeline2_local_path = os.path.join(tmp_dir, "pipeline2")
            pipeline2.saveToLocal(pipeline2_local_path)
            loaded_pipeline2 = Pipeline.loadFromLocal(pipeline2_local_path)

            assert pipeline2.uid == loaded_pipeline2.uid
            assert loaded_pipeline2.getStages()[0].getStages()[1].getMaxIter() == 200

            pipeline2_model_local_path = os.path.join(tmp_dir, "pipeline2_model")
            model2.saveToLocal(pipeline2_model_local_path)
            loaded_model2 = Pipeline.loadFromLocal(pipeline2_model_local_path)

            assert model2.uid == loaded_model2.uid
            assert loaded_model2.stages[0].stages[1].getMaxIter() == 200

            loaded_model2_transform_result = loaded_model2.transform(eval_dataset).toPandas()
            self._check_result(
                loaded_model2_transform_result, expected_predictions, expected_probabilities
            )

    @staticmethod
    def test_pipeline_copy():
        scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
        lorv2 = LORV2(
            maxIter=200, numTrainWorkers=2, learningRate=0.001, featuresCol="scaled_features"
        )

        pipeline = Pipeline(stages=[scaler, lorv2])

        copied_pipeline = pipeline.copy(
            {scaler.inputCol: "f1", lorv2.maxIter: 10, lorv2.numTrainWorkers: 1}
        )

        stages = copied_pipeline.getStages()

        assert stages[0].getInputCol() == "f1"
        assert stages[1].getOrDefault(stages[1].maxIter) == 10
        assert stages[1].getOrDefault(stages[1].numTrainWorkers) == 1
        assert stages[1].getOrDefault(stages[1].featuresCol) == "scaled_features"

        pipeline2 = Pipeline(stages=[pipeline])
        copied_pipeline2 = pipeline2.copy(
            {scaler.inputCol: "f2", lorv2.maxIter: 20, lorv2.numTrainWorkers: 20}
        )

        stages = copied_pipeline2.getStages()[0].getStages()

        assert stages[0].getInputCol() == "f2"
        assert stages[1].getOrDefault(stages[1].maxIter) == 20
        assert stages[1].getOrDefault(stages[1].numTrainWorkers) == 20
        assert stages[1].getOrDefault(stages[1].featuresCol) == "scaled_features"

        # test original stage instance params are not modified after pipeline copying.
        assert scaler.getInputCol() == "features"
        assert lorv2.getOrDefault(lorv2.maxIter) == 200


@unittest.skipIf(
    not should_test_connect or not have_torch or is_remote_only(),
    connect_requirement_message
    or torch_requirement_message
    or "pyspark-connect cannot test classic Spark",
)
class PipelineTests(PipelineTestsMixin, unittest.TestCase):
    def setUp(self) -> None:
        self.spark = SparkSession.builder.master("local[2]").getOrCreate()

    def tearDown(self) -> None:
        self.spark.stop()


if __name__ == "__main__":
    from pyspark.ml.tests.connect.test_legacy_mode_pipeline import *  # noqa: F401,F403

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
