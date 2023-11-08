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

from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, OneVsRest
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import (
    ParamGridBuilder,
    TrainValidationSplit,
    TrainValidationSplitModel,
)
from pyspark.testing.mlutils import (
    DummyLogisticRegression,
    SparkSessionTestCase,
)

from pyspark.ml.tests.tuning.test_tuning import ValidatorTestUtilsMixin


class TrainValidationSplitIONestedTests(SparkSessionTestCase, ValidatorTestUtilsMixin):
    def _run_test_save_load_pipeline_estimator(self, LogisticRegressionCls):
        temp_path = tempfile.mkdtemp()
        training = self.spark.createDataFrame(
            [
                (0, "a b c d e spark", 1.0),
                (1, "b d", 0.0),
                (2, "spark f g h", 1.0),
                (3, "hadoop mapreduce", 0.0),
                (4, "b spark who", 1.0),
                (5, "g d a y", 0.0),
                (6, "spark fly", 1.0),
                (7, "was mapreduce", 0.0),
            ],
            ["id", "text", "label"],
        )

        # Configure an ML pipeline, which consists of tree stages: tokenizer, hashingTF, and lr.
        tokenizer = Tokenizer(inputCol="text", outputCol="words")
        hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="features")

        ova = OneVsRest(classifier=LogisticRegressionCls())
        lr1 = LogisticRegressionCls().setMaxIter(5)
        lr2 = LogisticRegressionCls().setMaxIter(10)

        pipeline = Pipeline(stages=[tokenizer, hashingTF, ova])

        paramGrid = (
            ParamGridBuilder()
            .addGrid(hashingTF.numFeatures, [10, 100])
            .addGrid(ova.classifier, [lr1, lr2])
            .build()
        )

        tvs = TrainValidationSplit(
            estimator=pipeline,
            estimatorParamMaps=paramGrid,
            evaluator=MulticlassClassificationEvaluator(),
        )
        tvsPath = temp_path + "/tvs"
        tvs.save(tvsPath)
        loadedTvs = TrainValidationSplit.load(tvsPath)
        self.assert_param_maps_equal(loadedTvs.getEstimatorParamMaps(), paramGrid)
        self.assertEqual(loadedTvs.getEstimator().uid, tvs.getEstimator().uid)

        # Run train validation split, and choose the best set of parameters.
        tvsModel = tvs.fit(training)

        # test save/load of CrossValidatorModel
        tvsModelPath = temp_path + "/tvsModel"
        tvsModel.save(tvsModelPath)
        loadedModel = TrainValidationSplitModel.load(tvsModelPath)
        self.assertEqual(loadedModel.bestModel.uid, tvsModel.bestModel.uid)
        self.assertEqual(len(loadedModel.bestModel.stages), len(tvsModel.bestModel.stages))
        for loadedStage, originalStage in zip(
            loadedModel.bestModel.stages, tvsModel.bestModel.stages
        ):
            self.assertEqual(loadedStage.uid, originalStage.uid)

        # Test nested pipeline
        nested_pipeline = Pipeline(stages=[tokenizer, Pipeline(stages=[hashingTF, ova])])
        tvs2 = TrainValidationSplit(
            estimator=nested_pipeline,
            estimatorParamMaps=paramGrid,
            evaluator=MulticlassClassificationEvaluator(),
        )
        tvs2Path = temp_path + "/tvs2"
        tvs2.save(tvs2Path)
        loadedTvs2 = TrainValidationSplit.load(tvs2Path)
        self.assert_param_maps_equal(loadedTvs2.getEstimatorParamMaps(), paramGrid)
        self.assertEqual(loadedTvs2.getEstimator().uid, tvs2.getEstimator().uid)

        # Run train validation split, and choose the best set of parameters.
        tvsModel2 = tvs2.fit(training)
        # test save/load of CrossValidatorModel
        tvsModelPath2 = temp_path + "/tvsModel2"
        tvsModel2.save(tvsModelPath2)
        loadedModel2 = TrainValidationSplitModel.load(tvsModelPath2)
        self.assertEqual(loadedModel2.bestModel.uid, tvsModel2.bestModel.uid)
        loaded_nested_pipeline_model = loadedModel2.bestModel.stages[1]
        original_nested_pipeline_model = tvsModel2.bestModel.stages[1]
        self.assertEqual(loaded_nested_pipeline_model.uid, original_nested_pipeline_model.uid)
        self.assertEqual(
            len(loaded_nested_pipeline_model.stages), len(original_nested_pipeline_model.stages)
        )
        for loadedStage, originalStage in zip(
            loaded_nested_pipeline_model.stages, original_nested_pipeline_model.stages
        ):
            self.assertEqual(loadedStage.uid, originalStage.uid)

    def test_save_load_pipeline_estimator(self):
        self._run_test_save_load_pipeline_estimator(LogisticRegression)
        self._run_test_save_load_pipeline_estimator(DummyLogisticRegression)


if __name__ == "__main__":
    from pyspark.ml.tests.tuning.test_tvs_io_pipeline import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
