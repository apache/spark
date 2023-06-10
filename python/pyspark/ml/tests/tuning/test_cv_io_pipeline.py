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
    CrossValidator,
    CrossValidatorModel,
    ParamGridBuilder,
)
from pyspark.testing.mlutils import (
    DummyLogisticRegression,
    SparkSessionTestCase,
)
from pyspark.ml.tests.tuning.test_tuning import ValidatorTestUtilsMixin


class CrossValidatorIOPipelineTests(SparkSessionTestCase, ValidatorTestUtilsMixin):
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

        crossval = CrossValidator(
            estimator=pipeline,
            estimatorParamMaps=paramGrid,
            evaluator=MulticlassClassificationEvaluator(),
            numFolds=2,
        )  # use 3+ folds in practice
        cvPath = temp_path + "/cv"
        crossval.save(cvPath)
        loadedCV = CrossValidator.load(cvPath)
        self.assert_param_maps_equal(loadedCV.getEstimatorParamMaps(), paramGrid)
        self.assertEqual(loadedCV.getEstimator().uid, crossval.getEstimator().uid)

        # Run cross-validation, and choose the best set of parameters.
        cvModel = crossval.fit(training)

        # test save/load of CrossValidatorModel
        cvModelPath = temp_path + "/cvModel"
        cvModel.save(cvModelPath)
        loadedModel = CrossValidatorModel.load(cvModelPath)
        self.assertEqual(loadedModel.bestModel.uid, cvModel.bestModel.uid)
        self.assertEqual(len(loadedModel.bestModel.stages), len(cvModel.bestModel.stages))
        for loadedStage, originalStage in zip(
            loadedModel.bestModel.stages, cvModel.bestModel.stages
        ):
            self.assertEqual(loadedStage.uid, originalStage.uid)

        # Test nested pipeline
        nested_pipeline = Pipeline(stages=[tokenizer, Pipeline(stages=[hashingTF, ova])])
        crossval2 = CrossValidator(
            estimator=nested_pipeline,
            estimatorParamMaps=paramGrid,
            evaluator=MulticlassClassificationEvaluator(),
            numFolds=2,
        )  # use 3+ folds in practice
        cv2Path = temp_path + "/cv2"
        crossval2.save(cv2Path)
        loadedCV2 = CrossValidator.load(cv2Path)
        self.assert_param_maps_equal(loadedCV2.getEstimatorParamMaps(), paramGrid)
        self.assertEqual(loadedCV2.getEstimator().uid, crossval2.getEstimator().uid)

        # Run cross-validation, and choose the best set of parameters.
        cvModel2 = crossval2.fit(training)
        # test save/load of CrossValidatorModel
        cvModelPath2 = temp_path + "/cvModel2"
        cvModel2.save(cvModelPath2)
        loadedModel2 = CrossValidatorModel.load(cvModelPath2)
        self.assertEqual(loadedModel2.bestModel.uid, cvModel2.bestModel.uid)
        loaded_nested_pipeline_model = loadedModel2.bestModel.stages[1]
        original_nested_pipeline_model = cvModel2.bestModel.stages[1]
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
    from pyspark.ml.tests.tuning.test_cv_io_pipeline import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
