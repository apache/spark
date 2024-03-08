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

from pyspark.ml.classification import LogisticRegression, OneVsRest
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.linalg import Vectors
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
    def _run_test_save_load_nested_estimator(self, LogisticRegressionCls):
        # This tests saving and loading the trained model only.
        # Save/load for TrainValidationSplit will be added later: SPARK-13786
        temp_path = tempfile.mkdtemp()
        dataset = self.spark.createDataFrame(
            [
                (Vectors.dense([0.0]), 0.0),
                (Vectors.dense([0.4]), 1.0),
                (Vectors.dense([0.5]), 0.0),
                (Vectors.dense([0.6]), 1.0),
                (Vectors.dense([1.0]), 1.0),
            ]
            * 10,
            ["features", "label"],
        )
        ova = OneVsRest(classifier=LogisticRegressionCls())
        lr1 = LogisticRegressionCls().setMaxIter(100)
        lr2 = LogisticRegressionCls().setMaxIter(150)
        grid = ParamGridBuilder().addGrid(ova.classifier, [lr1, lr2]).build()
        evaluator = MulticlassClassificationEvaluator()

        tvs = TrainValidationSplit(estimator=ova, estimatorParamMaps=grid, evaluator=evaluator)
        tvsModel = tvs.fit(dataset)
        tvsPath = temp_path + "/tvs"
        tvs.save(tvsPath)
        loadedTvs = TrainValidationSplit.load(tvsPath)
        self.assert_param_maps_equal(loadedTvs.getEstimatorParamMaps(), grid)
        self.assertEqual(loadedTvs.getEstimator().uid, tvs.getEstimator().uid)
        self.assertEqual(loadedTvs.getEvaluator().uid, tvs.getEvaluator().uid)

        originalParamMap = tvs.getEstimatorParamMaps()
        loadedParamMap = loadedTvs.getEstimatorParamMaps()
        for i, param in enumerate(loadedParamMap):
            for p in param:
                if p.name == "classifier":
                    self.assertEqual(param[p].uid, originalParamMap[i][p].uid)
                else:
                    self.assertEqual(param[p], originalParamMap[i][p])

        tvsModelPath = temp_path + "/tvsModel"
        tvsModel.save(tvsModelPath)
        loadedModel = TrainValidationSplitModel.load(tvsModelPath)
        self.assert_param_maps_equal(loadedModel.getEstimatorParamMaps(), grid)
        self.assertEqual(loadedModel.bestModel.uid, tvsModel.bestModel.uid)

    def test_save_load_nested_estimator(self):
        self._run_test_save_load_nested_estimator(LogisticRegression)
        self._run_test_save_load_nested_estimator(DummyLogisticRegression)


if __name__ == "__main__":
    from pyspark.ml.tests.tuning.test_tvs_io_nested import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
