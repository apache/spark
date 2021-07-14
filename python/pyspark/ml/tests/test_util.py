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

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, OneVsRest
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors
from pyspark.ml.util import MetaAlgorithmReadWrite
from pyspark.testing.mlutils import SparkSessionTestCase


class MetaAlgorithmReadWriteTests(SparkSessionTestCase):

    def test_getAllNestedStages(self):
        def _check_uid_set_equal(stages, expected_stages):
            uids = set(map(lambda x: x.uid, stages))
            expected_uids = set(map(lambda x: x.uid, expected_stages))
            self.assertEqual(uids, expected_uids)

        df1 = self.spark.createDataFrame([
            (Vectors.dense([1., 2.]), 1.0),
            (Vectors.dense([-1., -2.]), 0.0),
        ], ['features', 'label'])
        df2 = self.spark.createDataFrame([
            (1., 2., 1.0),
            (1., 2., 0.0),
        ], ['a', 'b', 'label'])
        vs = VectorAssembler(inputCols=['a', 'b'], outputCol='features')
        lr = LogisticRegression()
        pipeline = Pipeline(stages=[vs, lr])
        pipelineModel = pipeline.fit(df2)
        ova = OneVsRest(classifier=lr)
        ovaModel = ova.fit(df1)

        ova_pipeline = Pipeline(stages=[vs, ova])
        nested_pipeline = Pipeline(stages=[ova_pipeline])

        _check_uid_set_equal(
            MetaAlgorithmReadWrite.getAllNestedStages(pipeline),
            [pipeline, vs, lr]
        )
        _check_uid_set_equal(
            MetaAlgorithmReadWrite.getAllNestedStages(pipelineModel),
            [pipelineModel] + pipelineModel.stages
        )
        _check_uid_set_equal(
            MetaAlgorithmReadWrite.getAllNestedStages(ova),
            [ova, lr]
        )
        _check_uid_set_equal(
            MetaAlgorithmReadWrite.getAllNestedStages(ovaModel),
            [ovaModel, lr] + ovaModel.models
        )
        _check_uid_set_equal(
            MetaAlgorithmReadWrite.getAllNestedStages(nested_pipeline),
            [nested_pipeline, ova_pipeline, vs, ova, lr]
        )


if __name__ == "__main__":
    from pyspark.ml.tests.test_util import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
