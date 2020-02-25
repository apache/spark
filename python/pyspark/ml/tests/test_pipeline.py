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

from pyspark.ml.pipeline import Pipeline
from pyspark.testing.mlutils import MockDataset, MockEstimator, MockTransformer, PySparkTestCase


class PipelineTests(PySparkTestCase):

    def test_pipeline(self):
        dataset = MockDataset()
        estimator0 = MockEstimator()
        transformer1 = MockTransformer()
        estimator2 = MockEstimator()
        transformer3 = MockTransformer()
        pipeline = Pipeline(stages=[estimator0, transformer1, estimator2, transformer3])
        pipeline_model = pipeline.fit(dataset, {estimator0.fake: 0, transformer1.fake: 1})
        model0, transformer1, model2, transformer3 = pipeline_model.stages
        self.assertEqual(0, model0.dataset_index)
        self.assertEqual(0, model0.getFake())
        self.assertEqual(1, transformer1.dataset_index)
        self.assertEqual(1, transformer1.getFake())
        self.assertEqual(2, dataset.index)
        self.assertIsNone(model2.dataset_index, "The last model shouldn't be called in fit.")
        self.assertIsNone(transformer3.dataset_index,
                          "The last transformer shouldn't be called in fit.")
        dataset = pipeline_model.transform(dataset)
        self.assertEqual(2, model0.dataset_index)
        self.assertEqual(3, transformer1.dataset_index)
        self.assertEqual(4, model2.dataset_index)
        self.assertEqual(5, transformer3.dataset_index)
        self.assertEqual(6, dataset.index)

    def test_identity_pipeline(self):
        dataset = MockDataset()

        def doTransform(pipeline):
            pipeline_model = pipeline.fit(dataset)
            return pipeline_model.transform(dataset)
        # check that empty pipeline did not perform any transformation
        self.assertEqual(dataset.index, doTransform(Pipeline(stages=[])).index)
        # check that failure to set stages param will raise KeyError for missing param
        self.assertRaises(KeyError, lambda: doTransform(Pipeline()))


if __name__ == "__main__":
    from pyspark.ml.tests.test_pipeline import *

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
