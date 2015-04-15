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

"""
Unit tests for Spark ML Python APIs.
"""

import sys

if sys.version_info[:2] <= (2, 6):
    try:
        import unittest2 as unittest
    except ImportError:
        sys.stderr.write('Please install unittest2 to test with Python 2.6 or earlier')
        sys.exit(1)
else:
    import unittest

from pyspark.tests import ReusedPySparkTestCase as PySparkTestCase
from pyspark.sql import DataFrame
from pyspark.ml.param import Param
from pyspark.ml.param.shared import HasMaxIter, HasInputCol
from pyspark.ml.pipeline import Transformer, Estimator, Pipeline


class MockDataset(DataFrame):

    def __init__(self):
        self.index = 0


class MockTransformer(Transformer):

    def __init__(self):
        super(MockTransformer, self).__init__()
        self.fake = Param(self, "fake", "fake")
        self.dataset_index = None
        self.fake_param_value = None

    def transform(self, dataset, params={}):
        self.dataset_index = dataset.index
        if self.fake in params:
            self.fake_param_value = params[self.fake]
        dataset.index += 1
        return dataset


class MockEstimator(Estimator):

    def __init__(self):
        super(MockEstimator, self).__init__()
        self.fake = Param(self, "fake", "fake")
        self.dataset_index = None
        self.fake_param_value = None
        self.model = None

    def fit(self, dataset, params={}):
        self.dataset_index = dataset.index
        if self.fake in params:
            self.fake_param_value = params[self.fake]
        model = MockModel()
        self.model = model
        return model


class MockModel(MockTransformer, Transformer):

    def __init__(self):
        super(MockModel, self).__init__()


class PipelineTests(PySparkTestCase):

    def test_pipeline(self):
        dataset = MockDataset()
        estimator0 = MockEstimator()
        transformer1 = MockTransformer()
        estimator2 = MockEstimator()
        transformer3 = MockTransformer()
        pipeline = Pipeline() \
            .setStages([estimator0, transformer1, estimator2, transformer3])
        pipeline_model = pipeline.fit(dataset, {estimator0.fake: 0, transformer1.fake: 1})
        self.assertEqual(0, estimator0.dataset_index)
        self.assertEqual(0, estimator0.fake_param_value)
        model0 = estimator0.model
        self.assertEqual(0, model0.dataset_index)
        self.assertEqual(1, transformer1.dataset_index)
        self.assertEqual(1, transformer1.fake_param_value)
        self.assertEqual(2, estimator2.dataset_index)
        model2 = estimator2.model
        self.assertIsNone(model2.dataset_index, "The model produced by the last estimator should "
                                                "not be called during fit.")
        dataset = pipeline_model.transform(dataset)
        self.assertEqual(2, model0.dataset_index)
        self.assertEqual(3, transformer1.dataset_index)
        self.assertEqual(4, model2.dataset_index)
        self.assertEqual(5, transformer3.dataset_index)
        self.assertEqual(6, dataset.index)


class TestParams(HasMaxIter, HasInputCol):
    """
    A subclass of Params mixed with HasMaxIter and HasInputCol.
    """

    def __init__(self):
        super(TestParams, self).__init__()
        self._setDefault(maxIter=10)


class ParamTests(PySparkTestCase):

    def test_param(self):
        testParams = TestParams()
        maxIter = testParams.maxIter
        self.assertEqual(maxIter.name, "maxIter")
        self.assertEqual(maxIter.doc, "max number of iterations")
        self.assertTrue(maxIter.parent is testParams)

    def test_params(self):
        testParams = TestParams()
        maxIter = testParams.maxIter
        inputCol = testParams.inputCol

        params = testParams.params
        self.assertEqual(params, [inputCol, maxIter])

        self.assertTrue(testParams.hasDefault(maxIter))
        self.assertFalse(testParams.isSet(maxIter))
        self.assertTrue(testParams.isDefined(maxIter))
        self.assertEqual(testParams.getMaxIter(), 10)
        testParams.setMaxIter(100)
        self.assertTrue(testParams.isSet(maxIter))
        self.assertEquals(testParams.getMaxIter(), 100)

        self.assertFalse(testParams.hasDefault(inputCol))
        self.assertFalse(testParams.isSet(inputCol))
        self.assertFalse(testParams.isDefined(inputCol))
        with self.assertRaises(KeyError):
            testParams.getInputCol()

        self.assertEquals(
            testParams.explainParams(),
            "\n".join(["inputCol: input column name (undefined)",
                       "maxIter: max number of iterations (default: 10, current: 100)"]))


if __name__ == "__main__":
    unittest.main()
