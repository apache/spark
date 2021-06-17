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

from pyspark.mllib.common import _to_java_object_rdd  # type: ignore[attr-defined]
from pyspark.mllib.util import LinearDataGenerator
from pyspark.mllib.util import MLUtils
from pyspark.mllib.linalg import SparseVector, DenseVector, Vectors
from pyspark.mllib.random import RandomRDDs
from pyspark.testing.mllibutils import MLlibTestCase


class MLUtilsTests(MLlibTestCase):
    def test_append_bias(self):
        data = [2.0, 2.0, 2.0]
        ret = MLUtils.appendBias(data)
        self.assertEqual(ret[3], 1.0)
        self.assertEqual(type(ret), DenseVector)

    def test_append_bias_with_vector(self):
        data = Vectors.dense([2.0, 2.0, 2.0])
        ret = MLUtils.appendBias(data)
        self.assertEqual(ret[3], 1.0)
        self.assertEqual(type(ret), DenseVector)

    def test_append_bias_with_sp_vector(self):
        data = Vectors.sparse(3, {0: 2.0, 2: 2.0})
        expected = Vectors.sparse(4, {0: 2.0, 2: 2.0, 3: 1.0})
        # Returned value must be SparseVector
        ret = MLUtils.appendBias(data)
        self.assertEqual(ret, expected)
        self.assertEqual(type(ret), SparseVector)

    def test_load_vectors(self):
        import shutil
        data = [
            [1.0, 2.0, 3.0],
            [1.0, 2.0, 3.0]
        ]
        temp_dir = tempfile.mkdtemp()
        load_vectors_path = os.path.join(temp_dir, "test_load_vectors")
        try:
            self.sc.parallelize(data).saveAsTextFile(load_vectors_path)
            ret_rdd = MLUtils.loadVectors(self.sc, load_vectors_path)
            ret = ret_rdd.collect()
            self.assertEqual(len(ret), 2)
            self.assertEqual(ret[0], DenseVector([1.0, 2.0, 3.0]))
            self.assertEqual(ret[1], DenseVector([1.0, 2.0, 3.0]))
        except:
            self.fail()
        finally:
            shutil.rmtree(load_vectors_path)


class LinearDataGeneratorTests(MLlibTestCase):
    def test_dim(self):
        linear_data = LinearDataGenerator.generateLinearInput(
            intercept=0.0, weights=[0.0, 0.0, 0.0],
            xMean=[0.0, 0.0, 0.0], xVariance=[0.33, 0.33, 0.33],
            nPoints=4, seed=0, eps=0.1)
        self.assertEqual(len(linear_data), 4)
        for point in linear_data:
            self.assertEqual(len(point.features), 3)

        linear_data = LinearDataGenerator.generateLinearRDD(
            sc=self.sc, nexamples=6, nfeatures=2, eps=0.1,
            nParts=2, intercept=0.0).collect()
        self.assertEqual(len(linear_data), 6)
        for point in linear_data:
            self.assertEqual(len(point.features), 2)


class SerDeTest(MLlibTestCase):
    def test_to_java_object_rdd(self):  # SPARK-6660
        data = RandomRDDs.uniformRDD(self.sc, 10, 5, seed=0)
        self.assertEqual(_to_java_object_rdd(data).count(), 10)


if __name__ == "__main__":
    from pyspark.mllib.tests.test_util import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports', verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
