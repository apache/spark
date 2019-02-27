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

import py4j

from pyspark.ml.linalg import DenseVector, Vectors
from pyspark.ml.regression import LinearRegression
from pyspark.ml.wrapper import _java2py, _py2java, JavaParams, JavaWrapper
from pyspark.testing.mllibutils import MLlibTestCase
from pyspark.testing.mlutils import SparkSessionTestCase


class JavaWrapperMemoryTests(SparkSessionTestCase):

    def test_java_object_gets_detached(self):
        df = self.spark.createDataFrame([(1.0, 2.0, Vectors.dense(1.0)),
                                         (0.0, 2.0, Vectors.sparse(1, [], []))],
                                        ["label", "weight", "features"])
        lr = LinearRegression(maxIter=1, regParam=0.0, solver="normal", weightCol="weight",
                              fitIntercept=False)

        model = lr.fit(df)
        summary = model.summary

        self.assertIsInstance(model, JavaWrapper)
        self.assertIsInstance(summary, JavaWrapper)
        self.assertIsInstance(model, JavaParams)
        self.assertNotIsInstance(summary, JavaParams)

        error_no_object = 'Target Object ID does not exist for this gateway'

        self.assertIn("LinearRegression_", model._java_obj.toString())
        self.assertIn("LinearRegressionTrainingSummary", summary._java_obj.toString())

        model.__del__()

        with self.assertRaisesRegexp(py4j.protocol.Py4JError, error_no_object):
            model._java_obj.toString()
        self.assertIn("LinearRegressionTrainingSummary", summary._java_obj.toString())

        try:
            summary.__del__()
        except:
            pass

        with self.assertRaisesRegexp(py4j.protocol.Py4JError, error_no_object):
            model._java_obj.toString()
        with self.assertRaisesRegexp(py4j.protocol.Py4JError, error_no_object):
            summary._java_obj.toString()


class WrapperTests(MLlibTestCase):

    def test_new_java_array(self):
        # test array of strings
        str_list = ["a", "b", "c"]
        java_class = self.sc._gateway.jvm.java.lang.String
        java_array = JavaWrapper._new_java_array(str_list, java_class)
        self.assertEqual(_java2py(self.sc, java_array), str_list)
        # test array of integers
        int_list = [1, 2, 3]
        java_class = self.sc._gateway.jvm.java.lang.Integer
        java_array = JavaWrapper._new_java_array(int_list, java_class)
        self.assertEqual(_java2py(self.sc, java_array), int_list)
        # test array of floats
        float_list = [0.1, 0.2, 0.3]
        java_class = self.sc._gateway.jvm.java.lang.Double
        java_array = JavaWrapper._new_java_array(float_list, java_class)
        self.assertEqual(_java2py(self.sc, java_array), float_list)
        # test array of bools
        bool_list = [False, True, True]
        java_class = self.sc._gateway.jvm.java.lang.Boolean
        java_array = JavaWrapper._new_java_array(bool_list, java_class)
        self.assertEqual(_java2py(self.sc, java_array), bool_list)
        # test array of Java DenseVectors
        v1 = DenseVector([0.0, 1.0])
        v2 = DenseVector([1.0, 0.0])
        vec_java_list = [_py2java(self.sc, v1), _py2java(self.sc, v2)]
        java_class = self.sc._gateway.jvm.org.apache.spark.ml.linalg.DenseVector
        java_array = JavaWrapper._new_java_array(vec_java_list, java_class)
        self.assertEqual(_java2py(self.sc, java_array), [v1, v2])
        # test empty array
        java_class = self.sc._gateway.jvm.java.lang.Integer
        java_array = JavaWrapper._new_java_array([], java_class)
        self.assertEqual(_java2py(self.sc, java_array), [])
        # test array of array of strings
        str_list = [["a", "b", "c"], ["d", "e"], ["f", "g", "h", "i"], []]
        expected_str_list = [("a", "b", "c", None), ("d", "e", None, None), ("f", "g", "h", "i"),
                             (None, None, None, None)]
        java_class = self.sc._gateway.jvm.java.lang.String
        java_array = JavaWrapper._new_java_array(str_list, java_class)
        self.assertEqual(_java2py(self.sc, java_array), expected_str_list)

if __name__ == "__main__":
    from pyspark.ml.tests.test_wrapper import *

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
