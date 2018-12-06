#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import numpy as np

from pyspark.ml import Estimator, Model, Transformer, UnaryTransformer
from pyspark.ml.param import Param, Params, TypeConverters
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.ml.wrapper import _java2py
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType
from pyspark.testing.utils import ReusedPySparkTestCase as PySparkTestCase


def check_params(test_self, py_stage, check_params_exist=True):
    """
    Checks common requirements for Params.params:
      - set of params exist in Java and Python and are ordered by names
      - param parent has the same UID as the object's UID
      - default param value from Java matches value in Python
      - optionally check if all params from Java also exist in Python
    """
    py_stage_str = "%s %s" % (type(py_stage), py_stage)
    if not hasattr(py_stage, "_to_java"):
        return
    java_stage = py_stage._to_java()
    if java_stage is None:
        return
    test_self.assertEqual(py_stage.uid, java_stage.uid(), msg=py_stage_str)
    if check_params_exist:
        param_names = [p.name for p in py_stage.params]
        java_params = list(java_stage.params())
        java_param_names = [jp.name() for jp in java_params]
        test_self.assertEqual(
            param_names, sorted(java_param_names),
            "Param list in Python does not match Java for %s:\nJava = %s\nPython = %s"
            % (py_stage_str, java_param_names, param_names))
    for p in py_stage.params:
        test_self.assertEqual(p.parent, py_stage.uid)
        java_param = java_stage.getParam(p.name)
        py_has_default = py_stage.hasDefault(p)
        java_has_default = java_stage.hasDefault(java_param)
        test_self.assertEqual(py_has_default, java_has_default,
                              "Default value mismatch of param %s for Params %s"
                              % (p.name, str(py_stage)))
        if py_has_default:
            if p.name == "seed":
                continue  # Random seeds between Spark and PySpark are different
            java_default = _java2py(test_self.sc,
                                    java_stage.clear(java_param).getOrDefault(java_param))
            py_stage._clear(p)
            py_default = py_stage.getOrDefault(p)
            # equality test for NaN is always False
            if isinstance(java_default, float) and np.isnan(java_default):
                java_default = "NaN"
                py_default = "NaN" if np.isnan(py_default) else "not NaN"
            test_self.assertEqual(
                java_default, py_default,
                "Java default %s != python default %s of param %s for Params %s"
                % (str(java_default), str(py_default), p.name, str(py_stage)))


class SparkSessionTestCase(PySparkTestCase):
    @classmethod
    def setUpClass(cls):
        PySparkTestCase.setUpClass()
        cls.spark = SparkSession(cls.sc)

    @classmethod
    def tearDownClass(cls):
        PySparkTestCase.tearDownClass()
        cls.spark.stop()


class MockDataset(DataFrame):

    def __init__(self):
        self.index = 0


class HasFake(Params):

    def __init__(self):
        super(HasFake, self).__init__()
        self.fake = Param(self, "fake", "fake param")

    def getFake(self):
        return self.getOrDefault(self.fake)


class MockTransformer(Transformer, HasFake):

    def __init__(self):
        super(MockTransformer, self).__init__()
        self.dataset_index = None

    def _transform(self, dataset):
        self.dataset_index = dataset.index
        dataset.index += 1
        return dataset


class MockUnaryTransformer(UnaryTransformer, DefaultParamsReadable, DefaultParamsWritable):

    shift = Param(Params._dummy(), "shift", "The amount by which to shift " +
                  "data in a DataFrame",
                  typeConverter=TypeConverters.toFloat)

    def __init__(self, shiftVal=1):
        super(MockUnaryTransformer, self).__init__()
        self._setDefault(shift=1)
        self._set(shift=shiftVal)

    def getShift(self):
        return self.getOrDefault(self.shift)

    def setShift(self, shift):
        self._set(shift=shift)

    def createTransformFunc(self):
        shiftVal = self.getShift()
        return lambda x: x + shiftVal

    def outputDataType(self):
        return DoubleType()

    def validateInputType(self, inputType):
        if inputType != DoubleType():
            raise TypeError("Bad input type: {}. ".format(inputType) +
                            "Requires Double.")


class MockEstimator(Estimator, HasFake):

    def __init__(self):
        super(MockEstimator, self).__init__()
        self.dataset_index = None

    def _fit(self, dataset):
        self.dataset_index = dataset.index
        model = MockModel()
        self._copyValues(model)
        return model


class MockModel(MockTransformer, Model, HasFake):
    pass
