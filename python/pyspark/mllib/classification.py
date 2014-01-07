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

from numpy import array, dot, shape
from pyspark import SparkContext
from pyspark.mllib._common import \
    _get_unmangled_rdd, _get_unmangled_double_vector_rdd, \
    _serialize_double_matrix, _deserialize_double_matrix, \
    _serialize_double_vector, _deserialize_double_vector, \
    _get_initial_weights, _serialize_rating, _regression_train_wrapper, \
    LinearModel, _linear_predictor_typecheck
from math import exp, log

class LogisticRegressionModel(LinearModel):
    """A linear binary classification model derived from logistic regression.

    >>> data = array([0.0, 0.0, 1.0, 1.0, 1.0, 2.0, 1.0, 3.0]).reshape(4,2)
    >>> lrm = LogisticRegressionWithSGD.train(sc, sc.parallelize(data))
    >>> lrm.predict(array([1.0])) != None
    True
    """
    def predict(self, x):
        _linear_predictor_typecheck(x, self._coeff)
        margin = dot(x, self._coeff) + self._intercept
        prob = 1/(1 + exp(-margin))
        return 1 if prob > 0.5 else 0

class LogisticRegressionWithSGD(object):
    @classmethod
    def train(cls, sc, data, iterations=100, step=1.0,
              mini_batch_fraction=1.0, initial_weights=None):
        """Train a logistic regression model on the given data."""
        return _regression_train_wrapper(sc, lambda d, i:
                sc._jvm.PythonMLLibAPI().trainLogisticRegressionModelWithSGD(d._jrdd,
                        iterations, step, mini_batch_fraction, i),
                LogisticRegressionModel, data, initial_weights)

class SVMModel(LinearModel):
    """A support vector machine.

    >>> data = array([0.0, 0.0, 1.0, 1.0, 1.0, 2.0, 1.0, 3.0]).reshape(4,2)
    >>> svm = SVMWithSGD.train(sc, sc.parallelize(data))
    >>> svm.predict(array([1.0])) != None
    True
    """
    def predict(self, x):
        _linear_predictor_typecheck(x, self._coeff)
        margin = dot(x, self._coeff) + self._intercept
        return 1 if margin >= 0 else 0

class SVMWithSGD(object):
    @classmethod
    def train(cls, sc, data, iterations=100, step=1.0, reg_param=1.0,
              mini_batch_fraction=1.0, initial_weights=None):
        """Train a support vector machine on the given data."""
        return _regression_train_wrapper(sc, lambda d, i:
                sc._jvm.PythonMLLibAPI().trainSVMModelWithSGD(d._jrdd,
                        iterations, step, reg_param, mini_batch_fraction, i),
                SVMModel, data, initial_weights)

def _test():
    import doctest
    globs = globals().copy()
    globs['sc'] = SparkContext('local[4]', 'PythonTest', batchSize=2)
    (failure_count, test_count) = doctest.testmod(globs=globs,
            optionflags=doctest.ELLIPSIS)
    globs['sc'].stop()
    if failure_count:
        exit(-1)

if __name__ == "__main__":
    _test()
