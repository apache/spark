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

from numpy import array, dot
from pyspark import SparkContext
from pyspark.mllib._common import \
    _get_unmangled_rdd, _get_unmangled_double_vector_rdd, \
    _serialize_double_matrix, _deserialize_double_matrix, \
    _serialize_double_vector, _deserialize_double_vector, \
    _get_initial_weights, _serialize_rating, _regression_train_wrapper, \
    _linear_predictor_typecheck

class LinearModel(object):
    """Something that has a vector of coefficients and an intercept."""
    def __init__(self, coeff, intercept):
        self._coeff = coeff
        self._intercept = intercept

class LinearRegressionModelBase(LinearModel):
    """A linear regression model.

    >>> lrmb = LinearRegressionModelBase(array([1.0, 2.0]), 0.1)
    >>> abs(lrmb.predict(array([-1.03, 7.777])) - 14.624) < 1e-6
    True
    """
    def predict(self, x):
        """Predict the value of the dependent variable given a vector x"""
        """containing values for the independent variables."""
        _linear_predictor_typecheck(x, self._coeff)
        return dot(self._coeff, x) + self._intercept

class LinearRegressionModel(LinearRegressionModelBase):
    """A linear regression model derived from a least-squares fit.

    >>> data = array([0.0, 0.0, 1.0, 1.0, 3.0, 2.0, 2.0, 3.0]).reshape(4,2)
    >>> lrm = LinearRegressionWithSGD.train(sc.parallelize(data), initialWeights=array([1.0]))
    """

class LinearRegressionWithSGD(object):
    @classmethod
    def train(cls, data, iterations=100, step=1.0,
              miniBatchFraction=1.0, initialWeights=None):
        """Train a linear regression model on the given data."""
        sc = data.context
        return _regression_train_wrapper(sc, lambda d, i:
                sc._jvm.PythonMLLibAPI().trainLinearRegressionModelWithSGD(
                        d._jrdd, iterations, step, miniBatchFraction, i),
                LinearRegressionModel, data, initialWeights)

class LassoModel(LinearRegressionModelBase):
    """A linear regression model derived from a least-squares fit with an
    l_1 penalty term.

    >>> data = array([0.0, 0.0, 1.0, 1.0, 3.0, 2.0, 2.0, 3.0]).reshape(4,2)
    >>> lrm = LassoWithSGD.train(sc.parallelize(data), initialWeights=array([1.0]))
    """

class LassoWithSGD(object):
    @classmethod
    def train(cls, data, iterations=100, step=1.0, regParam=1.0,
              miniBatchFraction=1.0, initialWeights=None):
        """Train a Lasso regression model on the given data."""
        sc = data.context
        return _regression_train_wrapper(sc, lambda d, i:
                sc._jvm.PythonMLLibAPI().trainLassoModelWithSGD(d._jrdd,
                        iterations, step, regParam, miniBatchFraction, i),
                LassoModel, data, initialWeights)

class RidgeRegressionModel(LinearRegressionModelBase):
    """A linear regression model derived from a least-squares fit with an
    l_2 penalty term.

    >>> data = array([0.0, 0.0, 1.0, 1.0, 3.0, 2.0, 2.0, 3.0]).reshape(4,2)
    >>> lrm = RidgeRegressionWithSGD.train(sc.parallelize(data), initialWeights=array([1.0]))
    """

class RidgeRegressionWithSGD(object):
    @classmethod
    def train(cls, data, iterations=100, step=1.0, regParam=1.0,
              miniBatchFraction=1.0, initialWeights=None):
        """Train a ridge regression model on the given data."""
        sc = data.context
        return _regression_train_wrapper(sc, lambda d, i:
                sc._jvm.PythonMLLibAPI().trainRidgeModelWithSGD(d._jrdd,
                        iterations, step, regParam, miniBatchFraction, i),
                RidgeRegressionModel, data, initialWeights)

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
