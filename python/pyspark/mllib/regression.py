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

import numpy as np
from numpy import array

from pyspark import SparkContext
from pyspark.mllib.linalg import SparseVector, _convert_to_vector
from pyspark.serializers import PickleSerializer, AutoBatchedSerializer

__all__ = ['LabeledPoint', 'LinearModel', 'LinearRegressionModel', 'RidgeRegressionModel'
           'LinearRegressionWithSGD', 'LassoWithSGD', 'RidgeRegressionWithSGD']


class LabeledPoint(object):

    """
    The features and labels of a data point.

    @param label: Label for this data point.
    @param features: Vector of features for this point (NumPy array, list,
        pyspark.mllib.linalg.SparseVector, or scipy.sparse column matrix)
    """

    def __init__(self, label, features):
        self.label = label
        self.features = _convert_to_vector(features)

    def __reduce__(self):
        return (LabeledPoint, (self.label, self.features))

    def __str__(self):
        return "(" + ",".join((str(self.label), str(self.features))) + ")"

    def __repr__(self):
        return "LabeledPoint(" + ",".join((repr(self.label), repr(self.features))) + ")"


class LinearModel(object):

    """A linear model that has a vector of coefficients and an intercept."""

    def __init__(self, weights, intercept):
        self._coeff = _convert_to_vector(weights)
        self._intercept = intercept

    @property
    def weights(self):
        return self._coeff

    @property
    def intercept(self):
        return self._intercept


class LinearRegressionModelBase(LinearModel):

    """A linear regression model.

    >>> lrmb = LinearRegressionModelBase(np.array([1.0, 2.0]), 0.1)
    >>> abs(lrmb.predict(np.array([-1.03, 7.777])) - 14.624) < 1e-6
    True
    >>> abs(lrmb.predict(SparseVector(2, {0: -1.03, 1: 7.777})) - 14.624) < 1e-6
    True
    """

    def predict(self, x):
        """
        Predict the value of the dependent variable given a vector x
        containing values for the independent variables.
        """
        return self.weights.dot(x) + self.intercept


class LinearRegressionModel(LinearRegressionModelBase):

    """A linear regression model derived from a least-squares fit.

    >>> from pyspark.mllib.regression import LabeledPoint
    >>> data = [
    ...     LabeledPoint(0.0, [0.0]),
    ...     LabeledPoint(1.0, [1.0]),
    ...     LabeledPoint(3.0, [2.0]),
    ...     LabeledPoint(2.0, [3.0])
    ... ]
    >>> lrm = LinearRegressionWithSGD.train(sc.parallelize(data), initialWeights=np.array([1.0]))
    >>> abs(lrm.predict(np.array([0.0])) - 0) < 0.5
    True
    >>> abs(lrm.predict(np.array([1.0])) - 1) < 0.5
    True
    >>> abs(lrm.predict(SparseVector(1, {0: 1.0})) - 1) < 0.5
    True
    >>> data = [
    ...     LabeledPoint(0.0, SparseVector(1, {0: 0.0})),
    ...     LabeledPoint(1.0, SparseVector(1, {0: 1.0})),
    ...     LabeledPoint(3.0, SparseVector(1, {0: 2.0})),
    ...     LabeledPoint(2.0, SparseVector(1, {0: 3.0}))
    ... ]
    >>> lrm = LinearRegressionWithSGD.train(sc.parallelize(data), initialWeights=array([1.0]))
    >>> abs(lrm.predict(array([0.0])) - 0) < 0.5
    True
    >>> abs(lrm.predict(SparseVector(1, {0: 1.0})) - 1) < 0.5
    True
    """


# train_func should take two parameters, namely data and initial_weights, and
# return the result of a call to the appropriate JVM stub.
# _regression_train_wrapper is responsible for setup and error checking.
def _regression_train_wrapper(sc, train_func, modelClass, data, initial_weights):
    initial_weights = initial_weights or [0.0] * len(data.first().features)
    ser = PickleSerializer()
    initial_bytes = bytearray(ser.dumps(_convert_to_vector(initial_weights)))
    # use AutoBatchedSerializer before cache to reduce the memory
    # overhead in JVM
    cached = data._reserialize(AutoBatchedSerializer(ser)).cache()
    ans = train_func(cached._to_java_object_rdd(), initial_bytes)
    assert len(ans) == 2, "JVM call result had unexpected length"
    weights = ser.loads(str(ans[0]))
    return modelClass(weights, ans[1])


class LinearRegressionWithSGD(object):

    @classmethod
    def train(cls, data, iterations=100, step=1.0, miniBatchFraction=1.0,
              initialWeights=None, regParam=1.0, regType="none", intercept=False):
        """
        Train a linear regression model on the given data.

        @param data:              The training data.
        @param iterations:        The number of iterations (default: 100).
        @param step:              The step parameter used in SGD
                                  (default: 1.0).
        @param miniBatchFraction: Fraction of data to be used for each SGD
                                  iteration.
        @param initialWeights:    The initial weights (default: None).
        @param regParam:          The regularizer parameter (default: 1.0).
        @param regType:           The type of regularizer used for training
                                  our model.
                                  Allowed values: "l1" for using L1Updater,
                                                  "l2" for using
                                                       SquaredL2Updater,
                                                  "none" for no regularizer.
                                  (default: "none")
        @param intercept:         Boolean parameter which indicates the use
                                  or not of the augmented representation for
                                  training data (i.e. whether bias features
                                  are activated or not).
        """
        sc = data.context

        def train(jrdd, i):
            return sc._jvm.PythonMLLibAPI().trainLinearRegressionModelWithSGD(
                jrdd, iterations, step, miniBatchFraction, i, regParam, regType, intercept)

        return _regression_train_wrapper(sc, train, LinearRegressionModel, data, initialWeights)


class LassoModel(LinearRegressionModelBase):

    """A linear regression model derived from a least-squares fit with an
    l_1 penalty term.

    >>> from pyspark.mllib.regression import LabeledPoint
    >>> data = [
    ...     LabeledPoint(0.0, [0.0]),
    ...     LabeledPoint(1.0, [1.0]),
    ...     LabeledPoint(3.0, [2.0]),
    ...     LabeledPoint(2.0, [3.0])
    ... ]
    >>> lrm = LassoWithSGD.train(sc.parallelize(data), initialWeights=array([1.0]))
    >>> abs(lrm.predict(np.array([0.0])) - 0) < 0.5
    True
    >>> abs(lrm.predict(np.array([1.0])) - 1) < 0.5
    True
    >>> abs(lrm.predict(SparseVector(1, {0: 1.0})) - 1) < 0.5
    True
    >>> data = [
    ...     LabeledPoint(0.0, SparseVector(1, {0: 0.0})),
    ...     LabeledPoint(1.0, SparseVector(1, {0: 1.0})),
    ...     LabeledPoint(3.0, SparseVector(1, {0: 2.0})),
    ...     LabeledPoint(2.0, SparseVector(1, {0: 3.0}))
    ... ]
    >>> lrm = LinearRegressionWithSGD.train(sc.parallelize(data), initialWeights=array([1.0]))
    >>> abs(lrm.predict(np.array([0.0])) - 0) < 0.5
    True
    >>> abs(lrm.predict(SparseVector(1, {0: 1.0})) - 1) < 0.5
    True
    """


class LassoWithSGD(object):

    @classmethod
    def train(cls, data, iterations=100, step=1.0, regParam=1.0,
              miniBatchFraction=1.0, initialWeights=None):
        """Train a Lasso regression model on the given data."""
        sc = data.context

        def train(jrdd, i):
            return sc._jvm.PythonMLLibAPI().trainLassoModelWithSGD(
                jrdd, iterations, step, regParam, miniBatchFraction, i)
        return _regression_train_wrapper(sc, train, LassoModel, data, initialWeights)


class RidgeRegressionModel(LinearRegressionModelBase):

    """A linear regression model derived from a least-squares fit with an
    l_2 penalty term.

    >>> from pyspark.mllib.regression import LabeledPoint
    >>> data = [
    ...     LabeledPoint(0.0, [0.0]),
    ...     LabeledPoint(1.0, [1.0]),
    ...     LabeledPoint(3.0, [2.0]),
    ...     LabeledPoint(2.0, [3.0])
    ... ]
    >>> lrm = RidgeRegressionWithSGD.train(sc.parallelize(data), initialWeights=array([1.0]))
    >>> abs(lrm.predict(np.array([0.0])) - 0) < 0.5
    True
    >>> abs(lrm.predict(np.array([1.0])) - 1) < 0.5
    True
    >>> abs(lrm.predict(SparseVector(1, {0: 1.0})) - 1) < 0.5
    True
    >>> data = [
    ...     LabeledPoint(0.0, SparseVector(1, {0: 0.0})),
    ...     LabeledPoint(1.0, SparseVector(1, {0: 1.0})),
    ...     LabeledPoint(3.0, SparseVector(1, {0: 2.0})),
    ...     LabeledPoint(2.0, SparseVector(1, {0: 3.0}))
    ... ]
    >>> lrm = LinearRegressionWithSGD.train(sc.parallelize(data), initialWeights=array([1.0]))
    >>> abs(lrm.predict(np.array([0.0])) - 0) < 0.5
    True
    >>> abs(lrm.predict(SparseVector(1, {0: 1.0})) - 1) < 0.5
    True
    """


class RidgeRegressionWithSGD(object):

    @classmethod
    def train(cls, data, iterations=100, step=1.0, regParam=1.0,
              miniBatchFraction=1.0, initialWeights=None):
        """Train a ridge regression model on the given data."""
        sc = data.context

        def train(jrdd, i):
            return sc._jvm.PythonMLLibAPI().trainRidgeModelWithSGD(
                jrdd, iterations, step, regParam, miniBatchFraction, i)

        return _regression_train_wrapper(sc, train, RidgeRegressionModel, data, initialWeights)


def _test():
    import doctest
    globs = globals().copy()
    globs['sc'] = SparkContext('local[4]', 'PythonTest', batchSize=2)
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    globs['sc'].stop()
    if failure_count:
        exit(-1)

if __name__ == "__main__":
    _test()
