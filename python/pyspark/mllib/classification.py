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

import numpy

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
    >>> lrm = LogisticRegressionWithSGD.train(sc.parallelize(data))
    >>> lrm.predict(array([1.0])) > 0
    True
    """
    def predict(self, x):
        _linear_predictor_typecheck(x, self._coeff)
        margin = dot(x, self._coeff) + self._intercept
        prob = 1/(1 + exp(-margin))
        return 1 if prob > 0.5 else 0

class LogisticRegressionWithSGD(object):
    @classmethod
    def train(cls, data, iterations=100, step=1.0,
              miniBatchFraction=1.0, initialWeights=None):
        """Train a logistic regression model on the given data."""
        sc = data.context
        return _regression_train_wrapper(sc, lambda d, i:
                sc._jvm.PythonMLLibAPI().trainLogisticRegressionModelWithSGD(d._jrdd,
                        iterations, step, miniBatchFraction, i),
                LogisticRegressionModel, data, initialWeights)

class SVMModel(LinearModel):
    """A support vector machine.

    >>> data = array([0.0, 0.0, 1.0, 1.0, 1.0, 2.0, 1.0, 3.0]).reshape(4,2)
    >>> svm = SVMWithSGD.train(sc.parallelize(data))
    >>> svm.predict(array([1.0])) > 0
    True
    """
    def predict(self, x):
        _linear_predictor_typecheck(x, self._coeff)
        margin = dot(x, self._coeff) + self._intercept
        return 1 if margin >= 0 else 0

class SVMWithSGD(object):
    @classmethod
    def train(cls, data, iterations=100, step=1.0, regParam=1.0,
              miniBatchFraction=1.0, initialWeights=None):
        """Train a support vector machine on the given data."""
        sc = data.context
        return _regression_train_wrapper(sc, lambda d, i:
                sc._jvm.PythonMLLibAPI().trainSVMModelWithSGD(d._jrdd,
                        iterations, step, regParam, miniBatchFraction, i),
                SVMModel, data, initialWeights)

class NaiveBayesModel(object):
    """
    Model for Naive Bayes classifiers.

    Contains two parameters:
    - pi: vector of logs of class priors (dimension C)
    - theta: matrix of logs of class conditional probabilities (CxD)

    >>> data = array([0.0, 0.0, 1.0, 0.0, 0.0, 2.0, 1.0, 1.0, 0.0]).reshape(3,3)
    >>> model = NaiveBayes.train(sc.parallelize(data))
    >>> model.predict(array([0.0, 1.0]))
    0
    >>> model.predict(array([1.0, 0.0]))
    1
    """

    def __init__(self, pi, theta):
        self.pi = pi
        self.theta = theta

    def predict(self, x):
        """Return the most likely class for a data vector x"""
        return numpy.argmax(self.pi + dot(x, self.theta))

class NaiveBayes(object):
    @classmethod
    def train(cls, data, lambda_=1.0):
        """
        Train a Naive Bayes model given an RDD of (label, features) vectors.

        This is the Multinomial NB (U{http://tinyurl.com/lsdw6p}) which can
        handle all kinds of discrete data.  For example, by converting
        documents into TF-IDF vectors, it can be used for document
        classification.  By making every vector a 0-1 vector, it can also be
        used as Bernoulli NB (U{http://tinyurl.com/p7c96j6}).

        @param data: RDD of NumPy vectors, one per element, where the first
               coordinate is the label and the rest is the feature vector
               (e.g. a count vector).
        @param lambda_: The smoothing parameter
        """
        sc = data.context
        dataBytes = _get_unmangled_double_vector_rdd(data)
        ans = sc._jvm.PythonMLLibAPI().trainNaiveBayes(dataBytes._jrdd, lambda_)
        return NaiveBayesModel(
            _deserialize_double_vector(ans[0]),
            _deserialize_double_matrix(ans[1]))


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
