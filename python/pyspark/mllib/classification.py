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

from math import exp

import numpy
from numpy import array

from pyspark.mllib.common import callMLlibFunc
from pyspark.mllib.linalg import SparseVector, _convert_to_vector
from pyspark.mllib.regression import LabeledPoint, LinearModel, _regression_train_wrapper


__all__ = ['LogisticRegressionModel', 'LogisticRegressionWithSGD', 'SVMModel',
           'SVMWithSGD', 'NaiveBayesModel', 'NaiveBayes']


class LogisticRegressionModel(LinearModel):

    """A linear binary classification model derived from logistic regression.

    >>> data = [
    ...     LabeledPoint(0.0, [0.0]),
    ...     LabeledPoint(1.0, [1.0]),
    ...     LabeledPoint(1.0, [2.0]),
    ...     LabeledPoint(1.0, [3.0])
    ... ]
    >>> lrm = LogisticRegressionWithSGD.train(sc.parallelize(data))
    >>> lrm.predict(array([1.0])) > 0
    True
    >>> lrm.predict(array([0.0])) <= 0
    True
    >>> sparse_data = [
    ...     LabeledPoint(0.0, SparseVector(2, {0: 0.0})),
    ...     LabeledPoint(1.0, SparseVector(2, {1: 1.0})),
    ...     LabeledPoint(0.0, SparseVector(2, {0: 0.0})),
    ...     LabeledPoint(1.0, SparseVector(2, {1: 2.0}))
    ... ]
    >>> lrm = LogisticRegressionWithSGD.train(sc.parallelize(sparse_data))
    >>> lrm.predict(array([0.0, 1.0])) > 0
    True
    >>> lrm.predict(array([0.0, 0.0])) <= 0
    True
    >>> lrm.predict(SparseVector(2, {1: 1.0})) > 0
    True
    >>> lrm.predict(SparseVector(2, {1: 0.0})) <= 0
    True
    """

    def predict(self, x):
        x = _convert_to_vector(x)
        margin = self.weights.dot(x) + self._intercept
        if margin > 0:
            prob = 1 / (1 + exp(-margin))
        else:
            exp_margin = exp(margin)
            prob = exp_margin / (1 + exp_margin)
        return 1 if prob > 0.5 else 0


class LogisticRegressionWithSGD(object):

    @classmethod
    def train(cls, data, iterations=100, step=1.0, miniBatchFraction=1.0,
              initialWeights=None, regParam=1.0, regType="none", intercept=False):
        """
        Train a logistic regression model on the given data.

        :param data:              The training data, an RDD of LabeledPoint.
        :param iterations:        The number of iterations (default: 100).
        :param step:              The step parameter used in SGD
                                  (default: 1.0).
        :param miniBatchFraction: Fraction of data to be used for each SGD
                                  iteration.
        :param initialWeights:    The initial weights (default: None).
        :param regParam:          The regularizer parameter (default: 1.0).
        :param regType:           The type of regularizer used for training
                                  our model.

                                  :Allowed values:
                                     - "l1" for using L1Updater
                                     - "l2" for using SquaredL2Updater
                                     - "none" for no regularizer

                                     (default: "none")

        @param intercept:         Boolean parameter which indicates the use
                                  or not of the augmented representation for
                                  training data (i.e. whether bias features
                                  are activated or not).
        """
        def train(rdd, i):
            return callMLlibFunc("trainLogisticRegressionModelWithSGD", rdd, iterations, step,
                                 miniBatchFraction, i, regParam, regType, intercept)

        return _regression_train_wrapper(train, LogisticRegressionModel, data, initialWeights)


class SVMModel(LinearModel):

    """A support vector machine.

    >>> data = [
    ...     LabeledPoint(0.0, [0.0]),
    ...     LabeledPoint(1.0, [1.0]),
    ...     LabeledPoint(1.0, [2.0]),
    ...     LabeledPoint(1.0, [3.0])
    ... ]
    >>> svm = SVMWithSGD.train(sc.parallelize(data))
    >>> svm.predict(array([1.0])) > 0
    True
    >>> sparse_data = [
    ...     LabeledPoint(0.0, SparseVector(2, {0: -1.0})),
    ...     LabeledPoint(1.0, SparseVector(2, {1: 1.0})),
    ...     LabeledPoint(0.0, SparseVector(2, {0: 0.0})),
    ...     LabeledPoint(1.0, SparseVector(2, {1: 2.0}))
    ... ]
    >>> svm = SVMWithSGD.train(sc.parallelize(sparse_data))
    >>> svm.predict(SparseVector(2, {1: 1.0})) > 0
    True
    >>> svm.predict(SparseVector(2, {0: -1.0})) <= 0
    True
    """

    def predict(self, x):
        x = _convert_to_vector(x)
        margin = self.weights.dot(x) + self.intercept
        return 1 if margin >= 0 else 0


class SVMWithSGD(object):

    @classmethod
    def train(cls, data, iterations=100, step=1.0, regParam=1.0,
              miniBatchFraction=1.0, initialWeights=None, regType="none", intercept=False):
        """
        Train a support vector machine on the given data.

        :param data:              The training data, an RDD of LabeledPoint.
        :param iterations:        The number of iterations (default: 100).
        :param step:              The step parameter used in SGD
                                  (default: 1.0).
        :param regParam:          The regularizer parameter (default: 1.0).
        :param miniBatchFraction: Fraction of data to be used for each SGD
                                  iteration.
        :param initialWeights:    The initial weights (default: None).
        :param regType:           The type of regularizer used for training
                                  our model.

                                  :Allowed values:
                                     - "l1" for using L1Updater
                                     - "l2" for using SquaredL2Updater,
                                     - "none" for no regularizer.

                                     (default: "none")

        @param intercept:         Boolean parameter which indicates the use
                                  or not of the augmented representation for
                                  training data (i.e. whether bias features
                                  are activated or not).
        """
        def train(rdd, i):
            return callMLlibFunc("trainSVMModelWithSGD", rdd, iterations, step, regParam,
                                 miniBatchFraction, i, regType, intercept)

        return _regression_train_wrapper(train, SVMModel, data, initialWeights)


class NaiveBayesModel(object):

    """
    Model for Naive Bayes classifiers.

    Contains two parameters:
    - pi: vector of logs of class priors (dimension C)
    - theta: matrix of logs of class conditional probabilities (CxD)

    >>> data = [
    ...     LabeledPoint(0.0, [0.0, 0.0]),
    ...     LabeledPoint(0.0, [0.0, 1.0]),
    ...     LabeledPoint(1.0, [1.0, 0.0]),
    ... ]
    >>> model = NaiveBayes.train(sc.parallelize(data))
    >>> model.predict(array([0.0, 1.0]))
    0.0
    >>> model.predict(array([1.0, 0.0]))
    1.0
    >>> sparse_data = [
    ...     LabeledPoint(0.0, SparseVector(2, {1: 0.0})),
    ...     LabeledPoint(0.0, SparseVector(2, {1: 1.0})),
    ...     LabeledPoint(1.0, SparseVector(2, {0: 1.0}))
    ... ]
    >>> model = NaiveBayes.train(sc.parallelize(sparse_data))
    >>> model.predict(SparseVector(2, {1: 1.0}))
    0.0
    >>> model.predict(SparseVector(2, {0: 1.0}))
    1.0
    """

    def __init__(self, labels, pi, theta):
        self.labels = labels
        self.pi = pi
        self.theta = theta

    def predict(self, x):
        """Return the most likely class for a data vector x"""
        x = _convert_to_vector(x)
        return self.labels[numpy.argmax(self.pi + x.dot(self.theta.transpose()))]


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

        :param data: RDD of LabeledPoint.
        :param lambda_: The smoothing parameter
        """
        first = data.first()
        if not isinstance(first, LabeledPoint):
            raise ValueError("`data` should be an RDD of LabeledPoint")
        labels, pi, theta = callMLlibFunc("trainNaiveBayes", data, lambda_)
        return NaiveBayesModel(labels.toArray(), pi.toArray(), numpy.array(theta))


def _test():
    import doctest
    from pyspark import SparkContext
    globs = globals().copy()
    globs['sc'] = SparkContext('local[4]', 'PythonTest', batchSize=2)
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    globs['sc'].stop()
    if failure_count:
        exit(-1)

if __name__ == "__main__":
    _test()
