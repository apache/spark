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

from pyspark import RDD
from pyspark.mllib.common import callMLlibFunc
from pyspark.mllib.linalg import SparseVector, _convert_to_vector
from pyspark.mllib.regression import LabeledPoint, LinearModel, _regression_train_wrapper


__all__ = ['LogisticRegressionModel', 'LogisticRegressionWithSGD', 'SVMModel',
           'SVMWithSGD', 'NaiveBayesModel', 'NaiveBayes']


class LinearBinaryClassificationModel(LinearModel):
    """
    Represents a linear binary classification model that predicts to whether an
    example is positive (1.0) or negative (0.0).
    """
    def __init__(self, weights, intercept):
        super(LinearBinaryClassificationModel, self).__init__(weights, intercept)
        self._threshold = None

    def setThreshold(self, value):
        """
        :: Experimental ::

        Sets the threshold that separates positive predictions from negative
        predictions. An example with prediction score greater than or equal
        to this threshold is identified as an positive, and negative otherwise.
        """
        self._threshold = value

    def clearThreshold(self):
        """
        :: Experimental ::

        Clears the threshold so that `predict` will output raw prediction scores.
        """
        self._threshold = None

    def predict(self, test):
        """
        Predict values for a single data point or an RDD of points using
        the model trained.
        """
        raise NotImplementedError


class LogisticRegressionModel(LinearBinaryClassificationModel):

    """A linear binary classification model derived from logistic regression.

    >>> data = [
    ...     LabeledPoint(0.0, [0.0, 1.0]),
    ...     LabeledPoint(1.0, [1.0, 0.0]),
    ... ]
    >>> lrm = LogisticRegressionWithSGD.train(sc.parallelize(data))
    >>> lrm.predict([1.0, 0.0])
    1
    >>> lrm.predict([0.0, 1.0])
    0
    >>> lrm.predict(sc.parallelize([[1.0, 0.0], [0.0, 1.0]])).collect()
    [1, 0]
    >>> lrm.clearThreshold()
    >>> lrm.predict([0.0, 1.0])
    0.123...

    >>> sparse_data = [
    ...     LabeledPoint(0.0, SparseVector(2, {0: 0.0})),
    ...     LabeledPoint(1.0, SparseVector(2, {1: 1.0})),
    ...     LabeledPoint(0.0, SparseVector(2, {0: 1.0})),
    ...     LabeledPoint(1.0, SparseVector(2, {1: 2.0}))
    ... ]
    >>> lrm = LogisticRegressionWithSGD.train(sc.parallelize(sparse_data))
    >>> lrm.predict(array([0.0, 1.0]))
    1
    >>> lrm.predict(array([1.0, 0.0]))
    0
    >>> lrm.predict(SparseVector(2, {1: 1.0}))
    1
    >>> lrm.predict(SparseVector(2, {0: 1.0}))
    0
    """
    def __init__(self, weights, intercept):
        super(LogisticRegressionModel, self).__init__(weights, intercept)
        self._threshold = 0.5

    def predict(self, x):
        """
        Predict values for a single data point or an RDD of points using
        the model trained.
        """
        if isinstance(x, RDD):
            return x.map(lambda v: self.predict(v))

        x = _convert_to_vector(x)
        margin = self.weights.dot(x) + self._intercept
        if margin > 0:
            prob = 1 / (1 + exp(-margin))
        else:
            exp_margin = exp(margin)
            prob = exp_margin / (1 + exp_margin)
        if self._threshold is None:
            return prob
        else:
            return 1 if prob > self._threshold else 0


class LogisticRegressionWithSGD(object):

    @classmethod
    def train(cls, data, iterations=100, step=1.0, miniBatchFraction=1.0,
              initialWeights=None, regParam=0.01, regType="l2", intercept=False):
        """
        Train a logistic regression model on the given data.

        :param data:              The training data, an RDD of LabeledPoint.
        :param iterations:        The number of iterations (default: 100).
        :param step:              The step parameter used in SGD
                                  (default: 1.0).
        :param miniBatchFraction: Fraction of data to be used for each SGD
                                  iteration.
        :param initialWeights:    The initial weights (default: None).
        :param regParam:          The regularizer parameter (default: 0.01).
        :param regType:           The type of regularizer used for training
                                  our model.

                                  :Allowed values:
                                     - "l1" for using L1 regularization
                                     - "l2" for using L2 regularization
                                     - None for no regularization

                                     (default: "l2")

        @param intercept:         Boolean parameter which indicates the use
                                  or not of the augmented representation for
                                  training data (i.e. whether bias features
                                  are activated or not).
        """
        def train(rdd, i):
            return callMLlibFunc("trainLogisticRegressionModelWithSGD", rdd, int(iterations),
                                 float(step), float(miniBatchFraction), i, float(regParam), regType,
                                 bool(intercept))

        return _regression_train_wrapper(train, LogisticRegressionModel, data, initialWeights)


class SVMModel(LinearBinaryClassificationModel):

    """A support vector machine.

    >>> data = [
    ...     LabeledPoint(0.0, [0.0]),
    ...     LabeledPoint(1.0, [1.0]),
    ...     LabeledPoint(1.0, [2.0]),
    ...     LabeledPoint(1.0, [3.0])
    ... ]
    >>> svm = SVMWithSGD.train(sc.parallelize(data))
    >>> svm.predict([1.0])
    1
    >>> svm.predict(sc.parallelize([[1.0]])).collect()
    [1]
    >>> svm.clearThreshold()
    >>> svm.predict(array([1.0]))
    1.25...

    >>> sparse_data = [
    ...     LabeledPoint(0.0, SparseVector(2, {0: -1.0})),
    ...     LabeledPoint(1.0, SparseVector(2, {1: 1.0})),
    ...     LabeledPoint(0.0, SparseVector(2, {0: 0.0})),
    ...     LabeledPoint(1.0, SparseVector(2, {1: 2.0}))
    ... ]
    >>> svm = SVMWithSGD.train(sc.parallelize(sparse_data))
    >>> svm.predict(SparseVector(2, {1: 1.0}))
    1
    >>> svm.predict(SparseVector(2, {0: -1.0}))
    0
    """
    def __init__(self, weights, intercept):
        super(SVMModel, self).__init__(weights, intercept)
        self._threshold = 0.0

    def predict(self, x):
        """
        Predict values for a single data point or an RDD of points using
        the model trained.
        """
        if isinstance(x, RDD):
            return x.map(lambda v: self.predict(v))

        x = _convert_to_vector(x)
        margin = self.weights.dot(x) + self.intercept
        if self._threshold is None:
            return margin
        else:
            return 1 if margin > self._threshold else 0


class SVMWithSGD(object):

    @classmethod
    def train(cls, data, iterations=100, step=1.0, regParam=0.01,
              miniBatchFraction=1.0, initialWeights=None, regType="l2", intercept=False):
        """
        Train a support vector machine on the given data.

        :param data:              The training data, an RDD of LabeledPoint.
        :param iterations:        The number of iterations (default: 100).
        :param step:              The step parameter used in SGD
                                  (default: 1.0).
        :param regParam:          The regularizer parameter (default: 0.01).
        :param miniBatchFraction: Fraction of data to be used for each SGD
                                  iteration.
        :param initialWeights:    The initial weights (default: None).
        :param regType:           The type of regularizer used for training
                                  our model.

                                  :Allowed values:
                                     - "l1" for using L1 regularization
                                     - "l2" for using L2 regularization
                                     - None for no regularization

                                     (default: "l2")

        @param intercept:         Boolean parameter which indicates the use
                                  or not of the augmented representation for
                                  training data (i.e. whether bias features
                                  are activated or not).
        """
        def train(rdd, i):
            return callMLlibFunc("trainSVMModelWithSGD", rdd, int(iterations), float(step),
                                 float(regParam), float(miniBatchFraction), i, regType,
                                 bool(intercept))

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
    >>> model.predict(sc.parallelize([[1.0, 0.0]])).collect()
    [1.0]
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
        """Return the most likely class for a data vector or an RDD of vectors"""
        if isinstance(x, RDD):
            return x.map(lambda v: self.predict(v))
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
    import pyspark.mllib.classification
    globs = pyspark.mllib.classification.__dict__.copy()
    globs['sc'] = SparkContext('local[4]', 'PythonTest', batchSize=2)
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    globs['sc'].stop()
    if failure_count:
        exit(-1)

if __name__ == "__main__":
    _test()
