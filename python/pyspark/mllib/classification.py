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
from pyspark.mllib.common import callMLlibFunc, _py2java, _java2py
from pyspark.mllib.linalg import DenseVector, SparseVector, _convert_to_vector
from pyspark.mllib.regression import LabeledPoint, LinearModel, _regression_train_wrapper
from pyspark.mllib.util import Saveable, Loader, inherit_doc


__all__ = ['LogisticRegressionModel', 'LogisticRegressionWithSGD', 'LogisticRegressionWithLBFGS',
           'SVMModel', 'SVMWithSGD', 'NaiveBayesModel', 'NaiveBayes']


class LinearClassificationModel(LinearModel):
    """
    A private abstract class representing a multiclass classification model.
    The categories are represented by int values: 0, 1, 2, etc.
    """
    def __init__(self, weights, intercept):
        super(LinearClassificationModel, self).__init__(weights, intercept)
        self._threshold = None

    def setThreshold(self, value):
        """
        .. note:: Experimental

        Sets the threshold that separates positive predictions from negative
        predictions. An example with prediction score greater than or equal
        to this threshold is identified as an positive, and negative otherwise.
        It is used for binary classification only.
        """
        self._threshold = value

    @property
    def threshold(self):
        """
        .. note:: Experimental

        Returns the threshold (if any) used for converting raw prediction scores
        into 0/1 predictions. It is used for binary classification only.
        """
        return self._threshold

    def clearThreshold(self):
        """
        .. note:: Experimental

        Clears the threshold so that `predict` will output raw prediction scores.
        It is used for binary classification only.
        """
        self._threshold = None

    def predict(self, test):
        """
        Predict values for a single data point or an RDD of points using
        the model trained.
        """
        raise NotImplementedError


class LogisticRegressionModel(LinearClassificationModel):

    """A linear binary classification model derived from logistic regression.

    >>> data = [
    ...     LabeledPoint(0.0, [0.0, 1.0]),
    ...     LabeledPoint(1.0, [1.0, 0.0]),
    ... ]
    >>> lrm = LogisticRegressionWithSGD.train(sc.parallelize(data), iterations=10)
    >>> lrm.predict([1.0, 0.0])
    1
    >>> lrm.predict([0.0, 1.0])
    0
    >>> lrm.predict(sc.parallelize([[1.0, 0.0], [0.0, 1.0]])).collect()
    [1, 0]
    >>> lrm.clearThreshold()
    >>> lrm.predict([0.0, 1.0])
    0.279...

    >>> sparse_data = [
    ...     LabeledPoint(0.0, SparseVector(2, {0: 0.0})),
    ...     LabeledPoint(1.0, SparseVector(2, {1: 1.0})),
    ...     LabeledPoint(0.0, SparseVector(2, {0: 1.0})),
    ...     LabeledPoint(1.0, SparseVector(2, {1: 2.0}))
    ... ]
    >>> lrm = LogisticRegressionWithSGD.train(sc.parallelize(sparse_data), iterations=10)
    >>> lrm.predict(array([0.0, 1.0]))
    1
    >>> lrm.predict(array([1.0, 0.0]))
    0
    >>> lrm.predict(SparseVector(2, {1: 1.0}))
    1
    >>> lrm.predict(SparseVector(2, {0: 1.0}))
    0
    >>> import os, tempfile
    >>> path = tempfile.mkdtemp()
    >>> lrm.save(sc, path)
    >>> sameModel = LogisticRegressionModel.load(sc, path)
    >>> sameModel.predict(array([0.0, 1.0]))
    1
    >>> sameModel.predict(SparseVector(2, {0: 1.0}))
    0
    >>> try:
    ...    os.removedirs(path)
    ... except:
    ...    pass
    >>> multi_class_data = [
    ...     LabeledPoint(0.0, [0.0, 1.0, 0.0]),
    ...     LabeledPoint(1.0, [1.0, 0.0, 0.0]),
    ...     LabeledPoint(2.0, [0.0, 0.0, 1.0])
    ... ]
    >>> data = sc.parallelize(multi_class_data)
    >>> mcm = LogisticRegressionWithLBFGS.train(data, iterations=10, numClasses=3)
    >>> mcm.predict([0.0, 0.5, 0.0])
    0
    >>> mcm.predict([0.8, 0.0, 0.0])
    1
    >>> mcm.predict([0.0, 0.0, 0.3])
    2
    """
    def __init__(self, weights, intercept, numFeatures, numClasses):
        super(LogisticRegressionModel, self).__init__(weights, intercept)
        self._numFeatures = int(numFeatures)
        self._numClasses = int(numClasses)
        self._threshold = 0.5
        if self._numClasses == 2:
            self._dataWithBiasSize = None
            self._weightsMatrix = None
        else:
            self._dataWithBiasSize = self._coeff.size / (self._numClasses - 1)
            self._weightsMatrix = self._coeff.toArray().reshape(self._numClasses - 1,
                                                                self._dataWithBiasSize)

    @property
    def numFeatures(self):
        return self._numFeatures

    @property
    def numClasses(self):
        return self._numClasses

    def predict(self, x):
        """
        Predict values for a single data point or an RDD of points using
        the model trained.
        """
        if isinstance(x, RDD):
            return x.map(lambda v: self.predict(v))

        x = _convert_to_vector(x)
        if self.numClasses == 2:
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
        else:
            best_class = 0
            max_margin = 0.0
            if x.size + 1 == self._dataWithBiasSize:
                for i in range(0, self._numClasses - 1):
                    margin = x.dot(self._weightsMatrix[i][0:x.size]) + \
                        self._weightsMatrix[i][x.size]
                    if margin > max_margin:
                        max_margin = margin
                        best_class = i + 1
            else:
                for i in range(0, self._numClasses - 1):
                    margin = x.dot(self._weightsMatrix[i])
                    if margin > max_margin:
                        max_margin = margin
                        best_class = i + 1
            return best_class

    def save(self, sc, path):
        java_model = sc._jvm.org.apache.spark.mllib.classification.LogisticRegressionModel(
            _py2java(sc, self._coeff), self.intercept, self.numFeatures, self.numClasses)
        java_model.save(sc._jsc.sc(), path)

    @classmethod
    def load(cls, sc, path):
        java_model = sc._jvm.org.apache.spark.mllib.classification.LogisticRegressionModel.load(
            sc._jsc.sc(), path)
        weights = _java2py(sc, java_model.weights())
        intercept = java_model.intercept()
        numFeatures = java_model.numFeatures()
        numClasses = java_model.numClasses()
        threshold = java_model.getThreshold().get()
        model = LogisticRegressionModel(weights, intercept, numFeatures, numClasses)
        model.setThreshold(threshold)
        return model


class LogisticRegressionWithSGD(object):

    @classmethod
    def train(cls, data, iterations=100, step=1.0, miniBatchFraction=1.0,
              initialWeights=None, regParam=0.01, regType="l2", intercept=False,
              validateData=True):
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

        :param intercept:         Boolean parameter which indicates the use
                                  or not of the augmented representation for
                                  training data (i.e. whether bias features
                                  are activated or not).
        :param validateData:      Boolean parameter which indicates if the
                                  algorithm should validate data before training.
                                  (default: True)
        """
        def train(rdd, i):
            return callMLlibFunc("trainLogisticRegressionModelWithSGD", rdd, int(iterations),
                                 float(step), float(miniBatchFraction), i, float(regParam), regType,
                                 bool(intercept), bool(validateData))

        return _regression_train_wrapper(train, LogisticRegressionModel, data, initialWeights)


class LogisticRegressionWithLBFGS(object):

    @classmethod
    def train(cls, data, iterations=100, initialWeights=None, regParam=0.01, regType="l2",
              intercept=False, corrections=10, tolerance=1e-4, validateData=True, numClasses=2):
        """
        Train a logistic regression model on the given data.

        :param data:           The training data, an RDD of LabeledPoint.
        :param iterations:     The number of iterations (default: 100).
        :param initialWeights: The initial weights (default: None).
        :param regParam:       The regularizer parameter (default: 0.01).
        :param regType:        The type of regularizer used for training
                               our model.

                               :Allowed values:
                                 - "l1" for using L1 regularization
                                 - "l2" for using L2 regularization
                                 - None for no regularization

                                 (default: "l2")

        :param intercept:      Boolean parameter which indicates the use
                               or not of the augmented representation for
                               training data (i.e. whether bias features
                               are activated or not).
        :param corrections:    The number of corrections used in the LBFGS
                               update (default: 10).
        :param tolerance:      The convergence tolerance of iterations for
                               L-BFGS (default: 1e-4).
        :param validateData:   Boolean parameter which indicates if the
                               algorithm should validate data before training.
                               (default: True)
        :param numClasses:     The number of classes (i.e., outcomes) a label can take
                               in Multinomial Logistic Regression (default: 2).

        >>> data = [
        ...     LabeledPoint(0.0, [0.0, 1.0]),
        ...     LabeledPoint(1.0, [1.0, 0.0]),
        ... ]
        >>> lrm = LogisticRegressionWithLBFGS.train(sc.parallelize(data), iterations=10)
        >>> lrm.predict([1.0, 0.0])
        1
        >>> lrm.predict([0.0, 1.0])
        0
        """
        def train(rdd, i):
            return callMLlibFunc("trainLogisticRegressionModelWithLBFGS", rdd, int(iterations), i,
                                 float(regParam), regType, bool(intercept), int(corrections),
                                 float(tolerance), bool(validateData), int(numClasses))

        if initialWeights is None:
            if numClasses == 2:
                initialWeights = [0.0] * len(data.first().features)
            else:
                if intercept:
                    initialWeights = [0.0] * (len(data.first().features) + 1) * (numClasses - 1)
                else:
                    initialWeights = [0.0] * len(data.first().features) * (numClasses - 1)
        return _regression_train_wrapper(train, LogisticRegressionModel, data, initialWeights)


class SVMModel(LinearClassificationModel):

    """A support vector machine.

    >>> data = [
    ...     LabeledPoint(0.0, [0.0]),
    ...     LabeledPoint(1.0, [1.0]),
    ...     LabeledPoint(1.0, [2.0]),
    ...     LabeledPoint(1.0, [3.0])
    ... ]
    >>> svm = SVMWithSGD.train(sc.parallelize(data), iterations=10)
    >>> svm.predict([1.0])
    1
    >>> svm.predict(sc.parallelize([[1.0]])).collect()
    [1]
    >>> svm.clearThreshold()
    >>> svm.predict(array([1.0]))
    1.44...

    >>> sparse_data = [
    ...     LabeledPoint(0.0, SparseVector(2, {0: -1.0})),
    ...     LabeledPoint(1.0, SparseVector(2, {1: 1.0})),
    ...     LabeledPoint(0.0, SparseVector(2, {0: 0.0})),
    ...     LabeledPoint(1.0, SparseVector(2, {1: 2.0}))
    ... ]
    >>> svm = SVMWithSGD.train(sc.parallelize(sparse_data), iterations=10)
    >>> svm.predict(SparseVector(2, {1: 1.0}))
    1
    >>> svm.predict(SparseVector(2, {0: -1.0}))
    0
    >>> import os, tempfile
    >>> path = tempfile.mkdtemp()
    >>> svm.save(sc, path)
    >>> sameModel = SVMModel.load(sc, path)
    >>> sameModel.predict(SparseVector(2, {1: 1.0}))
    1
    >>> sameModel.predict(SparseVector(2, {0: -1.0}))
    0
    >>> try:
    ...    os.removedirs(path)
    ... except:
    ...    pass
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

    def save(self, sc, path):
        java_model = sc._jvm.org.apache.spark.mllib.classification.SVMModel(
            _py2java(sc, self._coeff), self.intercept)
        java_model.save(sc._jsc.sc(), path)

    @classmethod
    def load(cls, sc, path):
        java_model = sc._jvm.org.apache.spark.mllib.classification.SVMModel.load(
            sc._jsc.sc(), path)
        weights = _java2py(sc, java_model.weights())
        intercept = java_model.intercept()
        threshold = java_model.getThreshold().get()
        model = SVMModel(weights, intercept)
        model.setThreshold(threshold)
        return model


class SVMWithSGD(object):

    @classmethod
    def train(cls, data, iterations=100, step=1.0, regParam=0.01,
              miniBatchFraction=1.0, initialWeights=None, regType="l2",
              intercept=False, validateData=True):
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

        :param intercept:         Boolean parameter which indicates the use
                                  or not of the augmented representation for
                                  training data (i.e. whether bias features
                                  are activated or not).
        :param validateData:      Boolean parameter which indicates if the
                                  algorithm should validate data before training.
                                  (default: True)
        """
        def train(rdd, i):
            return callMLlibFunc("trainSVMModelWithSGD", rdd, int(iterations), float(step),
                                 float(regParam), float(miniBatchFraction), i, regType,
                                 bool(intercept), bool(validateData))

        return _regression_train_wrapper(train, SVMModel, data, initialWeights)


@inherit_doc
class NaiveBayesModel(Saveable, Loader):

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
    >>> import os, tempfile
    >>> path = tempfile.mkdtemp()
    >>> model.save(sc, path)
    >>> sameModel = NaiveBayesModel.load(sc, path)
    >>> sameModel.predict(SparseVector(2, {0: 1.0})) == model.predict(SparseVector(2, {0: 1.0}))
    True
    >>> try:
    ...     os.removedirs(path)
    ... except OSError:
    ...     pass
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

    def save(self, sc, path):
        java_labels = _py2java(sc, self.labels.tolist())
        java_pi = _py2java(sc, self.pi.tolist())
        java_theta = _py2java(sc, self.theta.tolist())
        java_model = sc._jvm.org.apache.spark.mllib.classification.NaiveBayesModel(
            java_labels, java_pi, java_theta)
        java_model.save(sc._jsc.sc(), path)

    @classmethod
    def load(cls, sc, path):
        java_model = sc._jvm.org.apache.spark.mllib.classification.NaiveBayesModel.load(
            sc._jsc.sc(), path)
        # Can not unpickle array.array from Pyrolite in Python3 with "bytes"
        py_labels = _java2py(sc, java_model.labels(), "latin1")
        py_pi = _java2py(sc, java_model.pi(), "latin1")
        py_theta = _java2py(sc, java_model.theta(), "latin1")
        return NaiveBayesModel(py_labels, py_pi, numpy.array(py_theta))


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
