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
import warnings

import numpy
from numpy import array

from pyspark import RDD, since
from pyspark.streaming import DStream
from pyspark.mllib.common import callMLlibFunc, _py2java, _java2py
from pyspark.mllib.linalg import DenseVector, SparseVector, _convert_to_vector
from pyspark.mllib.regression import (
    LabeledPoint, LinearModel, _regression_train_wrapper,
    StreamingLinearAlgorithm)
from pyspark.mllib.util import Saveable, Loader, inherit_doc


__all__ = ['LogisticRegressionModel', 'LogisticRegressionWithSGD', 'LogisticRegressionWithLBFGS',
           'SVMModel', 'SVMWithSGD', 'NaiveBayesModel', 'NaiveBayes',
           'StreamingLogisticRegressionWithSGD']


class LinearClassificationModel(LinearModel):
    """
    A private abstract class representing a multiclass classification
    model. The categories are represented by int values: 0, 1, 2, etc.
    """
    def __init__(self, weights, intercept):
        super(LinearClassificationModel, self).__init__(weights, intercept)
        self._threshold = None

    @since('1.4.0')
    def setThreshold(self, value):
        """
        .. note:: Experimental

        Sets the threshold that separates positive predictions from
        negative predictions. An example with prediction score greater
        than or equal to this threshold is identified as an positive,
        and negative otherwise. It is used for binary classification
        only.
        """
        self._threshold = value

    @property
    @since('1.4.0')
    def threshold(self):
        """
        .. note:: Experimental

        Returns the threshold (if any) used for converting raw
        prediction scores into 0/1 predictions. It is used for
        binary classification only.
        """
        return self._threshold

    @since('1.4.0')
    def clearThreshold(self):
        """
        .. note:: Experimental

        Clears the threshold so that `predict` will output raw
        prediction scores. It is used for binary classification only.
        """
        self._threshold = None

    @since('1.4.0')
    def predict(self, test):
        """
        Predict values for a single data point or an RDD of points
        using the model trained.
        """
        raise NotImplementedError


class LogisticRegressionModel(LinearClassificationModel):

    """
    Classification model trained using Multinomial/Binary Logistic
    Regression.

    :param weights:
      Weights computed for every feature.
    :param intercept:
      Intercept computed for this model. (Only used in Binary Logistic
      Regression. In Multinomial Logistic Regression, the intercepts will
      not bea single value, so the intercepts will be part of the
      weights.)
    :param numFeatures:
      The dimension of the features.
    :param numClasses:
      The number of possible outcomes for k classes classification problem
      in Multinomial Logistic Regression. By default, it is binary
      logistic regression so numClasses will be set to 2.

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
    >>> from shutil import rmtree
    >>> try:
    ...    rmtree(path)
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

    .. versionadded:: 0.9.0
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
    @since('1.4.0')
    def numFeatures(self):
        """
        Dimension of the features.
        """
        return self._numFeatures

    @property
    @since('1.4.0')
    def numClasses(self):
        """
        Number of possible outcomes for k classes classification problem
        in Multinomial Logistic Regression.
        """
        return self._numClasses

    @since('0.9.0')
    def predict(self, x):
        """
        Predict values for a single data point or an RDD of points
        using the model trained.
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

    @since('1.4.0')
    def save(self, sc, path):
        """
        Save this model to the given path.
        """
        java_model = sc._jvm.org.apache.spark.mllib.classification.LogisticRegressionModel(
            _py2java(sc, self._coeff), self.intercept, self.numFeatures, self.numClasses)
        java_model.save(sc._jsc.sc(), path)

    @classmethod
    @since('1.4.0')
    def load(cls, sc, path):
        """
        Load a model from the given path.
        """
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
    """
    .. versionadded:: 0.9.0
    .. note:: Deprecated in 2.0.0. Use ml.classification.LogisticRegression or
            LogisticRegressionWithLBFGS.
    """
    @classmethod
    @since('0.9.0')
    def train(cls, data, iterations=100, step=1.0, miniBatchFraction=1.0,
              initialWeights=None, regParam=0.01, regType="l2", intercept=False,
              validateData=True, convergenceTol=0.001):
        """
        Train a logistic regression model on the given data.

        :param data:
          The training data, an RDD of LabeledPoint.
        :param iterations:
          The number of iterations.
          (default: 100)
        :param step:
          The step parameter used in SGD.
          (default: 1.0)
        :param miniBatchFraction:
          Fraction of data to be used for each SGD iteration.
          (default: 1.0)
        :param initialWeights:
          The initial weights.
          (default: None)
        :param regParam:
          The regularizer parameter.
          (default: 0.01)
        :param regType:
          The type of regularizer used for training our model.
          Supported values:

            - "l1" for using L1 regularization
            - "l2" for using L2 regularization (default)
            - None for no regularization
        :param intercept:
          Boolean parameter which indicates the use or not of the
          augmented representation for training data (i.e., whether bias
          features are activated or not).
          (default: False)
        :param validateData:
          Boolean parameter which indicates if the algorithm should
          validate data before training.
          (default: True)
        :param convergenceTol:
          A condition which decides iteration termination.
          (default: 0.001)
        """
        warnings.warn(
            "Deprecated in 2.0.0. Use ml.classification.LogisticRegression or "
            "LogisticRegressionWithLBFGS.")

        def train(rdd, i):
            return callMLlibFunc("trainLogisticRegressionModelWithSGD", rdd, int(iterations),
                                 float(step), float(miniBatchFraction), i, float(regParam), regType,
                                 bool(intercept), bool(validateData), float(convergenceTol))

        return _regression_train_wrapper(train, LogisticRegressionModel, data, initialWeights)


class LogisticRegressionWithLBFGS(object):
    """
    .. versionadded:: 1.2.0
    """
    @classmethod
    @since('1.2.0')
    def train(cls, data, iterations=100, initialWeights=None, regParam=0.0, regType="l2",
              intercept=False, corrections=10, tolerance=1e-6, validateData=True, numClasses=2):
        """
        Train a logistic regression model on the given data.

        :param data:
          The training data, an RDD of LabeledPoint.
        :param iterations:
          The number of iterations.
          (default: 100)
        :param initialWeights:
          The initial weights.
          (default: None)
        :param regParam:
          The regularizer parameter.
          (default: 0.0)
        :param regType:
          The type of regularizer used for training our model.
          Supported values:

            - "l1" for using L1 regularization
            - "l2" for using L2 regularization (default)
            - None for no regularization
        :param intercept:
          Boolean parameter which indicates the use or not of the
          augmented representation for training data (i.e., whether bias
          features are activated or not).
          (default: False)
        :param corrections:
          The number of corrections used in the LBFGS update.
          If a known updater is used for binary classification,
          it calls the ml implementation and this parameter will
          have no effect. (default: 10)
        :param tolerance:
          The convergence tolerance of iterations for L-BFGS.
          (default: 1e-6)
        :param validateData:
          Boolean parameter which indicates if the algorithm should
          validate data before training.
          (default: True)
        :param numClasses:
          The number of classes (i.e., outcomes) a label can take in
          Multinomial Logistic Regression.
          (default: 2)

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

    """
    Model for Support Vector Machines (SVMs).

    :param weights:
      Weights computed for every feature.
    :param intercept:
      Intercept computed for this model.

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
    >>> from shutil import rmtree
    >>> try:
    ...    rmtree(path)
    ... except:
    ...    pass

    .. versionadded:: 0.9.0
    """
    def __init__(self, weights, intercept):
        super(SVMModel, self).__init__(weights, intercept)
        self._threshold = 0.0

    @since('0.9.0')
    def predict(self, x):
        """
        Predict values for a single data point or an RDD of points
        using the model trained.
        """
        if isinstance(x, RDD):
            return x.map(lambda v: self.predict(v))

        x = _convert_to_vector(x)
        margin = self.weights.dot(x) + self.intercept
        if self._threshold is None:
            return margin
        else:
            return 1 if margin > self._threshold else 0

    @since('1.4.0')
    def save(self, sc, path):
        """
        Save this model to the given path.
        """
        java_model = sc._jvm.org.apache.spark.mllib.classification.SVMModel(
            _py2java(sc, self._coeff), self.intercept)
        java_model.save(sc._jsc.sc(), path)

    @classmethod
    @since('1.4.0')
    def load(cls, sc, path):
        """
        Load a model from the given path.
        """
        java_model = sc._jvm.org.apache.spark.mllib.classification.SVMModel.load(
            sc._jsc.sc(), path)
        weights = _java2py(sc, java_model.weights())
        intercept = java_model.intercept()
        threshold = java_model.getThreshold().get()
        model = SVMModel(weights, intercept)
        model.setThreshold(threshold)
        return model


class SVMWithSGD(object):
    """
    .. versionadded:: 0.9.0
    """

    @classmethod
    @since('0.9.0')
    def train(cls, data, iterations=100, step=1.0, regParam=0.01,
              miniBatchFraction=1.0, initialWeights=None, regType="l2",
              intercept=False, validateData=True, convergenceTol=0.001):
        """
        Train a support vector machine on the given data.

        :param data:
          The training data, an RDD of LabeledPoint.
        :param iterations:
          The number of iterations.
          (default: 100)
        :param step:
          The step parameter used in SGD.
          (default: 1.0)
        :param regParam:
          The regularizer parameter.
          (default: 0.01)
        :param miniBatchFraction:
          Fraction of data to be used for each SGD iteration.
          (default: 1.0)
        :param initialWeights:
          The initial weights.
          (default: None)
        :param regType:
          The type of regularizer used for training our model.
          Allowed values:

            - "l1" for using L1 regularization
            - "l2" for using L2 regularization (default)
            - None for no regularization
        :param intercept:
          Boolean parameter which indicates the use or not of the
          augmented representation for training data (i.e. whether bias
          features are activated or not).
          (default: False)
        :param validateData:
          Boolean parameter which indicates if the algorithm should
          validate data before training.
          (default: True)
        :param convergenceTol:
          A condition which decides iteration termination.
          (default: 0.001)
        """
        def train(rdd, i):
            return callMLlibFunc("trainSVMModelWithSGD", rdd, int(iterations), float(step),
                                 float(regParam), float(miniBatchFraction), i, regType,
                                 bool(intercept), bool(validateData), float(convergenceTol))

        return _regression_train_wrapper(train, SVMModel, data, initialWeights)


@inherit_doc
class NaiveBayesModel(Saveable, Loader):

    """
    Model for Naive Bayes classifiers.

    :param labels:
      List of labels.
    :param pi:
      Log of class priors, whose dimension is C, number of labels.
    :param theta:
      Log of class conditional probabilities, whose dimension is C-by-D,
      where D is number of features.

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
    >>> from shutil import rmtree
    >>> try:
    ...     rmtree(path)
    ... except OSError:
    ...     pass

    .. versionadded:: 0.9.0
    """
    def __init__(self, labels, pi, theta):
        self.labels = labels
        self.pi = pi
        self.theta = theta

    @since('0.9.0')
    def predict(self, x):
        """
        Return the most likely class for a data vector
        or an RDD of vectors
        """
        if isinstance(x, RDD):
            return x.map(lambda v: self.predict(v))
        x = _convert_to_vector(x)
        return self.labels[numpy.argmax(self.pi + x.dot(self.theta.transpose()))]

    def save(self, sc, path):
        """
        Save this model to the given path.
        """
        java_labels = _py2java(sc, self.labels.tolist())
        java_pi = _py2java(sc, self.pi.tolist())
        java_theta = _py2java(sc, self.theta.tolist())
        java_model = sc._jvm.org.apache.spark.mllib.classification.NaiveBayesModel(
            java_labels, java_pi, java_theta)
        java_model.save(sc._jsc.sc(), path)

    @classmethod
    @since('1.4.0')
    def load(cls, sc, path):
        """
        Load a model from the given path.
        """
        java_model = sc._jvm.org.apache.spark.mllib.classification.NaiveBayesModel.load(
            sc._jsc.sc(), path)
        # Can not unpickle array.array from Pyrolite in Python3 with "bytes"
        py_labels = _java2py(sc, java_model.labels(), "latin1")
        py_pi = _java2py(sc, java_model.pi(), "latin1")
        py_theta = _java2py(sc, java_model.theta(), "latin1")
        return NaiveBayesModel(py_labels, py_pi, numpy.array(py_theta))


class NaiveBayes(object):
    """
    .. versionadded:: 0.9.0
    """

    @classmethod
    @since('0.9.0')
    def train(cls, data, lambda_=1.0):
        """
        Train a Naive Bayes model given an RDD of (label, features)
        vectors.

        This is the Multinomial NB (U{http://tinyurl.com/lsdw6p}) which
        can handle all kinds of discrete data.  For example, by
        converting documents into TF-IDF vectors, it can be used for
        document classification. By making every vector a 0-1 vector,
        it can also be used as Bernoulli NB (U{http://tinyurl.com/p7c96j6}).
        The input feature values must be nonnegative.

        :param data:
          RDD of LabeledPoint.
        :param lambda_:
          The smoothing parameter.
          (default: 1.0)
        """
        first = data.first()
        if not isinstance(first, LabeledPoint):
            raise ValueError("`data` should be an RDD of LabeledPoint")
        labels, pi, theta = callMLlibFunc("trainNaiveBayesModel", data, lambda_)
        return NaiveBayesModel(labels.toArray(), pi.toArray(), numpy.array(theta))


@inherit_doc
class StreamingLogisticRegressionWithSGD(StreamingLinearAlgorithm):
    """
    Train or predict a logistic regression model on streaming data.
    Training uses Stochastic Gradient Descent to update the model based on
    each new batch of incoming data from a DStream.

    Each batch of data is assumed to be an RDD of LabeledPoints.
    The number of data points per batch can vary, but the number
    of features must be constant. An initial weight
    vector must be provided.

    :param stepSize:
      Step size for each iteration of gradient descent.
      (default: 0.1)
    :param numIterations:
      Number of iterations run for each batch of data.
      (default: 50)
    :param miniBatchFraction:
      Fraction of each batch of data to use for updates.
      (default: 1.0)
    :param regParam:
      L2 Regularization parameter.
      (default: 0.0)
    :param convergenceTol:
      Value used to determine when to terminate iterations.
      (default: 0.001)

    .. versionadded:: 1.5.0
    """
    def __init__(self, stepSize=0.1, numIterations=50, miniBatchFraction=1.0, regParam=0.0,
                 convergenceTol=0.001):
        self.stepSize = stepSize
        self.numIterations = numIterations
        self.regParam = regParam
        self.miniBatchFraction = miniBatchFraction
        self.convergenceTol = convergenceTol
        self._model = None
        super(StreamingLogisticRegressionWithSGD, self).__init__(
            model=self._model)

    @since('1.5.0')
    def setInitialWeights(self, initialWeights):
        """
        Set the initial value of weights.

        This must be set before running trainOn and predictOn.
        """
        initialWeights = _convert_to_vector(initialWeights)

        # LogisticRegressionWithSGD does only binary classification.
        self._model = LogisticRegressionModel(
            initialWeights, 0, initialWeights.size, 2)
        return self

    @since('1.5.0')
    def trainOn(self, dstream):
        """Train the model on the incoming dstream."""
        self._validate(dstream)

        def update(rdd):
            # LogisticRegressionWithSGD.train raises an error for an empty RDD.
            if not rdd.isEmpty():
                self._model = LogisticRegressionWithSGD.train(
                    rdd, self.numIterations, self.stepSize,
                    self.miniBatchFraction, self._model.weights,
                    regParam=self.regParam, convergenceTol=self.convergenceTol)

        dstream.foreachRDD(update)


def _test():
    import doctest
    from pyspark.sql import SparkSession
    import pyspark.mllib.classification
    globs = pyspark.mllib.classification.__dict__.copy()
    spark = SparkSession.builder\
        .master("local[4]")\
        .appName("mllib.classification tests")\
        .getOrCreate()
    globs['sc'] = spark.sparkContext
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    spark.stop()
    if failure_count:
        exit(-1)

if __name__ == "__main__":
    _test()
