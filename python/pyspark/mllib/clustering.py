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

import sys
import array as pyarray
import warnings

if sys.version > '3':
    xrange = range
    basestring = str

from math import exp, log

from numpy import array, random, tile

from collections import namedtuple

from pyspark import SparkContext, since
from pyspark.rdd import RDD, ignore_unicode_prefix
from pyspark.mllib.common import JavaModelWrapper, callMLlibFunc, callJavaFunc, _py2java, _java2py
from pyspark.mllib.linalg import SparseVector, _convert_to_vector, DenseVector
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.stat.distribution import MultivariateGaussian
from pyspark.mllib.util import Saveable, Loader, inherit_doc, JavaLoader, JavaSaveable
from pyspark.streaming import DStream

__all__ = ['BisectingKMeansModel', 'BisectingKMeans', 'KMeansModel', 'KMeans',
           'GaussianMixtureModel', 'GaussianMixture', 'PowerIterationClusteringModel',
           'PowerIterationClustering', 'StreamingKMeans', 'StreamingKMeansModel',
           'LDA', 'LDAModel']


@inherit_doc
class BisectingKMeansModel(JavaModelWrapper):
    """
    A clustering model derived from the bisecting k-means method.

    >>> data = array([0.0,0.0, 1.0,1.0, 9.0,8.0, 8.0,9.0]).reshape(4, 2)
    >>> bskm = BisectingKMeans()
    >>> model = bskm.train(sc.parallelize(data, 2), k=4)
    >>> p = array([0.0, 0.0])
    >>> model.predict(p)
    0
    >>> model.k
    4
    >>> model.computeCost(p)
    0.0

    .. versionadded:: 2.0.0
    """

    def __init__(self, java_model):
        super(BisectingKMeansModel, self).__init__(java_model)
        self.centers = [c.toArray() for c in self.call("clusterCenters")]

    @property
    @since('2.0.0')
    def clusterCenters(self):
        """Get the cluster centers, represented as a list of NumPy
        arrays."""
        return self.centers

    @property
    @since('2.0.0')
    def k(self):
        """Get the number of clusters"""
        return self.call("k")

    @since('2.0.0')
    def predict(self, x):
        """
        Find the cluster that each of the points belongs to in this
        model.

        :param x:
          A data point (or RDD of points) to determine cluster index.
        :return:
          Predicted cluster index or an RDD of predicted cluster indices
          if the input is an RDD.
        """
        if isinstance(x, RDD):
            vecs = x.map(_convert_to_vector)
            return self.call("predict", vecs)

        x = _convert_to_vector(x)
        return self.call("predict", x)

    @since('2.0.0')
    def computeCost(self, x):
        """
        Return the Bisecting K-means cost (sum of squared distances of
        points to their nearest center) for this model on the given
        data. If provided with an RDD of points returns the sum.

        :param point:
          A data point (or RDD of points) to compute the cost(s).
        """
        if isinstance(x, RDD):
            vecs = x.map(_convert_to_vector)
            return self.call("computeCost", vecs)

        return self.call("computeCost", _convert_to_vector(x))


class BisectingKMeans(object):
    """
    A bisecting k-means algorithm based on the paper "A comparison of
    document clustering techniques" by Steinbach, Karypis, and Kumar,
    with modification to fit Spark.
    The algorithm starts from a single cluster that contains all points.
    Iteratively it finds divisible clusters on the bottom level and
    bisects each of them using k-means, until there are `k` leaf
    clusters in total or no leaf clusters are divisible.
    The bisecting steps of clusters on the same level are grouped
    together to increase parallelism. If bisecting all divisible
    clusters on the bottom level would result more than `k` leaf
    clusters, larger clusters get higher priority.

    Based on
    U{http://glaros.dtc.umn.edu/gkhome/fetch/papers/docclusterKDDTMW00.pdf}
    Steinbach, Karypis, and Kumar, A comparison of document clustering
    techniques, KDD Workshop on Text Mining, 2000.

    .. versionadded:: 2.0.0
    """

    @classmethod
    @since('2.0.0')
    def train(self, rdd, k=4, maxIterations=20, minDivisibleClusterSize=1.0, seed=-1888008604):
        """
        Runs the bisecting k-means algorithm return the model.

        :param rdd:
          Training points as an `RDD` of `Vector` or convertible
          sequence types.
        :param k:
          The desired number of leaf clusters. The actual number could
          be smaller if there are no divisible leaf clusters.
          (default: 4)
        :param maxIterations:
          Maximum number of iterations allowed to split clusters.
          (default: 20)
        :param minDivisibleClusterSize:
          Minimum number of points (if >= 1.0) or the minimum proportion
          of points (if < 1.0) of a divisible cluster.
          (default: 1)
        :param seed:
          Random seed value for cluster initialization.
          (default: -1888008604 from classOf[BisectingKMeans].getName.##)
        """
        java_model = callMLlibFunc(
            "trainBisectingKMeans", rdd.map(_convert_to_vector),
            k, maxIterations, minDivisibleClusterSize, seed)
        return BisectingKMeansModel(java_model)


@inherit_doc
class KMeansModel(Saveable, Loader):

    """A clustering model derived from the k-means method.

    >>> data = array([0.0,0.0, 1.0,1.0, 9.0,8.0, 8.0,9.0]).reshape(4, 2)
    >>> model = KMeans.train(
    ...     sc.parallelize(data), 2, maxIterations=10, initializationMode="random",
    ...                    seed=50, initializationSteps=5, epsilon=1e-4)
    >>> model.predict(array([0.0, 0.0])) == model.predict(array([1.0, 1.0]))
    True
    >>> model.predict(array([8.0, 9.0])) == model.predict(array([9.0, 8.0]))
    True
    >>> model.k
    2
    >>> model.computeCost(sc.parallelize(data))
    2.0000000000000004
    >>> model = KMeans.train(sc.parallelize(data), 2)
    >>> sparse_data = [
    ...     SparseVector(3, {1: 1.0}),
    ...     SparseVector(3, {1: 1.1}),
    ...     SparseVector(3, {2: 1.0}),
    ...     SparseVector(3, {2: 1.1})
    ... ]
    >>> model = KMeans.train(sc.parallelize(sparse_data), 2, initializationMode="k-means||",
    ...                                     seed=50, initializationSteps=5, epsilon=1e-4)
    >>> model.predict(array([0., 1., 0.])) == model.predict(array([0, 1.1, 0.]))
    True
    >>> model.predict(array([0., 0., 1.])) == model.predict(array([0, 0, 1.1]))
    True
    >>> model.predict(sparse_data[0]) == model.predict(sparse_data[1])
    True
    >>> model.predict(sparse_data[2]) == model.predict(sparse_data[3])
    True
    >>> isinstance(model.clusterCenters, list)
    True
    >>> import os, tempfile
    >>> path = tempfile.mkdtemp()
    >>> model.save(sc, path)
    >>> sameModel = KMeansModel.load(sc, path)
    >>> sameModel.predict(sparse_data[0]) == model.predict(sparse_data[0])
    True
    >>> from shutil import rmtree
    >>> try:
    ...     rmtree(path)
    ... except OSError:
    ...     pass

    >>> data = array([-383.1,-382.9, 28.7,31.2, 366.2,367.3]).reshape(3, 2)
    >>> model = KMeans.train(sc.parallelize(data), 3, maxIterations=0,
    ...     initialModel = KMeansModel([(-1000.0,-1000.0),(5.0,5.0),(1000.0,1000.0)]))
    >>> model.clusterCenters
    [array([-1000., -1000.]), array([ 5.,  5.]), array([ 1000.,  1000.])]

    .. versionadded:: 0.9.0
    """

    def __init__(self, centers):
        self.centers = centers

    @property
    @since('1.0.0')
    def clusterCenters(self):
        """Get the cluster centers, represented as a list of NumPy arrays."""
        return self.centers

    @property
    @since('1.4.0')
    def k(self):
        """Total number of clusters."""
        return len(self.centers)

    @since('0.9.0')
    def predict(self, x):
        """
        Find the cluster that each of the points belongs to in this
        model.

        :param x:
          A data point (or RDD of points) to determine cluster index.
        :return:
          Predicted cluster index or an RDD of predicted cluster indices
          if the input is an RDD.
        """
        best = 0
        best_distance = float("inf")
        if isinstance(x, RDD):
            return x.map(self.predict)

        x = _convert_to_vector(x)
        for i in xrange(len(self.centers)):
            distance = x.squared_distance(self.centers[i])
            if distance < best_distance:
                best = i
                best_distance = distance
        return best

    @since('1.4.0')
    def computeCost(self, rdd):
        """
        Return the K-means cost (sum of squared distances of points to
        their nearest center) for this model on the given
        data.

        :param rdd:
          The RDD of points to compute the cost on.
        """
        cost = callMLlibFunc("computeCostKmeansModel", rdd.map(_convert_to_vector),
                             [_convert_to_vector(c) for c in self.centers])
        return cost

    @since('1.4.0')
    def save(self, sc, path):
        """
        Save this model to the given path.
        """
        java_centers = _py2java(sc, [_convert_to_vector(c) for c in self.centers])
        java_model = sc._jvm.org.apache.spark.mllib.clustering.KMeansModel(java_centers)
        java_model.save(sc._jsc.sc(), path)

    @classmethod
    @since('1.4.0')
    def load(cls, sc, path):
        """
        Load a model from the given path.
        """
        java_model = sc._jvm.org.apache.spark.mllib.clustering.KMeansModel.load(sc._jsc.sc(), path)
        return KMeansModel(_java2py(sc, java_model.clusterCenters()))


class KMeans(object):
    """
    .. versionadded:: 0.9.0
    """

    @classmethod
    @since('0.9.0')
    def train(cls, rdd, k, maxIterations=100, runs=1, initializationMode="k-means||",
              seed=None, initializationSteps=2, epsilon=1e-4, initialModel=None):
        """
        Train a k-means clustering model.

        :param rdd:
          Training points as an `RDD` of `Vector` or convertible
          sequence types.
        :param k:
          Number of clusters to create.
        :param maxIterations:
          Maximum number of iterations allowed.
          (default: 100)
        :param runs:
          This param has no effect since Spark 2.0.0.
        :param initializationMode:
          The initialization algorithm. This can be either "random" or
          "k-means||".
          (default: "k-means||")
        :param seed:
          Random seed value for cluster initialization. Set as None to
          generate seed based on system time.
          (default: None)
        :param initializationSteps:
          Number of steps for the k-means|| initialization mode.
          This is an advanced setting -- the default of 2 is almost
          always enough.
          (default: 2)
        :param epsilon:
          Distance threshold within which a center will be considered to
          have converged. If all centers move less than this Euclidean
          distance, iterations are stopped.
          (default: 1e-4)
        :param initialModel:
          Initial cluster centers can be provided as a KMeansModel object
          rather than using the random or k-means|| initializationModel.
          (default: None)
        """
        if runs != 1:
            warnings.warn("The param `runs` has no effect since Spark 2.0.0.")
        clusterInitialModel = []
        if initialModel is not None:
            if not isinstance(initialModel, KMeansModel):
                raise Exception("initialModel is of "+str(type(initialModel))+". It needs "
                                "to be of <type 'KMeansModel'>")
            clusterInitialModel = [_convert_to_vector(c) for c in initialModel.clusterCenters]
        model = callMLlibFunc("trainKMeansModel", rdd.map(_convert_to_vector), k, maxIterations,
                              runs, initializationMode, seed, initializationSteps, epsilon,
                              clusterInitialModel)
        centers = callJavaFunc(rdd.context, model.clusterCenters)
        return KMeansModel([c.toArray() for c in centers])


@inherit_doc
class GaussianMixtureModel(JavaModelWrapper, JavaSaveable, JavaLoader):

    """
    A clustering model derived from the Gaussian Mixture Model method.

    >>> from pyspark.mllib.linalg import Vectors, DenseMatrix
    >>> from numpy.testing import assert_equal
    >>> from shutil import rmtree
    >>> import os, tempfile

    >>> clusterdata_1 =  sc.parallelize(array([-0.1,-0.05,-0.01,-0.1,
    ...                                         0.9,0.8,0.75,0.935,
    ...                                        -0.83,-0.68,-0.91,-0.76 ]).reshape(6, 2), 2)
    >>> model = GaussianMixture.train(clusterdata_1, 3, convergenceTol=0.0001,
    ...                                 maxIterations=50, seed=10)
    >>> labels = model.predict(clusterdata_1).collect()
    >>> labels[0]==labels[1]
    False
    >>> labels[1]==labels[2]
    False
    >>> labels[4]==labels[5]
    True
    >>> model.predict([-0.1,-0.05])
    0
    >>> softPredicted = model.predictSoft([-0.1,-0.05])
    >>> abs(softPredicted[0] - 1.0) < 0.001
    True
    >>> abs(softPredicted[1] - 0.0) < 0.001
    True
    >>> abs(softPredicted[2] - 0.0) < 0.001
    True

    >>> path = tempfile.mkdtemp()
    >>> model.save(sc, path)
    >>> sameModel = GaussianMixtureModel.load(sc, path)
    >>> assert_equal(model.weights, sameModel.weights)
    >>> mus, sigmas = list(
    ...     zip(*[(g.mu, g.sigma) for g in model.gaussians]))
    >>> sameMus, sameSigmas = list(
    ...     zip(*[(g.mu, g.sigma) for g in sameModel.gaussians]))
    >>> mus == sameMus
    True
    >>> sigmas == sameSigmas
    True
    >>> from shutil import rmtree
    >>> try:
    ...     rmtree(path)
    ... except OSError:
    ...     pass

    >>> data =  array([-5.1971, -2.5359, -3.8220,
    ...                -5.2211, -5.0602,  4.7118,
    ...                 6.8989, 3.4592,  4.6322,
    ...                 5.7048,  4.6567, 5.5026,
    ...                 4.5605,  5.2043,  6.2734])
    >>> clusterdata_2 = sc.parallelize(data.reshape(5,3))
    >>> model = GaussianMixture.train(clusterdata_2, 2, convergenceTol=0.0001,
    ...                               maxIterations=150, seed=4)
    >>> labels = model.predict(clusterdata_2).collect()
    >>> labels[0]==labels[1]
    True
    >>> labels[2]==labels[3]==labels[4]
    True

    .. versionadded:: 1.3.0
    """

    @property
    @since('1.4.0')
    def weights(self):
        """
        Weights for each Gaussian distribution in the mixture, where weights[i] is
        the weight for Gaussian i, and weights.sum == 1.
        """
        return array(self.call("weights"))

    @property
    @since('1.4.0')
    def gaussians(self):
        """
        Array of MultivariateGaussian where gaussians[i] represents
        the Multivariate Gaussian (Normal) Distribution for Gaussian i.
        """
        return [
            MultivariateGaussian(gaussian[0], gaussian[1])
            for gaussian in self.call("gaussians")]

    @property
    @since('1.4.0')
    def k(self):
        """Number of gaussians in mixture."""
        return len(self.weights)

    @since('1.3.0')
    def predict(self, x):
        """
        Find the cluster to which the point 'x' or each point in RDD 'x'
        has maximum membership in this model.

        :param x:
          A feature vector or an RDD of vectors representing data points.
        :return:
          Predicted cluster label or an RDD of predicted cluster labels
          if the input is an RDD.
        """
        if isinstance(x, RDD):
            cluster_labels = self.predictSoft(x).map(lambda z: z.index(max(z)))
            return cluster_labels
        else:
            z = self.predictSoft(x)
            return z.argmax()

    @since('1.3.0')
    def predictSoft(self, x):
        """
        Find the membership of point 'x' or each point in RDD 'x' to all mixture components.

        :param x:
          A feature vector or an RDD of vectors representing data points.
        :return:
          The membership value to all mixture components for vector 'x'
          or each vector in RDD 'x'.
        """
        if isinstance(x, RDD):
            means, sigmas = zip(*[(g.mu, g.sigma) for g in self.gaussians])
            membership_matrix = callMLlibFunc("predictSoftGMM", x.map(_convert_to_vector),
                                              _convert_to_vector(self.weights), means, sigmas)
            return membership_matrix.map(lambda x: pyarray.array('d', x))
        else:
            return self.call("predictSoft", _convert_to_vector(x)).toArray()

    @classmethod
    @since('1.5.0')
    def load(cls, sc, path):
        """Load the GaussianMixtureModel from disk.

        :param sc:
          SparkContext.
        :param path:
          Path to where the model is stored.
        """
        model = cls._load_java(sc, path)
        wrapper = sc._jvm.org.apache.spark.mllib.api.python.GaussianMixtureModelWrapper(model)
        return cls(wrapper)


class GaussianMixture(object):
    """
    Learning algorithm for Gaussian Mixtures using the expectation-maximization algorithm.

    .. versionadded:: 1.3.0
    """
    @classmethod
    @since('1.3.0')
    def train(cls, rdd, k, convergenceTol=1e-3, maxIterations=100, seed=None, initialModel=None):
        """
        Train a Gaussian Mixture clustering model.

        :param rdd:
          Training points as an `RDD` of `Vector` or convertible
          sequence types.
        :param k:
          Number of independent Gaussians in the mixture model.
        :param convergenceTol:
          Maximum change in log-likelihood at which convergence is
          considered to have occurred.
          (default: 1e-3)
        :param maxIterations:
          Maximum number of iterations allowed.
          (default: 100)
        :param seed:
          Random seed for initial Gaussian distribution. Set as None to
          generate seed based on system time.
          (default: None)
        :param initialModel:
          Initial GMM starting point, bypassing the random
          initialization.
          (default: None)
        """
        initialModelWeights = None
        initialModelMu = None
        initialModelSigma = None
        if initialModel is not None:
            if initialModel.k != k:
                raise Exception("Mismatched cluster count, initialModel.k = %s, however k = %s"
                                % (initialModel.k, k))
            initialModelWeights = list(initialModel.weights)
            initialModelMu = [initialModel.gaussians[i].mu for i in range(initialModel.k)]
            initialModelSigma = [initialModel.gaussians[i].sigma for i in range(initialModel.k)]
        java_model = callMLlibFunc("trainGaussianMixtureModel", rdd.map(_convert_to_vector),
                                   k, convergenceTol, maxIterations, seed,
                                   initialModelWeights, initialModelMu, initialModelSigma)
        return GaussianMixtureModel(java_model)


class PowerIterationClusteringModel(JavaModelWrapper, JavaSaveable, JavaLoader):

    """
    Model produced by [[PowerIterationClustering]].

    >>> import math
    >>> def genCircle(r, n):
    ...     points = []
    ...     for i in range(0, n):
    ...         theta = 2.0 * math.pi * i / n
    ...         points.append((r * math.cos(theta), r * math.sin(theta)))
    ...     return points
    >>> def sim(x, y):
    ...     dist2 = (x[0] - y[0]) * (x[0] - y[0]) + (x[1] - y[1]) * (x[1] - y[1])
    ...     return math.exp(-dist2 / 2.0)
    >>> r1 = 1.0
    >>> n1 = 10
    >>> r2 = 4.0
    >>> n2 = 40
    >>> n = n1 + n2
    >>> points = genCircle(r1, n1) + genCircle(r2, n2)
    >>> similarities = [(i, j, sim(points[i], points[j])) for i in range(1, n) for j in range(0, i)]
    >>> rdd = sc.parallelize(similarities, 2)
    >>> model = PowerIterationClustering.train(rdd, 2, 40)
    >>> model.k
    2
    >>> result = sorted(model.assignments().collect(), key=lambda x: x.id)
    >>> result[0].cluster == result[1].cluster == result[2].cluster == result[3].cluster
    True
    >>> result[4].cluster == result[5].cluster == result[6].cluster == result[7].cluster
    True
    >>> import os, tempfile
    >>> path = tempfile.mkdtemp()
    >>> model.save(sc, path)
    >>> sameModel = PowerIterationClusteringModel.load(sc, path)
    >>> sameModel.k
    2
    >>> result = sorted(model.assignments().collect(), key=lambda x: x.id)
    >>> result[0].cluster == result[1].cluster == result[2].cluster == result[3].cluster
    True
    >>> result[4].cluster == result[5].cluster == result[6].cluster == result[7].cluster
    True
    >>> from shutil import rmtree
    >>> try:
    ...     rmtree(path)
    ... except OSError:
    ...     pass

    .. versionadded:: 1.5.0
    """

    @property
    @since('1.5.0')
    def k(self):
        """
        Returns the number of clusters.
        """
        return self.call("k")

    @since('1.5.0')
    def assignments(self):
        """
        Returns the cluster assignments of this model.
        """
        return self.call("getAssignments").map(
            lambda x: (PowerIterationClustering.Assignment(*x)))

    @classmethod
    @since('1.5.0')
    def load(cls, sc, path):
        """
        Load a model from the given path.
        """
        model = cls._load_java(sc, path)
        wrapper =\
            sc._jvm.org.apache.spark.mllib.api.python.PowerIterationClusteringModelWrapper(model)
        return PowerIterationClusteringModel(wrapper)


class PowerIterationClustering(object):
    """
    Power Iteration Clustering (PIC), a scalable graph clustering algorithm
    developed by [[http://www.cs.cmu.edu/~frank/papers/icml2010-pic-final.pdf Lin and Cohen]].
    From the abstract: PIC finds a very low-dimensional embedding of a
    dataset using truncated power iteration on a normalized pair-wise
    similarity matrix of the data.

    .. versionadded:: 1.5.0
    """

    @classmethod
    @since('1.5.0')
    def train(cls, rdd, k, maxIterations=100, initMode="random"):
        """
        :param rdd:
          An RDD of (i, j, s\ :sub:`ij`\) tuples representing the
          affinity matrix, which is the matrix A in the PIC paper.  The
          similarity s\ :sub:`ij`\ must be nonnegative.  This is a symmetric
          matrix and hence s\ :sub:`ij`\ = s\ :sub:`ji`\  For any (i, j) with
          nonzero similarity, there should be either (i, j, s\ :sub:`ij`\) or
          (j, i, s\ :sub:`ji`\) in the input.  Tuples with i = j are ignored,
          because it is assumed s\ :sub:`ij`\ = 0.0.
        :param k:
          Number of clusters.
        :param maxIterations:
          Maximum number of iterations of the PIC algorithm.
          (default: 100)
        :param initMode:
          Initialization mode. This can be either "random" to use
          a random vector as vertex properties, or "degree" to use
          normalized sum similarities.
          (default: "random")
        """
        model = callMLlibFunc("trainPowerIterationClusteringModel",
                              rdd.map(_convert_to_vector), int(k), int(maxIterations), initMode)
        return PowerIterationClusteringModel(model)

    class Assignment(namedtuple("Assignment", ["id", "cluster"])):
        """
        Represents an (id, cluster) tuple.

        .. versionadded:: 1.5.0
        """


class StreamingKMeansModel(KMeansModel):
    """
    Clustering model which can perform an online update of the centroids.

    The update formula for each centroid is given by

    * c_t+1 = ((c_t * n_t * a) + (x_t * m_t)) / (n_t + m_t)
    * n_t+1 = n_t * a + m_t

    where

    * c_t: Centroid at the n_th iteration.
    * n_t: Number of samples (or) weights associated with the centroid
           at the n_th iteration.
    * x_t: Centroid of the new data closest to c_t.
    * m_t: Number of samples (or) weights of the new data closest to c_t
    * c_t+1: New centroid.
    * n_t+1: New number of weights.
    * a: Decay Factor, which gives the forgetfulness.

    .. note:: If a is set to 1, it is the weighted mean of the previous
        and new data. If it set to zero, the old centroids are completely
        forgotten.

    :param clusterCenters:
      Initial cluster centers.
    :param clusterWeights:
      List of weights assigned to each cluster.

    >>> initCenters = [[0.0, 0.0], [1.0, 1.0]]
    >>> initWeights = [1.0, 1.0]
    >>> stkm = StreamingKMeansModel(initCenters, initWeights)
    >>> data = sc.parallelize([[-0.1, -0.1], [0.1, 0.1],
    ...                        [0.9, 0.9], [1.1, 1.1]])
    >>> stkm = stkm.update(data, 1.0, u"batches")
    >>> stkm.centers
    array([[ 0.,  0.],
           [ 1.,  1.]])
    >>> stkm.predict([-0.1, -0.1])
    0
    >>> stkm.predict([0.9, 0.9])
    1
    >>> stkm.clusterWeights
    [3.0, 3.0]
    >>> decayFactor = 0.0
    >>> data = sc.parallelize([DenseVector([1.5, 1.5]), DenseVector([0.2, 0.2])])
    >>> stkm = stkm.update(data, 0.0, u"batches")
    >>> stkm.centers
    array([[ 0.2,  0.2],
           [ 1.5,  1.5]])
    >>> stkm.clusterWeights
    [1.0, 1.0]
    >>> stkm.predict([0.2, 0.2])
    0
    >>> stkm.predict([1.5, 1.5])
    1

    .. versionadded:: 1.5.0
    """
    def __init__(self, clusterCenters, clusterWeights):
        super(StreamingKMeansModel, self).__init__(centers=clusterCenters)
        self._clusterWeights = list(clusterWeights)

    @property
    @since('1.5.0')
    def clusterWeights(self):
        """Return the cluster weights."""
        return self._clusterWeights

    @ignore_unicode_prefix
    @since('1.5.0')
    def update(self, data, decayFactor, timeUnit):
        """Update the centroids, according to data

        :param data:
          RDD with new data for the model update.
        :param decayFactor:
          Forgetfulness of the previous centroids.
        :param timeUnit:
          Can be "batches" or "points". If points, then the decay factor
          is raised to the power of number of new points and if batches,
          then decay factor will be used as is.
        """
        if not isinstance(data, RDD):
            raise TypeError("Data should be of an RDD, got %s." % type(data))
        data = data.map(_convert_to_vector)
        decayFactor = float(decayFactor)
        if timeUnit not in ["batches", "points"]:
            raise ValueError(
                "timeUnit should be 'batches' or 'points', got %s." % timeUnit)
        vectorCenters = [_convert_to_vector(center) for center in self.centers]
        updatedModel = callMLlibFunc(
            "updateStreamingKMeansModel", vectorCenters, self._clusterWeights,
            data, decayFactor, timeUnit)
        self.centers = array(updatedModel[0])
        self._clusterWeights = list(updatedModel[1])
        return self


class StreamingKMeans(object):
    """
    Provides methods to set k, decayFactor, timeUnit to configure the
    KMeans algorithm for fitting and predicting on incoming dstreams.
    More details on how the centroids are updated are provided under the
    docs of StreamingKMeansModel.

    :param k:
      Number of clusters.
      (default: 2)
    :param decayFactor:
      Forgetfulness of the previous centroids.
      (default: 1.0)
    :param timeUnit:
      Can be "batches" or "points". If points, then the decay factor is
      raised to the power of number of new points and if batches, then
      decay factor will be used as is.
      (default: "batches")

    .. versionadded:: 1.5.0
    """
    def __init__(self, k=2, decayFactor=1.0, timeUnit="batches"):
        self._k = k
        self._decayFactor = decayFactor
        if timeUnit not in ["batches", "points"]:
            raise ValueError(
                "timeUnit should be 'batches' or 'points', got %s." % timeUnit)
        self._timeUnit = timeUnit
        self._model = None

    @since('1.5.0')
    def latestModel(self):
        """Return the latest model"""
        return self._model

    def _validate(self, dstream):
        if self._model is None:
            raise ValueError(
                "Initial centers should be set either by setInitialCenters "
                "or setRandomCenters.")
        if not isinstance(dstream, DStream):
            raise TypeError(
                "Expected dstream to be of type DStream, "
                "got type %s" % type(dstream))

    @since('1.5.0')
    def setK(self, k):
        """Set number of clusters."""
        self._k = k
        return self

    @since('1.5.0')
    def setDecayFactor(self, decayFactor):
        """Set decay factor."""
        self._decayFactor = decayFactor
        return self

    @since('1.5.0')
    def setHalfLife(self, halfLife, timeUnit):
        """
        Set number of batches after which the centroids of that
        particular batch has half the weightage.
        """
        self._timeUnit = timeUnit
        self._decayFactor = exp(log(0.5) / halfLife)
        return self

    @since('1.5.0')
    def setInitialCenters(self, centers, weights):
        """
        Set initial centers. Should be set before calling trainOn.
        """
        self._model = StreamingKMeansModel(centers, weights)
        return self

    @since('1.5.0')
    def setRandomCenters(self, dim, weight, seed):
        """
        Set the initial centres to be random samples from
        a gaussian population with constant weights.
        """
        rng = random.RandomState(seed)
        clusterCenters = rng.randn(self._k, dim)
        clusterWeights = tile(weight, self._k)
        self._model = StreamingKMeansModel(clusterCenters, clusterWeights)
        return self

    @since('1.5.0')
    def trainOn(self, dstream):
        """Train the model on the incoming dstream."""
        self._validate(dstream)

        def update(rdd):
            self._model.update(rdd, self._decayFactor, self._timeUnit)

        dstream.foreachRDD(update)

    @since('1.5.0')
    def predictOn(self, dstream):
        """
        Make predictions on a dstream.
        Returns a transformed dstream object
        """
        self._validate(dstream)
        return dstream.map(lambda x: self._model.predict(x))

    @since('1.5.0')
    def predictOnValues(self, dstream):
        """
        Make predictions on a keyed dstream.
        Returns a transformed dstream object.
        """
        self._validate(dstream)
        return dstream.mapValues(lambda x: self._model.predict(x))


class LDAModel(JavaModelWrapper, JavaSaveable, Loader):

    """ A clustering model derived from the LDA method.

    Latent Dirichlet Allocation (LDA), a topic model designed for text documents.
    Terminology
    - "word" = "term": an element of the vocabulary
    - "token": instance of a term appearing in a document
    - "topic": multinomial distribution over words representing some concept
    References:
    - Original LDA paper (journal version):
    Blei, Ng, and Jordan.  "Latent Dirichlet Allocation."  JMLR, 2003.

    >>> from pyspark.mllib.linalg import Vectors
    >>> from numpy.testing import assert_almost_equal, assert_equal
    >>> data = [
    ...     [1, Vectors.dense([0.0, 1.0])],
    ...     [2, SparseVector(2, {0: 1.0})],
    ... ]
    >>> rdd =  sc.parallelize(data)
    >>> model = LDA.train(rdd, k=2, seed=1)
    >>> model.vocabSize()
    2
    >>> model.describeTopics()
    [([1, 0], [0.5..., 0.49...]), ([0, 1], [0.5..., 0.49...])]
    >>> model.describeTopics(1)
    [([1], [0.5...]), ([0], [0.5...])]

    >>> topics = model.topicsMatrix()
    >>> topics_expect = array([[0.5,  0.5], [0.5, 0.5]])
    >>> assert_almost_equal(topics, topics_expect, 1)

    >>> import os, tempfile
    >>> from shutil import rmtree
    >>> path = tempfile.mkdtemp()
    >>> model.save(sc, path)
    >>> sameModel = LDAModel.load(sc, path)
    >>> assert_equal(sameModel.topicsMatrix(), model.topicsMatrix())
    >>> sameModel.vocabSize() == model.vocabSize()
    True
    >>> try:
    ...     rmtree(path)
    ... except OSError:
    ...     pass

    .. versionadded:: 1.5.0
    """

    @since('1.5.0')
    def topicsMatrix(self):
        """Inferred topics, where each topic is represented by a distribution over terms."""
        return self.call("topicsMatrix").toArray()

    @since('1.5.0')
    def vocabSize(self):
        """Vocabulary size (number of terms or terms in the vocabulary)"""
        return self.call("vocabSize")

    @since('1.6.0')
    def describeTopics(self, maxTermsPerTopic=None):
        """Return the topics described by weighted terms.

        WARNING: If vocabSize and k are large, this can return a large object!

        :param maxTermsPerTopic:
          Maximum number of terms to collect for each topic.
          (default: vocabulary size)
        :return:
          Array over topics. Each topic is represented as a pair of
          matching arrays: (term indices, term weights in topic).
          Each topic's terms are sorted in order of decreasing weight.
        """
        if maxTermsPerTopic is None:
            topics = self.call("describeTopics")
        else:
            topics = self.call("describeTopics", maxTermsPerTopic)
        return topics

    @classmethod
    @since('1.5.0')
    def load(cls, sc, path):
        """Load the LDAModel from disk.

        :param sc:
          SparkContext.
        :param path:
          Path to where the model is stored.
        """
        if not isinstance(sc, SparkContext):
            raise TypeError("sc should be a SparkContext, got type %s" % type(sc))
        if not isinstance(path, basestring):
            raise TypeError("path should be a basestring, got type %s" % type(path))
        model = callMLlibFunc("loadLDAModel", sc, path)
        return LDAModel(model)


class LDA(object):
    """
    .. versionadded:: 1.5.0
    """

    @classmethod
    @since('1.5.0')
    def train(cls, rdd, k=10, maxIterations=20, docConcentration=-1.0,
              topicConcentration=-1.0, seed=None, checkpointInterval=10, optimizer="em"):
        """Train a LDA model.

        :param rdd:
          RDD of documents, which are tuples of document IDs and term
          (word) count vectors. The term count vectors are "bags of
          words" with a fixed-size vocabulary (where the vocabulary size
          is the length of the vector). Document IDs must be unique
          and >= 0.
        :param k:
          Number of topics to infer, i.e., the number of soft cluster
          centers.
          (default: 10)
        :param maxIterations:
          Maximum number of iterations allowed.
          (default: 20)
        :param docConcentration:
          Concentration parameter (commonly named "alpha") for the prior
          placed on documents' distributions over topics ("theta").
          (default: -1.0)
        :param topicConcentration:
          Concentration parameter (commonly named "beta" or "eta") for
          the prior placed on topics' distributions over terms.
          (default: -1.0)
        :param seed:
          Random seed for cluster initialization. Set as None to generate
          seed based on system time.
          (default: None)
        :param checkpointInterval:
          Period (in iterations) between checkpoints.
          (default: 10)
        :param optimizer:
          LDAOptimizer used to perform the actual calculation. Currently
          "em", "online" are supported.
          (default: "em")
        """
        model = callMLlibFunc("trainLDAModel", rdd, k, maxIterations,
                              docConcentration, topicConcentration, seed,
                              checkpointInterval, optimizer)
        return LDAModel(model)


def _test():
    import doctest
    import pyspark.mllib.clustering
    globs = pyspark.mllib.clustering.__dict__.copy()
    globs['sc'] = SparkContext('local[4]', 'PythonTest', batchSize=2)
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    globs['sc'].stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
