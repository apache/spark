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

if sys.version > '3':
    xrange = range

from math import exp, log

from numpy import array, random, tile

from pyspark import SparkContext
from pyspark.rdd import RDD, ignore_unicode_prefix
from pyspark.mllib.common import callMLlibFunc, callJavaFunc, _py2java, _java2py
from pyspark.mllib.linalg import SparseVector, _convert_to_vector, DenseVector
from pyspark.mllib.stat.distribution import MultivariateGaussian
from pyspark.mllib.util import Saveable, Loader, inherit_doc
from pyspark.streaming import DStream

__all__ = ['KMeansModel', 'KMeans', 'GaussianMixtureModel', 'GaussianMixture',
           'StreamingKMeans', 'StreamingKMeansModel']


@inherit_doc
class KMeansModel(Saveable, Loader):

    """A clustering model derived from the k-means method.

    >>> data = array([0.0,0.0, 1.0,1.0, 9.0,8.0, 8.0,9.0]).reshape(4, 2)
    >>> model = KMeans.train(
    ...     sc.parallelize(data), 2, maxIterations=10, runs=30, initializationMode="random",
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
    """

    def __init__(self, centers):
        self.centers = centers

    @property
    def clusterCenters(self):
        """Get the cluster centers, represented as a list of NumPy arrays."""
        return self.centers

    @property
    def k(self):
        """Total number of clusters."""
        return len(self.centers)

    def predict(self, x):
        """Find the cluster to which x belongs in this model."""
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

    def computeCost(self, rdd):
        """
        Return the K-means cost (sum of squared distances of points to
        their nearest center) for this model on the given data.
        """
        cost = callMLlibFunc("computeCostKmeansModel", rdd.map(_convert_to_vector),
                             [_convert_to_vector(c) for c in self.centers])
        return cost

    def save(self, sc, path):
        java_centers = _py2java(sc, [_convert_to_vector(c) for c in self.centers])
        java_model = sc._jvm.org.apache.spark.mllib.clustering.KMeansModel(java_centers)
        java_model.save(sc._jsc.sc(), path)

    @classmethod
    def load(cls, sc, path):
        java_model = sc._jvm.org.apache.spark.mllib.clustering.KMeansModel.load(sc._jsc.sc(), path)
        return KMeansModel(_java2py(sc, java_model.clusterCenters()))


class KMeans(object):

    @classmethod
    def train(cls, rdd, k, maxIterations=100, runs=1, initializationMode="k-means||",
              seed=None, initializationSteps=5, epsilon=1e-4):
        """Train a k-means clustering model."""
        model = callMLlibFunc("trainKMeansModel", rdd.map(_convert_to_vector), k, maxIterations,
                              runs, initializationMode, seed, initializationSteps, epsilon)
        centers = callJavaFunc(rdd.context, model.clusterCenters)
        return KMeansModel([c.toArray() for c in centers])


class GaussianMixtureModel(object):

    """A clustering model derived from the Gaussian Mixture Model method.

    >>> from pyspark.mllib.linalg import Vectors, DenseMatrix
    >>> clusterdata_1 =  sc.parallelize(array([-0.1,-0.05,-0.01,-0.1,
    ...                                         0.9,0.8,0.75,0.935,
    ...                                        -0.83,-0.68,-0.91,-0.76 ]).reshape(6, 2))
    >>> model = GaussianMixture.train(clusterdata_1, 3, convergenceTol=0.0001,
    ...                                 maxIterations=50, seed=10)
    >>> labels = model.predict(clusterdata_1).collect()
    >>> labels[0]==labels[1]
    False
    >>> labels[1]==labels[2]
    True
    >>> labels[4]==labels[5]
    True
    >>> data =  array([-5.1971, -2.5359, -3.8220,
    ...                -5.2211, -5.0602,  4.7118,
    ...                 6.8989, 3.4592,  4.6322,
    ...                 5.7048,  4.6567, 5.5026,
    ...                 4.5605,  5.2043,  6.2734])
    >>> clusterdata_2 = sc.parallelize(data.reshape(5,3))
    >>> model = GaussianMixture.train(clusterdata_2, 2, convergenceTol=0.0001,
    ...                               maxIterations=150, seed=10)
    >>> labels = model.predict(clusterdata_2).collect()
    >>> labels[0]==labels[1]==labels[2]
    True
    >>> labels[3]==labels[4]
    True
    >>> clusterdata_3 = sc.parallelize(data.reshape(15, 1))
    >>> im = GaussianMixtureModel([0.5, 0.5],
    ...      [MultivariateGaussian(Vectors.dense([-1.0]), DenseMatrix(1, 1, [1.0])),
    ...      MultivariateGaussian(Vectors.dense([1.0]), DenseMatrix(1, 1, [1.0]))])
    >>> model = GaussianMixture.train(clusterdata_3, 2, initialModel=im)
    """

    def __init__(self, weights, gaussians):
        self._weights = weights
        self._gaussians = gaussians
        self._k = len(self._weights)

    @property
    def weights(self):
        """
        Weights for each Gaussian distribution in the mixture, where weights[i] is
        the weight for Gaussian i, and weights.sum == 1.
        """
        return self._weights

    @property
    def gaussians(self):
        """
        Array of MultivariateGaussian where gaussians[i] represents
        the Multivariate Gaussian (Normal) Distribution for Gaussian i.
        """
        return self._gaussians

    @property
    def k(self):
        """Number of gaussians in mixture."""
        return self._k

    def predict(self, x):
        """
        Find the cluster to which the points in 'x' has maximum membership
        in this model.

        :param x:    RDD of data points.
        :return:     cluster_labels. RDD of cluster labels.
        """
        if isinstance(x, RDD):
            cluster_labels = self.predictSoft(x).map(lambda z: z.index(max(z)))
            return cluster_labels
        else:
            raise TypeError("x should be represented by an RDD, "
                            "but got %s." % type(x))

    def predictSoft(self, x):
        """
        Find the membership of each point in 'x' to all mixture components.

        :param x:    RDD of data points.
        :return:     membership_matrix. RDD of array of double values.
        """
        if isinstance(x, RDD):
            means, sigmas = zip(*[(g.mu, g.sigma) for g in self._gaussians])
            membership_matrix = callMLlibFunc("predictSoftGMM", x.map(_convert_to_vector),
                                              _convert_to_vector(self._weights), means, sigmas)
            return membership_matrix.map(lambda x: pyarray.array('d', x))
        else:
            raise TypeError("x should be represented by an RDD, "
                            "but got %s." % type(x))


class GaussianMixture(object):
    """
    Learning algorithm for Gaussian Mixtures using the expectation-maximization algorithm.

    :param data:            RDD of data points
    :param k:               Number of components
    :param convergenceTol:  Threshold value to check the convergence criteria. Defaults to 1e-3
    :param maxIterations:   Number of iterations. Default to 100
    :param seed:            Random Seed
    :param initialModel:    GaussianMixtureModel for initializing learning
    """
    @classmethod
    def train(cls, rdd, k, convergenceTol=1e-3, maxIterations=100, seed=None, initialModel=None):
        """Train a Gaussian Mixture clustering model."""
        initialModelWeights = None
        initialModelMu = None
        initialModelSigma = None
        if initialModel is not None:
            if initialModel.k != k:
                raise Exception("Mismatched cluster count, initialModel.k = %s, however k = %s"
                                % (initialModel.k, k))
            initialModelWeights = initialModel.weights
            initialModelMu = [initialModel.gaussians[i].mu for i in range(initialModel.k)]
            initialModelSigma = [initialModel.gaussians[i].sigma for i in range(initialModel.k)]
        weight, mu, sigma = callMLlibFunc("trainGaussianMixture", rdd.map(_convert_to_vector), k,
                                          convergenceTol, maxIterations, seed, initialModelWeights,
                                          initialModelMu, initialModelSigma)
        mvg_obj = [MultivariateGaussian(mu[i], sigma[i]) for i in range(k)]
        return GaussianMixtureModel(weight, mvg_obj)


class StreamingKMeansModel(KMeansModel):
    """
    .. note:: Experimental

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

    Note that if a is set to 1, it is the weighted mean of the previous
    and new data. If it set to zero, the old centroids are completely
    forgotten.

    :param clusterCenters: Initial cluster centers.
    :param clusterWeights: List of weights assigned to each cluster.

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
    """
    def __init__(self, clusterCenters, clusterWeights):
        super(StreamingKMeansModel, self).__init__(centers=clusterCenters)
        self._clusterWeights = list(clusterWeights)

    @property
    def clusterWeights(self):
        """Return the cluster weights."""
        return self._clusterWeights

    @ignore_unicode_prefix
    def update(self, data, decayFactor, timeUnit):
        """Update the centroids, according to data

        :param data: Should be a RDD that represents the new data.
        :param decayFactor: forgetfulness of the previous centroids.
        :param timeUnit: Can be "batches" or "points". If points, then the
                         decay factor is raised to the power of number of new
                         points and if batches, it is used as it is.
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
    .. note:: Experimental

    Provides methods to set k, decayFactor, timeUnit to configure the
    KMeans algorithm for fitting and predicting on incoming dstreams.
    More details on how the centroids are updated are provided under the
    docs of StreamingKMeansModel.

    :param k: int, number of clusters
    :param decayFactor: float, forgetfulness of the previous centroids.
    :param timeUnit: can be "batches" or "points". If points, then the
                     decayfactor is raised to the power of no. of new points.
    """
    def __init__(self, k=2, decayFactor=1.0, timeUnit="batches"):
        self._k = k
        self._decayFactor = decayFactor
        if timeUnit not in ["batches", "points"]:
            raise ValueError(
                "timeUnit should be 'batches' or 'points', got %s." % timeUnit)
        self._timeUnit = timeUnit
        self._model = None

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

    def setK(self, k):
        """Set number of clusters."""
        self._k = k
        return self

    def setDecayFactor(self, decayFactor):
        """Set decay factor."""
        self._decayFactor = decayFactor
        return self

    def setHalfLife(self, halfLife, timeUnit):
        """
        Set number of batches after which the centroids of that
        particular batch has half the weightage.
        """
        self._timeUnit = timeUnit
        self._decayFactor = exp(log(0.5) / halfLife)
        return self

    def setInitialCenters(self, centers, weights):
        """
        Set initial centers. Should be set before calling trainOn.
        """
        self._model = StreamingKMeansModel(centers, weights)
        return self

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

    def trainOn(self, dstream):
        """Train the model on the incoming dstream."""
        self._validate(dstream)

        def update(rdd):
            self._model.update(rdd, self._decayFactor, self._timeUnit)

        dstream.foreachRDD(update)

    def predictOn(self, dstream):
        """
        Make predictions on a dstream.
        Returns a transformed dstream object
        """
        self._validate(dstream)
        return dstream.map(lambda x: self._model.predict(x))

    def predictOnValues(self, dstream):
        """
        Make predictions on a keyed dstream.
        Returns a transformed dstream object.
        """
        self._validate(dstream)
        return dstream.mapValues(lambda x: self._model.predict(x))


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
