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

from numpy import array

from pyspark import RDD
from pyspark import SparkContext
from pyspark.mllib.common import callMLlibFunc, callJavaFunc, _py2java, _java2py
from pyspark.mllib.linalg import SparseVector, _convert_to_vector
from pyspark.mllib.stat.distribution import MultivariateGaussian
from pyspark.mllib.util import Saveable, Loader, inherit_doc

__all__ = ['KMeansModel', 'KMeans', 'GaussianMixtureModel', 'GaussianMixture']


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
