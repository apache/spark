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
from math import exp, log
from collections import namedtuple
from typing import Any, List, Optional, Tuple, TypeVar, Union, overload, TYPE_CHECKING

import numpy as np
from numpy import array, random, tile

from pyspark import SparkContext, since
from pyspark.core.rdd import RDD
from pyspark.mllib.common import JavaModelWrapper, callMLlibFunc, callJavaFunc, _py2java, _java2py
from pyspark.mllib.linalg import SparseVector, _convert_to_vector, DenseVector  # noqa: F401
from pyspark.mllib.stat.distribution import MultivariateGaussian
from pyspark.mllib.util import Saveable, Loader, inherit_doc, JavaLoader, JavaSaveable
from pyspark.streaming import DStream

if TYPE_CHECKING:
    from py4j.java_gateway import JavaObject
    from pyspark.mllib._typing import VectorLike

T = TypeVar("T")

__all__ = [
    "BisectingKMeansModel",
    "BisectingKMeans",
    "KMeansModel",
    "KMeans",
    "GaussianMixtureModel",
    "GaussianMixture",
    "PowerIterationClusteringModel",
    "PowerIterationClustering",
    "StreamingKMeans",
    "StreamingKMeansModel",
    "LDA",
    "LDAModel",
]


@inherit_doc
class BisectingKMeansModel(JavaModelWrapper):
    """
    A clustering model derived from the bisecting k-means method.

    .. versionadded:: 2.0.0

    Examples
    --------
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
    """

    def __init__(self, java_model: "JavaObject"):
        super(BisectingKMeansModel, self).__init__(java_model)
        self.centers = [c.toArray() for c in self.call("clusterCenters")]

    @property
    @since("2.0.0")
    def clusterCenters(self) -> List[np.ndarray]:
        """Get the cluster centers, represented as a list of NumPy
        arrays."""
        return self.centers

    @property
    @since("2.0.0")
    def k(self) -> int:
        """Get the number of clusters"""
        return self.call("k")

    @overload
    def predict(self, x: "VectorLike") -> int:
        ...

    @overload
    def predict(self, x: RDD["VectorLike"]) -> RDD[int]:
        ...

    def predict(self, x: Union["VectorLike", RDD["VectorLike"]]) -> Union[int, RDD[int]]:
        """
        Find the cluster that each of the points belongs to in this
        model.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        x : :py:class:`pyspark.mllib.linalg.Vector` or :py:class:`pyspark.RDD`
            A data point (or RDD of points) to determine cluster index.
            :py:class:`pyspark.mllib.linalg.Vector` can be replaced with equivalent
            objects (list, tuple, numpy.ndarray).

        Returns
        -------
        int or :py:class:`pyspark.RDD` of int
            Predicted cluster index or an RDD of predicted cluster indices
            if the input is an RDD.
        """
        if isinstance(x, RDD):
            vecs = x.map(_convert_to_vector)
            return self.call("predict", vecs)

        x = _convert_to_vector(x)
        return self.call("predict", x)

    def computeCost(self, x: Union["VectorLike", RDD["VectorLike"]]) -> float:
        """
        Return the Bisecting K-means cost (sum of squared distances of
        points to their nearest center) for this model on the given
        data. If provided with an RDD of points returns the sum.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        point : :py:class:`pyspark.mllib.linalg.Vector` or :py:class:`pyspark.RDD`
            A data point (or RDD of points) to compute the cost(s).
            :py:class:`pyspark.mllib.linalg.Vector` can be replaced with equivalent
            objects (list, tuple, numpy.ndarray).
        """
        if isinstance(x, RDD):
            vecs = x.map(_convert_to_vector)
            return self.call("computeCost", vecs)

        return self.call("computeCost", _convert_to_vector(x))


class BisectingKMeans:
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

    .. versionadded:: 2.0.0

    Notes
    -----
    See the original paper [1]_

    .. [1] Steinbach, M. et al. "A Comparison of Document Clustering Techniques." (2000).
        KDD Workshop on Text Mining, 2000
        http://glaros.dtc.umn.edu/gkhome/fetch/papers/docclusterKDDTMW00.pdf
    """

    @classmethod
    def train(
        cls,
        rdd: RDD["VectorLike"],
        k: int = 4,
        maxIterations: int = 20,
        minDivisibleClusterSize: float = 1.0,
        seed: int = -1888008604,
    ) -> BisectingKMeansModel:
        """
        Runs the bisecting k-means algorithm return the model.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        rdd : :py:class:`pyspark.RDD`
            Training points as an `RDD` of `Vector` or convertible
            sequence types.
        k : int, optional
            The desired number of leaf clusters. The actual number could
            be smaller if there are no divisible leaf clusters.
            (default: 4)
        maxIterations : int, optional
            Maximum number of iterations allowed to split clusters.
            (default: 20)
        minDivisibleClusterSize : float, optional
            Minimum number of points (if >= 1.0) or the minimum proportion
            of points (if < 1.0) of a divisible cluster.
            (default: 1)
        seed : int, optional
            Random seed value for cluster initialization.
            (default: -1888008604 from classOf[BisectingKMeans].getName.##)
        """
        java_model = callMLlibFunc(
            "trainBisectingKMeans",
            rdd.map(_convert_to_vector),
            k,
            maxIterations,
            minDivisibleClusterSize,
            seed,
        )
        return BisectingKMeansModel(java_model)


@inherit_doc
class KMeansModel(Saveable, Loader["KMeansModel"]):

    """A clustering model derived from the k-means method.

    .. versionadded:: 0.9.0

    Examples
    --------
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
    2.0
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
    """

    def __init__(self, centers: List["VectorLike"]):
        self.centers = centers

    @property
    @since("1.0.0")
    def clusterCenters(self) -> List["VectorLike"]:
        """Get the cluster centers, represented as a list of NumPy arrays."""
        return self.centers

    @property
    @since("1.4.0")
    def k(self) -> int:
        """Total number of clusters."""
        return len(self.centers)

    @overload
    def predict(self, x: "VectorLike") -> int:
        ...

    @overload
    def predict(self, x: RDD["VectorLike"]) -> RDD[int]:
        ...

    def predict(self, x: Union["VectorLike", RDD["VectorLike"]]) -> Union[int, RDD[int]]:
        """
        Find the cluster that each of the points belongs to in this
        model.

        .. versionadded:: 0.9.0

        Parameters
        ----------
        x : :py:class:`pyspark.mllib.linalg.Vector` or :py:class:`pyspark.RDD`
            A data point (or RDD of points) to determine cluster index.
            :py:class:`pyspark.mllib.linalg.Vector` can be replaced with equivalent
            objects (list, tuple, numpy.ndarray).

        Returns
        -------
        int or :py:class:`pyspark.RDD` of int
            Predicted cluster index or an RDD of predicted cluster indices
            if the input is an RDD.
        """
        best = 0
        best_distance = float("inf")
        if isinstance(x, RDD):
            return x.map(self.predict)

        x = _convert_to_vector(x)
        for i in range(len(self.centers)):
            distance = x.squared_distance(self.centers[i])  # type: ignore[attr-defined]
            if distance < best_distance:
                best = i
                best_distance = distance
        return best

    def computeCost(self, rdd: RDD["VectorLike"]) -> float:
        """
        Return the K-means cost (sum of squared distances of points to
        their nearest center) for this model on the given
        data.

        .. versionadded:: 1.4.0

        Parameters
        ----------
        rdd : ::py:class:`pyspark.RDD`
            The RDD of points to compute the cost on.
        """
        cost = callMLlibFunc(
            "computeCostKmeansModel",
            rdd.map(_convert_to_vector),
            [_convert_to_vector(c) for c in self.centers],
        )
        return cost

    @since("1.4.0")
    def save(self, sc: SparkContext, path: str) -> None:
        """
        Save this model to the given path.
        """
        assert sc._jvm is not None

        java_centers = _py2java(sc, [_convert_to_vector(c) for c in self.centers])
        java_model = sc._jvm.org.apache.spark.mllib.clustering.KMeansModel(java_centers)
        java_model.save(sc._jsc.sc(), path)

    @classmethod
    @since("1.4.0")
    def load(cls, sc: SparkContext, path: str) -> "KMeansModel":
        """
        Load a model from the given path.
        """
        assert sc._jvm is not None

        java_model = sc._jvm.org.apache.spark.mllib.clustering.KMeansModel.load(sc._jsc.sc(), path)
        return KMeansModel(_java2py(sc, java_model.clusterCenters()))


class KMeans:
    """
    K-means clustering.

    .. versionadded:: 0.9.0
    """

    @classmethod
    def train(
        cls,
        rdd: RDD["VectorLike"],
        k: int,
        maxIterations: int = 100,
        initializationMode: str = "k-means||",
        seed: Optional[int] = None,
        initializationSteps: int = 2,
        epsilon: float = 1e-4,
        initialModel: Optional[KMeansModel] = None,
        distanceMeasure: str = "euclidean",
    ) -> "KMeansModel":
        """
        Train a k-means clustering model.

        .. versionadded:: 0.9.0

        Parameters
        ----------
        rdd : ::py:class:`pyspark.RDD`
            Training points as an `RDD` of :py:class:`pyspark.mllib.linalg.Vector`
            or convertible sequence types.
        k : int
            Number of clusters to create.
        maxIterations : int, optional
            Maximum number of iterations allowed.
            (default: 100)
        initializationMode : str, optional
            The initialization algorithm. This can be either "random" or
            "k-means||".
            (default: "k-means||")
        seed : int, optional
            Random seed value for cluster initialization. Set as None to
            generate seed based on system time.
            (default: None)
        initializationSteps :
            Number of steps for the k-means|| initialization mode.
            This is an advanced setting -- the default of 2 is almost
            always enough.
            (default: 2)
        epsilon : float, optional
            Distance threshold within which a center will be considered to
            have converged. If all centers move less than this Euclidean
            distance, iterations are stopped.
            (default: 1e-4)
        initialModel : :py:class:`KMeansModel`, optional
            Initial cluster centers can be provided as a KMeansModel object
            rather than using the random or k-means|| initializationModel.
            (default: None)
        distanceMeasure : str, optional
            The distance measure used by the k-means algorithm.
            (default: "euclidean")
        """
        clusterInitialModel = []
        if initialModel is not None:
            if not isinstance(initialModel, KMeansModel):
                raise TypeError(
                    "initialModel is of " + str(type(initialModel)) + ". It needs "
                    "to be of <type 'KMeansModel'>"
                )
            clusterInitialModel = [_convert_to_vector(c) for c in initialModel.clusterCenters]
        model = callMLlibFunc(
            "trainKMeansModel",
            rdd.map(_convert_to_vector),
            k,
            maxIterations,
            initializationMode,
            seed,
            initializationSteps,
            epsilon,
            clusterInitialModel,
            distanceMeasure,
        )
        centers = callJavaFunc(rdd.context, model.clusterCenters)
        return KMeansModel([c.toArray() for c in centers])


@inherit_doc
class GaussianMixtureModel(JavaModelWrapper, JavaSaveable, JavaLoader["GaussianMixtureModel"]):

    """
    A clustering model derived from the Gaussian Mixture Model method.

    .. versionadded:: 1.3.0

    Examples
    --------
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
    >>> abs(softPredicted[0] - 1.0) < 0.03
    True
    >>> abs(softPredicted[1] - 0.0) < 0.03
    True
    >>> abs(softPredicted[2] - 0.0) < 0.03
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
    """

    @property
    @since("1.4.0")
    def weights(self) -> np.ndarray:
        """
        Weights for each Gaussian distribution in the mixture, where weights[i] is
        the weight for Gaussian i, and weights.sum == 1.
        """
        return array(self.call("weights"))

    @property
    @since("1.4.0")
    def gaussians(self) -> List[MultivariateGaussian]:
        """
        Array of MultivariateGaussian where gaussians[i] represents
        the Multivariate Gaussian (Normal) Distribution for Gaussian i.
        """
        return [
            MultivariateGaussian(gaussian[0], gaussian[1]) for gaussian in self.call("gaussians")
        ]

    @property
    @since("1.4.0")
    def k(self) -> int:
        """Number of gaussians in mixture."""
        return len(self.weights)

    @overload
    def predict(self, x: "VectorLike") -> np.int64:
        ...

    @overload
    def predict(self, x: RDD["VectorLike"]) -> RDD[int]:
        ...

    def predict(self, x: Union["VectorLike", RDD["VectorLike"]]) -> Union[np.int64, RDD[int]]:
        """
        Find the cluster to which the point 'x' or each point in RDD 'x'
        has maximum membership in this model.

        .. versionadded:: 1.3.0

        Parameters
        ----------
        x : :py:class:`pyspark.mllib.linalg.Vector` or :py:class:`pyspark.RDD`
            A feature vector or an RDD of vectors representing data points.

        Returns
        -------
        numpy.float64 or :py:class:`pyspark.RDD` of int
            Predicted cluster label or an RDD of predicted cluster labels
            if the input is an RDD.
        """
        if isinstance(x, RDD):
            cluster_labels = self.predictSoft(x).map(lambda z: z.index(max(z)))
            return cluster_labels
        else:
            z = self.predictSoft(x)
            return z.argmax()

    @overload
    def predictSoft(self, x: "VectorLike") -> np.ndarray:
        ...

    @overload
    def predictSoft(self, x: RDD["VectorLike"]) -> RDD[pyarray.array]:
        ...

    def predictSoft(
        self, x: Union["VectorLike", RDD["VectorLike"]]
    ) -> Union[np.ndarray, RDD[pyarray.array]]:
        """
        Find the membership of point 'x' or each point in RDD 'x' to all mixture components.

        .. versionadded:: 1.3.0

        Parameters
        ----------
        x : :py:class:`pyspark.mllib.linalg.Vector` or :py:class:`pyspark.RDD`
            A feature vector or an RDD of vectors representing data points.

        Returns
        -------
        numpy.ndarray or :py:class:`pyspark.RDD`
            The membership value to all mixture components for vector 'x'
            or each vector in RDD 'x'.
        """
        if isinstance(x, RDD):
            means, sigmas = zip(*[(g.mu, g.sigma) for g in self.gaussians])
            membership_matrix = callMLlibFunc(
                "predictSoftGMM",
                x.map(_convert_to_vector),
                _convert_to_vector(self.weights),
                means,
                sigmas,
            )
            return membership_matrix.map(lambda x: pyarray.array("d", x))
        else:
            return self.call("predictSoft", _convert_to_vector(x)).toArray()

    @classmethod
    def load(cls, sc: SparkContext, path: str) -> "GaussianMixtureModel":
        """Load the GaussianMixtureModel from disk.

        .. versionadded:: 1.5.0

        Parameters
        ----------
        sc : :py:class:`SparkContext`
        path : str
            Path to where the model is stored.
        """
        assert sc._jvm is not None

        model = cls._load_java(sc, path)
        wrapper = sc._jvm.org.apache.spark.mllib.api.python.GaussianMixtureModelWrapper(model)
        return cls(wrapper)


class GaussianMixture:
    """
    Learning algorithm for Gaussian Mixtures using the expectation-maximization algorithm.

    .. versionadded:: 1.3.0
    """

    @classmethod
    def train(
        cls,
        rdd: RDD["VectorLike"],
        k: int,
        convergenceTol: float = 1e-3,
        maxIterations: int = 100,
        seed: Optional[int] = None,
        initialModel: Optional[GaussianMixtureModel] = None,
    ) -> GaussianMixtureModel:
        """
        Train a Gaussian Mixture clustering model.

        .. versionadded:: 1.3.0

        Parameters
        ----------
        rdd : ::py:class:`pyspark.RDD`
            Training points as an `RDD` of :py:class:`pyspark.mllib.linalg.Vector`
            or convertible sequence types.
        k : int
            Number of independent Gaussians in the mixture model.
        convergenceTol : float, optional
            Maximum change in log-likelihood at which convergence is
            considered to have occurred.
            (default: 1e-3)
        maxIterations : int, optional
            Maximum number of iterations allowed.
            (default: 100)
        seed : int, optional
            Random seed for initial Gaussian distribution. Set as None to
            generate seed based on system time.
            (default: None)
        initialModel : GaussianMixtureModel, optional
            Initial GMM starting point, bypassing the random
            initialization.
            (default: None)
        """
        initialModelWeights = None
        initialModelMu = None
        initialModelSigma = None
        if initialModel is not None:
            if initialModel.k != k:
                raise ValueError(
                    "Mismatched cluster count, initialModel.k = %s, however k = %s"
                    % (initialModel.k, k)
                )
            initialModelWeights = list(initialModel.weights)
            initialModelMu = [initialModel.gaussians[i].mu for i in range(initialModel.k)]
            initialModelSigma = [initialModel.gaussians[i].sigma for i in range(initialModel.k)]
        java_model = callMLlibFunc(
            "trainGaussianMixtureModel",
            rdd.map(_convert_to_vector),
            k,
            convergenceTol,
            maxIterations,
            seed,
            initialModelWeights,
            initialModelMu,
            initialModelSigma,
        )
        return GaussianMixtureModel(java_model)


class PowerIterationClusteringModel(
    JavaModelWrapper, JavaSaveable, JavaLoader["PowerIterationClusteringModel"]
):

    """
    Model produced by :py:class:`PowerIterationClustering`.

    .. versionadded:: 1.5.0

    Examples
    --------
    >>> import math
    >>> def genCircle(r, n):
    ...     points = []
    ...     for i in range(0, n):
    ...         theta = 2.0 * math.pi * i / n
    ...         points.append((r * math.cos(theta), r * math.sin(theta)))
    ...     return points
    ...
    >>> def sim(x, y):
    ...     dist2 = (x[0] - y[0]) * (x[0] - y[0]) + (x[1] - y[1]) * (x[1] - y[1])
    ...     return math.exp(-dist2 / 2.0)
    ...
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
    """

    @property
    @since("1.5.0")
    def k(self) -> int:
        """
        Returns the number of clusters.
        """
        return self.call("k")

    @since("1.5.0")
    def assignments(self) -> RDD["PowerIterationClustering.Assignment"]:
        """
        Returns the cluster assignments of this model.
        """
        return self.call("getAssignments").map(lambda x: (PowerIterationClustering.Assignment(*x)))

    @classmethod
    @since("1.5.0")
    def load(cls, sc: SparkContext, path: str) -> "PowerIterationClusteringModel":
        """
        Load a model from the given path.
        """
        assert sc._jvm is not None

        model = cls._load_java(sc, path)
        wrapper = sc._jvm.org.apache.spark.mllib.api.python.PowerIterationClusteringModelWrapper(
            model
        )
        return PowerIterationClusteringModel(wrapper)


class PowerIterationClustering:
    """
    Power Iteration Clustering (PIC), a scalable graph clustering algorithm.


    Developed by Lin and Cohen [1]_. From the abstract:

        "PIC finds a very low-dimensional embedding of a
        dataset using truncated power iteration on a normalized pair-wise
        similarity matrix of the data."

    .. versionadded:: 1.5.0

    .. [1] Lin, Frank & Cohen, William. (2010). Power Iteration Clustering.
        http://www.cs.cmu.edu/~frank/papers/icml2010-pic-final.pdf
    """

    @classmethod
    def train(
        cls,
        rdd: RDD[Tuple[int, int, float]],
        k: int,
        maxIterations: int = 100,
        initMode: str = "random",
    ) -> PowerIterationClusteringModel:
        r"""
        Train PowerIterationClusteringModel

        .. versionadded:: 1.5.0

        Parameters
        ----------
        rdd : :py:class:`pyspark.RDD`
            An RDD of (i, j, s\ :sub:`ij`\) tuples representing the
            affinity matrix, which is the matrix A in the PIC paper.  The
            similarity s\ :sub:`ij`\ must be nonnegative.  This is a symmetric
            matrix and hence s\ :sub:`ij`\ = s\ :sub:`ji`\  For any (i, j) with
            nonzero similarity, there should be either (i, j, s\ :sub:`ij`\) or
            (j, i, s\ :sub:`ji`\) in the input.  Tuples with i = j are ignored,
            because it is assumed s\ :sub:`ij`\ = 0.0.
        k : int
            Number of clusters.
        maxIterations : int, optional
            Maximum number of iterations of the PIC algorithm.
            (default: 100)
        initMode : str, optional
            Initialization mode. This can be either "random" to use
            a random vector as vertex properties, or "degree" to use
            normalized sum similarities.
            (default: "random")
        """
        model = callMLlibFunc(
            "trainPowerIterationClusteringModel",
            rdd.map(_convert_to_vector),
            int(k),
            int(maxIterations),
            initMode,
        )
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

    - c_t+1 = ((c_t * n_t * a) + (x_t * m_t)) / (n_t + m_t)
    - n_t+1 = n_t * a + m_t

    where

    - c_t: Centroid at the n_th iteration.
    - n_t: Number of samples (or) weights associated with the centroid
      at the n_th iteration.
    - x_t: Centroid of the new data closest to c_t.
    - m_t: Number of samples (or) weights of the new data closest to c_t
    - c_t+1: New centroid.
    - n_t+1: New number of weights.
    - a: Decay Factor, which gives the forgetfulness.

    .. versionadded:: 1.5.0

    Parameters
    ----------
    clusterCenters : list of :py:class:`pyspark.mllib.linalg.Vector` or covertible
        Initial cluster centers.
    clusterWeights : :py:class:`pyspark.mllib.linalg.Vector` or covertible
        List of weights assigned to each cluster.

    Notes
    -----
    If a is set to 1, it is the weighted mean of the previous
    and new data. If it set to zero, the old centroids are completely
    forgotten.

    Examples
    --------
    >>> initCenters = [[0.0, 0.0], [1.0, 1.0]]
    >>> initWeights = [1.0, 1.0]
    >>> stkm = StreamingKMeansModel(initCenters, initWeights)
    >>> data = sc.parallelize([[-0.1, -0.1], [0.1, 0.1],
    ...                        [0.9, 0.9], [1.1, 1.1]])
    >>> stkm = stkm.update(data, 1.0, "batches")
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
    >>> stkm = stkm.update(data, 0.0, "batches")
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

    def __init__(self, clusterCenters: List["VectorLike"], clusterWeights: "VectorLike"):
        super(StreamingKMeansModel, self).__init__(centers=clusterCenters)
        self._clusterWeights = list(clusterWeights)  # type: ignore[arg-type]

    @property
    @since("1.5.0")
    def clusterWeights(self) -> List[np.float64]:
        """Return the cluster weights."""
        return self._clusterWeights

    @since("1.5.0")
    def update(
        self, data: RDD["VectorLike"], decayFactor: float, timeUnit: str
    ) -> "StreamingKMeansModel":
        """Update the centroids, according to data

        .. versionadded:: 1.5.0

        Parameters
        ----------
        data : :py:class:`pyspark.RDD`
            RDD with new data for the model update.
        decayFactor : float
            Forgetfulness of the previous centroids.
        timeUnit :  str
            Can be "batches" or "points". If points, then the decay factor
            is raised to the power of number of new points and if batches,
            then decay factor will be used as is.
        """
        if not isinstance(data, RDD):
            raise TypeError("Data should be of an RDD, got %s." % type(data))
        data = data.map(_convert_to_vector)
        decayFactor = float(decayFactor)
        if timeUnit not in ["batches", "points"]:
            raise ValueError("timeUnit should be 'batches' or 'points', got %s." % timeUnit)
        vectorCenters = [_convert_to_vector(center) for center in self.centers]
        updatedModel = callMLlibFunc(
            "updateStreamingKMeansModel",
            vectorCenters,
            self._clusterWeights,
            data,
            decayFactor,
            timeUnit,
        )
        self.centers = array(updatedModel[0])  # type: ignore[assignment]
        self._clusterWeights = list(updatedModel[1])
        return self


class StreamingKMeans:
    """
    Provides methods to set k, decayFactor, timeUnit to configure the
    KMeans algorithm for fitting and predicting on incoming dstreams.
    More details on how the centroids are updated are provided under the
    docs of StreamingKMeansModel.

    .. versionadded:: 1.5.0

    Parameters
    ----------
    k : int, optional
        Number of clusters.
        (default: 2)
    decayFactor : float, optional
        Forgetfulness of the previous centroids.
        (default: 1.0)
    timeUnit : str, optional
        Can be "batches" or "points". If points, then the decay factor is
        raised to the power of number of new points and if batches, then
        decay factor will be used as is.
        (default: "batches")
    """

    def __init__(self, k: int = 2, decayFactor: float = 1.0, timeUnit: str = "batches"):
        self._k = k
        self._decayFactor = decayFactor
        if timeUnit not in ["batches", "points"]:
            raise ValueError("timeUnit should be 'batches' or 'points', got %s." % timeUnit)
        self._timeUnit = timeUnit
        self._model: Optional[StreamingKMeansModel] = None

    @since("1.5.0")
    def latestModel(self) -> Optional[StreamingKMeansModel]:
        """Return the latest model"""
        return self._model

    def _validate(self, dstream: Any) -> None:
        if self._model is None:
            raise ValueError(
                "Initial centers should be set either by setInitialCenters " "or setRandomCenters."
            )
        if not isinstance(dstream, DStream):
            raise TypeError(
                "Expected dstream to be of type DStream, " "got type %s" % type(dstream)
            )

    @since("1.5.0")
    def setK(self, k: int) -> "StreamingKMeans":
        """Set number of clusters."""
        self._k = k
        return self

    @since("1.5.0")
    def setDecayFactor(self, decayFactor: float) -> "StreamingKMeans":
        """Set decay factor."""
        self._decayFactor = decayFactor
        return self

    @since("1.5.0")
    def setHalfLife(self, halfLife: float, timeUnit: str) -> "StreamingKMeans":
        """
        Set number of batches after which the centroids of that
        particular batch has half the weightage.
        """
        self._timeUnit = timeUnit
        self._decayFactor = exp(log(0.5) / halfLife)
        return self

    @since("1.5.0")
    def setInitialCenters(
        self, centers: List["VectorLike"], weights: List[float]
    ) -> "StreamingKMeans":
        """
        Set initial centers. Should be set before calling trainOn.
        """
        self._model = StreamingKMeansModel(centers, weights)
        return self

    @since("1.5.0")
    def setRandomCenters(self, dim: int, weight: float, seed: int) -> "StreamingKMeans":
        """
        Set the initial centers to be random samples from
        a gaussian population with constant weights.
        """
        rng = random.RandomState(seed)
        clusterCenters = rng.randn(self._k, dim)
        clusterWeights = tile(weight, self._k)
        self._model = StreamingKMeansModel(clusterCenters, clusterWeights)  # type: ignore[arg-type]
        return self

    @since("1.5.0")
    def trainOn(self, dstream: "DStream[VectorLike]") -> None:
        """Train the model on the incoming dstream."""
        self._validate(dstream)

        def update(rdd: RDD["VectorLike"]) -> None:
            self._model.update(rdd, self._decayFactor, self._timeUnit)  # type: ignore[union-attr]

        dstream.foreachRDD(update)

    @since("1.5.0")
    def predictOn(self, dstream: "DStream[VectorLike]") -> "DStream[int]":
        """
        Make predictions on a dstream.
        Returns a transformed dstream object
        """
        self._validate(dstream)
        return dstream.map(lambda x: self._model.predict(x))  # type: ignore[union-attr]

    @since("1.5.0")
    def predictOnValues(self, dstream: "DStream[Tuple[T, VectorLike]]") -> "DStream[Tuple[T, int]]":
        """
        Make predictions on a keyed dstream.
        Returns a transformed dstream object.
        """
        self._validate(dstream)
        return dstream.mapValues(lambda x: self._model.predict(x))  # type: ignore[union-attr]


class LDAModel(JavaModelWrapper, JavaSaveable, Loader["LDAModel"]):

    """A clustering model derived from the LDA method.

    Latent Dirichlet Allocation (LDA), a topic model designed for text documents.
    Terminology

    - "word" = "term": an element of the vocabulary
    - "token": instance of a term appearing in a document
    - "topic": multinomial distribution over words representing some concept

    .. versionadded:: 1.5.0

    Notes
    -----
    See the original LDA paper (journal version) [1]_

    .. [1] Blei, D. et al. "Latent Dirichlet Allocation."
        J. Mach. Learn. Res. 3 (2003): 993-1022.
        https://web.archive.org/web/20220128160306/https://www.jmlr.org/papers/v3/blei03a

    Examples
    --------
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
    """

    @since("1.5.0")
    def topicsMatrix(self) -> np.ndarray:
        """Inferred topics, where each topic is represented by a distribution over terms."""
        return self.call("topicsMatrix").toArray()

    @since("1.5.0")
    def vocabSize(self) -> int:
        """Vocabulary size (number of terms or terms in the vocabulary)"""
        return self.call("vocabSize")

    def describeTopics(
        self, maxTermsPerTopic: Optional[int] = None
    ) -> List[Tuple[List[int], List[float]]]:
        """Return the topics described by weighted terms.

        .. versionadded:: 1.6.0
        .. warning:: If vocabSize and k are large, this can return a large object!

        Parameters
        ----------
        maxTermsPerTopic : int, optional
            Maximum number of terms to collect for each topic.
            (default: vocabulary size)

        Returns
        -------
        list
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
    def load(cls, sc: SparkContext, path: str) -> "LDAModel":
        """Load the LDAModel from disk.

        .. versionadded:: 1.5.0

        Parameters
        ----------
        sc : :py:class:`pyspark.SparkContext`
        path : str
            Path to where the model is stored.
        """
        if not isinstance(sc, SparkContext):
            raise TypeError("sc should be a SparkContext, got type %s" % type(sc))
        if not isinstance(path, str):
            raise TypeError("path should be a string, got type %s" % type(path))
        model = callMLlibFunc("loadLDAModel", sc, path)
        return LDAModel(model)


class LDA:
    """
    Train Latent Dirichlet Allocation (LDA) model.

    .. versionadded:: 1.5.0
    """

    @classmethod
    def train(
        cls,
        rdd: RDD[Tuple[int, "VectorLike"]],
        k: int = 10,
        maxIterations: int = 20,
        docConcentration: float = -1.0,
        topicConcentration: float = -1.0,
        seed: Optional[int] = None,
        checkpointInterval: int = 10,
        optimizer: str = "em",
    ) -> LDAModel:
        """Train a LDA model.

        .. versionadded:: 1.5.0

        Parameters
        ----------
        rdd : :py:class:`pyspark.RDD`
            RDD of documents, which are tuples of document IDs and term
            (word) count vectors. The term count vectors are "bags of
            words" with a fixed-size vocabulary (where the vocabulary size
            is the length of the vector). Document IDs must be unique
            and >= 0.
        k : int, optional
            Number of topics to infer, i.e., the number of soft cluster
            centers.
            (default: 10)
        maxIterations : int, optional
            Maximum number of iterations allowed.
            (default: 20)
        docConcentration : float, optional
            Concentration parameter (commonly named "alpha") for the prior
            placed on documents' distributions over topics ("theta").
            (default: -1.0)
        topicConcentration : float, optional
            Concentration parameter (commonly named "beta" or "eta") for
            the prior placed on topics' distributions over terms.
            (default: -1.0)
        seed : int, optional
            Random seed for cluster initialization. Set as None to generate
            seed based on system time.
            (default: None)
        checkpointInterval : int, optional
            Period (in iterations) between checkpoints.
            (default: 10)
        optimizer : str, optional
            LDAOptimizer used to perform the actual calculation. Currently
            "em", "online" are supported.
            (default: "em")
        """
        model = callMLlibFunc(
            "trainLDAModel",
            rdd,
            k,
            maxIterations,
            docConcentration,
            topicConcentration,
            seed,
            checkpointInterval,
            optimizer,
        )
        return LDAModel(model)


def _test() -> None:
    import doctest
    import numpy
    import pyspark.mllib.clustering

    try:
        # Numpy 1.14+ changed it's string format.
        numpy.set_printoptions(legacy="1.13")
    except TypeError:
        pass
    globs = pyspark.mllib.clustering.__dict__.copy()
    globs["sc"] = SparkContext("local[4]", "PythonTest", batchSize=2)
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    globs["sc"].stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
