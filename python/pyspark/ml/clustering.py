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
import warnings
from typing import Any, Dict, List, Optional, TYPE_CHECKING
import functools

import numpy as np

from pyspark import since, keyword_only
from pyspark.ml.param.shared import (
    HasMaxIter,
    HasFeaturesCol,
    HasSeed,
    HasPredictionCol,
    HasAggregationDepth,
    HasWeightCol,
    HasTol,
    HasProbabilityCol,
    HasDistanceMeasure,
    HasCheckpointInterval,
    HasSolver,
    HasMaxBlockSizeInMB,
    Param,
    Params,
    TypeConverters,
)
from pyspark.ml.util import (
    JavaMLWritable,
    JavaMLReadable,
    GeneralJavaMLWritable,
    HasTrainingSummary,
    try_remote_attribute_relation,
)
from pyspark.ml.wrapper import JavaEstimator, JavaModel, JavaParams, JavaWrapper
from pyspark.ml.common import inherit_doc, _java2py
from pyspark.ml.stat import MultivariateGaussian
from pyspark.sql import DataFrame
from pyspark.ml.linalg import Vector, Matrix
from pyspark.sql.utils import is_remote

if TYPE_CHECKING:
    from pyspark.ml._typing import M
    from py4j.java_gateway import JavaObject


__all__ = [
    "BisectingKMeans",
    "BisectingKMeansModel",
    "BisectingKMeansSummary",
    "KMeans",
    "KMeansModel",
    "KMeansSummary",
    "GaussianMixture",
    "GaussianMixtureModel",
    "GaussianMixtureSummary",
    "LDA",
    "LDAModel",
    "LocalLDAModel",
    "DistributedLDAModel",
    "PowerIterationClustering",
]


class ClusteringSummary(JavaWrapper):
    """
    Clustering results for a given model.

    .. versionadded:: 2.1.0
    """

    @property
    @since("2.1.0")
    def predictionCol(self) -> str:
        """
        Name for column of predicted clusters in `predictions`.
        """
        return self._call_java("predictionCol")

    @property
    @since("2.1.0")
    @try_remote_attribute_relation
    def predictions(self) -> DataFrame:
        """
        DataFrame produced by the model's `transform` method.
        """
        return self._call_java("predictions")

    @property
    @since("2.1.0")
    def featuresCol(self) -> str:
        """
        Name for column of features in `predictions`.
        """
        return self._call_java("featuresCol")

    @property
    @since("2.1.0")
    def k(self) -> int:
        """
        The number of clusters the model was trained with.
        """
        return self._call_java("k")

    @property
    @since("2.1.0")
    @try_remote_attribute_relation
    def cluster(self) -> DataFrame:
        """
        DataFrame of predicted cluster centers for each training data point.
        """
        return self._call_java("cluster")

    @property
    @since("2.1.0")
    def clusterSizes(self) -> List[int]:
        """
        Size of (number of data points in) each cluster.
        """
        return self._call_java("clusterSizes")

    @property
    @since("2.4.0")
    def numIter(self) -> int:
        """
        Number of iterations.
        """
        return self._call_java("numIter")


@inherit_doc
class _GaussianMixtureParams(
    HasMaxIter,
    HasFeaturesCol,
    HasSeed,
    HasPredictionCol,
    HasProbabilityCol,
    HasTol,
    HasAggregationDepth,
    HasWeightCol,
):
    """
    Params for :py:class:`GaussianMixture` and :py:class:`GaussianMixtureModel`.

    .. versionadded:: 3.0.0
    """

    k: Param[int] = Param(
        Params._dummy(),
        "k",
        "Number of independent Gaussians in the mixture model. " + "Must be > 1.",
        typeConverter=TypeConverters.toInt,
    )

    def __init__(self, *args: Any):
        super(_GaussianMixtureParams, self).__init__(*args)
        self._setDefault(k=2, tol=0.01, maxIter=100, aggregationDepth=2)

    @since("2.0.0")
    def getK(self) -> int:
        """
        Gets the value of `k`
        """
        return self.getOrDefault(self.k)


class GaussianMixtureModel(
    JavaModel,
    _GaussianMixtureParams,
    JavaMLWritable,
    JavaMLReadable["GaussianMixtureModel"],
    HasTrainingSummary["GaussianMixtureSummary"],
):
    """
    Model fitted by GaussianMixture.

    .. versionadded:: 2.0.0
    """

    @since("3.0.0")
    def setFeaturesCol(self, value: str) -> "GaussianMixtureModel":
        """
        Sets the value of :py:attr:`featuresCol`.
        """
        return self._set(featuresCol=value)

    @since("3.0.0")
    def setPredictionCol(self, value: str) -> "GaussianMixtureModel":
        """
        Sets the value of :py:attr:`predictionCol`.
        """
        return self._set(predictionCol=value)

    @since("3.0.0")
    def setProbabilityCol(self, value: str) -> "GaussianMixtureModel":
        """
        Sets the value of :py:attr:`probabilityCol`.
        """
        return self._set(probabilityCol=value)

    @property
    @since("2.0.0")
    def weights(self) -> List[float]:
        """
        Weight for each Gaussian distribution in the mixture.
        This is a multinomial probability distribution over the k Gaussians,
        where weights[i] is the weight for Gaussian i, and weights sum to 1.
        """
        return self._call_java("weights")

    @property
    @since("3.0.0")
    def gaussians(self) -> List[MultivariateGaussian]:
        """
        Array of :py:class:`MultivariateGaussian` where gaussians[i] represents
        the Multivariate Gaussian (Normal) Distribution for Gaussian i
        """
        from pyspark.core.context import SparkContext

        sc = SparkContext._active_spark_context
        assert sc is not None and self._java_obj is not None

        jgaussians = self._java_obj.gaussians()
        return [
            MultivariateGaussian(_java2py(sc, jgaussian.mean()), _java2py(sc, jgaussian.cov()))
            for jgaussian in jgaussians
        ]

    @property
    @since("2.0.0")
    @try_remote_attribute_relation
    def gaussiansDF(self) -> DataFrame:
        """
        Retrieve Gaussian distributions as a DataFrame.
        Each row represents a Gaussian Distribution.
        The DataFrame has two columns: mean (Vector) and cov (Matrix).
        """
        return self._call_java("gaussiansDF")

    @property
    @since("2.1.0")
    def summary(self) -> "GaussianMixtureSummary":
        """
        Gets summary (cluster assignments, cluster sizes) of the model trained on the
        training set. An exception is thrown if no summary exists.
        """
        if self.hasSummary:
            return GaussianMixtureSummary(super(GaussianMixtureModel, self).summary)
        else:
            raise RuntimeError(
                "No training summary available for this %s" % self.__class__.__name__
            )

    @since("3.0.0")
    def predict(self, value: Vector) -> int:
        """
        Predict label for the given features.
        """
        return self._call_java("predict", value)

    @since("3.0.0")
    def predictProbability(self, value: Vector) -> Vector:
        """
        Predict probability for the given features.
        """
        return self._call_java("predictProbability", value)


@inherit_doc
class GaussianMixture(
    JavaEstimator[GaussianMixtureModel],
    _GaussianMixtureParams,
    JavaMLWritable,
    JavaMLReadable["GaussianMixture"],
):
    """
    GaussianMixture clustering.
    This class performs expectation maximization for multivariate Gaussian
    Mixture Models (GMMs).  A GMM represents a composite distribution of
    independent Gaussian distributions with associated "mixing" weights
    specifying each's contribution to the composite.

    Given a set of sample points, this class will maximize the log-likelihood
    for a mixture of k Gaussians, iterating until the log-likelihood changes by
    less than convergenceTol, or until it has reached the max number of iterations.
    While this process is generally guaranteed to converge, it is not guaranteed
    to find a global optimum.

    .. versionadded:: 2.0.0

    Notes
    -----
    For high-dimensional data (with many features), this algorithm may perform poorly.
    This is due to high-dimensional data (a) making it difficult to cluster at all
    (based on statistical/theoretical arguments) and (b) numerical issues with
    Gaussian distributions.

    Examples
    --------
    >>> from pyspark.ml.linalg import Vectors

    >>> data = [(Vectors.dense([-0.1, -0.05 ]),),
    ...         (Vectors.dense([-0.01, -0.1]),),
    ...         (Vectors.dense([0.9, 0.8]),),
    ...         (Vectors.dense([0.75, 0.935]),),
    ...         (Vectors.dense([-0.83, -0.68]),),
    ...         (Vectors.dense([-0.91, -0.76]),)]
    >>> df = spark.createDataFrame(data, ["features"])
    >>> gm = GaussianMixture(k=3, tol=0.0001, seed=10)
    >>> gm.getMaxIter()
    100
    >>> gm.setMaxIter(30)
    GaussianMixture...
    >>> gm.getMaxIter()
    30
    >>> model = gm.fit(df)
    >>> model.getAggregationDepth()
    2
    >>> model.getFeaturesCol()
    'features'
    >>> model.setPredictionCol("newPrediction")
    GaussianMixtureModel...
    >>> model.predict(df.head().features)
    2
    >>> model.predictProbability(df.head().features)
    DenseVector([0.0, 0.0, 1.0])
    >>> model.hasSummary
    True
    >>> summary = model.summary
    >>> summary.k
    3
    >>> summary.clusterSizes
    [2, 2, 2]
    >>> weights = model.weights
    >>> len(weights)
    3
    >>> gaussians = model.gaussians
    >>> len(gaussians)
    3
    >>> gaussians[0].mean
    DenseVector([0.825, 0.8675])
    >>> gaussians[0].cov
    DenseMatrix(2, 2, [0.0056, -0.0051, -0.0051, 0.0046], 0)
    >>> gaussians[1].mean
    DenseVector([-0.87, -0.72])
    >>> gaussians[1].cov
    DenseMatrix(2, 2, [0.0016, 0.0016, 0.0016, 0.0016], 0)
    >>> gaussians[2].mean
    DenseVector([-0.055, -0.075])
    >>> gaussians[2].cov
    DenseMatrix(2, 2, [0.002, -0.0011, -0.0011, 0.0006], 0)
    >>> model.gaussiansDF.select("mean").head()
    Row(mean=DenseVector([0.825, 0.8675]))
    >>> model.gaussiansDF.select("cov").head()
    Row(cov=DenseMatrix(2, 2, [0.0056, -0.0051, -0.0051, 0.0046], False))
    >>> transformed = model.transform(df).select("features", "newPrediction")
    >>> rows = transformed.collect()
    >>> rows[4].newPrediction == rows[5].newPrediction
    True
    >>> rows[2].newPrediction == rows[3].newPrediction
    True
    >>> gmm_path = temp_path + "/gmm"
    >>> gm.save(gmm_path)
    >>> gm2 = GaussianMixture.load(gmm_path)
    >>> gm2.getK()
    3
    >>> model_path = temp_path + "/gmm_model"
    >>> model.save(model_path)
    >>> model2 = GaussianMixtureModel.load(model_path)
    >>> model2.hasSummary
    False
    >>> model2.weights == model.weights
    True
    >>> model2.gaussians[0].mean == model.gaussians[0].mean
    True
    >>> model2.gaussians[0].cov == model.gaussians[0].cov
    True
    >>> model2.gaussians[1].mean == model.gaussians[1].mean
    True
    >>> model2.gaussians[1].cov == model.gaussians[1].cov
    True
    >>> model2.gaussians[2].mean == model.gaussians[2].mean
    True
    >>> model2.gaussians[2].cov == model.gaussians[2].cov
    True
    >>> model2.gaussiansDF.select("mean").head()
    Row(mean=DenseVector([0.825, 0.8675]))
    >>> model2.gaussiansDF.select("cov").head()
    Row(cov=DenseMatrix(2, 2, [0.0056, -0.0051, -0.0051, 0.0046], False))
    >>> model.transform(df).take(1) == model2.transform(df).take(1)
    True
    >>> gm2.setWeightCol("weight")
    GaussianMixture...
    """

    _input_kwargs: Dict[str, Any]

    @keyword_only
    def __init__(
        self,
        *,
        featuresCol: str = "features",
        predictionCol: str = "prediction",
        k: int = 2,
        probabilityCol: str = "probability",
        tol: float = 0.01,
        maxIter: int = 100,
        seed: Optional[int] = None,
        aggregationDepth: int = 2,
        weightCol: Optional[str] = None,
    ):
        """
        __init__(self, \\*, featuresCol="features", predictionCol="prediction", k=2, \
                 probabilityCol="probability", tol=0.01, maxIter=100, seed=None, \
                 aggregationDepth=2, weightCol=None)
        """
        super(GaussianMixture, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.clustering.GaussianMixture", self.uid
        )
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _create_model(self, java_model: "JavaObject") -> "GaussianMixtureModel":
        return GaussianMixtureModel(java_model)

    @keyword_only
    @since("2.0.0")
    def setParams(
        self,
        *,
        featuresCol: str = "features",
        predictionCol: str = "prediction",
        k: int = 2,
        probabilityCol: str = "probability",
        tol: float = 0.01,
        maxIter: int = 100,
        seed: Optional[int] = None,
        aggregationDepth: int = 2,
        weightCol: Optional[str] = None,
    ) -> "GaussianMixture":
        """
        setParams(self, \\*, featuresCol="features", predictionCol="prediction", k=2, \
                  probabilityCol="probability", tol=0.01, maxIter=100, seed=None, \
                  aggregationDepth=2, weightCol=None)

        Sets params for GaussianMixture.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("2.0.0")
    def setK(self, value: int) -> "GaussianMixture":
        """
        Sets the value of :py:attr:`k`.
        """
        return self._set(k=value)

    @since("2.0.0")
    def setMaxIter(self, value: int) -> "GaussianMixture":
        """
        Sets the value of :py:attr:`maxIter`.
        """
        return self._set(maxIter=value)

    @since("2.0.0")
    def setFeaturesCol(self, value: str) -> "GaussianMixture":
        """
        Sets the value of :py:attr:`featuresCol`.
        """
        return self._set(featuresCol=value)

    @since("2.0.0")
    def setPredictionCol(self, value: str) -> "GaussianMixture":
        """
        Sets the value of :py:attr:`predictionCol`.
        """
        return self._set(predictionCol=value)

    @since("2.0.0")
    def setProbabilityCol(self, value: str) -> "GaussianMixture":
        """
        Sets the value of :py:attr:`probabilityCol`.
        """
        return self._set(probabilityCol=value)

    @since("3.0.0")
    def setWeightCol(self, value: str) -> "GaussianMixture":
        """
        Sets the value of :py:attr:`weightCol`.
        """
        return self._set(weightCol=value)

    @since("2.0.0")
    def setSeed(self, value: int) -> "GaussianMixture":
        """
        Sets the value of :py:attr:`seed`.
        """
        return self._set(seed=value)

    @since("2.0.0")
    def setTol(self, value: float) -> "GaussianMixture":
        """
        Sets the value of :py:attr:`tol`.
        """
        return self._set(tol=value)

    @since("3.0.0")
    def setAggregationDepth(self, value: int) -> "GaussianMixture":
        """
        Sets the value of :py:attr:`aggregationDepth`.
        """
        return self._set(aggregationDepth=value)


class GaussianMixtureSummary(ClusteringSummary):
    """
    Gaussian mixture clustering results for a given model.

    .. versionadded:: 2.1.0
    """

    @property
    @since("2.1.0")
    def probabilityCol(self) -> str:
        """
        Name for column of predicted probability of each cluster in `predictions`.
        """
        return self._call_java("probabilityCol")

    @property
    @since("2.1.0")
    @try_remote_attribute_relation
    def probability(self) -> DataFrame:
        """
        DataFrame of probabilities of each cluster for each training data point.
        """
        return self._call_java("probability")

    @property
    @since("2.2.0")
    def logLikelihood(self) -> float:
        """
        Total log-likelihood for this model on the given data.
        """
        return self._call_java("logLikelihood")


class KMeansSummary(ClusteringSummary):
    """
    Summary of KMeans.

    .. versionadded:: 2.1.0
    """

    @property
    @since("2.4.0")
    def trainingCost(self) -> float:
        """
        K-means cost (sum of squared distances to the nearest centroid for all points in the
        training dataset). This is equivalent to sklearn's inertia.
        """
        return self._call_java("trainingCost")


@inherit_doc
class _KMeansParams(
    HasMaxIter,
    HasFeaturesCol,
    HasSeed,
    HasPredictionCol,
    HasTol,
    HasDistanceMeasure,
    HasWeightCol,
    HasSolver,
    HasMaxBlockSizeInMB,
):
    """
    Params for :py:class:`KMeans` and :py:class:`KMeansModel`.

    .. versionadded:: 3.0.0
    """

    k: Param[int] = Param(
        Params._dummy(),
        "k",
        "The number of clusters to create. Must be > 1.",
        typeConverter=TypeConverters.toInt,
    )
    initMode: Param[str] = Param(
        Params._dummy(),
        "initMode",
        'The initialization algorithm. This can be either "random" to '
        + 'choose random points as initial cluster centers, or "k-means||" '
        + "to use a parallel variant of k-means++",
        typeConverter=TypeConverters.toString,
    )
    initSteps: Param[int] = Param(
        Params._dummy(),
        "initSteps",
        "The number of steps for k-means|| " + "initialization mode. Must be > 0.",
        typeConverter=TypeConverters.toInt,
    )
    solver: Param[str] = Param(
        Params._dummy(),
        "solver",
        "The solver algorithm for optimization. Supported " + "options: auto, row, block.",
        typeConverter=TypeConverters.toString,
    )

    def __init__(self, *args: Any):
        super(_KMeansParams, self).__init__(*args)
        self._setDefault(
            k=2,
            initMode="k-means||",
            initSteps=2,
            tol=1e-4,
            maxIter=20,
            distanceMeasure="euclidean",
            solver="auto",
            maxBlockSizeInMB=0.0,
        )

    @since("1.5.0")
    def getK(self) -> int:
        """
        Gets the value of `k`
        """
        return self.getOrDefault(self.k)

    @since("1.5.0")
    def getInitMode(self) -> str:
        """
        Gets the value of `initMode`
        """
        return self.getOrDefault(self.initMode)

    @since("1.5.0")
    def getInitSteps(self) -> int:
        """
        Gets the value of `initSteps`
        """
        return self.getOrDefault(self.initSteps)


class KMeansModel(
    JavaModel,
    _KMeansParams,
    GeneralJavaMLWritable,
    JavaMLReadable["KMeansModel"],
    HasTrainingSummary["KMeansSummary"],
):
    """
    Model fitted by KMeans.

    .. versionadded:: 1.5.0
    """

    @since("3.0.0")
    def setFeaturesCol(self, value: str) -> "KMeansModel":
        """
        Sets the value of :py:attr:`featuresCol`.
        """
        return self._set(featuresCol=value)

    @since("3.0.0")
    def setPredictionCol(self, value: str) -> "KMeansModel":
        """
        Sets the value of :py:attr:`predictionCol`.
        """
        return self._set(predictionCol=value)

    @since("1.5.0")
    def clusterCenters(self) -> List[np.ndarray]:
        """Get the cluster centers, represented as a list of NumPy arrays."""
        matrix = self._call_java("clusterCenterMatrix")
        return [vec for vec in matrix.toArray()]

    @property
    @since("2.1.0")
    def summary(self) -> KMeansSummary:
        """
        Gets summary (cluster assignments, cluster sizes) of the model trained on the
        training set. An exception is thrown if no summary exists.
        """
        if self.hasSummary:
            return KMeansSummary(super(KMeansModel, self).summary)
        else:
            raise RuntimeError(
                "No training summary available for this %s" % self.__class__.__name__
            )

    @since("3.0.0")
    def predict(self, value: Vector) -> int:
        """
        Predict label for the given features.
        """
        return self._call_java("predict", value)


@inherit_doc
class KMeans(JavaEstimator[KMeansModel], _KMeansParams, JavaMLWritable, JavaMLReadable["KMeans"]):
    """
    K-means clustering with a k-means++ like initialization mode
    (the k-means|| algorithm by Bahmani et al).

    .. versionadded:: 1.5.0

    Examples
    --------
    >>> from pyspark.ml.linalg import Vectors
    >>> data = [(Vectors.dense([0.0, 0.0]), 2.0), (Vectors.dense([1.0, 1.0]), 2.0),
    ...         (Vectors.dense([9.0, 8.0]), 2.0), (Vectors.dense([8.0, 9.0]), 2.0)]
    >>> df = spark.createDataFrame(data, ["features", "weighCol"])
    >>> kmeans = KMeans(k=2)
    >>> kmeans.setSeed(1)
    KMeans...
    >>> kmeans.setWeightCol("weighCol")
    KMeans...
    >>> kmeans.setMaxIter(10)
    KMeans...
    >>> kmeans.getMaxIter()
    10
    >>> kmeans.clear(kmeans.maxIter)
    >>> kmeans.getSolver()
    'auto'
    >>> model = kmeans.fit(df)
    >>> model.getMaxBlockSizeInMB()
    0.0
    >>> model.getDistanceMeasure()
    'euclidean'
    >>> model.setPredictionCol("newPrediction")
    KMeansModel...
    >>> model.predict(df.head().features)
    0
    >>> centers = model.clusterCenters()
    >>> len(centers)
    2
    >>> transformed = model.transform(df).select("features", "newPrediction")
    >>> rows = transformed.collect()
    >>> rows[0].newPrediction == rows[1].newPrediction
    True
    >>> rows[2].newPrediction == rows[3].newPrediction
    True
    >>> model.hasSummary
    True
    >>> summary = model.summary
    >>> summary.k
    2
    >>> summary.clusterSizes
    [2, 2]
    >>> summary.trainingCost
    4.0
    >>> kmeans_path = temp_path + "/kmeans"
    >>> kmeans.save(kmeans_path)
    >>> kmeans2 = KMeans.load(kmeans_path)
    >>> kmeans2.getK()
    2
    >>> model_path = temp_path + "/kmeans_model"
    >>> model.save(model_path)
    >>> model2 = KMeansModel.load(model_path)
    >>> model2.hasSummary
    False
    >>> model.clusterCenters()[0] == model2.clusterCenters()[0]
    array([ True,  True], dtype=bool)
    >>> model.clusterCenters()[1] == model2.clusterCenters()[1]
    array([ True,  True], dtype=bool)
    >>> model.transform(df).take(1) == model2.transform(df).take(1)
    True
    """

    _input_kwargs: Dict[str, Any]

    @keyword_only
    def __init__(
        self,
        *,
        featuresCol: str = "features",
        predictionCol: str = "prediction",
        k: int = 2,
        initMode: str = "k-means||",
        initSteps: int = 2,
        tol: float = 1e-4,
        maxIter: int = 20,
        seed: Optional[int] = None,
        distanceMeasure: str = "euclidean",
        weightCol: Optional[str] = None,
        solver: str = "auto",
        maxBlockSizeInMB: float = 0.0,
    ):
        """
        __init__(self, \\*, featuresCol="features", predictionCol="prediction", k=2, \
                 initMode="k-means||", initSteps=2, tol=1e-4, maxIter=20, seed=None, \
                 distanceMeasure="euclidean", weightCol=None, solver="auto", \
                 maxBlockSizeInMB=0.0)
        """
        super(KMeans, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.clustering.KMeans", self.uid)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _create_model(self, java_model: "JavaObject") -> KMeansModel:
        return KMeansModel(java_model)

    @keyword_only
    @since("1.5.0")
    def setParams(
        self,
        *,
        featuresCol: str = "features",
        predictionCol: str = "prediction",
        k: int = 2,
        initMode: str = "k-means||",
        initSteps: int = 2,
        tol: float = 1e-4,
        maxIter: int = 20,
        seed: Optional[int] = None,
        distanceMeasure: str = "euclidean",
        weightCol: Optional[str] = None,
        solver: str = "auto",
        maxBlockSizeInMB: float = 0.0,
    ) -> "KMeans":
        """
        setParams(self, \\*, featuresCol="features", predictionCol="prediction", k=2, \
                  initMode="k-means||", initSteps=2, tol=1e-4, maxIter=20, seed=None, \
                  distanceMeasure="euclidean", weightCol=None, solver="auto", \
                  maxBlockSizeInMB=0.0)

        Sets params for KMeans.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("1.5.0")
    def setK(self, value: int) -> "KMeans":
        """
        Sets the value of :py:attr:`k`.
        """
        return self._set(k=value)

    @since("1.5.0")
    def setInitMode(self, value: str) -> "KMeans":
        """
        Sets the value of :py:attr:`initMode`.
        """
        return self._set(initMode=value)

    @since("1.5.0")
    def setInitSteps(self, value: int) -> "KMeans":
        """
        Sets the value of :py:attr:`initSteps`.
        """
        return self._set(initSteps=value)

    @since("2.4.0")
    def setDistanceMeasure(self, value: str) -> "KMeans":
        """
        Sets the value of :py:attr:`distanceMeasure`.
        """
        return self._set(distanceMeasure=value)

    @since("1.5.0")
    def setMaxIter(self, value: int) -> "KMeans":
        """
        Sets the value of :py:attr:`maxIter`.
        """
        return self._set(maxIter=value)

    @since("1.5.0")
    def setFeaturesCol(self, value: str) -> "KMeans":
        """
        Sets the value of :py:attr:`featuresCol`.
        """
        return self._set(featuresCol=value)

    @since("1.5.0")
    def setPredictionCol(self, value: str) -> "KMeans":
        """
        Sets the value of :py:attr:`predictionCol`.
        """
        return self._set(predictionCol=value)

    @since("1.5.0")
    def setSeed(self, value: int) -> "KMeans":
        """
        Sets the value of :py:attr:`seed`.
        """
        return self._set(seed=value)

    @since("1.5.0")
    def setTol(self, value: float) -> "KMeans":
        """
        Sets the value of :py:attr:`tol`.
        """
        return self._set(tol=value)

    @since("3.0.0")
    def setWeightCol(self, value: str) -> "KMeans":
        """
        Sets the value of :py:attr:`weightCol`.
        """
        return self._set(weightCol=value)

    @since("3.4.0")
    def setSolver(self, value: str) -> "KMeans":
        """
        Sets the value of :py:attr:`solver`.
        """
        return self._set(solver=value)

    @since("3.4.0")
    def setMaxBlockSizeInMB(self, value: float) -> "KMeans":
        """
        Sets the value of :py:attr:`maxBlockSizeInMB`.
        """
        return self._set(maxBlockSizeInMB=value)


@inherit_doc
class _BisectingKMeansParams(
    HasMaxIter,
    HasFeaturesCol,
    HasSeed,
    HasPredictionCol,
    HasDistanceMeasure,
    HasWeightCol,
):
    """
    Params for :py:class:`BisectingKMeans` and :py:class:`BisectingKMeansModel`.

    .. versionadded:: 3.0.0
    """

    k: Param[int] = Param(
        Params._dummy(),
        "k",
        "The desired number of leaf clusters. Must be > 1.",
        typeConverter=TypeConverters.toInt,
    )
    minDivisibleClusterSize: Param[float] = Param(
        Params._dummy(),
        "minDivisibleClusterSize",
        "The minimum number of points (if >= 1.0) or the minimum "
        + "proportion of points (if < 1.0) of a divisible cluster.",
        typeConverter=TypeConverters.toFloat,
    )

    def __init__(self, *args: Any):
        super(_BisectingKMeansParams, self).__init__(*args)
        self._setDefault(maxIter=20, k=4, minDivisibleClusterSize=1.0)

    @since("2.0.0")
    def getK(self) -> int:
        """
        Gets the value of `k` or its default value.
        """
        return self.getOrDefault(self.k)

    @since("2.0.0")
    def getMinDivisibleClusterSize(self) -> float:
        """
        Gets the value of `minDivisibleClusterSize` or its default value.
        """
        return self.getOrDefault(self.minDivisibleClusterSize)


class BisectingKMeansModel(
    JavaModel,
    _BisectingKMeansParams,
    JavaMLWritable,
    JavaMLReadable["BisectingKMeansModel"],
    HasTrainingSummary["BisectingKMeansSummary"],
):
    """
    Model fitted by BisectingKMeans.

    .. versionadded:: 2.0.0
    """

    @since("3.0.0")
    def setFeaturesCol(self, value: str) -> "BisectingKMeansModel":
        """
        Sets the value of :py:attr:`featuresCol`.
        """
        return self._set(featuresCol=value)

    @since("3.0.0")
    def setPredictionCol(self, value: str) -> "BisectingKMeansModel":
        """
        Sets the value of :py:attr:`predictionCol`.
        """
        return self._set(predictionCol=value)

    @since("2.0.0")
    def clusterCenters(self) -> List[np.ndarray]:
        """Get the cluster centers, represented as a list of NumPy arrays."""
        matrix = self._call_java("clusterCenterMatrix")
        return [vec for vec in matrix.toArray()]

    @since("2.0.0")
    def computeCost(self, dataset: DataFrame) -> float:
        """
        Computes the sum of squared distances between the input points
        and their corresponding cluster centers.

        .. deprecated:: 3.0.0
            It will be removed in future versions. Use :py:class:`ClusteringEvaluator` instead.
            You can also get the cost on the training dataset in the summary.
        """
        warnings.warn(
            "Deprecated in 3.0.0. It will be removed in future versions. Use "
            "ClusteringEvaluator instead. You can also get the cost on the training "
            "dataset in the summary.",
            FutureWarning,
        )
        return self._call_java("computeCost", dataset)

    @property
    @since("2.1.0")
    def summary(self) -> "BisectingKMeansSummary":
        """
        Gets summary (cluster assignments, cluster sizes) of the model trained on the
        training set. An exception is thrown if no summary exists.
        """
        if self.hasSummary:
            return BisectingKMeansSummary(super(BisectingKMeansModel, self).summary)
        else:
            raise RuntimeError(
                "No training summary available for this %s" % self.__class__.__name__
            )

    @since("3.0.0")
    def predict(self, value: Vector) -> int:
        """
        Predict label for the given features.
        """
        return self._call_java("predict", value)


@inherit_doc
class BisectingKMeans(
    JavaEstimator[BisectingKMeansModel],
    _BisectingKMeansParams,
    JavaMLWritable,
    JavaMLReadable["BisectingKMeans"],
):
    """
    A bisecting k-means algorithm based on the paper "A comparison of document clustering
    techniques" by Steinbach, Karypis, and Kumar, with modification to fit Spark.
    The algorithm starts from a single cluster that contains all points.
    Iteratively it finds divisible clusters on the bottom level and bisects each of them using
    k-means, until there are `k` leaf clusters in total or no leaf clusters are divisible.
    The bisecting steps of clusters on the same level are grouped together to increase parallelism.
    If bisecting all divisible clusters on the bottom level would result more than `k` leaf
    clusters, larger clusters get higher priority.

    .. versionadded:: 2.0.0

    Examples
    --------
    >>> from pyspark.ml.linalg import Vectors
    >>> data = [(Vectors.dense([0.0, 0.0]), 2.0), (Vectors.dense([1.0, 1.0]), 2.0),
    ...         (Vectors.dense([9.0, 8.0]), 2.0), (Vectors.dense([8.0, 9.0]), 2.0)]
    >>> df = spark.createDataFrame(data, ["features", "weighCol"])
    >>> bkm = BisectingKMeans(k=2, minDivisibleClusterSize=1.0)
    >>> bkm.setMaxIter(10)
    BisectingKMeans...
    >>> bkm.getMaxIter()
    10
    >>> bkm.clear(bkm.maxIter)
    >>> bkm.setSeed(1)
    BisectingKMeans...
    >>> bkm.setWeightCol("weighCol")
    BisectingKMeans...
    >>> bkm.getSeed()
    1
    >>> bkm.clear(bkm.seed)
    >>> model = bkm.fit(df)
    >>> model.getMaxIter()
    20
    >>> model.setPredictionCol("newPrediction")
    BisectingKMeansModel...
    >>> model.predict(df.head().features)
    0
    >>> centers = model.clusterCenters()
    >>> len(centers)
    2
    >>> model.computeCost(df)
    2.0
    >>> model.hasSummary
    True
    >>> summary = model.summary
    >>> summary.k
    2
    >>> summary.clusterSizes
    [2, 2]
    >>> summary.trainingCost
    4.000...
    >>> transformed = model.transform(df).select("features", "newPrediction")
    >>> rows = transformed.collect()
    >>> rows[0].newPrediction == rows[1].newPrediction
    True
    >>> rows[2].newPrediction == rows[3].newPrediction
    True
    >>> bkm_path = temp_path + "/bkm"
    >>> bkm.save(bkm_path)
    >>> bkm2 = BisectingKMeans.load(bkm_path)
    >>> bkm2.getK()
    2
    >>> bkm2.getDistanceMeasure()
    'euclidean'
    >>> model_path = temp_path + "/bkm_model"
    >>> model.save(model_path)
    >>> model2 = BisectingKMeansModel.load(model_path)
    >>> model2.hasSummary
    False
    >>> model.clusterCenters()[0] == model2.clusterCenters()[0]
    array([ True,  True], dtype=bool)
    >>> model.clusterCenters()[1] == model2.clusterCenters()[1]
    array([ True,  True], dtype=bool)
    >>> model.transform(df).take(1) == model2.transform(df).take(1)
    True
    """

    _input_kwargs: Dict[str, Any]

    @keyword_only
    def __init__(
        self,
        *,
        featuresCol: str = "features",
        predictionCol: str = "prediction",
        maxIter: int = 20,
        seed: Optional[int] = None,
        k: int = 4,
        minDivisibleClusterSize: float = 1.0,
        distanceMeasure: str = "euclidean",
        weightCol: Optional[str] = None,
    ):
        """
        __init__(self, \\*, featuresCol="features", predictionCol="prediction", maxIter=20, \
                 seed=None, k=4, minDivisibleClusterSize=1.0, distanceMeasure="euclidean", \
                 weightCol=None)
        """
        super(BisectingKMeans, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.clustering.BisectingKMeans", self.uid
        )
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("2.0.0")
    def setParams(
        self,
        *,
        featuresCol: str = "features",
        predictionCol: str = "prediction",
        maxIter: int = 20,
        seed: Optional[int] = None,
        k: int = 4,
        minDivisibleClusterSize: float = 1.0,
        distanceMeasure: str = "euclidean",
        weightCol: Optional[str] = None,
    ) -> "BisectingKMeans":
        """
        setParams(self, \\*, featuresCol="features", predictionCol="prediction", maxIter=20, \
                  seed=None, k=4, minDivisibleClusterSize=1.0, distanceMeasure="euclidean", \
                  weightCol=None)
        Sets params for BisectingKMeans.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("2.0.0")
    def setK(self, value: int) -> "BisectingKMeans":
        """
        Sets the value of :py:attr:`k`.
        """
        return self._set(k=value)

    @since("2.0.0")
    def setMinDivisibleClusterSize(self, value: float) -> "BisectingKMeans":
        """
        Sets the value of :py:attr:`minDivisibleClusterSize`.
        """
        return self._set(minDivisibleClusterSize=value)

    @since("2.4.0")
    def setDistanceMeasure(self, value: str) -> "BisectingKMeans":
        """
        Sets the value of :py:attr:`distanceMeasure`.
        """
        return self._set(distanceMeasure=value)

    @since("2.0.0")
    def setMaxIter(self, value: int) -> "BisectingKMeans":
        """
        Sets the value of :py:attr:`maxIter`.
        """
        return self._set(maxIter=value)

    @since("2.0.0")
    def setFeaturesCol(self, value: str) -> "BisectingKMeans":
        """
        Sets the value of :py:attr:`featuresCol`.
        """
        return self._set(featuresCol=value)

    @since("2.0.0")
    def setPredictionCol(self, value: str) -> "BisectingKMeans":
        """
        Sets the value of :py:attr:`predictionCol`.
        """
        return self._set(predictionCol=value)

    @since("2.0.0")
    def setSeed(self, value: int) -> "BisectingKMeans":
        """
        Sets the value of :py:attr:`seed`.
        """
        return self._set(seed=value)

    @since("3.0.0")
    def setWeightCol(self, value: str) -> "BisectingKMeans":
        """
        Sets the value of :py:attr:`weightCol`.
        """
        return self._set(weightCol=value)

    def _create_model(self, java_model: "JavaObject") -> BisectingKMeansModel:
        return BisectingKMeansModel(java_model)


class BisectingKMeansSummary(ClusteringSummary):
    """
    Bisecting KMeans clustering results for a given model.

    .. versionadded:: 2.1.0
    """

    @property
    @since("3.0.0")
    def trainingCost(self) -> float:
        """
        Sum of squared distances to the nearest centroid for all points in the training dataset.
        This is equivalent to sklearn's inertia.
        """
        return self._call_java("trainingCost")


@inherit_doc
class _LDAParams(HasMaxIter, HasFeaturesCol, HasSeed, HasCheckpointInterval):
    """
    Params for :py:class:`LDA` and :py:class:`LDAModel`.

    .. versionadded:: 3.0.0
    """

    k: Param[int] = Param(
        Params._dummy(),
        "k",
        "The number of topics (clusters) to infer. Must be > 1.",
        typeConverter=TypeConverters.toInt,
    )
    optimizer: Param[str] = Param(
        Params._dummy(),
        "optimizer",
        "Optimizer or inference algorithm used to estimate the LDA model.  "
        "Supported: online, em",
        typeConverter=TypeConverters.toString,
    )
    learningOffset: Param[float] = Param(
        Params._dummy(),
        "learningOffset",
        "A (positive) learning parameter that downweights early iterations."
        " Larger values make early iterations count less",
        typeConverter=TypeConverters.toFloat,
    )
    learningDecay: Param[float] = Param(
        Params._dummy(),
        "learningDecay",
        "Learning rate, set as an"
        "exponential decay rate. This should be between (0.5, 1.0] to "
        "guarantee asymptotic convergence.",
        typeConverter=TypeConverters.toFloat,
    )
    subsamplingRate: Param[float] = Param(
        Params._dummy(),
        "subsamplingRate",
        "Fraction of the corpus to be sampled and used in each iteration "
        "of mini-batch gradient descent, in range (0, 1].",
        typeConverter=TypeConverters.toFloat,
    )
    optimizeDocConcentration: Param[bool] = Param(
        Params._dummy(),
        "optimizeDocConcentration",
        "Indicates whether the docConcentration (Dirichlet parameter "
        "for document-topic distribution) will be optimized during "
        "training.",
        typeConverter=TypeConverters.toBoolean,
    )
    docConcentration: Param[List[float]] = Param(
        Params._dummy(),
        "docConcentration",
        'Concentration parameter (commonly named "alpha") for the '
        'prior placed on documents\' distributions over topics ("theta").',
        typeConverter=TypeConverters.toListFloat,
    )
    topicConcentration: Param[float] = Param(
        Params._dummy(),
        "topicConcentration",
        'Concentration parameter (commonly named "beta" or "eta") for '
        "the prior placed on topic' distributions over terms.",
        typeConverter=TypeConverters.toFloat,
    )
    topicDistributionCol: Param[str] = Param(
        Params._dummy(),
        "topicDistributionCol",
        "Output column with estimates of the topic mixture distribution "
        'for each document (often called "theta" in the literature). '
        "Returns a vector of zeros for an empty document.",
        typeConverter=TypeConverters.toString,
    )
    keepLastCheckpoint: Param[bool] = Param(
        Params._dummy(),
        "keepLastCheckpoint",
        "(For EM optimizer) If using checkpointing, this indicates whether"
        " to keep the last checkpoint. If false, then the checkpoint will be"
        " deleted. Deleting the checkpoint can cause failures if a data"
        " partition is lost, so set this bit with care.",
        TypeConverters.toBoolean,
    )

    def __init__(self, *args: Any):
        super(_LDAParams, self).__init__(*args)
        self._setDefault(
            maxIter=20,
            checkpointInterval=10,
            k=10,
            optimizer="online",
            learningOffset=1024.0,
            learningDecay=0.51,
            subsamplingRate=0.05,
            optimizeDocConcentration=True,
            topicDistributionCol="topicDistribution",
            keepLastCheckpoint=True,
        )

    @since("2.0.0")
    def getK(self) -> int:
        """
        Gets the value of :py:attr:`k` or its default value.
        """
        return self.getOrDefault(self.k)

    @since("2.0.0")
    def getOptimizer(self) -> str:
        """
        Gets the value of :py:attr:`optimizer` or its default value.
        """
        return self.getOrDefault(self.optimizer)

    @since("2.0.0")
    def getLearningOffset(self) -> float:
        """
        Gets the value of :py:attr:`learningOffset` or its default value.
        """
        return self.getOrDefault(self.learningOffset)

    @since("2.0.0")
    def getLearningDecay(self) -> float:
        """
        Gets the value of :py:attr:`learningDecay` or its default value.
        """
        return self.getOrDefault(self.learningDecay)

    @since("2.0.0")
    def getSubsamplingRate(self) -> float:
        """
        Gets the value of :py:attr:`subsamplingRate` or its default value.
        """
        return self.getOrDefault(self.subsamplingRate)

    @since("2.0.0")
    def getOptimizeDocConcentration(self) -> bool:
        """
        Gets the value of :py:attr:`optimizeDocConcentration` or its default value.
        """
        return self.getOrDefault(self.optimizeDocConcentration)

    @since("2.0.0")
    def getDocConcentration(self) -> List[float]:
        """
        Gets the value of :py:attr:`docConcentration` or its default value.
        """
        return self.getOrDefault(self.docConcentration)

    @since("2.0.0")
    def getTopicConcentration(self) -> float:
        """
        Gets the value of :py:attr:`topicConcentration` or its default value.
        """
        return self.getOrDefault(self.topicConcentration)

    @since("2.0.0")
    def getTopicDistributionCol(self) -> str:
        """
        Gets the value of :py:attr:`topicDistributionCol` or its default value.
        """
        return self.getOrDefault(self.topicDistributionCol)

    @since("2.0.0")
    def getKeepLastCheckpoint(self) -> bool:
        """
        Gets the value of :py:attr:`keepLastCheckpoint` or its default value.
        """
        return self.getOrDefault(self.keepLastCheckpoint)


@inherit_doc
class LDAModel(JavaModel, _LDAParams):
    """
    Latent Dirichlet Allocation (LDA) model.
    This abstraction permits for different underlying representations,
    including local and distributed data structures.

    .. versionadded:: 2.0.0
    """

    @since("3.0.0")
    def setFeaturesCol(self: "M", value: str) -> "M":
        """
        Sets the value of :py:attr:`featuresCol`.
        """
        return self._set(featuresCol=value)

    @since("3.0.0")
    def setSeed(self: "M", value: int) -> "M":
        """
        Sets the value of :py:attr:`seed`.
        """
        return self._set(seed=value)

    @since("3.0.0")
    def setTopicDistributionCol(self: "M", value: str) -> "M":
        """
        Sets the value of :py:attr:`topicDistributionCol`.
        """
        return self._set(topicDistributionCol=value)

    @since("2.0.0")
    def isDistributed(self) -> bool:
        """
        Indicates whether this instance is of type DistributedLDAModel
        """
        return self._call_java("isDistributed")

    @since("2.0.0")
    def vocabSize(self) -> int:
        """Vocabulary size (number of terms or words in the vocabulary)"""
        return self._call_java("vocabSize")

    @since("2.0.0")
    def topicsMatrix(self) -> Matrix:
        """
        Inferred topics, where each topic is represented by a distribution over terms.
        This is a matrix of size vocabSize x k, where each column is a topic.
        No guarantees are given about the ordering of the topics.

        .. warning:: If this model is actually a :py:class:`DistributedLDAModel`
            instance produced by the Expectation-Maximization ("em") `optimizer`,
            then this method could involve collecting a large amount of data
            to the driver (on the order of vocabSize x k).
        """
        return self._call_java("topicsMatrix")

    @since("2.0.0")
    def logLikelihood(self, dataset: DataFrame) -> float:
        """
        Calculates a lower bound on the log likelihood of the entire corpus.
        See Equation (16) in the Online LDA paper (Hoffman et al., 2010).

        .. warning:: If this model is an instance of :py:class:`DistributedLDAModel` (produced when
            :py:attr:`optimizer` is set to "em"), this involves collecting a large
            :py:func:`topicsMatrix` to the driver. This implementation may be changed in the future.
        """
        return self._call_java("logLikelihood", dataset)

    @since("2.0.0")
    def logPerplexity(self, dataset: DataFrame) -> float:
        """
        Calculate an upper bound on perplexity.  (Lower is better.)
        See Equation (16) in the Online LDA paper (Hoffman et al., 2010).

        .. warning:: If this model is an instance of :py:class:`DistributedLDAModel` (produced when
            :py:attr:`optimizer` is set to "em"), this involves collecting a large
            :py:func:`topicsMatrix` to the driver. This implementation may be changed in the future.
        """
        return self._call_java("logPerplexity", dataset)

    @since("2.0.0")
    @try_remote_attribute_relation
    def describeTopics(self, maxTermsPerTopic: int = 10) -> DataFrame:
        """
        Return the topics described by their top-weighted terms.
        """
        return self._call_java("describeTopics", maxTermsPerTopic)

    @since("2.0.0")
    def estimatedDocConcentration(self) -> Vector:
        """
        Value for :py:attr:`LDA.docConcentration` estimated from data.
        If Online LDA was used and :py:attr:`LDA.optimizeDocConcentration` was set to false,
        then this returns the fixed (given) value for the :py:attr:`LDA.docConcentration` parameter.
        """
        return self._call_java("estimatedDocConcentration")


@inherit_doc
class DistributedLDAModel(LDAModel, JavaMLReadable["DistributedLDAModel"], JavaMLWritable):
    """
    Distributed model fitted by :py:class:`LDA`.
    This type of model is currently only produced by Expectation-Maximization (EM).

    This model stores the inferred topics, the full training dataset, and the topic distribution
    for each training document.

    .. versionadded:: 2.0.0
    """

    @functools.cache
    @since("2.0.0")
    def toLocal(self) -> "LocalLDAModel":
        """
        Convert this distributed model to a local representation.  This discards info about the
        training dataset.

        .. warning:: This involves collecting a large :py:func:`topicsMatrix` to the driver.
        """
        model = LocalLDAModel(self._call_java("toLocal"))
        if is_remote():
            return model

        # SPARK-10931: Temporary fix to be removed once LDAModel defines Params
        model._create_params_from_java()
        model._transfer_params_from_java()

        return model

    @since("2.0.0")
    def trainingLogLikelihood(self) -> float:
        """
        Log likelihood of the observed tokens in the training set,
        given the current parameter estimates:
        log P(docs | topics, topic distributions for docs, Dirichlet hyperparameters)

        Notes
        -----
        - This excludes the prior; for that, use :py:func:`logPrior`.
        - Even with :py:func:`logPrior`, this is NOT the same as the data log likelihood given
            the hyperparameters.
        - This is computed from the topic distributions computed during training. If you call
            :py:func:`logLikelihood` on the same training dataset, the topic distributions
            will be computed again, possibly giving different results.
        """
        return self._call_java("trainingLogLikelihood")

    @since("2.0.0")
    def logPrior(self) -> float:
        """
        Log probability of the current parameter estimate:
        log P(topics, topic distributions for docs | alpha, eta)
        """
        return self._call_java("logPrior")

    def getCheckpointFiles(self) -> List[str]:
        """
        If using checkpointing and :py:attr:`LDA.keepLastCheckpoint` is set to true, then there may
        be saved checkpoint files.  This method is provided so that users can manage those files.

        .. versionadded:: 2.0.0

        Returns
        -------
        list
            List of checkpoint files from training

        Notes
        -----
        Removing the checkpoints can cause failures if a partition is lost and is needed
        by certain :py:class:`DistributedLDAModel` methods.  Reference counting will clean up
        the checkpoints when this model and derivative data go out of scope.
        """
        return self._call_java("getCheckpointFiles")


@inherit_doc
class LocalLDAModel(LDAModel, JavaMLReadable["LocalLDAModel"], JavaMLWritable):
    """
    Local (non-distributed) model fitted by :py:class:`LDA`.
    This model stores the inferred topics only; it does not store info about the training dataset.

    .. versionadded:: 2.0.0
    """

    pass


@inherit_doc
class LDA(JavaEstimator[LDAModel], _LDAParams, JavaMLReadable["LDA"], JavaMLWritable):
    """
    Latent Dirichlet Allocation (LDA), a topic model designed for text documents.

    Terminology:

     - "term" = "word": an element of the vocabulary
     - "token": instance of a term appearing in a document
     - "topic": multinomial distribution over terms representing some concept
     - "document": one piece of text, corresponding to one row in the input data

    Original LDA paper (journal version):
      Blei, Ng, and Jordan.  "Latent Dirichlet Allocation."  JMLR, 2003.

    Input data (featuresCol):
    LDA is given a collection of documents as input data, via the featuresCol parameter.
    Each document is specified as a :py:class:`Vector` of length vocabSize, where each entry is the
    count for the corresponding term (word) in the document.  Feature transformers such as
    :py:class:`pyspark.ml.feature.Tokenizer` and :py:class:`pyspark.ml.feature.CountVectorizer`
    can be useful for converting text to word count vectors.

    .. versionadded:: 2.0.0

    Examples
    --------
    >>> from pyspark.ml.linalg import Vectors, SparseVector
    >>> from pyspark.ml.clustering import LDA
    >>> df = spark.createDataFrame([[1, Vectors.dense([0.0, 1.0])],
    ...      [2, SparseVector(2, {0: 1.0})],], ["id", "features"])
    >>> lda = LDA(k=2, seed=1, optimizer="em")
    >>> lda.setMaxIter(10)
    LDA...
    >>> lda.getMaxIter()
    10
    >>> lda.clear(lda.maxIter)
    >>> model = lda.fit(df)
    >>> model.setSeed(1)
    DistributedLDAModel...
    >>> model.getTopicDistributionCol()
    'topicDistribution'
    >>> model.isDistributed()
    True
    >>> localModel = model.toLocal()
    >>> localModel.isDistributed()
    False
    >>> model.vocabSize()
    2
    >>> model.describeTopics().show()
    +-----+-----------+--------------------+
    |topic|termIndices|         termWeights|
    +-----+-----------+--------------------+
    |    0|     [1, 0]|[0.50401530077160...|
    |    1|     [0, 1]|[0.50401530077160...|
    +-----+-----------+--------------------+
    ...
    >>> model.topicsMatrix()
    DenseMatrix(2, 2, [0.496, 0.504, 0.504, 0.496], 0)
    >>> lda_path = temp_path + "/lda"
    >>> lda.save(lda_path)
    >>> sameLDA = LDA.load(lda_path)
    >>> distributed_model_path = temp_path + "/lda_distributed_model"
    >>> model.save(distributed_model_path)
    >>> sameModel = DistributedLDAModel.load(distributed_model_path)
    >>> local_model_path = temp_path + "/lda_local_model"
    >>> localModel.save(local_model_path)
    >>> sameLocalModel = LocalLDAModel.load(local_model_path)
    >>> model.transform(df).take(1) == sameLocalModel.transform(df).take(1)
    True
    """

    _input_kwargs: Dict[str, Any]

    @keyword_only
    def __init__(
        self,
        *,
        featuresCol: str = "features",
        maxIter: int = 20,
        seed: Optional[int] = None,
        checkpointInterval: int = 10,
        k: int = 10,
        optimizer: str = "online",
        learningOffset: float = 1024.0,
        learningDecay: float = 0.51,
        subsamplingRate: float = 0.05,
        optimizeDocConcentration: bool = True,
        docConcentration: Optional[List[float]] = None,
        topicConcentration: Optional[float] = None,
        topicDistributionCol: str = "topicDistribution",
        keepLastCheckpoint: bool = True,
    ):
        """
        __init__(self, \\*, featuresCol="features", maxIter=20, seed=None, checkpointInterval=10,\
                  k=10, optimizer="online", learningOffset=1024.0, learningDecay=0.51,\
                  subsamplingRate=0.05, optimizeDocConcentration=True,\
                  docConcentration=None, topicConcentration=None,\
                  topicDistributionCol="topicDistribution", keepLastCheckpoint=True)
        """
        super(LDA, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.clustering.LDA", self.uid)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _create_model(self, java_model: "JavaObject") -> LDAModel:
        if self.getOptimizer() == "em":
            return DistributedLDAModel(java_model)
        else:
            return LocalLDAModel(java_model)

    @keyword_only
    @since("2.0.0")
    def setParams(
        self,
        *,
        featuresCol: str = "features",
        maxIter: int = 20,
        seed: Optional[int] = None,
        checkpointInterval: int = 10,
        k: int = 10,
        optimizer: str = "online",
        learningOffset: float = 1024.0,
        learningDecay: float = 0.51,
        subsamplingRate: float = 0.05,
        optimizeDocConcentration: bool = True,
        docConcentration: Optional[List[float]] = None,
        topicConcentration: Optional[float] = None,
        topicDistributionCol: str = "topicDistribution",
        keepLastCheckpoint: bool = True,
    ) -> "LDA":
        """
        setParams(self, \\*, featuresCol="features", maxIter=20, seed=None, checkpointInterval=10,\
                  k=10, optimizer="online", learningOffset=1024.0, learningDecay=0.51,\
                  subsamplingRate=0.05, optimizeDocConcentration=True,\
                  docConcentration=None, topicConcentration=None,\
                  topicDistributionCol="topicDistribution", keepLastCheckpoint=True)

        Sets params for LDA.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("2.0.0")
    def setCheckpointInterval(self, value: int) -> "LDA":
        """
        Sets the value of :py:attr:`checkpointInterval`.
        """
        return self._set(checkpointInterval=value)

    @since("2.0.0")
    def setSeed(self, value: int) -> "LDA":
        """
        Sets the value of :py:attr:`seed`.
        """
        return self._set(seed=value)

    @since("2.0.0")
    def setK(self, value: int) -> "LDA":
        """
        Sets the value of :py:attr:`k`.

        >>> algo = LDA().setK(10)
        >>> algo.getK()
        10
        """
        return self._set(k=value)

    @since("2.0.0")
    def setOptimizer(self, value: str) -> "LDA":
        """
        Sets the value of :py:attr:`optimizer`.
        Currently only support 'em' and 'online'.

        Examples
        --------
        >>> algo = LDA().setOptimizer("em")
        >>> algo.getOptimizer()
        'em'
        """
        return self._set(optimizer=value)

    @since("2.0.0")
    def setLearningOffset(self, value: float) -> "LDA":
        """
        Sets the value of :py:attr:`learningOffset`.

        Examples
        --------
        >>> algo = LDA().setLearningOffset(100)
        >>> algo.getLearningOffset()
        100.0
        """
        return self._set(learningOffset=value)

    @since("2.0.0")
    def setLearningDecay(self, value: float) -> "LDA":
        """
        Sets the value of :py:attr:`learningDecay`.

        Examples
        --------
        >>> algo = LDA().setLearningDecay(0.1)
        >>> algo.getLearningDecay()
        0.1...
        """
        return self._set(learningDecay=value)

    @since("2.0.0")
    def setSubsamplingRate(self, value: float) -> "LDA":
        """
        Sets the value of :py:attr:`subsamplingRate`.

        Examples
        --------
        >>> algo = LDA().setSubsamplingRate(0.1)
        >>> algo.getSubsamplingRate()
        0.1...
        """
        return self._set(subsamplingRate=value)

    @since("2.0.0")
    def setOptimizeDocConcentration(self, value: bool) -> "LDA":
        """
        Sets the value of :py:attr:`optimizeDocConcentration`.

        Examples
        --------
        >>> algo = LDA().setOptimizeDocConcentration(True)
        >>> algo.getOptimizeDocConcentration()
        True
        """
        return self._set(optimizeDocConcentration=value)

    @since("2.0.0")
    def setDocConcentration(self, value: List[float]) -> "LDA":
        """
        Sets the value of :py:attr:`docConcentration`.

        Examples
        --------
        >>> algo = LDA().setDocConcentration([0.1, 0.2])
        >>> algo.getDocConcentration()
        [0.1..., 0.2...]
        """
        return self._set(docConcentration=value)

    @since("2.0.0")
    def setTopicConcentration(self, value: float) -> "LDA":
        """
        Sets the value of :py:attr:`topicConcentration`.

        Examples
        --------
        >>> algo = LDA().setTopicConcentration(0.5)
        >>> algo.getTopicConcentration()
        0.5...
        """
        return self._set(topicConcentration=value)

    @since("2.0.0")
    def setTopicDistributionCol(self, value: str) -> "LDA":
        """
        Sets the value of :py:attr:`topicDistributionCol`.

        Examples
        --------
        >>> algo = LDA().setTopicDistributionCol("topicDistributionCol")
        >>> algo.getTopicDistributionCol()
        'topicDistributionCol'
        """
        return self._set(topicDistributionCol=value)

    @since("2.0.0")
    def setKeepLastCheckpoint(self, value: bool) -> "LDA":
        """
        Sets the value of :py:attr:`keepLastCheckpoint`.

        Examples
        --------
        >>> algo = LDA().setKeepLastCheckpoint(False)
        >>> algo.getKeepLastCheckpoint()
        False
        """
        return self._set(keepLastCheckpoint=value)

    @since("2.0.0")
    def setMaxIter(self, value: int) -> "LDA":
        """
        Sets the value of :py:attr:`maxIter`.
        """
        return self._set(maxIter=value)

    @since("2.0.0")
    def setFeaturesCol(self, value: str) -> "LDA":
        """
        Sets the value of :py:attr:`featuresCol`.
        """
        return self._set(featuresCol=value)


@inherit_doc
class _PowerIterationClusteringParams(HasMaxIter, HasWeightCol):
    """
    Params for :py:class:`PowerIterationClustering`.

    .. versionadded:: 3.0.0
    """

    k: Param[int] = Param(
        Params._dummy(),
        "k",
        "The number of clusters to create. Must be > 1.",
        typeConverter=TypeConverters.toInt,
    )
    initMode: Param[str] = Param(
        Params._dummy(),
        "initMode",
        "The initialization algorithm. This can be either "
        + "'random' to use a random vector as vertex properties, or 'degree' to use "
        + "a normalized sum of similarities with other vertices.  Supported options: "
        + "'random' and 'degree'.",
        typeConverter=TypeConverters.toString,
    )
    srcCol: Param[str] = Param(
        Params._dummy(),
        "srcCol",
        "Name of the input column for source vertex IDs.",
        typeConverter=TypeConverters.toString,
    )
    dstCol: Param[str] = Param(
        Params._dummy(),
        "dstCol",
        "Name of the input column for destination vertex IDs.",
        typeConverter=TypeConverters.toString,
    )

    def __init__(self, *args: Any):
        super(_PowerIterationClusteringParams, self).__init__(*args)
        self._setDefault(k=2, maxIter=20, initMode="random", srcCol="src", dstCol="dst")

    @since("2.4.0")
    def getK(self) -> int:
        """
        Gets the value of :py:attr:`k` or its default value.
        """
        return self.getOrDefault(self.k)

    @since("2.4.0")
    def getInitMode(self) -> str:
        """
        Gets the value of :py:attr:`initMode` or its default value.
        """
        return self.getOrDefault(self.initMode)

    @since("2.4.0")
    def getSrcCol(self) -> str:
        """
        Gets the value of :py:attr:`srcCol` or its default value.
        """
        return self.getOrDefault(self.srcCol)

    @since("2.4.0")
    def getDstCol(self) -> str:
        """
        Gets the value of :py:attr:`dstCol` or its default value.
        """
        return self.getOrDefault(self.dstCol)


@inherit_doc
class PowerIterationClustering(
    _PowerIterationClusteringParams,
    JavaParams,
    JavaMLReadable["PowerIterationClustering"],
    JavaMLWritable,
):
    """
    Power Iteration Clustering (PIC), a scalable graph clustering algorithm developed by
    `Lin and Cohen <http://www.cs.cmu.edu/~frank/papers/icml2010-pic-final.pdf>`_. From the
    abstract: PIC finds a very low-dimensional embedding of a dataset using truncated power
    iteration on a normalized pair-wise similarity matrix of the data.

    This class is not yet an Estimator/Transformer, use :py:func:`assignClusters` method
    to run the PowerIterationClustering algorithm.

    .. versionadded:: 2.4.0

    Notes
    -----
    See `Wikipedia on Spectral clustering <http://en.wikipedia.org/wiki/Spectral_clustering>`_

    Examples
    --------
    >>> data = [(1, 0, 0.5),
    ...         (2, 0, 0.5), (2, 1, 0.7),
    ...         (3, 0, 0.5), (3, 1, 0.7), (3, 2, 0.9),
    ...         (4, 0, 0.5), (4, 1, 0.7), (4, 2, 0.9), (4, 3, 1.1),
    ...         (5, 0, 0.5), (5, 1, 0.7), (5, 2, 0.9), (5, 3, 1.1), (5, 4, 1.3)]
    >>> df = spark.createDataFrame(data).toDF("src", "dst", "weight").repartition(1)
    >>> pic = PowerIterationClustering(k=2, weightCol="weight")
    >>> pic.setMaxIter(40)
    PowerIterationClustering...
    >>> assignments = pic.assignClusters(df)
    >>> assignments.sort(assignments.id).show(truncate=False)
    +---+-------+
    |id |cluster|
    +---+-------+
    |0  |0      |
    |1  |0      |
    |2  |0      |
    |3  |0      |
    |4  |0      |
    |5  |1      |
    +---+-------+
    ...
    >>> pic_path = temp_path + "/pic"
    >>> pic.save(pic_path)
    >>> pic2 = PowerIterationClustering.load(pic_path)
    >>> pic2.getK()
    2
    >>> pic2.getMaxIter()
    40
    >>> pic2.assignClusters(df).take(6) == assignments.take(6)
    True
    """

    _input_kwargs: Dict[str, Any]

    @keyword_only
    def __init__(
        self,
        *,
        k: int = 2,
        maxIter: int = 20,
        initMode: str = "random",
        srcCol: str = "src",
        dstCol: str = "dst",
        weightCol: Optional[str] = None,
    ):
        """
        __init__(self, \\*, k=2, maxIter=20, initMode="random", srcCol="src", dstCol="dst",\
                 weightCol=None)
        """
        super(PowerIterationClustering, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.clustering.PowerIterationClustering", self.uid
        )
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("2.4.0")
    def setParams(
        self,
        *,
        k: int = 2,
        maxIter: int = 20,
        initMode: str = "random",
        srcCol: str = "src",
        dstCol: str = "dst",
        weightCol: Optional[str] = None,
    ) -> "PowerIterationClustering":
        """
        setParams(self, \\*, k=2, maxIter=20, initMode="random", srcCol="src", dstCol="dst",\
                  weightCol=None)
        Sets params for PowerIterationClustering.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("2.4.0")
    def setK(self, value: int) -> "PowerIterationClustering":
        """
        Sets the value of :py:attr:`k`.
        """
        return self._set(k=value)

    @since("2.4.0")
    def setInitMode(self, value: str) -> "PowerIterationClustering":
        """
        Sets the value of :py:attr:`initMode`.
        """
        return self._set(initMode=value)

    @since("2.4.0")
    def setSrcCol(self, value: str) -> "PowerIterationClustering":
        """
        Sets the value of :py:attr:`srcCol`.
        """
        return self._set(srcCol=value)

    @since("2.4.0")
    def setDstCol(self, value: str) -> "PowerIterationClustering":
        """
        Sets the value of :py:attr:`dstCol`.
        """
        return self._set(dstCol=value)

    @since("2.4.0")
    def setMaxIter(self, value: int) -> "PowerIterationClustering":
        """
        Sets the value of :py:attr:`maxIter`.
        """
        return self._set(maxIter=value)

    @since("2.4.0")
    def setWeightCol(self, value: str) -> "PowerIterationClustering":
        """
        Sets the value of :py:attr:`weightCol`.
        """
        return self._set(weightCol=value)

    @since("2.4.0")
    def assignClusters(self, dataset: DataFrame) -> DataFrame:
        """
        Run the PIC algorithm and returns a cluster assignment for each input vertex.

        Parameters
        ----------
        dataset : :py:class:`pyspark.sql.DataFrame`
          A dataset with columns src, dst, weight representing the affinity matrix,
          which is the matrix A in the PIC paper. Suppose the src column value is i,
          the dst column value is j, the weight column value is similarity s,,ij,,
          which must be nonnegative. This is a symmetric matrix and hence
          s,,ij,, = s,,ji,,. For any (i, j) with nonzero similarity, there should be
          either (i, j, s,,ij,,) or (j, i, s,,ji,,) in the input. Rows with i = j are
          ignored, because we assume s,,ij,, = 0.0.

        Returns
        -------
        :py:class:`pyspark.sql.DataFrame`
            A dataset that contains columns of vertex id and the corresponding cluster for
            the id. The schema of it will be:
            - id: Long
            - cluster: Int
        """
        self._transfer_params_to_java()
        assert self._java_obj is not None

        jdf = self._java_obj.assignClusters(dataset._jdf)
        return DataFrame(jdf, dataset.sparkSession)


if __name__ == "__main__":
    import doctest
    import numpy
    import pyspark.ml.clustering
    from pyspark.sql import SparkSession

    try:
        # Numpy 1.14+ changed it's string format.
        numpy.set_printoptions(legacy="1.13")
    except TypeError:
        pass
    globs = pyspark.ml.clustering.__dict__.copy()
    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    spark = SparkSession.builder.master("local[2]").appName("ml.clustering tests").getOrCreate()
    sc = spark.sparkContext
    globs["sc"] = sc
    globs["spark"] = spark
    import tempfile

    temp_path = tempfile.mkdtemp()
    globs["temp_path"] = temp_path
    try:
        (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
        spark.stop()
    finally:
        from shutil import rmtree

        try:
            rmtree(temp_path)
        except OSError:
            pass
    if failure_count:
        sys.exit(-1)
