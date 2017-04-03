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

from pyspark import since, keyword_only
from pyspark.ml.util import *
from pyspark.ml.wrapper import JavaEstimator, JavaModel, JavaWrapper
from pyspark.ml.param.shared import *
from pyspark.ml.common import inherit_doc

__all__ = ['BisectingKMeans', 'BisectingKMeansModel', 'BisectingKMeansSummary',
           'KMeans', 'KMeansModel',
           'GaussianMixture', 'GaussianMixtureModel', 'GaussianMixtureSummary',
           'LDA', 'LDAModel', 'LocalLDAModel', 'DistributedLDAModel']


class ClusteringSummary(JavaWrapper):
    """
    .. note:: Experimental

    Clustering results for a given model.

    .. versionadded:: 2.1.0
    """

    @property
    @since("2.1.0")
    def predictionCol(self):
        """
        Name for column of predicted clusters in `predictions`.
        """
        return self._call_java("predictionCol")

    @property
    @since("2.1.0")
    def predictions(self):
        """
        DataFrame produced by the model's `transform` method.
        """
        return self._call_java("predictions")

    @property
    @since("2.1.0")
    def featuresCol(self):
        """
        Name for column of features in `predictions`.
        """
        return self._call_java("featuresCol")

    @property
    @since("2.1.0")
    def k(self):
        """
        The number of clusters the model was trained with.
        """
        return self._call_java("k")

    @property
    @since("2.1.0")
    def cluster(self):
        """
        DataFrame of predicted cluster centers for each training data point.
        """
        return self._call_java("cluster")

    @property
    @since("2.1.0")
    def clusterSizes(self):
        """
        Size of (number of data points in) each cluster.
        """
        return self._call_java("clusterSizes")


class GaussianMixtureModel(JavaModel, JavaMLWritable, JavaMLReadable):
    """
    Model fitted by GaussianMixture.

    .. versionadded:: 2.0.0
    """

    @property
    @since("2.0.0")
    def weights(self):
        """
        Weight for each Gaussian distribution in the mixture.
        This is a multinomial probability distribution over the k Gaussians,
        where weights[i] is the weight for Gaussian i, and weights sum to 1.
        """
        return self._call_java("weights")

    @property
    @since("2.0.0")
    def gaussiansDF(self):
        """
        Retrieve Gaussian distributions as a DataFrame.
        Each row represents a Gaussian Distribution.
        The DataFrame has two columns: mean (Vector) and cov (Matrix).
        """
        return self._call_java("gaussiansDF")

    @property
    @since("2.1.0")
    def hasSummary(self):
        """
        Indicates whether a training summary exists for this model
        instance.
        """
        return self._call_java("hasSummary")

    @property
    @since("2.1.0")
    def summary(self):
        """
        Gets summary (e.g. cluster assignments, cluster sizes) of the model trained on the
        training set. An exception is thrown if no summary exists.
        """
        if self.hasSummary:
            return GaussianMixtureSummary(self._call_java("summary"))
        else:
            raise RuntimeError("No training summary available for this %s" %
                               self.__class__.__name__)


@inherit_doc
class GaussianMixture(JavaEstimator, HasFeaturesCol, HasPredictionCol, HasMaxIter, HasTol, HasSeed,
                      HasProbabilityCol, JavaMLWritable, JavaMLReadable):
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

    .. note:: For high-dimensional data (with many features), this algorithm may perform poorly.
          This is due to high-dimensional data (a) making it difficult to cluster at all
          (based on statistical/theoretical arguments) and (b) numerical issues with
          Gaussian distributions.

    >>> from pyspark.ml.linalg import Vectors

    >>> data = [(Vectors.dense([-0.1, -0.05 ]),),
    ...         (Vectors.dense([-0.01, -0.1]),),
    ...         (Vectors.dense([0.9, 0.8]),),
    ...         (Vectors.dense([0.75, 0.935]),),
    ...         (Vectors.dense([-0.83, -0.68]),),
    ...         (Vectors.dense([-0.91, -0.76]),)]
    >>> df = spark.createDataFrame(data, ["features"])
    >>> gm = GaussianMixture(k=3, tol=0.0001,
    ...                      maxIter=10, seed=10)
    >>> model = gm.fit(df)
    >>> model.hasSummary
    True
    >>> summary = model.summary
    >>> summary.k
    3
    >>> summary.clusterSizes
    [2, 2, 2]
    >>> summary.logLikelihood
    8.14636...
    >>> weights = model.weights
    >>> len(weights)
    3
    >>> model.gaussiansDF.select("mean").head()
    Row(mean=DenseVector([0.825, 0.8675]))
    >>> model.gaussiansDF.select("cov").head()
    Row(cov=DenseMatrix(2, 2, [0.0056, -0.0051, -0.0051, 0.0046], False))
    >>> transformed = model.transform(df).select("features", "prediction")
    >>> rows = transformed.collect()
    >>> rows[4].prediction == rows[5].prediction
    True
    >>> rows[2].prediction == rows[3].prediction
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
    >>> model2.gaussiansDF.select("mean").head()
    Row(mean=DenseVector([0.825, 0.8675]))
    >>> model2.gaussiansDF.select("cov").head()
    Row(cov=DenseMatrix(2, 2, [0.0056, -0.0051, -0.0051, 0.0046], False))

    .. versionadded:: 2.0.0
    """

    k = Param(Params._dummy(), "k", "Number of independent Gaussians in the mixture model. " +
              "Must be > 1.", typeConverter=TypeConverters.toInt)

    @keyword_only
    def __init__(self, featuresCol="features", predictionCol="prediction", k=2,
                 probabilityCol="probability", tol=0.01, maxIter=100, seed=None):
        """
        __init__(self, featuresCol="features", predictionCol="prediction", k=2, \
                 probabilityCol="probability", tol=0.01, maxIter=100, seed=None)
        """
        super(GaussianMixture, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.clustering.GaussianMixture",
                                            self.uid)
        self._setDefault(k=2, tol=0.01, maxIter=100)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _create_model(self, java_model):
        return GaussianMixtureModel(java_model)

    @keyword_only
    @since("2.0.0")
    def setParams(self, featuresCol="features", predictionCol="prediction", k=2,
                  probabilityCol="probability", tol=0.01, maxIter=100, seed=None):
        """
        setParams(self, featuresCol="features", predictionCol="prediction", k=2, \
                  probabilityCol="probability", tol=0.01, maxIter=100, seed=None)

        Sets params for GaussianMixture.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("2.0.0")
    def setK(self, value):
        """
        Sets the value of :py:attr:`k`.
        """
        return self._set(k=value)

    @since("2.0.0")
    def getK(self):
        """
        Gets the value of `k`
        """
        return self.getOrDefault(self.k)


class GaussianMixtureSummary(ClusteringSummary):
    """
    .. note:: Experimental

    Gaussian mixture clustering results for a given model.

    .. versionadded:: 2.1.0
    """

    @property
    @since("2.1.0")
    def probabilityCol(self):
        """
        Name for column of predicted probability of each cluster in `predictions`.
        """
        return self._call_java("probabilityCol")

    @property
    @since("2.1.0")
    def probability(self):
        """
        DataFrame of probabilities of each cluster for each training data point.
        """
        return self._call_java("probability")

    @property
    @since("2.2.0")
    def logLikelihood(self):
        """
        Total log-likelihood for this model on the given data.
        """
        return self._call_java("logLikelihood")


class KMeansSummary(ClusteringSummary):
    """
    .. note:: Experimental

    Summary of KMeans.

    .. versionadded:: 2.1.0
    """
    pass


class KMeansModel(JavaModel, JavaMLWritable, JavaMLReadable):
    """
    Model fitted by KMeans.

    .. versionadded:: 1.5.0
    """

    @since("1.5.0")
    def clusterCenters(self):
        """Get the cluster centers, represented as a list of NumPy arrays."""
        return [c.toArray() for c in self._call_java("clusterCenters")]

    @since("2.0.0")
    def computeCost(self, dataset):
        """
        Return the K-means cost (sum of squared distances of points to their nearest center)
        for this model on the given data.
        """
        return self._call_java("computeCost", dataset)

    @property
    @since("2.1.0")
    def hasSummary(self):
        """
        Indicates whether a training summary exists for this model instance.
        """
        return self._call_java("hasSummary")

    @property
    @since("2.1.0")
    def summary(self):
        """
        Gets summary (e.g. cluster assignments, cluster sizes) of the model trained on the
        training set. An exception is thrown if no summary exists.
        """
        if self.hasSummary:
            return KMeansSummary(self._call_java("summary"))
        else:
            raise RuntimeError("No training summary available for this %s" %
                               self.__class__.__name__)


@inherit_doc
class KMeans(JavaEstimator, HasFeaturesCol, HasPredictionCol, HasMaxIter, HasTol, HasSeed,
             JavaMLWritable, JavaMLReadable):
    """
    K-means clustering with a k-means++ like initialization mode
    (the k-means|| algorithm by Bahmani et al).

    >>> from pyspark.ml.linalg import Vectors
    >>> data = [(Vectors.dense([0.0, 0.0]),), (Vectors.dense([1.0, 1.0]),),
    ...         (Vectors.dense([9.0, 8.0]),), (Vectors.dense([8.0, 9.0]),)]
    >>> df = spark.createDataFrame(data, ["features"])
    >>> kmeans = KMeans(k=2, seed=1)
    >>> model = kmeans.fit(df)
    >>> centers = model.clusterCenters()
    >>> len(centers)
    2
    >>> model.computeCost(df)
    2.000...
    >>> transformed = model.transform(df).select("features", "prediction")
    >>> rows = transformed.collect()
    >>> rows[0].prediction == rows[1].prediction
    True
    >>> rows[2].prediction == rows[3].prediction
    True
    >>> model.hasSummary
    True
    >>> summary = model.summary
    >>> summary.k
    2
    >>> summary.clusterSizes
    [2, 2]
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

    .. versionadded:: 1.5.0
    """

    k = Param(Params._dummy(), "k", "The number of clusters to create. Must be > 1.",
              typeConverter=TypeConverters.toInt)
    initMode = Param(Params._dummy(), "initMode",
                     "The initialization algorithm. This can be either \"random\" to " +
                     "choose random points as initial cluster centers, or \"k-means||\" " +
                     "to use a parallel variant of k-means++",
                     typeConverter=TypeConverters.toString)
    initSteps = Param(Params._dummy(), "initSteps", "The number of steps for k-means|| " +
                      "initialization mode. Must be > 0.", typeConverter=TypeConverters.toInt)

    @keyword_only
    def __init__(self, featuresCol="features", predictionCol="prediction", k=2,
                 initMode="k-means||", initSteps=2, tol=1e-4, maxIter=20, seed=None):
        """
        __init__(self, featuresCol="features", predictionCol="prediction", k=2, \
                 initMode="k-means||", initSteps=2, tol=1e-4, maxIter=20, seed=None)
        """
        super(KMeans, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.clustering.KMeans", self.uid)
        self._setDefault(k=2, initMode="k-means||", initSteps=2, tol=1e-4, maxIter=20)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _create_model(self, java_model):
        return KMeansModel(java_model)

    @keyword_only
    @since("1.5.0")
    def setParams(self, featuresCol="features", predictionCol="prediction", k=2,
                  initMode="k-means||", initSteps=2, tol=1e-4, maxIter=20, seed=None):
        """
        setParams(self, featuresCol="features", predictionCol="prediction", k=2, \
                  initMode="k-means||", initSteps=2, tol=1e-4, maxIter=20, seed=None)

        Sets params for KMeans.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("1.5.0")
    def setK(self, value):
        """
        Sets the value of :py:attr:`k`.
        """
        return self._set(k=value)

    @since("1.5.0")
    def getK(self):
        """
        Gets the value of `k`
        """
        return self.getOrDefault(self.k)

    @since("1.5.0")
    def setInitMode(self, value):
        """
        Sets the value of :py:attr:`initMode`.
        """
        return self._set(initMode=value)

    @since("1.5.0")
    def getInitMode(self):
        """
        Gets the value of `initMode`
        """
        return self.getOrDefault(self.initMode)

    @since("1.5.0")
    def setInitSteps(self, value):
        """
        Sets the value of :py:attr:`initSteps`.
        """
        return self._set(initSteps=value)

    @since("1.5.0")
    def getInitSteps(self):
        """
        Gets the value of `initSteps`
        """
        return self.getOrDefault(self.initSteps)


class BisectingKMeansModel(JavaModel, JavaMLWritable, JavaMLReadable):
    """
    Model fitted by BisectingKMeans.

    .. versionadded:: 2.0.0
    """

    @since("2.0.0")
    def clusterCenters(self):
        """Get the cluster centers, represented as a list of NumPy arrays."""
        return [c.toArray() for c in self._call_java("clusterCenters")]

    @since("2.0.0")
    def computeCost(self, dataset):
        """
        Computes the sum of squared distances between the input points
        and their corresponding cluster centers.
        """
        return self._call_java("computeCost", dataset)

    @property
    @since("2.1.0")
    def hasSummary(self):
        """
        Indicates whether a training summary exists for this model instance.
        """
        return self._call_java("hasSummary")

    @property
    @since("2.1.0")
    def summary(self):
        """
        Gets summary (e.g. cluster assignments, cluster sizes) of the model trained on the
        training set. An exception is thrown if no summary exists.
        """
        if self.hasSummary:
            return BisectingKMeansSummary(self._call_java("summary"))
        else:
            raise RuntimeError("No training summary available for this %s" %
                               self.__class__.__name__)


@inherit_doc
class BisectingKMeans(JavaEstimator, HasFeaturesCol, HasPredictionCol, HasMaxIter, HasSeed,
                      JavaMLWritable, JavaMLReadable):
    """
    A bisecting k-means algorithm based on the paper "A comparison of document clustering
    techniques" by Steinbach, Karypis, and Kumar, with modification to fit Spark.
    The algorithm starts from a single cluster that contains all points.
    Iteratively it finds divisible clusters on the bottom level and bisects each of them using
    k-means, until there are `k` leaf clusters in total or no leaf clusters are divisible.
    The bisecting steps of clusters on the same level are grouped together to increase parallelism.
    If bisecting all divisible clusters on the bottom level would result more than `k` leaf
    clusters, larger clusters get higher priority.

    >>> from pyspark.ml.linalg import Vectors
    >>> data = [(Vectors.dense([0.0, 0.0]),), (Vectors.dense([1.0, 1.0]),),
    ...         (Vectors.dense([9.0, 8.0]),), (Vectors.dense([8.0, 9.0]),)]
    >>> df = spark.createDataFrame(data, ["features"])
    >>> bkm = BisectingKMeans(k=2, minDivisibleClusterSize=1.0)
    >>> model = bkm.fit(df)
    >>> centers = model.clusterCenters()
    >>> len(centers)
    2
    >>> model.computeCost(df)
    2.000...
    >>> model.hasSummary
    True
    >>> summary = model.summary
    >>> summary.k
    2
    >>> summary.clusterSizes
    [2, 2]
    >>> transformed = model.transform(df).select("features", "prediction")
    >>> rows = transformed.collect()
    >>> rows[0].prediction == rows[1].prediction
    True
    >>> rows[2].prediction == rows[3].prediction
    True
    >>> bkm_path = temp_path + "/bkm"
    >>> bkm.save(bkm_path)
    >>> bkm2 = BisectingKMeans.load(bkm_path)
    >>> bkm2.getK()
    2
    >>> model_path = temp_path + "/bkm_model"
    >>> model.save(model_path)
    >>> model2 = BisectingKMeansModel.load(model_path)
    >>> model2.hasSummary
    False
    >>> model.clusterCenters()[0] == model2.clusterCenters()[0]
    array([ True,  True], dtype=bool)
    >>> model.clusterCenters()[1] == model2.clusterCenters()[1]
    array([ True,  True], dtype=bool)

    .. versionadded:: 2.0.0
    """

    k = Param(Params._dummy(), "k", "The desired number of leaf clusters. Must be > 1.",
              typeConverter=TypeConverters.toInt)
    minDivisibleClusterSize = Param(Params._dummy(), "minDivisibleClusterSize",
                                    "The minimum number of points (if >= 1.0) or the minimum " +
                                    "proportion of points (if < 1.0) of a divisible cluster.",
                                    typeConverter=TypeConverters.toFloat)

    @keyword_only
    def __init__(self, featuresCol="features", predictionCol="prediction", maxIter=20,
                 seed=None, k=4, minDivisibleClusterSize=1.0):
        """
        __init__(self, featuresCol="features", predictionCol="prediction", maxIter=20, \
                 seed=None, k=4, minDivisibleClusterSize=1.0)
        """
        super(BisectingKMeans, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.clustering.BisectingKMeans",
                                            self.uid)
        self._setDefault(maxIter=20, k=4, minDivisibleClusterSize=1.0)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("2.0.0")
    def setParams(self, featuresCol="features", predictionCol="prediction", maxIter=20,
                  seed=None, k=4, minDivisibleClusterSize=1.0):
        """
        setParams(self, featuresCol="features", predictionCol="prediction", maxIter=20, \
                  seed=None, k=4, minDivisibleClusterSize=1.0)
        Sets params for BisectingKMeans.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("2.0.0")
    def setK(self, value):
        """
        Sets the value of :py:attr:`k`.
        """
        return self._set(k=value)

    @since("2.0.0")
    def getK(self):
        """
        Gets the value of `k` or its default value.
        """
        return self.getOrDefault(self.k)

    @since("2.0.0")
    def setMinDivisibleClusterSize(self, value):
        """
        Sets the value of :py:attr:`minDivisibleClusterSize`.
        """
        return self._set(minDivisibleClusterSize=value)

    @since("2.0.0")
    def getMinDivisibleClusterSize(self):
        """
        Gets the value of `minDivisibleClusterSize` or its default value.
        """
        return self.getOrDefault(self.minDivisibleClusterSize)

    def _create_model(self, java_model):
        return BisectingKMeansModel(java_model)


class BisectingKMeansSummary(ClusteringSummary):
    """
    .. note:: Experimental

    Bisecting KMeans clustering results for a given model.

    .. versionadded:: 2.1.0
    """
    pass


@inherit_doc
class LDAModel(JavaModel):
    """
    Latent Dirichlet Allocation (LDA) model.
    This abstraction permits for different underlying representations,
    including local and distributed data structures.

    .. versionadded:: 2.0.0
    """

    @since("2.0.0")
    def isDistributed(self):
        """
        Indicates whether this instance is of type DistributedLDAModel
        """
        return self._call_java("isDistributed")

    @since("2.0.0")
    def vocabSize(self):
        """Vocabulary size (number of terms or words in the vocabulary)"""
        return self._call_java("vocabSize")

    @since("2.0.0")
    def topicsMatrix(self):
        """
        Inferred topics, where each topic is represented by a distribution over terms.
        This is a matrix of size vocabSize x k, where each column is a topic.
        No guarantees are given about the ordering of the topics.

        WARNING: If this model is actually a :py:class:`DistributedLDAModel` instance produced by
        the Expectation-Maximization ("em") `optimizer`, then this method could involve
        collecting a large amount of data to the driver (on the order of vocabSize x k).
        """
        return self._call_java("topicsMatrix")

    @since("2.0.0")
    def logLikelihood(self, dataset):
        """
        Calculates a lower bound on the log likelihood of the entire corpus.
        See Equation (16) in the Online LDA paper (Hoffman et al., 2010).

        WARNING: If this model is an instance of :py:class:`DistributedLDAModel` (produced when
        :py:attr:`optimizer` is set to "em"), this involves collecting a large
        :py:func:`topicsMatrix` to the driver. This implementation may be changed in the future.
        """
        return self._call_java("logLikelihood", dataset)

    @since("2.0.0")
    def logPerplexity(self, dataset):
        """
        Calculate an upper bound on perplexity.  (Lower is better.)
        See Equation (16) in the Online LDA paper (Hoffman et al., 2010).

        WARNING: If this model is an instance of :py:class:`DistributedLDAModel` (produced when
        :py:attr:`optimizer` is set to "em"), this involves collecting a large
        :py:func:`topicsMatrix` to the driver. This implementation may be changed in the future.
        """
        return self._call_java("logPerplexity", dataset)

    @since("2.0.0")
    def describeTopics(self, maxTermsPerTopic=10):
        """
        Return the topics described by their top-weighted terms.
        """
        return self._call_java("describeTopics", maxTermsPerTopic)

    @since("2.0.0")
    def estimatedDocConcentration(self):
        """
        Value for :py:attr:`LDA.docConcentration` estimated from data.
        If Online LDA was used and :py:attr:`LDA.optimizeDocConcentration` was set to false,
        then this returns the fixed (given) value for the :py:attr:`LDA.docConcentration` parameter.
        """
        return self._call_java("estimatedDocConcentration")


@inherit_doc
class DistributedLDAModel(LDAModel, JavaMLReadable, JavaMLWritable):
    """
    Distributed model fitted by :py:class:`LDA`.
    This type of model is currently only produced by Expectation-Maximization (EM).

    This model stores the inferred topics, the full training dataset, and the topic distribution
    for each training document.

    .. versionadded:: 2.0.0
    """

    @since("2.0.0")
    def toLocal(self):
        """
        Convert this distributed model to a local representation.  This discards info about the
        training dataset.

        WARNING: This involves collecting a large :py:func:`topicsMatrix` to the driver.
        """
        return LocalLDAModel(self._call_java("toLocal"))

    @since("2.0.0")
    def trainingLogLikelihood(self):
        """
        Log likelihood of the observed tokens in the training set,
        given the current parameter estimates:
        log P(docs | topics, topic distributions for docs, Dirichlet hyperparameters)

        Notes:
          - This excludes the prior; for that, use :py:func:`logPrior`.
          - Even with :py:func:`logPrior`, this is NOT the same as the data log likelihood given
            the hyperparameters.
          - This is computed from the topic distributions computed during training. If you call
            :py:func:`logLikelihood` on the same training dataset, the topic distributions
            will be computed again, possibly giving different results.
        """
        return self._call_java("trainingLogLikelihood")

    @since("2.0.0")
    def logPrior(self):
        """
        Log probability of the current parameter estimate:
        log P(topics, topic distributions for docs | alpha, eta)
        """
        return self._call_java("logPrior")

    @since("2.0.0")
    def getCheckpointFiles(self):
        """
        If using checkpointing and :py:attr:`LDA.keepLastCheckpoint` is set to true, then there may
        be saved checkpoint files.  This method is provided so that users can manage those files.

        .. note:: Removing the checkpoints can cause failures if a partition is lost and is needed
            by certain :py:class:`DistributedLDAModel` methods.  Reference counting will clean up
            the checkpoints when this model and derivative data go out of scope.

        :return  List of checkpoint files from training
        """
        return self._call_java("getCheckpointFiles")


@inherit_doc
class LocalLDAModel(LDAModel, JavaMLReadable, JavaMLWritable):
    """
    Local (non-distributed) model fitted by :py:class:`LDA`.
    This model stores the inferred topics only; it does not store info about the training dataset.

    .. versionadded:: 2.0.0
    """
    pass


@inherit_doc
class LDA(JavaEstimator, HasFeaturesCol, HasMaxIter, HasSeed, HasCheckpointInterval,
          JavaMLReadable, JavaMLWritable):
    """
    Latent Dirichlet Allocation (LDA), a topic model designed for text documents.

    Terminology:

     - "term" = "word": an el
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

    >>> from pyspark.ml.linalg import Vectors, SparseVector
    >>> from pyspark.ml.clustering import LDA
    >>> df = spark.createDataFrame([[1, Vectors.dense([0.0, 1.0])],
    ...      [2, SparseVector(2, {0: 1.0})],], ["id", "features"])
    >>> lda = LDA(k=2, seed=1, optimizer="em")
    >>> model = lda.fit(df)
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

    .. versionadded:: 2.0.0
    """

    k = Param(Params._dummy(), "k", "The number of topics (clusters) to infer. Must be > 1.",
              typeConverter=TypeConverters.toInt)
    optimizer = Param(Params._dummy(), "optimizer",
                      "Optimizer or inference algorithm used to estimate the LDA model.  "
                      "Supported: online, em", typeConverter=TypeConverters.toString)
    learningOffset = Param(Params._dummy(), "learningOffset",
                           "A (positive) learning parameter that downweights early iterations."
                           " Larger values make early iterations count less",
                           typeConverter=TypeConverters.toFloat)
    learningDecay = Param(Params._dummy(), "learningDecay", "Learning rate, set as an"
                          "exponential decay rate. This should be between (0.5, 1.0] to "
                          "guarantee asymptotic convergence.", typeConverter=TypeConverters.toFloat)
    subsamplingRate = Param(Params._dummy(), "subsamplingRate",
                            "Fraction of the corpus to be sampled and used in each iteration "
                            "of mini-batch gradient descent, in range (0, 1].",
                            typeConverter=TypeConverters.toFloat)
    optimizeDocConcentration = Param(Params._dummy(), "optimizeDocConcentration",
                                     "Indicates whether the docConcentration (Dirichlet parameter "
                                     "for document-topic distribution) will be optimized during "
                                     "training.", typeConverter=TypeConverters.toBoolean)
    docConcentration = Param(Params._dummy(), "docConcentration",
                             "Concentration parameter (commonly named \"alpha\") for the "
                             "prior placed on documents' distributions over topics (\"theta\").",
                             typeConverter=TypeConverters.toListFloat)
    topicConcentration = Param(Params._dummy(), "topicConcentration",
                               "Concentration parameter (commonly named \"beta\" or \"eta\") for "
                               "the prior placed on topic' distributions over terms.",
                               typeConverter=TypeConverters.toFloat)
    topicDistributionCol = Param(Params._dummy(), "topicDistributionCol",
                                 "Output column with estimates of the topic mixture distribution "
                                 "for each document (often called \"theta\" in the literature). "
                                 "Returns a vector of zeros for an empty document.",
                                 typeConverter=TypeConverters.toString)
    keepLastCheckpoint = Param(Params._dummy(), "keepLastCheckpoint",
                               "(For EM optimizer) If using checkpointing, this indicates whether"
                               " to keep the last checkpoint. If false, then the checkpoint will be"
                               " deleted. Deleting the checkpoint can cause failures if a data"
                               " partition is lost, so set this bit with care.",
                               TypeConverters.toBoolean)

    @keyword_only
    def __init__(self, featuresCol="features", maxIter=20, seed=None, checkpointInterval=10,
                 k=10, optimizer="online", learningOffset=1024.0, learningDecay=0.51,
                 subsamplingRate=0.05, optimizeDocConcentration=True,
                 docConcentration=None, topicConcentration=None,
                 topicDistributionCol="topicDistribution", keepLastCheckpoint=True):
        """
        __init__(self, featuresCol="features", maxIter=20, seed=None, checkpointInterval=10,\
                  k=10, optimizer="online", learningOffset=1024.0, learningDecay=0.51,\
                  subsamplingRate=0.05, optimizeDocConcentration=True,\
                  docConcentration=None, topicConcentration=None,\
                  topicDistributionCol="topicDistribution", keepLastCheckpoint=True):
        """
        super(LDA, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.clustering.LDA", self.uid)
        self._setDefault(maxIter=20, checkpointInterval=10,
                         k=10, optimizer="online", learningOffset=1024.0, learningDecay=0.51,
                         subsamplingRate=0.05, optimizeDocConcentration=True,
                         topicDistributionCol="topicDistribution", keepLastCheckpoint=True)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _create_model(self, java_model):
        if self.getOptimizer() == "em":
            return DistributedLDAModel(java_model)
        else:
            return LocalLDAModel(java_model)

    @keyword_only
    @since("2.0.0")
    def setParams(self, featuresCol="features", maxIter=20, seed=None, checkpointInterval=10,
                  k=10, optimizer="online", learningOffset=1024.0, learningDecay=0.51,
                  subsamplingRate=0.05, optimizeDocConcentration=True,
                  docConcentration=None, topicConcentration=None,
                  topicDistributionCol="topicDistribution", keepLastCheckpoint=True):
        """
        setParams(self, featuresCol="features", maxIter=20, seed=None, checkpointInterval=10,\
                  k=10, optimizer="online", learningOffset=1024.0, learningDecay=0.51,\
                  subsamplingRate=0.05, optimizeDocConcentration=True,\
                  docConcentration=None, topicConcentration=None,\
                  topicDistributionCol="topicDistribution", keepLastCheckpoint=True):

        Sets params for LDA.
        """
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    @since("2.0.0")
    def setK(self, value):
        """
        Sets the value of :py:attr:`k`.

        >>> algo = LDA().setK(10)
        >>> algo.getK()
        10
        """
        return self._set(k=value)

    @since("2.0.0")
    def getK(self):
        """
        Gets the value of :py:attr:`k` or its default value.
        """
        return self.getOrDefault(self.k)

    @since("2.0.0")
    def setOptimizer(self, value):
        """
        Sets the value of :py:attr:`optimizer`.
        Currenlty only support 'em' and 'online'.

        >>> algo = LDA().setOptimizer("em")
        >>> algo.getOptimizer()
        'em'
        """
        return self._set(optimizer=value)

    @since("2.0.0")
    def getOptimizer(self):
        """
        Gets the value of :py:attr:`optimizer` or its default value.
        """
        return self.getOrDefault(self.optimizer)

    @since("2.0.0")
    def setLearningOffset(self, value):
        """
        Sets the value of :py:attr:`learningOffset`.

        >>> algo = LDA().setLearningOffset(100)
        >>> algo.getLearningOffset()
        100.0
        """
        return self._set(learningOffset=value)

    @since("2.0.0")
    def getLearningOffset(self):
        """
        Gets the value of :py:attr:`learningOffset` or its default value.
        """
        return self.getOrDefault(self.learningOffset)

    @since("2.0.0")
    def setLearningDecay(self, value):
        """
        Sets the value of :py:attr:`learningDecay`.

        >>> algo = LDA().setLearningDecay(0.1)
        >>> algo.getLearningDecay()
        0.1...
        """
        return self._set(learningDecay=value)

    @since("2.0.0")
    def getLearningDecay(self):
        """
        Gets the value of :py:attr:`learningDecay` or its default value.
        """
        return self.getOrDefault(self.learningDecay)

    @since("2.0.0")
    def setSubsamplingRate(self, value):
        """
        Sets the value of :py:attr:`subsamplingRate`.

        >>> algo = LDA().setSubsamplingRate(0.1)
        >>> algo.getSubsamplingRate()
        0.1...
        """
        return self._set(subsamplingRate=value)

    @since("2.0.0")
    def getSubsamplingRate(self):
        """
        Gets the value of :py:attr:`subsamplingRate` or its default value.
        """
        return self.getOrDefault(self.subsamplingRate)

    @since("2.0.0")
    def setOptimizeDocConcentration(self, value):
        """
        Sets the value of :py:attr:`optimizeDocConcentration`.

        >>> algo = LDA().setOptimizeDocConcentration(True)
        >>> algo.getOptimizeDocConcentration()
        True
        """
        return self._set(optimizeDocConcentration=value)

    @since("2.0.0")
    def getOptimizeDocConcentration(self):
        """
        Gets the value of :py:attr:`optimizeDocConcentration` or its default value.
        """
        return self.getOrDefault(self.optimizeDocConcentration)

    @since("2.0.0")
    def setDocConcentration(self, value):
        """
        Sets the value of :py:attr:`docConcentration`.

        >>> algo = LDA().setDocConcentration([0.1, 0.2])
        >>> algo.getDocConcentration()
        [0.1..., 0.2...]
        """
        return self._set(docConcentration=value)

    @since("2.0.0")
    def getDocConcentration(self):
        """
        Gets the value of :py:attr:`docConcentration` or its default value.
        """
        return self.getOrDefault(self.docConcentration)

    @since("2.0.0")
    def setTopicConcentration(self, value):
        """
        Sets the value of :py:attr:`topicConcentration`.

        >>> algo = LDA().setTopicConcentration(0.5)
        >>> algo.getTopicConcentration()
        0.5...
        """
        return self._set(topicConcentration=value)

    @since("2.0.0")
    def getTopicConcentration(self):
        """
        Gets the value of :py:attr:`topicConcentration` or its default value.
        """
        return self.getOrDefault(self.topicConcentration)

    @since("2.0.0")
    def setTopicDistributionCol(self, value):
        """
        Sets the value of :py:attr:`topicDistributionCol`.

        >>> algo = LDA().setTopicDistributionCol("topicDistributionCol")
        >>> algo.getTopicDistributionCol()
        'topicDistributionCol'
        """
        return self._set(topicDistributionCol=value)

    @since("2.0.0")
    def getTopicDistributionCol(self):
        """
        Gets the value of :py:attr:`topicDistributionCol` or its default value.
        """
        return self.getOrDefault(self.topicDistributionCol)

    @since("2.0.0")
    def setKeepLastCheckpoint(self, value):
        """
        Sets the value of :py:attr:`keepLastCheckpoint`.

        >>> algo = LDA().setKeepLastCheckpoint(False)
        >>> algo.getKeepLastCheckpoint()
        False
        """
        return self._set(keepLastCheckpoint=value)

    @since("2.0.0")
    def getKeepLastCheckpoint(self):
        """
        Gets the value of :py:attr:`keepLastCheckpoint` or its default value.
        """
        return self.getOrDefault(self.keepLastCheckpoint)


if __name__ == "__main__":
    import doctest
    import pyspark.ml.clustering
    from pyspark.sql import SparkSession
    globs = pyspark.ml.clustering.__dict__.copy()
    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    spark = SparkSession.builder\
        .master("local[2]")\
        .appName("ml.clustering tests")\
        .getOrCreate()
    sc = spark.sparkContext
    globs['sc'] = sc
    globs['spark'] = spark
    import tempfile
    temp_path = tempfile.mkdtemp()
    globs['temp_path'] = temp_path
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
        exit(-1)
