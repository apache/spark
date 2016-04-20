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

from pyspark import since
from pyspark.ml.util import *
from pyspark.ml.wrapper import JavaEstimator, JavaModel
from pyspark.ml.param.shared import *
from pyspark.mllib.common import inherit_doc

__all__ = ['KMeans', 'KMeansModel',
           'BisectingKMeans', 'BisectingKMeansModel',
           'LDA', 'LDAModel', 'LocalLDAModel', 'DistributedLDAModel']


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


@inherit_doc
class KMeans(JavaEstimator, HasFeaturesCol, HasPredictionCol, HasMaxIter, HasTol, HasSeed,
             JavaMLWritable, JavaMLReadable):
    """
    K-means clustering with support for multiple parallel runs and a k-means++ like initialization
    mode (the k-means|| algorithm by Bahmani et al). When multiple concurrent runs are requested,
    they are executed together with joint passes over the data for efficiency.

    >>> from pyspark.mllib.linalg import Vectors
    >>> data = [(Vectors.dense([0.0, 0.0]),), (Vectors.dense([1.0, 1.0]),),
    ...         (Vectors.dense([9.0, 8.0]),), (Vectors.dense([8.0, 9.0]),)]
    >>> df = sqlContext.createDataFrame(data, ["features"])
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
    >>> kmeans_path = temp_path + "/kmeans"
    >>> kmeans.save(kmeans_path)
    >>> kmeans2 = KMeans.load(kmeans_path)
    >>> kmeans2.getK()
    2
    >>> model_path = temp_path + "/kmeans_model"
    >>> model.save(model_path)
    >>> model2 = KMeansModel.load(model_path)
    >>> model.clusterCenters()[0] == model2.clusterCenters()[0]
    array([ True,  True], dtype=bool)
    >>> model.clusterCenters()[1] == model2.clusterCenters()[1]
    array([ True,  True], dtype=bool)

    .. versionadded:: 1.5.0
    """

    k = Param(Params._dummy(), "k", "number of clusters to create",
              typeConverter=TypeConverters.toInt)
    initMode = Param(Params._dummy(), "initMode",
                     "the initialization algorithm. This can be either \"random\" to " +
                     "choose random points as initial cluster centers, or \"k-means||\" " +
                     "to use a parallel variant of k-means++",
                     typeConverter=TypeConverters.toString)
    initSteps = Param(Params._dummy(), "initSteps", "steps for k-means initialization mode",
                      typeConverter=TypeConverters.toInt)

    @keyword_only
    def __init__(self, featuresCol="features", predictionCol="prediction", k=2,
                 initMode="k-means||", initSteps=5, tol=1e-4, maxIter=20, seed=None):
        """
        __init__(self, featuresCol="features", predictionCol="prediction", k=2, \
                 initMode="k-means||", initSteps=5, tol=1e-4, maxIter=20, seed=None)
        """
        super(KMeans, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.clustering.KMeans", self.uid)
        self._setDefault(k=2, initMode="k-means||", initSteps=5, tol=1e-4, maxIter=20)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    def _create_model(self, java_model):
        return KMeansModel(java_model)

    @keyword_only
    @since("1.5.0")
    def setParams(self, featuresCol="features", predictionCol="prediction", k=2,
                  initMode="k-means||", initSteps=5, tol=1e-4, maxIter=20, seed=None):
        """
        setParams(self, featuresCol="features", predictionCol="prediction", k=2, \
                  initMode="k-means||", initSteps=5, tol=1e-4, maxIter=20, seed=None)

        Sets params for KMeans.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    @since("1.5.0")
    def setK(self, value):
        """
        Sets the value of :py:attr:`k`.
        """
        self._set(k=value)
        return self

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
        self._set(initMode=value)
        return self

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
        self._set(initSteps=value)
        return self

    @since("1.5.0")
    def getInitSteps(self):
        """
        Gets the value of `initSteps`
        """
        return self.getOrDefault(self.initSteps)


class BisectingKMeansModel(JavaModel, JavaMLWritable, JavaMLReadable):
    """
    .. note:: Experimental

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


@inherit_doc
class BisectingKMeans(JavaEstimator, HasFeaturesCol, HasPredictionCol, HasMaxIter, HasSeed,
                      JavaMLWritable, JavaMLReadable):
    """
    .. note:: Experimental

    A bisecting k-means algorithm based on the paper "A comparison of document clustering
    techniques" by Steinbach, Karypis, and Kumar, with modification to fit Spark.
    The algorithm starts from a single cluster that contains all points.
    Iteratively it finds divisible clusters on the bottom level and bisects each of them using
    k-means, until there are `k` leaf clusters in total or no leaf clusters are divisible.
    The bisecting steps of clusters on the same level are grouped together to increase parallelism.
    If bisecting all divisible clusters on the bottom level would result more than `k` leaf
    clusters, larger clusters get higher priority.

    >>> from pyspark.mllib.linalg import Vectors
    >>> data = [(Vectors.dense([0.0, 0.0]),), (Vectors.dense([1.0, 1.0]),),
    ...         (Vectors.dense([9.0, 8.0]),), (Vectors.dense([8.0, 9.0]),)]
    >>> df = sqlContext.createDataFrame(data, ["features"])
    >>> bkm = BisectingKMeans(k=2, minDivisibleClusterSize=1.0)
    >>> model = bkm.fit(df)
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
    >>> bkm_path = temp_path + "/bkm"
    >>> bkm.save(bkm_path)
    >>> bkm2 = BisectingKMeans.load(bkm_path)
    >>> bkm2.getK()
    2
    >>> model_path = temp_path + "/bkm_model"
    >>> model.save(model_path)
    >>> model2 = BisectingKMeansModel.load(model_path)
    >>> model.clusterCenters()[0] == model2.clusterCenters()[0]
    array([ True,  True], dtype=bool)
    >>> model.clusterCenters()[1] == model2.clusterCenters()[1]
    array([ True,  True], dtype=bool)

    .. versionadded:: 2.0.0
    """

    k = Param(Params._dummy(), "k", "number of clusters to create",
              typeConverter=TypeConverters.toInt)
    minDivisibleClusterSize = Param(Params._dummy(), "minDivisibleClusterSize",
                                    "the minimum number of points (if >= 1.0) " +
                                    "or the minimum proportion",
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
        kwargs = self.__init__._input_kwargs
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
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    @since("2.0.0")
    def setK(self, value):
        """
        Sets the value of :py:attr:`k`.
        """
        self._set(k=value)
        return self

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
        self._set(minDivisibleClusterSize=value)
        return self

    @since("2.0.0")
    def getMinDivisibleClusterSize(self):
        """
        Gets the value of `minDivisibleClusterSize` or its default value.
        """
        return self.getOrDefault(self.minDivisibleClusterSize)

    def _create_model(self, java_model):
        return BisectingKMeansModel(java_model)


class LDAModel(JavaModel):
    """
    A clustering model derived from the LDA method.

    .. versionadded:: 2.0.0
    """

    @since("2.0.0")
    def isDistributed(self):
        """Indicates whether this instance is of type DistributedLDAModel"""
        return self._call_java("isDistributed")

    @since("2.0.0")
    def vocabSize(self):
        """Vocabulary size (number of terms or terms in the vocabulary)"""
        return self._call_java("vocabSize")

    @since("2.0.0")
    def topicsMatrix(self):
        """ Inferred topics, where each topic is represented by a distribution over terms.
        This is a matrix of size vocabSize x k, where each column is a topic.
        No guarantees are given about the ordering of the topics.

        WARNING: If this model is actually a [[DistributedLDAModel]] instance produced by
        the Expectation-Maximization ("em") [[optimizer]], then this method could involve
        collecting a large amount of data to the driver (on the order of vocabSize x k).
        """
        return self._call_java("topicsMatrix")

    @since("2.0.0")
    def logLikelihood(self, dataset):
        """Calculates a lower bound on the log likelihood of the entire corpus.
        See Equation (16) in the Online LDA paper (Hoffman et al., 2010).

        WARNING: If this model is an instance of [[DistributedLDAModel]] (produced when
        [[optimizer]] is set to "em"), this involves collecting a large [[topicsMatrix]] to the
        driver. This implementation may be changed in the future.
        """
        return self._call_java("logLikelihood", dataset)

    @since("2.0.0")
    def logPerplexity(self, dataset):
        """Calculate an upper bound bound on perplexity.  (Lower is better.)
        See Equation (16) in the Online LDA paper (Hoffman et al., 2010).

        WARNING: If this model is an instance of [[DistributedLDAModel]] (produced when
        [[optimizer]] is set to "em"), this involves collecting a large [[topicsMatrix]] to the
        driver. This implementation may be changed in the future.
        """
        return self._call_java("logPerplexity", dataset)

    @since("2.0.0")
    def describeTopics(self, maxTermsPerTopic=10):
        """Return the topics described by their top-weighted terms.

        WARNING: If vocabSize and k are large, this can return a large object!
        """
        return self._call_java("describeTopics", maxTermsPerTopic)

    @since("2.0.0")
    def estimatedDocConcentration(self):
        """Value for [[docConcentration]] estimated from data.
        If Online LDA was used and [[optimizeDocConcentration]] was set to false,
        then this returns the fixed (given) value for the [[docConcentration]] parameter.
        """
        return self._call_java("estimatedDocConcentration")


class DistributedLDAModel(LDAModel):
    """
    Model fitted by LDA.

    .. versionadded:: 2.0.0
    """
    def toLocal(self):
        return self._call_java("toLocal")


class LocalLDAModel(LDAModel):
    """
    Model fitted by LDA.

    .. versionadded:: 2.0.0
    """
    pass


class LDA(JavaEstimator, HasFeaturesCol, HasMaxIter, HasSeed, HasCheckpointInterval):
    """
    Latent Dirichlet Allocation (LDA), a topic model designed for text documents.
    Terminology
    - "word" = "term": an element of the vocabulary
    - "token": instance of a term appearing in a document
    - "topic": multinomial distribution over words representing some concept
    References:
    - Original LDA paper (journal version):
    Blei, Ng, and Jordan.  "Latent Dirichlet Allocation."  JMLR, 2003.

    >>> from pyspark.mllib.linalg import Vectors, SparseVector
    >>> from pyspark.ml.clustering import LDA
    >>> df = sqlContext.createDataFrame([[1, Vectors.dense([0.0, 1.0])], \
        [2, SparseVector(2, {0: 1.0})],], ["id", "features"])
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
    >>> model.estimatedDocConcentration()
    DenseVector([26.0, 26.0])

    .. versionadded:: 2.0.0
    """

    k = Param(Params._dummy(), "k", "number of topics (clusters) to infer")
    optimizer = Param(Params._dummy(), "optimizer",
                      "Optimizer or inference algorithm used to estimate the LDA model.  "
                      "Supported: online, em")
    learningOffset = Param(Params._dummy(), "learningOffset",
                           "A (positive) learning parameter that downweights early iterations."
                           " Larger values make early iterations count less")
    learningDecay = Param(Params._dummy(), "learningDecay", "Learning rate, set as an"
                          "exponential decay rate. This should be between (0.5, 1.0] to "
                          "guarantee asymptotic convergence.")
    subsamplingRate = Param(Params._dummy(), "subsamplingRate",
                            "Fraction of the corpus to be sampled and used in each iteration "
                            "of mini-batch gradient descent, in range (0, 1].")
    optimizeDocConcentration = Param(Params._dummy(), "optimizeDocConcentration",
                                     "Indicates whether the docConcentration (Dirichlet parameter "
                                     "for document-topic distribution) will be optimized during "
                                     "training.")
    docConcentration = Param(Params._dummy(), "docConcentration",
                             "Concentration parameter (commonly named \"alpha\") for the "
                             "prior placed on documents' distributions over topics (\"theta\").")
    topicConcentration = Param(Params._dummy(), "topicConcentration",
                               "Concentration parameter (commonly named \"beta\" or \"eta\") for "
                               "the prior placed on topic' distributions over terms.")
    topicDistributionCol = Param(Params._dummy(), "topicDistributionCol",
                                 "Output column with estimates of the topic mixture distribution "
                                 "for each document (often called \"theta\" in the literature). "
                                 "Returns a vector of zeros for an empty document.")

    @keyword_only
    def __init__(self, featuresCol="features", k=10,
                 optimizer="online", learningOffset=1024.0, learningDecay=0.51,
                 subsamplingRate=0.05, optimizeDocConcentration=True,
                 checkpointInterval=10, maxIter=20, docConcentration=None,
                 topicConcentration=None, topicDistributionCol="topicDistribution", seed=None):
        """
        __init__(self, featuresCol="features", k=10, \
                 optimizer="online", learningOffset=1024.0, learningDecay=0.51, \
                 subsamplingRate=0.05, optimizeDocConcentration=True, \
                 checkpointInterval=10, maxIter=20, docConcentration=None, \
                 topicConcentration=None, topicDistributionCol="topicDistribution", seed=None):
        """
        super(LDA, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.clustering.LDA", self.uid)
        self._setDefault(k=10, optimizer="online", learningOffset=1024.0, learningDecay=0.51,
                         subsamplingRate=0.05, optimizeDocConcentration=True,
                         checkpointInterval=10, maxIter=20)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    def _create_model(self, java_model):
        if self.getOptimizer() == "em":
            return DistributedLDAModel(java_model)
        else:
            return LocalLDAModel(java_model)

    @keyword_only
    @since("2.0.0")
    def setParams(self, featuresCol="features", k=10,
                  optimizer="online", learningOffset=1024.0, learningDecay=0.51,
                  subsamplingRate=0.05, optimizeDocConcentration=True,
                  checkpointInterval=10, maxIter=20, docConcentration=None,
                  topicConcentration=None,
                  topicDistributionCol="topicDistribution", seed=None):
        """
        setParams(self, featuresCol="features", k=10, \
                  optimizer="online", learningOffset=1024.0, learningDecay=0.51, \
                  subsamplingRate=0.05, optimizeDocConcentration=True, \
                  checkpointInterval=10, maxIter=20, docConcentration=None,
                  topicConcentration=None,
                  topicDistributionCol="topicDistribution", seed=None):

        Sets params for LDA.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    @since("2.0.0")
    def setK(self, value):
        """
        Sets the value of :py:attr:`k`.

        >>> algo = LDA().setK(10)
        >>> algo.getK()
        10
        """
        self._paramMap[self.k] = value
        return self

    @since("2.0.0")
    def getK(self):
        """
        Gets the value of `k` or its default value.
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
        self._paramMap[self.optimizer] = value
        return self

    @since("2.0.0")
    def getOptimizer(self):
        """
        Gets the value of `optimizer` or its default value.
        """
        return self.getOrDefault(self.optimizer)

    @since("2.0.0")
    def setLearningOffset(self, value):
        """
        Sets the value of :py:attr:`learningOffset`.
        >>> algo = LDA().setLearningOffset(100)
        >>> algo.getLearningOffset()
        100
        """
        self._paramMap[self.learningOffset] = value
        return self

    @since("2.0.0")
    def getLearningOffset(self):
        """
        Gets the value of `learningOffset` or its default value.
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
        self._paramMap[self.learningDecay] = value
        return self

    @since("2.0.0")
    def getLearningDecay(self):
        """
        Gets the value of `learningDecay` or its default value.
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
        self._paramMap[self.subsamplingRate] = value
        return self

    @since("2.0.0")
    def getSubsamplingRate(self):
        """
        Gets the value of `subsamplingRate` or its default value.
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
        self._paramMap[self.optimizeDocConcentration] = value
        return self

    @since("2.0.0")
    def getOptimizeDocConcentration(self):
        """
        Gets the value of `optimizeDocConcentration` or its default value.
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
        self._paramMap[self.docConcentration] = value
        return self

    @since("2.0.0")
    def getDocConcentration(self):
        """
        Gets the value of `docConcentration` or its default value.
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
        self._paramMap[self.topicConcentration] = value
        return self

    @since("2.0.0")
    def getTopicConcentration(self):
        """
        Gets the value of `topicConcentration` or its default value.
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
        self._paramMap[self.topicDistributionCol] = value
        return self

    @since("2.0.0")
    def getTopicDistributionCol(self):
        """
        Gets the value of `topicDistributionCol` or its default value.
        """
        return self.getOrDefault(self.topicDistributionCol)


if __name__ == "__main__":
    import doctest
    import pyspark.ml.clustering
    from pyspark.context import SparkContext
    from pyspark.sql import SQLContext
    globs = pyspark.ml.clustering.__dict__.copy()
    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    sc = SparkContext("local[2]", "ml.clustering tests")
    sqlContext = SQLContext(sc)
    globs['sc'] = sc
    globs['sqlContext'] = sqlContext
    import tempfile
    temp_path = tempfile.mkdtemp()
    globs['temp_path'] = temp_path
    try:
        (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
        sc.stop()
    finally:
        from shutil import rmtree
        try:
            rmtree(temp_path)
        except OSError:
            pass
    if failure_count:
        exit(-1)
