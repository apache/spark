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

__all__ = ['KMeans', 'KMeansModel', 'BisectingKMeans', 'BisectingKMeansModel']


class KMeansModel(JavaModel, MLWritable, MLReadable):
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
             MLWritable, MLReadable):
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
    >>> import os, tempfile
    >>> path = tempfile.mkdtemp()
    >>> kmeans_path = path + "/kmeans"
    >>> kmeans.save(kmeans_path)
    >>> kmeans2 = KMeans.load(kmeans_path)
    >>> kmeans2.getK()
    2
    >>> model_path = path + "/kmeans_model"
    >>> model.save(model_path)
    >>> model2 = KMeansModel.load(model_path)
    >>> model.clusterCenters()[0] == model2.clusterCenters()[0]
    array([ True,  True], dtype=bool)
    >>> model.clusterCenters()[1] == model2.clusterCenters()[1]
    array([ True,  True], dtype=bool)
    >>> from shutil import rmtree
    >>> try:
    ...     rmtree(path)
    ... except OSError:
    ...     pass

    .. versionadded:: 1.5.0
    """

    k = Param(Params._dummy(), "k", "number of clusters to create")
    initMode = Param(Params._dummy(), "initMode",
                     "the initialization algorithm. This can be either \"random\" to " +
                     "choose random points as initial cluster centers, or \"k-means||\" " +
                     "to use a parallel variant of k-means++")
    initSteps = Param(Params._dummy(), "initSteps", "steps for k-means initialization mode")

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
        self._paramMap[self.k] = value
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
        self._paramMap[self.initMode] = value
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
        self._paramMap[self.initSteps] = value
        return self

    @since("1.5.0")
    def getInitSteps(self):
        """
        Gets the value of `initSteps`
        """
        return self.getOrDefault(self.initSteps)


class BisectingKMeansModel(JavaModel):
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
class BisectingKMeans(JavaEstimator, HasFeaturesCol, HasPredictionCol, HasMaxIter, HasSeed):
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

    .. versionadded:: 2.0.0
    """

    k = Param(Params._dummy(), "k", "number of clusters to create")
    minDivisibleClusterSize = Param(Params._dummy(), "minDivisibleClusterSize",
                                    "the minimum number of points (if >= 1.0) " +
                                    "or the minimum proportion")

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
        self._paramMap[self.k] = value
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
        self._paramMap[self.minDivisibleClusterSize] = value
        return self

    @since("2.0.0")
    def getMinDivisibleClusterSize(self):
        """
        Gets the value of `minDivisibleClusterSize` or its default value.
        """
        return self.getOrDefault(self.minDivisibleClusterSize)

    def _create_model(self, java_model):
        return BisectingKMeansModel(java_model)


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
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    sc.stop()
    if failure_count:
        exit(-1)
