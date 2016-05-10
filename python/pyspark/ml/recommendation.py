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
from pyspark.ml.wrapper import JavaEstimator, JavaModel
from pyspark.ml.param.shared import *
from pyspark.mllib.common import inherit_doc


__all__ = ['ALS', 'ALSModel']


@inherit_doc
class ALS(JavaEstimator, HasCheckpointInterval, HasMaxIter, HasPredictionCol, HasRegParam, HasSeed,
          JavaMLWritable, JavaMLReadable):
    """
    Alternating Least Squares (ALS) matrix factorization.

    ALS attempts to estimate the ratings matrix `R` as the product of
    two lower-rank matrices, `X` and `Y`, i.e. `X * Yt = R`. Typically
    these approximations are called 'factor' matrices. The general
    approach is iterative. During each iteration, one of the factor
    matrices is held constant, while the other is solved for using least
    squares. The newly-solved factor matrix is then held constant while
    solving for the other factor matrix.

    This is a blocked implementation of the ALS factorization algorithm
    that groups the two sets of factors (referred to as "users" and
    "products") into blocks and reduces communication by only sending
    one copy of each user vector to each product block on each
    iteration, and only for the product blocks that need that user's
    feature vector. This is achieved by pre-computing some information
    about the ratings matrix to determine the "out-links" of each user
    (which blocks of products it will contribute to) and "in-link"
    information for each product (which of the feature vectors it
    receives from each user block it will depend on). This allows us to
    send only an array of feature vectors between each user block and
    product block, and have the product block find the users' ratings
    and update the products based on these messages.

    For implicit preference data, the algorithm used is based on
    `"Collaborative Filtering for Implicit Feedback Datasets",
    <http://dx.doi.org/10.1109/ICDM.2008.22>`_, adapted for the blocked
    approach used here.

    Essentially instead of finding the low-rank approximations to the
    rating matrix `R`, this finds the approximations for a preference
    matrix `P` where the elements of `P` are 1 if r > 0 and 0 if r <= 0.
    The ratings then act as 'confidence' values related to strength of
    indicated user preferences rather than explicit ratings given to
    items.

    >>> df = sqlContext.createDataFrame(
    ...     [(0, 0, 4.0), (0, 1, 2.0), (1, 1, 3.0), (1, 2, 4.0), (2, 1, 1.0), (2, 2, 5.0)],
    ...     ["user", "item", "rating"])
    >>> als = ALS(rank=10, maxIter=5)
    >>> model = als.fit(df)
    >>> model.rank
    10
    >>> model.userFactors.orderBy("id").collect()
    [Row(id=0, features=[...]), Row(id=1, ...), Row(id=2, ...)]
    >>> test = sqlContext.createDataFrame([(0, 2), (1, 0), (2, 0)], ["user", "item"])
    >>> predictions = sorted(model.transform(test).collect(), key=lambda r: r[0])
    >>> predictions[0]
    Row(user=0, item=2, prediction=-0.13807615637779236)
    >>> predictions[1]
    Row(user=1, item=0, prediction=2.6258413791656494)
    >>> predictions[2]
    Row(user=2, item=0, prediction=-1.5018409490585327)
    >>> als_path = temp_path + "/als"
    >>> als.save(als_path)
    >>> als2 = ALS.load(als_path)
    >>> als.getMaxIter()
    5
    >>> model_path = temp_path + "/als_model"
    >>> model.save(model_path)
    >>> model2 = ALSModel.load(model_path)
    >>> model.rank == model2.rank
    True
    >>> sorted(model.userFactors.collect()) == sorted(model2.userFactors.collect())
    True
    >>> sorted(model.itemFactors.collect()) == sorted(model2.itemFactors.collect())
    True

    .. versionadded:: 1.4.0
    """

    rank = Param(Params._dummy(), "rank", "rank of the factorization",
                 typeConverter=TypeConverters.toInt)
    numUserBlocks = Param(Params._dummy(), "numUserBlocks", "number of user blocks",
                          typeConverter=TypeConverters.toInt)
    numItemBlocks = Param(Params._dummy(), "numItemBlocks", "number of item blocks",
                          typeConverter=TypeConverters.toInt)
    implicitPrefs = Param(Params._dummy(), "implicitPrefs", "whether to use implicit preference",
                          typeConverter=TypeConverters.toBoolean)
    alpha = Param(Params._dummy(), "alpha", "alpha for implicit preference",
                  typeConverter=TypeConverters.toFloat)
    userCol = Param(Params._dummy(), "userCol", "column name for user ids",
                    typeConverter=TypeConverters.toString)
    itemCol = Param(Params._dummy(), "itemCol", "column name for item ids",
                    typeConverter=TypeConverters.toString)
    ratingCol = Param(Params._dummy(), "ratingCol", "column name for ratings",
                      typeConverter=TypeConverters.toString)
    nonnegative = Param(Params._dummy(), "nonnegative",
                        "whether to use nonnegative constraint for least squares",
                        typeConverter=TypeConverters.toBoolean)
    intermediateStorageLevel = Param(Params._dummy(), "intermediateStorageLevel",
                                     "StorageLevel for intermediate datasets. Cannot be 'NONE'. " +
                                     "Default: 'MEMORY_AND_DISK'.",
                                     typeConverter=TypeConverters.toString)
    finalStorageLevel = Param(Params._dummy(), "finalStorageLevel",
                              "StorageLevel for ALS model factors. " +
                              "Default: 'MEMORY_AND_DISK'.",
                              typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, rank=10, maxIter=10, regParam=0.1, numUserBlocks=10, numItemBlocks=10,
                 implicitPrefs=False, alpha=1.0, userCol="user", itemCol="item", seed=None,
                 ratingCol="rating", nonnegative=False, checkpointInterval=10,
                 intermediateStorageLevel="MEMORY_AND_DISK",
                 finalStorageLevel="MEMORY_AND_DISK"):
        """
        __init__(self, rank=10, maxIter=10, regParam=0.1, numUserBlocks=10, numItemBlocks=10, \
                 implicitPrefs=false, alpha=1.0, userCol="user", itemCol="item", seed=None, \
                 ratingCol="rating", nonnegative=false, checkpointInterval=10, \
                 intermediateStorageLevel="MEMORY_AND_DISK", \
                 finalStorageLevel="MEMORY_AND_DISK")
        """
        super(ALS, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.recommendation.ALS", self.uid)
        self._setDefault(rank=10, maxIter=10, regParam=0.1, numUserBlocks=10, numItemBlocks=10,
                         implicitPrefs=False, alpha=1.0, userCol="user", itemCol="item", seed=None,
                         ratingCol="rating", nonnegative=False, checkpointInterval=10,
                         intermediateStorageLevel="MEMORY_AND_DISK",
                         finalStorageLevel="MEMORY_AND_DISK")
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, rank=10, maxIter=10, regParam=0.1, numUserBlocks=10, numItemBlocks=10,
                  implicitPrefs=False, alpha=1.0, userCol="user", itemCol="item", seed=None,
                  ratingCol="rating", nonnegative=False, checkpointInterval=10,
                  intermediateStorageLevel="MEMORY_AND_DISK",
                  finalStorageLevel="MEMORY_AND_DISK"):
        """
        setParams(self, rank=10, maxIter=10, regParam=0.1, numUserBlocks=10, numItemBlocks=10, \
                 implicitPrefs=False, alpha=1.0, userCol="user", itemCol="item", seed=None, \
                 ratingCol="rating", nonnegative=False, checkpointInterval=10, \
                 intermediateStorageLevel="MEMORY_AND_DISK", \
                 finalStorageLevel="MEMORY_AND_DISK")
        Sets params for ALS.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return ALSModel(java_model)

    @since("1.4.0")
    def setRank(self, value):
        """
        Sets the value of :py:attr:`rank`.
        """
        return self._set(rank=value)

    @since("1.4.0")
    def getRank(self):
        """
        Gets the value of rank or its default value.
        """
        return self.getOrDefault(self.rank)

    @since("1.4.0")
    def setNumUserBlocks(self, value):
        """
        Sets the value of :py:attr:`numUserBlocks`.
        """
        return self._set(numUserBlocks=value)

    @since("1.4.0")
    def getNumUserBlocks(self):
        """
        Gets the value of numUserBlocks or its default value.
        """
        return self.getOrDefault(self.numUserBlocks)

    @since("1.4.0")
    def setNumItemBlocks(self, value):
        """
        Sets the value of :py:attr:`numItemBlocks`.
        """
        return self._set(numItemBlocks=value)

    @since("1.4.0")
    def getNumItemBlocks(self):
        """
        Gets the value of numItemBlocks or its default value.
        """
        return self.getOrDefault(self.numItemBlocks)

    @since("1.4.0")
    def setNumBlocks(self, value):
        """
        Sets both :py:attr:`numUserBlocks` and :py:attr:`numItemBlocks` to the specific value.
        """
        self._set(numUserBlocks=value)
        return self._set(numItemBlocks=value)

    @since("1.4.0")
    def setImplicitPrefs(self, value):
        """
        Sets the value of :py:attr:`implicitPrefs`.
        """
        return self._set(implicitPrefs=value)

    @since("1.4.0")
    def getImplicitPrefs(self):
        """
        Gets the value of implicitPrefs or its default value.
        """
        return self.getOrDefault(self.implicitPrefs)

    @since("1.4.0")
    def setAlpha(self, value):
        """
        Sets the value of :py:attr:`alpha`.
        """
        return self._set(alpha=value)

    @since("1.4.0")
    def getAlpha(self):
        """
        Gets the value of alpha or its default value.
        """
        return self.getOrDefault(self.alpha)

    @since("1.4.0")
    def setUserCol(self, value):
        """
        Sets the value of :py:attr:`userCol`.
        """
        return self._set(userCol=value)

    @since("1.4.0")
    def getUserCol(self):
        """
        Gets the value of userCol or its default value.
        """
        return self.getOrDefault(self.userCol)

    @since("1.4.0")
    def setItemCol(self, value):
        """
        Sets the value of :py:attr:`itemCol`.
        """
        return self._set(itemCol=value)

    @since("1.4.0")
    def getItemCol(self):
        """
        Gets the value of itemCol or its default value.
        """
        return self.getOrDefault(self.itemCol)

    @since("1.4.0")
    def setRatingCol(self, value):
        """
        Sets the value of :py:attr:`ratingCol`.
        """
        return self._set(ratingCol=value)

    @since("1.4.0")
    def getRatingCol(self):
        """
        Gets the value of ratingCol or its default value.
        """
        return self.getOrDefault(self.ratingCol)

    @since("1.4.0")
    def setNonnegative(self, value):
        """
        Sets the value of :py:attr:`nonnegative`.
        """
        return self._set(nonnegative=value)

    @since("1.4.0")
    def getNonnegative(self):
        """
        Gets the value of nonnegative or its default value.
        """
        return self.getOrDefault(self.nonnegative)

    @since("2.0.0")
    def setIntermediateStorageLevel(self, value):
        """
        Sets the value of :py:attr:`intermediateStorageLevel`.
        """
        return self._set(intermediateStorageLevel=value)

    @since("2.0.0")
    def getIntermediateStorageLevel(self):
        """
        Gets the value of intermediateStorageLevel or its default value.
        """
        return self.getOrDefault(self.intermediateStorageLevel)

    @since("2.0.0")
    def setFinalStorageLevel(self, value):
        """
        Sets the value of :py:attr:`finalStorageLevel`.
        """
        return self._set(finalStorageLevel=value)

    @since("2.0.0")
    def getFinalStorageLevel(self):
        """
        Gets the value of finalStorageLevel or its default value.
        """
        return self.getOrDefault(self.finalStorageLevel)


class ALSModel(JavaModel, JavaMLWritable, JavaMLReadable):
    """
    Model fitted by ALS.

    .. versionadded:: 1.4.0
    """

    @property
    @since("1.4.0")
    def rank(self):
        """rank of the matrix factorization model"""
        return self._call_java("rank")

    @property
    @since("1.4.0")
    def userFactors(self):
        """
        a DataFrame that stores user factors in two columns: `id` and
        `features`
        """
        return self._call_java("userFactors")

    @property
    @since("1.4.0")
    def itemFactors(self):
        """
        a DataFrame that stores item factors in two columns: `id` and
        `features`
        """
        return self._call_java("itemFactors")


if __name__ == "__main__":
    import doctest
    import pyspark.ml.recommendation
    from pyspark.context import SparkContext
    from pyspark.sql import SQLContext
    globs = pyspark.ml.recommendation.__dict__.copy()
    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    sc = SparkContext("local[2]", "ml.recommendation tests")
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
