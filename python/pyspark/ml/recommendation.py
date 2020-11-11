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

from pyspark import since, keyword_only
from pyspark.ml.param.shared import HasPredictionCol, HasBlockSize, HasMaxIter, HasRegParam, \
    HasCheckpointInterval, HasSeed
from pyspark.ml.wrapper import JavaEstimator, JavaModel
from pyspark.ml.common import inherit_doc
from pyspark.ml.param import Params, TypeConverters, Param
from pyspark.ml.util import JavaMLWritable, JavaMLReadable


__all__ = ['ALS', 'ALSModel']


@inherit_doc
class _ALSModelParams(HasPredictionCol, HasBlockSize):
    """
    Params for :py:class:`ALS` and :py:class:`ALSModel`.

    .. versionadded:: 3.0.0
    """

    userCol = Param(Params._dummy(), "userCol", "column name for user ids. Ids must be within " +
                    "the integer value range.", typeConverter=TypeConverters.toString)
    itemCol = Param(Params._dummy(), "itemCol", "column name for item ids. Ids must be within " +
                    "the integer value range.", typeConverter=TypeConverters.toString)
    coldStartStrategy = Param(Params._dummy(), "coldStartStrategy", "strategy for dealing with " +
                              "unknown or new users/items at prediction time. This may be useful " +
                              "in cross-validation or production scenarios, for handling " +
                              "user/item ids the model has not seen in the training data. " +
                              "Supported values: 'nan', 'drop'.",
                              typeConverter=TypeConverters.toString)

    def __init__(self, *args):
        super(_ALSModelParams, self).__init__(*args)
        self._setDefault(blockSize=4096)

    @since("1.4.0")
    def getUserCol(self):
        """
        Gets the value of userCol or its default value.
        """
        return self.getOrDefault(self.userCol)

    @since("1.4.0")
    def getItemCol(self):
        """
        Gets the value of itemCol or its default value.
        """
        return self.getOrDefault(self.itemCol)

    @since("2.2.0")
    def getColdStartStrategy(self):
        """
        Gets the value of coldStartStrategy or its default value.
        """
        return self.getOrDefault(self.coldStartStrategy)


@inherit_doc
class _ALSParams(_ALSModelParams, HasMaxIter, HasRegParam, HasCheckpointInterval, HasSeed):
    """
    Params for :py:class:`ALS`.

    .. versionadded:: 3.0.0
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

    ratingCol = Param(Params._dummy(), "ratingCol", "column name for ratings",
                      typeConverter=TypeConverters.toString)
    nonnegative = Param(Params._dummy(), "nonnegative",
                        "whether to use nonnegative constraint for least squares",
                        typeConverter=TypeConverters.toBoolean)
    intermediateStorageLevel = Param(Params._dummy(), "intermediateStorageLevel",
                                     "StorageLevel for intermediate datasets. Cannot be 'NONE'.",
                                     typeConverter=TypeConverters.toString)
    finalStorageLevel = Param(Params._dummy(), "finalStorageLevel",
                              "StorageLevel for ALS model factors.",
                              typeConverter=TypeConverters.toString)

    def __init__(self, *args):
        super(_ALSParams, self).__init__(*args)
        self._setDefault(rank=10, maxIter=10, regParam=0.1, numUserBlocks=10, numItemBlocks=10,
                         implicitPrefs=False, alpha=1.0, userCol="user", itemCol="item",
                         ratingCol="rating", nonnegative=False, checkpointInterval=10,
                         intermediateStorageLevel="MEMORY_AND_DISK",
                         finalStorageLevel="MEMORY_AND_DISK", coldStartStrategy="nan")

    @since("1.4.0")
    def getRank(self):
        """
        Gets the value of rank or its default value.
        """
        return self.getOrDefault(self.rank)

    @since("1.4.0")
    def getNumUserBlocks(self):
        """
        Gets the value of numUserBlocks or its default value.
        """
        return self.getOrDefault(self.numUserBlocks)

    @since("1.4.0")
    def getNumItemBlocks(self):
        """
        Gets the value of numItemBlocks or its default value.
        """
        return self.getOrDefault(self.numItemBlocks)

    @since("1.4.0")
    def getImplicitPrefs(self):
        """
        Gets the value of implicitPrefs or its default value.
        """
        return self.getOrDefault(self.implicitPrefs)

    @since("1.4.0")
    def getAlpha(self):
        """
        Gets the value of alpha or its default value.
        """
        return self.getOrDefault(self.alpha)

    @since("1.4.0")
    def getRatingCol(self):
        """
        Gets the value of ratingCol or its default value.
        """
        return self.getOrDefault(self.ratingCol)

    @since("1.4.0")
    def getNonnegative(self):
        """
        Gets the value of nonnegative or its default value.
        """
        return self.getOrDefault(self.nonnegative)

    @since("2.0.0")
    def getIntermediateStorageLevel(self):
        """
        Gets the value of intermediateStorageLevel or its default value.
        """
        return self.getOrDefault(self.intermediateStorageLevel)

    @since("2.0.0")
    def getFinalStorageLevel(self):
        """
        Gets the value of finalStorageLevel or its default value.
        """
        return self.getOrDefault(self.finalStorageLevel)


@inherit_doc
class ALS(JavaEstimator, _ALSParams, JavaMLWritable, JavaMLReadable):
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
    <https://doi.org/10.1109/ICDM.2008.22>`_, adapted for the blocked
    approach used here.

    Essentially instead of finding the low-rank approximations to the
    rating matrix `R`, this finds the approximations for a preference
    matrix `P` where the elements of `P` are 1 if r > 0 and 0 if r <= 0.
    The ratings then act as 'confidence' values related to strength of
    indicated user preferences rather than explicit ratings given to
    items.

    .. versionadded:: 1.4.0

    Notes
    -----
    The input rating dataframe to the ALS implementation should be deterministic.
    Nondeterministic data can cause failure during fitting ALS model.
    For example, an order-sensitive operation like sampling after a repartition makes
    dataframe output nondeterministic, like `df.repartition(2).sample(False, 0.5, 1618)`.
    Checkpointing sampled dataframe or adding a sort before sampling can help make the
    dataframe deterministic.

    Examples
    --------
    >>> df = spark.createDataFrame(
    ...     [(0, 0, 4.0), (0, 1, 2.0), (1, 1, 3.0), (1, 2, 4.0), (2, 1, 1.0), (2, 2, 5.0)],
    ...     ["user", "item", "rating"])
    >>> als = ALS(rank=10, seed=0)
    >>> als.setMaxIter(5)
    ALS...
    >>> als.getMaxIter()
    5
    >>> als.setRegParam(0.1)
    ALS...
    >>> als.getRegParam()
    0.1
    >>> als.clear(als.regParam)
    >>> model = als.fit(df)
    >>> model.getBlockSize()
    4096
    >>> model.getUserCol()
    'user'
    >>> model.setUserCol("user")
    ALSModel...
    >>> model.getItemCol()
    'item'
    >>> model.setPredictionCol("newPrediction")
    ALS...
    >>> model.rank
    10
    >>> model.userFactors.orderBy("id").collect()
    [Row(id=0, features=[...]), Row(id=1, ...), Row(id=2, ...)]
    >>> test = spark.createDataFrame([(0, 2), (1, 0), (2, 0)], ["user", "item"])
    >>> predictions = sorted(model.transform(test).collect(), key=lambda r: r[0])
    >>> predictions[0]
    Row(user=0, item=2, newPrediction=0.692910...)
    >>> predictions[1]
    Row(user=1, item=0, newPrediction=3.473569...)
    >>> predictions[2]
    Row(user=2, item=0, newPrediction=-0.899198...)
    >>> user_recs = model.recommendForAllUsers(3)
    >>> user_recs.where(user_recs.user == 0)\
        .select("recommendations.item", "recommendations.rating").collect()
    [Row(item=[0, 1, 2], rating=[3.910..., 1.997..., 0.692...])]
    >>> item_recs = model.recommendForAllItems(3)
    >>> item_recs.where(item_recs.item == 2)\
        .select("recommendations.user", "recommendations.rating").collect()
    [Row(user=[2, 1, 0], rating=[4.892..., 3.991..., 0.692...])]
    >>> user_subset = df.where(df.user == 2)
    >>> user_subset_recs = model.recommendForUserSubset(user_subset, 3)
    >>> user_subset_recs.select("recommendations.item", "recommendations.rating").first()
    Row(item=[2, 1, 0], rating=[4.892..., 1.076..., -0.899...])
    >>> item_subset = df.where(df.item == 0)
    >>> item_subset_recs = model.recommendForItemSubset(item_subset, 3)
    >>> item_subset_recs.select("recommendations.user", "recommendations.rating").first()
    Row(user=[0, 1, 2], rating=[3.910..., 3.473..., -0.899...])
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
    >>> model.transform(test).take(1) == model2.transform(test).take(1)
    True
    """

    @keyword_only
    def __init__(self, *, rank=10, maxIter=10, regParam=0.1, numUserBlocks=10,
                 numItemBlocks=10, implicitPrefs=False, alpha=1.0, userCol="user", itemCol="item",
                 seed=None, ratingCol="rating", nonnegative=False, checkpointInterval=10,
                 intermediateStorageLevel="MEMORY_AND_DISK",
                 finalStorageLevel="MEMORY_AND_DISK", coldStartStrategy="nan", blockSize=4096):
        """
        __init__(self, \\*, rank=10, maxIter=10, regParam=0.1, numUserBlocks=10,
                 numItemBlocks=10, implicitPrefs=False, alpha=1.0, userCol="user", itemCol="item", \
                 seed=None, ratingCol="rating", nonnegative=False, checkpointInterval=10, \
                 intermediateStorageLevel="MEMORY_AND_DISK", \
                 finalStorageLevel="MEMORY_AND_DISK", coldStartStrategy="nan", blockSize=4096)
        """
        super(ALS, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.recommendation.ALS", self.uid)
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, *, rank=10, maxIter=10, regParam=0.1, numUserBlocks=10,
                  numItemBlocks=10, implicitPrefs=False, alpha=1.0, userCol="user", itemCol="item",
                  seed=None, ratingCol="rating", nonnegative=False, checkpointInterval=10,
                  intermediateStorageLevel="MEMORY_AND_DISK",
                  finalStorageLevel="MEMORY_AND_DISK", coldStartStrategy="nan", blockSize=4096):
        """
        setParams(self, \\*, rank=10, maxIter=10, regParam=0.1, numUserBlocks=10, \
                 numItemBlocks=10, implicitPrefs=False, alpha=1.0, userCol="user", itemCol="item", \
                 seed=None, ratingCol="rating", nonnegative=False, checkpointInterval=10, \
                 intermediateStorageLevel="MEMORY_AND_DISK", \
                 finalStorageLevel="MEMORY_AND_DISK", coldStartStrategy="nan", blockSize=4096)
        Sets params for ALS.
        """
        kwargs = self._input_kwargs
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
    def setNumUserBlocks(self, value):
        """
        Sets the value of :py:attr:`numUserBlocks`.
        """
        return self._set(numUserBlocks=value)

    @since("1.4.0")
    def setNumItemBlocks(self, value):
        """
        Sets the value of :py:attr:`numItemBlocks`.
        """
        return self._set(numItemBlocks=value)

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
    def setAlpha(self, value):
        """
        Sets the value of :py:attr:`alpha`.
        """
        return self._set(alpha=value)

    @since("1.4.0")
    def setUserCol(self, value):
        """
        Sets the value of :py:attr:`userCol`.
        """
        return self._set(userCol=value)

    @since("1.4.0")
    def setItemCol(self, value):
        """
        Sets the value of :py:attr:`itemCol`.
        """
        return self._set(itemCol=value)

    @since("1.4.0")
    def setRatingCol(self, value):
        """
        Sets the value of :py:attr:`ratingCol`.
        """
        return self._set(ratingCol=value)

    @since("1.4.0")
    def setNonnegative(self, value):
        """
        Sets the value of :py:attr:`nonnegative`.
        """
        return self._set(nonnegative=value)

    @since("2.0.0")
    def setIntermediateStorageLevel(self, value):
        """
        Sets the value of :py:attr:`intermediateStorageLevel`.
        """
        return self._set(intermediateStorageLevel=value)

    @since("2.0.0")
    def setFinalStorageLevel(self, value):
        """
        Sets the value of :py:attr:`finalStorageLevel`.
        """
        return self._set(finalStorageLevel=value)

    @since("2.2.0")
    def setColdStartStrategy(self, value):
        """
        Sets the value of :py:attr:`coldStartStrategy`.
        """
        return self._set(coldStartStrategy=value)

    def setMaxIter(self, value):
        """
        Sets the value of :py:attr:`maxIter`.
        """
        return self._set(maxIter=value)

    def setRegParam(self, value):
        """
        Sets the value of :py:attr:`regParam`.
        """
        return self._set(regParam=value)

    def setPredictionCol(self, value):
        """
        Sets the value of :py:attr:`predictionCol`.
        """
        return self._set(predictionCol=value)

    def setCheckpointInterval(self, value):
        """
        Sets the value of :py:attr:`checkpointInterval`.
        """
        return self._set(checkpointInterval=value)

    def setSeed(self, value):
        """
        Sets the value of :py:attr:`seed`.
        """
        return self._set(seed=value)

    @since("3.0.0")
    def setBlockSize(self, value):
        """
        Sets the value of :py:attr:`blockSize`.
        """
        return self._set(blockSize=value)


class ALSModel(JavaModel, _ALSModelParams, JavaMLWritable, JavaMLReadable):
    """
    Model fitted by ALS.

    .. versionadded:: 1.4.0
    """

    @since("3.0.0")
    def setUserCol(self, value):
        """
        Sets the value of :py:attr:`userCol`.
        """
        return self._set(userCol=value)

    @since("3.0.0")
    def setItemCol(self, value):
        """
        Sets the value of :py:attr:`itemCol`.
        """
        return self._set(itemCol=value)

    @since("3.0.0")
    def setColdStartStrategy(self, value):
        """
        Sets the value of :py:attr:`coldStartStrategy`.
        """
        return self._set(coldStartStrategy=value)

    @since("3.0.0")
    def setPredictionCol(self, value):
        """
        Sets the value of :py:attr:`predictionCol`.
        """
        return self._set(predictionCol=value)

    @since("3.0.0")
    def setBlockSize(self, value):
        """
        Sets the value of :py:attr:`blockSize`.
        """
        return self._set(blockSize=value)

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

    def recommendForAllUsers(self, numItems):
        """
        Returns top `numItems` items recommended for each user, for all users.

        .. versionadded:: 2.2.0

        Parameters
        ----------
        numItems : int
            max number of recommendations for each user

        Returns
        -------
        :py:class:`pyspark.sql.DataFrame`
            a DataFrame of (userCol, recommendations), where recommendations are
            stored as an array of (itemCol, rating) Rows.
        """
        return self._call_java("recommendForAllUsers", numItems)

    def recommendForAllItems(self, numUsers):
        """
        Returns top `numUsers` users recommended for each item, for all items.

        .. versionadded:: 2.2.0

        Parameters
        ----------
        numUsers : int
            max number of recommendations for each item

        Returns
        -------
        :py:class:`pyspark.sql.DataFrame`
            a DataFrame of (itemCol, recommendations), where recommendations are
            stored as an array of (userCol, rating) Rows.
        """
        return self._call_java("recommendForAllItems", numUsers)

    def recommendForUserSubset(self, dataset, numItems):
        """
        Returns top `numItems` items recommended for each user id in the input data set. Note that
        if there are duplicate ids in the input dataset, only one set of recommendations per unique
        id will be returned.

        .. versionadded:: 2.3.0

        Parameters
        ----------
        dataset : :py:class:`pyspark.sql.DataFrame`
            a DataFrame containing a column of user ids. The column name must match `userCol`.
        numItems : int
            max number of recommendations for each user

        Returns
        -------
        :py:class:`pyspark.sql.DataFrame`
            a DataFrame of (userCol, recommendations), where recommendations are
            stored as an array of (itemCol, rating) Rows.
        """
        return self._call_java("recommendForUserSubset", dataset, numItems)

    def recommendForItemSubset(self, dataset, numUsers):
        """
        Returns top `numUsers` users recommended for each item id in the input data set. Note that
        if there are duplicate ids in the input dataset, only one set of recommendations per unique
        id will be returned.

        .. versionadded:: 2.3.0

        Parameters
        ----------
        dataset : :py:class:`pyspark.sql.DataFrame`
            a DataFrame containing a column of item ids. The column name must match `itemCol`.
        numUsers : int
            max number of recommendations for each item

        Returns
        -------
        :py:class:`pyspark.sql.DataFrame`
            a DataFrame of (itemCol, recommendations), where recommendations are
            stored as an array of (userCol, rating) Rows.
        """
        return self._call_java("recommendForItemSubset", dataset, numUsers)


if __name__ == "__main__":
    import doctest
    import pyspark.ml.recommendation
    from pyspark.sql import SparkSession
    globs = pyspark.ml.recommendation.__dict__.copy()
    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    spark = SparkSession.builder\
        .master("local[2]")\
        .appName("ml.recommendation tests")\
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
        sys.exit(-1)
