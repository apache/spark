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

from pyspark.ml.util import keyword_only
from pyspark.ml.wrapper import JavaEstimator, JavaModel
from pyspark.ml.param.shared import *
from pyspark.mllib.common import inherit_doc


__all__ = ['ALS', 'ALSModel']


@inherit_doc
class ALS(JavaEstimator, HasCheckpointInterval, HasMaxIter, HasPredictionCol, HasRegParam, HasSeed):
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
    "Collaborative Filtering for Implicit Feedback Datasets", available
    at `http://dx.doi.org/10.1109/ICDM.2008.22`, adapted for the blocked
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
    Row(user=0, item=2, prediction=0.39...)
    >>> predictions[1]
    Row(user=1, item=0, prediction=3.19...)
    >>> predictions[2]
    Row(user=2, item=0, prediction=-1.15...)
    """

    # a placeholder to make it appear in the generated doc
    rank = Param(Params._dummy(), "rank", "rank of the factorization")
    numUserBlocks = Param(Params._dummy(), "numUserBlocks", "number of user blocks")
    numItemBlocks = Param(Params._dummy(), "numItemBlocks", "number of item blocks")
    implicitPrefs = Param(Params._dummy(), "implicitPrefs", "whether to use implicit preference")
    alpha = Param(Params._dummy(), "alpha", "alpha for implicit preference")
    userCol = Param(Params._dummy(), "userCol", "column name for user ids")
    itemCol = Param(Params._dummy(), "itemCol", "column name for item ids")
    ratingCol = Param(Params._dummy(), "ratingCol", "column name for ratings")
    nonnegative = Param(Params._dummy(), "nonnegative",
                        "whether to use nonnegative constraint for least squares")

    @keyword_only
    def __init__(self, rank=10, maxIter=10, regParam=0.1, numUserBlocks=10, numItemBlocks=10,
                 implicitPrefs=False, alpha=1.0, userCol="user", itemCol="item", seed=None,
                 ratingCol="rating", nonnegative=False, checkpointInterval=10):
        """
        __init__(self, rank=10, maxIter=10, regParam=0.1, numUserBlocks=10, numItemBlocks=10, \
                 implicitPrefs=false, alpha=1.0, userCol="user", itemCol="item", seed=None, \
                 ratingCol="rating", nonnegative=false, checkpointInterval=10)
        """
        super(ALS, self).__init__()
        self._java_obj = self._new_java_obj("org.apache.spark.ml.recommendation.ALS", self.uid)
        self.rank = Param(self, "rank", "rank of the factorization")
        self.numUserBlocks = Param(self, "numUserBlocks", "number of user blocks")
        self.numItemBlocks = Param(self, "numItemBlocks", "number of item blocks")
        self.implicitPrefs = Param(self, "implicitPrefs", "whether to use implicit preference")
        self.alpha = Param(self, "alpha", "alpha for implicit preference")
        self.userCol = Param(self, "userCol", "column name for user ids")
        self.itemCol = Param(self, "itemCol", "column name for item ids")
        self.ratingCol = Param(self, "ratingCol", "column name for ratings")
        self.nonnegative = Param(self, "nonnegative",
                                 "whether to use nonnegative constraint for least squares")
        self._setDefault(rank=10, maxIter=10, regParam=0.1, numUserBlocks=10, numItemBlocks=10,
                         implicitPrefs=False, alpha=1.0, userCol="user", itemCol="item", seed=None,
                         ratingCol="rating", nonnegative=False, checkpointInterval=10)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, rank=10, maxIter=10, regParam=0.1, numUserBlocks=10, numItemBlocks=10,
                  implicitPrefs=False, alpha=1.0, userCol="user", itemCol="item", seed=None,
                  ratingCol="rating", nonnegative=False, checkpointInterval=10):
        """
        setParams(self, rank=10, maxIter=10, regParam=0.1, numUserBlocks=10, numItemBlocks=10, \
                 implicitPrefs=False, alpha=1.0, userCol="user", itemCol="item", seed=None, \
                 ratingCol="rating", nonnegative=False, checkpointInterval=10)
        Sets params for ALS.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return ALSModel(java_model)

    def setRank(self, value):
        """
        Sets the value of :py:attr:`rank`.
        """
        self._paramMap[self.rank] = value
        return self

    def getRank(self):
        """
        Gets the value of rank or its default value.
        """
        return self.getOrDefault(self.rank)

    def setNumUserBlocks(self, value):
        """
        Sets the value of :py:attr:`numUserBlocks`.
        """
        self._paramMap[self.numUserBlocks] = value
        return self

    def getNumUserBlocks(self):
        """
        Gets the value of numUserBlocks or its default value.
        """
        return self.getOrDefault(self.numUserBlocks)

    def setNumItemBlocks(self, value):
        """
        Sets the value of :py:attr:`numItemBlocks`.
        """
        self._paramMap[self.numItemBlocks] = value
        return self

    def getNumItemBlocks(self):
        """
        Gets the value of numItemBlocks or its default value.
        """
        return self.getOrDefault(self.numItemBlocks)

    def setNumBlocks(self, value):
        """
        Sets both :py:attr:`numUserBlocks` and :py:attr:`numItemBlocks` to the specific value.
        """
        self._paramMap[self.numUserBlocks] = value
        self._paramMap[self.numItemBlocks] = value

    def setImplicitPrefs(self, value):
        """
        Sets the value of :py:attr:`implicitPrefs`.
        """
        self._paramMap[self.implicitPrefs] = value
        return self

    def getImplicitPrefs(self):
        """
        Gets the value of implicitPrefs or its default value.
        """
        return self.getOrDefault(self.implicitPrefs)

    def setAlpha(self, value):
        """
        Sets the value of :py:attr:`alpha`.
        """
        self._paramMap[self.alpha] = value
        return self

    def getAlpha(self):
        """
        Gets the value of alpha or its default value.
        """
        return self.getOrDefault(self.alpha)

    def setUserCol(self, value):
        """
        Sets the value of :py:attr:`userCol`.
        """
        self._paramMap[self.userCol] = value
        return self

    def getUserCol(self):
        """
        Gets the value of userCol or its default value.
        """
        return self.getOrDefault(self.userCol)

    def setItemCol(self, value):
        """
        Sets the value of :py:attr:`itemCol`.
        """
        self._paramMap[self.itemCol] = value
        return self

    def getItemCol(self):
        """
        Gets the value of itemCol or its default value.
        """
        return self.getOrDefault(self.itemCol)

    def setRatingCol(self, value):
        """
        Sets the value of :py:attr:`ratingCol`.
        """
        self._paramMap[self.ratingCol] = value
        return self

    def getRatingCol(self):
        """
        Gets the value of ratingCol or its default value.
        """
        return self.getOrDefault(self.ratingCol)

    def setNonnegative(self, value):
        """
        Sets the value of :py:attr:`nonnegative`.
        """
        self._paramMap[self.nonnegative] = value
        return self

    def getNonnegative(self):
        """
        Gets the value of nonnegative or its default value.
        """
        return self.getOrDefault(self.nonnegative)


class ALSModel(JavaModel):
    """
    Model fitted by ALS.
    """

    @property
    def rank(self):
        """rank of the matrix factorization model"""
        return self._call_java("rank")

    @property
    def userFactors(self):
        """
        a DataFrame that stores user factors in two columns: `id` and
        `features`
        """
        return self._call_java("userFactors")

    @property
    def itemFactors(self):
        """
        a DataFrame that stores item factors in two columns: `id` and
        `features`
        """
        return self._call_java("itemFactors")


if __name__ == "__main__":
    import doctest
    from pyspark.context import SparkContext
    from pyspark.sql import SQLContext
    globs = globals().copy()
    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    sc = SparkContext("local[2]", "ml.recommendation tests")
    sqlContext = SQLContext(sc)
    globs['sc'] = sc
    globs['sqlContext'] = sqlContext
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    sc.stop()
    if failure_count:
        exit(-1)
