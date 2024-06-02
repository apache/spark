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

import array
import sys
from typing import Any, List, NamedTuple, Optional, Tuple, Type, Union

from pyspark import SparkContext, since
from pyspark.core.rdd import RDD
from pyspark.mllib.common import JavaModelWrapper, callMLlibFunc, inherit_doc
from pyspark.mllib.util import JavaLoader, JavaSaveable
from pyspark.sql import DataFrame

__all__ = ["MatrixFactorizationModel", "ALS", "Rating"]


class Rating(NamedTuple):
    """
    Represents a (user, product, rating) tuple.

    .. versionadded:: 1.2.0

    Examples
    --------
    >>> r = Rating(1, 2, 5.0)
    >>> (r.user, r.product, r.rating)
    (1, 2, 5.0)
    >>> (r[0], r[1], r[2])
    (1, 2, 5.0)
    """

    user: int
    product: int
    rating: float

    def __reduce__(self) -> Tuple[Type["Rating"], Tuple[int, int, float]]:
        return Rating, (int(self.user), int(self.product), float(self.rating))


@inherit_doc
class MatrixFactorizationModel(
    JavaModelWrapper, JavaSaveable, JavaLoader["MatrixFactorizationModel"]
):

    """A matrix factorisation model trained by regularized alternating
    least-squares.

    .. versionadded:: 0.9.0

    Examples
    --------
    >>> r1 = (1, 1, 1.0)
    >>> r2 = (1, 2, 2.0)
    >>> r3 = (2, 1, 2.0)
    >>> ratings = sc.parallelize([r1, r2, r3])
    >>> model = ALS.trainImplicit(ratings, 1, seed=10)
    >>> model.predict(2, 2)
    0.4...

    >>> testset = sc.parallelize([(1, 2), (1, 1)])
    >>> model = ALS.train(ratings, 2, seed=0)
    >>> model.predictAll(testset).collect()
    [Rating(user=1, product=1, rating=1.0...), Rating(user=1, product=2, rating=1.9...)]

    >>> model = ALS.train(ratings, 4, seed=10)
    >>> model.userFeatures().collect()
    [(1, array('d', [...])), (2, array('d', [...]))]

    >>> model.recommendUsers(1, 2)
    [Rating(user=2, product=1, rating=1.9...), Rating(user=1, product=1, rating=1.0...)]
    >>> model.recommendProducts(1, 2)
    [Rating(user=1, product=2, rating=1.9...), Rating(user=1, product=1, rating=1.0...)]
    >>> model.rank
    4

    >>> first_user = model.userFeatures().take(1)[0]
    >>> latents = first_user[1]
    >>> len(latents)
    4

    >>> model.productFeatures().collect()
    [(1, array('d', [...])), (2, array('d', [...]))]

    >>> first_product = model.productFeatures().take(1)[0]
    >>> latents = first_product[1]
    >>> len(latents)
    4

    >>> products_for_users = model.recommendProductsForUsers(1).collect()
    >>> len(products_for_users)
    2
    >>> products_for_users[0]
    (1, (Rating(user=1, product=2, rating=...),))

    >>> users_for_products = model.recommendUsersForProducts(1).collect()
    >>> len(users_for_products)
    2
    >>> users_for_products[0]
    (1, (Rating(user=2, product=1, rating=...),))

    >>> model = ALS.train(ratings, 1, nonnegative=True, seed=123456789)
    >>> model.predict(2, 2)
    3.73...

    >>> df = sqlContext.createDataFrame([Rating(1, 1, 1.0), Rating(1, 2, 2.0), Rating(2, 1, 2.0)])
    >>> model = ALS.train(df, 1, nonnegative=True, seed=123456789)
    >>> model.predict(2, 2)
    3.73...

    >>> model = ALS.trainImplicit(ratings, 1, nonnegative=True, seed=123456789)
    >>> model.predict(2, 2)
    0.4...

    >>> import os, tempfile
    >>> path = tempfile.mkdtemp()
    >>> model.save(sc, path)
    >>> sameModel = MatrixFactorizationModel.load(sc, path)
    >>> sameModel.predict(2, 2)
    0.4...
    >>> sameModel.predictAll(testset).collect()
    [Rating(...
    >>> from shutil import rmtree
    >>> try:
    ...     rmtree(path)
    ... except OSError:
    ...     pass
    """

    @since("0.9.0")
    def predict(self, user: int, product: int) -> float:
        """
        Predicts rating for the given user and product.
        """
        return self._java_model.predict(int(user), int(product))

    @since("0.9.0")
    def predictAll(self, user_product: RDD[Tuple[int, int]]) -> RDD[Rating]:
        """
        Returns a list of predicted ratings for input user and product
        pairs.
        """
        assert isinstance(user_product, RDD), "user_product should be RDD of (user, product)"
        first = user_product.first()
        assert len(first) == 2, "user_product should be RDD of (user, product)"
        user_product = user_product.map(lambda u_p: (int(u_p[0]), int(u_p[1])))
        return self.call("predict", user_product)

    @since("1.2.0")
    def userFeatures(self) -> RDD[Tuple[int, array.array]]:
        """
        Returns a paired RDD, where the first element is the user and the
        second is an array of features corresponding to that user.
        """
        return self.call("getUserFeatures").mapValues(lambda v: array.array("d", v))

    @since("1.2.0")
    def productFeatures(self) -> RDD[Tuple[int, array.array]]:
        """
        Returns a paired RDD, where the first element is the product and the
        second is an array of features corresponding to that product.
        """
        return self.call("getProductFeatures").mapValues(lambda v: array.array("d", v))

    @since("1.4.0")
    def recommendUsers(self, product: int, num: int) -> List[Rating]:
        """
        Recommends the top "num" number of users for a given product and
        returns a list of Rating objects sorted by the predicted rating in
        descending order.
        """
        return list(self.call("recommendUsers", product, num))

    @since("1.4.0")
    def recommendProducts(self, user: int, num: int) -> List[Rating]:
        """
        Recommends the top "num" number of products for a given user and
        returns a list of Rating objects sorted by the predicted rating in
        descending order.
        """
        return list(self.call("recommendProducts", user, num))

    def recommendProductsForUsers(self, num: int) -> RDD[Tuple[int, Tuple[Rating, ...]]]:
        """
        Recommends the top "num" number of products for all users. The
        number of recommendations returned per user may be less than "num".
        """
        return self.call("wrappedRecommendProductsForUsers", num)

    def recommendUsersForProducts(self, num: int) -> RDD[Tuple[int, Tuple[Rating, ...]]]:
        """
        Recommends the top "num" number of users for all products. The
        number of recommendations returned per product may be less than
        "num".
        """
        return self.call("wrappedRecommendUsersForProducts", num)

    @property
    @since("1.4.0")
    def rank(self) -> int:
        """Rank for the features in this model"""
        return self.call("rank")

    @classmethod
    @since("1.3.1")
    def load(cls, sc: SparkContext, path: str) -> "MatrixFactorizationModel":
        """Load a model from the given path"""
        model = cls._load_java(sc, path)
        assert sc._jvm is not None
        wrapper = sc._jvm.org.apache.spark.mllib.api.python.MatrixFactorizationModelWrapper(model)
        return MatrixFactorizationModel(wrapper)


class ALS:
    """Alternating Least Squares matrix factorization

    .. versionadded:: 0.9.0
    """

    @classmethod
    def _prepare(cls, ratings: Any) -> RDD[Rating]:
        if isinstance(ratings, RDD):
            pass
        elif isinstance(ratings, DataFrame):
            ratings = ratings.rdd
        else:
            raise TypeError(
                "Ratings should be represented by either an RDD or a DataFrame, "
                "but got %s." % type(ratings)
            )
        first = ratings.first()
        if isinstance(first, Rating):
            pass
        elif isinstance(first, (tuple, list)):
            ratings = ratings.map(lambda x: Rating(*x))
        else:
            raise TypeError("Expect a Rating or a tuple/list, but got %s." % type(first))
        return ratings

    @classmethod
    def train(
        cls,
        ratings: Union[RDD[Rating], RDD[Tuple[int, int, float]]],
        rank: int,
        iterations: int = 5,
        lambda_: float = 0.01,
        blocks: int = -1,
        nonnegative: bool = False,
        seed: Optional[int] = None,
    ) -> MatrixFactorizationModel:
        """
        Train a matrix factorization model given an RDD of ratings by users
        for a subset of products. The ratings matrix is approximated as the
        product of two lower-rank matrices of a given rank (number of
        features). To solve for these features, ALS is run iteratively with
        a configurable level of parallelism.

        .. versionadded:: 0.9.0

        Parameters
        ----------
        ratings : :py:class:`pyspark.RDD`
            RDD of `Rating` or (userID, productID, rating) tuple.
        rank : int
            Number of features to use (also referred to as the number of latent factors).
        iterations : int, optional
            Number of iterations of ALS.
            (default: 5)
        lambda\\_ : float, optional
            Regularization parameter.
            (default: 0.01)
        blocks : int, optional
            Number of blocks used to parallelize the computation. A value
            of -1 will use an auto-configured number of blocks.
            (default: -1)
        nonnegative : bool, optional
            A value of True will solve least-squares with nonnegativity
            constraints.
            (default: False)
        seed : bool, optional
            Random seed for initial matrix factorization model. A value
            of None will use system time as the seed.
            (default: None)
        """
        model = callMLlibFunc(
            "trainALSModel",
            cls._prepare(ratings),
            rank,
            iterations,
            lambda_,
            blocks,
            nonnegative,
            seed,
        )
        return MatrixFactorizationModel(model)

    @classmethod
    def trainImplicit(
        cls,
        ratings: Union[RDD[Rating], RDD[Tuple[int, int, float]]],
        rank: int,
        iterations: int = 5,
        lambda_: float = 0.01,
        blocks: int = -1,
        alpha: float = 0.01,
        nonnegative: bool = False,
        seed: Optional[int] = None,
    ) -> MatrixFactorizationModel:
        """
        Train a matrix factorization model given an RDD of 'implicit
        preferences' of users for a subset of products. The ratings matrix
        is approximated as the product of two lower-rank matrices of a
        given rank (number of features). To solve for these features, ALS
        is run iteratively with a configurable level of parallelism.

        .. versionadded:: 0.9.0

        Parameters
        ----------
        ratings : :py:class:`pyspark.RDD`
            RDD of `Rating` or (userID, productID, rating) tuple.
        rank : int
            Number of features to use (also referred to as the number of latent factors).
        iterations : int, optional
            Number of iterations of ALS.
            (default: 5)
        lambda\\_ : float, optional
            Regularization parameter.
            (default: 0.01)
        blocks : int, optional
            Number of blocks used to parallelize the computation. A value
            of -1 will use an auto-configured number of blocks.
            (default: -1)
        alpha : float, optional
            A constant used in computing confidence.
            (default: 0.01)
        nonnegative : bool, optional
            A value of True will solve least-squares with nonnegativity
            constraints.
            (default: False)
        seed : int, optional
            Random seed for initial matrix factorization model. A value
            of None will use system time as the seed.
            (default: None)
        """
        model = callMLlibFunc(
            "trainImplicitALSModel",
            cls._prepare(ratings),
            rank,
            iterations,
            lambda_,
            blocks,
            alpha,
            nonnegative,
            seed,
        )
        return MatrixFactorizationModel(model)


def _test() -> None:
    import doctest
    import pyspark.mllib.recommendation
    from pyspark.sql import SQLContext

    globs = pyspark.mllib.recommendation.__dict__.copy()
    sc = SparkContext("local[4]", "PythonTest")
    globs["sc"] = sc
    globs["sqlContext"] = SQLContext(sc)
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    globs["sc"].stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
