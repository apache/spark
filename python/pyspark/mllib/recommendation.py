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
from collections import namedtuple

from pyspark import SparkContext
from pyspark.rdd import RDD
from pyspark.mllib.common import JavaModelWrapper, callMLlibFunc, inherit_doc
from pyspark.mllib.util import JavaLoader, JavaSaveable
from pyspark.sql import DataFrame

__all__ = ['MatrixFactorizationModel', 'ALS', 'Rating']


class Rating(namedtuple("Rating", ["user", "product", "rating"])):
    """
    Represents a (user, product, rating) tuple.

    >>> r = Rating(1, 2, 5.0)
    >>> (r.user, r.product, r.rating)
    (1, 2, 5.0)
    >>> (r[0], r[1], r[2])
    (1, 2, 5.0)
    """

    def __reduce__(self):
        return Rating, (int(self.user), int(self.product), float(self.rating))


@inherit_doc
class MatrixFactorizationModel(JavaModelWrapper, JavaSaveable, JavaLoader):

    """A matrix factorisation model trained by regularized alternating
    least-squares.

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
    >>> len(latents) == 4
    True

    >>> model.productFeatures().collect()
    [(1, array('d', [...])), (2, array('d', [...]))]

    >>> first_product = model.productFeatures().take(1)[0]
    >>> latents = first_product[1]
    >>> len(latents) == 4
    True

    >>> model = ALS.train(ratings, 1, nonnegative=True, seed=10)
    >>> model.predict(2, 2)
    3.8...

    >>> df = sqlContext.createDataFrame([Rating(1, 1, 1.0), Rating(1, 2, 2.0), Rating(2, 1, 2.0)])
    >>> model = ALS.train(df, 1, nonnegative=True, seed=10)
    >>> model.predict(2, 2)
    3.8...

    >>> model = ALS.trainImplicit(ratings, 1, nonnegative=True, seed=10)
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
    >>> try:
    ...     os.removedirs(path)
    ... except OSError:
    ...     pass
    """
    def predict(self, user, product):
        """
        Predicts rating for the given user and product.
        """
        return self._java_model.predict(int(user), int(product))

    def predictAll(self, user_product):
        """
        Returns a list of predicted ratings for input user and product pairs.
        """
        assert isinstance(user_product, RDD), "user_product should be RDD of (user, product)"
        first = user_product.first()
        assert len(first) == 2, "user_product should be RDD of (user, product)"
        user_product = user_product.map(lambda u_p: (int(u_p[0]), int(u_p[1])))
        return self.call("predict", user_product)

    def userFeatures(self):
        """
        Returns a paired RDD, where the first element is the user and the
        second is an array of features corresponding to that user.
        """
        return self.call("getUserFeatures").mapValues(lambda v: array.array('d', v))

    def productFeatures(self):
        """
        Returns a paired RDD, where the first element is the product and the
        second is an array of features corresponding to that product.
        """
        return self.call("getProductFeatures").mapValues(lambda v: array.array('d', v))

    def recommendUsers(self, product, num):
        """
        Recommends the top "num" number of users for a given product and returns a list
        of Rating objects sorted by the predicted rating in descending order.
        """
        return list(self.call("recommendUsers", product, num))

    def recommendProducts(self, user, num):
        """
        Recommends the top "num" number of products for a given user and returns a list
        of Rating objects sorted by the predicted rating in descending order.
        """
        return list(self.call("recommendProducts", user, num))

    @property
    def rank(self):
        return self.call("rank")

    @classmethod
    def load(cls, sc, path):
        model = cls._load_java(sc, path)
        wrapper = sc._jvm.MatrixFactorizationModelWrapper(model)
        return MatrixFactorizationModel(wrapper)


class ALS(object):

    @classmethod
    def _prepare(cls, ratings):
        if isinstance(ratings, RDD):
            pass
        elif isinstance(ratings, DataFrame):
            ratings = ratings.rdd
        else:
            raise TypeError("Ratings should be represented by either an RDD or a DataFrame, "
                            "but got %s." % type(ratings))
        first = ratings.first()
        if isinstance(first, Rating):
            pass
        elif isinstance(first, (tuple, list)):
            ratings = ratings.map(lambda x: Rating(*x))
        else:
            raise TypeError("Expect a Rating or a tuple/list, but got %s." % type(first))
        return ratings

    @classmethod
    def train(cls, ratings, rank, iterations=5, lambda_=0.01, blocks=-1, nonnegative=False,
              seed=None):
        model = callMLlibFunc("trainALSModel", cls._prepare(ratings), rank, iterations,
                              lambda_, blocks, nonnegative, seed)
        return MatrixFactorizationModel(model)

    @classmethod
    def trainImplicit(cls, ratings, rank, iterations=5, lambda_=0.01, blocks=-1, alpha=0.01,
                      nonnegative=False, seed=None):
        model = callMLlibFunc("trainImplicitALSModel", cls._prepare(ratings), rank,
                              iterations, lambda_, blocks, alpha, nonnegative, seed)
        return MatrixFactorizationModel(model)


def _test():
    import doctest
    import pyspark.mllib.recommendation
    from pyspark.sql import SQLContext
    globs = pyspark.mllib.recommendation.__dict__.copy()
    sc = SparkContext('local[4]', 'PythonTest')
    globs['sc'] = sc
    globs['sqlContext'] = SQLContext(sc)
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    globs['sc'].stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
