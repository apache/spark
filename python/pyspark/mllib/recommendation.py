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

from pyspark import SparkContext
from pyspark.mllib._common import \
    _get_unmangled_rdd, _get_unmangled_double_vector_rdd, \
    _serialize_double_matrix, _deserialize_double_matrix, \
    _serialize_double_vector, _deserialize_double_vector, \
    _get_initial_weights, _serialize_rating, _regression_train_wrapper, \
    _serialize_tuple, RatingDeserializer
from pyspark.rdd import RDD


class MatrixFactorizationModel(object):

    """A matrix factorisation model trained by regularized alternating
    least-squares.

    >>> r1 = (1, 1, 1.0)
    >>> r2 = (1, 2, 2.0)
    >>> r3 = (2, 1, 2.0)
    >>> ratings = sc.parallelize([r1, r2, r3])
    >>> model = ALS.trainImplicit(ratings, 1)
    >>> model.predict(2,2) is not None
    True
    >>> testset = sc.parallelize([(1, 2), (1, 1)])
    >>> model.predictAll(testset).count() == 2
    True
    """

    def __init__(self, sc, java_model):
        self._context = sc
        self._java_model = java_model

    def __del__(self):
        self._context._gateway.detach(self._java_model)

    def predict(self, user, product):
        return self._java_model.predict(user, product)

    def predictAll(self, usersProducts):
        usersProductsJRDD = _get_unmangled_rdd(usersProducts, _serialize_tuple)
        return RDD(self._java_model.predict(usersProductsJRDD._jrdd),
                   self._context, RatingDeserializer())


class ALS(object):

    @classmethod
    def train(cls, ratings, rank, iterations=5, lambda_=0.01, blocks=-1):
        sc = ratings.context
        ratingBytes = _get_unmangled_rdd(ratings, _serialize_rating)
        mod = sc._jvm.PythonMLLibAPI().trainALSModel(
            ratingBytes._jrdd, rank, iterations, lambda_, blocks)
        return MatrixFactorizationModel(sc, mod)

    @classmethod
    def trainImplicit(cls, ratings, rank, iterations=5, lambda_=0.01, blocks=-1, alpha=0.01):
        sc = ratings.context
        ratingBytes = _get_unmangled_rdd(ratings, _serialize_rating)
        mod = sc._jvm.PythonMLLibAPI().trainImplicitALSModel(
            ratingBytes._jrdd, rank, iterations, lambda_, blocks, alpha)
        return MatrixFactorizationModel(sc, mod)


def _test():
    import doctest
    globs = globals().copy()
    globs['sc'] = SparkContext('local[4]', 'PythonTest', batchSize=2)
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    globs['sc'].stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
