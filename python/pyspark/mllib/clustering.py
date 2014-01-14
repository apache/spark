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

from numpy import array, dot
from math import sqrt
from pyspark import SparkContext
from pyspark.mllib._common import \
    _get_unmangled_rdd, _get_unmangled_double_vector_rdd, \
    _serialize_double_matrix, _deserialize_double_matrix, \
    _serialize_double_vector, _deserialize_double_vector, \
    _get_initial_weights, _serialize_rating, _regression_train_wrapper

class KMeansModel(object):
    """A clustering model derived from the k-means method.

    >>> data = array([0.0,0.0, 1.0,1.0, 9.0,8.0, 8.0,9.0]).reshape(4,2)
    >>> clusters = KMeans.train(sc.parallelize(data), 2, maxIterations=10, runs=30, initializationMode="random")
    >>> clusters.predict(array([0.0, 0.0])) == clusters.predict(array([1.0, 1.0]))
    True
    >>> clusters.predict(array([8.0, 9.0])) == clusters.predict(array([9.0, 8.0]))
    True
    >>> clusters = KMeans.train(sc.parallelize(data), 2)
    """
    def __init__(self, centers_):
        self.centers = centers_

    def predict(self, x):
        """Find the cluster to which x belongs in this model."""
        best = 0
        best_distance = 1e75
        for i in range(0, self.centers.shape[0]):
            diff = x - self.centers[i]
            distance = sqrt(dot(diff, diff))
            if distance < best_distance:
                best = i
                best_distance = distance
        return best

class KMeans(object):
    @classmethod
    def train(cls, data, k, maxIterations=100, runs=1,
            initializationMode="k-means||"):
        """Train a k-means clustering model."""
        sc = data.context
        dataBytes = _get_unmangled_double_vector_rdd(data)
        ans = sc._jvm.PythonMLLibAPI().trainKMeansModel(dataBytes._jrdd,
                k, maxIterations, runs, initializationMode)
        if len(ans) != 1:
            raise RuntimeError("JVM call result had unexpected length")
        elif type(ans[0]) != bytearray:
            raise RuntimeError("JVM call result had first element of type "
                    + type(ans[0]) + " which is not bytearray")
        return KMeansModel(_deserialize_double_matrix(ans[0]))

def _test():
    import doctest
    globs = globals().copy()
    globs['sc'] = SparkContext('local[4]', 'PythonTest', batchSize=2)
    (failure_count, test_count) = doctest.testmod(globs=globs,
            optionflags=doctest.ELLIPSIS)
    globs['sc'].stop()
    if failure_count:
        exit(-1)

if __name__ == "__main__":
    _test()
