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
from pyspark.serializers import PickleSerializer, AutoBatchedSerializer
from pyspark.mllib.linalg import SparseVector, _convert_to_vector, _to_java_object_rdd

__all__ = ['KMeansModel', 'KMeans']


class KMeansModel(object):

    """A clustering model derived from the k-means method.

    >>> from numpy import array
    >>> data = array([0.0,0.0, 1.0,1.0, 9.0,8.0, 8.0,9.0]).reshape(4,2)
    >>> model = KMeans.train(
    ...     sc.parallelize(data), 2, maxIterations=10, runs=30, initializationMode="random")
    >>> model.predict(array([0.0, 0.0])) == model.predict(array([1.0, 1.0]))
    True
    >>> model.predict(array([8.0, 9.0])) == model.predict(array([9.0, 8.0]))
    True
    >>> model = KMeans.train(sc.parallelize(data), 2)
    >>> sparse_data = [
    ...     SparseVector(3, {1: 1.0}),
    ...     SparseVector(3, {1: 1.1}),
    ...     SparseVector(3, {2: 1.0}),
    ...     SparseVector(3, {2: 1.1})
    ... ]
    >>> model = KMeans.train(sc.parallelize(sparse_data), 2, initializationMode="k-means||")
    >>> model.predict(array([0., 1., 0.])) == model.predict(array([0, 1.1, 0.]))
    True
    >>> model.predict(array([0., 0., 1.])) == model.predict(array([0, 0, 1.1]))
    True
    >>> model.predict(sparse_data[0]) == model.predict(sparse_data[1])
    True
    >>> model.predict(sparse_data[2]) == model.predict(sparse_data[3])
    True
    >>> type(model.clusterCenters)
    <type 'list'>
    """

    def __init__(self, centers):
        self.centers = centers

    @property
    def clusterCenters(self):
        """Get the cluster centers, represented as a list of NumPy arrays."""
        return self.centers

    def predict(self, x):
        """Find the cluster to which x belongs in this model."""
        best = 0
        best_distance = float("inf")
        x = _convert_to_vector(x)
        for i in xrange(len(self.centers)):
            distance = x.squared_distance(self.centers[i])
            if distance < best_distance:
                best = i
                best_distance = distance
        return best


class KMeans(object):

    @classmethod
    def train(cls, rdd, k, maxIterations=100, runs=1, initializationMode="k-means||"):
        """Train a k-means clustering model."""
        sc = rdd.context
        ser = PickleSerializer()
        # cache serialized data to avoid objects over head in JVM
        cached = rdd.map(_convert_to_vector)._reserialize(AutoBatchedSerializer(ser)).cache()
        model = sc._jvm.PythonMLLibAPI().trainKMeansModel(
            _to_java_object_rdd(cached), k, maxIterations, runs, initializationMode)
        bytes = sc._jvm.SerDe.dumps(model.clusterCenters())
        centers = ser.loads(str(bytes))
        return KMeansModel([c.toArray() for c in centers])


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
