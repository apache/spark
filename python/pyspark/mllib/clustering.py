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

from numpy import array, ndarray

from pyspark import SparkContext
from pyspark.serializers import PickleSerializer, AutoBatchedSerializer
from pyspark.mllib.common import callMLlibFunc, callJavaFunc, _to_java_object_rdd
from pyspark.mllib.linalg import Vector, SparseVector, _convert_to_vector

__all__ = ['KMeansModel', 'KMeans',
           'HierarchicalClustering', 'HierarchicalClusteringModel']


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
        # cache serialized data to avoid objects over head in JVM
        jcached = _to_java_object_rdd(rdd.map(_convert_to_vector), cache=True)
        model = callMLlibFunc("trainKMeansModel", jcached, k, maxIterations, runs,
                              initializationMode)
        centers = callJavaFunc(rdd.context, model.clusterCenters)
        return KMeansModel([c.toArray() for c in centers])


class HierarchicalClusteringModel(object):

    """A clustering model derived from the hierarchical clustering method.

    >>> from numpy import array
    >>> data = array([0.0,0.0, 1.0,1.0, 9.0,8.0, 8.0,9.0]).reshape(4,2)
    >>> train_rdd = sc.parallelize(data)
    >>> model = HierarchicalClustering.train(train_rdd, 2)
    >>> model.predict(array([0.0, 0.0])) == model.predict(array([1.0, 1.0]))
    True
    >>> model.predict(array([8.0, 9.0])) == model.predict(array([9.0, 8.0]))
    True
    >>> x = model.predict(data[0])
    >>> type(x)
    <type 'int'>
    >>> predicted_rdd = model.predict(train_rdd)
    >>> type(predicted_rdd)
    <class 'pyspark.rdd.RDD'>
    >>> predicted_rdd.collect() == [0, 0, 1, 1]
    True
    >>> sparse_data = [
    ...     SparseVector(3, {1: 1.0}),
    ...     SparseVector(3, {1: 1.1}),
    ...     SparseVector(3, {2: 1.0}),
    ...     SparseVector(3, {2: 1.1})
    ... ]
    >>> train_rdd = sc.parallelize(sparse_data)
    >>> model = HierarchicalClustering.train(train_rdd, 2, numRetries=100)
    >>> model.predict(array([0., 1., 0.])) == model.predict(array([0, 1.1, 0.]))
    True
    >>> model.predict(array([0., 0., 1.])) == model.predict(array([0, 0, 1.1]))
    True
    >>> model.predict(sparse_data[0]) == model.predict(sparse_data[1])
    True
    >>> model.predict(sparse_data[2]) == model.predict(sparse_data[3])
    True
    >>> x = model.predict(array([0., 1., 0.]))
    >>> type(x)
    <type 'int'>
    >>> predicted_rdd = model.predict(train_rdd)
    >>> type(predicted_rdd)
    <class 'pyspark.rdd.RDD'>
    >>> (predicted_rdd.collect() == [0, 0, 1, 1]
    ...     or predicted_rdd.collect() == [1, 1, 0, 0] )
    True
    >>> type(model.clusterCenters)
    <type 'list'>
    """

    def __init__(self, sc, java_model, centers):
        """
        :param sc:  Spark context
        :param java_model:  Handle to Java model object
        :param centers: the cluster centers
        """
        self._sc = sc
        self._java_model = java_model
        self.centers = centers

    def __del__(self):
        self._sc._gateway.detach(self._java_model)

    @property
    def clusterCenters(self):
        """Get the cluster centers, represented as a list of NumPy arrays."""
        return self.centers

    def predict(self, x):
        """Predict the closest cluster index

        :param x: a ndarray of list, a SparseVector or RDD[SparseVector]
        :return: the closest index or a RDD of int which means the closest index
        """
        if isinstance(x, ndarray) or isinstance(x, Vector):
            return self.__predict_by_array(x)
        elif isinstance(x, RDD):
            return self.__predict_by_rdd(x)
        else:
            print 'Invalid input data type x:' + type(x)

    def __predict_by_array(self, x):
        """Predict the closest cluster index with an ndarray or an SparseVector

        :param x: a vector
        :return: the closest cluster index
        """
        ser = PickleSerializer()
        bytes = bytearray(ser.dumps(_convert_to_vector(x)))
        vec = self._sc._jvm.SerDe.loads(bytes)
        result = self._java_model.predict(vec)
        return PickleSerializer().loads(str(self._sc._jvm.SerDe.dumps(result)))

    def __predict_by_rdd(self, x):
        """Predict the closest cluster index with a RDD
        :param x: a RDD of vector
        :return: a RDD of int
        """
        ser = PickleSerializer()
        cached = x.map(_convert_to_vector)._reserialize(AutoBatchedSerializer(ser)).cache()
        rdd = _to_java_object_rdd(cached)
        jrdd = self._java_model.predict(rdd)
        jpyrdd = self._sc._jvm.SerDe.javaToPython(jrdd)
        return RDD(jpyrdd, self._sc, AutoBatchedSerializer(PickleSerializer()))

    def cut(self, height):
        """Cut nodes and leaves in a cluster tree by a dendrogram height.
        :param height: a threshold to cut a cluster tree
        """
        ser = PickleSerializer()
        model = self._java_model.cut(height)
        bytes = self._sc._jvm.SerDe.dumps(model.getCenters())
        centers = ser.loads(str(bytes))
        return HierarchicalClusteringModel(self._sc, model, [c.toArray() for c in centers])

    def sum_of_variance(self):
        """Gets the sum of variance of all clusters.
        :return: sum of variance of all clusters
        """
        ser = PickleSerializer()
        model = self._java_model
        bytes = self._sc._jvm.SerDe.dumps(model.getSumOfVariance())
        variance = ser.loads(str(bytes))
        return variance

    def to_merge_list(self):
        """Extract an array for dendrogram

        the array is fit for SciPy's dendrogram
        :return: an array which is fit for scipy's dendrogram
        """
        ser = PickleSerializer()
        model = self._java_model
        bytes = self._sc._jvm.SerDe.dumps(model.toMergeList())
        centers = ser.loads(str(bytes))
        return array([c.toArray() for c in centers])


class HierarchicalClustering(object):

    @classmethod
    def train(cls, rdd, k,
              subIterations=20, numRetries=10, epsilon=1.0e-4, randomSeed=1, randomRange=0.1):
        """Train a hierarchical clustering model."""
        sc = rdd.context
        ser = PickleSerializer()
        # cache serialized data to avoid objects over head in JVM
        cached = rdd.map(_convert_to_vector)._reserialize(AutoBatchedSerializer(ser)).cache()
        model = sc._jvm.PythonMLLibAPI().trainHierarchicalClusteringModel(
            _to_java_object_rdd(cached), k,
            subIterations, numRetries, epsilon, randomSeed, randomRange)
        bytes = sc._jvm.SerDe.dumps(model.getCenters())
        centers = ser.loads(str(bytes))
        # TODO: because centers are SparseVector, wel will change them numpy.array.
        return HierarchicalClusteringModel(sc, model, [c for c in centers])


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
