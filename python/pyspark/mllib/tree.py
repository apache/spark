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

from py4j.java_collections import MapConverter

from pyspark import SparkContext, RDD
from pyspark.serializers import BatchedSerializer, PickleSerializer
from pyspark.mllib.linalg import Vector, _convert_to_vector
from pyspark.mllib.regression import LabeledPoint

__all__ = ['DecisionTreeModel', 'DecisionTree']


class DecisionTreeModel(object):

    """
    A decision tree model for classification or regression.

    EXPERIMENTAL: This is an experimental API.
                  It will probably be modified for Spark v1.2.
    """

    def __init__(self, sc, java_model):
        """
        :param sc:  Spark context
        :param java_model:  Handle to Java model object
        """
        self._sc = sc
        self._java_model = java_model

    def __del__(self):
        self._sc._gateway.detach(self._java_model)

    def predict(self, x):
        """
        Predict the label of one or more examples.
        :param x:  Data point (feature vector),
                   or an RDD of data points (feature vectors).
        """
        SerDe = self._sc._jvm.SerDe
        ser = PickleSerializer()
        if isinstance(x, RDD):
            # Bulk prediction
            first = x.take(1)
            if not first:
                return self._sc.parallelize([])
            if not isinstance(first[0], Vector):
                x = x.map(_convert_to_vector)
            jPred = self._java_model.predict(x._to_java_object_rdd()).toJavaRDD()
            jpyrdd = self._sc._jvm.PythonRDD.javaToPython(jPred)
            return RDD(jpyrdd, self._sc, BatchedSerializer(ser, 1024))

        else:
            # Assume x is a single data point.
            bytes = bytearray(ser.dumps(_convert_to_vector(x)))
            vec = self._sc._jvm.SerDe.loads(bytes)
            return self._java_model.predict(vec)

    def numNodes(self):
        return self._java_model.numNodes()

    def depth(self):
        return self._java_model.depth()

    def __repr__(self):
        return self._java_model.toString()


class DecisionTree(object):

    """
    Learning algorithm for a decision tree model
    for classification or regression.

    EXPERIMENTAL: This is an experimental API.
                  It will probably be modified for Spark v1.2.

    """

    @staticmethod
    def _train(data, type, numClasses, categoricalFeaturesInfo,
               impurity="gini", maxDepth=5, maxBins=32, minInstancesPerNode=1,
               minInfoGain=0.0):
        first = data.first()
        assert isinstance(first, LabeledPoint), "the data should be RDD of LabeledPoint"
        sc = data.context
        jrdd = data._to_java_object_rdd()
        cfiMap = MapConverter().convert(categoricalFeaturesInfo,
                                        sc._gateway._gateway_client)
        model = sc._jvm.PythonMLLibAPI().trainDecisionTreeModel(
            jrdd, type, numClasses, cfiMap,
            impurity, maxDepth, maxBins, minInstancesPerNode, minInfoGain)
        return DecisionTreeModel(sc, model)

    @staticmethod
    def trainClassifier(data, numClasses, categoricalFeaturesInfo,
                        impurity="gini", maxDepth=5, maxBins=32, minInstancesPerNode=1,
                        minInfoGain=0.0):
        """
        Train a DecisionTreeModel for classification.

        :param data: Training data: RDD of LabeledPoint.
                     Labels are integers {0,1,...,numClasses}.
        :param numClasses: Number of classes for classification.
        :param categoricalFeaturesInfo: Map from categorical feature index
                                        to number of categories.
                                        Any feature not in this map
                                        is treated as continuous.
        :param impurity: Supported values: "entropy" or "gini"
        :param maxDepth: Max depth of tree.
                         E.g., depth 0 means 1 leaf node.
                         Depth 1 means 1 internal node + 2 leaf nodes.
        :param maxBins: Number of bins used for finding splits at each node.
        :param minInstancesPerNode: Min number of instances required at child nodes to create
                                    the parent split
        :param minInfoGain: Min info gain required to create a split
        :return: DecisionTreeModel

        Example usage:

        >>> from numpy import array
        >>> from pyspark.mllib.regression import LabeledPoint
        >>> from pyspark.mllib.tree import DecisionTree
        >>> from pyspark.mllib.linalg import SparseVector
        >>>
        >>> data = [
        ...     LabeledPoint(0.0, [0.0]),
        ...     LabeledPoint(1.0, [1.0]),
        ...     LabeledPoint(1.0, [2.0]),
        ...     LabeledPoint(1.0, [3.0])
        ... ]
        >>> model = DecisionTree.trainClassifier(sc.parallelize(data), 2, {})
        >>> print model,  # it already has newline
        DecisionTreeModel classifier
          If (feature 0 <= 0.5)
           Predict: 0.0
          Else (feature 0 > 0.5)
           Predict: 1.0
        >>> model.predict(array([1.0])) > 0
        True
        >>> model.predict(array([0.0])) == 0
        True
        """
        return DecisionTree._train(data, "classification", numClasses, categoricalFeaturesInfo,
                                   impurity, maxDepth, maxBins, minInstancesPerNode, minInfoGain)

    @staticmethod
    def trainRegressor(data, categoricalFeaturesInfo,
                       impurity="variance", maxDepth=5, maxBins=32, minInstancesPerNode=1,
                       minInfoGain=0.0):
        """
        Train a DecisionTreeModel for regression.

        :param data: Training data: RDD of LabeledPoint.
                     Labels are real numbers.
        :param categoricalFeaturesInfo: Map from categorical feature index
                                        to number of categories.
                                        Any feature not in this map
                                        is treated as continuous.
        :param impurity: Supported values: "variance"
        :param maxDepth: Max depth of tree.
                         E.g., depth 0 means 1 leaf node.
                         Depth 1 means 1 internal node + 2 leaf nodes.
        :param maxBins: Number of bins used for finding splits at each node.
        :param minInstancesPerNode: Min number of instances required at child nodes to create
                                    the parent split
        :param minInfoGain: Min info gain required to create a split
        :return: DecisionTreeModel

        Example usage:

        >>> from numpy import array
        >>> from pyspark.mllib.regression import LabeledPoint
        >>> from pyspark.mllib.tree import DecisionTree
        >>> from pyspark.mllib.linalg import SparseVector
        >>>
        >>> sparse_data = [
        ...     LabeledPoint(0.0, SparseVector(2, {0: 0.0})),
        ...     LabeledPoint(1.0, SparseVector(2, {1: 1.0})),
        ...     LabeledPoint(0.0, SparseVector(2, {0: 0.0})),
        ...     LabeledPoint(1.0, SparseVector(2, {1: 2.0}))
        ... ]
        >>>
        >>> model = DecisionTree.trainRegressor(sc.parallelize(sparse_data), {})
        >>> model.predict(array([0.0, 1.0])) == 1
        True
        >>> model.predict(array([0.0, 0.0])) == 0
        True
        >>> model.predict(SparseVector(2, {1: 1.0})) == 1
        True
        >>> model.predict(SparseVector(2, {1: 0.0})) == 0
        True
        """
        return DecisionTree._train(data, "regression", 0, categoricalFeaturesInfo,
                                   impurity, maxDepth, maxBins, minInstancesPerNode, minInfoGain)


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
