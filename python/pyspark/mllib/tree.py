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
from pyspark.mllib._common import \
    _get_unmangled_rdd, _get_unmangled_double_vector_rdd, _serialize_double_vector, \
    _deserialize_labeled_point, _get_unmangled_labeled_point_rdd, \
    _deserialize_double
from pyspark.mllib.regression import LabeledPoint
from pyspark.serializers import NoOpSerializer


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
        pythonAPI = self._sc._jvm.PythonMLLibAPI()
        if isinstance(x, RDD):
            # Bulk prediction
            if x.count() == 0:
                return self._sc.parallelize([])
            dataBytes = _get_unmangled_double_vector_rdd(x, cache=False)
            jSerializedPreds = \
                pythonAPI.predictDecisionTreeModel(self._java_model,
                                                   dataBytes._jrdd)
            serializedPreds = RDD(jSerializedPreds, self._sc, NoOpSerializer())
            return serializedPreds.map(lambda bytes: _deserialize_double(bytearray(bytes)))
        else:
            # Assume x is a single data point.
            x_ = _serialize_double_vector(x)
            return pythonAPI.predictDecisionTreeModel(self._java_model, x_)

    def numNodes(self):
        return self._java_model.numNodes()

    def depth(self):
        return self._java_model.depth()

    def __str__(self):
        return self._java_model.toString()


class DecisionTree(object):

    """
    Learning algorithm for a decision tree model
    for classification or regression.

    EXPERIMENTAL: This is an experimental API.
                  It will probably be modified for Spark v1.2.

    Example usage:

    >>> from numpy import array
    >>> import sys
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
    >>> categoricalFeaturesInfo = {} # no categorical features
    >>> model = DecisionTree.trainClassifier(sc.parallelize(data), numClasses=2,
    ...                                      categoricalFeaturesInfo=categoricalFeaturesInfo)
    >>> sys.stdout.write(model)
    DecisionTreeModel classifier
      If (feature 0 <= 0.5)
       Predict: 0.0
      Else (feature 0 > 0.5)
       Predict: 1.0
    >>> model.predict(array([1.0])) > 0
    True
    >>> model.predict(array([0.0])) == 0
    True
    >>> sparse_data = [
    ...     LabeledPoint(0.0, SparseVector(2, {0: 0.0})),
    ...     LabeledPoint(1.0, SparseVector(2, {1: 1.0})),
    ...     LabeledPoint(0.0, SparseVector(2, {0: 0.0})),
    ...     LabeledPoint(1.0, SparseVector(2, {1: 2.0}))
    ... ]
    >>>
    >>> model = DecisionTree.trainRegressor(sc.parallelize(sparse_data),
    ...                                     categoricalFeaturesInfo=categoricalFeaturesInfo)
    >>> model.predict(array([0.0, 1.0])) == 1
    True
    >>> model.predict(array([0.0, 0.0])) == 0
    True
    >>> model.predict(SparseVector(2, {1: 1.0})) == 1
    True
    >>> model.predict(SparseVector(2, {1: 0.0})) == 0
    True
    """

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
        """
        sc = data.context
        dataBytes = _get_unmangled_labeled_point_rdd(data)
        categoricalFeaturesInfoJMap = \
            MapConverter().convert(categoricalFeaturesInfo,
                                   sc._gateway._gateway_client)
        model = sc._jvm.PythonMLLibAPI().trainDecisionTreeModel(
            dataBytes._jrdd, "classification",
            numClasses, categoricalFeaturesInfoJMap,
            impurity, maxDepth, maxBins, minInstancesPerNode, minInfoGain)
        dataBytes.unpersist()
        return DecisionTreeModel(sc, model)

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
        """
        sc = data.context
        dataBytes = _get_unmangled_labeled_point_rdd(data)
        categoricalFeaturesInfoJMap = \
            MapConverter().convert(categoricalFeaturesInfo,
                                   sc._gateway._gateway_client)
        model = sc._jvm.PythonMLLibAPI().trainDecisionTreeModel(
            dataBytes._jrdd, "regression",
            0, categoricalFeaturesInfoJMap,
            impurity, maxDepth, maxBins, minInstancesPerNode, minInfoGain)
        dataBytes.unpersist()
        return DecisionTreeModel(sc, model)


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
