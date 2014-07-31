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
    _convert_vector, \
    _dot, _get_unmangled_rdd, _get_unmangled_double_vector_rdd, \
    _serialize_double_matrix, _deserialize_double_matrix, \
    _serialize_double_vector, _deserialize_double_vector, \
    _deserialize_labeled_point, \
    _get_initial_weights, _serialize_rating, _regression_train_wrapper, \
    _linear_predictor_typecheck, _get_unmangled_labeled_point_rdd
from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.regression import LabeledPoint
from pyspark.serializers import NoOpSerializer

class DecisionTreeModel(object):
    """
    A decision tree model for classification or regression.

    WARNING: This is an experimental API.  It will probably be modified for Spark v1.2.
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
        :param x:  Either one data point (feature vector), or a dataset (RDD of feature vectors)
        """
        pythonAPI = self._sc._jvm.PythonMLLibAPI()
        if type(x) == RDD:
            # Bulk prediction
            dataBytes = _get_unmangled_double_vector_rdd(x)
            jSerializedPreds = pythonAPI.predictDecisionTreeModel(self._java_model, dataBytes._jrdd)
            serializedPreds = RDD(jSerializedPreds, self._sc, NoOpSerializer())
            return serializedPreds.map(lambda bytes: _deserialize_labeled_point(bytearray(bytes)))
        else:
            if type(x) == LabeledPoint:
                x_ = _serialize_double_vector(x.features)
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
    Learning algorithm for a decision tree model for classification or regression.

    WARNING: This is an experimental API.  It will probably be modified for Spark v1.2.
    """

    def run(self, data, datasetInfo):
        """
        :param data: RDD of NumPy vectors, one per element, where the first
                     coordinate is the label and the rest is the feature vector.
                     Labels are integers {0,1,...,numClasses}.
        :param datasetInfo: Dataset metadata
        :return: DecisionTreeClassifierModel
        """

    @staticmethod
    def trainClassifier(data, numClasses, categoricalFeaturesInfo={},
                        impurity="gini", maxDepth=4, maxBins=100):
        """
        Train a DecisionTreeModel for classification.

        :param data: RDD of NumPy vectors, one per element, where the first
                     coordinate is the label and the rest is the feature vector.
                     Labels are integers {0,1,...,numClasses}.
        :param numClasses: Number of classes for classification.
        :param categoricalFeaturesInfo: Map from categorical feature index to number of categories.
                                        Any feature not in this map is treated as continuous.
        :param impurity: Supported values: "entropy" or "gini"
        :param maxDepth: Max depth of tree.
                         E.g., depth 0 means 1 leaf node.
                         Depth 1 means 1 internal node + 2 leaf nodes.
        :param maxBins: Number of bins used for finding splits at each node.
        :return: DecisionTreeModel
        """
        return DecisionTree.train(data, "classification", numClasses, categoricalFeaturesInfo,
                                  impurity, maxDepth, maxBins)

    @staticmethod
    def trainRegressor(data, categoricalFeaturesInfo={},
                       impurity="variance", maxDepth=4, maxBins=100):
        """
        Train a DecisionTreeModel for regression.

        :param data: RDD of NumPy vectors, one per element, where the first
                     coordinate is the label and the rest is the feature vector.
                     Labels are real numbers.
        :param categoricalFeaturesInfo: Map from categorical feature index to number of categories.
                                        Any feature not in this map is treated as continuous.
        :param impurity: Supported values: "variance"
        :param maxDepth: Max depth of tree.
                         E.g., depth 0 means 1 leaf node.
                         Depth 1 means 1 internal node + 2 leaf nodes.
        :param maxBins: Number of bins used for finding splits at each node.
        :return: DecisionTreeModel
        """
        return DecisionTree.train(data, "regression", 0, categoricalFeaturesInfo,
                                  impurity, maxDepth, maxBins)


    @staticmethod
    def train(data, algo, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins=100):
        """
        Train a DecisionTreeModel for classification or regression.

        :param data: RDD of NumPy vectors, one per element, where the first
                     coordinate is the label and the rest is the feature vector.
                     For classification, labels are integers {0,1,...,numClasses}.
                     For regression, labels are real numbers.
        :param algo: "classification" or "regression"
        :param numClasses: Number of classes for classification.  0 or 1 indicates regression.
        :param categoricalFeaturesInfo: Map from categorical feature index to number of categories.
                                        Any feature not in this map is treated as continuous.
        :param impurity: For classification: "entropy" or "gini".  For regression: "variance".
        :param maxDepth: Max depth of tree.
                         E.g., depth 0 means 1 leaf node.
                         Depth 1 means 1 internal node + 2 leaf nodes.
        :param maxBins: Number of bins used for finding splits at each node.
        :return: DecisionTreeModel
        """
        sc = data.context
        dataBytes = _get_unmangled_labeled_point_rdd(data)
        categoricalFeaturesInfoJMap = \
            MapConverter().convert(categoricalFeaturesInfo, sc._gateway._gateway_client)
        model = sc._jvm.PythonMLLibAPI().trainDecisionTreeModel(
            dataBytes._jrdd, algo,
            numClasses, categoricalFeaturesInfoJMap,
            impurity, maxDepth, maxBins)
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
