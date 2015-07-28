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

from __future__ import absolute_import

import random

from pyspark import SparkContext, RDD
from pyspark.mllib.common import callMLlibFunc, inherit_doc, JavaModelWrapper
from pyspark.mllib.linalg import _convert_to_vector
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.util import JavaLoader, JavaSaveable

__all__ = ['DecisionTreeModel', 'DecisionTree', 'RandomForestModel',
           'RandomForest', 'GradientBoostedTreesModel', 'GradientBoostedTrees']


class TreeEnsembleModel(JavaModelWrapper, JavaSaveable):
    def predict(self, x):
        """
        Predict values for a single data point or an RDD of points using
        the model trained.

        Note: In Python, predict cannot currently be used within an RDD
              transformation or action.
              Call predict directly on the RDD instead.
        """
        if isinstance(x, RDD):
            return self.call("predict", x.map(_convert_to_vector))

        else:
            return self.call("predict", _convert_to_vector(x))

    def numTrees(self):
        """
        Get number of trees in ensemble.
        """
        return self.call("numTrees")

    def totalNumNodes(self):
        """
        Get total number of nodes, summed over all trees in the
        ensemble.
        """
        return self.call("totalNumNodes")

    def __repr__(self):
        """ Summary of model """
        return self._java_model.toString()

    def toDebugString(self):
        """ Full model """
        return self._java_model.toDebugString()


class DecisionTreeModel(JavaModelWrapper, JavaSaveable, JavaLoader):
    """
    .. note:: Experimental

    A decision tree model for classification or regression.
    """
    def predict(self, x):
        """
        Predict the label of one or more examples.

        Note: In Python, predict cannot currently be used within an RDD
              transformation or action.
              Call predict directly on the RDD instead.

        :param x:  Data point (feature vector),
                   or an RDD of data points (feature vectors).
        """
        if isinstance(x, RDD):
            return self.call("predict", x.map(_convert_to_vector))

        else:
            return self.call("predict", _convert_to_vector(x))

    def numNodes(self):
        return self._java_model.numNodes()

    def depth(self):
        return self._java_model.depth()

    def __repr__(self):
        """ summary of model. """
        return self._java_model.toString()

    def toDebugString(self):
        """ full model. """
        return self._java_model.toDebugString()

    @classmethod
    def _java_loader_class(cls):
        return "org.apache.spark.mllib.tree.model.DecisionTreeModel"


class DecisionTree(object):
    """
    .. note:: Experimental

    Learning algorithm for a decision tree model for classification or
    regression.
    """

    @classmethod
    def _train(cls, data, type, numClasses, features, impurity="gini", maxDepth=5, maxBins=32,
               minInstancesPerNode=1, minInfoGain=0.0):
        first = data.first()
        assert isinstance(first, LabeledPoint), "the data should be RDD of LabeledPoint"
        model = callMLlibFunc("trainDecisionTreeModel", data, type, numClasses, features,
                              impurity, maxDepth, maxBins, minInstancesPerNode, minInfoGain)
        return DecisionTreeModel(model)

    @classmethod
    def trainClassifier(cls, data, numClasses, categoricalFeaturesInfo,
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
        :param minInstancesPerNode: Min number of instances required at child
                                    nodes to create the parent split
        :param minInfoGain: Min info gain required to create a split
        :return: DecisionTreeModel

        Example usage:

        >>> from numpy import array
        >>> from pyspark.mllib.regression import LabeledPoint
        >>> from pyspark.mllib.tree import DecisionTree
        >>>
        >>> data = [
        ...     LabeledPoint(0.0, [0.0]),
        ...     LabeledPoint(1.0, [1.0]),
        ...     LabeledPoint(1.0, [2.0]),
        ...     LabeledPoint(1.0, [3.0])
        ... ]
        >>> model = DecisionTree.trainClassifier(sc.parallelize(data), 2, {})
        >>> print(model)
        DecisionTreeModel classifier of depth 1 with 3 nodes

        >>> print(model.toDebugString())
        DecisionTreeModel classifier of depth 1 with 3 nodes
          If (feature 0 <= 0.0)
           Predict: 0.0
          Else (feature 0 > 0.0)
           Predict: 1.0
        <BLANKLINE>
        >>> model.predict(array([1.0]))
        1.0
        >>> model.predict(array([0.0]))
        0.0
        >>> rdd = sc.parallelize([[1.0], [0.0]])
        >>> model.predict(rdd).collect()
        [1.0, 0.0]
        """
        return cls._train(data, "classification", numClasses, categoricalFeaturesInfo,
                          impurity, maxDepth, maxBins, minInstancesPerNode, minInfoGain)

    @classmethod
    def trainRegressor(cls, data, categoricalFeaturesInfo,
                       impurity="variance", maxDepth=5, maxBins=32, minInstancesPerNode=1,
                       minInfoGain=0.0):
        """
        Train a DecisionTreeModel for regression.

        :param data: Training data: RDD of LabeledPoint.
                     Labels are real numbers.
        :param categoricalFeaturesInfo: Map from categorical feature
                 index to number of categories.
                 Any feature not in this map is treated as continuous.
        :param impurity: Supported values: "variance"
        :param maxDepth: Max depth of tree.
                 E.g., depth 0 means 1 leaf node.
                 Depth 1 means 1 internal node + 2 leaf nodes.
        :param maxBins: Number of bins used for finding splits at each
                 node.
        :param minInstancesPerNode: Min number of instances required at
                 child nodes to create the parent split
        :param minInfoGain: Min info gain required to create a split
        :return: DecisionTreeModel

        Example usage:

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
        >>> model.predict(SparseVector(2, {1: 1.0}))
        1.0
        >>> model.predict(SparseVector(2, {1: 0.0}))
        0.0
        >>> rdd = sc.parallelize([[0.0, 1.0], [0.0, 0.0]])
        >>> model.predict(rdd).collect()
        [1.0, 0.0]
        """
        return cls._train(data, "regression", 0, categoricalFeaturesInfo,
                          impurity, maxDepth, maxBins, minInstancesPerNode, minInfoGain)


@inherit_doc
class RandomForestModel(TreeEnsembleModel, JavaLoader):
    """
    .. note:: Experimental

    Represents a random forest model.
    """

    @classmethod
    def _java_loader_class(cls):
        return "org.apache.spark.mllib.tree.model.RandomForestModel"


class RandomForest(object):
    """
    .. note:: Experimental

    Learning algorithm for a random forest model for classification or
    regression.
    """

    supportedFeatureSubsetStrategies = ("auto", "all", "sqrt", "log2", "onethird")

    @classmethod
    def _train(cls, data, algo, numClasses, categoricalFeaturesInfo, numTrees,
               featureSubsetStrategy, impurity, maxDepth, maxBins, seed):
        first = data.first()
        assert isinstance(first, LabeledPoint), "the data should be RDD of LabeledPoint"
        if featureSubsetStrategy not in cls.supportedFeatureSubsetStrategies:
            raise ValueError("unsupported featureSubsetStrategy: %s" % featureSubsetStrategy)
        if seed is None:
            seed = random.randint(0, 1 << 30)
        model = callMLlibFunc("trainRandomForestModel", data, algo, numClasses,
                              categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity,
                              maxDepth, maxBins, seed)
        return RandomForestModel(model)

    @classmethod
    def trainClassifier(cls, data, numClasses, categoricalFeaturesInfo, numTrees,
                        featureSubsetStrategy="auto", impurity="gini", maxDepth=4, maxBins=32,
                        seed=None):
        """
        Method to train a decision tree model for binary or multiclass
        classification.

        :param data: Training dataset: RDD of LabeledPoint. Labels
                 should take values {0, 1, ..., numClasses-1}.
        :param numClasses: number of classes for classification.
        :param categoricalFeaturesInfo: Map storing arity of categorical
                 features.  E.g., an entry (n -> k) indicates that
                 feature n is categorical with k categories indexed
                 from 0: {0, 1, ..., k-1}.
        :param numTrees: Number of trees in the random forest.
        :param featureSubsetStrategy: Number of features to consider for
                 splits at each node.
                 Supported: "auto" (default), "all", "sqrt", "log2", "onethird".
                 If "auto" is set, this parameter is set based on numTrees:
                 if numTrees == 1, set to "all";
                 if numTrees > 1 (forest) set to "sqrt".
        :param impurity: Criterion used for information gain calculation.
               Supported values: "gini" (recommended) or "entropy".
        :param maxDepth: Maximum depth of the tree.
                 E.g., depth 0 means 1 leaf node; depth 1 means
                 1 internal node + 2 leaf nodes. (default: 4)
        :param maxBins: maximum number of bins used for splitting
                 features
                 (default: 32)
        :param seed: Random seed for bootstrapping and choosing feature
                 subsets.
        :return: RandomForestModel that can be used for prediction

        Example usage:

        >>> from pyspark.mllib.regression import LabeledPoint
        >>> from pyspark.mllib.tree import RandomForest
        >>>
        >>> data = [
        ...     LabeledPoint(0.0, [0.0]),
        ...     LabeledPoint(0.0, [1.0]),
        ...     LabeledPoint(1.0, [2.0]),
        ...     LabeledPoint(1.0, [3.0])
        ... ]
        >>> model = RandomForest.trainClassifier(sc.parallelize(data), 2, {}, 3, seed=42)
        >>> model.numTrees()
        3
        >>> model.totalNumNodes()
        7
        >>> print(model)
        TreeEnsembleModel classifier with 3 trees
        <BLANKLINE>
        >>> print(model.toDebugString())
        TreeEnsembleModel classifier with 3 trees
        <BLANKLINE>
          Tree 0:
            Predict: 1.0
          Tree 1:
            If (feature 0 <= 1.0)
             Predict: 0.0
            Else (feature 0 > 1.0)
             Predict: 1.0
          Tree 2:
            If (feature 0 <= 1.0)
             Predict: 0.0
            Else (feature 0 > 1.0)
             Predict: 1.0
        <BLANKLINE>
        >>> model.predict([2.0])
        1.0
        >>> model.predict([0.0])
        0.0
        >>> rdd = sc.parallelize([[3.0], [1.0]])
        >>> model.predict(rdd).collect()
        [1.0, 0.0]
        """
        return cls._train(data, "classification", numClasses,
                          categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity,
                          maxDepth, maxBins, seed)

    @classmethod
    def trainRegressor(cls, data, categoricalFeaturesInfo, numTrees, featureSubsetStrategy="auto",
                       impurity="variance", maxDepth=4, maxBins=32, seed=None):
        """
        Method to train a decision tree model for regression.

        :param data: Training dataset: RDD of LabeledPoint. Labels are
               real numbers.
        :param categoricalFeaturesInfo: Map storing arity of categorical
               features. E.g., an entry (n -> k) indicates that feature
               n is categorical with k categories indexed from 0:
               {0, 1, ..., k-1}.
        :param numTrees: Number of trees in the random forest.
        :param featureSubsetStrategy: Number of features to consider for
                 splits at each node.
                 Supported: "auto" (default), "all", "sqrt", "log2", "onethird".
                 If "auto" is set, this parameter is set based on numTrees:
                 if numTrees == 1, set to "all";
                 if numTrees > 1 (forest) set to "onethird" for regression.
        :param impurity: Criterion used for information gain
                 calculation.
                 Supported values: "variance".
        :param maxDepth: Maximum depth of the tree. E.g., depth 0 means
                 1 leaf node; depth 1 means 1 internal node + 2 leaf
                 nodes. (default: 4)
        :param maxBins: maximum number of bins used for splitting
                 features (default: 32)
        :param seed: Random seed for bootstrapping and choosing feature
                 subsets.
        :return: RandomForestModel that can be used for prediction

        Example usage:

        >>> from pyspark.mllib.regression import LabeledPoint
        >>> from pyspark.mllib.tree import RandomForest
        >>> from pyspark.mllib.linalg import SparseVector
        >>>
        >>> sparse_data = [
        ...     LabeledPoint(0.0, SparseVector(2, {0: 1.0})),
        ...     LabeledPoint(1.0, SparseVector(2, {1: 1.0})),
        ...     LabeledPoint(0.0, SparseVector(2, {0: 1.0})),
        ...     LabeledPoint(1.0, SparseVector(2, {1: 2.0}))
        ... ]
        >>>
        >>> model = RandomForest.trainRegressor(sc.parallelize(sparse_data), {}, 2, seed=42)
        >>> model.numTrees()
        2
        >>> model.totalNumNodes()
        4
        >>> model.predict(SparseVector(2, {1: 1.0}))
        1.0
        >>> model.predict(SparseVector(2, {0: 1.0}))
        0.5
        >>> rdd = sc.parallelize([[0.0, 1.0], [1.0, 0.0]])
        >>> model.predict(rdd).collect()
        [1.0, 0.5]
        """
        return cls._train(data, "regression", 0, categoricalFeaturesInfo, numTrees,
                          featureSubsetStrategy, impurity, maxDepth, maxBins, seed)


@inherit_doc
class GradientBoostedTreesModel(TreeEnsembleModel, JavaLoader):
    """
    .. note:: Experimental

    Represents a gradient-boosted tree model.
    """

    @classmethod
    def _java_loader_class(cls):
        return "org.apache.spark.mllib.tree.model.GradientBoostedTreesModel"


class GradientBoostedTrees(object):
    """
    .. note:: Experimental

    Learning algorithm for a gradient boosted trees model for
    classification or regression.
    """

    @classmethod
    def _train(cls, data, algo, categoricalFeaturesInfo,
               loss, numIterations, learningRate, maxDepth, maxBins):
        first = data.first()
        assert isinstance(first, LabeledPoint), "the data should be RDD of LabeledPoint"
        model = callMLlibFunc("trainGradientBoostedTreesModel", data, algo, categoricalFeaturesInfo,
                              loss, numIterations, learningRate, maxDepth, maxBins)
        return GradientBoostedTreesModel(model)

    @classmethod
    def trainClassifier(cls, data, categoricalFeaturesInfo,
                        loss="logLoss", numIterations=100, learningRate=0.1, maxDepth=3,
                        maxBins=32):
        """
        Method to train a gradient-boosted trees model for
        classification.

        :param data: Training dataset: RDD of LabeledPoint.
                 Labels should take values {0, 1}.
        :param categoricalFeaturesInfo: Map storing arity of categorical
               features. E.g., an entry (n -> k) indicates that feature
               n is categorical with k categories indexed from 0:
               {0, 1, ..., k-1}.
        :param loss: Loss function used for minimization during gradient
                 boosting. Supported: {"logLoss" (default),
                 "leastSquaresError", "leastAbsoluteError"}.
        :param numIterations: Number of iterations of boosting.
                              (default: 100)
        :param learningRate: Learning rate for shrinking the
                 contribution of each estimator. The learning rate
                 should be between in the interval (0, 1].
                 (default: 0.1)
        :param maxDepth: Maximum depth of the tree. E.g., depth 0 means
                 1 leaf node; depth 1 means 1 internal node + 2 leaf
                 nodes. (default: 3)
        :param maxBins: maximum number of bins used for splitting
                 features (default: 32) DecisionTree requires maxBins >= max categories
        :return: GradientBoostedTreesModel that can be used for
                   prediction

        Example usage:

        >>> from pyspark.mllib.regression import LabeledPoint
        >>> from pyspark.mllib.tree import GradientBoostedTrees
        >>>
        >>> data = [
        ...     LabeledPoint(0.0, [0.0]),
        ...     LabeledPoint(0.0, [1.0]),
        ...     LabeledPoint(1.0, [2.0]),
        ...     LabeledPoint(1.0, [3.0])
        ... ]
        >>>
        >>> model = GradientBoostedTrees.trainClassifier(sc.parallelize(data), {}, numIterations=10)
        >>> model.numTrees()
        10
        >>> model.totalNumNodes()
        30
        >>> print(model)  # it already has newline
        TreeEnsembleModel classifier with 10 trees
        <BLANKLINE>
        >>> model.predict([2.0])
        1.0
        >>> model.predict([0.0])
        0.0
        >>> rdd = sc.parallelize([[2.0], [0.0]])
        >>> model.predict(rdd).collect()
        [1.0, 0.0]
        """
        return cls._train(data, "classification", categoricalFeaturesInfo,
                          loss, numIterations, learningRate, maxDepth, maxBins)

    @classmethod
    def trainRegressor(cls, data, categoricalFeaturesInfo,
                       loss="leastSquaresError", numIterations=100, learningRate=0.1, maxDepth=3,
                       maxBins=32):
        """
        Method to train a gradient-boosted trees model for regression.

        :param data: Training dataset: RDD of LabeledPoint. Labels are
               real numbers.
        :param categoricalFeaturesInfo: Map storing arity of categorical
               features. E.g., an entry (n -> k) indicates that feature
               n is categorical with k categories indexed from 0:
               {0, 1, ..., k-1}.
        :param loss: Loss function used for minimization during gradient
                 boosting. Supported: {"logLoss" (default),
                 "leastSquaresError", "leastAbsoluteError"}.
        :param numIterations: Number of iterations of boosting.
                              (default: 100)
        :param learningRate: Learning rate for shrinking the
                 contribution of each estimator. The learning rate
                 should be between in the interval (0, 1].
                 (default: 0.1)
        :param maxBins: maximum number of bins used for splitting
                 features (default: 32) DecisionTree requires maxBins >= max categories
        :param maxDepth: Maximum depth of the tree. E.g., depth 0 means
                 1 leaf node; depth 1 means 1 internal node + 2 leaf
                 nodes.  (default: 3)
        :return: GradientBoostedTreesModel that can be used for
                   prediction

        Example usage:

        >>> from pyspark.mllib.regression import LabeledPoint
        >>> from pyspark.mllib.tree import GradientBoostedTrees
        >>> from pyspark.mllib.linalg import SparseVector
        >>>
        >>> sparse_data = [
        ...     LabeledPoint(0.0, SparseVector(2, {0: 1.0})),
        ...     LabeledPoint(1.0, SparseVector(2, {1: 1.0})),
        ...     LabeledPoint(0.0, SparseVector(2, {0: 1.0})),
        ...     LabeledPoint(1.0, SparseVector(2, {1: 2.0}))
        ... ]
        >>>
        >>> data = sc.parallelize(sparse_data)
        >>> model = GradientBoostedTrees.trainRegressor(data, {}, numIterations=10)
        >>> model.numTrees()
        10
        >>> model.totalNumNodes()
        12
        >>> model.predict(SparseVector(2, {1: 1.0}))
        1.0
        >>> model.predict(SparseVector(2, {0: 1.0}))
        0.0
        >>> rdd = sc.parallelize([[0.0, 1.0], [1.0, 0.0]])
        >>> model.predict(rdd).collect()
        [1.0, 0.0]
        """
        return cls._train(data, "regression", categoricalFeaturesInfo,
                          loss, numIterations, learningRate, maxDepth, maxBins)


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
