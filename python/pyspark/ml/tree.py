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

from pyspark import since
from pyspark.ml.param import Params
from pyspark.ml.param.shared import HasCheckpointInterval, HasSeed, HasWeightCol, Param, \
    TypeConverters, HasMaxIter, HasStepSize, HasValidationIndicatorCol
from pyspark.ml.wrapper import JavaPredictionModel
from pyspark.ml.common import inherit_doc


@inherit_doc
class _DecisionTreeModel(JavaPredictionModel):
    """
    Abstraction for Decision Tree models.

    .. versionadded:: 1.5.0
    """

    @property
    @since("1.5.0")
    def numNodes(self):
        """Return number of nodes of the decision tree."""
        return self._call_java("numNodes")

    @property
    @since("1.5.0")
    def depth(self):
        """Return depth of the decision tree."""
        return self._call_java("depth")

    @property
    @since("2.0.0")
    def toDebugString(self):
        """Full description of model."""
        return self._call_java("toDebugString")

    @since("3.0.0")
    def predictLeaf(self, value):
        """
        Predict the indices of the leaves corresponding to the feature vector.
        """
        return self._call_java("predictLeaf", value)


class _DecisionTreeParams(HasCheckpointInterval, HasSeed, HasWeightCol):
    """
    Mixin for Decision Tree parameters.
    """

    leafCol = Param(Params._dummy(), "leafCol", "Leaf indices column name. Predicted leaf " +
                    "index of each instance in each tree by preorder.",
                    typeConverter=TypeConverters.toString)

    maxDepth = Param(Params._dummy(), "maxDepth", "Maximum depth of the tree. (>= 0) E.g., " +
                     "depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.",
                     typeConverter=TypeConverters.toInt)

    maxBins = Param(Params._dummy(), "maxBins", "Max number of bins for discretizing continuous " +
                    "features.  Must be >=2 and >= number of categories for any categorical " +
                    "feature.", typeConverter=TypeConverters.toInt)

    minInstancesPerNode = Param(Params._dummy(), "minInstancesPerNode", "Minimum number of " +
                                "instances each child must have after split. If a split causes " +
                                "the left or right child to have fewer than " +
                                "minInstancesPerNode, the split will be discarded as invalid. " +
                                "Should be >= 1.", typeConverter=TypeConverters.toInt)

    minWeightFractionPerNode = Param(Params._dummy(), "minWeightFractionPerNode", "Minimum "
                                     "fraction of the weighted sample count that each child "
                                     "must have after split. If a split causes the fraction "
                                     "of the total weight in the left or right child to be "
                                     "less than minWeightFractionPerNode, the split will be "
                                     "discarded as invalid. Should be in interval [0.0, 0.5).",
                                     typeConverter=TypeConverters.toFloat)

    minInfoGain = Param(Params._dummy(), "minInfoGain", "Minimum information gain for a split " +
                        "to be considered at a tree node.", typeConverter=TypeConverters.toFloat)

    maxMemoryInMB = Param(Params._dummy(), "maxMemoryInMB", "Maximum memory in MB allocated to " +
                          "histogram aggregation. If too small, then 1 node will be split per " +
                          "iteration, and its aggregates may exceed this size.",
                          typeConverter=TypeConverters.toInt)

    cacheNodeIds = Param(Params._dummy(), "cacheNodeIds", "If false, the algorithm will pass " +
                         "trees to executors to match instances with nodes. If true, the " +
                         "algorithm will cache node IDs for each instance. Caching can speed " +
                         "up training of deeper trees. Users can set how often should the cache " +
                         "be checkpointed or disable it by setting checkpointInterval.",
                         typeConverter=TypeConverters.toBoolean)

    def __init__(self):
        super(_DecisionTreeParams, self).__init__()

    def setLeafCol(self, value):
        """
        Sets the value of :py:attr:`leafCol`.
        """
        return self._set(leafCol=value)

    def getLeafCol(self):
        """
        Gets the value of leafCol or its default value.
        """
        return self.getOrDefault(self.leafCol)

    def getMaxDepth(self):
        """
        Gets the value of maxDepth or its default value.
        """
        return self.getOrDefault(self.maxDepth)

    def getMaxBins(self):
        """
        Gets the value of maxBins or its default value.
        """
        return self.getOrDefault(self.maxBins)

    def getMinInstancesPerNode(self):
        """
        Gets the value of minInstancesPerNode or its default value.
        """
        return self.getOrDefault(self.minInstancesPerNode)

    def getMinWeightFractionPerNode(self):
        """
        Gets the value of minWeightFractionPerNode or its default value.
        """
        return self.getOrDefault(self.minWeightFractionPerNode)

    def getMinInfoGain(self):
        """
        Gets the value of minInfoGain or its default value.
        """
        return self.getOrDefault(self.minInfoGain)

    def getMaxMemoryInMB(self):
        """
        Gets the value of maxMemoryInMB or its default value.
        """
        return self.getOrDefault(self.maxMemoryInMB)

    def getCacheNodeIds(self):
        """
        Gets the value of cacheNodeIds or its default value.
        """
        return self.getOrDefault(self.cacheNodeIds)


@inherit_doc
class _TreeEnsembleModel(JavaPredictionModel):
    """
    (private abstraction)
    Represents a tree ensemble model.
    """

    @property
    @since("2.0.0")
    def trees(self):
        """Trees in this ensemble. Warning: These have null parent Estimators."""
        return [_DecisionTreeModel(m) for m in list(self._call_java("trees"))]

    @property
    @since("2.0.0")
    def getNumTrees(self):
        """Number of trees in ensemble."""
        return self._call_java("getNumTrees")

    @property
    @since("1.5.0")
    def treeWeights(self):
        """Return the weights for each tree"""
        return list(self._call_java("javaTreeWeights"))

    @property
    @since("2.0.0")
    def totalNumNodes(self):
        """Total number of nodes, summed over all trees in the ensemble."""
        return self._call_java("totalNumNodes")

    @property
    @since("2.0.0")
    def toDebugString(self):
        """Full description of model."""
        return self._call_java("toDebugString")

    @since("3.0.0")
    def predictLeaf(self, value):
        """
        Predict the indices of the leaves corresponding to the feature vector.
        """
        return self._call_java("predictLeaf", value)


class _TreeEnsembleParams(_DecisionTreeParams):
    """
    Mixin for Decision Tree-based ensemble algorithms parameters.
    """

    subsamplingRate = Param(Params._dummy(), "subsamplingRate", "Fraction of the training data " +
                            "used for learning each decision tree, in range (0, 1].",
                            typeConverter=TypeConverters.toFloat)

    supportedFeatureSubsetStrategies = ["auto", "all", "onethird", "sqrt", "log2"]

    featureSubsetStrategy = \
        Param(Params._dummy(), "featureSubsetStrategy",
              "The number of features to consider for splits at each tree node. Supported " +
              "options: 'auto' (choose automatically for task: If numTrees == 1, set to " +
              "'all'. If numTrees > 1 (forest), set to 'sqrt' for classification and to " +
              "'onethird' for regression), 'all' (use all features), 'onethird' (use " +
              "1/3 of the features), 'sqrt' (use sqrt(number of features)), 'log2' (use " +
              "log2(number of features)), 'n' (when n is in the range (0, 1.0], use " +
              "n * number of features. When n is in the range (1, number of features), use" +
              " n features). default = 'auto'", typeConverter=TypeConverters.toString)

    def __init__(self):
        super(_TreeEnsembleParams, self).__init__()

    @since("1.4.0")
    def getSubsamplingRate(self):
        """
        Gets the value of subsamplingRate or its default value.
        """
        return self.getOrDefault(self.subsamplingRate)

    @since("1.4.0")
    def getFeatureSubsetStrategy(self):
        """
        Gets the value of featureSubsetStrategy or its default value.
        """
        return self.getOrDefault(self.featureSubsetStrategy)


class _RandomForestParams(_TreeEnsembleParams):
    """
    Private class to track supported random forest parameters.
    """

    numTrees = Param(Params._dummy(), "numTrees", "Number of trees to train (>= 1).",
                     typeConverter=TypeConverters.toInt)

    bootstrap = Param(Params._dummy(), "bootstrap", "Whether bootstrap samples are used "
                      "when building trees.", typeConverter=TypeConverters.toBoolean)

    def __init__(self):
        super(_RandomForestParams, self).__init__()

    @since("1.4.0")
    def getNumTrees(self):
        """
        Gets the value of numTrees or its default value.
        """
        return self.getOrDefault(self.numTrees)

    @since("3.0.0")
    def getBootstrap(self):
        """
        Gets the value of bootstrap or its default value.
        """
        return self.getOrDefault(self.bootstrap)


class _GBTParams(_TreeEnsembleParams, HasMaxIter, HasStepSize, HasValidationIndicatorCol):
    """
    Private class to track supported GBT params.
    """

    stepSize = Param(Params._dummy(), "stepSize",
                     "Step size (a.k.a. learning rate) in interval (0, 1] for shrinking " +
                     "the contribution of each estimator.",
                     typeConverter=TypeConverters.toFloat)

    validationTol = Param(Params._dummy(), "validationTol",
                          "Threshold for stopping early when fit with validation is used. " +
                          "If the error rate on the validation input changes by less than the " +
                          "validationTol, then learning will stop early (before `maxIter`). " +
                          "This parameter is ignored when fit without validation is used.",
                          typeConverter=TypeConverters.toFloat)

    @since("3.0.0")
    def getValidationTol(self):
        """
        Gets the value of validationTol or its default value.
        """
        return self.getOrDefault(self.validationTol)


class _HasVarianceImpurity(Params):
    """
    Private class to track supported impurity measures.
    """

    supportedImpurities = ["variance"]

    impurity = Param(Params._dummy(), "impurity",
                     "Criterion used for information gain calculation (case-insensitive). " +
                     "Supported options: " +
                     ", ".join(supportedImpurities), typeConverter=TypeConverters.toString)

    def __init__(self):
        super(_HasVarianceImpurity, self).__init__()

    @since("1.4.0")
    def getImpurity(self):
        """
        Gets the value of impurity or its default value.
        """
        return self.getOrDefault(self.impurity)


class _TreeClassifierParams(Params):
    """
    Private class to track supported impurity measures.

    .. versionadded:: 1.4.0
    """

    supportedImpurities = ["entropy", "gini"]

    impurity = Param(Params._dummy(), "impurity",
                     "Criterion used for information gain calculation (case-insensitive). " +
                     "Supported options: " +
                     ", ".join(supportedImpurities), typeConverter=TypeConverters.toString)

    def __init__(self):
        super(_TreeClassifierParams, self).__init__()

    @since("1.6.0")
    def getImpurity(self):
        """
        Gets the value of impurity or its default value.
        """
        return self.getOrDefault(self.impurity)


class _TreeRegressorParams(_HasVarianceImpurity):
    """
    Private class to track supported impurity measures.
    """
    pass
