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

import sys
import random

from pyspark import RDD, since
from pyspark.mllib.common import callMLlibFunc, inherit_doc, JavaModelWrapper
from pyspark.mllib.linalg import _convert_to_vector
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.util import JavaLoader, JavaSaveable
from typing import Dict, Optional, Tuple, Union, overload, TYPE_CHECKING
from pyspark.rdd import RDD

if TYPE_CHECKING:
    from pyspark.mllib._typing import VectorLike


__all__ = [
    "DecisionTreeModel",
    "DecisionTree",
    "RandomForestModel",
    "RandomForest",
    "GradientBoostedTreesModel",
    "GradientBoostedTrees",
]


class TreeEnsembleModel(JavaModelWrapper, JavaSaveable):
    """TreeEnsembleModel

    .. versionadded:: 1.3.0
    """

    @overload
    def predict(self, x: "VectorLike") -> float:
        ...

    @overload
    def predict(self, x: RDD["VectorLike"]) -> RDD[float]:
        ...

    def predict(self, x: Union["VectorLike", RDD["VectorLike"]]) -> Union[float, RDD[float]]:
        """
        Predict values for a single data point or an RDD of points using
        the model trained.

        .. versionadded:: 1.3.0

        Notes
        -----
        In Python, predict cannot currently be used within an RDD
        transformation or action.
        Call predict directly on the RDD instead.
        """
        if isinstance(x, RDD):
            return self.call("predict", x.map(_convert_to_vector))

        else:
            return self.call("predict", _convert_to_vector(x))

    @since("1.3.0")
    def numTrees(self) -> int:
        """
        Get number of trees in ensemble.
        """
        return self.call("numTrees")

    @since("1.3.0")
    def totalNumNodes(self) -> int:
        """
        Get total number of nodes, summed over all trees in the ensemble.
        """
        return self.call("totalNumNodes")

    def __repr__(self) -> str:
        """Summary of model"""
        return self._java_model.toString()

    @since("1.3.0")
    def toDebugString(self) -> str:
        """Full model"""
        return self._java_model.toDebugString()


class DecisionTreeModel(JavaModelWrapper, JavaSaveable, JavaLoader["DecisionTreeModel"]):
    """
    A decision tree model for classification or regression.

    .. versionadded:: 1.1.0
    """

    @overload
    def predict(self, x: "VectorLike") -> float:
        ...

    @overload
    def predict(self, x: RDD["VectorLike"]) -> RDD[float]:
        ...

    def predict(self, x: Union["VectorLike", RDD["VectorLike"]]) -> Union[float, RDD[float]]:
        """
        Predict the label of one or more examples.

        .. versionadded:: 1.1.0

        Parameters
        ----------
        x : :py:class:`pyspark.mllib.linalg.Vector` or :py:class:`pyspark.RDD`
            Data point (feature vector), or an RDD of data points (feature
            vectors).

        Notes
        -----
        In Python, predict cannot currently be used within an RDD
        transformation or action.
        Call predict directly on the RDD instead.
        """
        if isinstance(x, RDD):
            return self.call("predict", x.map(_convert_to_vector))

        else:
            return self.call("predict", _convert_to_vector(x))

    @since("1.1.0")
    def numNodes(self) -> int:
        """Get number of nodes in tree, including leaf nodes."""
        return self._java_model.numNodes()

    @since("1.1.0")
    def depth(self) -> int:
        """
        Get depth of tree (e.g. depth 0 means 1 leaf node, depth 1
        means 1 internal node + 2 leaf nodes).
        """
        return self._java_model.depth()

    def __repr__(self) -> str:
        """summary of model."""
        return self._java_model.toString()

    @since("1.2.0")
    def toDebugString(self) -> str:
        """full model."""
        return self._java_model.toDebugString()

    @classmethod
    def _java_loader_class(cls) -> str:
        return "org.apache.spark.mllib.tree.model.DecisionTreeModel"


class DecisionTree:
    """
    Learning algorithm for a decision tree model for classification or
    regression.

    .. versionadded:: 1.1.0
    """

    @classmethod
    def _train(
        cls,
        data: RDD[LabeledPoint],
        type: str,
        numClasses: int,
        features: Dict[int, int],
        impurity: str = "gini",
        maxDepth: int = 5,
        maxBins: int = 32,
        minInstancesPerNode: int = 1,
        minInfoGain: float = 0.0,
    ) -> DecisionTreeModel:
        first = data.first()
        assert isinstance(first, LabeledPoint), "the data should be RDD of LabeledPoint"
        model = callMLlibFunc(
            "trainDecisionTreeModel",
            data,
            type,
            numClasses,
            features,
            impurity,
            maxDepth,
            maxBins,
            minInstancesPerNode,
            minInfoGain,
        )
        return DecisionTreeModel(model)

    @classmethod
    def trainClassifier(
        cls,
        data: RDD[LabeledPoint],
        numClasses: int,
        categoricalFeaturesInfo: Dict[int, int],
        impurity: str = "gini",
        maxDepth: int = 5,
        maxBins: int = 32,
        minInstancesPerNode: int = 1,
        minInfoGain: float = 0.0,
    ) -> DecisionTreeModel:
        """
        Train a decision tree model for classification.

        .. versionadded:: 1.1.0

        Parameters
        ----------
        data :  :py:class:`pyspark.RDD`
            Training data: RDD of LabeledPoint. Labels should take values
            {0, 1, ..., numClasses-1}.
        numClasses : int
            Number of classes for classification.
        categoricalFeaturesInfo : dict
            Map storing arity of categorical features. An entry (n -> k)
            indicates that feature n is categorical with k categories
            indexed from 0: {0, 1, ..., k-1}.
        impurity : str, optional
            Criterion used for information gain calculation.
            Supported values: "gini" or "entropy".
            (default: "gini")
        maxDepth : int, optional
            Maximum depth of tree (e.g. depth 0 means 1 leaf node, depth 1
            means 1 internal node + 2 leaf nodes).
            (default: 5)
        maxBins : int, optional
            Number of bins used for finding splits at each node.
            (default: 32)
        minInstancesPerNode : int, optional
            Minimum number of instances required at child nodes to create
            the parent split.
            (default: 1)
        minInfoGain : float, optional
            Minimum info gain required to create a split.
            (default: 0.0)

        Returns
        -------
        :py:class:`DecisionTreeModel`

        Examples
        --------
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
          If (feature 0 <= 0.5)
           Predict: 0.0
          Else (feature 0 > 0.5)
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
        return cls._train(
            data,
            "classification",
            numClasses,
            categoricalFeaturesInfo,
            impurity,
            maxDepth,
            maxBins,
            minInstancesPerNode,
            minInfoGain,
        )

    @classmethod
    @since("1.1.0")
    def trainRegressor(
        cls,
        data: RDD[LabeledPoint],
        categoricalFeaturesInfo: Dict[int, int],
        impurity: str = "variance",
        maxDepth: int = 5,
        maxBins: int = 32,
        minInstancesPerNode: int = 1,
        minInfoGain: float = 0.0,
    ) -> DecisionTreeModel:
        """
        Train a decision tree model for regression.

        Parameters
        ----------
        data : :py:class:`pyspark.RDD`
            Training data: RDD of LabeledPoint. Labels are real numbers.
        categoricalFeaturesInfo : dict
            Map storing arity of categorical features. An entry (n -> k)
            indicates that feature n is categorical with k categories
            indexed from 0: {0, 1, ..., k-1}.
        impurity : str, optional
            Criterion used for information gain calculation.
            The only supported value for regression is "variance".
            (default: "variance")
        maxDepth : int, optional
            Maximum depth of tree (e.g. depth 0 means 1 leaf node, depth 1
            means 1 internal node + 2 leaf nodes).
            (default: 5)
        maxBins : int, optional
            Number of bins used for finding splits at each node.
            (default: 32)
        minInstancesPerNode : int, optional
            Minimum number of instances required at child nodes to create
            the parent split.
            (default: 1)
        minInfoGain : float, optional
            Minimum info gain required to create a split.
            (default: 0.0)

        Returns
        -------
        :py:class:`DecisionTreeModel`

        Examples
        --------
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
        return cls._train(
            data,
            "regression",
            0,
            categoricalFeaturesInfo,
            impurity,
            maxDepth,
            maxBins,
            minInstancesPerNode,
            minInfoGain,
        )


@inherit_doc
class RandomForestModel(TreeEnsembleModel, JavaLoader["RandomForestModel"]):
    """
    Represents a random forest model.

    .. versionadded:: 1.2.0
    """

    @classmethod
    def _java_loader_class(cls) -> str:
        return "org.apache.spark.mllib.tree.model.RandomForestModel"


class RandomForest:
    """
    Learning algorithm for a random forest model for classification or
    regression.

    .. versionadded:: 1.2.0
    """

    supportedFeatureSubsetStrategies: Tuple[str, ...] = ("auto", "all", "sqrt", "log2", "onethird")

    @classmethod
    def _train(
        cls,
        data: RDD[LabeledPoint],
        algo: str,
        numClasses: int,
        categoricalFeaturesInfo: Dict[int, int],
        numTrees: int,
        featureSubsetStrategy: str,
        impurity: str,
        maxDepth: int,
        maxBins: int,
        seed: Optional[int],
    ) -> RandomForestModel:
        first = data.first()
        assert isinstance(first, LabeledPoint), "the data should be RDD of LabeledPoint"
        if featureSubsetStrategy not in cls.supportedFeatureSubsetStrategies:
            raise ValueError("unsupported featureSubsetStrategy: %s" % featureSubsetStrategy)
        if seed is None:
            seed = random.randint(0, 1 << 30)
        model = callMLlibFunc(
            "trainRandomForestModel",
            data,
            algo,
            numClasses,
            categoricalFeaturesInfo,
            numTrees,
            featureSubsetStrategy,
            impurity,
            maxDepth,
            maxBins,
            seed,
        )
        return RandomForestModel(model)

    @classmethod
    def trainClassifier(
        cls,
        data: RDD[LabeledPoint],
        numClasses: int,
        categoricalFeaturesInfo: Dict[int, int],
        numTrees: int,
        featureSubsetStrategy: str = "auto",
        impurity: str = "gini",
        maxDepth: int = 4,
        maxBins: int = 32,
        seed: Optional[int] = None,
    ) -> RandomForestModel:
        """
        Train a random forest model for binary or multiclass
        classification.

        .. versionadded:: 1.2.0

        Parameters
        ----------
        data : :py:class:`pyspark.RDD`
            Training dataset: RDD of LabeledPoint. Labels should take values
            {0, 1, ..., numClasses-1}.
        numClasses : int
            Number of classes for classification.
        categoricalFeaturesInfo : dict
            Map storing arity of categorical features. An entry (n -> k)
            indicates that feature n is categorical with k categories
            indexed from 0: {0, 1, ..., k-1}.
        numTrees : int
            Number of trees in the random forest.
        featureSubsetStrategy : str, optional
            Number of features to consider for splits at each node.
            Supported values: "auto", "all", "sqrt", "log2", "onethird".
            If "auto" is set, this parameter is set based on numTrees:
            if numTrees == 1, set to "all";
            if numTrees > 1 (forest) set to "sqrt".
            (default: "auto")
        impurity : str, optional
            Criterion used for information gain calculation.
            Supported values: "gini" or "entropy".
            (default: "gini")
        maxDepth : int, optional
            Maximum depth of tree (e.g. depth 0 means 1 leaf node, depth 1
            means 1 internal node + 2 leaf nodes).
            (default: 4)
        maxBins : int, optional
            Maximum number of bins used for splitting features.
            (default: 32)
        seed : int, Optional
            Random seed for bootstrapping and choosing feature subsets.
            Set as None to generate seed based on system time.
            (default: None)

        Returns
        -------
        :py:class:`RandomForestModel`
            that can be used for prediction.

        Examples
        --------
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
            If (feature 0 <= 1.5)
             Predict: 0.0
            Else (feature 0 > 1.5)
             Predict: 1.0
          Tree 2:
            If (feature 0 <= 1.5)
             Predict: 0.0
            Else (feature 0 > 1.5)
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
        return cls._train(
            data,
            "classification",
            numClasses,
            categoricalFeaturesInfo,
            numTrees,
            featureSubsetStrategy,
            impurity,
            maxDepth,
            maxBins,
            seed,
        )

    @classmethod
    def trainRegressor(
        cls,
        data: RDD[LabeledPoint],
        categoricalFeaturesInfo: Dict[int, int],
        numTrees: int,
        featureSubsetStrategy: str = "auto",
        impurity: str = "variance",
        maxDepth: int = 4,
        maxBins: int = 32,
        seed: Optional[int] = None,
    ) -> RandomForestModel:
        """
        Train a random forest model for regression.

        .. versionadded:: 1.2.0

        Parameters
        ----------
        data : :py:class:`pyspark.RDD`
            Training dataset: RDD of LabeledPoint. Labels are real numbers.
        categoricalFeaturesInfo : dict
            Map storing arity of categorical features. An entry (n -> k)
            indicates that feature n is categorical with k categories
            indexed from 0: {0, 1, ..., k-1}.
        numTrees : int
            Number of trees in the random forest.
        featureSubsetStrategy : str, optional
            Number of features to consider for splits at each node.
            Supported values: "auto", "all", "sqrt", "log2", "onethird".
            If "auto" is set, this parameter is set based on numTrees:

            - if numTrees == 1, set to "all";
            - if numTrees > 1 (forest) set to "onethird" for regression.

            (default: "auto")
        impurity : str, optional
            Criterion used for information gain calculation.
            The only supported value for regression is "variance".
            (default: "variance")
        maxDepth : int, optional
            Maximum depth of tree (e.g. depth 0 means 1 leaf node, depth 1
            means 1 internal node + 2 leaf nodes).
            (default: 4)
        maxBins : int, optional
            Maximum number of bins used for splitting features.
            (default: 32)
        seed : int, optional
            Random seed for bootstrapping and choosing feature subsets.
            Set as None to generate seed based on system time.
            (default: None)

        Returns
        -------
        :py:class:`RandomForestModel`
            that can be used for prediction.

        Examples
        --------
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
        return cls._train(
            data,
            "regression",
            0,
            categoricalFeaturesInfo,
            numTrees,
            featureSubsetStrategy,
            impurity,
            maxDepth,
            maxBins,
            seed,
        )


@inherit_doc
class GradientBoostedTreesModel(TreeEnsembleModel, JavaLoader["GradientBoostedTreesModel"]):
    """
    Represents a gradient-boosted tree model.

    .. versionadded:: 1.3.0
    """

    @classmethod
    def _java_loader_class(cls) -> str:
        return "org.apache.spark.mllib.tree.model.GradientBoostedTreesModel"


class GradientBoostedTrees:
    """
    Learning algorithm for a gradient boosted trees model for
    classification or regression.

    .. versionadded:: 1.3.0
    """

    @classmethod
    def _train(
        cls,
        data: RDD[LabeledPoint],
        algo: str,
        categoricalFeaturesInfo: Dict[int, int],
        loss: str,
        numIterations: int,
        learningRate: float,
        maxDepth: int,
        maxBins: int,
    ) -> GradientBoostedTreesModel:
        first = data.first()
        assert isinstance(first, LabeledPoint), "the data should be RDD of LabeledPoint"
        model = callMLlibFunc(
            "trainGradientBoostedTreesModel",
            data,
            algo,
            categoricalFeaturesInfo,
            loss,
            numIterations,
            learningRate,
            maxDepth,
            maxBins,
        )
        return GradientBoostedTreesModel(model)

    @classmethod
    def trainClassifier(
        cls,
        data: RDD[LabeledPoint],
        categoricalFeaturesInfo: Dict[int, int],
        loss: str = "logLoss",
        numIterations: int = 100,
        learningRate: float = 0.1,
        maxDepth: int = 3,
        maxBins: int = 32,
    ) -> GradientBoostedTreesModel:
        """
        Train a gradient-boosted trees model for classification.

        .. versionadded:: 1.3.0

        Parameters
        ----------
        data : :py:class:`pyspark.RDD`
            Training dataset: RDD of LabeledPoint. Labels should take values
            {0, 1}.
        categoricalFeaturesInfo : dict
            Map storing arity of categorical features. An entry (n -> k)
            indicates that feature n is categorical with k categories
            indexed from 0: {0, 1, ..., k-1}.
        loss : str, optional
            Loss function used for minimization during gradient boosting.
            Supported values: "logLoss", "leastSquaresError",
            "leastAbsoluteError".
            (default: "logLoss")
        numIterations : int, optional
            Number of iterations of boosting.
            (default: 100)
        learningRate : float, optional
            Learning rate for shrinking the contribution of each estimator.
            The learning rate should be between in the interval (0, 1].
            (default: 0.1)
        maxDepth : int, optional
            Maximum depth of tree (e.g. depth 0 means 1 leaf node, depth 1
            means 1 internal node + 2 leaf nodes).
            (default: 3)
        maxBins : int, optional
            Maximum number of bins used for splitting features. DecisionTree
            requires maxBins >= max categories.
            (default: 32)

        Returns
        -------
        :py:class:`GradientBoostedTreesModel`
            that can be used for prediction.

        Examples
        --------
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
        return cls._train(
            data,
            "classification",
            categoricalFeaturesInfo,
            loss,
            numIterations,
            learningRate,
            maxDepth,
            maxBins,
        )

    @classmethod
    def trainRegressor(
        cls,
        data: RDD[LabeledPoint],
        categoricalFeaturesInfo: Dict[int, int],
        loss: str = "leastSquaresError",
        numIterations: int = 100,
        learningRate: float = 0.1,
        maxDepth: int = 3,
        maxBins: int = 32,
    ) -> GradientBoostedTreesModel:
        """
        Train a gradient-boosted trees model for regression.

        .. versionadded:: 1.3.0

        Parameters
        ----------
        data :
            Training dataset: RDD of LabeledPoint. Labels are real numbers.
        categoricalFeaturesInfo : dict
            Map storing arity of categorical features. An entry (n -> k)
            indicates that feature n is categorical with k categories
            indexed from 0: {0, 1, ..., k-1}.
        loss : str, optional
            Loss function used for minimization during gradient boosting.
            Supported values: "logLoss", "leastSquaresError",
            "leastAbsoluteError".
            (default: "leastSquaresError")
        numIterations : int, optional
            Number of iterations of boosting.
            (default: 100)
        learningRate : float, optional
            Learning rate for shrinking the contribution of each estimator.
            The learning rate should be between in the interval (0, 1].
            (default: 0.1)
        maxDepth : int, optional
            Maximum depth of tree (e.g. depth 0 means 1 leaf node, depth 1
            means 1 internal node + 2 leaf nodes).
            (default: 3)
        maxBins : int, optional
            Maximum number of bins used for splitting features. DecisionTree
            requires maxBins >= max categories.
            (default: 32)

        Returns
        -------
        :py:class:`GradientBoostedTreesModel`
            that can be used for prediction.

        Examples
        --------
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
        return cls._train(
            data,
            "regression",
            categoricalFeaturesInfo,
            loss,
            numIterations,
            learningRate,
            maxDepth,
            maxBins,
        )


def _test() -> None:
    import doctest

    globs = globals().copy()
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.master("local[4]").appName("mllib.tree tests").getOrCreate()
    globs["sc"] = spark.sparkContext
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
