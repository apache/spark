---
layout: global
title: Old Migration Guides - MLlib
displayTitle: <a href="mllib-guide.html">MLlib</a> - Old Migration Guides
description: MLlib migration guides from before Spark SPARK_VERSION_SHORT
---

The migration guide for the current Spark version is kept on the [MLlib Programming Guide main page](mllib-guide.html#migration-guide).

## From 1.1 to 1.2

The only API changes in MLlib v1.2 are in
[`DecisionTree`](api/scala/index.html#org.apache.spark.mllib.tree.DecisionTree),
which continues to be an experimental API in MLlib 1.2:

1. *(Breaking change)* The Scala API for classification takes a named argument specifying the number
of classes.  In MLlib v1.1, this argument was called `numClasses` in Python and
`numClassesForClassification` in Scala.  In MLlib v1.2, the names are both set to `numClasses`.
This `numClasses` parameter is specified either via
[`Strategy`](api/scala/index.html#org.apache.spark.mllib.tree.configuration.Strategy)
or via [`DecisionTree`](api/scala/index.html#org.apache.spark.mllib.tree.DecisionTree)
static `trainClassifier` and `trainRegressor` methods.

2. *(Breaking change)* The API for
[`Node`](api/scala/index.html#org.apache.spark.mllib.tree.model.Node) has changed.
This should generally not affect user code, unless the user manually constructs decision trees
(instead of using the `trainClassifier` or `trainRegressor` methods).
The tree `Node` now includes more information, including the probability of the predicted label
(for classification).

3. Printing methods' output has changed.  The `toString` (Scala/Java) and `__repr__` (Python) methods used to print the full model; they now print a summary.  For the full model, use `toDebugString`.

Examples in the Spark distribution and examples in the
[Decision Trees Guide](mllib-decision-tree.html#examples) have been updated accordingly.

## From 1.0 to 1.1

The only API changes in MLlib v1.1 are in
[`DecisionTree`](api/scala/index.html#org.apache.spark.mllib.tree.DecisionTree),
which continues to be an experimental API in MLlib 1.1:

1. *(Breaking change)* The meaning of tree depth has been changed by 1 in order to match
the implementations of trees in
[scikit-learn](http://scikit-learn.org/stable/modules/classes.html#module-sklearn.tree)
and in [rpart](http://cran.r-project.org/web/packages/rpart/index.html).
In MLlib v1.0, a depth-1 tree had 1 leaf node, and a depth-2 tree had 1 root node and 2 leaf nodes.
In MLlib v1.1, a depth-0 tree has 1 leaf node, and a depth-1 tree has 1 root node and 2 leaf nodes.
This depth is specified by the `maxDepth` parameter in
[`Strategy`](api/scala/index.html#org.apache.spark.mllib.tree.configuration.Strategy)
or via [`DecisionTree`](api/scala/index.html#org.apache.spark.mllib.tree.DecisionTree)
static `trainClassifier` and `trainRegressor` methods.

2. *(Non-breaking change)* We recommend using the newly added `trainClassifier` and `trainRegressor`
methods to build a [`DecisionTree`](api/scala/index.html#org.apache.spark.mllib.tree.DecisionTree),
rather than using the old parameter class `Strategy`.  These new training methods explicitly
separate classification and regression, and they replace specialized parameter types with
simple `String` types.

Examples of the new, recommended `trainClassifier` and `trainRegressor` are given in the
[Decision Trees Guide](mllib-decision-tree.html#examples).

## From 0.9 to 1.0

In MLlib v1.0, we support both dense and sparse input in a unified way, which introduces a few
breaking changes.  If your data is sparse, please store it in a sparse format instead of dense to
take advantage of sparsity in both storage and computation. Details are described below.

