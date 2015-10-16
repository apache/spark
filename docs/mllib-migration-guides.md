---
layout: global
title: Old Migration Guides - MLlib
displayTitle: <a href="mllib-guide.html">MLlib</a> - Old Migration Guides
description: MLlib migration guides from before Spark SPARK_VERSION_SHORT
---

The migration guide for the current Spark version is kept on the [MLlib Programming Guide main page](mllib-guide.html#migration-guide).

## From 1.3 to 1.4

In the `spark.mllib` package, there were several breaking changes, but all in `DeveloperApi` or `Experimental` APIs:

* Gradient-Boosted Trees
    * *(Breaking change)* The signature of the [`Loss.gradient`](api/scala/index.html#org.apache.spark.mllib.tree.loss.Loss) method was changed.  This is only an issues for users who wrote their own losses for GBTs.
    * *(Breaking change)* The `apply` and `copy` methods for the case class [`BoostingStrategy`](api/scala/index.html#org.apache.spark.mllib.tree.configuration.BoostingStrategy) have been changed because of a modification to the case class fields.  This could be an issue for users who use `BoostingStrategy` to set GBT parameters.
* *(Breaking change)* The return value of [`LDA.run`](api/scala/index.html#org.apache.spark.mllib.clustering.LDA) has changed.  It now returns an abstract class `LDAModel` instead of the concrete class `DistributedLDAModel`.  The object of type `LDAModel` can still be cast to the appropriate concrete type, which depends on the optimization algorithm.

In the `spark.ml` package, several major API changes occurred, including:

* `Param` and other APIs for specifying parameters
* `uid` unique IDs for Pipeline components
* Reorganization of certain classes

Since the `spark.ml` API was an alpha component in Spark 1.3, we do not list all changes here.
However, since 1.4 `spark.ml` is no longer an alpha component, we will provide details on any API
changes for future releases.

## From 1.2 to 1.3

In the `spark.mllib` package, there were several breaking changes.  The first change (in `ALS`) is the only one in a component not marked as Alpha or Experimental.

* *(Breaking change)* In [`ALS`](api/scala/index.html#org.apache.spark.mllib.recommendation.ALS), the extraneous method `solveLeastSquares` has been removed.  The `DeveloperApi` method `analyzeBlocks` was also removed.
* *(Breaking change)* [`StandardScalerModel`](api/scala/index.html#org.apache.spark.mllib.feature.StandardScalerModel) remains an Alpha component. In it, the `variance` method has been replaced with the `std` method.  To compute the column variance values returned by the original `variance` method, simply square the standard deviation values returned by `std`.
* *(Breaking change)* [`StreamingLinearRegressionWithSGD`](api/scala/index.html#org.apache.spark.mllib.regression.StreamingLinearRegressionWithSGD) remains an Experimental component.  In it, there were two changes:
    * The constructor taking arguments was removed in favor of a builder pattern using the default constructor plus parameter setter methods.
    * Variable `model` is no longer public.
* *(Breaking change)* [`DecisionTree`](api/scala/index.html#org.apache.spark.mllib.tree.DecisionTree) remains an Experimental component.  In it and its associated classes, there were several changes:
    * In `DecisionTree`, the deprecated class method `train` has been removed.  (The object/static `train` methods remain.)
    * In `Strategy`, the `checkpointDir` parameter has been removed.  Checkpointing is still supported, but the checkpoint directory must be set before calling tree and tree ensemble training.
* `PythonMLlibAPI` (the interface between Scala/Java and Python for MLlib) was a public API but is now private, declared `private[python]`.  This was never meant for external use.
* In linear regression (including Lasso and ridge regression), the squared loss is now divided by 2.
  So in order to produce the same result as in 1.2, the regularization parameter needs to be divided by 2 and the step size needs to be multiplied by 2.

In the `spark.ml` package, the main API changes are from Spark SQL.  We list the most important changes here:

* The old [SchemaRDD](http://spark.apache.org/docs/1.2.1/api/scala/index.html#org.apache.spark.sql.SchemaRDD) has been replaced with [DataFrame](api/scala/index.html#org.apache.spark.sql.DataFrame) with a somewhat modified API.  All algorithms in Spark ML which used to use SchemaRDD now use DataFrame.
* In Spark 1.2, we used implicit conversions from `RDD`s of `LabeledPoint` into `SchemaRDD`s by calling `import sqlContext._` where `sqlContext` was an instance of `SQLContext`.  These implicits have been moved, so we now call `import sqlContext.implicits._`.
* Java APIs for SQL have also changed accordingly.  Please see the examples above and the [Spark SQL Programming Guide](sql-programming-guide.html) for details.

Other changes were in `LogisticRegression`:

* The `scoreCol` output column (with default value "score") was renamed to be `probabilityCol` (with default value "probability").  The type was originally `Double` (for the probability of class 1.0), but it is now `Vector` (for the probability of each class, to support multiclass classification in the future).
* In Spark 1.2, `LogisticRegressionModel` did not include an intercept.  In Spark 1.3, it includes an intercept; however, it will always be 0.0 since it uses the default settings for [spark.mllib.LogisticRegressionWithLBFGS](api/scala/index.html#org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS).  The option to use an intercept will be added in the future.

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

