---
layout: global
title: Old Migration Guides - MLlib
displayTitle: Old Migration Guides - MLlib
description: MLlib migration guides from before Spark SPARK_VERSION_SHORT
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
 
     http://www.apache.org/licenses/LICENSE-2.0
 
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

The migration guide for the current Spark version is kept on the [MLlib Guide main page](ml-guide.html#migration-guide).

## From 2.1 to 2.2

### Breaking changes

There are no breaking changes.

### Deprecations and changes of behavior

**Deprecations**

There are no deprecations.

**Changes of behavior**

* [SPARK-19787](https://issues.apache.org/jira/browse/SPARK-19787):
 Default value of `regParam` changed from `1.0` to `0.1` for `ALS.train` method (marked `DeveloperApi`).
 **Note** this does _not affect_ the `ALS` Estimator or Model, nor MLlib's `ALS` class.
* [SPARK-14772](https://issues.apache.org/jira/browse/SPARK-14772):
 Fixed inconsistency between Python and Scala APIs for `Param.copy` method.
* [SPARK-11569](https://issues.apache.org/jira/browse/SPARK-11569):
 `StringIndexer` now handles `NULL` values in the same way as unseen values. Previously an exception
 would always be thrown regardless of the setting of the `handleInvalid` parameter.
 
## From 2.0 to 2.1

### Breaking changes
 
**Deprecated methods removed**

* `setLabelCol` in `feature.ChiSqSelectorModel`
* `numTrees` in `classification.RandomForestClassificationModel` (This now refers to the Param called `numTrees`)
* `numTrees` in `regression.RandomForestRegressionModel` (This now refers to the Param called `numTrees`)
* `model` in `regression.LinearRegressionSummary`
* `validateParams` in `PipelineStage`
* `validateParams` in `Evaluator`

### Deprecations and changes of behavior

**Deprecations**

* [SPARK-18592](https://issues.apache.org/jira/browse/SPARK-18592):
  Deprecate all Param setter methods except for input/output column Params for `DecisionTreeClassificationModel`, `GBTClassificationModel`, `RandomForestClassificationModel`, `DecisionTreeRegressionModel`, `GBTRegressionModel` and `RandomForestRegressionModel`

**Changes of behavior**

* [SPARK-17870](https://issues.apache.org/jira/browse/SPARK-17870):
 Fix a bug of `ChiSqSelector` which will likely change its result. Now `ChiSquareSelector` use pValue rather than raw statistic to select a fixed number of top features.
* [SPARK-3261](https://issues.apache.org/jira/browse/SPARK-3261):
 `KMeans` returns potentially fewer than k cluster centers in cases where k distinct centroids aren't available or aren't selected.
* [SPARK-17389](https://issues.apache.org/jira/browse/SPARK-17389):
 `KMeans` reduces the default number of steps from 5 to 2 for the k-means|| initialization mode.

## From 1.6 to 2.0

### Breaking changes

There were several breaking changes in Spark 2.0, which are outlined below.

**Linear algebra classes for DataFrame-based APIs**

Spark's linear algebra dependencies were moved to a new project, `mllib-local` 
(see [SPARK-13944](https://issues.apache.org/jira/browse/SPARK-13944)). 
As part of this change, the linear algebra classes were copied to a new package, `spark.ml.linalg`. 
The DataFrame-based APIs in `spark.ml` now depend on the `spark.ml.linalg` classes, 
leading to a few breaking changes, predominantly in various model classes 
(see [SPARK-14810](https://issues.apache.org/jira/browse/SPARK-14810) for a full list).

**Note:** the RDD-based APIs in `spark.mllib` continue to depend on the previous package `spark.mllib.linalg`.

_Converting vectors and matrices_

While most pipeline components support backward compatibility for loading, 
some existing `DataFrames` and pipelines in Spark versions prior to 2.0, that contain vector or matrix 
columns, may need to be migrated to the new `spark.ml` vector and matrix types. 
Utilities for converting `DataFrame` columns from `spark.mllib.linalg` to `spark.ml.linalg` types
(and vice versa) can be found in `spark.mllib.util.MLUtils`.

There are also utility methods available for converting single instances of 
vectors and matrices. Use the `asML` method on a `mllib.linalg.Vector` / `mllib.linalg.Matrix`
for converting to `ml.linalg` types, and 
`mllib.linalg.Vectors.fromML` / `mllib.linalg.Matrices.fromML` 
for converting to `mllib.linalg` types.

<div class="codetabs">
<div data-lang="scala"  markdown="1">

{% highlight scala %}
import org.apache.spark.mllib.util.MLUtils

// convert DataFrame columns
val convertedVecDF = MLUtils.convertVectorColumnsToML(vecDF)
val convertedMatrixDF = MLUtils.convertMatrixColumnsToML(matrixDF)
// convert a single vector or matrix
val mlVec: org.apache.spark.ml.linalg.Vector = mllibVec.asML
val mlMat: org.apache.spark.ml.linalg.Matrix = mllibMat.asML
{% endhighlight %}

Refer to the [`MLUtils` Scala docs](api/scala/index.html#org.apache.spark.mllib.util.MLUtils$) for further detail.
</div>

<div data-lang="java" markdown="1">

{% highlight java %}
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.sql.Dataset;

// convert DataFrame columns
Dataset<Row> convertedVecDF = MLUtils.convertVectorColumnsToML(vecDF);
Dataset<Row> convertedMatrixDF = MLUtils.convertMatrixColumnsToML(matrixDF);
// convert a single vector or matrix
org.apache.spark.ml.linalg.Vector mlVec = mllibVec.asML();
org.apache.spark.ml.linalg.Matrix mlMat = mllibMat.asML();
{% endhighlight %}

Refer to the [`MLUtils` Java docs](api/java/org/apache/spark/mllib/util/MLUtils.html) for further detail.
</div>

<div data-lang="python"  markdown="1">

{% highlight python %}
from pyspark.mllib.util import MLUtils

# convert DataFrame columns
convertedVecDF = MLUtils.convertVectorColumnsToML(vecDF)
convertedMatrixDF = MLUtils.convertMatrixColumnsToML(matrixDF)
# convert a single vector or matrix
mlVec = mllibVec.asML()
mlMat = mllibMat.asML()
{% endhighlight %}

Refer to the [`MLUtils` Python docs](api/python/pyspark.mllib.html#pyspark.mllib.util.MLUtils) for further detail.
</div>
</div>

**Deprecated methods removed**

Several deprecated methods were removed in the `spark.mllib` and `spark.ml` packages:

* `setScoreCol` in `ml.evaluation.BinaryClassificationEvaluator`
* `weights` in `LinearRegression` and `LogisticRegression` in `spark.ml`
* `setMaxNumIterations` in `mllib.optimization.LBFGS` (marked as `DeveloperApi`)
* `treeReduce` and `treeAggregate` in `mllib.rdd.RDDFunctions` (these functions are available on `RDD`s directly, and were marked as `DeveloperApi`)
* `defaultStategy` in `mllib.tree.configuration.Strategy`
* `build` in `mllib.tree.Node`
* libsvm loaders for multiclass and load/save labeledData methods in `mllib.util.MLUtils`

A full list of breaking changes can be found at [SPARK-14810](https://issues.apache.org/jira/browse/SPARK-14810).

### Deprecations and changes of behavior

**Deprecations**

Deprecations in the `spark.mllib` and `spark.ml` packages include:

* [SPARK-14984](https://issues.apache.org/jira/browse/SPARK-14984):
 In `spark.ml.regression.LinearRegressionSummary`, the `model` field has been deprecated.
* [SPARK-13784](https://issues.apache.org/jira/browse/SPARK-13784):
 In `spark.ml.regression.RandomForestRegressionModel` and `spark.ml.classification.RandomForestClassificationModel`,
 the `numTrees` parameter has been deprecated in favor of `getNumTrees` method.
* [SPARK-13761](https://issues.apache.org/jira/browse/SPARK-13761):
 In `spark.ml.param.Params`, the `validateParams` method has been deprecated.
 We move all functionality in overridden methods to the corresponding `transformSchema`.
* [SPARK-14829](https://issues.apache.org/jira/browse/SPARK-14829):
 In `spark.mllib` package, `LinearRegressionWithSGD`, `LassoWithSGD`, `RidgeRegressionWithSGD` and `LogisticRegressionWithSGD` have been deprecated.
 We encourage users to use `spark.ml.regression.LinearRegresson` and `spark.ml.classification.LogisticRegresson`.
* [SPARK-14900](https://issues.apache.org/jira/browse/SPARK-14900):
 In `spark.mllib.evaluation.MulticlassMetrics`, the parameters `precision`, `recall` and `fMeasure` have been deprecated in favor of `accuracy`.
* [SPARK-15644](https://issues.apache.org/jira/browse/SPARK-15644):
 In `spark.ml.util.MLReader` and `spark.ml.util.MLWriter`, the `context` method has been deprecated in favor of `session`.
* In `spark.ml.feature.ChiSqSelectorModel`, the `setLabelCol` method has been deprecated since it was not used by `ChiSqSelectorModel`.

**Changes of behavior**

Changes of behavior in the `spark.mllib` and `spark.ml` packages include:

* [SPARK-7780](https://issues.apache.org/jira/browse/SPARK-7780):
 `spark.mllib.classification.LogisticRegressionWithLBFGS` directly calls `spark.ml.classification.LogisticRegresson` for binary classification now.
 This will introduce the following behavior changes for `spark.mllib.classification.LogisticRegressionWithLBFGS`:
    * The intercept will not be regularized when training binary classification model with L1/L2 Updater.
    * If users set without regularization, training with or without feature scaling will return the same solution by the same convergence rate.
* [SPARK-13429](https://issues.apache.org/jira/browse/SPARK-13429):
 In order to provide better and consistent result with `spark.ml.classification.LogisticRegresson`,
 the default value of `spark.mllib.classification.LogisticRegressionWithLBFGS`: `convergenceTol` has been changed from 1E-4 to 1E-6.
* [SPARK-12363](https://issues.apache.org/jira/browse/SPARK-12363):
 Fix a bug of `PowerIterationClustering` which will likely change its result.
* [SPARK-13048](https://issues.apache.org/jira/browse/SPARK-13048):
 `LDA` using the `EM` optimizer will keep the last checkpoint by default, if checkpointing is being used.
* [SPARK-12153](https://issues.apache.org/jira/browse/SPARK-12153):
 `Word2Vec` now respects sentence boundaries. Previously, it did not handle them correctly.
* [SPARK-10574](https://issues.apache.org/jira/browse/SPARK-10574):
 `HashingTF` uses `MurmurHash3` as default hash algorithm in both `spark.ml` and `spark.mllib`.
* [SPARK-14768](https://issues.apache.org/jira/browse/SPARK-14768):
 The `expectedType` argument for PySpark `Param` was removed.
* [SPARK-14931](https://issues.apache.org/jira/browse/SPARK-14931):
 Some default `Param` values, which were mismatched between pipelines in Scala and Python, have been changed.
* [SPARK-13600](https://issues.apache.org/jira/browse/SPARK-13600):
 `QuantileDiscretizer` now uses `spark.sql.DataFrameStatFunctions.approxQuantile` to find splits (previously used custom sampling logic).
 The output buckets will differ for same input data and params.

## From 1.5 to 1.6

There are no breaking API changes in the `spark.mllib` or `spark.ml` packages, but there are
deprecations and changes of behavior.

Deprecations:

* [SPARK-11358](https://issues.apache.org/jira/browse/SPARK-11358):
 In `spark.mllib.clustering.KMeans`, the `runs` parameter has been deprecated.
* [SPARK-10592](https://issues.apache.org/jira/browse/SPARK-10592):
 In `spark.ml.classification.LogisticRegressionModel` and
 `spark.ml.regression.LinearRegressionModel`, the `weights` field has been deprecated in favor of
 the new name `coefficients`.  This helps disambiguate from instance (row) "weights" given to
 algorithms.

Changes of behavior:

* [SPARK-7770](https://issues.apache.org/jira/browse/SPARK-7770):
 `spark.mllib.tree.GradientBoostedTrees`: `validationTol` has changed semantics in 1.6.
 Previously, it was a threshold for absolute change in error. Now, it resembles the behavior of
 `GradientDescent`'s `convergenceTol`: For large errors, it uses relative error (relative to the
 previous error); for small errors (`< 0.01`), it uses absolute error.
* [SPARK-11069](https://issues.apache.org/jira/browse/SPARK-11069):
 `spark.ml.feature.RegexTokenizer`: Previously, it did not convert strings to lowercase before
 tokenizing. Now, it converts to lowercase by default, with an option not to. This matches the
 behavior of the simpler `Tokenizer` transformer.

## From 1.4 to 1.5

In the `spark.mllib` package, there are no breaking API changes but several behavior changes:

* [SPARK-9005](https://issues.apache.org/jira/browse/SPARK-9005):
  `RegressionMetrics.explainedVariance` returns the average regression sum of squares.
* [SPARK-8600](https://issues.apache.org/jira/browse/SPARK-8600): `NaiveBayesModel.labels` become
  sorted.
* [SPARK-3382](https://issues.apache.org/jira/browse/SPARK-3382): `GradientDescent` has a default
  convergence tolerance `1e-3`, and hence iterations might end earlier than 1.4.

In the `spark.ml` package, there exists one breaking API change and one behavior change:

* [SPARK-9268](https://issues.apache.org/jira/browse/SPARK-9268): Java's varargs support is removed
  from `Params.setDefault` due to a
  [Scala compiler bug](https://issues.scala-lang.org/browse/SI-9013).
* [SPARK-10097](https://issues.apache.org/jira/browse/SPARK-10097): `Evaluator.isLargerBetter` is
  added to indicate metric ordering. Metrics like RMSE no longer flip signs as in 1.4.

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

* The old [SchemaRDD](https://spark.apache.org/docs/1.2.1/api/scala/index.html#org.apache.spark.sql.SchemaRDD) has been replaced with [DataFrame](api/scala/index.html#org.apache.spark.sql.DataFrame) with a somewhat modified API.  All algorithms in `spark.ml` which used to use SchemaRDD now use DataFrame.
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

Examples of the new recommended `trainClassifier` and `trainRegressor` are given in the
[Decision Trees Guide](mllib-decision-tree.html#examples).

## From 0.9 to 1.0

In MLlib v1.0, we support both dense and sparse input in a unified way, which introduces a few
breaking changes.  If your data is sparse, please store it in a sparse format instead of dense to
take advantage of sparsity in both storage and computation. Details are described below.

