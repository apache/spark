---
layout: global
title: MLlib
displayTitle: Machine Learning Library (MLlib) Guide
description: MLlib machine learning library overview for Spark SPARK_VERSION_SHORT
---

MLlib is Spark's scalable machine learning library consisting of common learning algorithms and utilities,
including classification, regression, clustering, collaborative
filtering, dimensionality reduction, as well as underlying optimization primitives, as outlined below:

* [Data types](mllib-data-types.html)
* [Basic statistics](mllib-statistics.html)
  * summary statistics
  * correlations
  * stratified sampling
  * hypothesis testing
  * random data generation  
* [Classification and regression](mllib-classification-regression.html)
  * [linear models (SVMs, logistic regression, linear regression)](mllib-linear-methods.html)
  * [naive Bayes](mllib-naive-bayes.html)
  * [decision trees](mllib-decision-tree.html)
  * [ensembles of trees](mllib-ensembles.html) (Random Forests and Gradient-Boosted Trees)
* [Collaborative filtering](mllib-collaborative-filtering.html)
  * alternating least squares (ALS)
* [Clustering](mllib-clustering.html)
  * k-means
  * Gaussian mixture
  * power iteration
* [Dimensionality reduction](mllib-dimensionality-reduction.html)
  * singular value decomposition (SVD)
  * principal component analysis (PCA)
* [Feature extraction and transformation](mllib-feature-extraction.html)
* [Optimization (developer)](mllib-optimization.html)
  * stochastic gradient descent
  * limited-memory BFGS (L-BFGS)

MLlib is under active development.
The APIs marked `Experimental`/`DeveloperApi` may change in future releases, 
and the migration guide below will explain all changes between releases.

# spark.ml: high-level APIs for ML pipelines

Spark 1.2 includes a new package called `spark.ml`, which aims to provide a uniform set of
high-level APIs that help users create and tune practical machine learning pipelines.
It is currently an alpha component, and we would like to hear back from the community about
how it fits real-world use cases and how it could be improved.

Note that we will keep supporting and adding features to `spark.mllib` along with the
development of `spark.ml`.
Users should be comfortable using `spark.mllib` features and expect more features coming.
Developers should contribute new algorithms to `spark.mllib` and can optionally contribute
to `spark.ml`.

See the **[spark.ml programming guide](ml-guide.html)** for more information on this package.

# Dependencies

MLlib uses the linear algebra package [Breeze](http://www.scalanlp.org/),
which depends on [netlib-java](https://github.com/fommil/netlib-java),
and [jblas](https://github.com/mikiobraun/jblas). 
`netlib-java` and `jblas` depend on native Fortran routines.
You need to install the
[gfortran runtime library](https://github.com/mikiobraun/jblas/wiki/Missing-Libraries)
if it is not already present on your nodes.
MLlib will throw a linking error if it cannot detect these libraries automatically.
Due to license issues, we do not include `netlib-java`'s native libraries in MLlib's
dependency set under default settings.
If no native library is available at runtime, you will see a warning message.
To use native libraries from `netlib-java`, please build Spark with `-Pnetlib-lgpl` or
include `com.github.fommil.netlib:all:1.1.2` as a dependency of your project.
If you want to use optimized BLAS/LAPACK libraries such as
[OpenBLAS](http://www.openblas.net/), please link its shared libraries to
`/usr/lib/libblas.so.3` and `/usr/lib/liblapack.so.3`, respectively.
BLAS/LAPACK libraries on worker nodes should be built without multithreading.

To use MLlib in Python, you will need [NumPy](http://www.numpy.org) version 1.4 or newer.

---

# Migration Guide

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

<div class="codetabs">
<div data-lang="scala" markdown="1">

We used to represent a feature vector by `Array[Double]`, which is replaced by
[`Vector`](api/scala/index.html#org.apache.spark.mllib.linalg.Vector) in v1.0. Algorithms that used
to accept `RDD[Array[Double]]` now take
`RDD[Vector]`. [`LabeledPoint`](api/scala/index.html#org.apache.spark.mllib.regression.LabeledPoint)
is now a wrapper of `(Double, Vector)` instead of `(Double, Array[Double])`. Converting
`Array[Double]` to `Vector` is straightforward:

{% highlight scala %}
import org.apache.spark.mllib.linalg.{Vector, Vectors}

val array: Array[Double] = ... // a double array
val vector: Vector = Vectors.dense(array) // a dense vector
{% endhighlight %}

[`Vectors`](api/scala/index.html#org.apache.spark.mllib.linalg.Vectors$) provides factory methods to create sparse vectors.

*Note*: Scala imports `scala.collection.immutable.Vector` by default, so you have to import `org.apache.spark.mllib.linalg.Vector` explicitly to use MLlib's `Vector`.

</div>

<div data-lang="java" markdown="1">

We used to represent a feature vector by `double[]`, which is replaced by
[`Vector`](api/java/index.html?org/apache/spark/mllib/linalg/Vector.html) in v1.0. Algorithms that used
to accept `RDD<double[]>` now take
`RDD<Vector>`. [`LabeledPoint`](api/java/index.html?org/apache/spark/mllib/regression/LabeledPoint.html)
is now a wrapper of `(double, Vector)` instead of `(double, double[])`. Converting `double[]` to
`Vector` is straightforward:

{% highlight java %}
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

double[] array = ... // a double array
Vector vector = Vectors.dense(array); // a dense vector
{% endhighlight %}

[`Vectors`](api/scala/index.html#org.apache.spark.mllib.linalg.Vectors$) provides factory methods to
create sparse vectors.

</div>

<div data-lang="python" markdown="1">

We used to represent a labeled feature vector in a NumPy array, where the first entry corresponds to
the label and the rest are features.  This representation is replaced by class
[`LabeledPoint`](api/python/pyspark.mllib.regression.LabeledPoint-class.html), which takes both
dense and sparse feature vectors.

{% highlight python %}
from pyspark.mllib.linalg import SparseVector
from pyspark.mllib.regression import LabeledPoint

# Create a labeled point with a positive label and a dense feature vector.
pos = LabeledPoint(1.0, [1.0, 0.0, 3.0])

# Create a labeled point with a negative label and a sparse feature vector.
neg = LabeledPoint(0.0, SparseVector(3, [0, 2], [1.0, 3.0]))
{% endhighlight %}
</div>
</div>
