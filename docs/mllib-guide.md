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
  * [isotonic regression](mllib-isotonic-regression.html)
* [Collaborative filtering](mllib-collaborative-filtering.html)
  * alternating least squares (ALS)
* [Clustering](mllib-clustering.html)
  * [k-means](mllib-clustering.html#k-means)
  * [Gaussian mixture](mllib-clustering.html#gaussian-mixture)
  * [power iteration clustering (PIC)](mllib-clustering.html#power-iteration-clustering-pic)
  * [latent Dirichlet allocation (LDA)](mllib-clustering.html#latent-dirichlet-allocation-lda)
  * [streaming k-means](mllib-clustering.html#streaming-k-means)
* [Dimensionality reduction](mllib-dimensionality-reduction.html)
  * singular value decomposition (SVD)
  * principal component analysis (PCA)
* [Feature extraction and transformation](mllib-feature-extraction.html)
* [Frequent pattern mining](mllib-frequent-pattern-mining.html)
  * FP-growth
* [Optimization (developer)](mllib-optimization.html)
  * stochastic gradient descent
  * limited-memory BFGS (L-BFGS)

MLlib is under active development.
The APIs marked `Experimental`/`DeveloperApi` may change in future releases, 
and the migration guide below will explain all changes between releases.

# spark.ml: high-level APIs for ML pipelines

Spark 1.2 introduced a new package called `spark.ml`, which aims to provide a uniform set of
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

MLlib uses the linear algebra package
[Breeze](http://www.scalanlp.org/), which depends on
[netlib-java](https://github.com/fommil/netlib-java) for optimised
numerical processing. If natives are not available at runtime, you
will see a warning message and a pure JVM implementation will be used
instead.

To learn more about the benefits and background of system optimised
natives, you may wish to watch Sam Halliday's ScalaX talk on
[High Performance Linear Algebra in Scala](http://fommil.github.io/scalax14/#/)).

Due to licensing issues with runtime proprietary binaries, we do not
include `netlib-java`'s native proxies by default. To configure
`netlib-java` / Breeze to use system optimised binaries, include
`com.github.fommil.netlib:all:1.1.2` (or build Spark with
`-Pnetlib-lgpl`) as a dependency of your project and read the
[netlib-java](https://github.com/fommil/netlib-java) documentation for
your platform's additional installation instructions.

To use MLlib in Python, you will need [NumPy](http://www.numpy.org)
version 1.4 or newer.

---

# Migration Guide

For the `spark.ml` package, please see the [spark.ml Migration Guide](ml-guide.html#migration-guide).

## From 1.2 to 1.3

In the `spark.mllib` package, there were several breaking changes.  The first change (in `ALS`) is the only one in a component not marked as Alpha or Experimental.

* *(Breaking change)* In [`ALS`](api/scala/index.html#org.apache.spark.mllib.recommendation.ALS), the extraneous method `solveLeastSquares` has been removed.  The `DeveloperApi` method `analyzeBlocks` was also removed.
* *(Breaking change)* [`StandardScalerModel`](api/scala/index.html#org.apache.spark.mllib.feature.StandardScalerModel) remains an Alpha component. In it, the `variance` method has been replaced with the `std` method.  To compute the column variance values returned by the original `variance` method, simply square the standard deviation values returned by `std`.
* *(Breaking change)* [`StreamingLinearRegressionWithSGD`](api/scala/index.html#org.apache.spark.mllib.regression.StreamingLinearRegressionWithSGD) remains an Experimental component.  In it, there were two changes:
    * The constructor taking arguments was removed in favor of a builder patten using the default constructor plus parameter setter methods.
    * Variable `model` is no longer public.
* *(Breaking change)* [`DecisionTree`](api/scala/index.html#org.apache.spark.mllib.tree.DecisionTree) remains an Experimental component.  In it and its associated classes, there were several changes:
    * In `DecisionTree`, the deprecated class method `train` has been removed.  (The object/static `train` methods remain.)
    * In `Strategy`, the `checkpointDir` parameter has been removed.  Checkpointing is still supported, but the checkpoint directory must be set before calling tree and tree ensemble training.
* `PythonMLlibAPI` (the interface between Scala/Java and Python for MLlib) was a public API but is now private, declared `private[python]`.  This was never meant for external use.

## Previous Spark Versions

Earlier migration guides are archived [on this page](mllib-migration-guides.html).
