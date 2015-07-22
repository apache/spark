---
layout: global
title: MLlib
displayTitle: Machine Learning Library (MLlib) Guide
description: MLlib machine learning library overview for Spark SPARK_VERSION_SHORT
---

MLlib is Spark's scalable machine learning library consisting of common learning algorithms and utilities,
including classification, regression, clustering, collaborative
filtering, dimensionality reduction, as well as underlying optimization primitives.
Guides for individual algorithms are listed below.

The API is divided into 2 parts:

* [The original `spark.mllib` API](mllib-guide.html#mllib-types-algorithms-and-utilities) is the primary API.
* [The "Pipelines" `spark.ml` API](mllib-guide.html#sparkml-high-level-apis-for-ml-pipelines) is a higher-level API for constructing ML workflows.

We list major functionality from both below, with links to detailed guides.

# MLlib types, algorithms and utilities

This lists functionality included in `spark.mllib`, the main MLlib API.

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
* [PMML model export](mllib-pmml-model-export.html)

MLlib is under active development.
The APIs marked `Experimental`/`DeveloperApi` may change in future releases, 
and the migration guide below will explain all changes between releases.

# spark.ml: high-level APIs for ML pipelines

Spark 1.2 introduced a new package called `spark.ml`, which aims to provide a uniform set of
high-level APIs that help users create and tune practical machine learning pipelines.

*Graduated from Alpha!*  The Pipelines API is no longer an alpha component, although many elements of it are still `Experimental` or `DeveloperApi`.

Note that we will keep supporting and adding features to `spark.mllib` along with the
development of `spark.ml`.
Users should be comfortable using `spark.mllib` features and expect more features coming.
Developers should contribute new algorithms to `spark.mllib` and can optionally contribute
to `spark.ml`.

More detailed guides for `spark.ml` include:

* **[spark.ml programming guide](ml-guide.html)**: overview of the Pipelines API and major concepts
* [Feature transformers](ml-features.html): Details on transformers supported in the Pipelines API, including a few not in the lower-level `spark.mllib` API
* [Ensembles](ml-ensembles.html): Details on ensemble learning methods in the Pipelines API

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

## From 1.3 to 1.4

In the `spark.mllib` package, there were several breaking changes, but all in `DeveloperApi` or `Experimental` APIs:

* Gradient-Boosted Trees
    * *(Breaking change)* The signature of the [`Loss.gradient`](api/scala/index.html#org.apache.spark.mllib.tree.loss.Loss) method was changed.  This is only an issues for users who wrote their own losses for GBTs.
    * *(Breaking change)* The `apply` and `copy` methods for the case class [`BoostingStrategy`](api/scala/index.html#org.apache.spark.mllib.tree.configuration.BoostingStrategy) have been changed because of a modification to the case class fields.  This could be an issue for users who use `BoostingStrategy` to set GBT parameters.
* *(Breaking change)* The return value of [`LDA.run`](api/scala/index.html#org.apache.spark.mllib.clustering.LDA) has changed.  It now returns an abstract class `LDAModel` instead of the concrete class `DistributedLDAModel`.  The object of type `LDAModel` can still be cast to the appropriate concrete type, which depends on the optimization algorithm.

## Previous Spark Versions

Earlier migration guides are archived [on this page](mllib-migration-guides.html).
