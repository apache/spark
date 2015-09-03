---
layout: global
title: MLlib
displayTitle: Machine Learning Library (MLlib) Guide
description: MLlib machine learning library overview for Spark SPARK_VERSION_SHORT
---

MLlib is Spark's machine learning (ML) library.
Its goal is to make practical machine learning scalable and easy.
It consists of common learning algorithms and utilities, including classification, regression,
clustering, collaborative filtering, dimensionality reduction, as well as lower-level optimization
primitives and higher-level pipeline APIs.

It divides into two packages:

* [`spark.mllib`](mllib-guide.html#mllib-types-algorithms-and-utilities) contains the original API
  built on top of [RDDs](programming-guide.html#resilient-distributed-datasets-rdds).
* [`spark.ml`](mllib-guide.html#sparkml-high-level-apis-for-ml-pipelines) provides higher-level API
  built on top of [DataFrames](sql-programming-guide.html#dataframes) for constructing ML pipelines.

Using `spark.ml` is recommended because with DataFrames the API is more versatile and flexible.
But we will keep supporting `spark.mllib` along with the development of `spark.ml`.
Users should be comfortable using `spark.mllib` features and expect more features coming.
Developers should contribute new algorithms to `spark.ml` if they fit the ML pipeline concept well,
e.g., feature extractors and transformers.

We list major functionality from both below, with links to detailed guides.

# spark.mllib: data types, algorithms, and utilities

* [Data types](mllib-data-types.html)
* [Basic statistics](mllib-statistics.html)
  * [summary statistics](mllib-statistics.html#summary-statistics)
  * [correlations](mllib-statistics.html#correlations)
  * [stratified sampling](mllib-statistics.html#stratified-sampling)
  * [hypothesis testing](mllib-statistics.html#hypothesis-testing)
  * [random data generation](mllib-statistics.html#random-data-generation)
* [Classification and regression](mllib-classification-regression.html)
  * [linear models (SVMs, logistic regression, linear regression)](mllib-linear-methods.html)
  * [naive Bayes](mllib-naive-bayes.html)
  * [decision trees](mllib-decision-tree.html)
  * [ensembles of trees (Random Forests and Gradient-Boosted Trees)](mllib-ensembles.html)
  * [isotonic regression](mllib-isotonic-regression.html)
* [Collaborative filtering](mllib-collaborative-filtering.html)
  * [alternating least squares (ALS)](mllib-collaborative-filtering.html#collaborative-filtering)
* [Clustering](mllib-clustering.html)
  * [k-means](mllib-clustering.html#k-means)
  * [Gaussian mixture](mllib-clustering.html#gaussian-mixture)
  * [power iteration clustering (PIC)](mllib-clustering.html#power-iteration-clustering-pic)
  * [latent Dirichlet allocation (LDA)](mllib-clustering.html#latent-dirichlet-allocation-lda)
  * [streaming k-means](mllib-clustering.html#streaming-k-means)
* [Dimensionality reduction](mllib-dimensionality-reduction.html)
  * [singular value decomposition (SVD)](mllib-dimensionality-reduction.html#singular-value-decomposition-svd)
  * [principal component analysis (PCA)](mllib-dimensionality-reduction.html#principal-component-analysis-pca)
* [Feature extraction and transformation](mllib-feature-extraction.html)
* [Frequent pattern mining](mllib-frequent-pattern-mining.html)
  * [FP-growth](mllib-frequent-pattern-mining.html#fp-growth)
  * [association rules](mllib-frequent-pattern-mining.html#association-rules)
  * [PrefixSpan](mllib-frequent-pattern-mining.html#prefix-span)
* [Evaluation metrics](mllib-evaluation-metrics.html)
* [PMML model export](mllib-pmml-model-export.html)
* [Optimization (developer)](mllib-optimization.html)
  * [stochastic gradient descent](mllib-optimization.html#stochastic-gradient-descent-sgd)
  * [limited-memory BFGS (L-BFGS)](mllib-optimization.html#limited-memory-bfgs-l-bfgs)

# spark.ml: high-level APIs for ML pipelines

**[spark.ml programming guide](ml-guide.html)** provides an overview of the Pipelines API and major
concepts. It also contains sections on using algorithms within the Pipelines API, for example:

* [Feature extraction, transformation, and selection](ml-features.html)
* [Decision trees for classification and regression](ml-decision-tree.html)
* [Ensembles](ml-ensembles.html)
* [Linear methods with elastic net regularization](ml-linear-methods.html)
* [Multilayer perceptron classifier](ml-ann.html)

# Dependencies

MLlib uses the linear algebra package [Breeze](http://www.scalanlp.org/), which depends on
[netlib-java](https://github.com/fommil/netlib-java) for optimised numerical processing.
If natives libraries[^1] are not available at runtime, you will see a warning message and a pure JVM
implementation will be used instead.

Due to licensing issues with runtime proprietary binaries, we do not include `netlib-java`'s native
proxies by default.
To configure `netlib-java` / Breeze to use system optimised binaries, include
`com.github.fommil.netlib:all:1.1.2` (or build Spark with `-Pnetlib-lgpl`) as a dependency of your
project and read the [netlib-java](https://github.com/fommil/netlib-java) documentation for your
platform's additional installation instructions.

To use MLlib in Python, you will need [NumPy](http://www.numpy.org) version 1.4 or newer.

[^1]: To learn more about the benefits and background of system optimised natives, you may wish to
    watch Sam Halliday's ScalaX talk on [High Performance Linear Algebra in Scala](http://fommil.github.io/scalax14/#/).

# Migration guide

MLlib is under active development.
The APIs marked `Experimental`/`DeveloperApi` may change in future releases,
and the migration guide below will explain all changes between releases.

## From 1.4 to 1.5

In the `spark.mllib` package, there are no break API changes but several behavior changes:

* [SPARK-9005](https://issues.apache.org/jira/browse/SPARK-9005):
  `RegressionMetrics.explainedVariance` returns the average regression sum of squares.
* [SPARK-8600](https://issues.apache.org/jira/browse/SPARK-8600): `NaiveBayesModel.labels` become
  sorted.
* [SPARK-3382](https://issues.apache.org/jira/browse/SPARK-3382): `GradientDescent` has a default
  convergence tolerance `1e-3`, and hence iterations might end earlier than 1.4.

In the `spark.ml` package, there exists one break API change and one behavior change:

* [SPARK-9268](https://issues.apache.org/jira/browse/SPARK-9268): Java's varargs support is removed
  from `Params.setDefault` due to a
  [Scala compiler bug](https://issues.scala-lang.org/browse/SI-9013).
* [SPARK-10097](https://issues.apache.org/jira/browse/SPARK-10097): `Evaluator.isLargerBetter` is
  added to indicate metric ordering. Metrics like RMSE no longer flip signs as in 1.4.

## Previous Spark versions

Earlier migration guides are archived [on this page](mllib-migration-guides.html).

---
