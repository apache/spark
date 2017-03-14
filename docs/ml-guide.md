---
layout: global
title: "MLlib: Main Guide"
displayTitle: "Machine Learning Library (MLlib) Guide"
---

MLlib is Spark's machine learning (ML) library.
Its goal is to make practical machine learning scalable and easy.
At a high level, it provides tools such as:

* ML Algorithms: common learning algorithms such as classification, regression, clustering, and collaborative filtering
* Featurization: feature extraction, transformation, dimensionality reduction, and selection
* Pipelines: tools for constructing, evaluating, and tuning ML Pipelines
* Persistence: saving and load algorithms, models, and Pipelines
* Utilities: linear algebra, statistics, data handling, etc.

# Announcement: DataFrame-based API is primary API

**The MLlib RDD-based API is now in maintenance mode.**

As of Spark 2.0, the [RDD](programming-guide.html#resilient-distributed-datasets-rdds)-based APIs in the `spark.mllib` package have entered maintenance mode.
The primary Machine Learning API for Spark is now the [DataFrame](sql-programming-guide.html)-based API in the `spark.ml` package.

*What are the implications?*

* MLlib will still support the RDD-based API in `spark.mllib` with bug fixes.
* MLlib will not add new features to the RDD-based API.
* In the Spark 2.x releases, MLlib will add features to the DataFrames-based API to reach feature parity with the RDD-based API.
* After reaching feature parity (roughly estimated for Spark 2.2), the RDD-based API will be deprecated.
* The RDD-based API is expected to be removed in Spark 3.0.

*Why is MLlib switching to the DataFrame-based API?*

* DataFrames provide a more user-friendly API than RDDs.  The many benefits of DataFrames include Spark Datasources, SQL/DataFrame queries, Tungsten and Catalyst optimizations, and uniform APIs across languages.
* The DataFrame-based API for MLlib provides a uniform API across ML algorithms and across multiple languages.
* DataFrames facilitate practical ML Pipelines, particularly feature transformations.  See the [Pipelines guide](ml-pipeline.html) for details.

*What is "Spark ML"?*

* "Spark ML" is not an official name but occasionally used to refer to the MLlib DataFrame-based API.
  This is majorly due to the `org.apache.spark.ml` Scala package name used by the DataFrame-based API, 
  and the "Spark ML Pipelines" term we used initially to emphasize the pipeline concept.
  
*Is MLlib deprecated?*

* No. MLlib includes both the RDD-based API and the DataFrame-based API.
  The RDD-based API is now in maintenance mode.
  But neither API is deprecated, nor MLlib as a whole.

# Dependencies

MLlib uses the linear algebra package [Breeze](http://www.scalanlp.org/), which depends on
[netlib-java](https://github.com/fommil/netlib-java) for optimised numerical processing.
If native libraries[^1] are not available at runtime, you will see a warning message and a pure JVM
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

## Previous Spark versions

Earlier migration guides are archived [on this page](ml-migration-guides.html).

---
