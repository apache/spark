---
layout: global
title: "MLlib: Main Guide"
displayTitle: "Machine Learning Library (MLlib) Guide"
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

As of Spark 2.0, the [RDD](rdd-programming-guide.html#resilient-distributed-datasets-rdds)-based APIs in the `spark.mllib` package have entered maintenance mode.
The primary Machine Learning API for Spark is now the [DataFrame](sql-programming-guide.html)-based API in the `spark.ml` package.

*What are the implications?*

* MLlib will still support the RDD-based API in `spark.mllib` with bug fixes.
* MLlib will not add new features to the RDD-based API.
* In the Spark 2.x releases, MLlib will add features to the DataFrames-based API to reach feature parity with the RDD-based API.
* After reaching feature parity (roughly estimated for Spark 2.3), the RDD-based API will be deprecated.
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

The most popular native BLAS such as [Intel MKL](https://software.intel.com/en-us/mkl), [OpenBLAS](http://www.openblas.net), can use multiple threads in a single operation, which can conflict with Spark's execution model.

Configuring these BLAS implementations to use a single thread for operations may actually improve performance (see [SPARK-21305](https://issues.apache.org/jira/browse/SPARK-21305)). It is usually optimal to match this to the number of cores each Spark task is configured to use, which is 1 by default and typically left at 1.

Please refer to resources like the following to understand how to configure the number of threads these BLAS implementations use: [Intel MKL](https://software.intel.com/en-us/articles/recommended-settings-for-calling-intel-mkl-routines-from-multi-threaded-applications) and [OpenBLAS](https://github.com/xianyi/OpenBLAS/wiki/faq#multi-threaded).

To use MLlib in Python, you will need [NumPy](http://www.numpy.org) version 1.4 or newer.

[^1]: To learn more about the benefits and background of system optimised natives, you may wish to
    watch Sam Halliday's ScalaX talk on [High Performance Linear Algebra in Scala](http://fommil.github.io/scalax14/#/).

# Highlights in 2.3

The list below highlights some of the new features and enhancements added to MLlib in the `2.3`
release of Spark:

* Built-in support for reading images into a `DataFrame` was added
([SPARK-21866](https://issues.apache.org/jira/browse/SPARK-21866)).
* [`OneHotEncoderEstimator`](ml-features.html#onehotencoderestimator) was added, and should be
used instead of the existing `OneHotEncoder` transformer. The new estimator supports
transforming multiple columns.
* Multiple column support was also added to `QuantileDiscretizer` and `Bucketizer`
([SPARK-22397](https://issues.apache.org/jira/browse/SPARK-22397) and
[SPARK-20542](https://issues.apache.org/jira/browse/SPARK-20542))
* A new [`FeatureHasher`](ml-features.html#featurehasher) transformer was added
 ([SPARK-13969](https://issues.apache.org/jira/browse/SPARK-13969)).
* Added support for evaluating multiple models in parallel when performing cross-validation using
[`TrainValidationSplit` or `CrossValidator`](ml-tuning.html)
([SPARK-19357](https://issues.apache.org/jira/browse/SPARK-19357)).
* Improved support for custom pipeline components in Python (see
[SPARK-21633](https://issues.apache.org/jira/browse/SPARK-21633) and 
[SPARK-21542](https://issues.apache.org/jira/browse/SPARK-21542)).
* `DataFrame` functions for descriptive summary statistics over vector columns
([SPARK-19634](https://issues.apache.org/jira/browse/SPARK-19634)).
* Robust linear regression with Huber loss
([SPARK-3181](https://issues.apache.org/jira/browse/SPARK-3181)).

# Migration guide

MLlib is under active development.
The APIs marked `Experimental`/`DeveloperApi` may change in future releases,
and the migration guide below will explain all changes between releases.

## From 2.4 to 3.0

### Breaking changes

* `OneHotEncoder` which is deprecated in 2.3, is removed in 3.0 and `OneHotEncoderEstimator` is now renamed to `OneHotEncoder`.

### Changes of behavior

* [SPARK-11215](https://issues.apache.org/jira/browse/SPARK-11215):
 In Spark 2.4 and previous versions, when specifying `frequencyDesc` or `frequencyAsc` as
 `stringOrderType` param in `StringIndexer`, in case of equal frequency, the order of
 strings is undefined. Since Spark 3.0, the strings with equal frequency are further
 sorted by alphabet. And since Spark 3.0, `StringIndexer` supports encoding multiple
 columns.

## From 2.2 to 2.3

### Breaking changes

* The class and trait hierarchy for logistic regression model summaries was changed to be cleaner
and better accommodate the addition of the multi-class summary. This is a breaking change for user
code that casts a `LogisticRegressionTrainingSummary` to a
`BinaryLogisticRegressionTrainingSummary`. Users should instead use the `model.binarySummary`
method. See [SPARK-17139](https://issues.apache.org/jira/browse/SPARK-17139) for more detail 
(_note_ this is an `Experimental` API). This _does not_ affect the Python `summary` method, which
will still work correctly for both multinomial and binary cases.

### Deprecations and changes of behavior

**Deprecations**

* `OneHotEncoder` has been deprecated and will be removed in `3.0`. It has been replaced by the
new [`OneHotEncoderEstimator`](ml-features.html#onehotencoderestimator)
(see [SPARK-13030](https://issues.apache.org/jira/browse/SPARK-13030)). **Note** that
`OneHotEncoderEstimator` will be renamed to `OneHotEncoder` in `3.0` (but
`OneHotEncoderEstimator` will be kept as an alias).

**Changes of behavior**

* [SPARK-21027](https://issues.apache.org/jira/browse/SPARK-21027):
 The default parallelism used in `OneVsRest` is now set to 1 (i.e. serial). In `2.2` and
 earlier versions, the level of parallelism was set to the default threadpool size in Scala.
* [SPARK-22156](https://issues.apache.org/jira/browse/SPARK-22156):
 The learning rate update for `Word2Vec` was incorrect when `numIterations` was set greater than
 `1`. This will cause training results to be different between `2.3` and earlier versions.
* [SPARK-21681](https://issues.apache.org/jira/browse/SPARK-21681):
 Fixed an edge case bug in multinomial logistic regression that resulted in incorrect coefficients
 when some features had zero variance.
* [SPARK-16957](https://issues.apache.org/jira/browse/SPARK-16957):
 Tree algorithms now use mid-points for split values. This may change results from model training.
* [SPARK-14657](https://issues.apache.org/jira/browse/SPARK-14657):
 Fixed an issue where the features generated by `RFormula` without an intercept were inconsistent
 with the output in R. This may change results from model training in this scenario.
  
## Previous Spark versions

Earlier migration guides are archived [on this page](ml-migration-guides.html).

---
