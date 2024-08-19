---
layout: global
title: "Migration Guide: SparkR (R on Spark)"
displayTitle: "Migration Guide: SparkR (R on Spark)"
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

* Table of contents
{:toc}

Note that this migration guide describes the items specific to SparkR.
Many items of SQL migration can be applied when migrating SparkR to higher versions.
Please refer [Migration Guide: SQL, Datasets and DataFrame](sql-migration-guide.html).

## Upgrading from SparkR 3.1 to 3.2

 - Previously, SparkR automatically downloaded and installed the Spark distribution in user's cache directory to complete SparkR installation when SparkR runs in a plain R shell or Rscript, and the Spark distribution cannot be found. Now, it asks if users want to download and install or not. To restore the previous behavior, set `SPARKR_ASK_INSTALLATION` environment variable to `FALSE`.

## Upgrading from SparkR 2.4 to 3.0

 - The deprecated methods `parquetFile`, `saveAsParquetFile`, `jsonFile`, `jsonRDD` have been removed. Use `read.parquet`, `write.parquet`, `read.json` instead.

## Upgrading from SparkR 2.3 to 2.4

 - Previously, we don't check the validity of the size of the last layer in `spark.mlp`. For example, if the training data only has two labels, a `layers` param like `c(1, 3)` doesn't cause an error previously, now it does.

## Upgrading from SparkR 2.3 to 2.3.1 and above

 - In SparkR 2.3.0 and earlier, the `start` parameter of `substr` method was wrongly subtracted by one and considered as 0-based. This can lead to inconsistent substring results and also does not match with the behaviour with `substr` in R. In version 2.3.1 and later, it has been fixed so the `start` parameter of `substr` method is now 1-based. As an example, `substr(lit('abcdef'), 2, 4))` would result to `abc` in SparkR 2.3.0, and the result would be `bcd` in SparkR 2.3.1.

## Upgrading from SparkR 2.2 to 2.3

 - The `stringsAsFactors` parameter was previously ignored with `collect`, for example, in `collect(createDataFrame(iris), stringsAsFactors = TRUE))`. It has been corrected.
 - For `summary`, option for statistics to compute has been added. Its output is changed from that from `describe`.
 - A warning can be raised if versions of SparkR package and the Spark JVM do not match.

## Upgrading from SparkR 2.1 to 2.2

 - A `numPartitions` parameter has been added to `createDataFrame` and `as.DataFrame`. When splitting the data, the partition position calculation has been made to match the one in Scala.
 - The method `createExternalTable` has been deprecated to be replaced by `createTable`. Either methods can be called to create external or managed table. Additional catalog methods have also been added.
 - By default, derby.log is now saved to `tempdir()`. This will be created when instantiating the SparkSession with `enableHiveSupport` set to `TRUE`.
 - `spark.lda` was not setting the optimizer correctly. It has been corrected.
 - Several model summary outputs are updated to have `coefficients` as `matrix`. This includes `spark.logit`, `spark.kmeans`, `spark.glm`. Model summary outputs for `spark.gaussianMixture` have added log-likelihood as `loglik`.

## Upgrading from SparkR 2.0 to 3.1

 - `join` no longer performs Cartesian Product by default, use `crossJoin` instead.


## Upgrading from SparkR 1.6 to 2.0

 - The method `table` has been removed and replaced by `tableToDF`.
 - The class `DataFrame` has been renamed to `SparkDataFrame` to avoid name conflicts.
 - Spark's `SQLContext` and `HiveContext` have been deprecated to be replaced by `SparkSession`. Instead of `sparkR.init()`, call `sparkR.session()` in its place to instantiate the SparkSession. Once that is done, that currently active SparkSession will be used for SparkDataFrame operations.
 - The parameter `sparkExecutorEnv` is not supported by `sparkR.session`. To set environment for the executors, set Spark config properties with the prefix "spark.executorEnv.VAR_NAME", for example, "spark.executorEnv.PATH"
 - The `sqlContext` parameter is no longer required for these functions: `createDataFrame`, `as.DataFrame`, `read.json`, `jsonFile`, `read.parquet`, `parquetFile`, `read.text`, `sql`, `tables`, `tableNames`, `cacheTable`, `uncacheTable`, `clearCache`, `dropTempTable`, `read.df`, `loadDF`, `createExternalTable`.
 - The method `registerTempTable` has been deprecated to be replaced by `createOrReplaceTempView`.
 - The method `dropTempTable` has been deprecated to be replaced by `dropTempView`.
 - The `sc` SparkContext parameter is no longer required for these functions: `setJobGroup`, `clearJobGroup`, `cancelJobGroup`

## Upgrading from SparkR 1.5 to 1.6

 - Before Spark 1.6.0, the default mode for writes was `append`. It was changed in Spark 1.6.0 to `error` to match the Scala API.
 - SparkSQL converts `NA` in R to `null` and vice-versa.
 - Since 1.6.1, withColumn method in SparkR supports adding a new column to or replacing existing columns
   of the same name of a DataFrame.
