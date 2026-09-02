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
* MLlib added features to the DataFrame-based API in the Spark 2.x releases, reaching feature parity with the RDD-based API.

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

MLlib uses linear algebra packages [Breeze](http://www.scalanlp.org/) and [dev.ludovic.netlib](https://github.com/luhenry/netlib) for optimised numerical processing[^1]. Those packages may call native acceleration libraries such as [Intel MKL](https://software.intel.com/content/www/us/en/develop/tools/math-kernel-library.html) or [OpenBLAS](http://www.openblas.net) if they are available as system libraries or in runtime library paths.

However, native acceleration libraries can't be distributed with Spark. See [MLlib Linear Algebra Acceleration Guide](ml-linalg-guide.html) for how to enable accelerated linear algebra processing. If accelerated native libraries are not enabled, you will see a warning message like below and a pure JVM implementation will be used instead:
```
WARNING: Failed to load implementation from:dev.ludovic.netlib.blas.JNIBLAS
```

To use MLlib in Python, you will need [NumPy](http://www.numpy.org) version 1.23.2 or newer.

[^1]: To learn more about the benefits and background of system optimised natives, you may wish to
    watch Sam Halliday's ScalaX talk on [High Performance Linear Algebra in Scala](http://fommil.github.io/scalax14/).

# Migration Guide

The migration guide is now archived [on this page](ml-migration-guide.html).
