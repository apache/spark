..  Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

..  Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.


=========
DataFrame
=========

.. currentmodule:: pyspark.sql

.. autosummary::
    :toctree: api/

    DataFrame.__getattr__
    DataFrame.__getitem__
    DataFrame.agg
    DataFrame.alias
    DataFrame.approxQuantile
    DataFrame.asTable
    DataFrame.cache
    DataFrame.checkpoint
    DataFrame.coalesce
    DataFrame.colRegex
    DataFrame.collect
    DataFrame.columns
    DataFrame.corr
    DataFrame.count
    DataFrame.cov
    DataFrame.createGlobalTempView
    DataFrame.createOrReplaceGlobalTempView
    DataFrame.createOrReplaceTempView
    DataFrame.createTempView
    DataFrame.crossJoin
    DataFrame.crosstab
    DataFrame.cube
    DataFrame.describe
    DataFrame.distinct
    DataFrame.drop
    DataFrame.dropDuplicates
    DataFrame.dropDuplicatesWithinWatermark
    DataFrame.drop_duplicates
    DataFrame.dropna
    DataFrame.dtypes
    DataFrame.exceptAll
    DataFrame.executionInfo
    DataFrame.exists
    DataFrame.explain
    DataFrame.fillna
    DataFrame.filter
    DataFrame.first
    DataFrame.foreach
    DataFrame.foreachPartition
    DataFrame.freqItems
    DataFrame.groupBy
    DataFrame.groupingSets
    DataFrame.head
    DataFrame.hint
    DataFrame.inputFiles
    DataFrame.intersect
    DataFrame.intersectAll
    DataFrame.isEmpty
    DataFrame.isLocal
    DataFrame.isStreaming
    DataFrame.join
    DataFrame.limit
    DataFrame.lateralJoin
    DataFrame.localCheckpoint
    DataFrame.mapInPandas
    DataFrame.mapInArrow
    DataFrame.metadataColumn
    DataFrame.melt
    DataFrame.na
    DataFrame.observe
    DataFrame.offset
    DataFrame.orderBy
    DataFrame.persist
    DataFrame.plot
    DataFrame.printSchema
    DataFrame.randomSplit
    DataFrame.rdd
    DataFrame.registerTempTable
    DataFrame.repartition
    DataFrame.repartitionByRange
    DataFrame.replace
    DataFrame.rollup
    DataFrame.sameSemantics
    DataFrame.sample
    DataFrame.sampleBy
    DataFrame.scalar
    DataFrame.schema
    DataFrame.select
    DataFrame.selectExpr
    DataFrame.semanticHash
    DataFrame.show
    DataFrame.sort
    DataFrame.sortWithinPartitions
    DataFrame.sparkSession
    DataFrame.stat
    DataFrame.storageLevel
    DataFrame.subtract
    DataFrame.summary
    DataFrame.tail
    DataFrame.take
    DataFrame.to
    DataFrame.toArrow
    DataFrame.toDF
    DataFrame.toJSON
    DataFrame.toLocalIterator
    DataFrame.toPandas
    DataFrame.transform
    DataFrame.transpose
    DataFrame.union
    DataFrame.unionAll
    DataFrame.unionByName
    DataFrame.unpersist
    DataFrame.unpivot
    DataFrame.where
    DataFrame.withColumn
    DataFrame.withColumns
    DataFrame.withColumnRenamed
    DataFrame.withColumnsRenamed
    DataFrame.withMetadata
    DataFrame.withWatermark
    DataFrame.write
    DataFrame.writeStream
    DataFrame.writeTo
    DataFrame.mergeInto
    DataFrame.pandas_api
    DataFrameNaFunctions.drop
    DataFrameNaFunctions.fill
    DataFrameNaFunctions.replace
    DataFrameStatFunctions.approxQuantile
    DataFrameStatFunctions.corr
    DataFrameStatFunctions.cov
    DataFrameStatFunctions.crosstab
    DataFrameStatFunctions.freqItems
    DataFrameStatFunctions.sampleBy


Table Argument
--------------
``DataFrame.asTable`` returns a table argument in PySpark.

This class provides methods to specify partitioning, ordering, and single-partition constraints
when passing a DataFrame as a table argument to TVF(Table-Valued Function)s including
UDTF(User-Defined Table Function)s.

.. currentmodule:: pyspark.sql.table_arg

.. autosummary::
    :toctree: api/

    TableArg.partitionBy
    TableArg.orderBy
    TableArg.withSinglePartition


Plotting
--------
The ``DataFrame.plot`` attribute serves both as a callable method and a namespace, providing access to various
plotting functions via the ``PySparkPlotAccessor``. Users can call specific plotting methods in the format
``DataFrame.plot.<kind>``.

.. currentmodule:: pyspark.sql.plot.core

.. autosummary::
    :toctree: api/

    PySparkPlotAccessor.area
    PySparkPlotAccessor.bar
    PySparkPlotAccessor.barh
    PySparkPlotAccessor.line
    PySparkPlotAccessor.pie
    PySparkPlotAccessor.scatter
    PySparkPlotAccessor.box
    PySparkPlotAccessor.kde
    PySparkPlotAccessor.hist