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


===============================
Manipulation and Transformation
===============================
.. currentmodule:: pyspark.sql

.. autosummary::
    :toctree: api/

    DataFrame.agg
    DataFrame.alias
    DataFrame.colRegex
    DataFrame.cube
    DataFrame.distinct
    DataFrame.drop
    DataFrame.dropna
    DataFrame.dropDuplicates
    DataFrame.dropDuplicatesWithinWatermark
    DataFrame.drop_duplicates
    DataFrame.fillna
    DataFrame.filter
    DataFrame.foreach
    DataFrame.foreachPartition
    DataFrame.groupBy
    DataFrame.groupingSets
    DataFrame.limit
    DataFrame.mapInPandas
    DataFrame.mapInArrow
    DataFrame.melt
    DataFrame.na
    DataFrame.orderBy
    DataFrame.randomSplit
    DataFrame.replace
    DataFrame.rollup
    DataFrame.select
    DataFrame.selectExpr
    DataFrame.sort
    DataFrame.sortWithinPartitions
    DataFrame.transform
    DataFrame.unpivot
    DataFrame.where
    DataFrame.withColumn
    DataFrame.withColumns
    DataFrame.withColumnRenamed
    DataFrame.withColumnsRenamed
    DataFrame.withMetadata
    DataFrameNaFunctions.drop
    DataFrameNaFunctions.fill
    DataFrameNaFunctions.replace
