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


====================
Legacy Entry Points
====================
.. currentmodule:: pyspark.sql

:class:`SQLContext` was the primary entry point for Spark SQL in Spark 1.x.
As of Spark 2.0, it has been replaced by :class:`SparkSession`.
These classes are retained for backward compatibility only.

.. deprecated:: 3.0.0
    Use :func:`SparkSession.builder.getOrCreate` instead.

.. note::
    Under Spark Connect, :meth:`SQLContext.registerJavaFunction` and the whole
    :class:`HiveContext` are not supported and raise
    :class:`~pyspark.errors.PySparkNotImplementedError`,
    since they rely on a JVM ``SparkContext`` that does not exist in Connect mode.

SQLContext
----------

.. autosummary::
    :toctree: api/

    SQLContext

.. autosummary::
    :toctree: api/

    SQLContext.getOrCreate
    SQLContext.newSession
    SQLContext.setConf
    SQLContext.getConf
    SQLContext.udf
    SQLContext.udtf
    SQLContext.range
    SQLContext.registerFunction
    SQLContext.registerJavaFunction
    SQLContext.createDataFrame
    SQLContext.registerDataFrameAsTable
    SQLContext.dropTempTable
    SQLContext.createExternalTable
    SQLContext.sql
    SQLContext.table
    SQLContext.tables
    SQLContext.tableNames
    SQLContext.cacheTable
    SQLContext.uncacheTable
    SQLContext.clearCache
    SQLContext.read
    SQLContext.readStream
    SQLContext.streams

HiveContext
-----------

.. autosummary::
    :toctree: api/

    HiveContext
