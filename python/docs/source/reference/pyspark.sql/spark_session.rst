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


=============
Spark Session
=============
.. currentmodule:: pyspark.sql

The entry point to programming Spark with the Dataset and DataFrame API.
To create a Spark session, you should use ``SparkSession.builder`` attribute.
See also :class:`SparkSession`.

.. autosummary::
    :toctree: api/

    SparkSession.active

.. autosummary::
    :toctree: api/
    :template: autosummary/accessor_method.rst

    SparkSession.builder.appName
    SparkSession.builder.config
    SparkSession.builder.enableHiveSupport
    SparkSession.builder.getOrCreate
    SparkSession.builder.master
    SparkSession.builder.remote

.. autosummary::
    :toctree: api/

    SparkSession.catalog
    SparkSession.conf
    SparkSession.createDataFrame
    SparkSession.dataSource
    SparkSession.getActiveSession
    SparkSession.newSession
    SparkSession.profile
    SparkSession.range
    SparkSession.read
    SparkSession.readStream
    SparkSession.sparkContext
    SparkSession.sql
    SparkSession.stop
    SparkSession.streams
    SparkSession.table
    SparkSession.udf
    SparkSession.udtf
    SparkSession.version
    is_remote

Spark Connect Only
------------------

.. autosummary::
    :toctree: api/
    :template: autosummary/accessor_method.rst

    SparkSession.builder.create

.. autosummary::
    :toctree: api/

    SparkSession.addArtifact
    SparkSession.addArtifacts
    SparkSession.addTag
    SparkSession.clearProgressHandlers
    SparkSession.clearTags
    SparkSession.client
    SparkSession.copyFromLocalToFs
    SparkSession.getTags
    SparkSession.interruptAll
    SparkSession.interruptOperation
    SparkSession.interruptTag
    SparkSession.registerProgressHandler
    SparkSession.removeProgressHandler
    SparkSession.removeTag
