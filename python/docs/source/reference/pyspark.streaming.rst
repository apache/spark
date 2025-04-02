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


========================
Spark Streaming (Legacy)
========================

Core Classes
------------

.. currentmodule:: pyspark.streaming

.. autosummary::
    :toctree: api/

    StreamingContext
    DStream


Streaming Management
--------------------

.. currentmodule:: pyspark.streaming

.. autosummary::
    :toctree: api/

    StreamingContext.addStreamingListener
    StreamingContext.awaitTermination
    StreamingContext.awaitTerminationOrTimeout
    StreamingContext.checkpoint
    StreamingContext.getActive
    StreamingContext.getActiveOrCreate
    StreamingContext.getOrCreate
    StreamingContext.remember
    StreamingContext.sparkContext
    StreamingContext.start
    StreamingContext.stop
    StreamingContext.transform
    StreamingContext.union


Input and Output
----------------

.. autosummary::
    :toctree: api/

    StreamingContext.binaryRecordsStream
    StreamingContext.queueStream
    StreamingContext.socketTextStream
    StreamingContext.textFileStream
    DStream.pprint
    DStream.saveAsTextFiles


Transformations and Actions
---------------------------

.. currentmodule:: pyspark.streaming

.. autosummary::
    :toctree: api/

    DStream.cache
    DStream.checkpoint
    DStream.cogroup
    DStream.combineByKey
    DStream.context
    DStream.count
    DStream.countByValue
    DStream.countByValueAndWindow
    DStream.countByWindow
    DStream.filter
    DStream.flatMap
    DStream.flatMapValues
    DStream.foreachRDD
    DStream.fullOuterJoin
    DStream.glom
    DStream.groupByKey
    DStream.groupByKeyAndWindow
    DStream.join
    DStream.leftOuterJoin
    DStream.map
    DStream.mapPartitions
    DStream.mapPartitionsWithIndex
    DStream.mapValues
    DStream.partitionBy
    DStream.persist
    DStream.reduce
    DStream.reduceByKey
    DStream.reduceByKeyAndWindow
    DStream.reduceByWindow
    DStream.repartition
    DStream.rightOuterJoin
    DStream.slice
    DStream.transform
    DStream.transformWith
    DStream.union
    DStream.updateStateByKey
    DStream.window


Kinesis
-------

.. currentmodule:: pyspark.streaming.kinesis

.. autosummary::
    :toctree: api/

    KinesisUtils.createStream
    InitialPositionInStream.LATEST
    InitialPositionInStream.TRIM_HORIZON

