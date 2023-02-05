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


==========
Spark Core
==========

Public Classes
--------------

.. currentmodule:: pyspark

.. autosummary::
    :toctree: api/

    SparkContext
    RDD
    Broadcast
    Accumulator
    AccumulatorParam
    SparkConf
    SparkFiles
    StorageLevel
    TaskContext
    RDDBarrier
    BarrierTaskContext
    BarrierTaskInfo
    InheritableThread
    util.VersionUtils

Spark Context APIs
------------------

.. currentmodule:: pyspark

.. autosummary::
    :toctree: api/

    SparkContext.PACKAGE_EXTENSIONS
    SparkContext.accumulator
    SparkContext.addArchive
    SparkContext.addFile
    SparkContext.addPyFile
    SparkContext.applicationId
    SparkContext.binaryFiles
    SparkContext.binaryRecords
    SparkContext.broadcast
    SparkContext.cancelAllJobs
    SparkContext.cancelJobGroup
    SparkContext.defaultMinPartitions
    SparkContext.defaultParallelism
    SparkContext.dump_profiles
    SparkContext.emptyRDD
    SparkContext.getCheckpointDir
    SparkContext.getConf
    SparkContext.getLocalProperty
    SparkContext.getOrCreate
    SparkContext.hadoopFile
    SparkContext.hadoopRDD
    SparkContext.listArchives
    SparkContext.listFiles
    SparkContext.newAPIHadoopFile
    SparkContext.newAPIHadoopRDD
    SparkContext.parallelize
    SparkContext.pickleFile
    SparkContext.range
    SparkContext.resources
    SparkContext.runJob
    SparkContext.sequenceFile
    SparkContext.setCheckpointDir
    SparkContext.setJobDescription
    SparkContext.setJobGroup
    SparkContext.setLocalProperty
    SparkContext.setLogLevel
    SparkContext.setSystemProperty
    SparkContext.show_profiles
    SparkContext.sparkUser
    SparkContext.startTime
    SparkContext.statusTracker
    SparkContext.stop
    SparkContext.textFile
    SparkContext.uiWebUrl
    SparkContext.union
    SparkContext.version
    SparkContext.wholeTextFiles


RDD APIs
--------

.. currentmodule:: pyspark

.. autosummary::
    :toctree: api/

    RDD.aggregate
    RDD.aggregateByKey
    RDD.barrier
    RDD.cache
    RDD.cartesian
    RDD.checkpoint
    RDD.cleanShuffleDependencies
    RDD.coalesce
    RDD.cogroup
    RDD.collect
    RDD.collectAsMap
    RDD.collectWithJobGroup
    RDD.combineByKey
    RDD.context
    RDD.count
    RDD.countApprox
    RDD.countApproxDistinct
    RDD.countByKey
    RDD.countByValue
    RDD.distinct
    RDD.filter
    RDD.first
    RDD.flatMap
    RDD.flatMapValues
    RDD.fold
    RDD.foldByKey
    RDD.foreach
    RDD.foreachPartition
    RDD.fullOuterJoin
    RDD.getCheckpointFile
    RDD.getNumPartitions
    RDD.getResourceProfile
    RDD.getStorageLevel
    RDD.glom
    RDD.groupBy
    RDD.groupByKey
    RDD.groupWith
    RDD.histogram
    RDD.id
    RDD.intersection
    RDD.isCheckpointed
    RDD.isEmpty
    RDD.isLocallyCheckpointed
    RDD.join
    RDD.keyBy
    RDD.keys
    RDD.leftOuterJoin
    RDD.localCheckpoint
    RDD.lookup
    RDD.map
    RDD.mapPartitions
    RDD.mapPartitionsWithIndex
    RDD.mapPartitionsWithSplit
    RDD.mapValues
    RDD.max
    RDD.mean
    RDD.meanApprox
    RDD.min
    RDD.name
    RDD.partitionBy
    RDD.persist
    RDD.pipe
    RDD.randomSplit
    RDD.reduce
    RDD.reduceByKey
    RDD.reduceByKeyLocally
    RDD.repartition
    RDD.repartitionAndSortWithinPartitions
    RDD.rightOuterJoin
    RDD.sample
    RDD.sampleByKey
    RDD.sampleStdev
    RDD.sampleVariance
    RDD.saveAsHadoopDataset
    RDD.saveAsHadoopFile
    RDD.saveAsNewAPIHadoopDataset
    RDD.saveAsNewAPIHadoopFile
    RDD.saveAsPickleFile
    RDD.saveAsSequenceFile
    RDD.saveAsTextFile
    RDD.setName
    RDD.sortBy
    RDD.sortByKey
    RDD.stats
    RDD.stdev
    RDD.subtract
    RDD.subtractByKey
    RDD.sum
    RDD.sumApprox
    RDD.take
    RDD.takeOrdered
    RDD.takeSample
    RDD.toDebugString
    RDD.toLocalIterator
    RDD.top
    RDD.treeAggregate
    RDD.treeReduce
    RDD.union
    RDD.unpersist
    RDD.values
    RDD.variance
    RDD.withResources
    RDD.zip
    RDD.zipWithIndex
    RDD.zipWithUniqueId


Broadcast and Accumulator
-------------------------

.. currentmodule:: pyspark

.. autosummary::
    :toctree: api/

    Broadcast.destroy
    Broadcast.dump
    Broadcast.load
    Broadcast.load_from_path
    Broadcast.unpersist
    Broadcast.value
    Accumulator.add
    Accumulator.value
    AccumulatorParam.addInPlace
    AccumulatorParam.zero


Management
----------

.. currentmodule:: pyspark

.. autosummary::
    :toctree: api/

    inheritable_thread_target
    SparkConf.contains
    SparkConf.get
    SparkConf.getAll
    SparkConf.set
    SparkConf.setAll
    SparkConf.setAppName
    SparkConf.setExecutorEnv
    SparkConf.setIfMissing
    SparkConf.setMaster
    SparkConf.setSparkHome
    SparkConf.toDebugString
    SparkFiles.get
    SparkFiles.getRootDirectory
    StorageLevel.DISK_ONLY
    StorageLevel.DISK_ONLY_2
    StorageLevel.DISK_ONLY_3
    StorageLevel.MEMORY_AND_DISK
    StorageLevel.MEMORY_AND_DISK_2
    StorageLevel.MEMORY_AND_DISK_DESER
    StorageLevel.MEMORY_ONLY
    StorageLevel.MEMORY_ONLY_2
    StorageLevel.OFF_HEAP
    TaskContext.attemptNumber
    TaskContext.cpus
    TaskContext.get
    TaskContext.getLocalProperty
    TaskContext.partitionId
    TaskContext.resources
    TaskContext.stageId
    TaskContext.taskAttemptId
    RDDBarrier.mapPartitions
    RDDBarrier.mapPartitionsWithIndex
    BarrierTaskContext.allGather
    BarrierTaskContext.attemptNumber
    BarrierTaskContext.barrier
    BarrierTaskContext.cpus
    BarrierTaskContext.get
    BarrierTaskContext.getLocalProperty
    BarrierTaskContext.getTaskInfos
    BarrierTaskContext.partitionId
    BarrierTaskContext.resources
    BarrierTaskContext.stageId
    BarrierTaskContext.taskAttemptId
    util.VersionUtils.majorMinorVersion

