/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.status.protobuf

import java.util.Date

import org.apache.spark.{JobExecutionStatus, SparkFunSuite}
import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.metrics.ExecutorMetricType
import org.apache.spark.resource.{ExecutorResourceRequest, TaskResourceRequest}
import org.apache.spark.status._
import org.apache.spark.status.api.v1._

class KVStoreProtobufSerializerSuite extends SparkFunSuite {
  private val serializer = new KVStoreProtobufSerializer()

  test("Job data") {
    val input = new JobDataWrapper(
      new JobData(
        jobId = 1,
        name = "test",
        description = Some("test description"),
        submissionTime = Some(new Date(123456L)),
        completionTime = Some(new Date(654321L)),
        stageIds = Seq(1, 2, 3, 4),
        jobGroup = Some("group"),
        status = JobExecutionStatus.UNKNOWN,
        numTasks = 2,
        numActiveTasks = 3,
        numCompletedTasks = 4,
        numSkippedTasks = 5,
        numFailedTasks = 6,
        numKilledTasks = 7,
        numCompletedIndices = 8,
        numActiveStages = 9,
        numCompletedStages = 10,
        numSkippedStages = 11,
        numFailedStages = 12,
        killedTasksSummary = Map("a" -> 1, "b" -> 2)),
      Set(1, 2),
      Some(999)
    )

    val bytes = serializer.serialize(input)
    val result = serializer.deserialize(bytes, classOf[JobDataWrapper])
    assert(result.info.jobId == input.info.jobId)
    assert(result.info.description == input.info.description)
    assert(result.info.submissionTime == input.info.submissionTime)
    assert(result.info.completionTime == input.info.completionTime)
    assert(result.info.stageIds == input.info.stageIds)
    assert(result.info.jobGroup == input.info.jobGroup)
    assert(result.info.status == input.info.status)
    assert(result.info.numTasks == input.info.numTasks)
    assert(result.info.numActiveTasks == input.info.numActiveTasks)
    assert(result.info.numCompletedTasks == input.info.numCompletedTasks)
    assert(result.info.numSkippedTasks == input.info.numSkippedTasks)
    assert(result.info.numFailedTasks == input.info.numFailedTasks)
    assert(result.info.numKilledTasks == input.info.numKilledTasks)
    assert(result.info.numCompletedIndices == input.info.numCompletedIndices)
    assert(result.info.numActiveStages == input.info.numActiveStages)
    assert(result.info.numCompletedStages == input.info.numCompletedStages)
    assert(result.info.numSkippedStages == input.info.numSkippedStages)
    assert(result.info.numFailedStages == input.info.numFailedStages)
    assert(result.info.killedTasksSummary == input.info.killedTasksSummary)
    assert(result.skippedStages == input.skippedStages)
    assert(result.sqlExecutionId == input.sqlExecutionId)
  }

  test("Task Data") {
    val accumulatorUpdates = Seq(
      new AccumulableInfo(1L, "duration", Some("update"), "value1"),
      new AccumulableInfo(2L, "duration2", None, "value2")
    )
    val input = new TaskDataWrapper(
      taskId = 1,
      index = 2,
      attempt = 3,
      partitionId = 4,
      launchTime = 5L,
      resultFetchStart = 6L,
      duration = 10000L,
      executorId = "executor_id_1",
      host = "host_name",
      status = "SUCCESS",
      taskLocality = "LOCAL",
      speculative = true,
      accumulatorUpdates = accumulatorUpdates,
      errorMessage = Some("error"),
      hasMetrics = true,
      executorDeserializeTime = 7L,
      executorDeserializeCpuTime = 8L,
      executorRunTime = 9L,
      executorCpuTime = 10L,
      resultSize = 11L,
      jvmGcTime = 12L,
      resultSerializationTime = 13L,
      memoryBytesSpilled = 14L,
      diskBytesSpilled = 15L,
      peakExecutionMemory = 16L,
      inputBytesRead = 17L,
      inputRecordsRead = 18L,
      outputBytesWritten = 19L,
      outputRecordsWritten = 20L,
      shuffleRemoteBlocksFetched = 21L,
      shuffleLocalBlocksFetched = 22L,
      shuffleFetchWaitTime = 23L,
      shuffleRemoteBytesRead = 24L,
      shuffleRemoteBytesReadToDisk = 25L,
      shuffleLocalBytesRead = 26L,
      shuffleRecordsRead = 27L,
      shuffleBytesWritten = 28L,
      shuffleWriteTime = 29L,
      shuffleRecordsWritten = 30L,
      stageId = 31,
      stageAttemptId = 32)

    val bytes = serializer.serialize(input)
    val result = serializer.deserialize(bytes, classOf[TaskDataWrapper])
    assert(result.accumulatorUpdates.length == input.accumulatorUpdates.length)
    result.accumulatorUpdates.zip(input.accumulatorUpdates).foreach { case (a1, a2) =>
      assert(a1.id == a2.id)
      assert(a1.name == a2.name)
      assert(a1.update.getOrElse("") == a2.update.getOrElse(""))
      assert(a1.update == a2.update)
    }
    assert(result.taskId == input.taskId)
    assert(result.index == input.index)
    assert(result.attempt == input.attempt)
    assert(result.partitionId == input.partitionId)
    assert(result.launchTime == input.launchTime)
    assert(result.resultFetchStart == input.resultFetchStart)
    assert(result.duration == input.duration)
    assert(result.executorId == input.executorId)
    assert(result.host == input.host)
    assert(result.status == input.status)
    assert(result.taskLocality == input.taskLocality)
    assert(result.speculative == input.speculative)
    assert(result.errorMessage == input.errorMessage)
    assert(result.hasMetrics == input.hasMetrics)
    assert(result.executorDeserializeTime == input.executorDeserializeTime)
    assert(result.executorDeserializeCpuTime == input.executorDeserializeCpuTime)
    assert(result.executorRunTime == input.executorRunTime)
    assert(result.executorCpuTime == input.executorCpuTime)
    assert(result.resultSize == input.resultSize)
    assert(result.jvmGcTime == input.jvmGcTime)
    assert(result.resultSerializationTime == input.resultSerializationTime)
    assert(result.memoryBytesSpilled == input.memoryBytesSpilled)
    assert(result.diskBytesSpilled == input.diskBytesSpilled)
    assert(result.peakExecutionMemory == input.peakExecutionMemory)
    assert(result.inputBytesRead == input.inputBytesRead)
    assert(result.inputRecordsRead == input.inputRecordsRead)
    assert(result.outputBytesWritten == input.outputBytesWritten)
    assert(result.outputRecordsWritten == input.outputRecordsWritten)
    assert(result.shuffleRemoteBlocksFetched == input.shuffleRemoteBlocksFetched)
    assert(result.shuffleLocalBlocksFetched == input.shuffleLocalBlocksFetched)
    assert(result.shuffleFetchWaitTime == input.shuffleFetchWaitTime)
    assert(result.shuffleRemoteBytesRead == input.shuffleRemoteBytesRead)
    assert(result.shuffleRemoteBytesReadToDisk == input.shuffleRemoteBytesReadToDisk)
    assert(result.shuffleLocalBytesRead == input.shuffleLocalBytesRead)
    assert(result.shuffleRecordsRead == input.shuffleRecordsRead)
    assert(result.shuffleBytesWritten == input.shuffleBytesWritten)
    assert(result.shuffleWriteTime == input.shuffleWriteTime)
    assert(result.shuffleRecordsWritten == input.shuffleRecordsWritten)
    assert(result.stageId == input.stageId)
    assert(result.stageAttemptId == input.stageAttemptId)
  }

  test("Executor Stage Summary") {
    val peakMemoryMetrics =
      Some(new ExecutorMetrics(Array(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 1024L)))
    val info = new ExecutorStageSummary(
      taskTime = 1L,
      failedTasks = 2,
      succeededTasks = 3,
      killedTasks = 4,
      inputBytes = 5L,
      inputRecords = 6L,
      outputBytes = 7L,
      outputRecords = 8L,
      shuffleRead = 9L,
      shuffleReadRecords = 10L,
      shuffleWrite = 11L,
      shuffleWriteRecords = 12L,
      memoryBytesSpilled = 13L,
      diskBytesSpilled = 14L,
      isBlacklistedForStage = true,
      peakMemoryMetrics = peakMemoryMetrics,
      isExcludedForStage = false)
    val input = new ExecutorStageSummaryWrapper(
      stageId = 1,
      stageAttemptId = 2,
      executorId = "executor_id_1",
      info = info)
    val bytes = serializer.serialize(input)
    val result = serializer.deserialize(bytes, classOf[ExecutorStageSummaryWrapper])
    assert(result.stageId == input.stageId)
    assert(result.stageAttemptId == input.stageAttemptId)
    assert(result.executorId == input.executorId)
    assert(result.info.taskTime == input.info.taskTime)
    assert(result.info.failedTasks == input.info.failedTasks)
    assert(result.info.succeededTasks == input.info.succeededTasks)
    assert(result.info.killedTasks == input.info.killedTasks)
    assert(result.info.inputBytes == input.info.inputBytes)
    assert(result.info.inputRecords == input.info.inputRecords)
    assert(result.info.outputBytes == input.info.outputBytes)
    assert(result.info.outputRecords == input.info.outputRecords)
    assert(result.info.shuffleRead == input.info.shuffleRead)
    assert(result.info.shuffleReadRecords == input.info.shuffleReadRecords)
    assert(result.info.shuffleWrite == input.info.shuffleWrite)
    assert(result.info.shuffleWriteRecords == input.info.shuffleWriteRecords)
    assert(result.info.memoryBytesSpilled == input.info.memoryBytesSpilled)
    assert(result.info.diskBytesSpilled == input.info.diskBytesSpilled)
    assert(result.info.isBlacklistedForStage == input.info.isBlacklistedForStage)
    assert(result.info.isExcludedForStage == input.info.isExcludedForStage)
    assert(result.info.peakMemoryMetrics.isDefined)
    ExecutorMetricType.metricToOffset.foreach { case (name, index) =>
      result.info.peakMemoryMetrics.get.getMetricValue(name) ==
        input.info.peakMemoryMetrics.get.getMetricValue(name)
    }
  }

  test("Application Environment Info") {
    val input = new ApplicationEnvironmentInfoWrapper(
      new ApplicationEnvironmentInfo(
        runtime = new RuntimeInfo(
          javaVersion = "1.8",
          javaHome = "/tmp/java",
          scalaVersion = "2.13"),
        sparkProperties = Seq(("spark.conf.1", "1"), ("spark.conf.2", "2")),
        hadoopProperties = Seq(("hadoop.conf.conf1", "1"), ("hadoop.conf2", "val2")),
        systemProperties = Seq(("sys.prop.1", "value1"), ("sys.prop.2", "value2")),
        metricsProperties = Seq(("metric.1", "klass1"), ("metric2", "klass2")),
        classpathEntries = Seq(("/jar1", "System"), ("/jar2", "User")),
        resourceProfiles = Seq(new ResourceProfileInfo(
          id = 0,
          executorResources = Map(
            "0" -> new ExecutorResourceRequest(
              resourceName = "exec1",
              amount = 1,
              discoveryScript = "script0",
              vendor = "apache"),
            "1" -> new ExecutorResourceRequest(
              resourceName = "exec2",
              amount = 1,
              discoveryScript = "script1",
              vendor = "apache")
          ),
          taskResources = Map(
            "0" -> new TaskResourceRequest(resourceName = "exec1", amount = 1),
            "1" -> new TaskResourceRequest(resourceName = "exec2", amount = 1)
          )
        ))
      )
    )

    val bytes = serializer.serialize(input)
    val result = serializer.deserialize(bytes, classOf[ApplicationEnvironmentInfoWrapper])
    assert(result.info.runtime.javaVersion == input.info.runtime.javaVersion)
    assert(result.info.runtime.javaHome == input.info.runtime.javaHome)
    assert(result.info.runtime.scalaVersion == input.info.runtime.scalaVersion)
    assert(result.info.sparkProperties.length == input.info.sparkProperties.length)
    result.info.sparkProperties.zip(input.info.sparkProperties).foreach { case (p1, p2) =>
      assert(p1 == p2)
    }
    assert(result.info.hadoopProperties.length == input.info.hadoopProperties.length)
    result.info.hadoopProperties.zip(input.info.hadoopProperties).foreach { case (p1, p2) =>
      assert(p1 == p2)
    }
    assert(result.info.systemProperties.length == input.info.systemProperties.length)
    result.info.systemProperties.zip(input.info.systemProperties).foreach { case (p1, p2) =>
      assert(p1 == p2)
    }
    assert(result.info.metricsProperties.length == input.info.metricsProperties.length)
    result.info.metricsProperties.zip(input.info.metricsProperties).foreach { case (p1, p2) =>
      assert(p1 == p2)
    }
    assert(result.info.classpathEntries.length == input.info.classpathEntries.length)
    result.info.classpathEntries.zip(input.info.classpathEntries).foreach { case (p1, p2) =>
      assert(p1 == p2)
    }
    assert(result.info.resourceProfiles.length == input.info.resourceProfiles.length)
    result.info.resourceProfiles.zip(input.info.resourceProfiles).foreach { case (p1, p2) =>
      assert(p1.id == p2.id)
      assert(p1.executorResources.size == p2.executorResources.size)
      assert(p1.executorResources.keys.size == p2.executorResources.keys.size)
      p1.executorResources.keysIterator.foreach { k =>
        assert(p1.executorResources.contains(k))
        assert(p2.executorResources.contains(k))
        assert(p1.executorResources(k) == p2.executorResources(k))
      }
      assert(p1.taskResources.size == p2.taskResources.size)
      assert(p1.taskResources.keys.size == p2.taskResources.keys.size)
      p1.taskResources.keysIterator.foreach { k =>
        assert(p1.taskResources.contains(k))
        assert(p2.taskResources.contains(k))
        assert(p1.taskResources(k) == p2.taskResources(k))
      }
    }
  }

  test("Application Info") {
    val attempts: Seq[ApplicationAttemptInfo] = Seq(
      ApplicationAttemptInfo(
        attemptId = Some("001"),
        startTime = new Date(1),
        endTime = new Date(10),
        lastUpdated = new Date(11),
        duration = 100,
        sparkUser = "user",
        completed = false,
        appSparkVersion = "3.4.0"
      ),
      ApplicationAttemptInfo(
        attemptId = Some("002"),
        startTime = new Date(12L),
        endTime = new Date(17L),
        lastUpdated = new Date(18L),
        duration = 100,
        sparkUser = "user",
        completed = true,
        appSparkVersion = "3.4.0"
      ))
    val input = new ApplicationInfoWrapper(
      ApplicationInfo(
        id = "2",
        name = "app_2",
        coresGranted = Some(1),
        maxCores = Some(2),
        coresPerExecutor = Some(3),
        memoryPerExecutorMB = Some(64),
        attempts = attempts)
    )

    val bytes = serializer.serialize(input)
    val result = serializer.deserialize(bytes, classOf[ApplicationInfoWrapper])
    assert(result.info.id == input.info.id)
    assert(result.info.name == input.info.name)
    assert(result.info.coresGranted == input.info.coresGranted)
    assert(result.info.maxCores == input.info.maxCores)
    assert(result.info.coresPerExecutor == input.info.coresPerExecutor)
    assert(result.info.memoryPerExecutorMB == input.info.memoryPerExecutorMB)
    assert(result.info.attempts.length == input.info.attempts.length)
    result.info.attempts.zip(input.info.attempts).foreach { case (a1, a2) =>
      assert(a1.attemptId == a2.attemptId)
      assert(a1.startTime == a2.startTime)
      assert(a1.endTime == a2.endTime)
      assert(a1.lastUpdated == a2.lastUpdated)
      assert(a1.duration == a2.duration)
      assert(a1.sparkUser == a2.sparkUser)
      assert(a1.completed == a2.completed)
      assert(a1.appSparkVersion == a2.appSparkVersion)
    }
  }

  test("RDD Storage Info") {
    val rddDataDistribution = Seq(
      new RDDDataDistribution(
        address = "add1",
        memoryUsed = 6,
        memoryRemaining = 8,
        diskUsed = 100,
        onHeapMemoryUsed = Some(101),
        offHeapMemoryUsed = Some(102),
        onHeapMemoryRemaining = Some(103),
        offHeapMemoryRemaining = Some(104))
    )
    val rddPartitionInfo = Seq(
      new RDDPartitionInfo(
        blockName = "block_1",
        storageLevel = "IN_MEM",
        memoryUsed = 105,
        diskUsed = 106,
        executors = Seq("exec_0", "exec_1"))
    )
    val inputs = Seq(
      new RDDStorageInfoWrapper(
        info = new RDDStorageInfo(
          id = 1,
          name = "rdd_1",
          numPartitions = 2,
          numCachedPartitions = 3,
          storageLevel = "ON_DISK",
          memoryUsed = 64,
          diskUsed = 128,
          dataDistribution = Some(rddDataDistribution),
          partitions = Some(rddPartitionInfo)
        )
      ),
      new RDDStorageInfoWrapper(
        info = new RDDStorageInfo(
          id = 2,
          name = "rdd_2",
          numPartitions = 7,
          numCachedPartitions = 4,
          storageLevel = "IN_MEMORY",
          memoryUsed = 70,
          diskUsed = 256,
          dataDistribution = None,
          partitions = Some(Seq.empty)
        )
      )
    )
    inputs.foreach { input =>
      val bytes = serializer.serialize(input)
      val result = serializer.deserialize(bytes, classOf[RDDStorageInfoWrapper])
      assert(result.info.id == input.info.id)
      assert(result.info.name == input.info.name)
      assert(result.info.numPartitions == input.info.numPartitions)
      assert(result.info.numCachedPartitions == input.info.numCachedPartitions)
      assert(result.info.storageLevel == input.info.storageLevel)
      assert(result.info.memoryUsed == input.info.memoryUsed)
      assert(result.info.diskUsed == input.info.diskUsed)

      assert(result.info.dataDistribution.isDefined == input.info.dataDistribution.isDefined)
      if (result.info.dataDistribution.isDefined && input.info.dataDistribution.isDefined) {
        assert(result.info.dataDistribution.get.length == input.info.dataDistribution.get.length)
        result.info.dataDistribution.get.zip(input.info.dataDistribution.get).foreach {
          case (d1, d2) =>
            assert(d1.address == d2.address)
            assert(d1.memoryUsed == d2.memoryUsed)
            assert(d1.memoryRemaining == d2.memoryRemaining)
            assert(d1.diskUsed == d2.diskUsed)
            assert(d1.onHeapMemoryUsed == d2.onHeapMemoryUsed)
            assert(d1.offHeapMemoryUsed == d2.offHeapMemoryUsed)
            assert(d1.onHeapMemoryRemaining == d2.onHeapMemoryRemaining)
            assert(d1.offHeapMemoryRemaining == d2.offHeapMemoryRemaining)
        }
      }

      assert(result.info.partitions.isDefined == input.info.partitions.isDefined)
      if (result.info.partitions.isDefined && input.info.partitions.isDefined) {
        assert(result.info.partitions.get.length == input.info.partitions.get.length)
        result.info.partitions.get.zip(input.info.partitions.get).foreach { case (p1, p2) =>
          assert(p1.blockName == p2.blockName)
          assert(p1.storageLevel == p2.storageLevel)
          assert(p1.memoryUsed == p2.memoryUsed)
          assert(p1.diskUsed == p2.diskUsed)
          assert(p1.executors.length == p2.executors.length)
          p1.executors.zip(p2.executors).foreach { case (e1, e2) =>
            e1 == e2
          }
        }
      }
    }
  }

  test("Stream Block Data") {
    val input = new StreamBlockData(
      name = "a",
      executorId = "executor-1",
      hostPort = "123",
      storageLevel = "LOCAL",
      useMemory = true,
      useDisk = false,
      deserialized = true,
      memSize = 1L,
      diskSize = 2L)
    val bytes = serializer.serialize(input)
    val result = serializer.deserialize(bytes, classOf[StreamBlockData])
    assert(result.name == input.name)
    assert(result.executorId == input.executorId)
    assert(result.hostPort == input.hostPort)
    assert(result.storageLevel == input.storageLevel)
    assert(result.useMemory == input.useMemory)
    assert(result.useDisk == input.useDisk)
    assert(result.deserialized == input.deserialized)
    assert(result.memSize == input.memSize)
    assert(result.diskSize == input.diskSize)
  }

  test("Resource Profile") {
    val input = new ResourceProfileWrapper(
      rpInfo = new ResourceProfileInfo(
        id = 0,
        executorResources = Map(
          "0" -> new ExecutorResourceRequest(
            resourceName = "exec1",
            amount = 64,
            discoveryScript = "script0",
            vendor = "apache_2"),
          "1" -> new ExecutorResourceRequest(
            resourceName = "exec2",
            amount = 65,
            discoveryScript = "script1",
            vendor = "apache_1")
        ),
        taskResources = Map(
          "0" -> new TaskResourceRequest(resourceName = "exec1", amount = 1),
          "1" -> new TaskResourceRequest(resourceName = "exec2", amount = 1)
        )
      )
    )

    val bytes = serializer.serialize(input)
    val result = serializer.deserialize(bytes, classOf[ResourceProfileWrapper])
    assert(result.rpInfo.id == input.rpInfo.id)
    assert(result.rpInfo.executorResources.size == input.rpInfo.executorResources.size)
    assert(result.rpInfo.executorResources.keys.size == input.rpInfo.executorResources.keys.size)
    result.rpInfo.executorResources.keysIterator.foreach { k =>
      assert(result.rpInfo.executorResources.contains(k))
      assert(input.rpInfo.executorResources.contains(k))
      assert(result.rpInfo.executorResources(k) == input.rpInfo.executorResources(k))
    }
    assert(result.rpInfo.taskResources.size == input.rpInfo.taskResources.size)
    assert(result.rpInfo.taskResources.keys.size == input.rpInfo.taskResources.keys.size)
    result.rpInfo.taskResources.keysIterator.foreach { k =>
      assert(result.rpInfo.taskResources.contains(k))
      assert(input.rpInfo.taskResources.contains(k))
      assert(result.rpInfo.taskResources(k) == input.rpInfo.taskResources(k))
    }
  }

  test("CachedQuantile") {
    val input = new CachedQuantile(
      stageId = 1,
      stageAttemptId = 2,
      quantile = "a",
      taskCount = 3L,
      duration = 4L,
      executorDeserializeTime = 5.1,
      executorDeserializeCpuTime = 6.1,
      executorRunTime = 7.1,
      executorCpuTime = 8.1,
      resultSize = 9.1,
      jvmGcTime = 10.1,
      resultSerializationTime = 11.1,
      gettingResultTime = 12.1,
      schedulerDelay = 13.1,
      peakExecutionMemory = 14.1,
      memoryBytesSpilled = 15.1,
      diskBytesSpilled = 16.1,
      bytesRead = 17.1,
      recordsRead = 18.1,
      bytesWritten = 19.1,
      recordsWritten = 20.1,
      shuffleReadBytes = 21.1,
      shuffleRecordsRead = 22.1,
      shuffleRemoteBlocksFetched = 23.1,
      shuffleLocalBlocksFetched = 24.1,
      shuffleFetchWaitTime = 25.1,
      shuffleRemoteBytesRead = 26.1,
      shuffleRemoteBytesReadToDisk = 27.1,
      shuffleTotalBlocksFetched = 28.1,
      shuffleWriteBytes = 29.1,
      shuffleWriteRecords = 30.1,
      shuffleWriteTime = 31.1)
    val bytes = serializer.serialize(input)
    val result = serializer.deserialize(bytes, classOf[CachedQuantile])
    assert(result.stageId == input.stageId)
    assert(result.stageAttemptId == input.stageAttemptId)
    assert(result.quantile == input.quantile)
    assert(result.taskCount == input.taskCount)
    assert(result.duration == input.duration)
    assert(result.executorDeserializeTime == input.executorDeserializeTime)
    assert(result.executorDeserializeCpuTime == input.executorDeserializeCpuTime)
    assert(result.executorRunTime == input.executorRunTime)
    assert(result.executorCpuTime == input.executorCpuTime)
    assert(result.resultSize == input.resultSize)
    assert(result.jvmGcTime == input.jvmGcTime)
    assert(result.resultSerializationTime == input.resultSerializationTime)
    assert(result.gettingResultTime == input.gettingResultTime)
    assert(result.schedulerDelay == input.schedulerDelay)
    assert(result.peakExecutionMemory == input.peakExecutionMemory)
    assert(result.memoryBytesSpilled == input.memoryBytesSpilled)
    assert(result.diskBytesSpilled == input.diskBytesSpilled)
    assert(result.bytesRead == input.bytesRead)
    assert(result.recordsRead == input.recordsRead)
    assert(result.bytesWritten == input.bytesWritten)
    assert(result.recordsWritten == input.recordsWritten)
    assert(result.shuffleReadBytes == input.shuffleReadBytes)
    assert(result.shuffleRecordsRead == input.shuffleRecordsRead)
    assert(result.shuffleRemoteBlocksFetched == input.shuffleRemoteBlocksFetched)
    assert(result.shuffleLocalBlocksFetched == input.shuffleLocalBlocksFetched)
    assert(result.shuffleFetchWaitTime == input.shuffleFetchWaitTime)
    assert(result.shuffleRemoteBytesRead == input.shuffleRemoteBytesRead)
    assert(result.shuffleRemoteBytesReadToDisk == input.shuffleRemoteBytesReadToDisk)
    assert(result.shuffleTotalBlocksFetched == input.shuffleTotalBlocksFetched)
    assert(result.shuffleWriteBytes == input.shuffleWriteBytes)
    assert(result.shuffleWriteRecords == input.shuffleWriteRecords)
    assert(result.shuffleWriteTime == input.shuffleWriteTime)
  }

  test("Speculation Stage Summary") {
    val input = new SpeculationStageSummaryWrapper(
      stageId = 1,
      stageAttemptId = 2,
      info = new SpeculationStageSummary(
        numTasks = 3,
        numActiveTasks = 4,
        numCompletedTasks = 5,
        numFailedTasks = 6,
        numKilledTasks = 7
      )
    )
    val bytes = serializer.serialize(input)
    val result = serializer.deserialize(bytes, classOf[SpeculationStageSummaryWrapper])
    assert(result.stageId == input.stageId)
    assert(result.stageAttemptId == input.stageAttemptId)
    assert(result.info.numTasks == input.info.numTasks)
    assert(result.info.numActiveTasks == input.info.numActiveTasks)
    assert(result.info.numCompletedTasks == input.info.numCompletedTasks)
    assert(result.info.numFailedTasks == input.info.numFailedTasks)
    assert(result.info.numKilledTasks == input.info.numKilledTasks)
  }
}
