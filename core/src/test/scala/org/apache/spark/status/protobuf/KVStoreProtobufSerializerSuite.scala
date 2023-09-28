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

import scala.collection.mutable
import scala.io.Source

import org.apache.spark.{JobExecutionStatus, SparkFunSuite}
import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.metrics.ExecutorMetricType
import org.apache.spark.rdd.DeterministicLevel
import org.apache.spark.resource.{ExecutorResourceRequest, ResourceInformation, TaskResourceRequest}
import org.apache.spark.status._
import org.apache.spark.status.api.v1._
import org.apache.spark.ui.scope.{RDDOperationEdge, RDDOperationNode}
import org.apache.spark.util.Utils.tryWithResource

class KVStoreProtobufSerializerSuite extends SparkFunSuite {
  private val serializer = new KVStoreProtobufSerializer()

  test("All the string fields must be optional to avoid NPE") {
    val protoFile = getWorkspaceFilePath(
      "core", "src", "main", "protobuf", "org", "apache", "spark", "status", "protobuf",
      "store_types.proto")

    val containsStringRegex = "\\s*string .*"
    val invalidDefinition = new mutable.ArrayBuffer[(String, Int)]()
    var lineNumber = 1
    tryWithResource(Source.fromFile(protoFile.toFile.getCanonicalPath)) { file =>
      file.getLines().foreach { line =>
        if (line.matches(containsStringRegex)) {
          invalidDefinition.append((line, lineNumber))
        }
        lineNumber += 1
      }
    }
    val errorMessage = new StringBuilder()
    errorMessage.append(
      """
        |All the string fields should be defined as `optional string` for handling null string.
        |Please update the following fields:
        |""".stripMargin)
    invalidDefinition.foreach { case (line, num) =>
      errorMessage.append(s"line #$num: $line\n")
    }
    assert(invalidDefinition.isEmpty, errorMessage)
  }

  test("Job data") {
    Seq(
      ("test", Some("test description"), Some("group"), Seq("tag1", "tag2")),
      (null, None, None, Seq())
    ).foreach { case (name, description, jobGroup, jobTags) =>
      val input = new JobDataWrapper(
        new JobData(
          jobId = 1,
          name = name,
          description = description,
          submissionTime = Some(new Date(123456L)),
          completionTime = Some(new Date(654321L)),
          stageIds = Seq(1, 2, 3, 4),
          jobGroup = jobGroup,
          jobTags = jobTags,
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
      assert(result.info.jobTags == input.info.jobTags)
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
  }

  test("Task Data") {
    val accumulatorUpdates = Seq(
      new AccumulableInfo(1L, "duration", Some("update"), "value1"),
      new AccumulableInfo(2L, "duration2", None, "value2"),
      new AccumulableInfo(-1L, null, None, null)
    )
    Seq(
      ("executor_id_1", "host_name", "SUCCESS", "LOCAL"),
      (null, null, null, null)
    ).foreach { case (executorId, host, status, taskLocality) =>
      val input = new TaskDataWrapper(
        taskId = 1,
        index = 2,
        attempt = 3,
        partitionId = 4,
        launchTime = 5L,
        resultFetchStart = 6L,
        duration = 10000L,
        executorId = executorId,
        host = host,
        status = status,
        taskLocality = taskLocality,
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
        shuffleCorruptMergedBlockChunks = 28L,
        shuffleMergedFetchFallbackCount = 29L,
        shuffleMergedRemoteBlocksFetched = 30L,
        shuffleMergedLocalBlocksFetched = 31L,
        shuffleMergedRemoteChunksFetched = 32L,
        shuffleMergedLocalChunksFetched = 33L,
        shuffleMergedRemoteBytesRead = 34L,
        shuffleMergedLocalBytesRead = 35L,
        shuffleRemoteReqsDuration = 36L,
        shuffleMergedRemoteReqDuration = 37L,
        shuffleBytesWritten = 38L,
        shuffleWriteTime = 39L,
        shuffleRecordsWritten = 40L,
        stageId = 41,
        stageAttemptId = 42)

      val bytes = serializer.serialize(input)
      val result = serializer.deserialize(bytes, classOf[TaskDataWrapper])
      checkAnswer(result.accumulatorUpdates, input.accumulatorUpdates)
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
      assert(result.shuffleCorruptMergedBlockChunks == input.shuffleCorruptMergedBlockChunks)
      assert(result.shuffleMergedFetchFallbackCount == input.shuffleMergedFetchFallbackCount)
      assert(result.shuffleMergedRemoteBlocksFetched == input.shuffleMergedRemoteBlocksFetched)
      assert(result.shuffleMergedLocalBlocksFetched == input.shuffleMergedLocalBlocksFetched)
      assert(result.shuffleMergedRemoteChunksFetched == input.shuffleMergedRemoteChunksFetched)
      assert(result.shuffleMergedLocalChunksFetched == input.shuffleMergedLocalChunksFetched)
      assert(result.shuffleMergedRemoteBytesRead == input.shuffleMergedRemoteBytesRead)
      assert(result.shuffleMergedLocalBytesRead == input.shuffleMergedLocalBytesRead)
      assert(result.shuffleRemoteReqsDuration == input.shuffleRemoteReqsDuration)
      assert(result.shuffleMergedRemoteReqDuration == input.shuffleMergedRemoteReqDuration)
      assert(result.shuffleBytesWritten == input.shuffleBytesWritten)
      assert(result.shuffleWriteTime == input.shuffleWriteTime)
      assert(result.shuffleRecordsWritten == input.shuffleRecordsWritten)
      assert(result.stageId == input.stageId)
      assert(result.stageAttemptId == input.stageAttemptId)
    }
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
    Seq("executor_id_1", null).foreach { executorId =>
      val input = new ExecutorStageSummaryWrapper(
        stageId = 1,
        stageAttemptId = 2,
        executorId = executorId,
        info = info)
      val bytes = serializer.serialize(input)
      val result = serializer.deserialize(bytes, classOf[ExecutorStageSummaryWrapper])
      assert(result.stageId == input.stageId)
      assert(result.stageAttemptId == input.stageAttemptId)
      assert(result.executorId == input.executorId)
      checkAnswer(result.info, input.info)
    }
  }

  test("Application Environment Info") {
    testApplicationEnvironmentInfoWrapperSerDe("1.8", "/tmp/java", "2.13")
  }

  test("Application Environment Info with nulls") {
    testApplicationEnvironmentInfoWrapperSerDe(null, null, null)
  }

  private def testApplicationEnvironmentInfoWrapperSerDe(
      javaVersion: String, javaHome: String, scalaVersion: String): Unit = {
    val input = new ApplicationEnvironmentInfoWrapper(
      new ApplicationEnvironmentInfo(
        runtime = new RuntimeInfo(
          javaVersion = javaVersion,
          javaHome = javaHome,
          scalaVersion = scalaVersion),
        sparkProperties = Seq(("spark.conf.1", "1"), ("spark.conf.2", "2"), (null, null)),
        hadoopProperties =
          Seq(("hadoop.conf.conf1", "1"), ("hadoop.conf2", "val2"), (null, "val3")),
        systemProperties =
          Seq(("sys.prop.1", "value1"), ("sys.prop.2", "value2"), ("sys.prop.3", null)),
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
              resourceName = null,
              amount = 1,
              discoveryScript = null,
              vendor = null)
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
    testApplicationInfoWrapperSerDe("2", "app_2")
  }

  test("Application Info with nulls") {
    testApplicationInfoWrapperSerDe(null, null)
  }

  private def testApplicationInfoWrapperSerDe(id: String, name: String): Unit = {
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
        sparkUser = null,
        completed = true,
        appSparkVersion = null
      ))
    val input = new ApplicationInfoWrapper(
      ApplicationInfo(
        id = id,
        name = name,
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
        offHeapMemoryRemaining = Some(104)),
      new RDDDataDistribution(
        address = null,
        memoryUsed = 60,
        memoryRemaining = 80,
        diskUsed = 1000,
        onHeapMemoryUsed = Some(1010),
        offHeapMemoryUsed = Some(1020),
        onHeapMemoryRemaining = Some(1030),
        offHeapMemoryRemaining = Some(1040))
    )
    val rddPartitionInfo = Seq(
      new RDDPartitionInfo(
        blockName = "block_1",
        storageLevel = "IN_MEM",
        memoryUsed = 105,
        diskUsed = 106,
        executors = Seq("exec_0", "exec_1")),
      new RDDPartitionInfo(
        blockName = null,
        storageLevel = null,
        memoryUsed = 105,
        diskUsed = 106,
        executors = Seq("exec_2", "exec_3"))
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
      ),
      new RDDStorageInfoWrapper(
        info = new RDDStorageInfo(
          id = 3,
          name = null,
          numPartitions = 8,
          numCachedPartitions = 5,
          storageLevel = null,
          memoryUsed = 100,
          diskUsed = 2560,
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
    val normal = new StreamBlockData(
      name = "a",
      executorId = "executor-1",
      hostPort = "123",
      storageLevel = "LOCAL",
      useMemory = true,
      useDisk = false,
      deserialized = true,
      memSize = 1L,
      diskSize = 2L)
    val withNull = new StreamBlockData(
      name = null,
      executorId = null,
      hostPort = null,
      storageLevel = null,
      useMemory = true,
      useDisk = false,
      deserialized = true,
      memSize = 1L,
      diskSize = 2L)
    Seq(normal, withNull).foreach { input =>
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
    Seq("a", null).foreach { quantile =>
      val input = new CachedQuantile(
        stageId = 1,
        stageAttemptId = 2,
        quantile = quantile,
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
        shuffleCorruptMergedBlockChunks = 29.1,
        shuffleMergedFetchFallbackCount = 30.1,
        shuffleMergedRemoteBlocksFetched = 31.1,
        shuffleMergedLocalBlocksFetched = 32.1,
        shuffleMergedRemoteChunksFetched = 33.1,
        shuffleMergedLocalChunksFetched = 34.1,
        shuffleMergedRemoteBytesRead = 35.1,
        shuffleMergedLocalBytesRead = 36.1,
        shuffleRemoteReqsDuration = 37.1,
        shuffleMergedRemoteReqsDuration = 38.1,
        shuffleWriteBytes = 39.1,
        shuffleWriteRecords = 40.1,
        shuffleWriteTime = 41.1)
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
      assert(result.shuffleCorruptMergedBlockChunks == input.shuffleCorruptMergedBlockChunks)
      assert(result.shuffleMergedFetchFallbackCount == input.shuffleMergedFetchFallbackCount)
      assert(result.shuffleMergedRemoteBlocksFetched == input.shuffleMergedRemoteBlocksFetched)
      assert(result.shuffleMergedLocalBlocksFetched == input.shuffleMergedLocalBlocksFetched)
      assert(result.shuffleMergedRemoteChunksFetched == input.shuffleMergedRemoteChunksFetched)
      assert(result.shuffleMergedLocalChunksFetched == input.shuffleMergedLocalChunksFetched)
      assert(result.shuffleMergedRemoteBytesRead == input.shuffleMergedRemoteBytesRead)
      assert(result.shuffleMergedLocalBytesRead == input.shuffleMergedLocalBytesRead)
      assert(result.shuffleRemoteReqsDuration == input.shuffleRemoteReqsDuration)
      assert(result.shuffleMergedRemoteReqsDuration == input.shuffleMergedRemoteReqsDuration)
      assert(result.shuffleWriteBytes == input.shuffleWriteBytes)
      assert(result.shuffleWriteRecords == input.shuffleWriteRecords)
      assert(result.shuffleWriteTime == input.shuffleWriteTime)
    }
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
    checkAnswer(result.info, input.info)
  }

  test("Executor Summary") {
    val memoryMetrics =
      Some(new MemoryMetrics(
        usedOnHeapStorageMemory = 15,
        usedOffHeapStorageMemory = 16,
        totalOnHeapStorageMemory = 17,
        totalOffHeapStorageMemory = 18))
    val peakMemoryMetric =
      Some(new ExecutorMetrics(Array(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 1024L)))
    val resources =
      Map("resource1" -> new ResourceInformation("re1", Array("add1", "add2")),
        "resource1" -> new ResourceInformation(null, null))
    Seq(("id_1", "localhost:7777"), (null, "")).foreach { case (id, hostPort) =>
      val input = new ExecutorSummaryWrapper(
        info = new ExecutorSummary(
          id = id,
          hostPort = hostPort,
          isActive = true,
          rddBlocks = 1,
          memoryUsed = 64,
          diskUsed = 128,
          totalCores = 2,
          maxTasks = 6,
          activeTasks = 5,
          failedTasks = 4,
          completedTasks = 3,
          totalTasks = 7,
          totalDuration = 8,
          totalGCTime = 9,
          totalInputBytes = 10,
          totalShuffleRead = 11,
          totalShuffleWrite = 12,
          isBlacklisted = false,
          maxMemory = 256,
          addTime = new Date(13),
          removeTime = Some(new Date(14)),
          removeReason = Some("reason_1"),
          executorLogs = Map("log1" -> "logs/log1.log", "log2" -> "/log/log2.log"),
          memoryMetrics = memoryMetrics,
          blacklistedInStages = Set(19, 20, 21),
          peakMemoryMetrics = peakMemoryMetric,
          attributes = Map("attri1" -> "value1", "attri2" -> "val2"),
          resources = resources,
          resourceProfileId = 22,
          isExcluded = true,
          excludedInStages = Set(23, 24)
        )
      )

      val bytes = serializer.serialize(input)
      val result = serializer.deserialize(bytes, classOf[ExecutorSummaryWrapper])

      assert(result.info.id == input.info.id)
      assert(result.info.hostPort == input.info.hostPort)
      assert(result.info.isActive == input.info.isActive)
      assert(result.info.rddBlocks == input.info.rddBlocks)
      assert(result.info.memoryUsed == input.info.memoryUsed)
      assert(result.info.diskUsed == input.info.diskUsed)
      assert(result.info.totalCores == input.info.totalCores)
      assert(result.info.maxTasks == input.info.maxTasks)
      assert(result.info.activeTasks == input.info.activeTasks)
      assert(result.info.failedTasks == input.info.failedTasks)
      assert(result.info.completedTasks == input.info.completedTasks)
      assert(result.info.totalTasks == input.info.totalTasks)
      assert(result.info.totalDuration == input.info.totalDuration)
      assert(result.info.totalGCTime == input.info.totalGCTime)
      assert(result.info.totalInputBytes == input.info.totalInputBytes)
      assert(result.info.totalShuffleRead == input.info.totalShuffleRead)
      assert(result.info.totalShuffleWrite == input.info.totalShuffleWrite)
      assert(result.info.isBlacklisted == input.info.isBlacklisted)
      assert(result.info.maxMemory == input.info.maxMemory)
      assert(result.info.addTime == input.info.addTime)
      assert(result.info.removeTime == input.info.removeTime)
      assert(result.info.removeReason == input.info.removeReason)

      assert(result.info.executorLogs.size == input.info.executorLogs.size)
      result.info.executorLogs.keys.foreach { k =>
        assert(input.info.executorLogs.contains(k))
        assert(result.info.executorLogs(k) == input.info.executorLogs(k))
      }

      assert(result.info.memoryMetrics.isDefined == input.info.memoryMetrics.isDefined)
      if (result.info.memoryMetrics.isDefined && input.info.memoryMetrics.isDefined) {
        assert(result.info.memoryMetrics.get.usedOnHeapStorageMemory ==
          input.info.memoryMetrics.get.usedOnHeapStorageMemory)
        assert(result.info.memoryMetrics.get.usedOffHeapStorageMemory ==
          input.info.memoryMetrics.get.usedOffHeapStorageMemory)
        assert(result.info.memoryMetrics.get.totalOnHeapStorageMemory ==
          input.info.memoryMetrics.get.totalOnHeapStorageMemory)
        assert(result.info.memoryMetrics.get.totalOffHeapStorageMemory ==
          input.info.memoryMetrics.get.totalOffHeapStorageMemory)
      }

      assert(result.info.blacklistedInStages.size == input.info.blacklistedInStages.size)
      result.info.blacklistedInStages.foreach { stage =>
        assert(input.info.blacklistedInStages.contains(stage))
      }

      assert(result.info.peakMemoryMetrics.isDefined == input.info.peakMemoryMetrics.isDefined)
      if (result.info.peakMemoryMetrics.isDefined && input.info.peakMemoryMetrics.isDefined) {
        checkAnswer(result.info.peakMemoryMetrics.get, input.info.peakMemoryMetrics.get)
      }

      assert(result.info.attributes.size == input.info.attributes.size)
      result.info.attributes.keys.foreach { k =>
        assert(input.info.attributes.contains(k))
        assert(result.info.attributes(k) == input.info.attributes(k))
      }

      assert(result.info.resources.size == input.info.resources.size)
      result.info.resources.keys.foreach { k =>
        assert(input.info.resources.contains(k))
        assert(result.info.resources(k).name == input.info.resources(k).name)
        if (input.info.resources(k).addresses != null) {
          result.info.resources(k).addresses.zip(input.info.resources(k).addresses).foreach {
            case (a1, a2) =>
              assert(a1 == a2)
          }
        } else {
          assert(result.info.resources(k).addresses.isEmpty)
        }
      }

      assert(result.info.resourceProfileId == input.info.resourceProfileId)
      assert(result.info.isExcluded == input.info.isExcluded)

      assert(result.info.excludedInStages.size == input.info.excludedInStages.size)
      result.info.excludedInStages.foreach { stage =>
        assert(input.info.excludedInStages.contains(stage))
      }
    }
  }

  test("Process Summary") {
    Seq(
      ("id_1", "localhost:2020"),
      (null, "") // hostPort can't be null. Otherwise there will be NPE.
    ).foreach { case(id, hostPort) =>
      val input = new ProcessSummaryWrapper(
        info = new ProcessSummary(
          id = id,
          hostPort = hostPort,
          isActive = true,
          totalCores = 4,
          addTime = new Date(1234567L),
          removeTime = Some(new Date(1234568L)),
          processLogs = Map("log1" -> "log/log1", "log2" -> "logs/log2.log")
        )
      )
      val bytes = serializer.serialize(input)
      val result = serializer.deserialize(bytes, classOf[ProcessSummaryWrapper])
      assert(result.info.id == input.info.id)
      assert(result.info.hostPort == input.info.hostPort)
      assert(result.info.isActive == input.info.isActive)
      assert(result.info.totalCores == input.info.totalCores)
      assert(result.info.addTime == input.info.addTime)
      assert(result.info.removeTime == input.info.removeTime)
      assert(result.info.processLogs.size == input.info.processLogs.size)
      result.info.processLogs.keys.foreach { k =>
        assert(input.info.processLogs.contains(k))
        assert(result.info.processLogs(k) == input.info.processLogs(k))
      }
    }
  }

  test("RDD Operation Graph") {
    val input = new RDDOperationGraphWrapper(
      stageId = 1,
      edges = Seq(
        RDDOperationEdge(fromId = 2, toId = 3)
      ),
      outgoingEdges = Seq(
        RDDOperationEdge(fromId = 4, toId = 5),
        RDDOperationEdge(fromId = 6, toId = 7)
      ),
      incomingEdges = Seq(
        RDDOperationEdge(fromId = 8, toId = 9),
        RDDOperationEdge(fromId = 10, toId = 11),
        RDDOperationEdge(fromId = 12, toId = 13)
      ),
      rootCluster = new RDDOperationClusterWrapper(
        id = "id_1",
        name = "name1",
        childNodes = Seq(
          RDDOperationNode(
            id = 14,
            name = "name2",
            cached = true,
            barrier = false,
            callsite = "callsite_1",
            outputDeterministicLevel = DeterministicLevel.INDETERMINATE),
          RDDOperationNode(
            id = 20,
            name = null,
            cached = true,
            barrier = false,
            callsite = null,
            outputDeterministicLevel = DeterministicLevel.DETERMINATE)),
        childClusters = Seq(
          new RDDOperationClusterWrapper(
            id = "id_1",
            name = "name1",
            childNodes = Seq(
              RDDOperationNode(
                id = 15,
                name = "name3",
                cached = false,
                barrier = true,
                callsite = "callsite_2",
                outputDeterministicLevel = DeterministicLevel.UNORDERED)),
            childClusters = Seq.empty
          ),
          new RDDOperationClusterWrapper(
            id = null,
            name = null,
            childNodes = Seq(
              RDDOperationNode(
                id = 21,
                name = null,
                cached = false,
                barrier = true,
                callsite = null,
                outputDeterministicLevel = DeterministicLevel.UNORDERED)),
            childClusters = Seq.empty
          ))
      )
    )
    val bytes = serializer.serialize(input)
    val result = serializer.deserialize(bytes, classOf[RDDOperationGraphWrapper])

    assert(result.stageId == input.stageId)
    assert(result.edges.size == input.edges.size)
    result.edges.zip(input.edges).foreach { case (e1, e2) =>
      assert(e1.fromId == e2.fromId)
      assert(e1.toId == e2.toId)
    }
    assert(result.outgoingEdges.size == input.outgoingEdges.size)
    result.outgoingEdges.zip(input.outgoingEdges).foreach { case (e1, e2) =>
      assert(e1.fromId == e2.fromId)
      assert(e1.toId == e2.toId)
    }
    assert(result.incomingEdges.size == input.incomingEdges.size)
    result.incomingEdges.zip(input.incomingEdges).foreach { case (e1, e2) =>
      assert(e1.fromId == e2.fromId)
      assert(e1.toId == e2.toId)
    }

    def compareClusters(c1: RDDOperationClusterWrapper, c2: RDDOperationClusterWrapper): Unit = {
      assert(c1.id == c2.id)
      assert(c1.name == c2.name)
      assert(c1.childNodes.size == c2.childNodes.size)
      c1.childNodes.zip(c2.childNodes).foreach { case (n1, n2) =>
        assert(n1.id == n2.id)
        assert(n1.name == n2.name)
        assert(n1.cached == n2.cached)
        assert(n1.barrier == n2.barrier)
        assert(n1.callsite == n2.callsite)
        assert(n1.outputDeterministicLevel == n2.outputDeterministicLevel)
      }
      assert(c1.childClusters.size == c2.childClusters.size)
      c1.childClusters.zip(c2.childClusters).foreach {
        case (_c1, _c2) => compareClusters(_c1, _c2)
      }
    }

    compareClusters(result.rootCluster, input.rootCluster)
  }

  test("Stage Data") {
    testStageDataSerDe("name", "test details", "test scheduling pool")
  }

  test("Stage Data with null strings") {
    testStageDataSerDe(null, null, null)
  }

  private def testStageDataSerDe(name: String, details: String, schedulingPool: String): Unit = {
    val accumulatorUpdates = Seq(
      new AccumulableInfo(1L, "duration", Some("update"), "value1"),
      new AccumulableInfo(2L, "duration2", None, "value2")
    )
    val inputMetrics = new InputMetrics(
      bytesRead = 1L,
      recordsRead = 2L)
    val outputMetrics = new OutputMetrics(
      bytesWritten = 1L,
      recordsWritten = 2L
    )
    val shufflePushReadMetrics = new ShufflePushReadMetrics(
      corruptMergedBlockChunks = 1L,
      mergedFetchFallbackCount = 2L,
      remoteMergedBlocksFetched = 3L,
      localMergedBlocksFetched = 4L,
      remoteMergedChunksFetched = 5L,
      localMergedChunksFetched = 6L,
      remoteMergedBytesRead = 7L,
      localMergedBytesRead = 8L,
      remoteMergedReqsDuration = 9L
    )
    val shuffleReadMetrics = new ShuffleReadMetrics(
      remoteBlocksFetched = 1L,
      localBlocksFetched = 2L,
      fetchWaitTime = 3L,
      remoteBytesRead = 4L,
      remoteBytesReadToDisk = 5L,
      localBytesRead = 6L,
      recordsRead = 7L,
      remoteReqsDuration = 8L,
      shufflePushReadMetrics = shufflePushReadMetrics
    )
    val shuffleWriteMetrics = new ShuffleWriteMetrics(
      bytesWritten = 1L,
      writeTime = 2L,
      recordsWritten = 3L
    )
    val taskMetrics = new TaskMetrics(
      executorDeserializeTime = 1L,
      executorDeserializeCpuTime = 2L,
      executorRunTime = 3L,
      executorCpuTime = 4L,
      resultSize = 5L,
      jvmGcTime = 6L,
      resultSerializationTime = 7L,
      memoryBytesSpilled = 8L,
      diskBytesSpilled = 9L,
      peakExecutionMemory = 10L,
      inputMetrics = inputMetrics,
      outputMetrics = outputMetrics,
      shuffleReadMetrics = shuffleReadMetrics,
      shuffleWriteMetrics = shuffleWriteMetrics
    )
    val taskData1 = new TaskData(
      taskId = 1L,
      index = 2,
      attempt = 3,
      partitionId = 4,
      launchTime = new Date(123456L),
      resultFetchStart = Some(new Date(223456L)),
      duration = Some(10000L),
      executorId = "executor_id_1",
      host = "host_name_1",
      status = "SUCCESS",
      taskLocality = "LOCAL",
      speculative = true,
      accumulatorUpdates = accumulatorUpdates,
      errorMessage = Some("error_1"),
      taskMetrics = Some(taskMetrics),
      executorLogs = Map("executor_id_1" -> "executor_log_1"),
      schedulerDelay = 5L,
      gettingResultTime = 6L
    )
    val taskData2 = new TaskData(
      taskId = 11L,
      index = 12,
      attempt = 13,
      partitionId = 14,
      launchTime = new Date(1123456L),
      resultFetchStart = Some(new Date(1223456L)),
      duration = Some(110000L),
      executorId = null,
      host = null,
      status = null,
      taskLocality = null,
      speculative = false,
      accumulatorUpdates = accumulatorUpdates,
      errorMessage = Some("error_2"),
      taskMetrics = Some(taskMetrics),
      executorLogs = Map("executor_id_2" -> "executor_log_2"),
      schedulerDelay = 15L,
      gettingResultTime = 16L
    )
    val tasks = Some(
      Map(1L -> taskData1, 2L -> taskData2)
    )
    val peakMemoryMetrics =
      Some(new ExecutorMetrics(Array(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 1024L)))
    val executorStageSummary1 = new ExecutorStageSummary(
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
    val executorStageSummary2 = new ExecutorStageSummary(
      taskTime = 11L,
      failedTasks = 12,
      succeededTasks = 13,
      killedTasks = 14,
      inputBytes = 15L,
      inputRecords = 16L,
      outputBytes = 17L,
      outputRecords = 18L,
      shuffleRead = 19L,
      shuffleReadRecords = 110L,
      shuffleWrite = 111L,
      shuffleWriteRecords = 112L,
      memoryBytesSpilled = 113L,
      diskBytesSpilled = 114L,
      isBlacklistedForStage = false,
      peakMemoryMetrics = peakMemoryMetrics,
      isExcludedForStage = true)
    val executorSummary = Some(
      Map("executor_id_1" -> executorStageSummary1, "executor_id_2" -> executorStageSummary2)
    )
    val speculationStageSummary = new SpeculationStageSummary(
      numTasks = 3,
      numActiveTasks = 4,
      numCompletedTasks = 5,
      numFailedTasks = 6,
      numKilledTasks = 7
    )
    val inputMetricDistributions = new InputMetricDistributions(
      bytesRead = IndexedSeq(1.001D, 2.001D),
      recordsRead = IndexedSeq(3.001D, 4.001D)
    )
    val outputMetricDistributions = new OutputMetricDistributions(
      bytesWritten = IndexedSeq(1.001D, 2.001D),
      recordsWritten = IndexedSeq(3.001D, 4.001D)
    )
    val shufflePushReadMetricDistributions = new ShufflePushReadMetricDistributions(
      corruptMergedBlockChunks = IndexedSeq(1.001D, 2.001D),
      mergedFetchFallbackCount = IndexedSeq(2.011D, 3.01D),
      remoteMergedBlocksFetched = IndexedSeq(1.101D, 2.1D),
      localMergedBlocksFetched = IndexedSeq(3.01D, 3.101D),
      remoteMergedChunksFetched = IndexedSeq(1.001D, 2.001D),
      localMergedChunksFetched = IndexedSeq(2.110D, 3.101D),
      remoteMergedBytesRead = IndexedSeq(1.001D, 2.001D),
      localMergedBytesRead = IndexedSeq(4.101D, 3.011D),
      remoteMergedReqsDuration = IndexedSeq(3.001D, 4.101D)
    )
    val shuffleReadMetricDistributions = new ShuffleReadMetricDistributions(
      readBytes = IndexedSeq(1.001D, 2.001D),
      readRecords = IndexedSeq(3.001D, 4.001D),
      remoteBlocksFetched = IndexedSeq(5.001D, 6.001D),
      localBlocksFetched = IndexedSeq(7.001D, 8.001D),
      fetchWaitTime = IndexedSeq(9.001D, 10.001D),
      remoteBytesRead = IndexedSeq(11.001D, 12.001D),
      remoteBytesReadToDisk = IndexedSeq(13.001D, 14.001D),
      totalBlocksFetched = IndexedSeq(15.001D, 16.001D),
      remoteReqsDuration = IndexedSeq(11.01D, 14.001D),
      shufflePushReadMetricsDist = shufflePushReadMetricDistributions
    )
    val shuffleWriteMetricDistributions = new ShuffleWriteMetricDistributions(
      writeBytes = IndexedSeq(1.001D, 2.001D),
      writeRecords = IndexedSeq(3.001D, 4.001D),
      writeTime = IndexedSeq(5.001D, 6.001D)
    )
    val taskMetricDistributions = new TaskMetricDistributions(
      quantiles = IndexedSeq(1.001D, 2.001D),
      duration = IndexedSeq(3.001D, 4.001D),
      executorDeserializeTime = IndexedSeq(5.001D, 6.001D),
      executorDeserializeCpuTime = IndexedSeq(7.001D, 8.001D),
      executorRunTime = IndexedSeq(9.001D, 10.001D),
      executorCpuTime = IndexedSeq(11.001D, 12.001D),
      resultSize = IndexedSeq(13.001D, 14.001D),
      jvmGcTime = IndexedSeq(15.001D, 16.001D),
      resultSerializationTime = IndexedSeq(17.001D, 18.001D),
      gettingResultTime = IndexedSeq(19.001D, 20.001D),
      schedulerDelay = IndexedSeq(21.001D, 22.001D),
      peakExecutionMemory = IndexedSeq(23.001D, 24.001D),
      memoryBytesSpilled = IndexedSeq(25.001D, 26.001D),
      diskBytesSpilled = IndexedSeq(27.001D, 28.001D),
      inputMetrics = inputMetricDistributions,
      outputMetrics = outputMetricDistributions,
      shuffleReadMetrics = shuffleReadMetricDistributions,
      shuffleWriteMetrics = shuffleWriteMetricDistributions
    )
    val executorPeakMetricsDistributions = new ExecutorPeakMetricsDistributions(
      quantiles = IndexedSeq(1.001D, 2.001D),
      executorMetrics = IndexedSeq(
        new ExecutorMetrics(Array(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 1024L)))
    )
    val executorMetricsDistributions = new ExecutorMetricsDistributions(
      quantiles = IndexedSeq(1.001D, 2.001D),
      taskTime = IndexedSeq(3.001D, 4.001D),
      failedTasks = IndexedSeq(5.001D, 6.001D),
      succeededTasks = IndexedSeq(7.001D, 8.001D),
      killedTasks = IndexedSeq(9.001D, 10.001D),
      inputBytes = IndexedSeq(11.001D, 12.001D),
      inputRecords = IndexedSeq(13.001D, 14.001D),
      outputBytes = IndexedSeq(15.001D, 16.001D),
      outputRecords = IndexedSeq(17.001D, 18.001D),
      shuffleRead = IndexedSeq(19.001D, 20.001D),
      shuffleReadRecords = IndexedSeq(21.001D, 22.001D),
      shuffleWrite = IndexedSeq(23.001D, 24.001D),
      shuffleWriteRecords = IndexedSeq(25.001D, 24.001D),
      memoryBytesSpilled = IndexedSeq(27.001D, 28.001D),
      diskBytesSpilled = IndexedSeq(29.001D, 30.001D),
      peakMemoryMetrics = executorPeakMetricsDistributions
    )
    val info = new StageData(
      status = StageStatus.COMPLETE,
      stageId = 1,
      attemptId = 2,
      numTasks = 3,
      numActiveTasks = 4,
      numCompleteTasks = 5,
      numFailedTasks = 6,
      numKilledTasks = 7,
      numCompletedIndices = 8,
      submissionTime = Some(new Date(123456L)),
      firstTaskLaunchedTime = Some(new Date(234567L)),
      completionTime = Some(new Date(654321L)),
      failureReason = Some("failure reason"),
      executorDeserializeTime = 9L,
      executorDeserializeCpuTime = 10L,
      executorRunTime = 11L,
      executorCpuTime = 12L,
      resultSize = 13L,
      jvmGcTime = 14L,
      resultSerializationTime = 15L,
      memoryBytesSpilled = 16L,
      diskBytesSpilled = 17L,
      peakExecutionMemory = 18L,
      inputBytes = 19L,
      inputRecords = 20L,
      outputBytes = 21L,
      outputRecords = 22L,
      shuffleRemoteBlocksFetched = 23L,
      shuffleLocalBlocksFetched = 24L,
      shuffleFetchWaitTime = 25L,
      shuffleRemoteBytesRead = 26L,
      shuffleRemoteBytesReadToDisk = 27L,
      shuffleLocalBytesRead = 28L,
      shuffleReadBytes = 29L,
      shuffleReadRecords = 30L,
      shuffleCorruptMergedBlockChunks = 31L,
      shuffleMergedFetchFallbackCount = 32L,
      shuffleMergedRemoteBlocksFetched = 33L,
      shuffleMergedLocalBlocksFetched = 34L,
      shuffleMergedRemoteChunksFetched = 35L,
      shuffleMergedLocalChunksFetched = 36L,
      shuffleMergedRemoteBytesRead = 37L,
      shuffleMergedLocalBytesRead = 38L,
      shuffleRemoteReqsDuration = 39L,
      shuffleMergedRemoteReqsDuration = 40L,
      shuffleWriteBytes = 41L,
      shuffleWriteTime = 42L,
      shuffleWriteRecords = 43L,
      name = name,
      description = Some("test description"),
      details = details,
      schedulingPool = schedulingPool,
      rddIds = Seq(1, 2, 3, 4, 5, 6),
      accumulatorUpdates = accumulatorUpdates,
      tasks = tasks,
      executorSummary = executorSummary,
      speculationSummary = Some(speculationStageSummary),
      killedTasksSummary = Map("task_1" -> 1),
      resourceProfileId = 34,
      peakExecutorMetrics = peakMemoryMetrics,
      taskMetricsDistributions = Some(taskMetricDistributions),
      executorMetricsDistributions = Some(executorMetricsDistributions),
      isShufflePushEnabled = true,
      shuffleMergersCount = 10
    )
    val input = new StageDataWrapper(
      info = info,
      jobIds = Set(1, 2, 3, 4),
      locality = Map(
        "PROCESS_LOCAL" -> 1L,
        "NODE_LOCAL" -> 2L
      )
    )

    val bytes = serializer.serialize(input)
    val result = serializer.deserialize(bytes, classOf[StageDataWrapper])

    assert(result.jobIds == input.jobIds)
    assert(result.locality == input.locality)

    assert(result.info.status == input.info.status)
    assert(result.info.stageId == input.info.stageId)
    assert(result.info.attemptId == input.info.attemptId)
    assert(result.info.numTasks == input.info.numTasks)
    assert(result.info.numActiveTasks == input.info.numActiveTasks)
    assert(result.info.numCompleteTasks == input.info.numCompleteTasks)
    assert(result.info.numFailedTasks == input.info.numFailedTasks)
    assert(result.info.numKilledTasks == input.info.numKilledTasks)
    assert(result.info.numCompletedIndices == input.info.numCompletedIndices)

    assert(result.info.submissionTime == input.info.submissionTime)
    assert(result.info.firstTaskLaunchedTime == input.info.firstTaskLaunchedTime)
    assert(result.info.completionTime == input.info.completionTime)
    assert(result.info.failureReason == input.info.failureReason)

    assert(result.info.executorDeserializeTime == input.info.executorDeserializeTime)
    assert(result.info.executorDeserializeCpuTime == input.info.executorDeserializeCpuTime)
    assert(result.info.executorRunTime == input.info.executorRunTime)
    assert(result.info.executorCpuTime == input.info.executorCpuTime)
    assert(result.info.resultSize == input.info.resultSize)
    assert(result.info.jvmGcTime == input.info.jvmGcTime)
    assert(result.info.resultSerializationTime == input.info.resultSerializationTime)
    assert(result.info.memoryBytesSpilled == input.info.memoryBytesSpilled)
    assert(result.info.diskBytesSpilled == input.info.diskBytesSpilled)
    assert(result.info.peakExecutionMemory == input.info.peakExecutionMemory)
    assert(result.info.inputBytes == input.info.inputBytes)
    assert(result.info.inputRecords == input.info.inputRecords)
    assert(result.info.outputBytes == input.info.outputBytes)
    assert(result.info.outputRecords == input.info.outputRecords)
    assert(result.info.shuffleRemoteBlocksFetched == input.info.shuffleRemoteBlocksFetched)
    assert(result.info.shuffleLocalBlocksFetched == input.info.shuffleLocalBlocksFetched)
    assert(result.info.shuffleFetchWaitTime == input.info.shuffleFetchWaitTime)
    assert(result.info.shuffleRemoteBytesRead == input.info.shuffleRemoteBytesRead)
    assert(result.info.shuffleRemoteBytesReadToDisk == input.info.shuffleRemoteBytesReadToDisk)
    assert(result.info.shuffleLocalBytesRead == input.info.shuffleLocalBytesRead)
    assert(result.info.shuffleReadBytes == input.info.shuffleReadBytes)
    assert(result.info.shuffleReadRecords == input.info.shuffleReadRecords)
    assert(result.info.shuffleCorruptMergedBlockChunks ==
      input.info.shuffleCorruptMergedBlockChunks)
    assert(result.info.shuffleMergedFetchFallbackCount ==
      input.info.shuffleMergedFetchFallbackCount)
    assert(result.info.shuffleMergedRemoteBlocksFetched ==
      input.info.shuffleMergedRemoteBlocksFetched)
    assert(result.info.shuffleMergedLocalBlocksFetched ==
      input.info.shuffleMergedLocalBlocksFetched)
    assert(result.info.shuffleMergedRemoteChunksFetched ==
      input.info.shuffleMergedRemoteChunksFetched)
    assert(result.info.shuffleMergedLocalChunksFetched ==
      input.info.shuffleMergedLocalChunksFetched)
    assert(result.info.shuffleMergedRemoteBytesRead ==
      input.info.shuffleMergedRemoteBytesRead)
    assert(result.info.shuffleMergedLocalBytesRead ==
      input.info.shuffleMergedLocalBytesRead)
    assert(result.info.shuffleRemoteReqsDuration ==
      input.info.shuffleRemoteReqsDuration)
    assert(result.info.shuffleMergedRemoteReqsDuration ==
      input.info.shuffleMergedRemoteReqsDuration)
    assert(result.info.shuffleWriteBytes == input.info.shuffleWriteBytes)
    assert(result.info.shuffleWriteTime == input.info.shuffleWriteTime)
    assert(result.info.shuffleWriteRecords == input.info.shuffleWriteRecords)

    assert(result.info.name == input.info.name)
    assert(result.info.description == input.info.description)
    assert(result.info.details == input.info.details)
    assert(result.info.schedulingPool == input.info.schedulingPool)

    assert(result.info.rddIds == input.info.rddIds)
    checkAnswer(result.info.accumulatorUpdates, input.info.accumulatorUpdates)

    assert(result.info.tasks.isDefined == input.info.tasks.isDefined)
    if (result.info.tasks.isDefined && input.info.tasks.isDefined) {
      checkIdTask(result.info.tasks.get, input.info.tasks.get)
    }

    assert(result.info.executorSummary.isDefined == input.info.executorSummary.isDefined)
    if (result.info.executorSummary.isDefined && input.info.executorSummary.isDefined) {
      checkAnswer(result.info.executorSummary.get, input.info.executorSummary.get)
    }

    assert(result.info.speculationSummary.isDefined == input.info.speculationSummary.isDefined)
    if (result.info.speculationSummary.isDefined && input.info.speculationSummary.isDefined) {
      checkAnswer(result.info.speculationSummary.get, input.info.speculationSummary.get)
    }
    assert(result.info.killedTasksSummary == input.info.killedTasksSummary)
    assert(result.info.resourceProfileId == input.info.resourceProfileId)
    assert(result.info.peakExecutorMetrics.isDefined == input.info.peakExecutorMetrics.isDefined)
    if (result.info.peakExecutorMetrics.isDefined && input.info.peakExecutorMetrics.isDefined) {
      checkAnswer(result.info.peakExecutorMetrics.get, input.info.peakExecutorMetrics.get)
    }
    assert(result.info.taskMetricsDistributions.isDefined ==
      input.info.taskMetricsDistributions.isDefined)
    if (result.info.taskMetricsDistributions.isDefined &&
      input.info.taskMetricsDistributions.isDefined) {
      checkAnswer(result.info.taskMetricsDistributions.get, input.info.taskMetricsDistributions.get)
    }
    assert(result.info.executorMetricsDistributions.isDefined ==
      input.info.executorMetricsDistributions.isDefined)
    if (result.info.executorMetricsDistributions.isDefined &&
      input.info.executorMetricsDistributions.isDefined) {
      checkAnswer(result.info.executorMetricsDistributions.get,
        input.info.executorMetricsDistributions.get)
    }
    assert(result.info.isShufflePushEnabled == input.info.isShufflePushEnabled)
    assert(result.info.shuffleMergersCount == input.info.shuffleMergersCount)
  }

  test("AppSummary") {
    val input = new AppSummary(
      numCompletedJobs = 10,
      numCompletedStages = 20
    )
    val bytes = serializer.serialize(input)
    val result = serializer.deserialize(bytes, classOf[AppSummary])
    assert(result.numCompletedJobs == input.numCompletedJobs)
    assert(result.numCompletedStages == input.numCompletedStages)
  }

  test("PoolData") {
    Seq("big-pool", null).foreach { name =>
      val input = new PoolData(
        name = name,
        stageIds = Set(11, 13, 15, 17)
      )
      val bytes = serializer.serialize(input)
      val result = serializer.deserialize(bytes, classOf[PoolData])
      assert(result.name == input.name)
      assert(result.stageIds == input.stageIds)
    }
  }

  private def checkAnswer(result: TaskMetrics, expected: TaskMetrics): Unit = {
    assert(result.executorDeserializeTime == expected.executorDeserializeTime)
    assert(result.executorDeserializeCpuTime == expected.executorDeserializeCpuTime)
    assert(result.executorRunTime == expected.executorRunTime)
    assert(result.executorCpuTime == expected.executorCpuTime)
    assert(result.resultSize == expected.resultSize)
    assert(result.jvmGcTime == expected.jvmGcTime)
    assert(result.resultSerializationTime == expected.resultSerializationTime)
    assert(result.memoryBytesSpilled == expected.memoryBytesSpilled)
    assert(result.diskBytesSpilled == expected.diskBytesSpilled)
    assert(result.peakExecutionMemory == expected.peakExecutionMemory)
    checkAnswer(result.inputMetrics, expected.inputMetrics)
    checkAnswer(result.outputMetrics, expected.outputMetrics)
    checkAnswer(result.shuffleReadMetrics, expected.shuffleReadMetrics)
    checkAnswer(result.shuffleWriteMetrics, expected.shuffleWriteMetrics)
  }

  private def checkAnswer(result: InputMetrics, expected: InputMetrics): Unit = {
    assert(result.bytesRead == expected.bytesRead)
    assert(result.recordsRead == expected.recordsRead)
  }

  private def checkAnswer(result: OutputMetrics, expected: OutputMetrics): Unit = {
    assert(result.bytesWritten == expected.bytesWritten)
    assert(result.recordsWritten == expected.recordsWritten)
  }

  private def checkAnswer(result: ShuffleReadMetrics, expected: ShuffleReadMetrics): Unit = {
    assert(result.remoteBlocksFetched == expected.remoteBlocksFetched)
    assert(result.localBlocksFetched == expected.localBlocksFetched)
    assert(result.fetchWaitTime == expected.fetchWaitTime)
    assert(result.remoteBytesRead == expected.remoteBytesRead)
    assert(result.remoteBytesReadToDisk == expected.remoteBytesReadToDisk)
    assert(result.localBytesRead == expected.localBytesRead)
    assert(result.recordsRead == expected.recordsRead)
    assert(result.remoteReqsDuration == expected.remoteReqsDuration)
    checkAnswer(result.shufflePushReadMetrics, expected.shufflePushReadMetrics)
  }

  private def checkAnswer(result: ShufflePushReadMetrics,
      expected: ShufflePushReadMetrics): Unit = {
    assert(result.corruptMergedBlockChunks == expected.corruptMergedBlockChunks)
    assert(result.mergedFetchFallbackCount == expected.mergedFetchFallbackCount)
    assert(result.remoteMergedBlocksFetched == expected.remoteMergedBlocksFetched)
    assert(result.localMergedBlocksFetched == expected.localMergedBlocksFetched)
    assert(result.remoteMergedChunksFetched == expected.remoteMergedChunksFetched)
    assert(result.localMergedChunksFetched == expected.localMergedChunksFetched)
    assert(result.remoteMergedBytesRead == expected.remoteMergedBytesRead)
    assert(result.localMergedBytesRead == expected.localMergedBytesRead)
    assert(result.remoteMergedReqsDuration == expected.remoteMergedReqsDuration)
  }

  private def checkAnswer(result: ShuffleWriteMetrics, expected: ShuffleWriteMetrics): Unit = {
    assert(result.bytesWritten == expected.bytesWritten)
    assert(result.writeTime == expected.writeTime)
    assert(result.recordsWritten == expected.recordsWritten)
  }

  private def checkAnswer(result: collection.Seq[AccumulableInfo],
      expected: collection.Seq[AccumulableInfo]): Unit = {
    assert(result.length == expected.length)
    result.zip(expected).foreach { case (a1, a2) =>
      assert(a1.id == a2.id)
      assert(a1.name == a2.name)
      assert(a1.update.getOrElse("") == a2.update.getOrElse(""))
      assert(a1.update == a2.update)
      assert(a1.value == a2.value)
    }
  }

  private def checkIdTask(result: Map[Long, TaskData], expected: Map[Long, TaskData]): Unit = {
    assert(result.size == expected.size)
    assert(result.keys.size == expected.keys.size)
    result.keysIterator.foreach { k =>
      assert(expected.contains(k))
      checkAnswer(result(k), expected(k))
    }
  }

  private def checkAnswer(result: TaskData, expected: TaskData): Unit = {
    assert(result.taskId == expected.taskId)
    assert(result.index == expected.index)
    assert(result.attempt == expected.attempt)
    assert(result.partitionId == expected.partitionId)
    assert(result.launchTime == expected.launchTime)
    assert(result.resultFetchStart == expected.resultFetchStart)
    assert(result.duration == expected.duration)
    assert(result.executorId == expected.executorId)
    assert(result.host == expected.host)
    assert(result.status == expected.status)
    assert(result.taskLocality == expected.taskLocality)
    assert(result.speculative == expected.speculative)
    checkAnswer(result.accumulatorUpdates, expected.accumulatorUpdates)
    assert(result.errorMessage == expected.errorMessage)
    assert(result.taskMetrics.isDefined == expected.taskMetrics.isDefined)
    if (result.taskMetrics.isDefined && expected.taskMetrics.isDefined) {
      checkAnswer(result.taskMetrics.get, expected.taskMetrics.get)
    }
  }

  private def checkAnswer(result: Map[String, ExecutorStageSummary],
      expected: Map[String, ExecutorStageSummary]): Unit = {
    assert(result.size == expected.size)
    assert(result.keys.size == expected.keys.size)
    result.keysIterator.foreach { k =>
      assert(expected.contains(k))
      checkAnswer(result(k), expected(k))
    }
  }

  private def checkAnswer(result: ExecutorStageSummary,
      expected: ExecutorStageSummary): Unit = {
    assert(result.taskTime == expected.taskTime)
    assert(result.failedTasks == expected.failedTasks)
    assert(result.succeededTasks == expected.succeededTasks)
    assert(result.killedTasks == expected.killedTasks)
    assert(result.inputBytes == expected.inputBytes)
    assert(result.inputRecords == expected.inputRecords)
    assert(result.outputBytes == expected.outputBytes)
    assert(result.outputRecords == expected.outputRecords)
    assert(result.shuffleRead == expected.shuffleRead)
    assert(result.shuffleReadRecords == expected.shuffleReadRecords)
    assert(result.shuffleWrite == expected.shuffleWrite)
    assert(result.shuffleWriteRecords == expected.shuffleWriteRecords)
    assert(result.memoryBytesSpilled == expected.memoryBytesSpilled)
    assert(result.diskBytesSpilled == expected.diskBytesSpilled)
    assert(result.isBlacklistedForStage == expected.isBlacklistedForStage)
    assert(result.isExcludedForStage == expected.isExcludedForStage)
    assert(result.peakMemoryMetrics.isDefined == expected.peakMemoryMetrics.isDefined)
    if (result.peakMemoryMetrics.isDefined && expected.peakMemoryMetrics.isDefined) {
      checkAnswer(result.peakMemoryMetrics.get, expected.peakMemoryMetrics.get)
    }
  }

  private def checkAnswer(result: SpeculationStageSummary,
      expected: SpeculationStageSummary): Unit = {
    assert(result.numTasks == expected.numTasks)
    assert(result.numActiveTasks == expected.numActiveTasks)
    assert(result.numCompletedTasks == expected.numCompletedTasks)
    assert(result.numFailedTasks == expected.numFailedTasks)
    assert(result.numKilledTasks == expected.numKilledTasks)
  }

  private def checkAnswer(result: ExecutorMetrics, expected: ExecutorMetrics): Unit = {
    ExecutorMetricType.metricToOffset.foreach { case (name, _) =>
      result.getMetricValue(name) == expected.getMetricValue(name)
    }
  }

  private def checkAnswer(result: TaskMetricDistributions,
      expected: TaskMetricDistributions): Unit = {
    assert(result.quantiles == expected.quantiles)
    assert(result.duration == expected.duration)
    assert(result.executorDeserializeTime == expected.executorDeserializeTime)
    assert(result.executorDeserializeCpuTime == expected.executorDeserializeCpuTime)
    assert(result.executorRunTime == expected.executorRunTime)
    assert(result.executorCpuTime == expected.executorCpuTime)
    assert(result.resultSize == expected.resultSize)
    assert(result.jvmGcTime == expected.jvmGcTime)
    assert(result.resultSerializationTime == expected.resultSerializationTime)
    assert(result.gettingResultTime == expected.gettingResultTime)
    assert(result.schedulerDelay == expected.schedulerDelay)
    assert(result.peakExecutionMemory == expected.peakExecutionMemory)
    assert(result.memoryBytesSpilled == expected.memoryBytesSpilled)
    assert(result.diskBytesSpilled == expected.diskBytesSpilled)

    checkAnswer(result.inputMetrics, expected.inputMetrics)
    checkAnswer(result.outputMetrics, expected.outputMetrics)
    checkAnswer(result.shuffleReadMetrics, expected.shuffleReadMetrics)
    checkAnswer(result.shuffleWriteMetrics, expected.shuffleWriteMetrics)
  }

  private def checkAnswer(result: InputMetricDistributions,
      expected: InputMetricDistributions): Unit = {
    assert(result.bytesRead == expected.bytesRead)
    assert(result.recordsRead == expected.recordsRead)
  }

  private def checkAnswer(result: OutputMetricDistributions,
      expected: OutputMetricDistributions): Unit = {
    assert(result.bytesWritten == expected.bytesWritten)
    assert(result.recordsWritten == expected.recordsWritten)
  }

  private def checkAnswer(result: ShuffleReadMetricDistributions,
      expected: ShuffleReadMetricDistributions): Unit = {
    assert(result.readBytes == expected.readBytes)
    assert(result.readRecords == expected.readRecords)
    assert(result.remoteBlocksFetched == expected.remoteBlocksFetched)
    assert(result.localBlocksFetched == expected.localBlocksFetched)
    assert(result.fetchWaitTime == expected.fetchWaitTime)
    assert(result.remoteBytesRead == expected.remoteBytesRead)
    assert(result.remoteBytesReadToDisk == expected.remoteBytesReadToDisk)
    assert(result.totalBlocksFetched == expected.totalBlocksFetched)
    assert(result.remoteReqsDuration == expected.remoteReqsDuration)
    checkAnswer(result.shufflePushReadMetricsDist, expected.shufflePushReadMetricsDist)
  }

  private def checkAnswer(result: ShufflePushReadMetricDistributions,
      expected: ShufflePushReadMetricDistributions): Unit = {
    assert(result.corruptMergedBlockChunks == expected.corruptMergedBlockChunks)
    assert(result.mergedFetchFallbackCount == expected.mergedFetchFallbackCount)
    assert(result.remoteMergedBlocksFetched == expected.remoteMergedBlocksFetched)
    assert(result.localMergedBlocksFetched == expected.localMergedBlocksFetched)
    assert(result.remoteMergedChunksFetched == expected.remoteMergedChunksFetched)
    assert(result.localMergedChunksFetched == expected.localMergedChunksFetched)
    assert(result.remoteMergedBytesRead == expected.remoteMergedBytesRead)
    assert(result.localMergedBytesRead == expected.localMergedBytesRead)
    assert(result.remoteMergedReqsDuration == expected.remoteMergedReqsDuration)
  }

  private def checkAnswer(result: ShuffleWriteMetricDistributions,
      expected: ShuffleWriteMetricDistributions): Unit = {
    assert(result.writeBytes == expected.writeBytes)
    assert(result.writeRecords == expected.writeRecords)
    assert(result.writeTime == expected.writeTime)
  }

  private def checkAnswer(result: ExecutorMetricsDistributions,
      expected: ExecutorMetricsDistributions): Unit = {
    assert(result.quantiles == expected.quantiles)

    assert(result.taskTime == expected.taskTime)
    assert(result.failedTasks == expected.failedTasks)
    assert(result.succeededTasks == expected.succeededTasks)
    assert(result.killedTasks == expected.killedTasks)
    assert(result.inputBytes == expected.inputBytes)
    assert(result.inputRecords == expected.inputRecords)
    assert(result.outputBytes == expected.outputBytes)
    assert(result.outputRecords == expected.outputRecords)
    assert(result.shuffleRead == expected.shuffleRead)
    assert(result.shuffleReadRecords == expected.shuffleReadRecords)
    assert(result.shuffleWrite == expected.shuffleWrite)
    assert(result.shuffleWriteRecords == expected.shuffleWriteRecords)
    assert(result.memoryBytesSpilled == expected.memoryBytesSpilled)
    assert(result.diskBytesSpilled == expected.diskBytesSpilled)
    checkAnswer(result.peakMemoryMetrics, expected.peakMemoryMetrics)
  }

  private def checkAnswer(result: ExecutorPeakMetricsDistributions,
      expected: ExecutorPeakMetricsDistributions): Unit = {
    assert(result.quantiles == expected.quantiles)
    assert(result.executorMetrics.size == expected.executorMetrics.size)
    result.executorMetrics.zip(expected.executorMetrics).foreach { case (a1, a2) =>
      checkAnswer(a1, a2)
    }
  }
}
