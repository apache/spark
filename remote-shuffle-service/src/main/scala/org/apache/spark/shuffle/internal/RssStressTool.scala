/*
 * This file is copied from Uber Remote Shuffle Service
(https://github.com/uber/RemoteShuffleService) and modified.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.internal

import java.nio.file.Paths
import java.util
import java.util.Random
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Consumer

import org.apache.commons.lang3.StringUtils
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.remoteshuffle.{StreamServer, StreamServerConfig}
import org.apache.spark.remoteshuffle.clients.{MultiServerAsyncWriteClient, MultiServerSyncWriteClient, ServerReplicationGroupUtil, ShuffleDataWriter, ShuffleWriteConfig}
import org.apache.spark.remoteshuffle.common.{AppMapId, AppShuffleId, AppTaskAttemptId, ServerDetail, ServerList}
import org.apache.spark.remoteshuffle.metadata.ServiceRegistry
import org.apache.spark.remoteshuffle.storage.ShuffleFileStorage
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.shuffle._
import org.apache.spark.{HashPartitioner, MapOutputTrackerMaster, ShuffleDependency, SparkConf, SparkContext, SparkEnv}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/** *
 * This is a stress tool to start multiple shuffle servers and write/read shuffle records.
 */
class RssStressTool extends Logging {
  private var registryServer: StreamServer = null
  private val servers = new util.ArrayList[StreamServer]
  private val serverDetails = new util.ArrayList[ServerDetail]

  private val random = new Random
  private val storage = new ShuffleFileStorage()

  // Successfully written records (by last mapper task attempt) in shuffle files
  private val successShuffleWrittenRecords = new AtomicLong

  // Threads for all map tasks
  private val allMapThreads = new util.ArrayList[Thread]
  private val mapThreadErrors = new AtomicLong

  private var serverRootDirs = new util.ArrayList[String]
  private var workDir = "./tmp/rss"

  private var numServerThreads = 5
  private var appId = "app_" + System.nanoTime
  private var appAttempt = "exec1"
  private var appShuffleId = new AppShuffleId(appId, appAttempt, 1)

  // Number of servers
  var numServers = 4
  // Number of total map tasks
  var numMaps = 10
  // Number of total partitions
  var numPartitions = 7
  // Number of splits for shuffle file
  var numSplits = 3
  // Writer buffer size to use
  var writerBufferSize = RssOpts.writerBufferSize.defaultValue.get
  // Writer buffer spill size to use
  var writerBufferSpill = RssOpts.writerBufferSpill.defaultValue.get

  private var numMapTaskRetries = 3
  // This tool generates a range of map tasks to simulate uploading data.
  // This field specifies the lower bound (inclusive) of the map id.
  private var startMapId = 0
  // This field specifies the upper bound (inclusive) of the map id.
  private var endMapId = numMaps - 1
  // Number of servers (or replication groups) per partition
  private var partitionFanout = 1
  // Number of shuffle data replicas
  private var numReplicas = 1
  // Number of milliseconds we delay a map task to start. This is to simulate different map
  // tasks starting at different time
  private var mapDelay = 1000
  // Whether to use connection pool
  private var useConnectionPool = false
  private var writeClientQueueSize = 0
  private var writeClientThreads = 4

  // Total number of test values to use. This tool wil generate a list of test values and use them
  // to fill shuffle data.
  private var numTestValues = 1000
  // Max length for test values to use. This tool wil generate a list of test values and use them
  private var maxTestValueLen = 100000

  private var sparkConf: SparkConf = null
  private var sparkContext: SparkContext = null
  private var mapOutputTrackerMaster: MapOutputTrackerMaster = null
  private var shuffleDependency: ShuffleDependency[String, String, String] = null

  private val taskAttemptIdSeed = new AtomicLong;

  def setup(): Unit = {
    this.endMapId = this.startMapId + numMaps - 1

    // Start Remote Shuffle Service servers
    registryServer = startNewServer(null)

    (0 until numServers).foreach(i => {
      logInfo(s"Starting new server $i")
      val server = startNewServer(registryServer.getShuffleConnectionString)
      servers.add(server)
      serverRootDirs.add(server.getRootDir)
      serverDetails
        .add(new ServerDetail(server.getServerId, s"localhost:${server.getShufflePort}"))
    })

    // Set up Spark environment
    sparkConf = new SparkConf().setAppName("testApp")
      .setMaster(s"local[2]")
      .set("spark.ui.enabled", "false")
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.app.id", appId)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.RssShuffleManager")
      .set("spark.shuffle.rss.dataCenter", ServiceRegistry.DEFAULT_DATA_CENTER)
      .set("spark.shuffle.rss.cluster", ServiceRegistry.DEFAULT_TEST_CLUSTER)
      .set("spark.shuffle.rss.serviceRegistry.type", ServiceRegistry.TYPE_STANDALONE)
      .set("spark.shuffle.rss.serviceRegistry.server", registryServer.getShuffleConnectionString)
      .set("spark.shuffle.rss.networkTimeout", "30000")
      .set("spark.shuffle.rss.networkRetries", "0")
      .set("spark.shuffle.rss.maxWaitTime", "10000")
      .set("spark.shuffle.rss.reader.dataAvailableWaitTime", "30000")

    sparkContext = new SparkContext(sparkConf)

    mapOutputTrackerMaster = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    mapOutputTrackerMaster.registerShuffle(appShuffleId.getShuffleId, numMaps)

    val rdd = sparkContext.parallelize(1 to 100, numMaps)
      .map(t => (t.toString -> t.toString))
      .partitionBy(new HashPartitioner(numPartitions))
    shuffleDependency = new ShuffleDependency[String, String, String](rdd, rdd.partitioner.get)
  }

  def cleanup(): Unit = {
    sparkContext.stop()

    servers.forEach(new Consumer[StreamServer] {
      override def accept(t: StreamServer): Unit = {
        if (t != null) {
          shutdownServer(t)
        }
      }
    })

    if (registryServer != null) {
      shutdownServer(registryServer)
    }

    logInfo(String.format("Deleting files: %s", StringUtils.join(serverRootDirs, ", ")))
    deleteDirectories(serverRootDirs)
    logInfo(String.format("Deleted files: %s", StringUtils.join(serverRootDirs, ", ")))
  }

  def run(): Unit = {
    logInfo(String.format("Server root dirs: %s", StringUtils.join(serverRootDirs, ':')))

    // Generate test values to use
    val testValues = new util.ArrayList[String]
    testValues.add(null)
    testValues.add("")
    while (testValues.size < numTestValues) {
      val ch = ('a' + random.nextInt(26)).toChar
      val repeats = random.nextInt(maxTestValueLen)
      val str = StringUtils.repeat(ch, repeats)
      testValues.add(str)
    }

    // Create map task threads to write shuffle data
    val simulatedNumberOfAttemptsForMappers = new util.ArrayList[Integer]
    val fetchTaskAttemptIds = new util.ArrayList[Long]
    var i = startMapId
    while (i <= endMapId) {
      val value = random.nextInt(numMapTaskRetries) + 1
      simulatedNumberOfAttemptsForMappers.add(value)
      i += 1
    }
    var mapId = startMapId
    while (mapId <= endMapId) {
      val index = mapId - startMapId
      val appMapId = new AppMapId(appShuffleId.getAppId, appShuffleId.getAppAttempt,
        appShuffleId.getShuffleId, mapId)
      val simulatedNumberOfAttempts = simulatedNumberOfAttemptsForMappers.get(index)
      val thread = new Thread(new Runnable {
        override def run(): Unit = {
          var attempt = 1
          while (attempt <= simulatedNumberOfAttempts) {
            val taskAttemptId = taskAttemptIdSeed.getAndIncrement
            val isLastTaskAttempt = attempt == simulatedNumberOfAttempts
            if (isLastTaskAttempt) {
              fetchTaskAttemptIds.synchronized {
                fetchTaskAttemptIds.add(taskAttemptId)
              }
            }
            simulateMapperTask(testValues, appMapId, taskAttemptId, isLastTaskAttempt)
            attempt += 1;
          }
        }
      })
      thread.setName(String.format("[Map Thread %s]", appMapId))
      thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
        override def uncaughtException(t: Thread, e: Throwable): Unit = {
          logError(String.format("Mapper thread %s got exception", t.getName), e)
          mapThreadErrors.incrementAndGet
        }
      })
      allMapThreads.add(thread)

      mapId += 1;
    }

    // Start map task threads
    allMapThreads.asScala.foreach((t: Thread) => t.start())
    // Wait for map tasks to finish
    allMapThreads.asScala.foreach((t: Thread) => t.join())

    if (mapThreadErrors.get > 0) {
      throw new RuntimeException("Number of errors in map threads: " + mapThreadErrors)
    }

    // Read shuffle data
    val allReadData = new ListBuffer[Product2[String, String]]()
    (0 until numPartitions).foreach(p => {
      allReadData.appendAll(readShuffleData(p))
    })

    logInfo(s"Total read records: ${allReadData.size}")

    allReadData.foreach(t => {
      if (!testValues.contains(t._1)) {
        throw new RuntimeException(s"Detected failure, read unexpected record key: ${t._1}")
      }
      if (!testValues.contains(t._2)) {
        throw new RuntimeException(s"Detected failure, read unexpected record value: ${t._2}")
      }
    })

    val expectedNumRecords = testValues.size() * numMaps
    if (allReadData.size != expectedNumRecords) {
      throw new RuntimeException(
        s"Detected failure, expected records: $expectedNumRecords, actual records: ${
          allReadData.size
        }")
    }

    logInfo("Test run finished successfully")
  }

  private def startNewServer(registryServer: String): StreamServer = {
    var serverDirName = s"server_${System.nanoTime}"
    var serverDirFullPath = Paths.get(workDir, serverDirName).toString
    while ( {
      storage.exists(serverDirFullPath)
    }) {
      serverDirName = s"server_${System.nanoTime}"
      serverDirFullPath = Paths.get(workDir, serverDirName).toString
    }
    val serverConfig = new StreamServerConfig
    serverConfig.setNettyAcceptThreads(numServerThreads)
    serverConfig.setNettyWorkerThreads(numServerThreads)
    serverConfig.setStorage(storage)
    serverConfig.setShufflePort(0)
    serverConfig.setHttpPort(0)
    serverConfig.setRootDirectory(serverDirFullPath)
    serverConfig.setDataCenter(ServiceRegistry.DEFAULT_DATA_CENTER)
    serverConfig.setCluster(ServiceRegistry.DEFAULT_TEST_CLUSTER)
    serverConfig.setAppMemoryRetentionMillis(TimeUnit.HOURS.toMillis(1))

    serverConfig.setServiceRegistryType(ServiceRegistry.TYPE_STANDALONE)

    if (registryServer != null) {
      serverConfig.setRegistryServer(registryServer)
    }

    val server = new StreamServer(serverConfig)
    server.run()
    logInfo(s"Started server, port: ${
      server.getShufflePort
    }, rootDir: $serverDirFullPath, $serverConfig")
    server
  }

  private def simulateMapperTask(testValues: util.List[String], appMapId: AppMapId,
                                 taskAttemptId: Long, isLastTaskAttempt: Boolean): Unit = {
    if (mapDelay > 0) {
      val delayMillis = random.nextInt(mapDelay)
      logInfo(s"Delaying map $appMapId: $delayMillis")
      Thread.sleep(delayMillis)
    }

    val shuffleWriteConfig = new ShuffleWriteConfig(numSplits.toShort)

    var writeClient: ShuffleDataWriter = null
    val networkTimeoutMillis = 120 * 1000
    val maxTryingMillis = networkTimeoutMillis * 3
    val serverReplicationGroups = ServerReplicationGroupUtil
      .createReplicationGroups(serverDetails, numReplicas)
    val finishUploadAck = true // TODO make this configurable

    if (writeClientQueueSize == 0) {
      val aClient = new MultiServerSyncWriteClient(serverReplicationGroups, partitionFanout,
        networkTimeoutMillis, maxTryingMillis, finishUploadAck, useConnectionPool, "user1", appId,
        appAttempt, shuffleWriteConfig)
      aClient.connect()
      writeClient = aClient
    }
    else {
      val aClient = new MultiServerAsyncWriteClient(serverReplicationGroups, partitionFanout,
        networkTimeoutMillis, maxTryingMillis, finishUploadAck, useConnectionPool,
        writeClientQueueSize, writeClientThreads, "user1", appId, appAttempt, shuffleWriteConfig)
      aClient.connect()
      writeClient = aClient
    }

    val shuffleWriter = new RssShuffleWriter(
      rssServers = new ServerList(serverDetails),
      writeClient = writeClient,
      mapInfo = new AppTaskAttemptId(appMapId, taskAttemptId),
      serializer = new KryoSerializer(sparkConf),
      bufferOptions = BufferManagerOptions(writerBufferSize,
        256 * 1024 * 1024,
        writerBufferSpill,
        false),
      shuffleDependency = shuffleDependency,
      shuffleWriteMetrics = new ShuffleWriteMetrics()
    )

    logInfo(s"Map $appMapId attempt $taskAttemptId started, write client: $writeClient")

    val records = testValues.asScala.map(t => {
      val index = random.nextInt(testValues.size())
      (t, testValues.get(index))
    })

    shuffleWriter.write(records.iterator)
    if (isLastTaskAttempt) {
      successShuffleWrittenRecords.addAndGet(records.size)
    }

    val mapStatus = shuffleWriter.stop(true)
    mapOutputTrackerMaster
      .registerMapOutput(appShuffleId.getShuffleId, appMapId.getMapId.intValue(), mapStatus.get)

    // TODO simulate broken map tasks without proper closing
    logInfo(s"Map $appMapId attempt $taskAttemptId finished")
  }

  private def readShuffleData(readPartitionId: Int): Seq[Product2[String, String]] = {
    val shuffleReader = new RssShuffleReader[String, String](
      user = "user1",
      shuffleInfo = appShuffleId,
      startMapIndex = 0,
      endMapIndex = Integer.MAX_VALUE,
      startPartition = readPartitionId,
      endPartition = readPartitionId + 1,
      serializer = shuffleDependency.serializer,
      context = new MockTaskContext(1, 0, taskAttemptIdSeed.incrementAndGet()),
      shuffleDependency = shuffleDependency,
      rssServers = new ServerList(serverDetails),
      partitionFanout = 1,
      timeoutMillis = 30000,
      maxRetryMillis = 60000,
      dataAvailablePollInterval = 1000,
      dataAvailableWaitTime = 30000,
      shuffleReplicas = 1,
      checkShuffleReplicaConsistency = true,
      shuffleMetrics = new ShuffleReadMetricsReporter() {
        override private[spark] def incRemoteBlocksFetched(v: Long): Unit = {}

        override private[spark] def incLocalBlocksFetched(v: Long): Unit = {}

        override private[spark] def incRemoteBytesRead(v: Long): Unit = {}

        override private[spark] def incRemoteBytesReadToDisk(v: Long): Unit = {}

        override private[spark] def incLocalBytesRead(v: Long): Unit = {}

        override private[spark] def incFetchWaitTime(v: Long): Unit = {}

        override private[spark] def incRecordsRead(v: Long): Unit = {}
      }
    )

    val shuffleReaderIterator = shuffleReader.read()
    val readRecords = shuffleReaderIterator.toList
    logInfo(s"Read ${readRecords.size} records for partition $readPartitionId")
    readRecords
  }

  private def shutdownServer(server: StreamServer): Unit = {
    logInfo(String.format("Shutting down server: %s", server))
    server.shutdown(true)
  }

  private def deleteDirectories(directories: util.List[String]): Unit = {
    directories.stream().forEach(new Consumer[String] {
      override def accept(t: String): Unit = {
        logInfo("Deleting directory: " + t)
        if (!storage.exists(t)) {
          logInfo("Directory not exist: " + t)
        }
        else {
          storage.deleteDirectory(t)
          logInfo("Deleted directory: " + t)
        }
      }
    })
  }
}

object RssStressTool extends Logging {

  def main(args: Array[String]): Unit = {
    var runMinutes = 1
    var maxTestValueLen = 100000

    var i = 0
    while (i < args.length) {
      val argName = args(i)
      i += 1
      if (argName.equalsIgnoreCase("-runMinutes")) {
        runMinutes = args(i).toInt
        i += 1
      } else if (argName.equalsIgnoreCase("-maxTestValueLen")) {
        maxTestValueLen = args(i).toInt
        i += 1
      } else {
        throw new RuntimeException(s"Invalid argument: $argName")
      }
    }

    val startTime = System.currentTimeMillis()
    while (System.currentTimeMillis() - startTime < runMinutes * 60 * 1000) {
      runOnce()
    }
  }

  private def runOnce(): Unit = {
    val random = new Random()

    val tool = new RssStressTool()

    tool.numServers = 1 + random.nextInt(10)
    tool.numMaps = 1 + random.nextInt(10)
    tool.numPartitions = 1 + random.nextInt(10)
    tool.numSplits = 1 + random.nextInt(10)

    tool.writerBufferSize = 1 + random.nextInt(64 * 1024)
    tool.writerBufferSpill = tool.writerBufferSize + random.nextInt(256 * 1024)

    logInfo(s"Running test, numServers: ${tool.numServers}, " +
      s"numMaps: ${tool.numMaps}, numPartitions: ${tool.numPartitions}, numSplits: ${
        tool.numSplits
      }, " +
      s"writerBufferSize: ${tool.writerBufferSize}, writerBufferSpill: ${tool.writerBufferSpill}")

    try {
      tool.setup()
      tool.run()
    } finally {
      tool.cleanup()
    }
  }
}