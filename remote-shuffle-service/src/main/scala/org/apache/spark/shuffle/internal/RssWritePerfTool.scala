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

import com.google.common.primitives.Longs
import org.apache.commons.lang3.StringUtils
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.remoteshuffle.clients._
import org.apache.spark.remoteshuffle.common._
import org.apache.spark.remoteshuffle.metadata.ServiceRegistry
import org.apache.spark.remoteshuffle.storage.ShuffleFileStorage
import org.apache.spark.remoteshuffle.{StreamServer, StreamServerConfig}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.shuffle._
import org.apache.spark.{HashPartitioner, MapOutputTrackerMaster, Partitioner, ShuffleDependency, SparkConf, SparkContext, SparkEnv}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util
import java.util.Random
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Consumer
import scala.collection.JavaConverters._

class RssWritePerfToolPartitioner(val partitions: Int) extends Partitioner {
  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    val b = key.asInstanceOf[Array[Byte]]
    val prefix = Longs.fromBytes(0, b(0), b(1), b(2), b(3), b(4), b(5), b(6))
    (prefix % numPartitions).toInt
  }
}

/** *
 * This is a stress tool to start multiple shuffle servers and write shuffle records.
 */
class RssWritePerfTool extends Logging {
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
  private var workDir = Files.createTempDirectory("rss_").toFile().getAbsolutePath()

  private var numServerThreads = 5
  private var appId = "app_" + System.nanoTime
  private var appAttempt = "exec1"
  private var appShuffleId = new AppShuffleId(appId, appAttempt, 1)

  // Number of servers
  var numServers = 4
  // Number of total map tasks
  var numMaps = 10
  // Number of records in each map tasks
  var numMapRecords = 100
  // Number of total partitions
  var numPartitions = 100
  // Number of splits for shuffle file
  var numSplits = 1
  // Writer buffer size to use
  var writerBufferSize = RssOpts.writerBufferSize.defaultValue.get
  // Writer buffer spill size to use
  var writerBufferSpill = RssOpts.writerBufferSpill.defaultValue.get

  // This tool generates a range of map tasks to simulate uploading data.
  // This field specifies the lower bound (inclusive) of the map id.
  private var startMapId = 0
  // This field specifies the upper bound (inclusive) of the map id.
  private var endMapId = numMaps - 1
  // Number of servers (or replication groups) per partition
  private var partitionFanout = 1
  // Number of shuffle data replicas
  private var numReplicas = 1
  // Whether to use connection pool
  private var useConnectionPool = false
  private var writeClientQueueSize = 0
  private var writeClientThreads = 4

  // Total number of test values to use. This tool wil generate a list of test values and use them
  // to fill shuffle data.
  private var numTestValues = 1000
  // Max length for test values to use. This tool wil generate a list of test values and use them
  private var testValueLen = 100

  private var sparkConf: SparkConf = null
  private var sparkContext: SparkContext = null
  private var mapOutputTrackerMaster: MapOutputTrackerMaster = null
  private var shuffleDependency: ShuffleDependency[Array[Byte], Array[Byte], Array[Byte]] = null

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

    val rdd = sparkContext.parallelize(1 to numMaps, numMaps)
      .map(t => (t.toString.getBytes(StandardCharsets.UTF_8)
        -> t.toString.getBytes(StandardCharsets.UTF_8)))
      .partitionBy(new RssWritePerfToolPartitioner(numPartitions))
    shuffleDependency = new ShuffleDependency[Array[Byte], Array[Byte], Array[Byte]](
      rdd, rdd.partitioner.get)
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
    val testValues = new util.ArrayList[Array[Byte]]
    while (testValues.size < numTestValues) {
      val ch = ('a' + random.nextInt(26)).toChar
      val str = StringUtils.repeat(ch, testValueLen)
      testValues.add(str.getBytes(StandardCharsets.UTF_8))
    }

    var mapId = startMapId
    while (mapId <= endMapId) {
      val appMapId = new AppMapId(appShuffleId.getAppId, appShuffleId.getAppAttempt,
        appShuffleId.getShuffleId, mapId)
      val thread = new Thread(new Runnable {
        override def run(): Unit = {
          val taskAttemptId = taskAttemptIdSeed.getAndIncrement
          simulateMapperTask(testValues, appMapId, taskAttemptId)
        }
      })
      thread.setName(String.format("[Map Thread %s]", appMapId))
      thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
        override def uncaughtException(t: Thread, e: Throwable): Unit = {
          logError(String.format("Mapper thread %s got exception", t.getName), e)
          e.printStackTrace()
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

  private def simulateMapperTask(testValues: util.List[Array[Byte]], appMapId: AppMapId,
                                 taskAttemptId: Long): Unit = {
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

    val recordIterator = Iterator.tabulate(numMapRecords) { t =>
      val index1 = t % testValues.size()
      val index2 = (t + 1) % testValues.size()
      (testValues.get(index1), testValues.get(index2))
    }

    shuffleWriter.write(recordIterator)
    successShuffleWrittenRecords.addAndGet(numMapRecords)

    val mapStatus = shuffleWriter.stop(true)
    mapOutputTrackerMaster
      .registerMapOutput(appShuffleId.getShuffleId, appMapId.getMapId.intValue(), mapStatus.get)

    // TODO simulate broken map tasks without proper closing
    logInfo(s"Map $appMapId attempt $taskAttemptId finished")
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

object RssWritePerfTool extends Logging {

  def main(args: Array[String]): Unit = {
    runOnce()
  }

  private def runOnce(): Unit = {
    val tool = new RssWritePerfTool()

    tool.numServers = 2
    tool.numMaps = 4
    tool.numMapRecords = 30000
    tool.numPartitions = 3000
    tool.numSplits = 1

    tool.writerBufferSize = RssOpts.writerBufferSize.defaultValue.get
    tool.writerBufferSpill = RssOpts.writerBufferSpill.defaultValue.get

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