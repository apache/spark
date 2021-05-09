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

import org.apache.commons.lang3.StringUtils
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.remoteshuffle.clients.{MultiServerAsyncWriteClient, MultiServerSyncWriteClient, ServerReplicationGroupUtil, ShuffleDataWriter, ShuffleWriteConfig}
import org.apache.spark.remoteshuffle.common._
import org.apache.spark.remoteshuffle.metadata.ServiceRegistry
import org.apache.spark.remoteshuffle.storage.ShuffleFileStorage
import org.apache.spark.remoteshuffle.{StreamServer, StreamServerConfig}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.shuffle._
import org.apache.spark.{MapOutputTrackerMaster, ShuffleDependency, SparkConf, SparkContext, SparkEnv}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util
import java.util.concurrent.TimeUnit
import java.util.function.Consumer

class RssWritePerfTool extends ShuffleWritePerfTool with Logging {
  private var registryServer: StreamServer = null
  private val servers = new util.ArrayList[StreamServer]
  private val serverDetails = new util.ArrayList[ServerDetail]

  private val storage = new ShuffleFileStorage()

  private var serverRootDirs = new util.ArrayList[String]
  private var workDir = Files.createTempDirectory("rss_").toFile().getAbsolutePath()

  private var numServerThreads = 2

  // Number of servers
  var numServers = 4
  // Number of splits for shuffle file
  var numSplits = 1
  // Writer buffer size to use
  var writerBufferSize = RssOpts.writerBufferSize.defaultValue.get
  // Writer buffer spill size to use
  var writerBufferSpill = RssOpts.writerBufferSpill.defaultValue.get

  // Number of shuffle data replicas
  private var numReplicas = 1
  // Whether to use connection pool
  private var useConnectionPool = false
  private var writeClientQueueSize = 0
  private var writeClientThreads = 4

  override def createShuffleWriter(
      shuffleId: Int,
      shuffleDependency: ShuffleDependency[Array[Byte], Array[Byte], Array[Byte]],
      appMapId: AppMapId,
      taskAttemptId: Long):
  ShuffleWriter[Array[Byte], Array[Byte]] = {
    val shuffleWriteConfig = new ShuffleWriteConfig(numSplits.toShort)
    val partitionFanout = 1

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

    new RssShuffleWriter(
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
  }

  override def setup(): Unit = {
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
      .partitionBy(new ShuffleWritePerfToolPartitioner(numPartitions))
    shuffleDependency = new ShuffleDependency[Array[Byte], Array[Byte], Array[Byte]](
      rdd, rdd.partitioner.get)
  }

  override def cleanup(): Unit = {
    if (sparkContext != null) {
      sparkContext.stop()
    }

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
    try {
      tool.setup()
      tool.run()
    } finally {
      tool.cleanup()
    }
  }
}