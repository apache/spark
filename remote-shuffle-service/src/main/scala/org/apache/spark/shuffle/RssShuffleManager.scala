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

package org.apache.spark.shuffle

import java.util
import java.util.Random
import java.util.function.Supplier

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.remoteshuffle.RssBuildInfo
import org.apache.spark.remoteshuffle.clients.{MultiServerAsyncWriteClient, MultiServerHeartbeatClient, MultiServerSyncWriteClient, MultiServerWriteClient, PooledWriteClientFactory, ServerConnectionStringCache, ServerConnectionStringResolver, ServerReplicationGroupUtil, ShuffleWriteConfig}
import org.apache.spark.remoteshuffle.common.{AppShuffleId, AppTaskAttemptId, ServerDetail, ServerList}
import org.apache.spark.remoteshuffle.exceptions.{RssException, RssInvalidStateException, RssNoServerAvailableException, RssServerResolveException}
import org.apache.spark.remoteshuffle.metadata.{ServerSequenceServiceRegistry, ServiceRegistry, ServiceRegistryUtils, StandaloneServiceRegistryClient}
import org.apache.spark.remoteshuffle.metrics.M3Stats
import org.apache.spark.remoteshuffle.util.{ExceptionUtils, RetryUtils, ServerHostAndPort, ThreadUtils}
import org.apache.spark.shuffle.internal.{BufferManagerOptions, RssSparkListener, RssUtils}

import scala.collection.JavaConverters

class RssShuffleManager(conf: SparkConf) extends ShuffleManager with Logging {
  logInfo(s"Creating ShuffleManager instance: ${this.getClass.getSimpleName}, version: ${
    RssBuildInfo.Version
  }, revision: ${RssBuildInfo.Revision}")

  private val SparkYarnQueueConfigKey = "spark.yarn.queue"
  private val NumRssServersMetricName = "numRssServers2"
  private val RssDataCenterTagName = "rssDataCenter"
  private val RssClusterTagName = "rssCluster"
  private val UserMetricTagName = "user"

  private val networkTimeoutMillis = conf.get(RssOpts.networkTimeout).toInt
  private val pollInterval = conf.get(RssOpts.pollInterval)
  private val dataAvailableWaitTime = conf.get(RssOpts.readerDataAvailableWaitTime)

  private val serviceRegistry = createServiceRegistry
  private val dataCenter = getDataCenter
  private val cluster = conf.get(RssOpts.cluster)

  private val executorCores = conf.getInt("spark.executor.cores", 1)

  private val writerBufferSize = conf.get(RssOpts.writerBufferSize)
  private val writerBufferMax = conf.get(RssOpts.writerBufferMax)
  private val writerBufferSpill = conf.get(RssOpts.writerBufferSpill)
  private val writerBufferSpillByEachExecutor = if (executorCores >= 1) {
    writerBufferSpill / executorCores
  } else {
    writerBufferSpill
  }
  private val writerSupportAggregate = conf.get(RssOpts.writerSupportAggregate)
  private val bufferOptions = BufferManagerOptions(
    individualBufferSize = writerBufferSize,
    individualBufferMax = writerBufferMax,
    bufferSpillThreshold = writerBufferSpillByEachExecutor,
    supportAggregate = writerSupportAggregate
  )

  private def getSparkContext = {
    SparkContext.getActive.get
  }

  // This method is called in Spark driver side, and Spark driver will make some
  // decision, e.g. determining what
  // RSS servers to use. Then Spark driver will return a ShuffleHandle and pass
  // that ShuffleHandle to executors (getWriter/getReader).
  override def registerShuffle[K, V, C](shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    // RSS does not support speculation yet, due to the random task attempt ids
    // (finished map task attempt id not always increasing).
    // We will fall back to SortShuffleManager if speculation is configured to true.
    val useSpeculation = conf.getBoolean("spark.speculation", false)
    if (useSpeculation) {
      throw new RssException("Do not support speculation in Remote Shuffle Service")
    }

    logInfo(s"Use ShuffleManager: ${this.getClass().getSimpleName()}")

    val numPartitions = dependency.partitioner.numPartitions;

    val sparkContext = getSparkContext

    val user = sparkContext.sparkUser
    val queue = conf.get(SparkYarnQueueConfigKey, "")

    val appId = conf.getAppId
    val appAttempt = sparkContext.applicationAttemptId.getOrElse("0")

    val heartbeatClient = MultiServerHeartbeatClient.getInstance();
    heartbeatClient.setAppContext(user, appId, appAttempt)

    var rssServerSelectionResult: RssServerSelectionResult = null

    val excludeHostsConfigValue = conf.get(RssOpts.excludeHosts)
    val excludeHosts = excludeHostsConfigValue.split(",").filter(!_.isEmpty).distinct

    rssServerSelectionResult = getRssServers(numPartitions, excludeHosts)
    val rssServers = rssServerSelectionResult.servers
    logInfo(s"Selected ${
      rssServers.size
    } RSS servers for shuffle $shuffleId, partitions: $numPartitions, replicas: ${
      rssServerSelectionResult.replicas
    }, partition fanout: ${rssServerSelectionResult.partitionFanout}, ${rssServers.mkString(",")}")

    val tagMap = new java.util.HashMap[String, String]()
    tagMap.put(RssDataCenterTagName, dataCenter)
    tagMap.put(RssClusterTagName, cluster)
    tagMap.put(UserMetricTagName, user)
    M3Stats.getDefaultScope.tagged(tagMap).gauge(NumRssServersMetricName).update(rssServers.length)

    RssSparkListener.registerSparkListenerOnlyOnce(sparkContext, () =>
      new RssSparkListener(
        user,
        conf.getAppId,
        appAttempt,
        rssServerSelectionResult.servers.map(_.getConnectionString()),
        networkTimeoutMillis))

    val dependencyInfo = s"numPartitions: ${dependency.partitioner.numPartitions}, " +
      s"serializer: ${dependency.serializer.getClass().getSimpleName()}, " +
      s"keyOrdering: ${dependency.keyOrdering}, " +
      s"aggregator: ${dependency.aggregator}, " +
      s"mapSideCombine: ${dependency.mapSideCombine}, " +
      s"keyClassName: ${dependency.keyClassName}, " +
      s"valueClassName: ${dependency.valueClassName}"

    logInfo(s"registerShuffle: $appId, $appAttempt, $shuffleId, $dependencyInfo")

    val rssServerHandles = rssServerSelectionResult.servers
      .map(t => new RssShuffleServerHandle(t.getServerId(), t.getConnectionString())).toArray
    new RssShuffleHandle(shuffleId, appId, appAttempt, user, queue, dependency, rssServerHandles,
      rssServerSelectionResult.partitionFanout)
  }

  // This method is called in Spark executor, getting information from Spark driver via
  // the ShuffleHandle.
  override def getWriter[K, V](handle: ShuffleHandle, mapId: Long, context: TaskContext,
                               metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    logInfo(s"getWriter: Use ShuffleManager: ${
      this.getClass().getSimpleName()
    }, $handle, mapId: $mapId, stageId: ${context.stageId()}, shuffleId: ${handle.shuffleId}")

    handle match {
      case rssShuffleHandle: RssShuffleHandle[K@unchecked, V@unchecked, _] =>
        val writerQueueSize = conf.get(RssOpts.writerQueueSize)

        val mapInfo = new AppTaskAttemptId(
          conf.getAppId,
          rssShuffleHandle.appAttempt,
          handle.shuffleId,
          mapId.intValue(), // TODO Spark 3.0 for mapId.intValue()
          context.taskAttemptId()
        )

        logDebug(s"getWriter $mapInfo")

        val serializer = rssShuffleHandle.dependency.serializer
        val maxWaitMillis = conf.get(RssOpts.maxWaitTime)
        val useConnectionPool = conf.get(RssOpts.useConnectionPool)
        val rssMapsPerSplit = conf.get(RssOpts.mapsPerSplit)
        var rssNumSplits = 1
        val rssMinSplits = conf.get(RssOpts.minSplits)
        val rssMaxSplits = conf.get(RssOpts.maxSplits)
        if (rssNumSplits < rssMinSplits) {
          rssNumSplits = rssMinSplits
        } else if (rssNumSplits > rssMaxSplits) {
          rssNumSplits = rssMaxSplits
        }
        val shuffleWriteConfig = new ShuffleWriteConfig(rssNumSplits.toShort)
        val rssReplicas = conf.get(RssOpts.replicas)
        if (rssReplicas <= 0) {
          throw new RssException(s"Invalid config value for ${RssOpts.replicas.key}: $rssReplicas")
        }
        val rssServers: ServerList = ServerConnectionStringCache.getInstance()
          .getServerList(rssShuffleHandle.getServerList)
        val serverReplicationGroups = ServerReplicationGroupUtil
          .createReplicationGroups(rssServers.getSevers, rssReplicas)

        val serverConnectionResolver = new ServerConnectionStringResolver {
          override def resolveConnection(serverId: String): ServerDetail = {
            val serverDetailInShuffleHandle = rssShuffleHandle.getServerList
              .getSeverDetail(serverId)
            if (serverDetailInShuffleHandle == null) {
              throw new FetchFailedException(
                bmAddress = RssUtils
                  .createMapTaskDummyBlockManagerId(mapInfo.getMapId, mapInfo.getTaskAttemptId),
                shuffleId = rssShuffleHandle.shuffleId,
                mapId = -1,
                mapIndex = -1,
                reduceId = 0,
                message = s"Failed to get server detail for $serverId from shuffle " +
                  s"handle: $rssShuffleHandle")
            }
            // random sleep some time to avoid request spike on service registry
            val random = new Random()
            val randomWaitMillis = random.nextInt(pollInterval)
            ThreadUtils.sleep(randomWaitMillis)
            val lookupResult = serviceRegistry
              .lookupServers(dataCenter, cluster, util.Arrays.asList(serverId))
            if (lookupResult == null) {
              throw new RssServerResolveException(s"Got null when looking up server for $serverId")
            }
            if (lookupResult.size() != 1) {
              throw new RssInvalidStateException(
                s"Invalid result $lookupResult when looking up server for $serverId")
            }
            val refreshedServer: ServerDetail = lookupResult.get(0)
            // add refreshed server into cache so future server lookup from the cache will get
            // latest server.
            ServerConnectionStringCache.getInstance().updateServer(serverId, refreshedServer)
            if (!refreshedServer.equals(serverDetailInShuffleHandle)) {
              throw new FetchFailedException(
                bmAddress = RssUtils
                  .createMapTaskDummyBlockManagerId(mapInfo.getMapId, mapInfo.getTaskAttemptId),
                shuffleId = rssShuffleHandle.shuffleId,
                mapId = -1,
                mapIndex = -1,
                reduceId = 0,
                message = s"Detected server restart, current server: $refreshedServer, " +
                  s"previous server: $serverDetailInShuffleHandle")
            }
            refreshedServer
          }
        }

        val writerAsyncFinish = conf.get(RssOpts.writerAsyncFinish)
        val finishUploadAck = !writerAsyncFinish

        RetryUtils.retry(pollInterval, pollInterval * 10, maxWaitMillis, "create write client",
          new Supplier[ShuffleWriter[K, V]] {
            override def get(): ShuffleWriter[K, V] = {
              val writeClient: MultiServerWriteClient =
                if (writerQueueSize == 0) {
                  logInfo(s"Use replicated sync writer, $rssNumSplits splits, ${
                    rssShuffleHandle.partitionFanout
                  } partition fanout, $serverReplicationGroups, finishUploadAck: $finishUploadAck")
                  new MultiServerSyncWriteClient(
                    serverReplicationGroups,
                    rssShuffleHandle.partitionFanout,
                    networkTimeoutMillis,
                    maxWaitMillis,
                    finishUploadAck,
                    useConnectionPool,
                    rssShuffleHandle.user,
                    rssShuffleHandle.appId,
                    rssShuffleHandle.appAttempt,
                    shuffleWriteConfig)
                } else {
                  val maxThreads = conf.get(RssOpts.writerMaxThreads)
                  val serverThreadRatio = 8.0
                  val numThreadsBasedOnShuffleServers = Math
                    .ceil(rssShuffleHandle.rssServers.length.toDouble / serverThreadRatio)
                  val numThreads = Math.min(numThreadsBasedOnShuffleServers, maxThreads).toInt
                  logInfo(
                    s"Use replicated async writer with queue " +
                      s"size $writerQueueSize threads $numThreads, " +
                      s"$rssNumSplits splits, ${rssShuffleHandle.partitionFanout} partition " +
                      s"fanout, $serverReplicationGroups, finishUploadAck: $finishUploadAck")
                  new MultiServerAsyncWriteClient(
                    serverReplicationGroups,
                    rssShuffleHandle.partitionFanout,
                    networkTimeoutMillis,
                    maxWaitMillis,
                    finishUploadAck,
                    useConnectionPool,
                    writerQueueSize,
                    numThreads,
                    rssShuffleHandle.user,
                    rssShuffleHandle.appId,
                    rssShuffleHandle.appAttempt,
                    shuffleWriteConfig)
                }

              try {
                writeClient.connect()

                new RssShuffleWriter(
                  new ServerList(rssShuffleHandle.rssServers.map(_.toServerDetail()).toArray),
                  writeClient,
                  mapInfo,
                  serializer,
                  bufferOptions,
                  rssShuffleHandle.dependency,
                  metrics)
              } catch {
                case ex: Throwable =>
                  ExceptionUtils.closeWithoutException(writeClient)
                  throw ex
              }
            }
          })
    }
  }

  override def getReader[K, C](handle: ShuffleHandle, startMapIndex: Int, endMapIndex: Int,
                               startPartition: Int, endPartition: Int, context: TaskContext,
                               metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    logInfo(s"getReader: Use ShuffleManager: ${
      this.getClass().getSimpleName()
    }, $handle, partitions: [$startPartition, $endPartition)")

    val rssShuffleHandle = handle.asInstanceOf[RssShuffleHandle[K, _, C]]
    val shuffleInfo = new AppShuffleId(
      conf.getAppId,
      rssShuffleHandle.appAttempt,
      handle.shuffleId
    )

    val serializer = rssShuffleHandle.dependency.serializer
    val rssReplicas = conf.get(RssOpts.replicas)
    val rssCheckReplicaConsistency = conf.get(RssOpts.checkReplicaConsistency)
    val maxWaitMillis = conf.get(RssOpts.maxWaitTime)
    val rssServers = ServerConnectionStringCache.getInstance()
      .getServerList(rssShuffleHandle.getServerList)
    new RssShuffleReader(
      user = rssShuffleHandle.user,
      shuffleInfo = shuffleInfo,
      startMapIndex = startMapIndex,
      endMapIndex = endMapIndex,
      startPartition = startPartition,
      endPartition = endPartition,
      serializer = serializer,
      context = context,
      shuffleDependency = rssShuffleHandle.dependency,
      rssServers = rssServers,
      partitionFanout = rssShuffleHandle.partitionFanout,
      timeoutMillis = networkTimeoutMillis,
      maxRetryMillis = maxWaitMillis.toInt,
      dataAvailablePollInterval = pollInterval,
      dataAvailableWaitTime = dataAvailableWaitTime,
      shuffleReplicas = rssReplicas,
      checkShuffleReplicaConsistency = rssCheckReplicaConsistency,
      shuffleMetrics = metrics)
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    true
  }

  override def shuffleBlockResolver: ShuffleBlockResolver = {
    new RssShuffleBlockResolver()
  }

  override def stop(): Unit = {
    PooledWriteClientFactory.getInstance().shutdown();
    serviceRegistry.close()
    M3Stats.closeDefaultScope()
  }

  private def createServiceRegistry: ServiceRegistry = {
    val serviceRegistryType = conf.get(RssOpts.serviceRegistryType)
    logInfo(s"Service registry type: $serviceRegistryType")

    serviceRegistryType match {
      case ServiceRegistry.TYPE_SERVER_SEQUENCE =>
        new ServerSequenceServiceRegistry(
          conf.get(RssOpts.serverSequenceServerId),
          conf.get(RssOpts.serverSequenceConnectionString),
          conf.get(RssOpts.serverSequenceStartIndex),
          conf.get(RssOpts.serverSequenceEndIndex)
        )
      case ServiceRegistry.TYPE_STANDALONE =>
        val serviceRegistryServer = conf.get(RssOpts.serviceRegistryServer)
        if (serviceRegistryServer == null || serviceRegistryServer.isEmpty) {
          throw new RssException(s"${RssOpts.serviceRegistryServer.key} configure is not set")
        }
        val hostAndPort = ServerHostAndPort.fromString(serviceRegistryServer)
        new StandaloneServiceRegistryClient(hostAndPort.getHost, hostAndPort.getPort,
          networkTimeoutMillis, "rss")
      case _ => throw new RuntimeException(s"Invalid service registry type: $serviceRegistryType")
    }
  }

  private def getDataCenter: String = {
    conf.get(RssOpts.dataCenter)
  }

  private def getRssServers(numPartitions: Int,
                            excludeHosts: Seq[String]): RssServerSelectionResult = {
    val maxServerCount = conf.get(RssOpts.maxServerCount)
    val minServerCount = conf.get(RssOpts.minServerCount)

    var selectedServerCount = maxServerCount

    val shuffleServerRatio = conf.get(RssOpts.serverRatio)
    val serverCountEstimate = Math.ceil(numPartitions.doubleValue() / shuffleServerRatio)
      .intValue()
    if (selectedServerCount > serverCountEstimate) {
      selectedServerCount = serverCountEstimate
    }

    if (selectedServerCount > numPartitions) {
      selectedServerCount = numPartitions
    }

    if (selectedServerCount <= 0) {
      selectedServerCount = 1
    }

    val rssReplicas = conf.get(RssOpts.replicas)
    selectedServerCount = selectedServerCount * rssReplicas

    if (selectedServerCount < minServerCount) {
      selectedServerCount = minServerCount
    }

    val excludeHostsJavaCollection = JavaConverters.asJavaCollectionConverter(excludeHosts)
      .asJavaCollection
    val servers = ServiceRegistryUtils
      .getReachableServers(serviceRegistry, selectedServerCount, networkTimeoutMillis, dataCenter,
        cluster, excludeHostsJavaCollection)
    if (servers.isEmpty) {
      throw new RssNoServerAvailableException("There is no reachable RSS server")
    }

    MultiServerHeartbeatClient.getInstance().addServers(servers)

    val serverArray = servers.toArray(new Array[ServerDetail](0))

    var partitionFanout = 1
    if (minServerCount > 1) {
      // if min server count is configured, try to distribute a single partition on
      // multiple servers
      val numReplicationGroups = serverArray.length / rssReplicas
      if (numReplicationGroups > numPartitions) {
        partitionFanout = numReplicationGroups / numPartitions
      }
    }

    RssServerSelectionResult(serverArray, rssReplicas, partitionFanout)
  }

}
