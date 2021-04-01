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

import java.util.UUID

import org.apache.spark._
import org.apache.spark.executor.{ShuffleWriteMetrics, TempShuffleReadMetrics}
import org.apache.spark.remoteshuffle.exceptions.RssNetworkException
import org.apache.spark.remoteshuffle.testutil.RssMiniCluster
import org.scalatest.Assertions._
import org.slf4j.LoggerFactory
import org.testng.annotations._

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class RssShuffleManagerReplicateServerGroupTest {

  val logger = LoggerFactory.getLogger(classOf[RssShuffleManagerReplicateServerGroupTest])

  var appId: String = null
  val numRssServers = 8

  var sc: SparkContext = null

  var rssTestCluster: RssMiniCluster = null
  private var shuffleManagers = ArrayBuffer[RssShuffleManager]()

  @BeforeMethod
  def beforeTestMethod(): Unit = {
    appId = UUID.randomUUID().toString()
    shuffleManagers.clear()
    rssTestCluster = new RssMiniCluster(numRssServers, appId)
    logger.info("Started RSS cluster")
  }

  @AfterMethod
  def afterTestMethod(): Unit = {
    logger.info("Stopping SparkContext")
    sc.stop()
    logger.info("Stopping shuffle managers")
    shuffleManagers.foreach(m => m.stop())
    logger.info("Stopping RSS cluster")
    rssTestCluster.stop()
  }

  @Test
  def runApplication_noRssReplica(): Unit = {
    val conf = TestUtil
      .newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
    runWithSparkConf(conf)
  }

  @Test
  def runApplication_oneRssReplica(): Unit = {
    val conf = TestUtil
      .newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
    conf.set("spark.shuffle.rss.replicas", "1")
    runWithSparkConf(conf)
  }

  @Test
  def runApplication_twoRssReplicas(): Unit = {
    val conf = TestUtil
      .newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
    conf.set("spark.shuffle.rss.serverRatio", "1")
    conf.set("spark.shuffle.rss.replicas", "2")
    runWithSparkConf(conf)
  }

  @Test
  def runApplication_twoRssReplicas_shutDownFirstServerBeforeShuffleWrite(): Unit = {
    val conf = TestUtil
      .newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
    conf.set("spark.shuffle.rss.serverRatio", "1")
    conf.set("spark.shuffle.rss.replicas", "2")
    runWithSparkConf(conf, actionBeforeShuffleWrite = rssShuffleHandler => rssTestCluster
      .shutdownStreamServers(rssShuffleHandler.rssServers(0).serverId))
  }

  @Test
  def runApplication_twoRssReplicas_shutDownFirstServerBeforeShuffleRead(): Unit = {
    val conf = TestUtil
      .newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
    conf.set("spark.shuffle.rss.serverRatio", "1")
    conf.set("spark.shuffle.rss.replicas", "2")
    runWithSparkConf(conf, actionBeforeShuffleRead = rssShuffleHandler => rssTestCluster
      .shutdownStreamServers(rssShuffleHandler.rssServers(0).serverId))
  }

  @Test(expectedExceptions = Array(classOf[RssNetworkException]))
  def runApplication_twoRssReplicas_shutDownAllServersBeforeShuffleWrite(): Unit = {
    val conf = TestUtil
      .newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
    conf.set("spark.shuffle.rss.serverRatio", "1")
    conf.set("spark.shuffle.rss.replicas", "2")
    runWithSparkConf(conf, actionBeforeShuffleWrite = rssShuffleHandler => {
      rssShuffleHandler.rssServers
        .foreach(server => rssTestCluster.shutdownStreamServers(server.serverId))
    })
  }

  @Test(expectedExceptions = Array(classOf[FetchFailedException]))
  def runApplication_twoRssReplicas_shutDownAllServersBeforeShuffleRead(): Unit = {
    val conf = TestUtil
      .newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
    conf.set("spark.shuffle.rss.serverRatio", "1")
    conf.set("spark.shuffle.rss.replicas", "2")
    conf.set("spark.shuffle.rss.networkTimeout", "2000")
    runWithSparkConf(conf, actionBeforeShuffleRead = rssShuffleHandler => {
      rssShuffleHandler.rssServers
        .foreach(server => rssTestCluster.shutdownStreamServers(server.serverId))
    })
  }

  @Test
  def runApplication_oneRssReplica_eightMinServerCount(): Unit = {
    val conf = TestUtil
      .newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
    conf.set("spark.shuffle.rss.serverRatio", "1")
    conf.set("spark.shuffle.rss.replicas", "1")
    conf.set("spark.shuffle.rss.minServerCount", "8")
    runWithSparkConf(conf)
  }

  @Test
  def runApplication_twoRssReplicas_eightMinServerCount(): Unit = {
    val conf = TestUtil
      .newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
    conf.set("spark.shuffle.rss.serverRatio", "1")
    conf.set("spark.shuffle.rss.replicas", "2")
    conf.set("spark.shuffle.rss.minServerCount", "8")
    runWithSparkConf(conf)
  }

  def runWithSparkConf(conf: SparkConf,
                       actionBeforeShuffleWrite: RssShuffleHandle[Int, Int, Int] => Unit = _ => Unit,
                       actionBeforeShuffleRead: RssShuffleHandle[Int, Int, Int] => Unit = _ => Unit): Unit = {
    conf.getAll.foreach(t => logger.info(s"Spark config: ${t._1}=${t._2}"))
    sc = new SparkContext(conf)

    val driverShuffleManager = new RssShuffleManager(conf)
    shuffleManagers :+= driverShuffleManager

    val shuffleId = 1
    val numMaps = 10
    val numValuesInMap = 1000
    val numPartitions = 2

    val rdd = sc.parallelize(1 to 100)
      .map(t => (t -> t * 2))
      .partitionBy(new HashPartitioner(numPartitions))
    val shuffleDependency = new ShuffleDependency[Int, Int, Int](rdd, rdd.partitioner.get)

    val shuffleHandle = driverShuffleManager.registerShuffle(shuffleId, shuffleDependency)

    val mapOutputTrackerMaster = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    mapOutputTrackerMaster.registerShuffle(shuffleId, numMaps)

    // Spark creates different ShuffleManager instances in driver and executor, thus we create
    // another instance to simulate the situation inside executor
    val executorShuffleManager = new RssShuffleManager(conf)
    shuffleManagers :+= executorShuffleManager

    actionBeforeShuffleWrite(shuffleHandle.asInstanceOf[RssShuffleHandle[Int, Int, Int]])

    // run map tasks, we will run these tasks again later to simulate task retry
    (0 until numMaps).toList.map(mapId => {
      logger.info(s"Running shuffle writer with map id $mapId")
      val taskAttemptId = mapId + 1000
      val mapTaskContext = new MockTaskContext(shuffleId, mapId, taskAttemptId)
      val shuffleWriter = executorShuffleManager
        .getWriter[Int, Int](shuffleHandle, mapId, mapTaskContext, new ShuffleWriteMetrics())
      val records = (1 to numValuesInMap).map(t => (mapId * 10000 + t) -> (mapId * 10000 + t * 2))
        .toList
      shuffleWriter.write(records.iterator)
      val mapStatus = shuffleWriter.stop(true).get
      mapOutputTrackerMaster.registerMapOutput(shuffleId, mapId, mapStatus)
    })

    val allWrittenRecords = new ListBuffer[(Int, Int)]()

    // run map tasks again with new task attempt id, to simulate task retry
    val mapStatus = (0 until numMaps).toList.map(mapId => {
      logger.info(s"Stopping shuffle writer with map id $mapId")
      val taskAttemptId = mapId + 2000
      val mapTaskContext = new MockTaskContext(shuffleId, mapId, taskAttemptId)
      val shuffleWriter = executorShuffleManager
        .getWriter[Int, Int](shuffleHandle, mapId, mapTaskContext, new ShuffleWriteMetrics())
      val records = (1 to numValuesInMap).map(t => (mapId * 10000 + t) -> (mapId * 10000 + t * 2))
        .toList
      allWrittenRecords ++= records
      shuffleWriter.write(records.iterator)
      val mapStatus = shuffleWriter.stop(true).get
      mapOutputTrackerMaster.registerMapOutput(shuffleId, mapId, mapStatus)
    })

    assert(mapStatus.size === numMaps)

    val partition0Records = allWrittenRecords.filter(_._1 % 2 == 0).sorted
    val partition1Records = allWrittenRecords.filter(_._1 % 2 == 1).sorted

    actionBeforeShuffleRead(shuffleHandle.asInstanceOf[RssShuffleHandle[Int, Int, Int]])

    {
      val startPartition = 0
      val endPartition = 0
      logger.info(s"Running shuffle reader with partition [$startPartition, $endPartition)")
      val reduceTaskContext = new MockTaskContext(shuffleId, startPartition)
      val shuffleReader = executorShuffleManager
        .getReader(shuffleHandle, startPartition, endPartition, reduceTaskContext,
          new TempShuffleReadMetrics())
      val readRecords = shuffleReader.read().map(t => t.asInstanceOf[(Int, Int)]).toList.sorted
      assert(readRecords.size === numMaps * numValuesInMap / numPartitions)
      assert(readRecords === partition0Records)
    }
    {
      val startPartition = 0
      val endPartition = 1
      logger.info(s"Running shuffle reader with partition [$startPartition, $endPartition)")
      val reduceTaskContext = new MockTaskContext(shuffleId, startPartition)
      val shuffleReader = executorShuffleManager
        .getReader(shuffleHandle, startPartition, endPartition, reduceTaskContext,
          new TempShuffleReadMetrics())
      val readRecords = shuffleReader.read().map(t => t.asInstanceOf[(Int, Int)]).toList.sorted
      assert(readRecords.size === numMaps * numValuesInMap / numPartitions)
      assert(readRecords === partition0Records)
    }
    {
      val startPartition = 1
      val endPartition = 2
      logger.info(s"Running shuffle reader with partition [$startPartition, $endPartition)")
      val reduceTaskContext = new MockTaskContext(shuffleId, startPartition)
      val shuffleReader = executorShuffleManager
        .getReader(shuffleHandle, startPartition, endPartition, reduceTaskContext,
          new TempShuffleReadMetrics())
      val readRecords = shuffleReader.read().map(t => t.asInstanceOf[(Int, Int)]).toList.sorted
      assert(readRecords.size === numMaps * numValuesInMap / numPartitions)
      assert(readRecords === partition1Records)
    }
    {
      val startPartition = 0
      val endPartition = 2
      logger.info(s"Running shuffle reader with partition [$startPartition, $endPartition)")
      val reduceTaskContext = new MockTaskContext(shuffleId, startPartition)
      val shuffleReader = executorShuffleManager
        .getReader(shuffleHandle, startPartition, endPartition, reduceTaskContext,
          new TempShuffleReadMetrics())
      val readRecords = shuffleReader.read().map(t => t.asInstanceOf[(Int, Int)]).toList.sorted
      assert(readRecords.size === numMaps * numValuesInMap)
      assert(readRecords === allWrittenRecords.sorted)
    }
  }

}
