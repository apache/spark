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
import org.apache.spark.remoteshuffle.exceptions.RssException
import org.apache.spark.remoteshuffle.testutil.RssMiniCluster
import org.apache.spark.remoteshuffle.util.NetworkUtils
import org.scalatest.Assertions._
import org.testng.annotations._

import scala.collection.mutable.ArrayBuffer


class RssShuffleManagerTest {

  var appId: String = null
  val numRssServers = 2

  var sc: SparkContext = null

  var rssTestCluster: RssMiniCluster = null
  private var shuffleManagers = ArrayBuffer[RssShuffleManager]()

  @BeforeMethod
  def beforeTestMethod(): Unit = {
    appId = UUID.randomUUID().toString()
    shuffleManagers.clear()
    rssTestCluster = new RssMiniCluster(numRssServers, appId)
  }

  @AfterMethod
  def afterTestMethod(): Unit = {
    sc.stop()
    shuffleManagers.foreach(m => m.stop())
    rssTestCluster.stop()
  }

  @Test
  def runWithSyncClient(): Unit = {
    val conf = TestUtil
      .newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
    runWithSparkConf(conf)
  }

  @Test
  def runWithSyncClient_recordWithNullKeyValue(): Unit = {
    val conf = TestUtil
      .newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
    runWithSparkConf_recordWithNullKeyValue(conf)
  }

  @Test
  def runWithDefaultConfig(): Unit = {
    val conf = TestUtil
      .newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
    runWithSparkConf(conf)
  }

  @Test
  def runWithWriterAsyncFinish(): Unit = {
    val conf = TestUtil
      .newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
    conf.set("spark.shuffle.rss.writer.asyncFinish", "true")
    runWithSparkConf(conf)
  }

  @Test
  def runWithSyncWriter(): Unit = {
    val conf = TestUtil
      .newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
    conf.set("spark.shuffle.rss.writer.queueSize", "0")
    runWithSparkConf(conf)
  }

  @Test
  def runWithAsyncWriter(): Unit = {
    val conf = TestUtil
      .newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
    conf.set("spark.shuffle.rss.writer.queueSize", "100")
    runWithSparkConf(conf)
  }

  @Test
  def run_singleSplit(): Unit = {
    val conf = TestUtil
      .newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
    conf.set("spark.shuffle.rss.numSplits", "1")
    runWithSparkConf(conf)
  }

  @Test
  def runWithoutConnectionPool_multiSplits(): Unit = {
    val conf = TestUtil
      .newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
    conf.set("spark.shuffle.rss.useConnectionPool", "false")
    conf.set("spark.shuffle.rss.numSplits", "3")
    runWithSparkConf(conf)
  }

  @Test
  def runWithConnectionPool_compressed(): Unit = {
    val conf = TestUtil
      .newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
    conf.set("spark.shuffle.rss.useConnectionPool", "true")
    runWithSparkConf(conf)
  }

  @Test
  def runWithRssDisabled(): Unit = {
    val conf = TestUtil
      .newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
    runWithSparkConf(conf)
  }

  @Test
  def runWithMaxServerCountEqualOne(): Unit = {
    val conf = TestUtil
      .newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
    conf.set("spark.shuffle.rss.maxServerCount", "1")
    runWithSparkConf(conf)
  }

  @Test
  def runWithMaxServerCountEqualMaxInteger(): Unit = {
    val conf = TestUtil
      .newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
    conf.set("spark.shuffle.rss.maxServerCount", Integer.MAX_VALUE.toString())
    runWithSparkConf(conf)
  }

  @Test(expectedExceptions = Array(classOf[RssException]))
  def excludeHosts(): Unit = {
    val conf = TestUtil
      .newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
    conf.set("spark.shuffle.rss.excludeHosts",
      s"localhost,${NetworkUtils.getLocalHostName},${NetworkUtils.getLocalFQDN}")

    sc = new SparkContext(conf)

    val driverShuffleManager = new RssShuffleManager(conf)
    shuffleManagers :+= driverShuffleManager

    val shuffleId = 1
    val numMaps = 10
    val numPartitions = 5

    val rdd = sc.parallelize(1 to 100)
      .map(t => (t -> t * 2))
      .partitionBy(new HashPartitioner(numPartitions))
    val shuffleDependency = new ShuffleDependency[Int, Int, Int](rdd, rdd.partitioner.get)

    driverShuffleManager.registerShuffle(shuffleId, shuffleDependency)
  }

  def runWithSparkConf(conf: SparkConf): Unit = {
    sc = new SparkContext(conf)

    val driverShuffleManager = new RssShuffleManager(conf)
    shuffleManagers :+= driverShuffleManager

    val shuffleId = 1
    val numMaps = 10
    val numValuesInMap = 100
    val numPartitions = 5

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

    val mapStatus = (0 until numMaps).toList.par.map(mapId => {
      val taskAttemptId = mapId + 1000
      val mapTaskContext = new MockTaskContext(shuffleId, mapId, taskAttemptId)
      val shuffleWriter = executorShuffleManager
        .getWriter[Int, Int](shuffleHandle, mapId, mapTaskContext, new ShuffleWriteMetrics())
      val records = (1 to numValuesInMap).map(t => (mapId * 1000 + t) -> (mapId * 1000 + t * 2))
        .iterator
      shuffleWriter.write(records)
      val mapStatus = shuffleWriter.stop(true).get

      mapOutputTrackerMaster.registerMapOutput(shuffleId, mapId, mapStatus)
    })

    assert(mapStatus.size === numMaps)

    {
      val startPartition = 0
      val endPartition = 0
      val reduceTaskContext = new MockTaskContext(shuffleId, startPartition)
      val shuffleReader = executorShuffleManager
        .getReader(shuffleHandle, startPartition, endPartition, reduceTaskContext,
          new TempShuffleReadMetrics())
      val readRecords = shuffleReader.read().toList
      assert(readRecords.size === numMaps * numValuesInMap / numPartitions)
    }
    {
      val startPartition = 0
      val endPartition = 1
      val reduceTaskContext = new MockTaskContext(shuffleId, startPartition)
      val shuffleReader = executorShuffleManager
        .getReader(shuffleHandle, startPartition, endPartition, reduceTaskContext,
          new TempShuffleReadMetrics())
      val readRecords = shuffleReader.read().toList
      assert(readRecords.size === numMaps * numValuesInMap / numPartitions)
    }
    {
      val startPartition = 0
      val endPartition = 2
      val reduceTaskContext = new MockTaskContext(shuffleId, startPartition)
      val shuffleReader = executorShuffleManager
        .getReader(shuffleHandle, startPartition, endPartition, reduceTaskContext,
          new TempShuffleReadMetrics())
      val readRecords = shuffleReader.read().toList
      assert(readRecords.size === 2 * numMaps * numValuesInMap / numPartitions)
    }
  }

  def runWithSparkConf_recordWithNullKeyValue(conf: SparkConf): Unit = {
    sc = new SparkContext(conf)

    val driverShuffleManager = new RssShuffleManager(conf)
    shuffleManagers :+= driverShuffleManager

    val shuffleId = 1
    val mapId = 0
    val numMaps = 1
    val numPartitions = 1
    val partitionId = 0

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

    val mapTaskContext = new MockTaskContext(shuffleId, mapId)
    val shuffleWriter = executorShuffleManager
      .getWriter[String, String](shuffleHandle, mapId, mapTaskContext, new ShuffleWriteMetrics())
    val records = List((null, ""), ("", null), (null, null))
    shuffleWriter.write(records.iterator)
    val mapStatus = shuffleWriter.stop(true).get
    mapOutputTrackerMaster.registerMapOutput(shuffleId, mapId, mapStatus)

    val reduceTaskContext = new MockTaskContext(shuffleId, partitionId)
    val shuffleReader = executorShuffleManager
      .getReader(shuffleHandle, partitionId, partitionId, reduceTaskContext,
        new TempShuffleReadMetrics())
    val readRecords = shuffleReader.read().toList
    assert(readRecords.size === 3)
    assert(readRecords === records)
  }
}
