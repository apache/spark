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

import org.apache.spark._
import org.apache.spark.executor.{ShuffleWriteMetrics, TempShuffleReadMetrics}
import org.apache.spark.remoteshuffle.testutil.TestStreamServer
import org.scalatest.Assertions._
import org.testng.annotations._

import java.util.UUID
import scala.collection.mutable.ArrayBuffer


class RssShuffleManagerWithServerSequenceRegistryTest {

  var appId: String = null

  var sc: SparkContext = null

  var testStreamServer: TestStreamServer = null
  private var shuffleManagers = ArrayBuffer[RssShuffleManager]()

  @BeforeMethod
  def beforeTestMethod(): Unit = {
    appId = UUID.randomUUID().toString()
    shuffleManagers.clear()

    testStreamServer = TestStreamServer.createRunningServerWithLocalStandaloneRegistryServer(appId)
  }

  @AfterMethod
  def afterTestMethod(): Unit = {
    sc.stop()
    shuffleManagers.foreach(m => m.stop())
    testStreamServer.shutdown()
  }

  @Test
  def runSparkApplication(): Unit = {
    val conf = TestUtil.newSparkConfWithStandAloneRegistryServer(appId, "")
    conf.set("spark.shuffle.rss.serviceRegistry.type", "serverSequence")
    conf.set("spark.shuffle.rss.serverSequence.connectionString",
      s"localhost:${testStreamServer.getShufflePort}")
    conf.set("spark.shuffle.rss.serverSequence.serverId", testStreamServer.getServerId)
    runWithSparkConf(conf)
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
    mapOutputTrackerMaster.registerShuffle(shuffleId, numMaps, numPartitions)

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

}
