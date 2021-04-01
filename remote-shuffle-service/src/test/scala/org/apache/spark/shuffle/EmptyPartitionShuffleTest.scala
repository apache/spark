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
import org.apache.spark.remoteshuffle.testutil.RssMiniCluster
import org.scalatest.Assertions._
import org.testng.annotations._

import scala.collection.mutable.ArrayBuffer

/** *
 * This is to test scenario where there is some partitions with no data.
 */
class EmptyPartitionShuffleTest {

  var appId: String = null
  val numRssServers = 2

  var sc: SparkContext = null

  var rssTestCluster: RssMiniCluster = null
  private var shuffleManagers = ArrayBuffer[RssShuffleManager]();

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
  def runTest(): Unit = {
    val conf = TestUtil
      .newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)

    sc = new SparkContext(conf)

    val driverShuffleManager = new RssShuffleManager(conf)
    shuffleManagers :+= driverShuffleManager

    val shuffleId = 1
    val numMaps = 1
    val numValuesInMap = 5
    val numPartitions = 2

    // This test will use shuffle records (key/pairs) like: (0, 0), (2, 1), (4, 2), (6, 3), (8, 4).
    // It uses HashPartitioner with two partitions. Thus each shuffle record will be assigned to partition 0,
    // because HashPartitioner will get the modulus of the integer as the partition id. Thus partition 1 will
    // be empty.

    val rdd = sc.parallelize(1 to 1, numMaps)
      .map(t => (t * 2 -> t))
      .partitionBy(new HashPartitioner(numPartitions))

    val shuffleDependency = new ShuffleDependency[Int, Int, Int](rdd, rdd.partitioner.get)

    val shuffleHandle = driverShuffleManager.registerShuffle(shuffleId, shuffleDependency)

    val mapOutputTrackerMaster = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    mapOutputTrackerMaster.registerShuffle(shuffleId, numMaps)

    // Spark creates different ShuffleManager instances in driver and executor, thus we create
    // another instance to simulate the situation inside executor
    val executorShuffleManager = new RssShuffleManager(conf)
    shuffleManagers :+= executorShuffleManager

    (0 until numMaps).toList.par.map(mapId => {
      val mapTaskContext = new MockTaskContext(shuffleId, mapId)
      val shuffleWriter = executorShuffleManager
        .getWriter[Int, Int](shuffleHandle, mapId, mapTaskContext, new ShuffleWriteMetrics())
      val records = (0 until numValuesInMap).map(t => t * 2 -> t).iterator
      shuffleWriter.write(records)
      val mapStatus = shuffleWriter.stop(true).get
      mapOutputTrackerMaster.registerMapOutput(shuffleId, mapId, mapStatus)
    })

    // partition 0 should be: (0, 0), (2, 1), (4, 2), (6, 3), (8, 4)
    {
      val startPartition = 0
      val endPartition = 0
      val reduceTaskContext = new MockTaskContext(shuffleId, startPartition)
      val shuffleReader = executorShuffleManager
        .getReader(shuffleHandle, startPartition, endPartition, reduceTaskContext,
          new TempShuffleReadMetrics())
      val readRecords = shuffleReader.read().toList
      assert(readRecords.size === numValuesInMap)
      assert(readRecords(0)._1.toString === "0")
      assert(readRecords(0)._2.toString === "0")
      assert(readRecords(4)._1.toString === "8")
      assert(readRecords(4)._2.toString === "4")
    }
    // partition 1 should has no data
    {
      val startPartition = 1
      val endPartition = 1
      val reduceTaskContext = new MockTaskContext(shuffleId, startPartition)
      val shuffleReader = executorShuffleManager
        .getReader(shuffleHandle, startPartition, endPartition, reduceTaskContext,
          new TempShuffleReadMetrics())
      val readRecords = shuffleReader.read().toList
      assert(readRecords.size === 0)
    }
  }

}
