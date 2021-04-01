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
import org.apache.spark.executor.TempShuffleReadMetrics
import org.apache.spark.remoteshuffle.testutil.RssMiniCluster
import org.scalatest.Assertions._
import org.testng.annotations._

import scala.collection.mutable.ArrayBuffer

/** *
 * This is to test scenario where there is no mapper task (zero number of mappers).
 */
class ZeroMapperShuffleTest {

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
  def runTest(): Unit = {
    val conf = TestUtil
      .newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)

    sc = new SparkContext(conf)

    val driverShuffleManager = new RssShuffleManager(conf)
    shuffleManagers :+= driverShuffleManager

    val shuffleId = 1
    val numMaps = 0
    val partitionId = 0
    val numPartitions = 1

    val rdd = sc.parallelize(Seq.empty[Int], numMaps)
      .map(t => (t -> t * 2))
      .partitionBy(new HashPartitioner(numPartitions))

    assert(rdd.partitions.size === numPartitions)

    val shuffleDependency = new ShuffleDependency[Int, Int, Int](rdd, rdd.partitioner.get)

    val shuffleHandle = driverShuffleManager.registerShuffle(shuffleId, shuffleDependency)

    val mapOutputTrackerMaster = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    mapOutputTrackerMaster.registerShuffle(shuffleId, numMaps)

    // Spark creates different ShuffleManager instances in driver and executor, thus we create
    // another instance to simulate the situation inside executor
    val executorShuffleManager = new RssShuffleManager(conf)
    shuffleManagers :+= executorShuffleManager

    val reduceTaskContext = new MockTaskContext(shuffleId, partitionId)
    val shuffleReader = executorShuffleManager
      .getReader(shuffleHandle, partitionId, partitionId, reduceTaskContext,
        new TempShuffleReadMetrics())
    val readRecords = shuffleReader.read().toList
    assert(readRecords.size === 0)
  }

}
