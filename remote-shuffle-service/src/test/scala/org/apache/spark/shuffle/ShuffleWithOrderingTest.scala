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

import org.apache.spark.remoteshuffle.testutil.{RssMiniCluster, StreamServerTestUtils}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.Assertions._
import org.testng.annotations._

/** *
 * This is to test scenario where shuffle reader has ordering
 */
class ShuffleWithOrderingTest {

  var appId: String = null
  val numRssServers = 2

  var sc: SparkContext = null

  var rssTestCluster: RssMiniCluster = null

  @BeforeMethod
  def beforeTestMethod(): Unit = {
    appId = UUID.randomUUID().toString()

    val rootDirs = StreamServerTestUtils.createTempDirectories(numRssServers)
    rssTestCluster = new RssMiniCluster(rootDirs, appId)
  }

  @AfterMethod
  def afterTestMethod(): Unit = {
    sc.stop()

    rssTestCluster.stop()
  }

  @Test
  def sortByKey(): Unit = {
    val conf = TestUtil
      .newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
    runWithConf(conf)
  }

  @Test
  def sortByKey_fileBufferLargeValue(): Unit = {
    val conf = TestUtil
      .newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
    conf.set("spark.shuffle.file.buffer", "1m")
    runWithConf(conf)
  }

  @Test
  def sortByKey_initialMemoryThresholdLargeValue(): Unit = {
    val conf = TestUtil
      .newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
    conf.set("spark.shuffle.spill.initialMemoryThreshold", "128000000")
    runWithConf(conf)
  }

  private def runWithConf(conf: SparkConf): Unit = {
    sc = new SparkContext(conf)

    val numValues = 1000
    val numMaps = 3
    val numPartitions = 5

    val rdd = sc.parallelize(0 until numValues, numMaps)
      .map(t => (t -> t * 2))
      .sortByKey(ascending = false, numPartitions = numPartitions)
    val result = rdd.collect()

    assert(sc.env.shuffleManager.getClass.getSimpleName === "RssShuffleManager")
    assert(result.size === numValues)

    for (i <- 0 until result.size - 1) {
      val currentRow = result(i)
      val nextRow = result(i + 1)
      assert(currentRow._1 > nextRow._1)
    }
  }
}
