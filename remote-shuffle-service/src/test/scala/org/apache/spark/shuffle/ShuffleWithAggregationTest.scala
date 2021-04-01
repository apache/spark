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

import org.apache.spark.SparkContext
import org.apache.spark.remoteshuffle.testutil.{RssMiniCluster, StreamServerTestUtils}
import org.scalatest.Assertions._
import org.testng.annotations._

/** *
 * This is to test shuffle with aggregation
 */
class ShuffleWithAggregationTest {

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
  def foldByKey(): Unit = {
    val conf = TestUtil
      .newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)

    sc = new SparkContext(conf)

    val numValues = 1000
    val numMaps = 3
    val numPartitions = 5

    val rdd = sc.parallelize(0 until numValues, numMaps)
      .map(t => ((t / 2) -> (t * 2).longValue()))
      .foldByKey(0, numPartitions)((v1, v2) => v1 + v2)
    val result = rdd.collect()

    assert(sc.env.shuffleManager.getClass.getSimpleName === "RssShuffleManager")
    assert(result.size === numValues / 2)

    for (i <- 0 until result.size) {
      val key = result(i)._1
      val value = result(i)._2
      assert(key * 2 * 2 + (key * 2 + 1) * 2 === value)
    }

    val keys = result.map(_._1).distinct.sorted
    assert(keys.length === numValues / 2)
    assert(keys(0) === 0)
    assert(keys.last === (numValues - 1) / 2)
  }

}
