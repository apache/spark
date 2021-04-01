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

import org.apache.spark.SparkConf
import org.apache.spark.remoteshuffle.testutil.{RssMiniCluster, StreamServerTestUtils}
import org.apache.spark.sql.SparkSession
import org.testng.Assert
import org.testng.annotations._

class SparkSqlOptimizeSkewedJoinTest {

  var appId: String = null
  val numRssServers = 2

  var spark: SparkSession = null

  var rssTestCluster: RssMiniCluster = null

  @BeforeMethod
  def beforeTestMethod(): Unit = {
    appId = UUID.randomUUID().toString()

    val rootDirs = StreamServerTestUtils.createTempDirectories(numRssServers)
    rssTestCluster = new RssMiniCluster(rootDirs, appId)
  }

  @AfterMethod
  def afterTestMethod(): Unit = {
    spark.stop()

    rssTestCluster.stop()
  }

  @Test
  def runWithSparkDefaultShuffle(): Unit = {
    val conf = TestUtil
      .newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)

    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
    conf.set("spark.sql.adaptive.localShuffleReader.enabled", "false")
    conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "2")
    conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "1B")
    conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "40B")
    conf.set("spark.sql.autoBroadcastJoinThreshold", "1B")
    conf.set("spark.sql.shuffle.partitions", "4")

    conf.remove("spark.shuffle.manager")

    runWithConf(conf)
  }

  @Test
  def runWithRssShuffle(): Unit = {
    val conf = TestUtil
      .newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)

    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
    conf.set("spark.sql.adaptive.localShuffleReader.enabled", "false")
    conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "2")
    conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "1B")
    conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "40B")
    conf.set("spark.sql.autoBroadcastJoinThreshold", "1B")
    conf.set("spark.sql.shuffle.partitions", "4")

    runWithConf(conf)
  }

  private def runWithConf(conf: SparkConf): Unit = {
    spark = SparkSession.builder
      .master("local")
      .appName("Test")
      .config(conf)
      .getOrCreate()

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._
    val left = spark.sparkContext.parallelize(
      (0 to 3).map(t => {
        val key = t % 2
        if (key == 0) {
          LeftStringKV(key.toString,
            t.toString + t.toString + t.toString + t.toString + t.toString)
        } else {
          LeftStringKV(key.toString, t.toString)
        }
      }),
      4).toDF()
    left.createOrReplaceTempView("left")
    val right = spark.sparkContext.parallelize(Seq(
      RightStringKV("1", "100"), RightStringKV("1", "101"),
      RightStringKV("2", "200"), RightStringKV("2", "201"),
      RightStringKV("3", "300"), RightStringKV("3", "301")
    ), 2).toDF()
    right.createOrReplaceTempView("right")

    val df = spark.sql(
      "SELECT left.value, right.key, right.value FROM left JOIN right ON left.key = right.key WHERE left.key = '1' order by 1, 2, 3")

    val result = df.collect()
    Assert.assertEquals(result.length, 4)

    var row = result(0)
    Assert.assertEquals(row.get(0), "1")
    Assert.assertEquals(row.get(1), "1")
    Assert.assertEquals(row.get(2), "100")

    row = result(1)
    Assert.assertEquals(row.get(0), "1")
    Assert.assertEquals(row.get(1), "1")
    Assert.assertEquals(row.get(2), "101")

    row = result(2)
    Assert.assertEquals(row.get(0), "3")
    Assert.assertEquals(row.get(1), "1")
    Assert.assertEquals(row.get(2), "100")

    row = result(3)
    Assert.assertEquals(row.get(0), "3")
    Assert.assertEquals(row.get(1), "1")
    Assert.assertEquals(row.get(2), "101")
  }

}
