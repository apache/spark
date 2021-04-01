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

class ShuffleWithSparkSqlLargeDataTest {

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
  def runWithDefault(): Unit = {
    val conf = TestUtil
      .newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
    runWithConf(conf)
  }

  private def runWithConf(conf: SparkConf): Unit = {
    val numPartitions = 5

    conf.set("spark.sql.shuffle.partitions", numPartitions.toString)

    spark = SparkSession.builder
      .master("local")
      .appName("Test")
      .config(conf)
      .getOrCreate()

    spark.udf.register("generateString", TestUdfs.generateString _)

    val minId = 1
    val maxId = 10000
    val numMaps = 3

    spark.range(minId, maxId + 1, 1, numMaps).createOrReplaceTempView("idTable")

    val df = spark.sql(
      """
        | select intValue, count(distinct str) count
        | from (
        | select id, id%2 intValue, generateString(id) str
        | from idTable
        | )
        | group by 1
        | order by 1
        |""".stripMargin)
    df.explain()

    val result = df.collect()
    Assert.assertEquals(result.length, 2)

    var row = result(0)
    Assert.assertEquals(row.get(0).toString, "0")
    Assert.assertEquals(row.get(1).toString, (maxId / 2).toString)

    row = result(1)
    Assert.assertEquals(row.get(0).toString, "1")
    Assert.assertEquals(row.get(1).toString, (maxId / 2).toString)
  }

}
