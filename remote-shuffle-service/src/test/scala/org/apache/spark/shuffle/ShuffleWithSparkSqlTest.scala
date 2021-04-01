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

class ShuffleWithSparkSqlTest {

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

  @Test
  def runWithTwoExecutorCores(): Unit = {
    val conf = TestUtil
      .newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
    conf.set("spark.executor.cores", "2")
    runWithConf(conf)
  }

  @Test
  def nullValueInSql(): Unit = {
    val conf = TestUtil
      .newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)

    val numPartitions = 7

    conf.set("spark.sql.shuffle.partitions", numPartitions.toString)

    spark = SparkSession.builder
      .master("local")
      .appName("Test")
      .config(conf)
      .getOrCreate()

    val df = spark.sql(
      """
        | select k, min(v) min, max(v)
        | from (
        | select null k, 1 v
        | union all
        | select null k, 2 v
        | union all
        | select '' k, 3 v
        | union all
        | select '' k, 4 v
        | union all
        | select 'str1' k, 5 v
        | union all
        | select 'str1' k, 6 v
        | )
        | group by 1
        | order by 1
        |""".stripMargin)
    df.explain()

    val result = df.collect()
    Assert.assertEquals(result.length, 3)

    var row = result(0)
    Assert.assertEquals(row.getString(0), null)
    Assert.assertEquals(row.getInt(1), 1)
    Assert.assertEquals(row.getInt(2), 2)

    row = result(1)
    Assert.assertEquals(row.getString(0), "")
    Assert.assertEquals(row.getInt(1), 3)
    Assert.assertEquals(row.getInt(2), 4)

    row = result(2)
    Assert.assertEquals(row.getString(0), "str1")
    Assert.assertEquals(row.getInt(1), 5)
    Assert.assertEquals(row.getInt(2), 6)
  }

  private def runWithConf(conf: SparkConf): Unit = {
    val numPartitions = 5

    conf.set("spark.sql.shuffle.partitions", numPartitions.toString)

    spark = SparkSession.builder
      .master("local")
      .appName("Test")
      .config(conf)
      .getOrCreate()

    spark.udf.register("intToString", TestUdfs.intToString _)

    val minId = 1
    val maxId = 100
    val numMaps = 3

    spark.range(minId, maxId + 1, 1, numMaps).createOrReplaceTempView("idTable")

    val df = spark.sql(
      """
        | select intValue, max(str) max, sum(id) sum
        | from (
        | select id, id%2 intValue, intToString(id) str
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
    Assert.assertEquals(row.get(1).toString, TestUdfs.intToString(maxId))

    row = result(1)
    Assert.assertEquals(row.get(0).toString, "1")
    Assert.assertEquals(row.get(1).toString, TestUdfs.intToString(maxId - 1))
  }

}
