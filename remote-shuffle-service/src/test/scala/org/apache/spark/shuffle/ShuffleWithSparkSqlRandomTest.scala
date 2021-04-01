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

import java.util.{Random, UUID}

import org.apache.spark.internal.Logging
import org.apache.spark.remoteshuffle.testutil.{RssMiniCluster, StreamServerTestUtils}
import org.apache.spark.sql.SparkSession
import org.testng.Assert
import org.testng.annotations._

class ShuffleWithSparkSqlRandomTest extends Logging {

  val random = new Random()

  @Test
  def run(): Unit = {
    (0 until 2).foreach(_ => runSingleRound())
  }

  private def runSingleRound(): Unit = {
    val numRssServers = 1 + random.nextInt(3)
    val numIdTableRows = 1 + random.nextInt(1000)
    val numRowGroups = 1 + random.nextInt(numIdTableRows)

    val numExecutorCores = 1 + random.nextInt(4)
    val bufferSize = 1 + random.nextInt(64 * 1024)
    val bufferSpill = 1 + random.nextInt(128 * 1024)

    val numMaps = 1 + random.nextInt(10)
    val numPartitions = 1 + random.nextInt(10)

    logInfo(s"Running, numRssServers: $numRssServers, " +
      s"numIdTableRows: $numIdTableRows, numRowGroups: $numRowGroups, " +
      s"numExecutorCores: $numExecutorCores, bufferSize: $bufferSize, bufferSpill: $bufferSpill, " +
      s"numMaps: $numMaps, numPartitions: $numPartitions")

    val appId = UUID.randomUUID().toString()

    val rootDirs = StreamServerTestUtils.createTempDirectories(numRssServers)
    val rssTestCluster = new RssMiniCluster(rootDirs, appId)

    val conf = TestUtil
      .newSparkConfWithStandAloneRegistryServer(appId, rssTestCluster.getRegistryServerConnection)
    conf.set("spark.executor.cores", numExecutorCores.toString)
    conf.set("spark.sql.shuffle.partitions", numPartitions.toString)
    conf.set("spark.shuffle.rss.writer.bufferSize", bufferSize.toString)
    conf.set("spark.shuffle.rss.writer.bufferSpill", bufferSpill.toString)

    val spark = SparkSession.builder
      .master("local")
      .appName("Test")
      .config(conf)
      .getOrCreate()

    spark.udf.register("randomString", TestUdfs.randomString _)

    spark.range(0, numIdTableRows, 1, numMaps).createOrReplaceTempView("idTable")

    val df = spark.sql(
      s"""
         | select groupKey, count(distinct str) count, max(str) max, min(str) min
         | from (
         | select id % $numRowGroups groupKey, randomString() str from idTable
         | union all
         | select 0, ''
         | )
         | group by 1
         | order by 1
         |""".stripMargin)
    df.explain()

    val result = df.collect()
    Assert.assertEquals(result.size, numRowGroups)

    var row = result(0)
    Assert.assertEquals(row.getLong(0), 0)
    Assert.assertTrue(row.getLong(1) >= 0)
    Assert.assertTrue(row.getLong(1) <= TestUdfs.testValues.size + 1)
    Assert.assertTrue(TestUdfs.testValues.contains(row.getString(2)))
    Assert.assertEquals(row.getString(3), "")

    row = result.last
    Assert.assertEquals(row.getLong(0), numRowGroups - 1)
    Assert.assertTrue(row.getLong(1) >= 0)
    Assert.assertTrue(row.getLong(1) <= TestUdfs.testValues.size + 1)
    Assert.assertTrue(TestUdfs.testValues.contains(row.getString(2)))
    Assert.assertTrue(TestUdfs.testValues.contains(row.getString(3)))

    spark.stop()

    rssTestCluster.stop()
  }


}
