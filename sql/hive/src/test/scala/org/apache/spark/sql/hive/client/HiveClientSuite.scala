/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.client

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualTo, GreaterThan, GreaterThanOrEqual, In, LessThan, LessThanOrEqual, Like, Literal, Or}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.types.{ByteType, IntegerType, StringType}

class HiveClientSuite extends SparkFunSuite {
  import CatalystSqlParser._

  private val clientBuilder = new HiveClientBuilder

  private val tryDirectSqlKey = HiveConf.ConfVars.METASTORE_TRY_DIRECT_SQL.varname

  private val testPartitionCount = 3 * 24 * 4
  private val client = init(true)

  private def init(tryDirectSql: Boolean): HiveClient = {
    val storageFormat = CatalogStorageFormat(
      locationUri = None,
      inputFormat = None,
      outputFormat = None,
      serde = None,
      compressed = false,
      properties = Map.empty)

    val hadoopConf = new Configuration()
    hadoopConf.setBoolean(tryDirectSqlKey, tryDirectSql)
    val client = clientBuilder.buildClient(HiveUtils.hiveExecutionVersion, hadoopConf)
    client
      .runSqlHive("CREATE TABLE test (value INT) PARTITIONED BY (ds INT, h INT, chunk STRING)")

    val partitions =
      for {
        ds <- 20170101 to 20170103
        h <- 0 to 23
        chunk <- Seq("aa", "ab", "ba", "bb")
      } yield CatalogTablePartition(Map(
        "ds" -> ds.toString,
        "h" -> h.toString,
        "chunk" -> chunk
      ), storageFormat)
    assert(partitions.size == testPartitionCount)

    client.createPartitions(
      "default", "test", partitions, ignoreIfExists = false)
    client
  }

  test(s"getPartitionsByFilter returns all partitions when $tryDirectSqlKey=false") {
    val client = init(false)
    val filteredPartitions = client.getPartitionsByFilter(client.getTable("default", "test"),
      Seq(parseExpression("ds=20170101")))

    assert(filteredPartitions.size == testPartitionCount)
  }

  test("getPartitionsByFilter: ds=20170101") {
    testMetastorePartitionFiltering(
      "ds=20170101",
      20170101 to 20170101,
      0 to 23,
      "aa" :: "ab" :: "ba" :: "bb" :: Nil)
  }

  test("getPartitionsByFilter: chunk='aa'") {
    testMetastorePartitionFiltering(
      "chunk='aa'",
      20170101 to 20170103,
      0 to 23,
      "aa" :: Nil)
  }

  test("getPartitionsByFilter: 20170101=ds") {
    testMetastorePartitionFiltering(
      "20170101=ds",
      20170101 to 20170101,
      0 to 23,
      "aa" :: "ab" :: "ba" :: "bb" :: Nil)
  }

  test("getPartitionsByFilter: ds=cast('20170101' as int)") {
    testMetastorePartitionFiltering(
      "ds=cast('20170101' as int)",
      20170101 to 20170101,
      0 to 23,
      "aa" :: "ab" :: "ba" :: "bb" :: Nil)
  }

  test("getPartitionsByFilter: ds=(20170101 + 1)") {
    testMetastorePartitionFiltering(
      "ds=(20170101 + 1)",
      20170102 to 20170102,
      0 to 23,
      "aa" :: "ab" :: "ba" :: "bb" :: Nil)
  }

  test("getPartitionsByFilter: (20170101 + 1)=ds") {
    testMetastorePartitionFiltering(
      "(20170101 + 1)=ds",
      20170102 to 20170102,
      0 to 23,
      "aa" :: "ab" :: "ba" :: "bb" :: Nil)
  }

  test("getPartitionsByFilter: ds=20170101 and h=10") {
    testMetastorePartitionFiltering(
      "ds=20170101 and h=10",
      20170101 to 20170101,
      10 to 10,
      "aa" :: "ab" :: "ba" :: "bb" :: Nil)
  }

  test("getPartitionsByFilter: ds=20170101 or ds=20170102") {
    testMetastorePartitionFiltering(
      "ds=20170101 or ds=20170102",
      20170101 to 20170102,
      0 to 23,
      "aa" :: "ab" :: "ba" :: "bb" :: Nil)
  }

  test("getPartitionsByFilter: ds in (20170102, 20170103)") {
    testMetastorePartitionFiltering(
      "ds in (20170102, 20170103)",
      20170102 to 20170103,
      0 to 23,
      "aa" :: "ab" :: "ba" :: "bb" :: Nil)
  }

  test("getPartitionsByFilter: (ds=20170101 and h>=8) or (ds=20170102 and h<8)") {
    val day1 = (20170101 to 20170101, 8 to 23, Seq("aa", "ab", "ba", "bb"))
    val day2 = (20170102 to 20170102, 0 to 7, Seq("aa", "ab", "ba", "bb"))
    testMetastorePartitionFiltering(
      "(ds=20170101 and h>=8) or (ds=20170102 and h<8)",
      day1 :: day2 :: Nil)
  }

  test("getPartitionsByFilter: " +
      "chunk in ('ab', 'ba') and ((ds=20170101 and h>=8) or (ds=20170102 and h<8))") {
    val day1 = (20170101 to 20170101, 8 to 23, Seq("ab", "ba"))
    val day2 = (20170102 to 20170102, 0 to 7, Seq("ab", "ba"))
    testMetastorePartitionFiltering(
      "chunk in ('ab', 'ba') and ((ds=20170101 and h>=8) or (ds=20170102 and h<8))",
      day1 :: day2 :: Nil)
  }

  private def testMetastorePartitionFiltering(filterString: String,
      expectedDs: Seq[Int], expectedH: Seq[Int], expectedChunks: Seq[String]): Unit = {
    testMetastorePartitionFiltering(filterString, (expectedDs, expectedH, expectedChunks) :: Nil)
  }

  private def testMetastorePartitionFiltering(filterString: String,
      expectedPartitionCubes: Seq[(Seq[Int], Seq[Int], Seq[String])]): Unit = {
    val filteredPartitions = client.getPartitionsByFilter(client.getTable("default", "test"),
      Seq(
        parseExpression(filterString)
      ))

    val expectedPartitionCount = expectedPartitionCubes.map {
      case (expectedDs, expectedH, expectedChunks) =>
        expectedDs.size * expectedH.size * expectedChunks.size
    }.sum

    val expectedPartitions = expectedPartitionCubes.map {
      case (expectedDs, expectedH, expectedChunks) =>
        for {
          ds <- expectedDs
          h <- expectedH
          chunk <- expectedChunks
        } yield Set(
          "ds" -> ds.toString,
          "h" -> h.toString,
          "chunk" -> chunk
        )
    }.reduce(_ ++ _)

    val actualFilteredPartitionCount = filteredPartitions.size

    assert(actualFilteredPartitionCount == expectedPartitionCount,
      s"Expected $expectedPartitionCount partitions but got $actualFilteredPartitionCount")
    assert(filteredPartitions.map(_.spec.toSet).toSet == expectedPartitions.toSet)
  }
}
