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

import java.time.Instant

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.spark.sql.Column
import org.scalatest.{BeforeAndAfterAll, Ignore}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EmptyRow, EqualTo, Expression, In, InSet, Literal}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.DataTypes

class HivePartitionFilteringSuite(version: String)
    extends HiveVersionSuite(version) with BeforeAndAfterAll {
  import CatalystSqlParser._

  private val tryDirectSqlKey = HiveConf.ConfVars.METASTORE_TRY_DIRECT_SQL.varname

  private val testPartitionCount = 3 * 24 * 4 * 2 * 4
  private val chunkValues = Seq("aa", "ab", "ba", "bb")
  private val dValues = 20170101 to 20170103
  private val hValues = 0 to 23
  private val tValues =
    Seq(Instant.parse("2017-12-24T00:00:00.00Z"), Instant.parse("2017-12-25T00:00:00.00Z"))
  private val decValues = Seq(BigDecimal(1D), BigDecimal(2D), BigDecimal(3D), BigDecimal(4D))

  private def init(tryDirectSql: Boolean): HiveClient = {
    val hadoopConf = new Configuration()
    hadoopConf.setBoolean(tryDirectSqlKey, tryDirectSql)
    val client = buildClient(hadoopConf)

    val storageFormat = CatalogStorageFormat(
      locationUri = None,
      inputFormat = None,
      outputFormat = None,
      serde = None,
      compressed = false,
      properties = Map.empty)

    client
      .runSqlHive("CREATE TABLE test (value INT) " +
        "PARTITIONED BY (ds INT, h INT, chunk STRING, t TIMESTAMP, d DECIMAL)")

    val partitions =
      for {
        ds <- dValues
        h <- hValues
        chunk <- chunkValues
        t <- tValues
        d <- decValues
      } yield CatalogTablePartition(Map(
        "ds" -> ds.toString,
        "h" -> h.toString,
        "chunk" -> chunk,
        "t" -> t.getEpochSecond.toString,
        "d" -> d.toString
      ), storageFormat)
    assert(partitions.size == testPartitionCount)

    client.createPartitions(
      "default", "test", partitions, ignoreIfExists = false)

    client
  }

  override def beforeAll() {
    client = init(true)
  }

  test(s"getPartitionsByFilter returns all partitions when $tryDirectSqlKey=false") {
    val client = init(false)
    val filteredPartitions = client.getPartitionsByFilter(client.getTable("default", "test"),
      Seq(parseExpression("ds=20170101")))

    assert(filteredPartitions.size == testPartitionCount)
  }

  test("getPartitionsByFilter: ds<=>20170101") {
    // Should return all partitions where <=> is not supported
    assertNoFilterIsApplied("ds<=>20170101")
  }

  test("getPartitionsByFilter: ds=20170101") {
    testMetastorePartitionFiltering("ds=20170101", 20170101 to 20170101)
  }

  test("getPartitionsByFilter: ds=(20170101 + 1) and h=0") {
    // Should return all partitions where h=0 because getPartitionsByFilter does not support
    // comparisons to non-literal values
    testMetastorePartitionFiltering(
      "ds=(20170101 + 1) and h=0",
      dValues, 0 to 0)
  }

  test("getPartitionsByFilter: chunk='aa'") {
    testMetastorePartitionFiltering(
      "chunk='aa'",
      dValues, hValues,
      "aa" :: Nil)
  }

  test("getPartitionsByFilter: 20170101=ds") {
    testMetastorePartitionFiltering(
      "20170101=ds",
      20170101 to 20170101)
  }

  test("getPartitionsByFilter: must ignore unsupported expressions") {
    testMetastorePartitionFiltering(
      "ds is not null and chunk is not null and 20170101=ds and chunk = 'aa'",
      20170101 to 20170101,
      hValues,
      "aa" :: Nil)
  }

  test("getPartitionsByFilter: multiple or single expressions expressions yield the same result") {
    testMetastorePartitionFiltering(
      "ds is not null and chunk is not null and (20170101=ds) and (chunk = 'aa')",
      20170101 to 20170101,
      hValues,
      "aa" :: Nil)

    testMetastorePartitionFiltering(
      Seq(parseExpression("ds is not null"),
        parseExpression("chunk is not null"),
        parseExpression("(20170101=ds)"),
        EqualTo(AttributeReference("chunk", DataTypes.StringType)(), Literal.apply("aa"))),
      20170101 to 20170101,
      hValues,
      "aa" :: Nil,
      tValues,
      decValues)
  }

  test("getPartitionsByFilter: ds=20170101 and h=10") {
    testMetastorePartitionFiltering(
      "ds=20170101 and h=10",
      20170101 to 20170101,
      10 to 10)
  }

  test("getPartitionsByFilter: ds=20170101 or ds=20170102") {
    testMetastorePartitionFiltering(
      "ds=20170101 or ds=20170102",
      20170101 to 20170102)
  }

  test("getPartitionsByFilter: ds in (20170102, 20170103) (using IN expression)") {
    testMetastorePartitionFiltering(
      "ds in (20170102, 20170103)",
      20170102 to 20170103)
  }

  test("getPartitionsByFilter: ds in (20170102, 20170103) (using INSET expression)") {
    testMetastorePartitionFiltering(
      Seq(parseExpression("ds in (20170102, 20170103)") match {
        case expr @ In(v, list) if expr.inSetConvertible =>
          InSet(v, list.map(_.eval(EmptyRow)).toSet)
      }),
      20170102 to 20170103,
      hValues,
      chunkValues,
      tValues,
      decValues)
  }

  test("getPartitionsByFilter: chunk in ('ab', 'ba') (using IN expression)") {
    testMetastorePartitionFiltering(
      "chunk in ('ab', 'ba')",
      dValues,
      hValues,
      "ab" :: "ba" :: Nil)
  }

  test("getPartitionsByFilter: chunk in ('ab', 'ba') (using INSET expression)") {
    testMetastorePartitionFiltering(
      Seq(parseExpression("chunk in ('ab', 'ba')") match {
        case expr @ In(v, list) if expr.inSetConvertible =>
          InSet(v, list.map(_.eval(EmptyRow)).toSet)
      }),
      dValues,
      hValues,
      "ab" :: "ba" :: Nil,
      tValues,
      decValues)
  }

  test("getPartitionsByFilter: (ds=20170101 and h>=8) or (ds=20170102 and h<8)") {
    val day1 = (20170101 to 20170101, 8 to 23, chunkValues, tValues, decValues)
    val day2 = (20170102 to 20170102, 0 to 7, chunkValues, tValues, decValues)
    testMetastorePartitionFiltering(
      "(ds=20170101 and h>=8) or (ds=20170102 and h<8)",
      day1 :: day2 :: Nil)
  }

  test("getPartitionsByFilter: (ds=20170101 and h>=8) or (ds=20170102 and h<(7+1))") {
    val day1 = (20170101 to 20170101, 8 to 23, chunkValues, tValues, decValues)
    // Day 2 should include all hours because we can't build a filter for h<(7+1)
    val day2 = (20170102 to 20170102, 0 to 23, chunkValues, tValues, decValues)
    testMetastorePartitionFiltering(
      "(ds=20170101 and h>=8) or (ds=20170102 and h<(7+1))",
      day1 :: day2 :: Nil)
  }

  test("getPartitionsByFilter: " +
      "chunk in ('ab', 'ba') and ((ds=20170101 and h>=8) or (ds=20170102 and h<8))") {
    val day1 = (20170101 to 20170101, 8 to 23, Seq("ab", "ba"), tValues, decValues)
    val day2 = (20170102 to 20170102, 0 to 7, Seq("ab", "ba"), tValues, decValues)
    testMetastorePartitionFiltering(
      "chunk in ('ab', 'ba') and ((ds=20170101 and h>=8) or (ds=20170102 and h<8))",
      day1 :: day2 :: Nil)
  }

  ignore("TODO: create hive metastore for integration test " +
    "that supports timestamp and decimal pruning") {
    test("getPartitionsByFilter: t = '2017-12-24T00:00:00.00Z' (timestamp test)") {
      testMetastorePartitionFiltering(
        "t = '2017-12-24T00:00:00.00Z'",
        dValues,
        hValues,
        chunkValues,
        Seq(Instant.parse("2017-12-24T00:00:00.00Z"))
      )
    }

    test("getPartitionsByFilter: d = 4.0 (decimal test)") {
      testMetastorePartitionFiltering(
        "d = 4.0",
        dValues,
        hValues,
        chunkValues,
        tValues,
        Seq(BigDecimal(4.0D))
      )
    }
  }

  private def assertNoFilterIsApplied(expression: String) = {
    val filteredPartitions = client.getPartitionsByFilter(client.getTable("default", "test"),
      Seq(parseExpression(expression)))

    assert(filteredPartitions.size == testPartitionCount)
  }

  private def testMetastorePartitionFiltering(
      filters: Seq[Expression],
      expectedDs: Seq[Int],
      expectedH: Seq[Int],
      expectedChunks: Seq[String],
      expectedTs: Seq[Instant],
      expectedDecs: Seq[BigDecimal]): Unit = {
    testMetastorePartitionFiltering(
      filters,
      (expectedDs, expectedH, expectedChunks, expectedTs, expectedDecs) :: Nil)
  }

  private def testMetastorePartitionFiltering(
      filterString: String,
      expectedDs: Seq[Int] = dValues,
      expectedH: Seq[Int] = hValues,
      expectedChunks: Seq[String] = chunkValues,
      expectedTs: Seq[Instant] = tValues,
      expectedDecs: Seq[BigDecimal] = decValues): Unit = {
    testMetastorePartitionFiltering(Seq(parseExpression(filterString)),
      expectedDs,
      expectedH,
      expectedChunks,
      expectedTs,
      expectedDecs)
  }

  private def testMetastorePartitionFiltering(
      filterString: String,
      expectedPartitionCubes: Seq[(Seq[Int], Seq[Int], Seq[String], Seq[Instant], Seq[BigDecimal])]
  ): Unit = {
    testMetastorePartitionFiltering(Seq(parseExpression(filterString)),
      expectedPartitionCubes)
  }

  private def testMetastorePartitionFiltering(
      predicates: Seq[Expression],
      expectedPartitionCubes:
        Seq[(Seq[Int], Seq[Int], Seq[String], Seq[Instant], Seq[BigDecimal])]): Unit = {
    val filteredPartitions =
      client.getPartitionsByFilter(client.getTable("default", "test"), predicates)

    val expectedPartitionCount = expectedPartitionCubes.map {
      case (expectedDs, expectedH, expectedChunks, expectedTs, expectedDecs) =>
        expectedDs.size * expectedH.size * expectedChunks.size * expectedTs.size * expectedDecs.size
    }.sum

    val expectedPartitions = expectedPartitionCubes.map {
      case (expectedDs, expectedH, expectedChunks, expectedTs, expectedDecs) =>
        for {
          ds <- expectedDs
          h <- expectedH
          chunk <- expectedChunks
          t <- expectedTs
          d <- expectedDecs
        } yield Set(
          "ds" -> ds.toString,
          "h" -> h.toString,
          "chunk" -> chunk,
          "t" -> t.getEpochSecond.toString,
          "d" -> d.toString()
        )
    }.reduce(_ ++ _)

    val actualFilteredPartitionCount = filteredPartitions.size

    assert(actualFilteredPartitionCount == expectedPartitionCount,
      s"Expected $expectedPartitionCount partitions but got $actualFilteredPartitionCount")
    assert(filteredPartitions.map(_.spec.toSet).toSet == expectedPartitions.toSet)
  }
}
