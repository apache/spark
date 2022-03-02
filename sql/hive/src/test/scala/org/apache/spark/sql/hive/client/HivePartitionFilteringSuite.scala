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

import java.sql.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
import org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe
import org.apache.hadoop.mapred.TextInputFormat
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils.DEFAULT_PARTITION_NAME
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, DateType, IntegerType, LongType, StringType, StructType}
import org.apache.spark.util.Utils

class HivePartitionFilteringSuite(version: String)
    extends HiveVersionSuite(version) with BeforeAndAfterAll with SQLHelper {

  private val tryDirectSqlKey = HiveConf.ConfVars.METASTORE_TRY_DIRECT_SQL.varname
  private val fallbackKey = SQLConf.HIVE_METASTORE_PARTITION_PRUNING_FALLBACK_ON_EXCEPTION.key
  private val pruningFastFallback = SQLConf.HIVE_METASTORE_PARTITION_PRUNING_FAST_FALLBACK.key

  // Support default partition in metastoredirectsql since HIVE-11898(Hive 2.0.0).
  private val defaultPartition = if (version >= "2.0") Some(DEFAULT_PARTITION_NAME) else None

  private val dsValue = 20170101 to 20170103
  private val hValue = 0 to 4
  private val chunkValue = Seq("aa", "ab", "ba", "bb")
  private val dateValue = Seq("2019-01-01", "2019-01-02", "2019-01-03") ++ defaultPartition
  private val dateStrValue = Seq("2020-01-01", "2020-01-02", "2020-01-03", "20200104", "20200105")
  private val testPartitionCount =
    dsValue.size * hValue.size * chunkValue.size * dateValue.size * dateStrValue.size

  private val storageFormat = CatalogStorageFormat(
    locationUri = None,
    inputFormat = Some(classOf[TextInputFormat].getName),
    outputFormat = Some(classOf[HiveIgnoreKeyTextOutputFormat[_, _]].getName),
    serde = Some(classOf[LazySimpleSerDe].getName()),
    compressed = false,
    properties = Map.empty
  )

  // Avoid repeatedly constructing multiple hive instances that do not use direct sql
  private var clientWithoutDirectSql: HiveClient = _

  private def init(tryDirectSql: Boolean): HiveClient = {
    val hadoopConf = new Configuration()
    hadoopConf.setBoolean(tryDirectSqlKey, tryDirectSql)
    hadoopConf.set("hive.metastore.warehouse.dir", Utils.createTempDir().toURI().toString())
    val client = buildClient(hadoopConf)
    val tableSchema =
      new StructType().add("value", "int").add("ds", "int").add("h", "int").add("chunk", "string")
        .add("d", "date").add("datestr", "string")
    val table = CatalogTable(
      identifier = TableIdentifier("test", Some("default")),
      tableType = CatalogTableType.MANAGED,
      schema = tableSchema,
      partitionColumnNames = Seq("ds", "h", "chunk", "d", "datestr"),
      storage = storageFormat)
    client.createTable(table, ignoreIfExists = false)

    val partitions =
      for {
        ds <- dsValue
        h <- hValue
        chunk <- chunkValue
        date <- dateValue
        dateStr <- dateStrValue
      } yield CatalogTablePartition(Map(
        "ds" -> ds.toString,
        "h" -> h.toString,
        "chunk" -> chunk,
        "d" -> date,
        "datestr" -> dateStr
      ), storageFormat)
    assert(partitions.size == testPartitionCount)

    client.createPartitions(
      "default", "test", partitions, ignoreIfExists = false)
    client
  }

  private def attr(name: String): Attribute = {
    client.getTable("default", "test").partitionSchema.fields
        .find(field => field.name.equals(name)) match {
      case Some(field) => AttributeReference(field.name, field.dataType)()
      case None =>
        fail(s"Illegal name of partition attribute: $name")
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    client = init(true)
    clientWithoutDirectSql = init(false)
  }

  test(s"getPartitionsByFilter returns all partitions when $fallbackKey=true") {
    withSQLConf(fallbackKey -> "true") {
      val filteredPartitions = clientWithoutDirectSql.getPartitionsByFilter(
        clientWithoutDirectSql.getTable("default", "test"),
        Seq(attr("ds") === 20170101))

      assert(filteredPartitions.size == testPartitionCount)
    }
  }

  test(s"getPartitionsByFilter should fail when $fallbackKey=false") {
    withSQLConf(fallbackKey -> "false") {
      val e = intercept[RuntimeException](
        clientWithoutDirectSql.getPartitionsByFilter(
          clientWithoutDirectSql.getTable("default", "test"),
          Seq(attr("ds") === 20170101)))
      assert(e.getMessage.contains("Caught Hive MetaException"))
    }
  }

  test("getPartitionsByFilter: ds<=>20170101") {
    // Should return all partitions where <=> is not supported
    testMetastorePartitionFiltering(
      attr("ds") <=> 20170101,
      dsValue,
      hValue,
      chunkValue,
      dateValue,
      dateStrValue)
  }

  test("getPartitionsByFilter: ds=20170101") {
    testMetastorePartitionFiltering(
      attr("ds") === 20170101,
      20170101 to 20170101,
      hValue,
      chunkValue,
      dateValue,
      dateStrValue)
  }

  test("getPartitionsByFilter: ds=(20170101 + 1) and h=0") {
    // Should return all partitions where h=0 because getPartitionsByFilter does not support
    // comparisons to non-literal values
    testMetastorePartitionFiltering(
      attr("ds") === (Literal(20170101) + 1) && attr("h") === 0,
      dsValue,
      0 to 0,
      chunkValue,
      dateValue,
      dateStrValue)
  }

  test("getPartitionsByFilter: chunk='aa'") {
    testMetastorePartitionFiltering(
      attr("chunk") === "aa",
      dsValue,
      hValue,
      "aa" :: Nil,
      dateValue,
      dateStrValue)
  }

  test("getPartitionsByFilter: cast(chunk as int)=1 (not a valid partition predicate)") {
    testMetastorePartitionFiltering(
      attr("chunk").cast(IntegerType) === 1,
      dsValue,
      hValue,
      chunkValue,
      dateValue,
      dateStrValue)
  }

  test("getPartitionsByFilter: cast(chunk as boolean)=true (not a valid partition predicate)") {
    testMetastorePartitionFiltering(
      attr("chunk").cast(BooleanType) === true,
      dsValue,
      hValue,
      chunkValue,
      dateValue,
      dateStrValue)
  }

  test("getPartitionsByFilter: 20170101=ds") {
    testMetastorePartitionFiltering(
      Literal(20170101) === attr("ds"),
      20170101 to 20170101,
      hValue,
      chunkValue,
      dateValue,
      dateStrValue)
  }

  test("getPartitionsByFilter: ds=20170101 and h=2") {
    testMetastorePartitionFiltering(
      attr("ds") === 20170101 && attr("h") === 2,
      20170101 to 20170101,
      2 to 2,
      chunkValue,
      dateValue,
      dateStrValue)
  }

  test("getPartitionsByFilter: cast(ds as long)=20170101L and h=2") {
    testMetastorePartitionFiltering(
      attr("ds").cast(LongType) === 20170101L && attr("h") === 2,
      20170101 to 20170101,
      2 to 2,
      chunkValue,
      dateValue,
      dateStrValue)
  }

  test("getPartitionsByFilter: ds=20170101 or ds=20170102") {
    testMetastorePartitionFiltering(
      attr("ds") === 20170101 || attr("ds") === 20170102,
      20170101 to 20170102,
      hValue,
      chunkValue,
      dateValue,
      dateStrValue)
  }

  test("getPartitionsByFilter: ds in (20170102, 20170103) (using IN expression)") {
    testMetastorePartitionFiltering(
      attr("ds").in(20170102, 20170103),
      20170102 to 20170103,
      hValue,
      chunkValue,
      dateValue,
      dateStrValue)
  }

  test("getPartitionsByFilter: cast(ds as long) in (20170102L, 20170103L) (using IN expression)") {
    testMetastorePartitionFiltering(
      attr("ds").cast(LongType).in(20170102L, 20170103L),
      20170102 to 20170103,
      hValue,
      chunkValue,
      dateValue,
      dateStrValue)
  }

  test("getPartitionsByFilter: ds in (20170102, 20170103) (using INSET expression)") {
    testMetastorePartitionFiltering(
      attr("ds").in(20170102, 20170103),
      20170102 to 20170103,
      hValue,
      chunkValue,
      dateValue,
      dateStrValue, {
        case expr @ In(v, list) if expr.inSetConvertible =>
          InSet(v, list.map(_.eval(EmptyRow)).toSet)
      })
  }

  test("getPartitionsByFilter: cast(ds as long) in (20170102L, 20170103L) (using INSET expression)")
  {
    testMetastorePartitionFiltering(
      attr("ds").cast(LongType).in(20170102L, 20170103L),
      20170102 to 20170103,
      hValue,
      chunkValue,
      dateValue,
      dateStrValue, {
        case expr @ In(v, list) if expr.inSetConvertible =>
          InSet(v, list.map(_.eval(EmptyRow)).toSet)
      })
  }

  test("getPartitionsByFilter: chunk in ('ab', 'ba') (using IN expression)") {
    testMetastorePartitionFiltering(
      attr("chunk").in("ab", "ba"),
      dsValue,
      hValue,
      "ab" :: "ba" :: Nil,
      dateValue,
      dateStrValue)
  }

  test("getPartitionsByFilter: chunk in ('ab', 'ba') (using INSET expression)") {
    testMetastorePartitionFiltering(
      attr("chunk").in("ab", "ba"),
      dsValue,
      hValue,
      "ab" :: "ba" :: Nil,
      dateValue,
      dateStrValue, {
        case expr @ In(v, list) if expr.inSetConvertible =>
          InSet(v, list.map(_.eval(EmptyRow)).toSet)
      })
  }

  test("getPartitionsByFilter: (ds=20170101 and h>=2) or (ds=20170102 and h<2)") {
    val day1 = (20170101 to 20170101, 2 to 4, chunkValue, dateValue, dateStrValue)
    val day2 = (20170102 to 20170102, 0 to 1, chunkValue, dateValue, dateStrValue)
    testMetastorePartitionFiltering((attr("ds") === 20170101 && attr("h") >= 2) ||
        (attr("ds") === 20170102 && attr("h") < 2), day1 :: day2 :: Nil)
  }

  test("getPartitionsByFilter: (ds=20170101 and h>=2) or (ds=20170102 and h<(1+1))") {
    val day1 = (20170101 to 20170101, 2 to 4, chunkValue, dateValue, dateStrValue)
    // Day 2 should include all hours because we can't build a filter for h<(7+1)
    val day2 = (20170102 to 20170102, 0 to 4, chunkValue, dateValue, dateStrValue)
    testMetastorePartitionFiltering((attr("ds") === 20170101 && attr("h") >= 2) ||
        (attr("ds") === 20170102 && attr("h") < (Literal(1) + 1)), day1 :: day2 :: Nil)
  }

  test("getPartitionsByFilter: " +
      "chunk in ('ab', 'ba') and ((ds=20170101 and h>=2) or (ds=20170102 and h<2))") {
    val day1 = (20170101 to 20170101, 2 to 4, Seq("ab", "ba"), dateValue, dateStrValue)
    val day2 = (20170102 to 20170102, 0 to 1, Seq("ab", "ba"), dateValue, dateStrValue)
    testMetastorePartitionFiltering(attr("chunk").in("ab", "ba") &&
        ((attr("ds") === 20170101 && attr("h") >= 2) || (attr("ds") === 20170102 && attr("h") < 2)),
      day1 :: day2 :: Nil)
  }

  test("getPartitionsByFilter: chunk contains bb") {
    testMetastorePartitionFiltering(
      attr("chunk").contains("bb"),
      dsValue,
      hValue,
      Seq("bb"),
      dateValue,
      dateStrValue)
  }

  test("getPartitionsByFilter: chunk startsWith b") {
    testMetastorePartitionFiltering(
      attr("chunk").startsWith("b"),
      dsValue,
      hValue,
      Seq("ba", "bb"),
      dateValue,
      dateStrValue)
  }

  test("getPartitionsByFilter: chunk endsWith b") {
    testMetastorePartitionFiltering(
      attr("chunk").endsWith("b"),
      dsValue,
      hValue,
      Seq("ab", "bb"),
      dateValue,
      dateStrValue)
  }

  test("getPartitionsByFilter: chunk in ('ab', 'ba') and ((cast(ds as string)>'20170102')") {
    testMetastorePartitionFiltering(
      attr("chunk").in("ab", "ba") && (attr("ds").cast(StringType) > "20170102"),
      dsValue,
      hValue,
      Seq("ab", "ba"),
      dateValue,
      dateStrValue)
  }

  test("getPartitionsByFilter: ds<>20170101") {
    testMetastorePartitionFiltering(
      attr("ds") =!= 20170101,
      20170102 to 20170103,
      hValue,
      chunkValue,
      dateValue,
      dateStrValue)
  }

  test("getPartitionsByFilter: h<>0 and chunk<>ab and d<>2019-01-01") {
    testMetastorePartitionFiltering(
      attr("h") =!= 0 && attr("chunk") =!= "ab" && attr("d") =!= Date.valueOf("2019-01-01"),
      dsValue,
      1 to 4,
      Seq("aa", "ba", "bb"),
      Seq("2019-01-02", "2019-01-03"),
      dateStrValue)
  }

  test("getPartitionsByFilter: d=2019-01-01") {
    testMetastorePartitionFiltering(
      attr("d") === Date.valueOf("2019-01-01"),
      dsValue,
      hValue,
      chunkValue,
      Seq("2019-01-01"),
      dateStrValue)
  }

  test("getPartitionsByFilter: d>2019-01-02") {
    testMetastorePartitionFiltering(
      attr("d") > Date.valueOf("2019-01-02"),
      dsValue,
      hValue,
      chunkValue,
      Seq("2019-01-03"),
      dateStrValue)
  }

  test("getPartitionsByFilter: In(d, 2019-01-01, 2019-01-02)") {
    testMetastorePartitionFiltering(
      In(attr("d"),
        Seq("2019-01-01", "2019-01-02").map(d => Literal(Date.valueOf(d)))),
      dsValue,
      hValue,
      chunkValue,
      Seq("2019-01-01", "2019-01-02"),
      dateStrValue)
  }

  test("getPartitionsByFilter: InSet(d, 2019-01-01, 2019-01-02)") {
    testMetastorePartitionFiltering(
      InSet(attr("d"),
        Set("2019-01-01", "2019-01-02").map(d => Literal(Date.valueOf(d)).eval(EmptyRow))),
      dsValue,
      hValue,
      chunkValue,
      Seq("2019-01-01", "2019-01-02"),
      dateStrValue)
  }

  test("getPartitionsByFilter: not in/inset string type") {
    def check(condition: Expression, result: Seq[String]): Unit = {
      testMetastorePartitionFiltering(
        condition,
        dsValue,
        hValue,
        result,
        dateValue,
        dateStrValue
      )
    }

    check(
      Not(In(attr("chunk"), Seq(Literal("aa"), Literal("ab")))),
      Seq("ba", "bb")
    )
    check(
      Not(In(attr("chunk"), Seq(Literal("aa"), Literal("ab"), Literal(null)))),
      chunkValue
    )

    check(
      Not(InSet(attr("chunk"), Set(Literal("aa").eval(), Literal("ab").eval()))),
      Seq("ba", "bb")
    )
    check(
      Not(InSet(attr("chunk"), Set("aa", "ab", null))),
      chunkValue
    )
  }

  test("getPartitionsByFilter: not in/inset date type") {
    def check(condition: Expression, result: Seq[String]): Unit = {
      testMetastorePartitionFiltering(
        condition,
        dsValue,
        hValue,
        chunkValue,
        result,
        dateStrValue
      )
    }

    check(
      Not(In(attr("d"),
        Seq(Literal(Date.valueOf("2019-01-01")),
          Literal(Date.valueOf("2019-01-02"))))),
      Seq("2019-01-03")
    )
    check(
      Not(In(attr("d"),
        Seq(Literal(Date.valueOf("2019-01-01")),
          Literal(Date.valueOf("2019-01-02")), Literal(null)))),
      dateValue
    )

    check(
      Not(InSet(attr("d"),
        Set(Literal(Date.valueOf("2019-01-01")).eval(),
          Literal(Date.valueOf("2019-01-02")).eval()))),
      Seq("2019-01-03")
    )
    check(
      Not(InSet(attr("d"),
        Set(Literal(Date.valueOf("2019-01-01")).eval(),
          Literal(Date.valueOf("2019-01-02")).eval(), null))),
      dateValue
    )
  }

  test("getPartitionsByFilter: cast(datestr as date)= 2020-01-01") {
    testMetastorePartitionFiltering(
      attr("datestr").cast(DateType) === Date.valueOf("2020-01-01"),
      dsValue,
      hValue,
      chunkValue,
      dateValue,
      dateStrValue)
  }

  test("getPartitionsByFilter: IS NULL / IS NOT NULL") {
    // returns all partitions
    Seq(attr("d").isNull, attr("d").isNotNull).foreach { filterExpr =>
      testMetastorePartitionFiltering(
        filterExpr,
        dsValue,
        hValue,
        chunkValue,
        dateValue,
        dateStrValue)
    }
  }

  test("getPartitionsByFilter: IS NULL / IS NOT NULL with other filter") {
    Seq(attr("d").isNull, attr("d").isNotNull).foreach { filterExpr =>
      testMetastorePartitionFiltering(
        filterExpr && attr("d") === Date.valueOf("2019-01-01"),
        dsValue,
        hValue,
        chunkValue,
        Seq("2019-01-01"),
        dateStrValue)
    }
  }

  test("getPartitionsByFilter: d =!= 2019-01-01") {
    testMetastorePartitionFiltering(
      attr("d") =!= Date.valueOf("2019-01-01"),
      dsValue,
      hValue,
      chunkValue,
      Seq("2019-01-02", "2019-01-03"),
      dateStrValue)
  }

  test("getPartitionsByFilter: d =!= 2019-01-01 || IS NULL") {
    testMetastorePartitionFiltering(
      attr("d") =!= Date.valueOf("2019-01-01") || attr("d").isNull,
      dsValue,
      hValue,
      chunkValue,
      dateValue,
      dateStrValue)
  }

  test("getPartitionsByFilter: d <=> 2019-01-01") {
    testMetastorePartitionFiltering(
      attr("d") <=> Date.valueOf("2019-01-01"),
      dsValue,
      hValue,
      chunkValue,
      dateValue,
      dateStrValue)
  }

  test("getPartitionsByFilter: d <=> null") {
    testMetastorePartitionFiltering(
      attr("d") <=> Literal(null, DateType),
      dsValue,
      hValue,
      chunkValue,
      dateValue,
      dateStrValue)
  }

  test("SPARK-35437: getPartitionsByFilter: substr(chunk,0,1)=a") {
    Seq("true" -> Seq("aa", "ab"), "false" -> chunkValue).foreach { t =>
      withSQLConf(pruningFastFallback -> t._1) {
        testMetastorePartitionFiltering(
          Substring(attr("chunk"), Literal(0), Literal(1)) === "a",
          dsValue,
          hValue,
          t._2,
          dateValue,
          dateStrValue)
      }
    }
  }

  test("SPARK-35437: getPartitionsByFilter: year(d)=2019") {
    Seq("true" -> Seq("2019-01-01", "2019-01-02", "2019-01-03"),
      "false" -> dateValue).foreach { t =>
      withSQLConf(pruningFastFallback -> t._1) {
        testMetastorePartitionFiltering(
          Year(attr("d")) === 2019,
          dsValue,
          hValue,
          chunkValue,
          t._2,
          dateStrValue)
      }
    }
  }

  test("SPARK-35437: getPartitionsByFilter: datestr=concat(2020-,01-,01)") {
    Seq("true" -> Seq("2020-01-01"), "false" -> dateStrValue).foreach { t =>
      withSQLConf(pruningFastFallback -> t._1) {
        testMetastorePartitionFiltering(
          attr("datestr") === Concat(Seq("2020-", "01-", "01")),
          dsValue,
          hValue,
          chunkValue,
          dateValue,
          t._2)
      }
    }
  }

  test(s"SPARK-35437: getPartitionsByFilter: ds=20170101 when $fallbackKey=true") {
    withSQLConf(fallbackKey -> "true", pruningFastFallback -> "true") {
      val filteredPartitions = clientWithoutDirectSql.getPartitionsByFilter(
        clientWithoutDirectSql.getTable("default", "test"),
        Seq(attr("ds") === 20170101))

      assert(filteredPartitions.size == 1 * hValue.size * chunkValue.size *
        dateValue.size * dateStrValue.size)
    }
  }

  test("SPARK-35437: getPartitionsByFilter: relax cast if does not need timezone") {
    // does not need time zone
    Seq(("true", "20200104" :: Nil), ("false", dateStrValue)).foreach {
      case (pruningFastFallbackEnabled, prunedPartition) =>
        withSQLConf(pruningFastFallback -> pruningFastFallbackEnabled) {
          testMetastorePartitionFiltering(
            attr("datestr").cast(IntegerType) === 20200104,
            dsValue,
            hValue,
            chunkValue,
            dateValue,
            prunedPartition)
        }
    }

    // need time zone
    Seq("true", "false").foreach { pruningFastFallbackEnabled =>
      withSQLConf(pruningFastFallback -> pruningFastFallbackEnabled) {
        testMetastorePartitionFiltering(
          attr("datestr").cast(DateType) === Date.valueOf("2020-01-01"),
          dsValue,
          hValue,
          chunkValue,
          dateValue,
          dateStrValue)
      }
    }
  }

  private def testMetastorePartitionFiltering(
      filterExpr: Expression,
      expectedDs: Seq[Int],
      expectedH: Seq[Int],
      expectedChunks: Seq[String],
      expectedD: Seq[String],
      expectedDatestr: Seq[String]): Unit = {
    testMetastorePartitionFiltering(
      filterExpr,
      (expectedDs, expectedH, expectedChunks, expectedD, expectedDatestr) :: Nil,
      identity)
  }

  private def testMetastorePartitionFiltering(
      filterExpr: Expression,
      expectedDs: Seq[Int],
      expectedH: Seq[Int],
      expectedChunks: Seq[String],
      expectedD: Seq[String],
      expectedDatestr: Seq[String],
      transform: Expression => Expression): Unit = {
    testMetastorePartitionFiltering(
      filterExpr,
      (expectedDs, expectedH, expectedChunks, expectedD, expectedDatestr) :: Nil,
      transform)
  }

  private def testMetastorePartitionFiltering(
      filterExpr: Expression,
      expectedPartitionCubes:
        Seq[(Seq[Int], Seq[Int], Seq[String], Seq[String], Seq[String])]): Unit = {
    testMetastorePartitionFiltering(filterExpr, expectedPartitionCubes, identity)
  }

  private def testMetastorePartitionFiltering(
      filterExpr: Expression,
      expectedPartitionCubes: Seq[(Seq[Int], Seq[Int], Seq[String], Seq[String], Seq[String])],
      transform: Expression => Expression): Unit = {
    val filteredPartitions = client.getPartitionsByFilter(client.getTable("default", "test"),
      Seq(
        transform(filterExpr)
      ))

    val expectedPartitionCount = expectedPartitionCubes.map {
      case (expectedDs, expectedH, expectedChunks, expectedD, expectedDatestr) =>
        expectedDs.size * expectedH.size * expectedChunks.size *
          expectedD.size * expectedDatestr.size
    }.sum

    val expectedPartitions = expectedPartitionCubes.map {
      case (expectedDs, expectedH, expectedChunks, expectedD, expectedDatestr) =>
        for {
          ds <- expectedDs
          h <- expectedH
          chunk <- expectedChunks
          d <- expectedD
          datestr <- expectedDatestr
        } yield Set(
          "ds" -> ds.toString,
          "h" -> h.toString,
          "chunk" -> chunk,
          "d" -> d,
          "datestr" -> datestr
        )
    }.reduce(_ ++ _)

    val actualFilteredPartitionCount = filteredPartitions.size

    assert(actualFilteredPartitionCount == expectedPartitionCount,
      s"Expected $expectedPartitionCount partitions but got $actualFilteredPartitionCount")
    assert(filteredPartitions.map(_.spec.toSet).toSet == expectedPartitions.toSet)
  }
}
