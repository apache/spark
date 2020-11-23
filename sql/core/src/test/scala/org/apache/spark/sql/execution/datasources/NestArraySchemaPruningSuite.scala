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

package org.apache.spark.sql.execution.datasources

import java.io.File

import org.scalactic.Equality

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.SchemaPruningTest
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType


class NestArraySchemaPruningSuite
  extends QueryTest
    with FileBasedDataSourceTest
    with SchemaPruningTest
    with SharedSparkSession
    with AdaptiveSparkPlanHelper {
  case class AdRecord(positions: Array[Positions])
  case class Positions(imps: Array[Impression])
  case class Impression(id: String, ad: Advertising, clicks: Array[Clicks])
  case class Advertising(index: Int)
  case class Clicks(fraud_type: Int)

  val adRecords = AdRecord(Array(Positions(Array(Impression("1", Advertising(1),
    Array(Clicks(0), Clicks(1))))))) :: AdRecord(Array(Positions(Array(
    Impression("2", Advertising(2), Array(Clicks(1), Clicks(2))))))) :: Nil

  testSchemaPruning("Nested arrays for pruning schema") {
    val queryIndex = sql("select positions.imps.ad.index from adRecords")
    checkScan(queryIndex,
      "struct<positions:array<struct<imps:array<struct<ad:struct<index:int>>>>>>")
    checkAnswer(queryIndex, Row(Seq(Seq(1))) :: Row(Seq(Seq(2))) :: Nil)

    val queryId = sql("select positions.imps.id from adRecords")
    checkScan(queryId,
      "struct<positions:array<struct<imps:array<struct<id:string>>>>>")
    checkAnswer(queryId, Row(Seq(Seq("1"))) :: Row(Seq(Seq("2"))) :: Nil)

    val queryIndexAndFraud =
      sql("select positions.imps.ad.index, positions.imps.clicks.fraud_type from adRecords")
    checkScan(queryIndexAndFraud, "struct<positions:array<struct<imps:array<struct<ad:struct" +
      "<index:int>, clicks:array<struct<fraud_type:int>>>>>>>")
    checkAnswer(queryIndexAndFraud, Row(Seq(Seq(1)), Seq(Seq(Seq(0, 1))))
      :: Row(Seq(Seq(2)), Seq(Seq(Seq(1, 2)))) :: Nil)
  }

  protected def testSchemaPruning(testName: String)(testThunk: => Unit): Unit = {
    test(s"$testName") {
      withSQLConf(vectorizedReaderEnabledKey -> "true") {
        withData(testThunk)
      }
      withSQLConf(vectorizedReaderEnabledKey -> "false") {
        withData(testThunk)
      }
    }
  }

  private def withData(testThunk: => Unit): Unit = {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      makeDataSourceFile(adRecords, new File(path + "/ad_records/a=1"))

      val schema = "`positions` ARRAY<STRUCT<`imps`: ARRAY<STRUCT<`id`: STRING, " +
        "`ad`: STRUCT<`index`: INT>, `clicks`: ARRAY<STRUCT<`fraud_type`: INT>>>>>>"
      spark.read.format(dataSourceName).schema(schema).load(path + "/ad_records")
        .createOrReplaceTempView("adRecords")

      testThunk
    }
  }

  protected val schemaEquality = new Equality[StructType] {
    override def areEqual(a: StructType, b: Any): Boolean =
      b match {
        case otherType: StructType => a.sameType(otherType)
        case _ => false
      }
  }

  protected def checkScan(df: DataFrame, expectedSchemaCatalogStrings: String*): Unit = {
    checkScanSchemata(df, expectedSchemaCatalogStrings: _*)
    // We check here that we can execute the query without throwing an exception. The results
    // themselves are irrelevant, and should be checked elsewhere as needed
    df.collect()
  }

  protected def checkScanSchemata(df: DataFrame, expectedSchemaCatalogStrings: String*): Unit = {
    val fileSourceScanSchemata =
      collect(df.queryExecution.executedPlan) {
        case scan: FileSourceScanExec => scan.requiredSchema
      }
    assert(fileSourceScanSchemata.size === expectedSchemaCatalogStrings.size,
      s"Found ${fileSourceScanSchemata.size} file sources in dataframe, " +
        s"but expected $expectedSchemaCatalogStrings")
    fileSourceScanSchemata.zip(expectedSchemaCatalogStrings).foreach {
      case (scanSchema, expectedScanSchemaCatalogString) =>
        val expectedScanSchema = CatalystSqlParser.parseDataType(expectedScanSchemaCatalogString)
        implicit val equality = schemaEquality
        assert(scanSchema === expectedScanSchema)
    }
  }

  override protected val dataSourceName: String = "parquet"
  override protected val vectorizedReaderEnabledKey: String =
    SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key
}
