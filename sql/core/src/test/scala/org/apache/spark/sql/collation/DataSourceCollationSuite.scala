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

package org.apache.spark.sql.collation

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.text.TextFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

abstract class DataSourceCollationSuite extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  protected val lcaseCollation = "UNICODE_CI"

  protected val dataSourceMap = Map(
    "parquet" -> new ParquetFileFormat,
    "orc" -> new OrcFileFormat,
    "json" -> new JsonFileFormat,
    "csv" -> new CSVFileFormat,
    "text" -> new TextFileFormat
  )

  def testDataSourceWrite(dataSource: String, useCollations: Boolean): Unit = {
    withTempPath { path =>
      val stringType = if (useCollations) s"COLLATE(c, '$lcaseCollation')" else "c"
      val fileFormat = dataSourceMap(dataSource)

      val df = sql(
        s"""
           |SELECT
           |  $stringType as c1
           |FROM VALUES ('a'), ('b'), ('A')
           |AS data(c)
           |""".stripMargin)

      val action = () => df.write
        .format(dataSource)
        .save(path.getAbsolutePath)

      if (!useCollations || fileFormat.supportCollations) {
        action()

        val expectedRowCnt = if (useCollations) 2 else 1
        val expectedCollation = if (useCollations) lcaseCollation else "UTF8_BINARY"
        val readBack = spark.read.format(dataSource).load(path.getAbsolutePath)
        val columnName = readBack.schema.fields.head.name
        checkAnswer(
          readBack.selectExpr(s"collation($columnName)").distinct(), Seq(Row(expectedCollation)))
        assert(readBack.where(s"$columnName = 'a'").count() === expectedRowCnt)
      } else {
        checkError(
          exception = intercept[AnalysisException] {
            action()
          },
          errorClass = "UNSUPPORTED_COLLATIONS_FOR_DATASOURCE",
          sqlState = "0A000",
          Map("columnName" -> "`c1`", "format" -> fileFormat.toString)
        )
      }
    }
  }

  def testDataSourceSaveAsTable(dataSource: String, useCollations: Boolean): Unit = {
    val tableName = "tbl"
    withTable(tableName) {
      val stringType = if (useCollations) s"COLLATE(c, '$lcaseCollation')" else "c"

      val df = sql(
        s"""
           |SELECT
           |  $stringType as c1
           |FROM VALUES ('a'), ('b'), ('A')
           |AS data(c)
           |""".stripMargin)

      // should always work
      df.write
        .format(dataSource)
        .saveAsTable(tableName)

      verifyForTable(tableName, "c1", useCollations)
    }
  }

  def testManagedTableInsert(dataSource: String, useCollations: Boolean): Unit = {
    val tableName = "tbl"
    withTable(tableName) {
      val stringType = if (useCollations) s"STRING COLLATE $lcaseCollation" else "STRING"

      // should always work
      sql(
        s"""
           |CREATE TABLE $tableName (c1 $stringType)
           |USING $dataSource
           |""".stripMargin)

      sql(s"INSERT INTO $tableName VALUES ('a'), ('b'), ('A')")

      verifyForTable(tableName, "c1", useCollations)
    }
  }

  private def verifyForTable(
      tableName: String,
      columnName: String,
      useCollations: Boolean): Unit = {
    val expectedCollation = if (useCollations) lcaseCollation else "UTF8_BINARY"
    val expectedRowCnt = if (useCollations) 2 else 1
    checkAnswer(
      sql(s"SELECT DISTINCT COLLATION($columnName) FROM $tableName"), Seq(Row(expectedCollation)))
    assert(
      sql(s"SELECT * FROM $tableName WHERE $columnName = 'a'").collect().length === expectedRowCnt)

    val newCollation = "UNICODE"
    sql(s"ALTER TABLE $tableName ALTER COLUMN $columnName TYPE STRING COLLATE $newCollation")
    checkAnswer(
      sql(s"SELECT DISTINCT COLLATION(c1) FROM $tableName"), Seq(Row(newCollation)))
    assert(sql(s"SELECT * FROM $tableName WHERE $columnName = 'a'").collect().length === 1)
  }

  dataSourceMap.keys.foreach { source =>
    Seq(true, false).foreach { useCollations =>
      test(s"data source write $source with collations: $useCollations") {
        testDataSourceWrite(source, useCollations)
      }

      test(s"data source save as table $source with collations: $useCollations") {
        testDataSourceSaveAsTable(source, useCollations)
      }

      test(s"managed table insert $source with collations: $useCollations") {
          testManagedTableInsert(source, useCollations)
      }
    }
  }
}

class V1DataSourceCollationSuite extends DataSourceCollationSuite {
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, dataSourceMap.keys.mkString(","))
}

class V2DataSourceCollationSuite extends DataSourceCollationSuite {
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "")
}
