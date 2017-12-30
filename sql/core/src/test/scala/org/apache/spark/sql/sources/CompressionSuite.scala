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

package org.apache.spark.sql.sources

import java.io.File

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSQLContext, SQLTestUtils}
import org.apache.spark.util.Utils

abstract class CompressionTestUtils extends QueryTest with SQLTestUtils with BeforeAndAfterAll {

  var originalParquetCompressionCodeName: String = _
  var originalOrcCompressionCodeName: String = _

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    originalParquetCompressionCodeName = spark.conf.get(SQLConf.PARQUET_COMPRESSION.key)
    originalOrcCompressionCodeName = spark.conf.get(SQLConf.ORC_COMPRESSION.key)
    spark.conf.set(SQLConf.PARQUET_COMPRESSION.key, "snappy")
    spark.conf.set(SQLConf.ORC_COMPRESSION.key, "snappy")
  }

  protected override def afterAll(): Unit = {
    try {
      spark.conf.set(SQLConf.PARQUET_COMPRESSION.key, originalParquetCompressionCodeName)
      spark.conf.set(SQLConf.ORC_COMPRESSION.key, originalOrcCompressionCodeName)
    } finally {
      super.afterAll()
    }
  }

  protected override lazy val sql = spark.sql _

  protected def checkCTASCompression(
      formatClause: String,
      optionClause: String,
      expectedFileNameSuffix: String): Unit = {
    withTable("tab1") {
      sql(
        s"""
          |CREATE TABLE tab1
          |$formatClause
          |$optionClause
          |AS SELECT 1 as col1
        """.stripMargin)
      val path = spark.sessionState.catalog.getTableMetadata(TableIdentifier("tab1")).location
      val leafFiles = Utils.recursiveList(new File(path))
      assert(leafFiles.count(_.getName.endsWith(expectedFileNameSuffix)) == 1)
    }
  }

  protected def checkInsertCompression(
      format: String,
      isNative: Boolean,
      optionClause: String,
      tablePropertiesClause: String,
      isPartitioned: Boolean,
      expectedFileNameSuffix: String): Unit = {
    withTable("tab1") {
      val (schemaClause, partitionClause) = if (isPartitioned) {
        if (isNative) {
          ("(col1 int, col2 int)", "PARTITIONED BY (col2)")
        } else {
          ("(col1 int)", "PARTITIONED BY (col2 int)")
        }
      } else {
        ("(col1 int, col2 int)", "")
      }
      val formatClause = if (isNative) s"USING $format" else s"STORED AS $format"
      sql(
        s"""
           |CREATE TABLE tab1 $schemaClause
           |$formatClause
           |$optionClause
           |$partitionClause
           |$tablePropertiesClause
         """.stripMargin)
      sql(
        """
          |INSERT INTO TABLE tab1
          |SELECT 1 as col1, 2 as col2
        """.stripMargin)
      val path = if (isPartitioned) {
        spark.sessionState.catalog.getPartition(TableIdentifier("tab1"), Map("col2" -> "2"))
          .location
      } else {
        spark.sessionState.catalog.getTableMetadata(TableIdentifier("tab1")).location
      }
      val leafFiles = Utils.recursiveList(new File(path))
      assert(leafFiles.count(_.getName.endsWith(expectedFileNameSuffix)) == 1)
    }
  }
}

class CompressionSuite extends CompressionTestUtils with SharedSQLContext {

  test("CTAS against native data source table - parquet") {
    checkCTASCompression(
      formatClause = "USING parquet",
      optionClause = "OPTIONS('compression' = 'gzip')",
      expectedFileNameSuffix = "gz.parquet"
    )

    checkCTASCompression(
      formatClause = "USING parquet",
      optionClause = "TBLPROPERTIES('compression' = 'gzip')",
      expectedFileNameSuffix = "gz.parquet"
    )

    checkCTASCompression(
      formatClause = "USING parquet",
      optionClause =
        "OPTIONS('compression' = 'gzip') TBLPROPERTIES('compression' = 'uncompressed')",
      expectedFileNameSuffix = "gz.parquet"
    )
  }

  test("INSERT against native data source table - parquet") {
    Seq("false", "true").foreach { isPartitioned =>
      checkInsertCompression(
        format = "parquet",
        isNative = true,
        optionClause = "OPTIONS('compression' = 'gzip')",
        tablePropertiesClause = "",
        isPartitioned = isPartitioned.toBoolean,
        expectedFileNameSuffix = "gz.parquet"
      )

      checkInsertCompression(
        format = "parquet",
        isNative = true,
        optionClause = "",
        tablePropertiesClause = "TBLPROPERTIES('compression' = 'gzip')",
        isPartitioned = isPartitioned.toBoolean,
        expectedFileNameSuffix = "gz.parquet"
      )
    }
  }

}