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

package org.apache.spark.sql.execution.datasources.parquet

import java.io.File

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetOutputFormat

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext

class ParquetCompressionCodecPrecedenceSuite extends ParquetTest with SharedSQLContext {
  test("Test `spark.sql.parquet.compression.codec` config") {
    Seq("NONE", "UNCOMPRESSED", "SNAPPY", "GZIP", "LZO", "LZ4", "BROTLI", "ZSTD").foreach { c =>
      withSQLConf(SQLConf.PARQUET_COMPRESSION.key -> c) {
        val expected = if (c == "NONE") "UNCOMPRESSED" else c
        val option = new ParquetOptions(Map.empty[String, String], spark.sessionState.conf)
        assert(option.compressionCodecClassName == expected)
      }
    }
  }

  test("[SPARK-21786] Test Acquiring 'compressionCodecClassName' for parquet in right order.") {
    // When "compression" is configured, it should be the first choice.
    withSQLConf(SQLConf.PARQUET_COMPRESSION.key -> "snappy") {
      val props = Map("compression" -> "uncompressed", ParquetOutputFormat.COMPRESSION -> "gzip")
      val option = new ParquetOptions(props, spark.sessionState.conf)
      assert(option.compressionCodecClassName == "UNCOMPRESSED")
    }

    // When "compression" is not configured, "parquet.compression" should be the preferred choice.
    withSQLConf(SQLConf.PARQUET_COMPRESSION.key -> "snappy") {
      val props = Map(ParquetOutputFormat.COMPRESSION -> "gzip")
      val option = new ParquetOptions(props, spark.sessionState.conf)
      assert(option.compressionCodecClassName == "GZIP")
    }

    // When both "compression" and "parquet.compression" are not configured,
    // spark.sql.parquet.compression.codec should be the right choice.
    withSQLConf(SQLConf.PARQUET_COMPRESSION.key -> "snappy") {
      val props = Map.empty[String, String]
      val option = new ParquetOptions(props, spark.sessionState.conf)
      assert(option.compressionCodecClassName == "SNAPPY")
    }
  }

  private def getTableCompressionCodec(path: String): Seq[String] = {
    val hadoopConf = spark.sessionState.newHadoopConf()
    val codecs = for {
      footer <- readAllFootersWithoutSummaryFiles(new Path(path), hadoopConf)
      block <- footer.getParquetMetadata.getBlocks.asScala
      column <- block.getColumns.asScala
    } yield column.getCodec.name()
    codecs.distinct
  }

  private def createTableWithCompression(
      tableName: String,
      isPartitioned: Boolean,
      compressionCodec: String,
      rootDir: File): Unit = {
    val options =
      s"""
        |OPTIONS('path'='${rootDir.toURI.toString.stripSuffix("/")}/$tableName',
        |'parquet.compression'='$compressionCodec')
       """.stripMargin
    val partitionCreate = if (isPartitioned) "PARTITIONED BY (p)" else ""
    sql(
      s"""
        |CREATE TABLE $tableName USING Parquet $options $partitionCreate
        |AS SELECT 1 AS col1, 2 AS p
       """.stripMargin)
  }

  private def checkCompressionCodec(compressionCodec: String, isPartitioned: Boolean): Unit = {
    withTempDir { tmpDir =>
      val tempTableName = "TempParquetTable"
      withTable(tempTableName) {
        createTableWithCompression(tempTableName, isPartitioned, compressionCodec, tmpDir)
        val partitionPath = if (isPartitioned) "p=2" else ""
        val path = s"${tmpDir.getPath.stripSuffix("/")}/$tempTableName/$partitionPath"
        val realCompressionCodecs = getTableCompressionCodec(path)
        assert(realCompressionCodecs.forall(_ == compressionCodec))
      }
    }
  }

  test("Create parquet table with compression") {
    Seq(true, false).foreach { isPartitioned =>
      Seq("UNCOMPRESSED", "SNAPPY", "GZIP").foreach { compressionCodec =>
        checkCompressionCodec(compressionCodec, isPartitioned)
      }
    }
  }

  test("Create table with unknown compression") {
    Seq(true, false).foreach { isPartitioned =>
      val exception = intercept[IllegalArgumentException] {
        checkCompressionCodec("aa", isPartitioned)
      }
      assert(exception.getMessage.contains("Codec [aa] is not available"))
    }
  }
}
