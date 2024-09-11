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

import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetOutputFormat

import org.apache.spark.SparkIllegalArgumentException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ParquetCompressionCodecPrecedenceSuite extends ParquetTest with SharedSparkSession {
  test("Test `spark.sql.parquet.compression.codec` config") {
    ParquetCompressionCodec.values().foreach { codec =>
      withSQLConf(SQLConf.PARQUET_COMPRESSION.key -> codec.name()) {
        val expected = codec.getCompressionCodec.name()
        val option = new ParquetOptions(Map.empty[String, String], spark.sessionState.conf)
        assert(option.compressionCodecClassName == expected)
      }
    }
  }

  test("[SPARK-21786] Test Acquiring 'compressionCodecClassName' for parquet in right order.") {
    // When "compression" is configured, it should be the first choice.
    withSQLConf(
      SQLConf.PARQUET_COMPRESSION.key -> ParquetCompressionCodec.SNAPPY.lowerCaseName()) {
      val props = Map(
        "compression" -> ParquetCompressionCodec.UNCOMPRESSED.lowerCaseName(),
        ParquetOutputFormat.COMPRESSION -> ParquetCompressionCodec.GZIP.lowerCaseName())
      val option = new ParquetOptions(props, spark.sessionState.conf)
      assert(option.compressionCodecClassName == ParquetCompressionCodec.UNCOMPRESSED.name)
    }

    // When "compression" is not configured, "parquet.compression" should be the preferred choice.
    withSQLConf(
      SQLConf.PARQUET_COMPRESSION.key -> ParquetCompressionCodec.SNAPPY.lowerCaseName()) {
      val props =
        Map(ParquetOutputFormat.COMPRESSION -> ParquetCompressionCodec.GZIP.lowerCaseName())
      val option = new ParquetOptions(props, spark.sessionState.conf)
      assert(option.compressionCodecClassName == ParquetCompressionCodec.GZIP.name)
    }

    // When both "compression" and "parquet.compression" are not configured,
    // spark.sql.parquet.compression.codec should be the right choice.
    withSQLConf(
      SQLConf.PARQUET_COMPRESSION.key -> ParquetCompressionCodec.SNAPPY.lowerCaseName()) {
      val props = Map.empty[String, String]
      val option = new ParquetOptions(props, spark.sessionState.conf)
      assert(option.compressionCodecClassName == ParquetCompressionCodec.SNAPPY.name)
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
    val codecs = ParquetCompressionCodec.availableCodecs.asScala.map(_.name())
    Seq(true, false).foreach { isPartitioned =>
      codecs.foreach { compressionCodec =>
        checkCompressionCodec(compressionCodec, isPartitioned)
      }
    }
  }

  test("Create table with unknown compression") {
    Seq(true, false).foreach { isPartitioned =>
      checkError(
        exception = intercept[SparkIllegalArgumentException] {
          checkCompressionCodec("aa", isPartitioned)
        },
        condition = "CODEC_NOT_AVAILABLE.WITH_AVAILABLE_CODECS_SUGGESTION",
        parameters = Map(
          "codecName" -> "aa",
          "availableCodecs" -> ("brotli, uncompressed, lzo, snappy, " +
            "lz4_raw, none, zstd, lz4, gzip")))
    }
  }
}
