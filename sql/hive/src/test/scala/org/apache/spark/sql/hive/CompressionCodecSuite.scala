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

package org.apache.spark.sql.hive

import java.io.File
import java.util.Locale

import scala.jdk.CollectionConverters._

import org.apache.hadoop.fs.Path
import org.apache.orc.OrcConf.COMPRESS
import org.apache.parquet.hadoop.ParquetOutputFormat
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.execution.datasources.orc.{OrcCompressionCodec, OrcOptions}
import org.apache.spark.sql.execution.datasources.parquet.{ParquetCompressionCodec, ParquetOptions, ParquetTest}
import org.apache.spark.sql.hive.orc.OrcFileOperator
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf

class CompressionCodecSuite extends TestHiveSingleton with ParquetTest with BeforeAndAfterAll {
  import spark.implicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    (0 until maxRecordNum).toDF("a").createOrReplaceTempView("table_source")
  }

  override def afterAll(): Unit = {
    try {
      spark.catalog.dropTempView("table_source")
    } finally {
      super.afterAll()
    }
  }

  private val maxRecordNum = 50

  private def getConvertMetastoreConfName(format: String): String = {
    format.toLowerCase(Locale.ROOT) match {
      case "parquet" => HiveUtils.CONVERT_METASTORE_PARQUET.key
      case "orc" => HiveUtils.CONVERT_METASTORE_ORC.key
    }
  }

  private def getSparkCompressionConfName(format: String): String = {
    format.toLowerCase(Locale.ROOT) match {
      case "parquet" => SQLConf.PARQUET_COMPRESSION.key
      case "orc" => SQLConf.ORC_COMPRESSION.key
    }
  }

  private def getHiveCompressPropName(format: String): String = {
    format.toLowerCase(Locale.ROOT) match {
      case "parquet" => ParquetOutputFormat.COMPRESSION
      case "orc" => COMPRESS.getAttribute
    }
  }

  private def normalizeCodecName(format: String, name: String): String = {
    format.toLowerCase(Locale.ROOT) match {
      case "parquet" => ParquetOptions.getParquetCompressionCodecName(name)
      case "orc" => OrcOptions.getORCCompressionCodecName(name)
    }
  }

  private def getTableCompressionCodec(path: String, format: String): Seq[String] = {
    val hadoopConf = spark.sessionState.newHadoopConf()
    val codecs = format.toLowerCase(Locale.ROOT) match {
      case "parquet" => for {
        footer <- readAllFootersWithoutSummaryFiles(new Path(path), hadoopConf)
        block <- footer.getParquetMetadata.getBlocks.asScala
        column <- block.getColumns.asScala
      } yield column.getCodec.name()
      case "orc" => new File(path).listFiles().filter { file =>
        file.isFile && !file.getName.endsWith(".crc") && file.getName != "_SUCCESS"
      }.map { orcFile =>
        OrcFileOperator.getFileReader(orcFile.toPath.toString).get.getCompression.toString
      }.toSeq
    }
    codecs.distinct
  }

  private def createTable(
      rootDir: File,
      tableName: String,
      isPartitioned: Boolean,
      format: String,
      compressionCodec: Option[String]): Unit = {
    val tblProperties = compressionCodec match {
      case Some(prop) => s"TBLPROPERTIES('${getHiveCompressPropName(format)}'='$prop')"
      case _ => ""
    }
    val partitionCreate = if (isPartitioned) "PARTITIONED BY (p string)" else ""
    sql(
      s"""
        |CREATE TABLE $tableName(a int)
        |$partitionCreate
        |STORED AS $format
        |LOCATION '${rootDir.toURI.toString.stripSuffix("/")}/$tableName'
        |$tblProperties
      """.stripMargin)
  }

  private def writeDataToTable(
      tableName: String,
      partitionValue: Option[String]): Unit = {
    val partitionInsert = partitionValue.map(p => s"partition (p='$p')").mkString
    sql(
      s"""
        |INSERT INTO TABLE $tableName
        |$partitionInsert
        |SELECT * FROM table_source
      """.stripMargin)
  }

  private def writeDataToTableUsingCTAS(
      rootDir: File,
      tableName: String,
      partitionValue: Option[String],
      format: String,
      compressionCodec: Option[String]): Unit = {
    val partitionCreate = partitionValue.map(p => s"PARTITIONED BY (p)").mkString
    val compressionOption = compressionCodec.map { codec =>
      s",'${getHiveCompressPropName(format)}'='$codec'"
    }.mkString
    val partitionSelect = partitionValue.map(p => s",'$p' AS p").mkString
    sql(
      s"""
        |CREATE TABLE $tableName
        |USING $format
        |OPTIONS('path'='${rootDir.toURI.toString.stripSuffix("/")}/$tableName' $compressionOption)
        |$partitionCreate
        |AS SELECT * $partitionSelect FROM table_source
      """.stripMargin)
  }

  private def getPreparedTablePath(
      tmpDir: File,
      tableName: String,
      isPartitioned: Boolean,
      format: String,
      compressionCodec: Option[String],
      usingCTAS: Boolean): String = {
    val partitionValue = if (isPartitioned) Some("test") else None
    if (usingCTAS) {
      writeDataToTableUsingCTAS(tmpDir, tableName, partitionValue, format, compressionCodec)
    } else {
      createTable(tmpDir, tableName, isPartitioned, format, compressionCodec)
      writeDataToTable(tableName, partitionValue)
    }
    getTablePartitionPath(tmpDir, tableName, partitionValue)
  }

  private def getTableSize(path: String): Long = {
    val dir = new File(path)
    val files = dir.listFiles().filter(_.getName.startsWith("part-"))
    files.map(_.length()).sum
  }

  private def getTablePartitionPath(
      dir: File,
      tableName: String,
      partitionValue: Option[String]) = {
    val partitionPath = partitionValue.map(p => s"p=$p").mkString
    s"${dir.getPath.stripSuffix("/")}/$tableName/$partitionPath"
  }

  private def getUncompressedDataSizeByFormat(
      format: String, isPartitioned: Boolean, usingCTAS: Boolean): Long = {
    var totalSize = 0L
    val tableName = s"tbl_$format"
    val codecName = normalizeCodecName(format, "uncompressed")
    withSQLConf(getSparkCompressionConfName(format) -> codecName) {
      withTempDir { tmpDir =>
        withTable(tableName) {
          val compressionCodec = Option(codecName)
          val path = getPreparedTablePath(
            tmpDir, tableName, isPartitioned, format, compressionCodec, usingCTAS)
          totalSize = getTableSize(path)
        }
      }
    }
    assert(totalSize > 0L)
    totalSize
  }

  private def checkCompressionCodecForTable(
      format: String,
      isPartitioned: Boolean,
      compressionCodec: Option[String],
      usingCTAS: Boolean)
      (assertion: (String, Long) => Unit): Unit = {
    val tableName =
      if (usingCTAS) s"tbl_$format$isPartitioned" else s"tbl_$format${isPartitioned}_CAST"
    withTempDir { tmpDir =>
      withTable(tableName) {
        val path = getPreparedTablePath(
          tmpDir, tableName, isPartitioned, format, compressionCodec, usingCTAS)
        val relCompressionCodecs = getTableCompressionCodec(path, format)
        assert(relCompressionCodecs.length == 1)
        val tableSize = getTableSize(path)
        assertion(relCompressionCodecs.head, tableSize)
      }
    }
  }

  private def checkTableCompressionCodecForCodecs(
      format: String,
      isPartitioned: Boolean,
      convertMetastore: Boolean,
      usingCTAS: Boolean,
      compressionCodecs: List[String],
      tableCompressionCodecs: List[String])
      (assertionCompressionCodec: (Option[String], String, String, Long) => Unit): Unit = {
    withSQLConf(getConvertMetastoreConfName(format) -> convertMetastore.toString) {
      tableCompressionCodecs.zipAll(compressionCodecs, null, "SNAPPY").foreach {
        case (tableCompression, sessionCompressionCodec) =>
          withSQLConf(getSparkCompressionConfName(format) -> sessionCompressionCodec) {
            // 'tableCompression = null' means no table-level compression
            val compression = Option(tableCompression)
            checkCompressionCodecForTable(format, isPartitioned, compression, usingCTAS) {
              case (realCompressionCodec, tableSize) =>
                assertionCompressionCodec(
                  compression, sessionCompressionCodec, realCompressionCodec, tableSize)
            }
          }
      }
    }
  }

  // When the amount of data is small, compressed data size may be larger than uncompressed one,
  // so we just check the difference when compressionCodec is not NONE or UNCOMPRESSED.
  private def checkTableSize(
      format: String,
      compressionCodec: String,
      isPartitioned: Boolean,
      convertMetastore: Boolean,
      usingCTAS: Boolean,
      tableSize: Long): Boolean = {
    val uncompressedSize = getUncompressedDataSizeByFormat(format, isPartitioned, usingCTAS)
    compressionCodec match {
      case "UNCOMPRESSED" if format == "parquet" => tableSize == uncompressedSize
      case "NONE" if format == "orc" => tableSize == uncompressedSize
      case _ => tableSize != uncompressedSize
    }
  }

  def checkForTableWithCompressProp(
      format: String,
      tableCompressCodecs: List[String],
      sessionCompressCodecs: List[String]): Unit = {
    Seq(true, false).foreach { isPartitioned =>
      Seq(true, false).foreach { convertMetastore =>
        Seq(true, false).foreach { usingCTAS =>
          checkTableCompressionCodecForCodecs(
            format,
            isPartitioned,
            convertMetastore,
            usingCTAS,
            compressionCodecs = sessionCompressCodecs,
            tableCompressionCodecs = tableCompressCodecs) {
            case (tableCodec, sessionCodec, realCodec, tableSize) =>
              val expectCodec = tableCodec.getOrElse(sessionCodec)
              assert(expectCodec == realCodec)
              assert(checkTableSize(
                format, expectCodec, isPartitioned, convertMetastore, usingCTAS, tableSize))
          }
        }
      }
    }
  }

  test("both table-level and session-level compression are set") {
    checkForTableWithCompressProp("parquet",
      tableCompressCodecs = List(
        ParquetCompressionCodec.UNCOMPRESSED.name,
        ParquetCompressionCodec.SNAPPY.name,
        ParquetCompressionCodec.GZIP.name),
      sessionCompressCodecs = List(
        ParquetCompressionCodec.SNAPPY.name,
        ParquetCompressionCodec.GZIP.name,
        ParquetCompressionCodec.SNAPPY.name))
    checkForTableWithCompressProp("orc",
      tableCompressCodecs =
        List(
          OrcCompressionCodec.NONE.name,
          OrcCompressionCodec.SNAPPY.name,
          OrcCompressionCodec.ZLIB.name),
      sessionCompressCodecs =
        List(
          OrcCompressionCodec.SNAPPY.name,
          OrcCompressionCodec.ZLIB.name,
          OrcCompressionCodec.SNAPPY.name))
  }

  test("table-level compression is not set but session-level compressions is set ") {
    checkForTableWithCompressProp("parquet",
      tableCompressCodecs = List.empty,
      sessionCompressCodecs = List(
        ParquetCompressionCodec.UNCOMPRESSED.name,
        ParquetCompressionCodec.SNAPPY.name,
        ParquetCompressionCodec.GZIP.name))
    checkForTableWithCompressProp("orc",
      tableCompressCodecs = List.empty,
      sessionCompressCodecs =
        List(
          OrcCompressionCodec.NONE.name,
          OrcCompressionCodec.SNAPPY.name,
          OrcCompressionCodec.ZLIB.name))
  }

  def checkTableWriteWithCompressionCodecs(format: String, compressCodecs: List[String]): Unit = {
    Seq(true, false).foreach { isPartitioned =>
      Seq(true, false).foreach { convertMetastore =>
        withTempDir { tmpDir =>
          val tableName = s"tbl_$format$isPartitioned"
          createTable(tmpDir, tableName, isPartitioned, format, None)
          withTable(tableName) {
            compressCodecs.foreach { compressionCodec =>
              val partitionValue = if (isPartitioned) Some(compressionCodec) else None
              withSQLConf(getConvertMetastoreConfName(format) -> convertMetastore.toString,
                getSparkCompressionConfName(format) -> compressionCodec
              ) { writeDataToTable(tableName, partitionValue) }
            }
            val tablePath = getTablePartitionPath(tmpDir, tableName, None)
            val realCompressionCodecs =
              if (isPartitioned) compressCodecs.flatMap { codec =>
                getTableCompressionCodec(s"$tablePath/p=$codec", format)
              } else {
                getTableCompressionCodec(tablePath, format)
              }

            assert(realCompressionCodecs.distinct.sorted == compressCodecs.sorted)
            val recordsNum = sql(s"SELECT * from $tableName").count()
            assert(recordsNum == maxRecordNum * compressCodecs.length)
          }
        }
      }
    }
  }

  test("test table containing mixed compression codec") {
    checkTableWriteWithCompressionCodecs("parquet",
      List(
        ParquetCompressionCodec.UNCOMPRESSED.name,
        ParquetCompressionCodec.SNAPPY.name,
        ParquetCompressionCodec.GZIP.name))
    checkTableWriteWithCompressionCodecs(
      "orc",
      List(
        OrcCompressionCodec.NONE.name,
        OrcCompressionCodec.SNAPPY.name,
        OrcCompressionCodec.ZLIB.name))
  }
}
