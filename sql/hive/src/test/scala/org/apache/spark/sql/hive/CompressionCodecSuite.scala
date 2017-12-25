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

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path
import org.apache.orc.OrcConf.COMPRESS
import org.apache.parquet.hadoop.ParquetOutputFormat

import org.apache.spark.sql.execution.datasources.parquet.ParquetTest
import org.apache.spark.sql.hive.orc.OrcFileOperator
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf

class CompressionCodecSuite extends TestHiveSingleton with ParquetTest {
  import spark.implicits._

  private val maxRecordNum = 100000

  private def getConvertMetastoreConfName(format: String): String = format match {
    case "parquet" => HiveUtils.CONVERT_METASTORE_PARQUET.key
    case "orc" => HiveUtils.CONVERT_METASTORE_ORC.key
  }

  private def getSparkCompressionConfName(format: String): String = format match {
    case "parquet" => SQLConf.PARQUET_COMPRESSION.key
    case "orc" => SQLConf.ORC_COMPRESSION.key
  }

  private def getHiveCompressPropName(format: String): String = {
    format.toLowerCase match {
      case "parquet" => ParquetOutputFormat.COMPRESSION
      case "orc" => COMPRESS.getAttribute
    }
  }

  private def getTableCompressionCodec(path: String, format: String): String = {
    val hadoopConf = spark.sessionState.newHadoopConf()
    val codecs = format match {
      case "parquet" => for {
        footer <- readAllFootersWithoutSummaryFiles(new Path(path), hadoopConf)
        block <- footer.getParquetMetadata.getBlocks.asScala
        column <- block.getColumns.asScala
      } yield column.getCodec.name()
      case "orc" => new File(path).listFiles().filter{ file =>
        file.isFile && !file.getName.endsWith(".crc") && file.getName != "_SUCCESS"
      }.map { orcFile =>
        OrcFileOperator.getFileReader(orcFile.toPath.toString).get.getCompression.toString
      }.toSeq
    }

    assert(codecs.distinct.length == 1)
    codecs.head
  }

  private def writeDataToTable(
    rootDir: File,
    tableName: String,
    isPartitioned: Boolean,
    format: String,
    compressionCodec: Option[String],
    dataSourceName: String = "table_source"): Unit = {
    val tblProperties = compressionCodec match {
      case Some(prop) => s"TBLPROPERTIES('${getHiveCompressPropName(format)}'='$prop')"
      case _ => ""
    }
    val partitionCreate = if (isPartitioned) "PARTITIONED BY (p int)" else ""
    sql(
      s"""
         |CREATE TABLE $tableName(a int)
         |$partitionCreate
         |STORED AS $format
         |LOCATION '${rootDir.toURI.toString.stripSuffix("/")}/$tableName'
         |$tblProperties
       """.stripMargin)

    val partitionInsert = if (isPartitioned) s"partition (p=10000)" else ""
    sql(
      s"""
         |INSERT OVERWRITE TABLE $tableName
         |$partitionInsert
         |SELECT * FROM $dataSourceName
       """.stripMargin)
  }

  private def getTableSize(path: String): Long = {
    val dir = new File(path)
    val files = dir.listFiles().filter(_.getName.startsWith("part-"))
    files.map(_.length()).sum
  }

  private def getDataSizeByFormat(format: String, compressionCodec: Option[String], isPartitioned: Boolean): Long = {
    var totalSize = 0L
    val tableName = s"tbl_$format${compressionCodec.mkString}"
    withTempView("datasource_table") {
      (0 until maxRecordNum).toDF("a").createOrReplaceTempView("datasource_table")
      withSQLConf(getSparkCompressionConfName(format) -> compressionCodec.getOrElse("uncompressed")) {
        withTempDir { tmpDir =>
          withTable(tableName) {
            writeDataToTable(tmpDir, tableName, isPartitioned, format, compressionCodec, "datasource_table")
            val partition = if (isPartitioned) "p=10000" else ""
            val path =s"${tmpDir.getPath.stripSuffix("/")}/${tableName}/$partition"
            totalSize = getTableSize(path)
          }
        }
      }
    }
    assert(totalSize > 0L)
    totalSize
  }

  private def checkCompressionCodecForTable(
    format: String,
    isPartitioned: Boolean,
    compressionCodec: Option[String])
    (assertion: (String, Long) => Unit): Unit = {
    val tableName = s"tbl_$format${isPartitioned}"
    withTempDir { tmpDir =>
      withTable(tableName) {
        writeDataToTable(tmpDir, tableName, isPartitioned, format, compressionCodec)
        val partition = if (isPartitioned) "p=10000" else ""
        val path = s"${tmpDir.getPath.stripSuffix("/")}/${tableName}/$partition"
        val relCompressionCodec = getTableCompressionCodec(path, format)
        val tableSize = getTableSize(path)
        assertion(relCompressionCodec, tableSize)
      }
    }
  }

  private def checkTableCompressionCodecForCodecs(
    format: String,
    isPartitioned: Boolean,
    convertMetastore: Boolean,
    compressionCodecs: List[String],
    tableCompressionCodecs: List[String])
    (assertionCompressionCodec: (Option[String], String, String, Long) => Unit): Unit = {
    withSQLConf(getConvertMetastoreConfName(format) -> convertMetastore.toString) {
      tableCompressionCodecs.foreach { tableCompression =>
        compressionCodecs.foreach { sessionCompressionCodec =>
          withSQLConf(getSparkCompressionConfName(format) -> sessionCompressionCodec) {
            // 'tableCompression = null' means no table-level compression
            val compression = Option(tableCompression)
            checkCompressionCodecForTable(format, isPartitioned, compression) {
              case (realCompressionCodec, tableSize) => assertionCompressionCodec(compression,
                sessionCompressionCodec, realCompressionCodec, tableSize)
            }
          }
        }
      }
    }
  }

  // To check if the compressionCodec takes effect, we check the data size with uncompressed size.
  // and because partitioned table's schema may different with non-partitioned table's schema when
  // convertMetastore is true, e.g. parquet, we save them independently.
  private val partitionedParquetTableUncompressedSize = getDataSizeByFormat("parquet", None, true)
  private val nonpartitionedParquetTableUncompressedSize = getDataSizeByFormat("parquet", None, false)

  // Orc data seems be consistent within partitioned table and non-partitioned table,
  // but we just make the code look the same.
  private val partitionedOrcTableUncompressedSize = getDataSizeByFormat("orc", None, true)
  private val nonpartitionedOrcTableUncompressedSize = getDataSizeByFormat("orc", None, false)

  // When the amount of data is small, compressed data size may be smaller than uncompressed one,
  // so we just check the difference when compressionCodec is not NONE or UNCOMPRESSED.
  // When convertMetastore is false, the uncompressed table size should be same as
  // `partitionedParquetTableUncompressedSize`, regardless of whether there is a partition.
  private def checkTableSize(format: String, compressionCodec: String,
    ispartitioned: Boolean, convertMetastore: Boolean, tableSize: Long): Boolean = {
    format match {
      case "parquet" => val uncompressedSize =
          if(!convertMetastore || ispartitioned) partitionedParquetTableUncompressedSize
          else nonpartitionedParquetTableUncompressedSize

        if(compressionCodec == "UNCOMPRESSED") tableSize == uncompressedSize
        else tableSize != uncompressedSize
      case "orc" => val uncompressedSize =
        if(!convertMetastore || ispartitioned) partitionedOrcTableUncompressedSize
        else nonpartitionedOrcTableUncompressedSize

        if(compressionCodec == "NONE") tableSize == uncompressedSize
        else tableSize != uncompressedSize
      case _ => false
    }
  }

  private def testCompressionCodec(testCondition: String)(f: => Unit): Unit = {
    test("[SPARK-21786] - Check the priority between table-level compression and " +
      s"session-level compression $testCondition") {
      withTempView("table_source") {
        (0 until maxRecordNum).toDF("a").createOrReplaceTempView("table_source")
        f
      }
    }
  }

  testCompressionCodec("when table-level and session-level compression are both configured and " +
    "convertMetastore is false") {
    def checkForTableWithCompressProp(format: String, compressCodecs: List[String]): Unit = {
      // For tables with table-level compression property, when
      // 'spark.sql.hive.convertMetastore[Parquet|Orc]' was set to 'false', partitioned tables
      // and non-partitioned tables will always take the table-level compression
      // configuration first and ignore session compression configuration.
      // Check for partitioned table, when convertMetastore is false
      checkTableCompressionCodecForCodecs(
        format = format,
        isPartitioned = true,
        convertMetastore = false,
        compressionCodecs = compressCodecs,
        tableCompressionCodecs = compressCodecs) {
        case (tableCompressionCodec, sessionCompressionCodec, realCompressionCodec, tableSize) =>
          // Expect table-level take effect
          assert(tableCompressionCodec.get == realCompressionCodec)
          assert(checkTableSize(format, tableCompressionCodec.get, true, false, tableSize))
      }

      // Check for non-partitioned table, when convertMetastoreParquet is false
      checkTableCompressionCodecForCodecs(
        format = format,
        isPartitioned = false,
        convertMetastore = false,
        compressionCodecs = compressCodecs,
        tableCompressionCodecs = compressCodecs) {
        case (tableCompressionCodec, sessionCompressionCodec, realCompressionCodec, tableSize) =>
          // Expect table-level take effect
          assert(tableCompressionCodec.get == realCompressionCodec)
          assert(checkTableSize(format, tableCompressionCodec.get, false, false, tableSize))
      }
    }

    checkForTableWithCompressProp("parquet", List("UNCOMPRESSED", "SNAPPY", "GZIP"))
    checkForTableWithCompressProp("orc", List("NONE", "SNAPPY", "ZLIB"))
  }

  testCompressionCodec("when there's no table-level compression and convertMetastore is false") {
    def checkForTableWithoutCompressProp(format: String, compressCodecs: List[String]): Unit = {
      // For tables without table-level compression property, session-level compression
      // configuration will take effect.
      // Check for partitioned table, when convertMetastore is false
      checkTableCompressionCodecForCodecs(
        format = format,
        isPartitioned = true,
        convertMetastore = false,
        compressionCodecs = compressCodecs,
        tableCompressionCodecs = List(null)) {
        case (tableCompressionCodec, sessionCompressionCodec, realCompressionCodec, tableSize) =>
          // Expect session-level take effect
          assert(sessionCompressionCodec == realCompressionCodec)
          assert(checkTableSize(format, sessionCompressionCodec, true, false, tableSize))
      }

      // Check for non-partitioned table, when convertMetastore is false
      checkTableCompressionCodecForCodecs(
        format = format,
        isPartitioned = false,
        convertMetastore = false,
        compressionCodecs = compressCodecs,
        tableCompressionCodecs = List(null)) {
        case (tableCompressionCodec, sessionCompressionCodec, realCompressionCodec, tableSize) =>
          // Expect session-level take effect
          assert(sessionCompressionCodec == realCompressionCodec)
          assert(checkTableSize(format, sessionCompressionCodec, false, false, tableSize))
      }
    }

    checkForTableWithoutCompressProp("parquet", List("UNCOMPRESSED", "SNAPPY", "GZIP"))
    checkForTableWithoutCompressProp("orc", List("NONE", "SNAPPY", "ZLIB"))
  }

  testCompressionCodec("when table-level and session-level compression are both configured and " +
    "convertMetastore is true") {
    def checkForTableWithCompressProp(format: String, compressCodecs: List[String]): Unit = {
      // For tables with table-level compression property, when
      // 'spark.sql.hive.convertMetastore[Parquet|Orc]' was set to 'true', partitioned tables
      // will always take the table-level compression configuration first, but non-partitioned
      // tables will take the session-level compression configuration.
      // Check for partitioned table, when convertMetastore is true
      checkTableCompressionCodecForCodecs(
        format = format,
        isPartitioned = true,
        convertMetastore = true,
        compressionCodecs = compressCodecs,
        tableCompressionCodecs = compressCodecs) {
        case (tableCompressionCodec, sessionCompressionCodec, realCompressionCodec, tableSize) =>
          // Expect table-level take effect
          assert(tableCompressionCodec.get == realCompressionCodec)
          assert(checkTableSize(format, tableCompressionCodec.get, true, true, tableSize))
      }

      // Check for non-partitioned table, when convertMetastore is true
      checkTableCompressionCodecForCodecs(
        format = format,
        isPartitioned = false,
        convertMetastore = true,
        compressionCodecs = compressCodecs,
        tableCompressionCodecs = compressCodecs) {
        case (tableCompressionCodec, sessionCompressionCodec, realCompressionCodec, tableSize) =>
          // Expect session-level take effect
          assert(sessionCompressionCodec == realCompressionCodec)
          assert(checkTableSize(format, sessionCompressionCodec, false, true, tableSize))
      }
    }

    checkForTableWithCompressProp("parquet", List("UNCOMPRESSED", "SNAPPY", "GZIP"))
    checkForTableWithCompressProp("orc", List("NONE", "SNAPPY", "ZLIB"))
  }

  testCompressionCodec("when there's no table-level compression and convertMetastore is true") {
    def checkForTableWithoutCompressProp(format: String, compressCodecs: List[String]): Unit = {
      // For tables without table-level compression property, session-level compression
      // configuration will take effect.
      // Check for partitioned table, when convertMetastore is true
      checkTableCompressionCodecForCodecs(
        format = format,
        isPartitioned = true,
        convertMetastore = true,
        compressionCodecs = compressCodecs,
        tableCompressionCodecs = List(null)) {
        case (tableCompressionCodec, sessionCompressionCodec, realCompressionCodec, tableSize) =>
          // Expect session-level take effect
          assert(sessionCompressionCodec == realCompressionCodec)
          assert(checkTableSize(format, sessionCompressionCodec, true, true, tableSize))
      }

      // Check for non-partitioned table, when convertMetastore is true
      checkTableCompressionCodecForCodecs(
        format = format,
        isPartitioned = false,
        convertMetastore = true,
        compressionCodecs = compressCodecs,
        tableCompressionCodecs = List(null)) {
        case (tableCompressionCodec, sessionCompressionCodec, realCompressionCodec, tableSize) =>
          // Expect session-level take effect
          assert(sessionCompressionCodec == realCompressionCodec)
          assert(checkTableSize(format, sessionCompressionCodec, false, true, tableSize))
      }
    }

    checkForTableWithoutCompressProp("parquet", List("UNCOMPRESSED", "SNAPPY", "GZIP"))
    checkForTableWithoutCompressProp("orc", List("NONE", "SNAPPY", "ZLIB"))
  }
}