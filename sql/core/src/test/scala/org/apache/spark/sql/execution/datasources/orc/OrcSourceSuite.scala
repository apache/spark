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

package org.apache.spark.sql.execution.datasources.orc

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8
import java.sql.Timestamp
import java.util.Locale

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.orc.OrcConf.COMPRESS
import org.apache.orc.OrcFile
import org.apache.orc.OrcProto.ColumnEncoding.Kind.{DICTIONARY_V2, DIRECT, DIRECT_V2}
import org.apache.orc.OrcProto.Stream.Kind
import org.apache.orc.impl.RecordReaderImpl
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SPARK_VERSION_SHORT
import org.apache.spark.sql.{Row, SPARK_VERSION_METADATA_KEY}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils

case class OrcData(intField: Int, stringField: String)

abstract class OrcSuite extends OrcTest with BeforeAndAfterAll {
  import testImplicits._

  var orcTableDir: File = null
  var orcTableAsDir: File = null

  protected override def beforeAll(): Unit = {
    super.beforeAll()

    orcTableAsDir = Utils.createTempDir(namePrefix = "orctests")
    orcTableDir = Utils.createTempDir(namePrefix = "orctests")

    sparkContext
      .makeRDD(1 to 10)
      .map(i => OrcData(i, s"part-$i"))
      .toDF()
      .createOrReplaceTempView("orc_temp_table")
  }

  protected def testBloomFilterCreation(bloomFilterKind: Kind) {
    val tableName = "bloomFilter"

    withTempDir { dir =>
      withTable(tableName) {
        val sqlStatement = orcImp match {
          case "native" =>
            s"""
               |CREATE TABLE $tableName (a INT, b STRING)
               |USING ORC
               |OPTIONS (
               |  path '${dir.toURI}',
               |  orc.bloom.filter.columns '*',
               |  orc.bloom.filter.fpp 0.1
               |)
            """.stripMargin
          case "hive" =>
            s"""
               |CREATE TABLE $tableName (a INT, b STRING)
               |STORED AS ORC
               |LOCATION '${dir.toURI}'
               |TBLPROPERTIES (
               |  orc.bloom.filter.columns='*',
               |  orc.bloom.filter.fpp=0.1
               |)
            """.stripMargin
          case impl =>
            throw new UnsupportedOperationException(s"Unknown ORC implementation: $impl")
        }

        sql(sqlStatement)
        sql(s"INSERT INTO $tableName VALUES (1, 'str')")

        val partFiles = dir.listFiles()
          .filter(f => f.isFile && !f.getName.startsWith(".") && !f.getName.startsWith("_"))
        assert(partFiles.length === 1)

        val orcFilePath = new Path(partFiles.head.getAbsolutePath)
        val readerOptions = OrcFile.readerOptions(new Configuration())
        val reader = OrcFile.createReader(orcFilePath, readerOptions)
        var recordReader: RecordReaderImpl = null
        try {
          recordReader = reader.rows.asInstanceOf[RecordReaderImpl]

          // BloomFilter array is created for all types; `struct`, int (`a`), string (`b`)
          val sargColumns = Array(true, true, true)
          val orcIndex = recordReader.readRowIndex(0, null, sargColumns)

          // Check the types and counts of bloom filters
          assert(orcIndex.getBloomFilterKinds.forall(_ === bloomFilterKind))
          assert(orcIndex.getBloomFilterIndex.forall(_.getBloomFilterCount > 0))
        } finally {
          if (recordReader != null) {
            recordReader.close()
          }
        }
      }
    }
  }

  protected def testSelectiveDictionaryEncoding(isSelective: Boolean, isHive23: Boolean = false) {
    val tableName = "orcTable"

    withTempDir { dir =>
      withTable(tableName) {
        val sqlStatement = orcImp match {
          case "native" =>
            s"""
               |CREATE TABLE $tableName (zipcode STRING, uniqColumn STRING, value DOUBLE)
               |USING ORC
               |OPTIONS (
               |  path '${dir.toURI}',
               |  orc.dictionary.key.threshold '1.0',
               |  orc.column.encoding.direct 'uniqColumn'
               |)
            """.stripMargin
          case "hive" =>
            s"""
               |CREATE TABLE $tableName (zipcode STRING, uniqColumn STRING, value DOUBLE)
               |STORED AS ORC
               |LOCATION '${dir.toURI}'
               |TBLPROPERTIES (
               |  orc.dictionary.key.threshold '1.0',
               |  hive.exec.orc.dictionary.key.size.threshold '1.0',
               |  orc.column.encoding.direct 'uniqColumn'
               |)
            """.stripMargin
          case impl =>
            throw new UnsupportedOperationException(s"Unknown ORC implementation: $impl")
        }

        sql(sqlStatement)
        sql(s"INSERT INTO $tableName VALUES ('94086', 'random-uuid-string', 0.0)")

        val partFiles = dir.listFiles()
          .filter(f => f.isFile && !f.getName.startsWith(".") && !f.getName.startsWith("_"))
        assert(partFiles.length === 1)

        val orcFilePath = new Path(partFiles.head.getAbsolutePath)
        val readerOptions = OrcFile.readerOptions(new Configuration())
        val reader = OrcFile.createReader(orcFilePath, readerOptions)
        var recordReader: RecordReaderImpl = null
        try {
          recordReader = reader.rows.asInstanceOf[RecordReaderImpl]

          // Check the kind
          val stripe = recordReader.readStripeFooter(reader.getStripes.get(0))

          // The encodings are divided into direct or dictionary-based categories and
          // further refined as to whether they use RLE v1 or v2. RLE v1 is used by
          // Hive 0.11 and RLE v2 is introduced in Hive 0.12 ORC with more improvements.
          // For more details, see https://orc.apache.org/specification/
          assert(stripe.getColumns(1).getKind === DICTIONARY_V2)
          if (isSelective || isHive23) {
            assert(stripe.getColumns(2).getKind === DIRECT_V2)
          } else {
            assert(stripe.getColumns(2).getKind === DICTIONARY_V2)
          }
          // Floating point types are stored with DIRECT encoding in IEEE 754 floating
          // point bit layout.
          assert(stripe.getColumns(3).getKind === DIRECT)
        } finally {
          if (recordReader != null) {
            recordReader.close()
          }
        }
      }
    }
  }

  test("create temporary orc table") {
    checkAnswer(sql("SELECT COUNT(*) FROM normal_orc_source"), Row(10))

    checkAnswer(
      sql("SELECT * FROM normal_orc_source"),
      (1 to 10).map(i => Row(i, s"part-$i")))

    checkAnswer(
      sql("SELECT * FROM normal_orc_source where intField > 5"),
      (6 to 10).map(i => Row(i, s"part-$i")))

    checkAnswer(
      sql("SELECT COUNT(intField), stringField FROM normal_orc_source GROUP BY stringField"),
      (1 to 10).map(i => Row(1, s"part-$i")))
  }

  test("create temporary orc table as") {
    checkAnswer(sql("SELECT COUNT(*) FROM normal_orc_as_source"), Row(10))

    checkAnswer(
      sql("SELECT * FROM normal_orc_source"),
      (1 to 10).map(i => Row(i, s"part-$i")))

    checkAnswer(
      sql("SELECT * FROM normal_orc_source WHERE intField > 5"),
      (6 to 10).map(i => Row(i, s"part-$i")))

    checkAnswer(
      sql("SELECT COUNT(intField), stringField FROM normal_orc_source GROUP BY stringField"),
      (1 to 10).map(i => Row(1, s"part-$i")))
  }

  test("appending insert") {
    sql("INSERT INTO TABLE normal_orc_source SELECT * FROM orc_temp_table WHERE intField > 5")

    checkAnswer(
      sql("SELECT * FROM normal_orc_source"),
      (1 to 5).map(i => Row(i, s"part-$i")) ++ (6 to 10).flatMap { i =>
        Seq.fill(2)(Row(i, s"part-$i"))
      })
  }

  test("overwrite insert") {
    sql(
      """INSERT OVERWRITE TABLE normal_orc_as_source
        |SELECT * FROM orc_temp_table WHERE intField > 5
      """.stripMargin)

    checkAnswer(
      sql("SELECT * FROM normal_orc_as_source"),
      (6 to 10).map(i => Row(i, s"part-$i")))
  }

  test("write null values") {
    sql("DROP TABLE IF EXISTS orcNullValues")

    val df = sql(
      """
        |SELECT
        |  CAST(null as TINYINT) as c0,
        |  CAST(null as SMALLINT) as c1,
        |  CAST(null as INT) as c2,
        |  CAST(null as BIGINT) as c3,
        |  CAST(null as FLOAT) as c4,
        |  CAST(null as DOUBLE) as c5,
        |  CAST(null as DECIMAL(7,2)) as c6,
        |  CAST(null as TIMESTAMP) as c7,
        |  CAST(null as DATE) as c8,
        |  CAST(null as STRING) as c9,
        |  CAST(null as VARCHAR(10)) as c10
        |FROM orc_temp_table limit 1
      """.stripMargin)

    df.write.format("orc").saveAsTable("orcNullValues")

    checkAnswer(
      sql("SELECT * FROM orcNullValues"),
      Row.fromSeq(Seq.fill(11)(null)))

    sql("DROP TABLE IF EXISTS orcNullValues")
  }

  test("SPARK-18433: Improve DataSource option keys to be more case-insensitive") {
    val conf = spark.sessionState.conf
    val option = new OrcOptions(Map(COMPRESS.getAttribute.toUpperCase(Locale.ROOT) -> "NONE"), conf)
    assert(option.compressionCodec == "NONE")
  }

  test("SPARK-21839: Add SQL config for ORC compression") {
    val conf = spark.sessionState.conf
    // Test if the default of spark.sql.orc.compression.codec is snappy
    assert(new OrcOptions(Map.empty[String, String], conf).compressionCodec == "SNAPPY")

    // OrcOptions's parameters have a higher priority than SQL configuration.
    // `compression` -> `orc.compression` -> `spark.sql.orc.compression.codec`
    withSQLConf(SQLConf.ORC_COMPRESSION.key -> "uncompressed") {
      assert(new OrcOptions(Map.empty[String, String], conf).compressionCodec == "NONE")
      val map1 = Map(COMPRESS.getAttribute -> "zlib")
      val map2 = Map(COMPRESS.getAttribute -> "zlib", "compression" -> "lzo")
      assert(new OrcOptions(map1, conf).compressionCodec == "ZLIB")
      assert(new OrcOptions(map2, conf).compressionCodec == "LZO")
    }

    // Test all the valid options of spark.sql.orc.compression.codec
    Seq("NONE", "UNCOMPRESSED", "SNAPPY", "ZLIB", "LZO").foreach { c =>
      withSQLConf(SQLConf.ORC_COMPRESSION.key -> c) {
        val expected = if (c == "UNCOMPRESSED") "NONE" else c
        assert(new OrcOptions(Map.empty[String, String], conf).compressionCodec == expected)
      }
    }
  }

  test("SPARK-23340 Empty float/double array columns raise EOFException") {
    Seq(Seq(Array.empty[Float]).toDF(), Seq(Array.empty[Double]).toDF()).foreach { df =>
      withTempPath { path =>
        df.write.format("orc").save(path.getCanonicalPath)
        checkAnswer(spark.read.orc(path.getCanonicalPath), df)
      }
    }
  }

  test("SPARK-24322 Fix incorrect workaround for bug in java.sql.Timestamp") {
    withTempPath { path =>
      val ts = Timestamp.valueOf("1900-05-05 12:34:56.000789")
      Seq(ts).toDF.write.orc(path.getCanonicalPath)
      checkAnswer(spark.read.orc(path.getCanonicalPath), Row(ts))
    }
  }

  test("Write Spark version into ORC file metadata") {
    withTempPath { path =>
      spark.range(1).repartition(1).write.orc(path.getCanonicalPath)

      val partFiles = path.listFiles()
        .filter(f => f.isFile && !f.getName.startsWith(".") && !f.getName.startsWith("_"))
      assert(partFiles.length === 1)

      val orcFilePath = new Path(partFiles.head.getAbsolutePath)
      val readerOptions = OrcFile.readerOptions(new Configuration())
      val reader = OrcFile.createReader(orcFilePath, readerOptions)
      val version = UTF_8.decode(reader.getMetadataValue(SPARK_VERSION_METADATA_KEY)).toString
      assert(version === SPARK_VERSION_SHORT)
    }
  }
}

class OrcSourceSuite extends OrcSuite with SharedSQLContext {

  protected override def beforeAll(): Unit = {
    super.beforeAll()

    sql(
      s"""CREATE TABLE normal_orc(
         |  intField INT,
         |  stringField STRING
         |)
         |USING ORC
         |LOCATION '${orcTableAsDir.toURI}'
       """.stripMargin)

    sql(
      s"""INSERT INTO TABLE normal_orc
         |SELECT intField, stringField FROM orc_temp_table
       """.stripMargin)

    spark.sql(
      s"""CREATE TEMPORARY VIEW normal_orc_source
         |USING ORC
         |OPTIONS (
         |  PATH '${new File(orcTableAsDir.getAbsolutePath).toURI}'
         |)
       """.stripMargin)

    spark.sql(
      s"""CREATE TEMPORARY VIEW normal_orc_as_source
         |USING ORC
         |OPTIONS (
         |  PATH '${new File(orcTableAsDir.getAbsolutePath).toURI}'
         |)
       """.stripMargin)
  }

  test("Check BloomFilter creation") {
    testBloomFilterCreation(Kind.BLOOM_FILTER_UTF8) // After ORC-101
  }

  test("Enforce direct encoding column-wise selectively") {
    testSelectiveDictionaryEncoding(isSelective = true)
  }
}
