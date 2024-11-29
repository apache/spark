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
import java.sql.{Date, Timestamp}
import java.time.{Duration, Period}
import java.util.Locale

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.logging.log4j.Level
import org.apache.orc.OrcConf.COMPRESS
import org.apache.orc.OrcFile
import org.apache.orc.OrcProto.ColumnEncoding.Kind.{DICTIONARY_V2, DIRECT, DIRECT_V2}
import org.apache.orc.OrcProto.Stream.Kind
import org.apache.orc.impl.RecordReaderImpl
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.{SPARK_VERSION_SHORT, SparkConf, SparkException}
import org.apache.spark.sql.{Row, SPARK_VERSION_METADATA_KEY}
import org.apache.spark.sql.execution.datasources.{CommonFileDataSourceSuite, SchemaMergeUtils}
import org.apache.spark.sql.execution.datasources.orc.OrcCompressionCodec._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtilsBase}
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

case class OrcData(intField: Int, stringField: String)

abstract class OrcSuite
  extends OrcTest with BeforeAndAfterAll with CommonFileDataSourceSuite with SQLTestUtilsBase {
  import testImplicits._

  override protected def dataSourceFormat = "orc"

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

  protected def testBloomFilterCreation(bloomFilterKind: Kind): Unit = {
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

  protected def testSelectiveDictionaryEncoding(isSelective: Boolean, isHiveOrc: Boolean): Unit = {
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
          if (isSelective || isHiveOrc) {
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

  protected def testMergeSchemasInParallel(
      ignoreCorruptFiles: Boolean,
      schemaReader: (Seq[FileStatus], Configuration, Boolean) => Seq[StructType]): Unit = {
    withSQLConf(
      SQLConf.IGNORE_CORRUPT_FILES.key -> ignoreCorruptFiles.toString,
      SQLConf.ORC_IMPLEMENTATION.key -> orcImp) {
      withTempDir { dir =>
        val fs = FileSystem.get(spark.sessionState.newHadoopConf())
        val basePath = dir.getCanonicalPath

        val path1 = new Path(basePath, "first")
        val path2 = new Path(basePath, "second")
        val path3 = new Path(basePath, "third")

        spark.range(1).toDF("a").coalesce(1).write.orc(path1.toString)
        spark.range(1, 2).toDF("b").coalesce(1).write.orc(path2.toString)
        spark.range(2, 3).toDF("a").coalesce(1).write.json(path3.toString)

        val fileStatuses =
          Seq(fs.listStatus(path1), fs.listStatus(path2), fs.listStatus(path3)).flatten

        val schema = SchemaMergeUtils.mergeSchemasInParallel(
          spark, Map.empty, fileStatuses, schemaReader)

        assert(schema.isDefined)
        assert(schema.get == StructType(Seq(
          StructField("a", LongType, true),
          StructField("b", LongType, true))))
      }
    }
  }

  protected def testMergeSchemasInParallel(
      schemaReader: (Seq[FileStatus], Configuration, Boolean) => Seq[StructType]): Unit = {
    testMergeSchemasInParallel(true, schemaReader)
    checkErrorMatchPVals(
      exception = intercept[SparkException] {
        testMergeSchemasInParallel(false, schemaReader)
      }.getCause.getCause.asInstanceOf[SparkException],
      condition = "FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER",
      parameters = Map("path" -> "file:.*")
    )
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
    val option =
      new OrcOptions(Map(COMPRESS.getAttribute.toUpperCase(Locale.ROOT) -> NONE.name()), conf)
    assert(option.compressionCodec == OrcCompressionCodec.NONE.name())
  }

  test("SPARK-21839: Add SQL config for ORC compression") {
    val conf = spark.sessionState.conf
    // Test if the default of spark.sql.orc.compression.codec is used.
    assert(new OrcOptions(Map.empty[String, String], conf).compressionCodec ==
        SQLConf.ORC_COMPRESSION.defaultValueString.toUpperCase(Locale.ROOT))

    // OrcOptions's parameters have a higher priority than SQL configuration.
    // `compression` -> `orc.compression` -> `spark.sql.orc.compression.codec`
    withSQLConf(SQLConf.ORC_COMPRESSION.key -> UNCOMPRESSED.lowerCaseName()) {
      assert(new OrcOptions(Map.empty[String, String], conf).compressionCodec == NONE.name())
      val zlibCodec = ZLIB.lowerCaseName()
      val map1 = Map(COMPRESS.getAttribute -> zlibCodec)
      val map2 = Map(COMPRESS.getAttribute -> zlibCodec, "compression" -> LZO.lowerCaseName())
      assert(new OrcOptions(map1, conf).compressionCodec == ZLIB.name())
      assert(new OrcOptions(map2, conf).compressionCodec == LZO.name())
    }

    // Test all the valid options of spark.sql.orc.compression.codec
    OrcCompressionCodec.values().map(_.name()).foreach { c =>
      withSQLConf(SQLConf.ORC_COMPRESSION.key -> c) {
        val expected = OrcCompressionCodec.valueOf(c).getCompressionKind.name()
        assert(new OrcOptions(Map.empty[String, String], conf).compressionCodec == expected)
      }
    }
  }

  // SPARK-28885 String value is not allowed to be stored as numeric type with
  // ANSI store assignment policy.
  ignore("SPARK-23340 Empty float/double array columns raise EOFException") {
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
      Seq(ts).toDF().write.orc(path.getCanonicalPath)
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
      Utils.tryWithResource(OrcFile.createReader(orcFilePath, readerOptions)) { reader =>
        val version = UTF_8.decode(reader.getMetadataValue(SPARK_VERSION_METADATA_KEY)).toString
        assert(version === SPARK_VERSION_SHORT)
      }
    }
  }

  test("SPARK-11412 test orc merge schema option") {
    val conf = spark.sessionState.conf
    // Test if the default of spark.sql.orc.mergeSchema is false
    assert(new OrcOptions(Map.empty[String, String], conf).mergeSchema == false)

    // OrcOptions's parameters have a higher priority than SQL configuration.
    // `mergeSchema` -> `spark.sql.orc.mergeSchema`
    withSQLConf(SQLConf.ORC_SCHEMA_MERGING_ENABLED.key -> "true") {
      val map1 = Map(OrcOptions.MERGE_SCHEMA -> "true")
      val map2 = Map(OrcOptions.MERGE_SCHEMA -> "false")
      assert(new OrcOptions(map1, conf).mergeSchema == true)
      assert(new OrcOptions(map2, conf).mergeSchema == false)
    }

    withSQLConf(SQLConf.ORC_SCHEMA_MERGING_ENABLED.key -> "false") {
      val map1 = Map(OrcOptions.MERGE_SCHEMA -> "true")
      val map2 = Map(OrcOptions.MERGE_SCHEMA -> "false")
      assert(new OrcOptions(map1, conf).mergeSchema == true)
      assert(new OrcOptions(map2, conf).mergeSchema == false)
    }
  }

  test("SPARK-11412 test enabling/disabling schema merging") {
    def testSchemaMerging(expectedColumnNumber: Int): Unit = {
      withTempDir { dir =>
        val basePath = dir.getCanonicalPath
        spark.range(0, 10).toDF("a").write.orc(new Path(basePath, "foo=1").toString)
        spark.range(0, 10).toDF("b").write.orc(new Path(basePath, "foo=2").toString)
        assert(spark.read.orc(basePath).columns.length === expectedColumnNumber)

        // OrcOptions.MERGE_SCHEMA has higher priority
        assert(spark.read.option(OrcOptions.MERGE_SCHEMA, true)
          .orc(basePath).columns.length === 3)
        assert(spark.read.option(OrcOptions.MERGE_SCHEMA, false)
          .orc(basePath).columns.length === 2)
      }
    }

    withSQLConf(SQLConf.ORC_SCHEMA_MERGING_ENABLED.key -> "true") {
      testSchemaMerging(3)
    }

    withSQLConf(SQLConf.ORC_SCHEMA_MERGING_ENABLED.key -> "false") {
      testSchemaMerging(2)
    }
  }

  test("SPARK-11412 test enabling/disabling schema merging with data type conflicts") {
    withTempDir { dir =>
      val basePath = dir.getCanonicalPath
      spark.range(0, 10).toDF("a").write.orc(new Path(basePath, "foo=1").toString)
      spark.range(0, 10).map(s => s"value_$s").toDF("a")
        .write.orc(new Path(basePath, "foo=2").toString)

      // with schema merging, there should throw exception
      withSQLConf(SQLConf.ORC_SCHEMA_MERGING_ENABLED.key -> "true") {
        val ex = intercept[SparkException] {
          spark.read.orc(basePath).columns.length
        }
        assert(ex.getCondition == "CANNOT_MERGE_SCHEMAS")
        assert(ex.getCause.asInstanceOf[SparkException].getCondition ===
          "CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE")
      }

      // it is ok if no schema merging
      withSQLConf(SQLConf.ORC_SCHEMA_MERGING_ENABLED.key -> "false") {
        assert(spark.read.orc(basePath).columns.length === 2)
      }
    }
  }

  test("SPARK-11412 test schema merging with corrupt files") {
    withSQLConf(SQLConf.ORC_SCHEMA_MERGING_ENABLED.key -> "true") {
      withTempDir { dir =>
        val basePath = dir.getCanonicalPath
        spark.range(0, 10).toDF("a").write.orc(new Path(basePath, "foo=1").toString)
        spark.range(0, 10).toDF("b").write.orc(new Path(basePath, "foo=2").toString)
        spark.range(0, 10).toDF("c").write.json(new Path(basePath, "foo=3").toString)

        // ignore corrupt files
        withSQLConf(SQLConf.IGNORE_CORRUPT_FILES.key -> "true") {
          assert(spark.read.orc(basePath).columns.length === 3)
        }

        // don't ignore corrupt files
        withSQLConf(SQLConf.IGNORE_CORRUPT_FILES.key -> "false") {
          checkErrorMatchPVals(
            exception = intercept[SparkException] {
              spark.read.orc(basePath).columns.length
            }.getCause.getCause.asInstanceOf[SparkException],
            condition = "FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER",
            parameters = Map("path" -> "file:.*")
          )
        }
      }
    }
  }

  test("SPARK-31238: compatibility with Spark 2.4 in reading dates") {
    withAllNativeOrcReaders {
      checkAnswer(
        readResourceOrcFile("test-data/before_1582_date_v2_4.snappy.orc"),
        Row(java.sql.Date.valueOf("1200-01-01")))
    }
  }

  test("SPARK-31238, SPARK-31423: rebasing dates in write") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      Seq("1001-01-01", "1582-10-10").toDF("dateS")
        .select($"dateS".cast("date").as("date"))
        .write
        .orc(path)

      withAllNativeOrcReaders {
        checkAnswer(
          spark.read.orc(path),
          Seq(Row(Date.valueOf("1001-01-01")), Row(Date.valueOf("1582-10-15"))))
      }
    }
  }

  test("SPARK-31284: compatibility with Spark 2.4 in reading timestamps") {
    withAllNativeOrcReaders {
      checkAnswer(
        readResourceOrcFile("test-data/before_1582_ts_v2_4.snappy.orc"),
        Row(java.sql.Timestamp.valueOf("1001-01-01 01:02:03.123456")))
    }
  }

  test("SPARK-31284, SPARK-31423: rebasing timestamps in write") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      Seq("1001-01-01 01:02:03.123456", "1582-10-10 11:12:13.654321").toDF("tsS")
        .select($"tsS".cast("timestamp").as("ts"))
        .write
        .orc(path)

      withAllNativeOrcReaders {
        checkAnswer(
          spark.read.orc(path),
          Seq(
            Row(java.sql.Timestamp.valueOf("1001-01-01 01:02:03.123456")),
            Row(java.sql.Timestamp.valueOf("1582-10-15 11:12:13.654321"))))
      }
    }
  }

  test("SPARK-35612: Support LZ4 compression in ORC data source") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.range(3).write.option("compression", LZ4.lowerCaseName()).orc(path)
      checkAnswer(spark.read.orc(path), Seq(Row(0), Row(1), Row(2)))
      val files = OrcUtils.listOrcFiles(path, spark.sessionState.newHadoopConf())
      assert(files.nonEmpty && files.forall(_.getName.contains(LZ4.lowerCaseName())))
    }
  }

  test("SPARK-33978: Write and read a file with ZSTD compression") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.range(3).write.option("compression", ZSTD.lowerCaseName()).orc(path)
      checkAnswer(spark.read.orc(path), Seq(Row(0), Row(1), Row(2)))
      val files = OrcUtils.listOrcFiles(path, spark.sessionState.newHadoopConf())
      assert(files.nonEmpty && files.forall(_.getName.contains(ZSTD.lowerCaseName())))
    }
  }

  test("SPARK-37841: Skip updating stats for files not been created") {
    withTempPath { path =>
      val logAppender = new LogAppender()

      withLogAppender(logAppender, level = Option(Level.WARN)) {
        spark.range(0, 3, 1, 4).write.orc(path.getCanonicalPath)
      }
      val events = logAppender.loggingEvents
      assert {
        !events.exists { _.getMessage.getFormattedMessage
          .contains("This could be due to the output format not writing empty files")
        }
      }
    }
  }

  test("SPARK-37841: ORC sources write empty file with schema") {
    withTempPath { path =>
      val canonicalPath = path.getCanonicalPath
      // creates an empty data set
      spark.range(1, 1, 1, 1).write.orc(canonicalPath)
      assert(spark.read.orc(canonicalPath).isEmpty,
        "ORC sources shall write an empty file contains meta if necessary")
    }
  }

  test("SPARK-40667: validate Orc Options") {
    assert(OrcOptions.getAllOptions.size == 3)
    // Please add validation on any new Orc options here
    assert(OrcOptions.isValidOption("mergeSchema"))
    assert(OrcOptions.isValidOption("orc.compress"))
    assert(OrcOptions.isValidOption("compression"))
  }
}

abstract class OrcSourceSuite extends OrcSuite with SharedSparkSession {

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
    testSelectiveDictionaryEncoding(isSelective = true, isHiveOrc = false)
  }

  test("SPARK-11412 read and merge orc schemas in parallel") {
    testMergeSchemasInParallel(OrcUtils.readOrcSchemasInParallel)
  }

  test("SPARK-31580: Read a file written before ORC-569") {
    // Test ORC file came from ORC-621
    val df = readResourceOrcFile("test-data/TestStringDictionary.testRowIndex.orc")
    assert(df.where("str < 'row 001000'").count() === 1000)
  }

  test("SPARK-34897: Support reconcile schemas based on index after nested column pruning") {
    withTable("t1") {
      spark.sql(
        """
          |CREATE TABLE t1 (
          |  _col0 INT,
          |  _col1 STRING,
          |  _col2 STRUCT<c1: STRING, c2: STRING, c3: STRING, c4: BIGINT>)
          |USING ORC
          |""".stripMargin)

      spark.sql("INSERT INTO t1 values(1, '2', struct('a', 'b', 'c', 10L))")
      checkAnswer(spark.sql("SELECT _col0, _col2.c1 FROM t1"), Seq(Row(1, "a")))
    }
  }

  test("SPARK-36663: OrcUtils.toCatalystSchema should correctly handle " +
    "a column name which consists of only numbers") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql("SELECT 'a' as `1`, 'b' as `2`, 'c' as `3`").write.orc(path)
      val df = spark.read.orc(path)
      checkAnswer(df, Row("a", "b", "c"))
      assert(df.schema.toArray ===
        Array(
          StructField("1", StringType),
          StructField("2", StringType),
          StructField("3", StringType)))
    }

    // test for struct in struct
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql(
        "SELECT 'a' as `10`, named_struct('20', 'b', '30', named_struct('40', 'c')) as `50`")
        .write.orc(path)
      val df = spark.read.orc(path)
      checkAnswer(df, Row("a", Row("b", Row("c"))))
      assert(df.schema.toArray === Array(
        StructField("10", StringType),
        StructField("50",
          StructType(
            StructField("20", StringType) ::
            StructField("30",
              StructType(
                StructField("40", StringType) :: Nil)) :: Nil))))
    }

    // test for struct in array
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql("SELECT array(array(named_struct('123', 'a'), named_struct('123', 'b'))) as `789`")
        .write.orc(path)
      val df = spark.read.orc(path)
      checkAnswer(df, Row(Seq(Seq(Row("a"), Row("b")))))
      assert(df.schema.toArray === Array(
        StructField("789",
          ArrayType(
            ArrayType(
              StructType(
                StructField("123", StringType) :: Nil))))))
    }

    // test for struct in map
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql(
        """
          |SELECT
          |  map(
          |    named_struct('123', 'a'),
          |    map(
          |      named_struct('456', 'b'),
          |      named_struct('789', 'c'))) as `012`""".stripMargin).write.orc(path)
      val df = spark.read.orc(path)
      checkAnswer(df, Row(Map(Row("a") -> Map(Row("b") -> Row("c")))))
      assert(df.schema.toArray === Array(
        StructField("012",
          MapType(
            StructType(
              StructField("123", StringType) :: Nil),
            MapType(
              StructType(
                StructField("456", StringType) :: Nil),
              StructType(
                StructField("789", StringType) :: Nil))))))
    }

    // test for deeply nested struct with complex types
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      spark.sql(
        """
          |SELECT
          |  named_struct('123',
          |    array(
          |      map(
          |        named_struct('456', 'a'),
          |        named_struct('789', 'b')))) as `1000`,
          |  named_struct('123',
          |    map(
          |      array(named_struct('456', 'a')),
          |      array(named_struct('789', 'b')))) as `2000`,
          |  array(
          |    named_struct('123',
          |      map(
          |        named_struct('456', 'a'),
          |        named_struct('789', 'b')))) as `3000`,
          |  array(
          |    map(
          |      named_struct('123', 'a'),
          |      named_struct('456', 'b'))) as `4000`,
          |  map(
          |    named_struct('123',
          |      array(
          |        named_struct('456', 'a'))),
          |    named_struct('789',
          |      array(
          |        named_struct('012', 'b')))) as `5000`,
          |  map(
          |    array(
          |      named_struct('123', 'a')),
          |    array(
          |      named_struct('456', 'b'))) as `6000`
        """.stripMargin).write.orc(path)
      val df = spark.read.orc(path)
      checkAnswer(df, Row(
        Row(Seq(Map(Row("a") -> Row("b")))),
        Row(Map(Seq(Row("a")) -> Seq(Row("b")))),
        Seq(Row(Map(Row("a") -> Row("b")))),
        Seq(Map(Row("a") -> Row("b"))),
        Map(Row(Seq(Row("a"))) -> Row(Seq(Row("b")))),
        Map(Seq(Row("a")) -> Seq(Row("b")))))
      assert(df.schema.toArray === Array(
        StructField("1000",
          StructType(
            StructField("123",
              ArrayType(
                MapType(
                  StructType(
                    StructField("456", StringType) :: Nil),
                  StructType(
                    StructField("789", StringType) :: Nil)))) :: Nil)),
        StructField("2000",
          StructType(
            StructField("123",
              MapType(
                ArrayType(
                  StructType(
                    StructField("456", StringType) :: Nil)),
                ArrayType(
                  StructType(
                    StructField("789", StringType) :: Nil)))) :: Nil)),
        StructField("3000",
          ArrayType(
            StructType(
              StructField("123",
                MapType(
                  StructType(
                    StructField("456", StringType) :: Nil),
                  StructType(
                    StructField("789", StringType) :: Nil))) :: Nil))),
        StructField("4000",
          ArrayType(
            MapType(
              StructType(
                StructField("123", StringType) :: Nil),
              StructType(
                StructField("456", StringType) :: Nil)))),
        StructField("5000",
          MapType(
            StructType(
              StructField("123",
                ArrayType(
                  StructType(
                    StructField("456", StringType) :: Nil))) :: Nil),
            StructType(
              StructField("789",
                ArrayType(
                  StructType(
                    StructField("012", StringType) :: Nil))) :: Nil))),
        StructField("6000",
          MapType(
            ArrayType(
              StructType(
                StructField("123", StringType) :: Nil)),
            ArrayType(
              StructType(
                StructField("456", StringType) :: Nil))))))
    }
  }

  withAllNativeOrcReaders {
    Seq(true, false).foreach { vecReaderNestedColEnabled =>
      val vecReaderEnabled = SQLConf.get.orcVectorizedReaderEnabled
      test("SPARK-36931: Support reading and writing ANSI intervals (" +
        s"${SQLConf.ORC_VECTORIZED_READER_ENABLED.key}=$vecReaderEnabled, " +
        s"${SQLConf.ORC_VECTORIZED_READER_NESTED_COLUMN_ENABLED.key}=$vecReaderNestedColEnabled)") {

        withSQLConf(
          SQLConf.ORC_VECTORIZED_READER_ENABLED.key ->
            vecReaderEnabled.toString,
          SQLConf.ORC_VECTORIZED_READER_NESTED_COLUMN_ENABLED.key ->
            vecReaderNestedColEnabled.toString) {
          Seq(
            YearMonthIntervalType() -> ((i: Int) => Period.of(i, i, 0)),
            DayTimeIntervalType() -> ((i: Int) => Duration.ofDays(i).plusSeconds(i))
          ).foreach { case (it, f) =>
            val data = (1 to 10).map(i => Row(i, f(i)))
            val schema = StructType(Array(StructField("d", IntegerType, false),
              StructField("i", it, false)))
            withTempPath { file =>
              val df = spark.createDataFrame(sparkContext.parallelize(data), schema)
              df.write.orc(file.getCanonicalPath)
              val df2 = spark.read.orc(file.getCanonicalPath)
              checkAnswer(df2, df.collect().toSeq)
            }
          }

          // Tests for ANSI intervals in complex types.
          withTempPath { file =>
            val df = spark.sql(
              """SELECT
                |  named_struct('interval', interval '1-2' year to month) a,
                |  array(interval '1 2:3' day to minute) b,
                |  map('key', interval '10' year) c,
                |  map(interval '20' second, 'value') d""".stripMargin)
            df.write.orc(file.getCanonicalPath)
            val df2 = spark.read.orc(file.getCanonicalPath)
            checkAnswer(df2, df.collect().toSeq)
          }
        }
      }
    }
  }

  test("SPARK-37812: Reuse result row when deserializing a struct") {
    val queries = Seq(
      // struct in an array
      """SELECT
        |  array(
        |    named_struct(
        |      'a1', 1,
        |      'a2', 2),
        |    named_struct(
        |      'a1', 3,
        |      'a2', 4)
        |  ) as col1
        |""".stripMargin,

      // struct as values in a map
      """SELECT
        |  map(
        |    'ns1',
        |    named_struct(
        |      'a1', 1,
        |      'a2', 2),
        |    'ns2',
        |    named_struct(
        |      'a1', 3,
        |      'a2', 4)
        |  ) as col1
        |""".stripMargin,

      // struct as keys in a map
      """SELECT
        |  map(
        |    named_struct(
        |      'a1', 1,
        |      'a2', 2),
        |    1,
        |    named_struct(
        |      'a1', 3,
        |      'a2', 4),
        |    2
        |  ) as col1
        |""".stripMargin,

      // struct in a struct in an array
      """SELECT
        |  array(
        |    named_struct(
        |      'a', named_struct(
        |        'a1', 1,
        |        'a2', 2),
        |      'b', named_struct(
        |        'b1', 3,
        |        'b2', 4)
        |    ),
        |    named_struct(
        |      'a', named_struct(
        |        'a1', 5,
        |        'a2', 6),
        |      'b', named_struct(
        |        'b1', 7,
        |        'b2', 8)
        |    )
        |  ) as col1
        |""".stripMargin,

      // struct in a struct as values in a map
      """SELECT
        |  map(
        |    'ns1',
        |    named_struct(
        |      'a', named_struct(
        |        'a1', 1,
        |        'a2', 2),
        |      'b', named_struct(
        |        'b1', 3,
        |        'b2', 4)
        |    ),
        |    'ns2',
        |    named_struct(
        |      'a', named_struct(
        |        'a1', 5,
        |        'a2', 6),
        |      'b', named_struct(
        |        'b1', 7,
        |        'b2', 8)
        |    )
        |  ) as col1
        |""".stripMargin,

      // struct in a struct as keys in a map
      """SELECT
        |  map(
        |    named_struct(
        |      'a', named_struct(
        |        'a1', 1,
        |        'a2', 2),
        |      'b', named_struct(
        |        'b1', 3,
        |        'b2', 4)
        |    ),
        |    1,
        |    named_struct(
        |      'a', named_struct(
        |        'a1', 5,
        |        'a2', 6),
        |      'b', named_struct(
        |        'b1', 7,
        |        'b2', 8)
        |    ),
        |    2
        |  ) as col1
        |""".stripMargin,

      // multi-row test
      """SELECT * FROM VALUES
        |  (named_struct(
        |    'a', 1,
        |    'b', 2)),
        |  (named_struct(
        |    'a', 3,
        |    'b', 4)),
        |  (named_struct(
        |    'a', 5,
        |    'b', 6))
        |tbl(c1)
        |""".stripMargin
    )

    queries.foreach { query =>
      withAllNativeOrcReaders {
        Seq(true, false).foreach { vecReaderNestedColEnabled =>
          // SPARK-37812 only applies to the configuration where
          // ORC_VECTORIZED_READER_NESTED_COLUMN_ENABLED is false. However, these
          // are good general correctness tests for the other configurations as well.
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_NESTED_COLUMN_ENABLED.key ->
            vecReaderNestedColEnabled.toString) {
            withTempPath { file =>
              val df = sql(query)
              // use coalesce so we write just 1 file for the multi-row case
              df.coalesce(1).write.orc(file.getCanonicalPath)
              val df2 = spark.read.orc(file.getCanonicalPath)
              checkAnswer(df2, df.collect().toSeq)
            }
          }
        }
      }
    }
  }
}

class OrcSourceV1Suite extends OrcSourceSuite {
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "orc")
}

class OrcSourceV2Suite extends OrcSourceSuite {
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "")
}
