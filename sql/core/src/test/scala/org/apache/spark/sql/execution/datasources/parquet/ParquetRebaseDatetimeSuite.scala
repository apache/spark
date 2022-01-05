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

import java.nio.file.{Files, Paths, StandardCopyOption}
import java.sql.{Date, Timestamp}

import org.apache.spark.{SPARK_VERSION_SHORT, SparkConf, SparkException, SparkUpgradeException}
import org.apache.spark.sql.{QueryTest, Row, SPARK_LEGACY_DATETIME_METADATA_KEY, SPARK_LEGACY_INT96_METADATA_KEY, SPARK_TIMEZONE_METADATA_KEY}
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.{LegacyBehaviorPolicy, ParquetOutputTimestampType}
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy.{CORRECTED, EXCEPTION, LEGACY}
import org.apache.spark.sql.internal.SQLConf.ParquetOutputTimestampType.{INT96, TIMESTAMP_MICROS, TIMESTAMP_MILLIS}
import org.apache.spark.sql.test.SharedSparkSession

abstract class ParquetRebaseDatetimeSuite
  extends QueryTest
  with ParquetTest
  with SharedSparkSession {

  import testImplicits._

  // It generates input files for the test below:
  // "SPARK-31159, SPARK-37705: compatibility with Spark 2.4/3.2 in reading dates/timestamps"
  ignore("SPARK-31806: generate test files for checking compatibility with Spark 2.4/3.2") {
    val resourceDir = "sql/core/src/test/resources/test-data"
    val version = SPARK_VERSION_SHORT.replaceAll("\\.", "_")
    val N = 8
    def save(
        in: Seq[(String, String)],
        t: String,
        dstFile: String,
        options: Map[String, String] = Map.empty): Unit = {
      withTempDir { dir =>
        in.toDF("dict", "plain")
          .select($"dict".cast(t), $"plain".cast(t))
          .repartition(1)
          .write
          .mode("overwrite")
          .options(options)
          .parquet(dir.getCanonicalPath)
        Files.copy(
          dir.listFiles().filter(_.getName.endsWith(".snappy.parquet")).head.toPath,
          Paths.get(resourceDir, dstFile),
          StandardCopyOption.REPLACE_EXISTING)
      }
    }
    DateTimeTestUtils.withDefaultTimeZone(DateTimeTestUtils.LA) {
      withSQLConf(
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> DateTimeTestUtils.LA.getId,
        SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key -> LEGACY.toString,
        SQLConf.PARQUET_INT96_REBASE_MODE_IN_WRITE.key -> LEGACY.toString) {
        save(
          (1 to N).map(i => ("1001-01-01", s"1001-01-0$i")),
          "date",
          s"before_1582_date_v$version.snappy.parquet")
        withSQLConf(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> "TIMESTAMP_MILLIS") {
          save(
            (1 to N).map(i => ("1001-01-01 01:02:03.123", s"1001-01-0$i 01:02:03.123")),
            "timestamp",
            s"before_1582_timestamp_millis_v$version.snappy.parquet")
        }
        val usTs = (1 to N).map(i => ("1001-01-01 01:02:03.123456", s"1001-01-0$i 01:02:03.123456"))
        withSQLConf(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> "TIMESTAMP_MICROS") {
          save(usTs, "timestamp", s"before_1582_timestamp_micros_v$version.snappy.parquet")
        }
        withSQLConf(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> "INT96") {
          // Comparing to other logical types, Parquet-MR chooses dictionary encoding for the
          // INT96 logical type because it consumes less memory for small column cardinality.
          // Huge parquet files doesn't make sense to place to the resource folder. That's why
          // we explicitly set `parquet.enable.dictionary` and generate two files w/ and w/o
          // dictionary encoding.
          save(
            usTs,
            "timestamp",
            s"before_1582_timestamp_int96_plain_v$version.snappy.parquet",
            Map("parquet.enable.dictionary" -> "false"))
          save(
            usTs,
            "timestamp",
            s"before_1582_timestamp_int96_dict_v$version.snappy.parquet",
            Map("parquet.enable.dictionary" -> "true"))
        }
      }
    }
  }

  private def inReadConfToOptions(
      conf: String,
      mode: LegacyBehaviorPolicy.Value): Map[String, String] = conf match {
    case SQLConf.PARQUET_INT96_REBASE_MODE_IN_READ.key =>
      Map(ParquetOptions.INT96_REBASE_MODE -> mode.toString)
    case _ => Map(ParquetOptions.DATETIME_REBASE_MODE -> mode.toString)
  }

  private def runInMode(
      conf: String,
      modes: Seq[LegacyBehaviorPolicy.Value])(f: Map[String, String] => Unit): Unit = {
    modes.foreach { mode =>
      withSQLConf(conf -> mode.toString) { f(Map.empty) }
    }
    withSQLConf(conf -> EXCEPTION.toString) {
      modes.foreach { mode =>
        f(inReadConfToOptions(conf, mode))
      }
    }
  }

  test("SPARK-31159, SPARK-37705: compatibility with Spark 2.4/3.2 in reading dates/timestamps") {
    val N = 8
    // test reading the existing 2.4 files and new 3.0 files (with rebase on/off) together.
    def checkReadMixedFiles[T](
        fileName: String,
        catalystType: String,
        rowFunc: Int => (String, String),
        toJavaType: String => T,
        checkDefaultLegacyRead: String => Unit,
        tsOutputType: String = "TIMESTAMP_MICROS",
        inWriteConf: String = SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key,
        inReadConf: String = SQLConf.PARQUET_REBASE_MODE_IN_READ.key): Unit = {
      withAllParquetWriters {
        withTempPaths(2) { paths =>
          paths.foreach(_.delete())
        val oldPath = getResourceParquetFilePath("test-data/" + fileName)
        val path3_x = paths(0).getCanonicalPath
        val path3_x_rebase = paths(1).getCanonicalPath
          val df = Seq.tabulate(N)(rowFunc).toDF("dict", "plain")
            .select($"dict".cast(catalystType), $"plain".cast(catalystType))
          withSQLConf(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> tsOutputType) {
          checkDefaultLegacyRead(oldPath)
            withSQLConf(inWriteConf -> CORRECTED.toString) {
            df.write.mode("overwrite").parquet(path3_x)
            }
            withSQLConf(inWriteConf -> LEGACY.toString) {
            df.write.parquet(path3_x_rebase)
            }
          }
          // For Parquet files written by Spark 3.0, we know the writer info and don't need the
          // config to guide the rebase behavior.
          runInMode(inReadConf, Seq(LEGACY)) { options =>
            checkAnswer(
            spark.read.format("parquet").options(options).load(oldPath, path3_x, path3_x_rebase),
              (0 until N).flatMap { i =>
                val (dictS, plainS) = rowFunc(i)
                Seq.tabulate(3) { _ =>
                  Row(toJavaType(dictS), toJavaType(plainS))
                }
              })
          }
        }
      }
    }
    def successInRead(path: String): Unit = spark.read.parquet(path).collect()
    def failInRead(path: String): Unit = {
      val e = intercept[SparkException](spark.read.parquet(path).collect())
      assert(e.getCause.isInstanceOf[SparkUpgradeException])
    }
    Seq(
      // By default we should fail to read ancient datetime values when parquet files don't
      // contain Spark version.
      "2_4_5" -> failInRead _,
      "2_4_6" -> successInRead _,
      "3_2_0" -> successInRead _).foreach { case (version, checkDefaultRead) =>
      withAllParquetReaders {
        checkReadMixedFiles(
          s"before_1582_date_v$version.snappy.parquet",
          "date",
          (i: Int) => ("1001-01-01", s"1001-01-0${i + 1}"),
          java.sql.Date.valueOf,
          checkDefaultRead)
        checkReadMixedFiles(
          s"before_1582_timestamp_micros_v$version.snappy.parquet",
          "timestamp",
          (i: Int) => ("1001-01-01 01:02:03.123456", s"1001-01-0${i + 1} 01:02:03.123456"),
          java.sql.Timestamp.valueOf,
          checkDefaultRead)
        checkReadMixedFiles(
          s"before_1582_timestamp_millis_v$version.snappy.parquet",
          "timestamp",
          (i: Int) => ("1001-01-01 01:02:03.123", s"1001-01-0${i + 1} 01:02:03.123"),
          java.sql.Timestamp.valueOf,
          checkDefaultRead,
          tsOutputType = "TIMESTAMP_MILLIS")
      }
    }
    Seq(
      "2_4_5" -> failInRead _,
      "2_4_6" -> successInRead _).foreach { case (version, checkDefaultRead) =>
      withAllParquetReaders {
        Seq("plain", "dict").foreach { enc =>
          checkReadMixedFiles(
            s"before_1582_timestamp_int96_${enc}_v$version.snappy.parquet",
            "timestamp",
            (i: Int) => ("1001-01-01 01:02:03.123456", s"1001-01-0${i + 1} 01:02:03.123456"),
            java.sql.Timestamp.valueOf,
            checkDefaultRead,
            tsOutputType = "INT96",
            inWriteConf = SQLConf.PARQUET_INT96_REBASE_MODE_IN_WRITE.key,
            inReadConf = SQLConf.PARQUET_INT96_REBASE_MODE_IN_READ.key)
        }
      }
    }
  }

  test("SPARK-31159, SPARK-37705: rebasing timestamps in write") {
    val N = 8
    Seq(false, true).foreach { dictionaryEncoding =>
      withAllParquetWriters {
        Seq(
          (
            "TIMESTAMP_MILLIS",
            "1001-01-01 01:02:03.123",
            "1001-01-07 01:09:05.123",
            SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key,
            SQLConf.PARQUET_REBASE_MODE_IN_READ.key),
          (
            "TIMESTAMP_MICROS",
            "1001-01-01 01:02:03.123456",
            "1001-01-07 01:09:05.123456",
            SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key,
            SQLConf.PARQUET_REBASE_MODE_IN_READ.key),
          (
            "INT96",
            "1001-01-01 01:02:03.123456",
            "1001-01-07 01:09:05.123456",
            SQLConf.PARQUET_INT96_REBASE_MODE_IN_WRITE.key,
            SQLConf.PARQUET_INT96_REBASE_MODE_IN_READ.key
          )
        ).foreach { case (outType, tsStr, nonRebased, inWriteConf, inReadConf) =>
        // Ignore the default JVM time zone and use the session time zone instead of it in rebasing.
        DateTimeTestUtils.withDefaultTimeZone(DateTimeTestUtils.JST) {
          withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> DateTimeTestUtils.LA.getId) {
          withClue(s"output type $outType") {
            withSQLConf(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> outType) {
              withTempPath { dir =>
                val path = dir.getAbsolutePath
                withSQLConf(inWriteConf -> LEGACY.toString) {
                  Seq.tabulate(N)(_ => tsStr).toDF("tsS")
                    .select($"tsS".cast("timestamp").as("ts"))
                    .repartition(1)
                    .write
                    .option("parquet.enable.dictionary", dictionaryEncoding)
                    .parquet(path)
                }

                withAllParquetReaders {
                    // The file metadata indicates if it needs rebase or not, so we can always get
                    // the correct result regardless of the "rebase mode" config.
                  runInMode(inReadConf, Seq(LEGACY, CORRECTED, EXCEPTION)) { options =>
                    checkAnswer(
                        spark.read.options(options).parquet(path).select($"ts".cast("string")),
                        Seq.tabulate(N)(_ => Row(tsStr)))
                  }

                  // Force to not rebase to prove the written datetime values are rebased
                  // and we will get wrong result if we don't rebase while reading.
                  withSQLConf("spark.test.forceNoRebase" -> "true") {
                    checkAnswer(
                        spark.read.parquet(path).select($"ts".cast("string")),
                        Seq.tabulate(N)(_ => Row(nonRebased)))
                    }
                  }
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  test("SPARK-31159: rebasing dates in write") {
    val N = 8
    Seq(false, true).foreach { dictionaryEncoding =>
      withAllParquetWriters {
        withTempPath { dir =>
          val path = dir.getAbsolutePath
          withSQLConf(SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key -> LEGACY.toString) {
            Seq.tabulate(N)(_ => "1001-01-01").toDF("dateS")
              .select($"dateS".cast("date").as("date"))
              .repartition(1)
              .write
              .option("parquet.enable.dictionary", dictionaryEncoding)
              .parquet(path)
          }

          withAllParquetReaders {
            // The file metadata indicates if it needs rebase or not, so we can always get the
            // correct result regardless of the "rebase mode" config.
            runInMode(
              SQLConf.PARQUET_REBASE_MODE_IN_READ.key,
              Seq(LEGACY, CORRECTED, EXCEPTION)) { options =>
              checkAnswer(
                spark.read.options(options).parquet(path),
                Seq.tabulate(N)(_ => Row(Date.valueOf("1001-01-01"))))
            }

            // Force to not rebase to prove the written datetime values are rebased and we will get
            // wrong result if we don't rebase while reading.
            withSQLConf("spark.test.forceNoRebase" -> "true") {
              checkAnswer(
                spark.read.parquet(path),
                Seq.tabulate(N)(_ => Row(Date.valueOf("1001-01-07"))))
            }
          }
        }
      }
    }
  }

  test("SPARK-33163, SPARK-37705: write the metadata keys 'org.apache.spark.legacyDateTime' " +
    "and 'org.apache.spark.timeZone'") {
    def checkMetadataKey(dir: java.io.File, exists: Boolean): Unit = {
      Seq("timestamp '1000-01-01 01:02:03'", "date '1000-01-01'").foreach { dt =>
        withSQLConf(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key ->
          ParquetOutputTimestampType.TIMESTAMP_MICROS.toString) {
          sql(s"SELECT $dt AS dt")
            .repartition(1)
            .write
            .mode("overwrite")
            .parquet(dir.getAbsolutePath)
          val metaData = getMetaData(dir)
          val expected = if (exists) Some("") else None
          assert(metaData.get(SPARK_LEGACY_DATETIME_METADATA_KEY) === expected)
          val expectedTz = if (exists) Some(SQLConf.get.sessionLocalTimeZone) else None
          assert(metaData.get(SPARK_TIMEZONE_METADATA_KEY) === expectedTz)
        }
      }
    }
    withAllParquetWriters {
      withSQLConf(SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key -> LEGACY.toString) {
        withTempPath { dir =>
          checkMetadataKey(dir, exists = true)
        }
      }
      withSQLConf(SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key -> CORRECTED.toString) {
        withTempPath { dir =>
          checkMetadataKey(dir, exists = false)
        }
      }
      withSQLConf(SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key -> EXCEPTION.toString) {
        withTempPath { dir => intercept[SparkException] {
          checkMetadataKey(dir, exists = false)
        }
        }
      }
    }
  }

  test("SPARK-33160, SPARK-37705: write the metadata key 'org.apache.spark.legacyINT96' " +
    "and 'org.apache.spark.timeZone'") {
    def saveTs(dir: java.io.File, ts: String = "1000-01-01 01:02:03"): Unit = {
      Seq(Timestamp.valueOf(ts)).toDF()
        .repartition(1)
        .write
        .parquet(dir.getAbsolutePath)
    }
    withAllParquetWriters {
      withSQLConf(SQLConf.PARQUET_INT96_REBASE_MODE_IN_WRITE.key -> LEGACY.toString) {
        withTempPath { dir =>
          saveTs(dir)
        assert(getMetaData(dir)(SPARK_LEGACY_INT96_METADATA_KEY) === "")
        assert(getMetaData(dir)(SPARK_TIMEZONE_METADATA_KEY) === SQLConf.get.sessionLocalTimeZone)
        }
      }
      withSQLConf(SQLConf.PARQUET_INT96_REBASE_MODE_IN_WRITE.key -> CORRECTED.toString) {
        withTempPath { dir =>
          saveTs(dir)
        assert(getMetaData(dir).get(SPARK_LEGACY_INT96_METADATA_KEY).isEmpty)
        assert(getMetaData(dir).get(SPARK_TIMEZONE_METADATA_KEY).isEmpty)
        }
      }
      withSQLConf(SQLConf.PARQUET_INT96_REBASE_MODE_IN_WRITE.key -> EXCEPTION.toString) {
        withTempPath { dir => intercept[SparkException] {
          saveTs(dir)
        }
        }
      }
      withSQLConf(SQLConf.PARQUET_INT96_REBASE_MODE_IN_WRITE.key -> EXCEPTION.toString) {
        withTempPath { dir =>
          saveTs(dir, "2020-10-22 01:02:03")
        assert(getMetaData(dir).get(SPARK_LEGACY_INT96_METADATA_KEY).isEmpty)
        assert(getMetaData(dir).get(SPARK_TIMEZONE_METADATA_KEY).isEmpty)
        }
      }
    }
  }

  test("SPARK-35427: datetime rebasing in the EXCEPTION mode") {
    def checkTsWrite(): Unit = {
      withTempPath { dir =>
        val df = Seq("1001-01-01 01:02:03.123")
          .toDF("str")
          .select($"str".cast("timestamp").as("dt"))
        val e = intercept[SparkException] {
          df.write.parquet(dir.getCanonicalPath)
        }
        val errMsg = e.getCause.getCause.getCause.asInstanceOf[SparkUpgradeException].getMessage
        assert(errMsg.contains("You may get a different result due to the upgrading"))
      }
    }
    withAllParquetWriters {
      withSQLConf(SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key -> EXCEPTION.toString) {
        Seq(TIMESTAMP_MICROS, TIMESTAMP_MILLIS).foreach { tsType =>
          withSQLConf(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> tsType.toString) {
            checkTsWrite()
          }
        }
        withTempPath { dir =>
          val df = Seq(java.sql.Date.valueOf("1001-01-01")).toDF("dt")
          val e = intercept[SparkException] {
            df.write.parquet(dir.getCanonicalPath)
          }
          val errMsg = e.getCause.getCause.getCause.asInstanceOf[SparkUpgradeException].getMessage
          assert(errMsg.contains("You may get a different result due to the upgrading"))
        }
      }
      withSQLConf(
        SQLConf.PARQUET_INT96_REBASE_MODE_IN_WRITE.key -> EXCEPTION.toString,
        SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> INT96.toString) {
        checkTsWrite()
      }
    }

    def checkRead(fileName: String): Unit = {
      val e = intercept[SparkException] {
        spark.read.parquet(getResourceParquetFilePath("test-data/" + fileName)).collect()
      }
      val errMsg = e.getCause.asInstanceOf[SparkUpgradeException].getMessage
      assert(errMsg.contains("You may get a different result due to the upgrading"))
    }
    withAllParquetWriters {
      withSQLConf(SQLConf.PARQUET_REBASE_MODE_IN_READ.key -> EXCEPTION.toString) {
        Seq(
          "before_1582_date_v2_4_5.snappy.parquet",
          "before_1582_timestamp_micros_v2_4_5.snappy.parquet",
          "before_1582_timestamp_millis_v2_4_5.snappy.parquet").foreach(checkRead)
      }
      withSQLConf(SQLConf.PARQUET_INT96_REBASE_MODE_IN_READ.key -> EXCEPTION.toString) {
        checkRead("before_1582_timestamp_int96_dict_v2_4_5.snappy.parquet")
      }
    }
  }
}

class ParquetRebaseDatetimeV1Suite extends ParquetRebaseDatetimeSuite {
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "parquet")
}

class ParquetRebaseDatetimeV2Suite extends ParquetRebaseDatetimeSuite {
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "")
}
