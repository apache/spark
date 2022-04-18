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

package org.apache.spark.sql.errors

import java.util.Locale

import test.org.apache.spark.sql.connector.JavaSimpleWritableDataSource

import org.apache.spark.{SparkArithmeticException, SparkDateTimeException, SparkException, SparkIllegalStateException, SparkRuntimeException, SparkUnsupportedOperationException, SparkUpgradeException}
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.catalyst.util.BadRecordException
import org.apache.spark.sql.connector.SimpleWritableDataSource
import org.apache.spark.sql.execution.QueryExecutionException
import org.apache.spark.sql.execution.datasources.orc.OrcTest
import org.apache.spark.sql.execution.datasources.parquet.ParquetTest
import org.apache.spark.sql.functions.{lit, lower, struct, sum}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy.EXCEPTION
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DecimalType, StructType, TimestampType}
import org.apache.spark.sql.util.ArrowUtils

class QueryExecutionErrorsSuite extends QueryTest
  with ParquetTest with OrcTest with SharedSparkSession {

  import testImplicits._

  private def getAesInputs(): (DataFrame, DataFrame) = {
    val encryptedText16 = "4Hv0UKCx6nfUeAoPZo1z+w=="
    val encryptedText24 = "NeTYNgA+PCQBN50DA//O2w=="
    val encryptedText32 = "9J3iZbIxnmaG+OIA9Amd+A=="
    val encryptedEmptyText16 = "jmTOhz8XTbskI/zYFFgOFQ=="
    val encryptedEmptyText24 = "9RDK70sHNzqAFRcpfGM5gQ=="
    val encryptedEmptyText32 = "j9IDsCvlYXtcVJUf4FAjQQ=="

    val df1 = Seq("Spark", "").toDF
    val df2 = Seq(
      (encryptedText16, encryptedText24, encryptedText32),
      (encryptedEmptyText16, encryptedEmptyText24, encryptedEmptyText32)
    ).toDF("value16", "value24", "value32")

    (df1, df2)
  }

  test("INVALID_PARAMETER_VALUE: invalid key lengths in AES functions") {
    val (df1, df2) = getAesInputs()
    def checkInvalidKeyLength(df: => DataFrame): Unit = {
      val e = intercept[SparkException] {
        df.collect
      }.getCause.asInstanceOf[SparkRuntimeException]
      assert(e.getErrorClass === "INVALID_PARAMETER_VALUE")
      assert(e.getSqlState === "22023")
      assert(e.getMessage.matches(
        "The value of parameter\\(s\\) 'key' in the `aes_encrypt`/`aes_decrypt` function " +
        "is invalid: expects a binary value with 16, 24 or 32 bytes, but got \\d+ bytes."))
    }

    // Encryption failure - invalid key length
    checkInvalidKeyLength(df1.selectExpr("aes_encrypt(value, '12345678901234567')"))
    checkInvalidKeyLength(df1.selectExpr("aes_encrypt(value, binary('123456789012345'))"))
    checkInvalidKeyLength(df1.selectExpr("aes_encrypt(value, binary(''))"))

    // Decryption failure - invalid key length
    Seq("value16", "value24", "value32").foreach { colName =>
      checkInvalidKeyLength(df2.selectExpr(
        s"aes_decrypt(unbase64($colName), '12345678901234567')"))
      checkInvalidKeyLength(df2.selectExpr(
        s"aes_decrypt(unbase64($colName), binary('123456789012345'))"))
      checkInvalidKeyLength(df2.selectExpr(
        s"aes_decrypt(unbase64($colName), '')"))
      checkInvalidKeyLength(df2.selectExpr(
        s"aes_decrypt(unbase64($colName), binary(''))"))
    }
  }

  test("INVALID_PARAMETER_VALUE: AES decrypt failure - key mismatch") {
    val (_, df2) = getAesInputs()
    Seq(
      ("value16", "1234567812345678"),
      ("value24", "123456781234567812345678"),
      ("value32", "12345678123456781234567812345678")).foreach { case (colName, key) =>
      val e = intercept[SparkException] {
        df2.selectExpr(s"aes_decrypt(unbase64($colName), binary('$key'), 'ECB')").collect
      }.getCause.asInstanceOf[SparkRuntimeException]
      assert(e.getErrorClass === "INVALID_PARAMETER_VALUE")
      assert(e.getSqlState === "22023")
      assert(e.getMessage ===
        "The value of parameter(s) 'expr, key' in the `aes_encrypt`/`aes_decrypt` function " +
        "is invalid: Detail message: " +
        "Given final block not properly padded. " +
        "Such issues can arise if a bad key is used during decryption.")
    }
  }

  test("UNSUPPORTED_FEATURE: unsupported combinations of AES modes and padding") {
    val key16 = "abcdefghijklmnop"
    val key32 = "abcdefghijklmnop12345678ABCDEFGH"
    val (df1, df2) = getAesInputs()
    def checkUnsupportedMode(df: => DataFrame): Unit = {
      val e = intercept[SparkException] {
        df.collect
      }.getCause.asInstanceOf[SparkRuntimeException]
      assert(e.getErrorClass === "UNSUPPORTED_FEATURE")
      assert(e.getSqlState === "0A000")
      assert(e.getMessage.matches("""The feature is not supported: AES-\w+ with the padding \w+""" +
        " by the `aes_encrypt`/`aes_decrypt` function."))
    }

    // Unsupported AES mode and padding in encrypt
    checkUnsupportedMode(df1.selectExpr(s"aes_encrypt(value, '$key16', 'CBC')"))
    checkUnsupportedMode(df1.selectExpr(s"aes_encrypt(value, '$key16', 'ECB', 'NoPadding')"))

    // Unsupported AES mode and padding in decrypt
    checkUnsupportedMode(df2.selectExpr(s"aes_decrypt(value16, '$key16', 'GSM')"))
    checkUnsupportedMode(df2.selectExpr(s"aes_decrypt(value16, '$key16', 'GCM', 'PKCS')"))
    checkUnsupportedMode(df2.selectExpr(s"aes_decrypt(value32, '$key32', 'ECB', 'None')"))
  }

  test("UNSUPPORTED_FEATURE: unsupported types (map and struct) in lit()") {
    def checkUnsupportedTypeInLiteral(v: Any): Unit = {
      val e1 = intercept[SparkRuntimeException] { lit(v) }
      assert(e1.getErrorClass === "UNSUPPORTED_FEATURE")
      assert(e1.getSqlState === "0A000")
      assert(e1.getMessage.matches("""The feature is not supported: literal for '.+' of .+\."""))
    }
    checkUnsupportedTypeInLiteral(Map("key1" -> 1, "key2" -> 2))
    checkUnsupportedTypeInLiteral(("mike", 29, 1.0))

    val e2 = intercept[SparkRuntimeException] {
      trainingSales
        .groupBy($"sales.year")
        .pivot(struct(lower(trainingSales("sales.course")), trainingSales("training")))
        .agg(sum($"sales.earnings"))
        .collect()
    }
    assert(e2.getMessage === "The feature is not supported: pivoting by the value" +
      """ '[dotnet,Dummies]' of the column data type 'struct<col1:string,training:string>'.""")
  }

  test("UNSUPPORTED_FEATURE: unsupported pivot operations") {
    val e1 = intercept[SparkUnsupportedOperationException] {
      trainingSales
        .groupBy($"sales.year")
        .pivot($"sales.course")
        .pivot($"training")
        .agg(sum($"sales.earnings"))
        .collect()
    }
    assert(e1.getErrorClass === "UNSUPPORTED_FEATURE")
    assert(e1.getSqlState === "0A000")
    assert(e1.getMessage === "The feature is not supported: Repeated pivots.")

    val e2 = intercept[SparkUnsupportedOperationException] {
      trainingSales
        .rollup($"sales.year")
        .pivot($"training")
        .agg(sum($"sales.earnings"))
        .collect()
    }
    assert(e2.getErrorClass === "UNSUPPORTED_FEATURE")
    assert(e2.getSqlState === "0A000")
    assert(e2.getMessage === "The feature is not supported: Pivot not after a groupBy.")
  }

  test("INCONSISTENT_BEHAVIOR_CROSS_VERSION: " +
    "compatibility with Spark 2.4/3.2 in reading/writing dates") {

    // Fail to read ancient datetime values.
    withSQLConf(SQLConf.PARQUET_REBASE_MODE_IN_READ.key -> EXCEPTION.toString) {
      val fileName = "before_1582_date_v2_4_5.snappy.parquet"
      val filePath = getResourceParquetFilePath("test-data/" + fileName)
      val e = intercept[SparkException] {
        spark.read.parquet(filePath).collect()
      }.getCause.asInstanceOf[SparkUpgradeException]

      val format = "Parquet"
      val config = SQLConf.PARQUET_REBASE_MODE_IN_READ.key
      val option = "datetimeRebaseMode"
      assert(e.getErrorClass === "INCONSISTENT_BEHAVIOR_CROSS_VERSION")
      assert(e.getMessage ===
        "You may get a different result due to the upgrading to Spark >= 3.0: " +
          s"""
             |reading dates before 1582-10-15 or timestamps before 1900-01-01T00:00:00Z
             |from $format files can be ambiguous, as the files may be written by
             |Spark 2.x or legacy versions of Hive, which uses a legacy hybrid calendar
             |that is different from Spark 3.0+'s Proleptic Gregorian calendar.
             |See more details in SPARK-31404. You can set the SQL config '$config' or
             |the datasource option '$option' to 'LEGACY' to rebase the datetime values
             |w.r.t. the calendar difference during reading. To read the datetime values
             |as it is, set the SQL config '$config' or the datasource option '$option'
             |to 'CORRECTED'.
             |""".stripMargin)
    }

    // Fail to write ancient datetime values.
    withSQLConf(SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key -> EXCEPTION.toString) {
      withTempPath { dir =>
        val df = Seq(java.sql.Date.valueOf("1001-01-01")).toDF("dt")
        val e = intercept[SparkException] {
          df.write.parquet(dir.getCanonicalPath)
        }.getCause.getCause.getCause.asInstanceOf[SparkUpgradeException]

        val format = "Parquet"
        val config = SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key
        assert(e.getErrorClass === "INCONSISTENT_BEHAVIOR_CROSS_VERSION")
        assert(e.getMessage ===
          "You may get a different result due to the upgrading to Spark >= 3.0: " +
            s"""
               |writing dates before 1582-10-15 or timestamps before 1900-01-01T00:00:00Z
               |into $format files can be dangerous, as the files may be read by Spark 2.x
               |or legacy versions of Hive later, which uses a legacy hybrid calendar that
               |is different from Spark 3.0+'s Proleptic Gregorian calendar. See more
               |details in SPARK-31404. You can set $config to 'LEGACY' to rebase the
               |datetime values w.r.t. the calendar difference during writing, to get maximum
               |interoperability. Or set $config to 'CORRECTED' to write the datetime values
               |as it is, if you are 100% sure that the written files will only be read by
               |Spark 3.0+ or other systems that use Proleptic Gregorian calendar.
               |""".stripMargin)
      }
    }
  }

  test("UNSUPPORTED_OPERATION: timeZoneId not specified while converting TimestampType to Arrow") {
    val schema = new StructType().add("value", TimestampType)
    val e = intercept[SparkUnsupportedOperationException] {
      ArrowUtils.toArrowSchema(schema, null)
    }

    assert(e.getErrorClass === "UNSUPPORTED_OPERATION")
    assert(e.getMessage === "The operation is not supported: " +
      "timestamp must supply timeZoneId parameter while converting to ArrowType")
  }

  test("UNSUPPORTED_OPERATION - SPARK-36346: can't read Timestamp as TimestampNTZ") {
    withTempPath { file =>
      sql("select timestamp_ltz'2019-03-21 00:02:03'").write.orc(file.getCanonicalPath)
      withAllNativeOrcReaders {
        val e = intercept[SparkException] {
          spark.read.schema("time timestamp_ntz").orc(file.getCanonicalPath).collect()
        }.getCause.asInstanceOf[SparkUnsupportedOperationException]

        assert(e.getErrorClass === "UNSUPPORTED_OPERATION")
        assert(e.getMessage === "The operation is not supported: " +
          "Unable to convert timestamp of Orc to data type 'timestamp_ntz'")
      }
    }
  }

  test("UNSUPPORTED_OPERATION - SPARK-38504: can't read TimestampNTZ as TimestampLTZ") {
    withTempPath { file =>
      sql("select timestamp_ntz'2019-03-21 00:02:03'").write.orc(file.getCanonicalPath)
      withAllNativeOrcReaders {
        val e = intercept[SparkException] {
          spark.read.schema("time timestamp_ltz").orc(file.getCanonicalPath).collect()
        }.getCause.asInstanceOf[SparkUnsupportedOperationException]

        assert(e.getErrorClass === "UNSUPPORTED_OPERATION")
        assert(e.getMessage === "The operation is not supported: " +
          "Unable to convert timestamp ntz of Orc to data type 'timestamp_ltz'")
      }
    }
  }

  test("DATETIME_OVERFLOW: timestampadd() overflows its input timestamp") {
    val e = intercept[SparkArithmeticException] {
      sql("select timestampadd(YEAR, 1000000, timestamp'2022-03-09 01:02:03')").collect()
    }
    assert(e.getErrorClass === "DATETIME_OVERFLOW")
    assert(e.getSqlState === "22008")
    assert(e.getMessage ===
      "Datetime operation overflow: add 1000000 YEAR to TIMESTAMP '2022-03-09 01:02:03'.")
  }

  test("CANNOT_PARSE_DECIMAL: unparseable decimal") {
    val e1 = intercept[SparkException] {
      withTempPath { path =>

        // original text
        val df1 = Seq(
          "money",
          "\"$92,807.99\""
        ).toDF()

        df1.coalesce(1).write.text(path.getAbsolutePath)

        val schema = new StructType().add("money", DecimalType.DoubleDecimal)
        spark
          .read
          .schema(schema)
          .format("csv")
          .option("header", "true")
          .option("locale", Locale.ROOT.toLanguageTag)
          .option("multiLine", "true")
          .option("inferSchema", "false")
          .option("mode", "FAILFAST")
          .load(path.getAbsolutePath).select($"money").collect()
      }
    }
    assert(e1.getCause.isInstanceOf[QueryExecutionException])

    val e2 = e1.getCause.asInstanceOf[QueryExecutionException]
    assert(e2.getCause.isInstanceOf[SparkException])

    val e3 = e2.getCause.asInstanceOf[SparkException]
    assert(e3.getCause.isInstanceOf[BadRecordException])

    val e4 = e3.getCause.asInstanceOf[BadRecordException]
    assert(e4.getCause.isInstanceOf[SparkIllegalStateException])

    val e5 = e4.getCause.asInstanceOf[SparkIllegalStateException]
    assert(e5.getErrorClass === "CANNOT_PARSE_DECIMAL")
    assert(e5.getSqlState === "42000")
    assert(e5.getMessage === "Cannot parse decimal")
  }

  test("WRITING_JOB_ABORTED: read of input data fails in the middle") {
    Seq(classOf[SimpleWritableDataSource], classOf[JavaSimpleWritableDataSource]).foreach { cls =>
      withTempPath { file =>
        val path = file.getCanonicalPath
        assert(spark.read.format(cls.getName).option("path", path).load().collect().isEmpty)
        // test transaction
        val failingUdf = org.apache.spark.sql.functions.udf {
          var count = 0
          (id: Long) => {
            if (count > 5) {
              throw new RuntimeException("testing error")
            }
            count += 1
            id
          }
        }
        val input = spark.range(15).select(failingUdf($"id").as(Symbol("i")))
          .select($"i", -$"i" as Symbol("j"))
        val e = intercept[SparkException] {
          input.write.format(cls.getName).option("path", path).mode("overwrite").save()
        }
        assert(e.getMessage === "Writing job aborted")
        assert(e.getErrorClass === "WRITING_JOB_ABORTED")
        assert(e.getSqlState === "40000")
        // make sure we don't have partial data.
        assert(spark.read.format(cls.getName).option("path", path).load().collect().isEmpty)
      }
    }
  }

  test("CAST_CAUSES_OVERFLOW: from timestamp to int") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val e = intercept[SparkArithmeticException] {
        sql("select CAST(TIMESTAMP '9999-12-31T12:13:14.56789Z' AS INT)").collect()
      }
      assert(e.getErrorClass === "CAST_CAUSES_OVERFLOW")
      assert(e.getSqlState === "22005")
      assert(e.getMessage === "Casting 253402258394567890L to int causes overflow. " +
        "To return NULL instead, use 'try_cast'. " +
        "If necessary set spark.sql.ansi.enabled to false to bypass this error.")
    }
  }

  test("DIVIDE_BY_ZERO: can't divide an integer by zero") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val e = intercept[SparkArithmeticException] {
        sql("select 6/0").collect()
      }
      assert(e.getErrorClass === "DIVIDE_BY_ZERO")
      assert(e.getSqlState === "22012")
      assert(e.getMessage ===
        "divide by zero. To return NULL instead, use 'try_divide'. If necessary set " +
          "spark.sql.ansi.enabled to false (except for ANSI interval type) to bypass this error." +
          """
            |== SQL(line 1, position 7) ==
            |select 6/0
            |       ^^^
            |""".stripMargin)
    }
  }

  test("INVALID_FRACTION_OF_SECOND: in the function make_timestamp") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val e = intercept[SparkDateTimeException] {
        sql("select make_timestamp(2012, 11, 30, 9, 19, 60.66666666)").collect()
      }
      assert(e.getErrorClass === "INVALID_FRACTION_OF_SECOND")
      assert(e.getSqlState === "22023")
      assert(e.getMessage === "The fraction of sec must be zero. Valid range is [0, 60]. " +
        "If necessary set spark.sql.ansi.enabled to false to bypass this error. ")
    }
  }
}
