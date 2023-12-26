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

import org.apache.hadoop.fs.Path
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetOutputFormat}

import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.SchemaColumnConvertNotSupportedException
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.{LegacyBehaviorPolicy, SQLConf}
import org.apache.spark.sql.internal.SQLConf.ParquetOutputTimestampType
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class ParquetTypeWideningSuite
    extends QueryTest
    with ParquetTest
    with SharedSparkSession
    with AdaptiveSparkPlanHelper {

  import testImplicits._

  /**
   * Write a Parquet file with the given values stored using type `fromType` and read it back
   * using type `toType` with each Parquet reader. If `expectError` returns true, check that an
   * error is thrown during the read. Otherwise check that the data read matches the data written.
   */
  private def checkAllParquetReaders(
      values: Seq[String],
      fromType: DataType,
      toType: DataType,
      expectError: => Boolean): Unit = {
    val timestampRebaseModes = toType match {
      case _: TimestampNTZType | _: DateType =>
        Seq(LegacyBehaviorPolicy.CORRECTED, LegacyBehaviorPolicy.LEGACY)
      case _ =>
        Seq(LegacyBehaviorPolicy.CORRECTED)
    }
    for {
      dictionaryEnabled <- Seq(true, false)
      timestampRebaseMode <- timestampRebaseModes
    }
    withClue(
      s"with dictionary encoding '$dictionaryEnabled' with timestamp rebase mode " +
        s"'$timestampRebaseMode''") {
      withAllParquetWriters {
        withTempDir { dir =>
          val expected =
            writeParquetFiles(dir, values, fromType, dictionaryEnabled, timestampRebaseMode)
          withAllParquetReaders {
            if (expectError) {
              val exception = intercept[SparkException] {
                readParquetFiles(dir, toType).collect()
              }
              assert(
                exception.getCause.getCause
                  .isInstanceOf[SchemaColumnConvertNotSupportedException] ||
                exception.getCause.getCause
                  .isInstanceOf[org.apache.parquet.io.ParquetDecodingException] ||
                exception.getCause.getMessage.contains(
                  "Unable to create Parquet converter for data type"))
            } else {
              checkAnswer(readParquetFiles(dir, toType), expected.select($"a".cast(toType)))
            }
          }
        }
      }
    }
  }

  /**
   * Reads all parquet files in the given directory using the given type.
   */
  private def readParquetFiles(dir: File, dataType: DataType): DataFrame = {
    spark.read.schema(s"a ${dataType.sql}").parquet(dir.getAbsolutePath)
  }

  /**
   * Writes values to a parquet file in the given directory using the given type and returns a
   * DataFrame corresponding to the data written. If dictionaryEnabled is true, the columns will
   * be dictionary encoded. Each provided value is repeated 10 times to allow dictionary encoding
   * to be used. timestampRebaseMode can be either "CORRECTED" or "LEGACY", see
   * [[SQLConf.PARQUET_REBASE_MODE_IN_WRITE]]
   */
  private def writeParquetFiles(
      dir: File,
      values: Seq[String],
      dataType: DataType,
      dictionaryEnabled: Boolean,
      timestampRebaseMode: LegacyBehaviorPolicy.Value = LegacyBehaviorPolicy.CORRECTED)
    : DataFrame = {
    val repeatedValues = List.fill(if (dictionaryEnabled) 10 else 1)(values).flatten
    val df = repeatedValues.toDF("a").select(col("a").cast(dataType))
    withSQLConf(
      ParquetOutputFormat.ENABLE_DICTIONARY -> dictionaryEnabled.toString,
      SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key -> timestampRebaseMode.toString) {
      df.write.mode("overwrite").parquet(dir.getAbsolutePath)
    }

    // Decimals stored as byte arrays (precision > 18) are not dictionary encoded.
    if (dictionaryEnabled && !DecimalType.isByteArrayDecimalType(dataType)) {
      assertAllParquetFilesDictionaryEncoded(dir)
    }
    df
  }

  /**
   * Asserts that all parquet files in the given directory have all their columns dictionary
   * encoded.
   */
  private def assertAllParquetFilesDictionaryEncoded(dir: File): Unit = {
    dir.listFiles(_.getName.endsWith(".parquet")).foreach { file =>
      val parquetMetadata = ParquetFileReader.readFooter(
        spark.sessionState.newHadoopConf(),
        new Path(dir.toString, file.getName),
        ParquetMetadataConverter.NO_FILTER)
      parquetMetadata.getBlocks.forEach { block =>
        block.getColumns.forEach { col =>
          assert(
            col.hasDictionaryPage,
            "This test covers dictionary encoding but column " +
              s"'${col.getPath.toDotString}' in the test data is not dictionary encoded.")
        }
      }
    }
  }

  for {
    (values: Seq[String], fromType: DataType, toType: DataType) <- Seq(
      (Seq("1", "2", Short.MinValue.toString), ShortType, IntegerType),
      // Int->Short isn't a widening conversion but Parquet stores both as INT32 so it just works.
      (Seq("1", "2", Short.MinValue.toString), IntegerType, ShortType),
      (Seq("1", "2", Int.MinValue.toString), IntegerType, LongType),
      (Seq("1", "2", Short.MinValue.toString), ShortType, DoubleType),
      (Seq("1", "2", Int.MinValue.toString), IntegerType, DoubleType),
      (Seq("1.23", "10.34"), FloatType, DoubleType),
      (Seq("2020-01-01", "2020-01-02", "1312-02-27"), DateType, TimestampNTZType)
    )
  }
    test(s"parquet widening conversion $fromType -> $toType") {
      checkAllParquetReaders(values, fromType, toType, expectError = false)
    }

  for {
    (values: Seq[String], fromType: DataType, toType: DataType) <- Seq(
      (Seq("1", "2", Int.MinValue.toString), LongType, IntegerType),
      (Seq("1.23", "10.34"), DoubleType, FloatType),
      (Seq("1.23", "10.34"), FloatType, LongType),
      (Seq("1", "10"), LongType, DateType),
      (Seq("1", "10"), IntegerType, TimestampType),
      (Seq("1", "10"), IntegerType, TimestampNTZType),
      (Seq("2020-01-01", "2020-01-02", "1312-02-27"), DateType, TimestampType)
    )
  }
    test(s"unsupported parquet conversion $fromType -> $toType") {
      checkAllParquetReaders(values, fromType, toType, expectError = true)
    }

  for {
    (values: Seq[String], fromType: DataType, toType: DataType) <- Seq(
      (Seq("2020-01-01", "2020-01-02", "1312-02-27"), TimestampType, DateType),
      (Seq("2020-01-01", "2020-01-02", "1312-02-27"), TimestampNTZType, DateType))
    outputTimestampType <- ParquetOutputTimestampType.values
  }
  test(s"unsupported parquet timestamp conversion $fromType ($outputTimestampType) -> $toType") {
    withSQLConf(
      SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> outputTimestampType.toString,
      SQLConf.PARQUET_INT96_REBASE_MODE_IN_WRITE.key -> LegacyBehaviorPolicy.CORRECTED.toString
    ) {
      checkAllParquetReaders(values, fromType, toType, expectError = true)
    }
  }

  for {
    (fromPrecision, toPrecision) <-
    // Test widening and narrowing precision between the same and different decimal physical
    // parquet types:
    // - INT32: precisions 5, 7
    // - INT64: precisions 10, 12
    // - FIXED_LEN_BYTE_ARRAY: precisions 20, 22
    Seq(5 -> 7, 5 -> 10, 5 -> 20, 10 -> 12, 10 -> 20, 20 -> 22) ++
      Seq(7 -> 5, 10 -> 5, 20 -> 5, 12 -> 10, 20 -> 10, 22 -> 20)
  }
    test(
      s"parquet decimal precision change Decimal($fromPrecision, 2) -> Decimal($toPrecision, 2)") {
      checkAllParquetReaders(
        values = Seq("1.23", "10.34"),
        fromType = DecimalType(fromPrecision, 2),
        toType = DecimalType(toPrecision, 2),
        expectError = fromPrecision > toPrecision &&
          // parquet-mr allows reading decimals into a smaller precision decimal type without
          // checking for overflows. See test below.
          spark.conf.get(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key).toBoolean)
    }

  test("parquet decimal type change Decimal(5, 2) -> Decimal(3, 2) overflows with parquet-mr") {
    withTempDir { dir =>
      withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
        writeParquetFiles(
          dir,
          values = Seq("123.45", "999.99"),
          DecimalType(5, 2),
          dictionaryEnabled = false)
        checkAnswer(readParquetFiles(dir, DecimalType(3, 2)), Row(null) :: Row(null) :: Nil)
      }
    }
  }

  test("parquet decimal type change IntegerType -> ShortType overflows") {
    withTempDir { dir =>
      withAllParquetReaders {
        // Int & Short are both stored as INT32 in Parquet but Int.MinValue will overflow when
        // reading as Short in Spark.
        val overflowValue = Short.MaxValue.toInt + 1
        writeParquetFiles(
          dir,
          Seq(overflowValue.toString),
          IntegerType,
          dictionaryEnabled = false)
        checkAnswer(readParquetFiles(dir, ShortType), Row(Short.MinValue))
      }
    }
  }
}
