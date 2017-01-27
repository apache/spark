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
package org.apache.spark.sql

import java.io.File
import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot}
import org.apache.arrow.vector.file.json.JsonFileReader
import org.apache.arrow.vector.util.Validator

import org.apache.spark.sql.test.SharedSQLContext


// NOTE - nullable type can be declared as Option[*] or java.lang.*
private[sql] case class ShortData(i: Int, a_s: Short, b_s: Option[Short])
private[sql] case class IntData(i: Int, a_i: Int, b_i: Option[Int])
private[sql] case class LongData(i: Int, a_l: Long, b_l: java.lang.Long)
private[sql] case class FloatData(i: Int, a_f: Float, b_f: Option[Float])
private[sql] case class DoubleData(i: Int, a_d: Double, b_d: Option[Double])


class ArrowConvertersSuite extends SharedSQLContext {
  import testImplicits._

  private def testFile(fileName: String): String = {
    Thread.currentThread().getContextClassLoader.getResource(fileName).getFile
  }

  test("collect to arrow record batch") {
    val arrowPayload = indexData.collectAsArrow()
    assert(arrowPayload.nonEmpty)
    arrowPayload.foreach(arrowRecordBatch => assert(arrowRecordBatch.getLength > 0))
    arrowPayload.foreach(arrowRecordBatch => assert(arrowRecordBatch.getNodes.size() > 0))
    arrowPayload.foreach(arrowRecordBatch => arrowRecordBatch.close())
  }

  test("standard type conversion") {
    collectAndValidate(indexData, "test-data/arrow/indexData-ints.json")
    collectAndValidate(largeAndSmallInts, "test-data/arrow/largeAndSmall-ints.json")
    collectAndValidate(salary, "test-data/arrow/salary-doubles.json")
  }

  test("standard type nullable conversion") {
    collectAndValidate(shortData, "test-data/arrow/shortData-16bit_ints-nullable.json")
    collectAndValidate(intData, "test-data/arrow/intData-32bit_ints-nullable.json")
    collectAndValidate(longData, "test-data/arrow/longData-64bit_ints-nullable.json")
    collectAndValidate(floatData, "test-data/arrow/floatData-single_precision-nullable.json")
    collectAndValidate(doubleData, "test-data/arrow/doubleData-double_precision-nullable.json")
  }

  test("boolean type conversion") {
    val boolData = Seq(true, true, false, true).toDF("a_bool")
    collectAndValidate(boolData, "test-data/arrow/boolData.json")
  }

  test("byte type conversion") {
    val byteData = Seq(1.toByte, (-1).toByte, 64.toByte, Byte.MaxValue).toDF("a_byte")
    collectAndValidate(byteData, "test-data/arrow/byteData.json")
  }

  test("mixed standard type nullable conversion") {
    val mixedData = Seq(shortData, intData, longData, floatData, doubleData)
      .reduce((a, b) => a.join(b, "i")).sort("i")
    collectAndValidate(mixedData, "test-data/arrow/mixedData-standard-nullable.json")
  }

  test("partitioned DataFrame") {
    collectAndValidate(testData2, "test-data/arrow/testData2-ints.json")
  }

  test("string type conversion") {
    collectAndValidate(upperCaseData, "test-data/arrow/uppercase-strings.json")
    collectAndValidate(lowerCaseData, "test-data/arrow/lowercase-strings.json")
    val nullStringsColOnly = nullStrings.select(nullStrings.columns(1))
    collectAndValidate(nullStringsColOnly, "test-data/arrow/null-strings.json")
  }

  ignore("date conversion") {
    collectAndValidate(dateTimeData, "test-data/arrow/datetimeData-strings.json")
  }

  test("timestamp conversion") {
    collectAndValidate(dateTimeData.select($"c_timestamp"), "test-data/arrow/timestampData.json")
  }

  // Arrow json reader doesn't support binary data
  ignore("binary type conversion") {
    collectAndValidate(binaryData, "test-data/arrow/binaryData.json")
  }

  test("nested type conversion") { }

  test("array type conversion") { }

  test("mapped type conversion") { }

  test("floating-point NaN") {
    val nanData = Seq((1, 1.2F, Double.NaN), (2, Float.NaN, 1.23)).toDF("i", "NaN_f", "NaN_d")
    collectAndValidate(nanData, "test-data/arrow/nanData-floating_point.json")
  }

  test("convert int column with null to arrow") {
    collectAndValidate(nullInts, "test-data/arrow/null-ints.json")
    collectAndValidate(testData3, "test-data/arrow/null-ints-mixed.json")
    collectAndValidate(allNulls, "test-data/arrow/allNulls-ints.json")
  }

  test("empty frame collect") {
    val arrowPayload = spark.emptyDataFrame.collectAsArrow()
    assert(arrowPayload.nonEmpty)
    arrowPayload.foreach(emptyBatch => assert(emptyBatch.getLength == 0))
  }

  test("unsupported types") {
    def runUnsupported(block: => Unit): Unit = {
      val msg = intercept[UnsupportedOperationException] {
        block
      }
      assert(msg.getMessage.contains("Unsupported data type"))
    }

    runUnsupported {
      collectAndValidate(decimalData, "test-data/arrow/decimalData-BigDecimal.json")
    }
  }

  test("test Arrow Validator") {

    // Missing test file
    intercept[NullPointerException] {
      collectAndValidate(indexData, "test-data/arrow/missing-file")
    }

    // Different schema
    intercept[IllegalArgumentException] {
      collectAndValidate(shortData, "test-data/arrow/intData-32bit_ints-nullable.json")
    }

    // Different values
    intercept[IllegalArgumentException] {
      collectAndValidate(indexData.sort($"i".desc), "test-data/arrow/indexData-ints.json")
    }
  }

  /** Test that a converted DataFrame to Arrow record batch equals batch read from JSON file */
  private def collectAndValidate(df: DataFrame, arrowFile: String) {
    val jsonFilePath = testFile(arrowFile)

    val converter = new ArrowConverters
    val jsonReader = new JsonFileReader(new File(jsonFilePath), converter.allocator)

    val arrowSchema = ArrowConverters.schemaToArrowSchema(df.schema)
    val jsonSchema = jsonReader.start()
    Validator.compareSchemas(arrowSchema, jsonSchema)

    val arrowPayload = df.collectAsArrow(Some(converter))
    val arrowRoot = new VectorSchemaRoot(arrowSchema, converter.allocator)
    val vectorLoader = new VectorLoader(arrowRoot)
    arrowPayload.foreach(vectorLoader.load)
    val jsonRoot = jsonReader.read()

    Validator.compareVectorSchemaRoot(arrowRoot, jsonRoot)
  }

  protected lazy val indexData = Seq(1, 2, 3, 4, 5, 6).toDF("i")

  protected lazy val shortData: DataFrame = {
    spark.sparkContext.parallelize(
      ShortData(1, 1, Some(1)) ::
      ShortData(2, -1, None) ::
      ShortData(3, 2, None) ::
      ShortData(4, -2, Some(-2)) ::
      ShortData(5, 32767, None) ::
      ShortData(6, -32768, Some(-32768)) :: Nil).toDF()
  }

  protected lazy val intData: DataFrame = {
    spark.sparkContext.parallelize(
      IntData(1, 1, Some(1)) ::
      IntData(2, -1, None) ::
      IntData(3, 2, None) ::
      IntData(4, -2, Some(-2)) ::
      IntData(5, 2147483647, None) ::
      IntData(6, -2147483648, Some(-2147483648)) :: Nil).toDF()
  }

  protected lazy val longData: DataFrame = {
    spark.sparkContext.parallelize(
      LongData(1, 1L, 1L) ::
      LongData(2, -1L, null) ::
      LongData(3, 2L, null) ::
      LongData(4, -2, -2L) ::
      LongData(5, 9223372036854775807L, null) ::
      LongData(6, -9223372036854775808L, -9223372036854775808L) :: Nil).toDF()
  }

  protected lazy val floatData: DataFrame = {
    spark.sparkContext.parallelize(
      FloatData(1, 1.0f, Some(1.1f)) ::
      FloatData(2, 2.0f, None) ::
      FloatData(3, 0.01f, None) ::
      FloatData(4, 200.0f, Some(2.2f)) ::
      FloatData(5, 0.0001f, None) ::
      FloatData(6, 20000.0f, Some(3.3f)) :: Nil).toDF()
  }

  protected lazy val doubleData: DataFrame = {
    spark.sparkContext.parallelize(
      DoubleData(1, 1.0, Some(1.1)) ::
      DoubleData(2, 2.0, None) ::
      DoubleData(3, 0.01, None) ::
      DoubleData(4, 200.0, Some(2.2)) ::
      DoubleData(5, 0.0001, None) ::
      DoubleData(6, 20000.0, Some(3.3)) :: Nil).toDF()
  }

  protected lazy val dateTimeData: DataFrame = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS z", Locale.US)
    val d1 = new Date(sdf.parse("2015-04-08 13:10:15.000 UTC").getTime)
    val d2 = new Date(sdf.parse("2015-04-08 13:10:15.000 UTC").getTime)
    val ts1 = new Timestamp(sdf.parse("2013-04-08 01:10:15.567 UTC").getTime)
    val ts2 = new Timestamp(sdf.parse("2013-04-08 13:10:10.789 UTC").getTime)
    Seq((d1, sdf.format(d1), ts1), (d2, sdf.format(d2), ts2))
      .toDF("a_date", "b_string", "c_timestamp")
  }
}
