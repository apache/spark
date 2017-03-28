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
import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat
import java.util.Locale

import com.google.common.io.Files
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot}
import org.apache.arrow.vector.file.json.JsonFileReader
import org.apache.arrow.vector.util.Validator
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkException
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils


class ArrowConvertersSuite extends SharedSQLContext with BeforeAndAfterAll {
  import testImplicits._

  private var tempDataPath: String = _

  private def collectAsArrow(df: DataFrame,
                             converter: Option[ArrowConverters] = None): ArrowPayload = {
    val cnvtr = converter.getOrElse(new ArrowConverters)
    val payloadByteArrays = df.toArrowPayloadBytes().collect()
    cnvtr.readPayloadByteArrays(payloadByteArrays)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDataPath = Utils.createTempDir(namePrefix = "arrow").getAbsolutePath
  }

  test("collect to arrow record batch") {
    val indexData = (1 to 6).toDF("i")
    val arrowPayload = collectAsArrow(indexData)
    assert(arrowPayload.nonEmpty)
    val arrowBatches = arrowPayload.toArray
    assert(arrowBatches.length == indexData.rdd.getNumPartitions)
    val rowCount = arrowBatches.map(batch => batch.getLength).sum
    assert(rowCount === indexData.count())
    arrowBatches.foreach(batch => assert(batch.getNodes.size() > 0))
    arrowBatches.foreach(batch => batch.close())
  }

  test("numeric type conversion") {
    collectAndValidate(indexData)
    collectAndValidate(shortData)
    collectAndValidate(intData)
    collectAndValidate(longData)
    collectAndValidate(floatData)
    collectAndValidate(doubleData)
  }

  test("mixed numeric type conversion") {
    collectAndValidate(mixedNumericData)
  }

  test("boolean type conversion") {
    collectAndValidate(boolData)
  }

  test("string type conversion") {
    collectAndValidate(stringData)
  }

  test("byte type conversion") {
    collectAndValidate(byteData)
  }

  ignore("timestamp conversion") {
    collectAndValidate(timestampData)
  }

  // TODO: Not currently supported in Arrow JSON reader
  ignore("date conversion") {
    // collectAndValidate(dateTimeData)
  }

  // TODO: Not currently supported in Arrow JSON reader
  ignore("binary type conversion") {
    // collectAndValidate(binaryData)
  }

  test("floating-point NaN") {
    collectAndValidate(floatNaNData)
  }

  test("partitioned DataFrame") {
    val converter = new ArrowConverters
    val schema = testData2.schema
    val arrowPayload = collectAsArrow(testData2, Some(converter))
    val arrowBatches = arrowPayload.toArray
    // NOTE: testData2 should have 2 partitions -> 2 arrow batches in payload
    assert(arrowBatches.length === 2)
    val pl1 = new ArrowStaticPayload(arrowBatches(0))
    val pl2 = new ArrowStaticPayload(arrowBatches(1))
    // Generate JSON files
    val a = List[Int](1, 1, 2, 2, 3, 3)
    val b = List[Int](1, 2, 1, 2, 1, 2)
    val fields = Seq(new IntegerType("a", is_signed = true, 32, nullable = false),
      new IntegerType("b", is_signed = true, 32, nullable = false))
    def getBatch(x: Seq[Int], y: Seq[Int]): JSONRecordBatch = {
      val columns = Seq(new PrimitiveColumn("a", x.length, x.map(_ => true), x),
        new PrimitiveColumn("b", y.length, y.map(_ => true), y))
      new JSONRecordBatch(x.length, columns)
    }
    val json1 = new JSONFile(new JSONSchema(fields), Seq(getBatch(a.take(3), b.take(3))))
    val json2 = new JSONFile(new JSONSchema(fields), Seq(getBatch(a.takeRight(3), b.takeRight(3))))
    val tempFile1 = new File(tempDataPath, "testData2-ints-part1.json")
    val tempFile2 = new File(tempDataPath, "testData2-ints-part2.json")
    json1.write(tempFile1)
    json2.write(tempFile2)
    validateConversion(schema, pl1, tempFile1, Some(converter))
    validateConversion(schema, pl2, tempFile2, Some(converter))
  }

  test("empty frame collect") {
    val arrowPayload = collectAsArrow(spark.emptyDataFrame)
    assert(arrowPayload.isEmpty)
  }

  test("empty partition collect") {
    val emptyPart = spark.sparkContext.parallelize(Seq(1), 2).toDF("i")
    val arrowPayload = collectAsArrow(emptyPart)
    val arrowBatches = arrowPayload.toArray
    assert(arrowBatches.length === 2)
    assert(arrowBatches.count(_.getLength == 0) === 1)
    assert(arrowBatches.count(_.getLength == 1) === 1)
  }

  test("unsupported types") {
    def runUnsupported(block: => Unit): Unit = {
      val msg = intercept[SparkException] {
        block
      }
      assert(msg.getMessage.contains("Unsupported data type"))
      assert(msg.getCause.getClass === classOf[UnsupportedOperationException])
    }

    runUnsupported { collectAsArrow(decimalData) }
    runUnsupported { collectAsArrow(arrayData.toDF()) }
    runUnsupported { collectAsArrow(mapData.toDF()) }
    runUnsupported { collectAsArrow(complexData) }
  }

  test("test Arrow Validator") {
    val sdata = shortData
    val idata = intData

    // Different schema
    intercept[IllegalArgumentException] {
      collectAndValidate(DataTuple(sdata.df, idata.json, idata.file))
    }

    // Different values
    intercept[IllegalArgumentException] {
      collectAndValidate(DataTuple(idata.df.sort($"a_i".desc), idata.json, idata.file))
    }
  }

  /** Test that a converted DataFrame to Arrow record batch equals batch read from JSON file */
  private def collectAndValidate(data: DataTuple): Unit = {
    val converter = new ArrowConverters
    // NOTE: coalesce to single partition because can only load 1 batch in validator
    val arrowPayload = collectAsArrow(data.df.coalesce(1), Some(converter))
    val tempFile = new File(tempDataPath, data.file)
    data.json.write(tempFile)
    validateConversion(data.df.schema, arrowPayload, tempFile, Some(converter))
  }

  private def validateConversion(sparkSchema: StructType,
                                 arrowPayload: ArrowPayload,
                                 jsonFile: File,
                                 converterOpt: Option[ArrowConverters] = None): Unit = {
    val converter = converterOpt.getOrElse(new ArrowConverters)
    val jsonReader = new JsonFileReader(jsonFile, converter.allocator)

    val arrowSchema = ArrowConverters.schemaToArrowSchema(sparkSchema)
    val jsonSchema = jsonReader.start()
    Validator.compareSchemas(arrowSchema, jsonSchema)

    val arrowRoot = new VectorSchemaRoot(arrowSchema, converter.allocator)
    val vectorLoader = new VectorLoader(arrowRoot)
    arrowPayload.foreach(vectorLoader.load)
    val jsonRoot = jsonReader.read()
    Validator.compareVectorSchemaRoot(arrowRoot, jsonRoot)
  }

  // Create Spark DataFrame and matching Arrow JSON at same time for validation
  private case class DataTuple(df: DataFrame, json: JSONFile, file: String)

  private def indexData: DataTuple = {
    val data = List[Int](1, 2, 3, 4, 5, 6)
    val fields = Seq(new IntegerType("i", is_signed = true, 32, nullable = false))
    val schema = new JSONSchema(fields)
    val columns = Seq(new PrimitiveColumn("i", data.length, data.map(_ => true), data))
    val batch = new JSONRecordBatch(data.length, columns)
    DataTuple(data.toDF("i"), new JSONFile(schema, Seq(batch)), "indexData-ints.json")
  }

  private def shortData: DataTuple = {
    val a_s = List[Short](1, -1, 2, -2, 32767, -32768)
    val b_s = List[Option[Short]](Some(1), None, None, Some(-2), None, Some(-32768))
    val fields = Seq(new IntegerType("a_s", is_signed = true, 16, nullable = false),
      new IntegerType("b_s", is_signed = true, 16, nullable = true))
    val schema = new JSONSchema(fields)
    val b_s_values = b_s.map(_.map(_.toInt).getOrElse(0))
    val columns = Seq(
      new PrimitiveColumn("a_s", a_s.length, a_s.map(_ => true), a_s.map(_.toInt)),
      new PrimitiveColumn("b_s", b_s.length, b_s.map(_.isDefined), b_s_values))
    val batch = new JSONRecordBatch(a_s.length, columns)
    val df = a_s.zip(b_s).toDF("a_s", "b_s")
    DataTuple(df, new JSONFile(schema, Seq(batch)), "integer-16bit.json")
  }

  private def intData: DataTuple = {
    val a_i = List[Int](1, -1, 2, -2, 2147483647, -2147483648)
    val b_i = List[Option[Int]](Some(1), None, None, Some(-2), None, Some(-2147483648))
    val fields = Seq(new IntegerType("a_i", is_signed = true, 32, nullable = false),
      new IntegerType("b_i", is_signed = true, 32, nullable = true))
    val schema = new JSONSchema(fields)
    val columns = Seq(
      new PrimitiveColumn("a_i", a_i.length, a_i.map(_ => true), a_i),
      new PrimitiveColumn("b_i", b_i.length, b_i.map(_.isDefined), b_i.map(_.getOrElse(0))))
    val batch = new JSONRecordBatch(a_i.length, columns)
    val df = a_i.zip(b_i).toDF("a_i", "b_i")
    DataTuple(df, new JSONFile(schema, Seq(batch)), "integer-32bit.json")
  }

  private def longData: DataTuple = {
    val a_l = List[Long](1, -1, 2, -2, 9223372036854775807L, -9223372036854775808L)
    val b_l = List[Option[Long]](Some(1), None, None, Some(-2), None, Some(-9223372036854775808L))
    val fields = Seq(new IntegerType("a_l", is_signed = true, 64, nullable = false),
      new IntegerType("b_l", is_signed = true, 64, nullable = true))
    val schema = new JSONSchema(fields)
    val columns = Seq(
      new PrimitiveColumn("a_l", a_l.length, a_l.map(_ => true), a_l),
      new PrimitiveColumn("b_l", b_l.length, b_l.map(_.isDefined), b_l.map(_.getOrElse(0L))))
    val batch = new JSONRecordBatch(a_l.length, columns)
    val df = a_l.zip(b_l).toDF("a_l", "b_l")
    DataTuple(df, new JSONFile(schema, Seq(batch)), "integer-64bit.json")
  }

  private def floatData: DataTuple = {
    val a_f = List(1.0f, 2.0f, 0.01f, 200.0f, 0.0001f, 20000.0f)
    val b_f = List[Option[Float]](Some(1.1f), None, None, Some(2.2f), None, Some(3.3f))
    val fields = Seq(new FloatingPointType("a_f", 32, nullable = false),
      new FloatingPointType("b_f", 32, nullable = true))
    val schema = new JSONSchema(fields)
    val columns = Seq(new PrimitiveColumn("a_f", a_f.length, a_f.map(_ => true), a_f),
      new PrimitiveColumn("b_f", b_f.length, b_f.map(_.isDefined), b_f.map(_.getOrElse(0.0f))))
    val batch = new JSONRecordBatch(a_f.length, columns)
    val df = a_f.zip(b_f).toDF("a_f", "b_f")
    DataTuple(df, new JSONFile(schema, Seq(batch)), "floating_point-single_precision.json")
  }

  private def doubleData: DataTuple = {
    val a_d = List(1.0, 2.0, 0.01, 200.0, 0.0001, 20000.0)
    val b_d = List[Option[Double]](Some(1.1), None, None, Some(2.2), None, Some(3.3))
    val fields = Seq(new FloatingPointType("a_d", 64, nullable = false),
      new FloatingPointType("b_d", 64, nullable = true))
    val schema = new JSONSchema(fields)
    val columns = Seq(new PrimitiveColumn("a_d", a_d.length, a_d.map(_ => true), a_d),
      new PrimitiveColumn("b_d", b_d.length, b_d.map(_.isDefined), b_d.map(_.getOrElse(0.0))))
    val batch = new JSONRecordBatch(a_d.length, columns)
    val df = a_d.zip(b_d).toDF("a_d", "b_d")
    DataTuple(df, new JSONFile(schema, Seq(batch)), "floating_point-double_precision.json")
  }

  private def mixedNumericData: DataTuple = {
    val data = List(1, 2, 3, 4, 5, 6)
    val fields = Seq(new IntegerType("a", is_signed = true, 16, nullable = false),
      new FloatingPointType("b", 32, nullable = false),
      new IntegerType("c", is_signed = true, 32, nullable = false),
      new FloatingPointType("d", 64, nullable = false),
      new IntegerType("e", is_signed = true, 64, nullable = false))
    val schema = new JSONSchema(fields)
    val columns = Seq(new PrimitiveColumn("a", data.length, data.map(_ => true), data),
      new PrimitiveColumn("b", data.length, data.map(_ => true), data.map(_.toFloat)),
      new PrimitiveColumn("c", data.length, data.map(_ => true), data),
      new PrimitiveColumn("d", data.length, data.map(_ => true), data.map(_.toDouble)),
      new PrimitiveColumn("e", data.length, data.map(_ => true), data)
    )
    val batch = new JSONRecordBatch(data.length, columns)
    val data_tuples = for (d <- data) yield {
      (d.toShort, d.toFloat, d.toInt, d.toDouble, d.toLong)
    }
    val df = data_tuples.toDF("a", "b", "c", "d", "e")
    DataTuple(df, new JSONFile(schema, Seq(batch)), "mixed_numeric_types.json")
  }

  private def boolData: DataTuple = {
    val data = Seq(true, true, false, true)
    val fields = Seq(new BooleanType("a_bool", nullable = false))
    val schema = new JSONSchema(fields)
    val columns = Seq(new PrimitiveColumn("a_bool", data.length, data.map(_ => true), data))
    val batch = new JSONRecordBatch(data.length, columns)
    DataTuple(data.toDF("a_bool"), new JSONFile(schema, Seq(batch)), "boolData.json")
  }

  private def stringData: DataTuple = {
    val upperCase = Seq("A", "B", "C")
    val lowerCase = Seq("a", "b", "c")
    val nullStr = Seq("ab", "CDE", null)
    val fields = Seq(new StringType("upper_case", nullable = true),
      new StringType("lower_case", nullable = true),
      new StringType("null_str", nullable = true))
    val schema = new JSONSchema(fields)
    val columns = Seq(
      new StringColumn("upper_case", upperCase.length, upperCase.map(_ => true), upperCase),
      new StringColumn("lower_case", lowerCase.length, lowerCase.map(_ => true), lowerCase),
      new StringColumn("null_str", nullStr.length, nullStr.map(_ != null),
        nullStr.map { s => if (s == null) "" else s}
      ))
    val batch = new JSONRecordBatch(upperCase.length, columns)
    val df = (upperCase, lowerCase, nullStr).zipped.toList
      .toDF("upper_case", "lower_case", "null_str")
    DataTuple(df, new JSONFile(schema, Seq(batch)), "stringData.json")
  }

  private def byteData: DataTuple = {
    val data = List[Byte](1.toByte, (-1).toByte, 64.toByte, Byte.MaxValue)
    val fields = Seq(new IntegerType("a_byte", is_signed = true, 8, nullable = false))
    val schema = new JSONSchema(fields)
    val columns = Seq(
      new PrimitiveColumn("a_byte", data.length, data.map(_ => true), data.map(_.toInt)))
    val batch = new JSONRecordBatch(data.length, columns)
    DataTuple(data.toDF("a_byte"), new JSONFile(schema, Seq(batch)), "byteData.json")
  }

  private def floatNaNData: DataTuple = {
    val fnan = Seq(1.2F, Float.NaN)
    val dnan = Seq(Double.NaN, 1.2)
    val fields = Seq(new FloatingPointType("NaN_f", 32, nullable = false),
      new FloatingPointType("NaN_d", 64, nullable = false))
    val schema = new JSONSchema(fields)
    val columns = Seq(new PrimitiveColumn("NaN_f", fnan.length, fnan.map(_ => true), fnan),
      new PrimitiveColumn("NaN_d", dnan.length, dnan.map(_ => true), dnan))
    val batch = new JSONRecordBatch(fnan.length, columns)
    val df = fnan.zip(dnan).toDF("NaN_f", "NaN_d")
    DataTuple(df, new JSONFile(schema, Seq(batch)), "nanData-floating_point.json")
  }

  private def timestampData: DataTuple = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS z", Locale.US)
    val ts1 = new Timestamp(sdf.parse("2013-04-08 01:10:15.567 UTC").getTime)
    val ts2 = new Timestamp(sdf.parse("2013-04-08 13:10:10.789 UTC").getTime)
    val data = Seq(ts1, ts2)
    val schema = new JSONSchema(Seq(new TimestampType("c_timestamp")))
    val columns = Seq(
      new PrimitiveColumn("c_timestamp", data.length, data.map(_ => true), data.map(_.getTime)))
    val batch = new JSONRecordBatch(data.length, columns)
    DataTuple(data.toDF("c_timestamp"), new JSONFile(schema, Seq(batch)), "timestampData.json")
  }

  private def dateData: DataTuple = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS z", Locale.US)
    val d1 = new Date(sdf.parse("2015-04-08 13:10:15.000 UTC").getTime)
    val d2 = new Date(sdf.parse("2015-04-08 13:10:15.000 UTC").getTime)
    val ts1 = new Timestamp(sdf.parse("2013-04-08 01:10:15.567 UTC").getTime)
    val ts2 = new Timestamp(sdf.parse("2013-04-08 13:10:10.789 UTC").getTime)
    val df = Seq((d1, sdf.format(d1), ts1), (d2, sdf.format(d2), ts2))
      .toDF("a_date", "b_string", "c_timestamp")
    val jsonFile = new JSONFile(new JSONSchema(Seq.empty[DataType]), Seq.empty[JSONRecordBatch])
    DataTuple(df, jsonFile, "dateData.json")
  }

  /**
   * Arrow JSON Format Data Generation
   * Referenced from https://github.com/apache/arrow/blob/master/integration/integration_test.py
   * TODO: Look into using JSON generation from parquet-vector.jar
   */

  private abstract class DataType(name: String, nullable: Boolean) {
    def _get_type: JObject
    def _get_type_layout: JField
    def _get_children: JArray
    def get_json: JObject = {
      JObject(
        "name" -> name,
        "type" -> _get_type,
        "nullable" -> nullable,
        "children" -> _get_children,
        "typeLayout" -> _get_type_layout)
    }
  }

  private abstract class Column(name: String, count: Int) {
    def _get_children: JArray
    def _get_buffers: JObject
    def get_json: JObject = {
      val entries = JObject(
        "name" -> name,
        "count" -> count
      ).merge(_get_buffers)

      val children = _get_children
      if (children.arr.nonEmpty) entries.merge(JObject("children" -> children)) else entries
    }
  }

  private abstract class PrimitiveType(name: String, nullable: Boolean)
    extends DataType(name, nullable) {
    val bit_width: Int
    override def _get_children: JArray = JArray(List.empty)
    override def _get_type_layout: JField = {
      JField("vectors", JArray(List(
        JObject("type" -> "VALIDITY", "typeBitWidth" -> 1),
        JObject("type" -> "DATA", "typeBitWidth" -> bit_width)
      )))
    }
  }

  private class PrimitiveColumn[T <% JValue](name: String,
                                             count: Int,
                                             is_valid: Seq[Boolean],
                                             values: Seq[T])
    extends Column(name, count) {
    override def _get_children: JArray = JArray(List.empty)
    override def _get_buffers: JObject = {
      JObject(
        "VALIDITY" -> is_valid.map(b => if (b) 1 else 0),
        "DATA" -> values)
    }
  }

  private class IntegerType(name: String,
                            is_signed: Boolean,
                            override val bit_width: Int,
                            nullable: Boolean)
    extends PrimitiveType(name, nullable = nullable) {
    override def _get_type: JObject = {
      JObject(
        "name" -> "int",
        "isSigned" -> is_signed,
        "bitWidth" -> bit_width)
    }
  }

  private class FloatingPointType(name: String, override val bit_width: Int, nullable: Boolean)
    extends PrimitiveType(name, nullable = nullable) {
    override def _get_type: JObject = {
      val precision = bit_width match {
        case 16 => "HALF"
        case 32 => "SINGLE"
        case 64 => "DOUBLE"
      }
      JObject(
        "name" -> "floatingpoint",
        "precision" -> precision)
    }
  }

  private class BooleanType(name: String, nullable: Boolean)
    extends PrimitiveType(name, nullable = nullable) {
    override val bit_width = 1
    override def _get_type: JObject = JObject("name" -> JString("bool"))
  }

  private class BinaryType(name: String, nullable: Boolean)
    extends PrimitiveType(name, nullable = nullable) {
    override val bit_width = 8
    override def _get_type: JObject = JObject("name" -> JString("binary"))
    override def _get_type_layout: JField = {
      JField("vectors", JArray(List(
        JObject("type" -> "VALIDITY", "typeBitWidth" -> 1),
        JObject("type" -> "OFFSET", "typeBitWidth" -> 32),
        JObject("type" -> "DATA", "typeBitWidth" -> bit_width)
      )))
    }
  }

  private class StringType(name: String, nullable: Boolean)
    extends BinaryType(name, nullable = nullable) {
    override def _get_type: JObject = JObject("name" -> JString("utf8"))
  }

  private class TimestampType(name: String) extends PrimitiveType(name, nullable = true) {
    override val bit_width = 64
    override def _get_type: JObject = {
      JObject(
        "name" -> "timestamp",
        "unit" -> "MILLISECOND")
    }
  }

  private class JSONSchema(fields: Seq[DataType]) {
    def get_json: JObject = {
      JObject("fields" -> JArray(fields.map(_.get_json).toList))
    }
  }

  private class BinaryColumn(name: String,
                             count: Int,
                             is_valid: Seq[Boolean],
                             values: Seq[String])
    extends PrimitiveColumn(name, count, is_valid, values) {
    def _encode_value(v: String): String = {
      v.map(c => String.format("%h", c.toString)).reduce(_ + _)
    }
    override def _get_buffers: JObject = {
      var offset = 0
      val offsets = scala.collection.mutable.ArrayBuffer[Int](offset)
      val data = values.zip(is_valid).map { case (value, isval) =>
        if (isval) offset += value.length
        val element = _encode_value(if (isval) value else "")
        offsets += offset
        element
      }
      JObject(
        "VALIDITY" -> is_valid.map(b => if (b) 1 else 0),
        "OFFSET" -> offsets,
        "DATA" -> data)
    }
  }

  private class StringColumn(name: String,
                             count: Int,
                             is_valid: Seq[Boolean],
                             values: Seq[String])
    extends BinaryColumn(name, count, is_valid, values) {
    override def _encode_value(v: String): String = v
  }

  private class JSONRecordBatch(count: Int, columns: Seq[Column]) {
    def get_json: JObject = {
      JObject(
        "count" -> count,
        "columns" -> columns.map(_.get_json))
    }
  }

  private class JSONFile(schema: JSONSchema, batches: Seq[JSONRecordBatch]) {
    def get_json: JObject = {
      JObject(
        "schema" -> schema.get_json,
        "batches" -> batches.map(_.get_json))
    }
    def write(file: File): Unit = {
      val json = pretty(render(get_json))
      Files.write(json, file, StandardCharsets.UTF_8)
    }
  }
}
