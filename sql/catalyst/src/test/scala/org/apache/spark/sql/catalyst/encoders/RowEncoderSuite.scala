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

package org.apache.spark.sql.catalyst.encoders

import scala.collection.mutable
import scala.util.Random

import org.apache.spark.SparkRuntimeException
import org.apache.spark.sql.{RandomDataGenerator, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.CodegenInterpretedPlanTest
import org.apache.spark.sql.catalyst.util.{ArrayData, DateTimeUtils, GenericArrayData, IntervalUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DataTypeTestUtils.{dayTimeIntervalTypes, yearMonthIntervalTypes}

@SQLUserDefinedType(udt = classOf[ExamplePointUDT])
class ExamplePoint(val x: Double, val y: Double) extends Serializable {
  override def hashCode: Int = 41 * (41 + x.toInt) + y.toInt
  override def equals(that: Any): Boolean = {
    that match {
      case e: ExamplePoint =>
        (this.x == e.x || (this.x.isNaN && e.x.isNaN) || (this.x.isInfinity && e.x.isInfinity)) &&
          (this.y == e.y || (this.y.isNaN && e.y.isNaN) || (this.y.isInfinity && e.y.isInfinity))
      case _ => false
    }
  }
}

/**
 * User-defined type for [[ExamplePoint]].
 */
class ExamplePointUDT extends UserDefinedType[ExamplePoint] {

  override def sqlType: DataType = ArrayType(DoubleType, false)

  override def pyUDT: String = "pyspark.sql.tests.ExamplePointUDT"

  override def serialize(p: ExamplePoint): GenericArrayData = {
    val output = new Array[Any](2)
    output(0) = p.x
    output(1) = p.y
    new GenericArrayData(output)
  }

  override def deserialize(datum: Any): ExamplePoint = {
    datum match {
      case values: ArrayData =>
        if (values.numElements() > 1) {
          new ExamplePoint(values.getDouble(0), values.getDouble(1))
        } else {
          val random = new Random()
          new ExamplePoint(random.nextDouble(), random.nextDouble())
        }
    }
  }

  override def userClass: Class[ExamplePoint] = classOf[ExamplePoint]

  private[spark] override def asNullable: ExamplePointUDT = this
}

class RowEncoderSuite extends CodegenInterpretedPlanTest {

  private val structOfString = new StructType().add("str", StringType)
  private val structOfUDT = new StructType().add("udt", new ExamplePointUDT, false)
  private val arrayOfString = ArrayType(StringType)
  private val arrayOfNull = ArrayType(NullType)
  private val mapOfString = MapType(StringType, StringType)
  private val arrayOfUDT = ArrayType(new ExamplePointUDT, false)

  private def toRow(encoder: ExpressionEncoder[Row], row: Row): InternalRow = {
    encoder.createSerializer().apply(row)
  }

  private def fromRow(encoder: ExpressionEncoder[Row], row: InternalRow): Row = {
    encoder.createDeserializer().apply(row)
  }

  private def roundTrip(encoder: ExpressionEncoder[Row], row: Row): Row = {
    fromRow(encoder, toRow(encoder, row))
  }

  encodeDecodeTest(
    new StructType()
      .add("null", NullType)
      .add("boolean", BooleanType)
      .add("byte", ByteType)
      .add("short", ShortType)
      .add("int", IntegerType)
      .add("long", LongType)
      .add("float", FloatType)
      .add("double", DoubleType)
      .add("decimal", DecimalType.SYSTEM_DEFAULT)
      .add("string", StringType)
      .add("binary", BinaryType)
      .add("date", DateType)
      .add("timestamp", TimestampType)
      .add("udt", new ExamplePointUDT))

  encodeDecodeTest(
    new StructType()
      .add("arrayOfNull", arrayOfNull)
      .add("arrayOfString", arrayOfString)
      .add("arrayOfArrayOfString", ArrayType(arrayOfString))
      .add("arrayOfArrayOfInt", ArrayType(ArrayType(IntegerType)))
      .add("arrayOfMap", ArrayType(mapOfString))
      .add("arrayOfStruct", ArrayType(structOfString))
      .add("arrayOfUDT", arrayOfUDT))

  encodeDecodeTest(
    new StructType()
      .add("mapOfIntAndString", MapType(IntegerType, StringType))
      .add("mapOfStringAndArray", MapType(StringType, arrayOfString))
      .add("mapOfArrayAndInt", MapType(arrayOfString, IntegerType))
      .add("mapOfArray", MapType(arrayOfString, arrayOfString))
      .add("mapOfStringAndStruct", MapType(StringType, structOfString))
      .add("mapOfStructAndString", MapType(structOfString, StringType))
      .add("mapOfStruct", MapType(structOfString, structOfString)))

  encodeDecodeTest(
    new StructType()
      .add("structOfString", structOfString)
      .add("structOfStructOfString", new StructType().add("struct", structOfString))
      .add("structOfArray", new StructType().add("array", arrayOfString))
      .add("structOfMap", new StructType().add("map", mapOfString))
      .add("structOfArrayAndMap",
        new StructType().add("array", arrayOfString).add("map", mapOfString))
      .add("structOfUDT", structOfUDT))

  test("encode/decode decimal type") {
    val schema = new StructType()
      .add("int", IntegerType)
      .add("string", StringType)
      .add("double", DoubleType)
      .add("java_decimal", DecimalType.SYSTEM_DEFAULT)
      .add("scala_decimal", DecimalType.SYSTEM_DEFAULT)
      .add("catalyst_decimal", DecimalType.SYSTEM_DEFAULT)

    val encoder = ExpressionEncoder(schema).resolveAndBind()

    val javaDecimal = new java.math.BigDecimal("1234.5678")
    val scalaDecimal = BigDecimal("1234.5678")
    val catalystDecimal = Decimal("1234.5678")

    val input = Row(100, "test", 0.123, javaDecimal, scalaDecimal, catalystDecimal)
    val convertedBack = roundTrip(encoder, input)
    // Decimal will be converted back to Java BigDecimal when decoding.
    assert(convertedBack.getDecimal(3).compareTo(javaDecimal) == 0)
    assert(convertedBack.getDecimal(4).compareTo(scalaDecimal.bigDecimal) == 0)
    assert(convertedBack.getDecimal(5).compareTo(catalystDecimal.toJavaBigDecimal) == 0)
  }

  test("RowEncoder should preserve decimal precision and scale") {
    val schema = new StructType().add("decimal", DecimalType(10, 5), false)
    val encoder = ExpressionEncoder(schema).resolveAndBind()
    val decimal = Decimal("67123.45")
    val input = Row(decimal)
    val row = toRow(encoder, input)

    assert(row.toSeq(schema).head == decimal)
  }

  test("SPARK-23179: RowEncoder should respect nullOnOverflow for decimals") {
    val schema = new StructType().add("decimal", DecimalType.SYSTEM_DEFAULT)
    testDecimalOverflow(schema, Row(BigDecimal("9" * 100)))
    testDecimalOverflow(schema, Row(new java.math.BigDecimal("9" * 100)))
  }

  private def testDecimalOverflow(schema: StructType, row: Row): Unit = {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val encoder = ExpressionEncoder(schema).resolveAndBind()
      intercept[Exception] {
        toRow(encoder, row)
      } match {
        case e: ArithmeticException =>
          assert(e.getMessage.contains("cannot be represented as Decimal"))
        case e: RuntimeException =>
          assert(e.getCause.isInstanceOf[ArithmeticException])
          assert(e.getCause.getMessage.contains("cannot be represented as Decimal"))
      }
    }

    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      val encoder = ExpressionEncoder(schema).resolveAndBind()
      assert(roundTrip(encoder, row).get(0) == null)
    }
  }

  test("RowEncoder should preserve schema nullability") {
    val schema = new StructType().add("int", IntegerType, nullable = false)
    val encoder = ExpressionEncoder(schema).resolveAndBind()
    assert(encoder.serializer.length == 1)
    assert(encoder.serializer.head.dataType == IntegerType)
    assert(encoder.serializer.head.nullable == false)
  }

  test("RowEncoder should preserve nested column name") {
    val schema = new StructType().add(
      "struct",
      new StructType()
        .add("i", IntegerType, nullable = false)
        .add(
          "s",
          new StructType().add("int", IntegerType, nullable = false),
          nullable = false),
      nullable = false)
    val encoder = ExpressionEncoder(schema).resolveAndBind()
    assert(encoder.serializer.length == 1)
    assert(encoder.serializer.head.dataType ==
      new StructType()
      .add("i", IntegerType, nullable = false)
      .add(
        "s",
        new StructType().add("int", IntegerType, nullable = false),
        nullable = false))
    assert(encoder.serializer.head.nullable == false)
  }

  test("RowEncoder should support primitive arrays") {
    val schema = new StructType()
      .add("booleanPrimitiveArray", ArrayType(BooleanType, false))
      .add("bytePrimitiveArray", ArrayType(ByteType, false))
      .add("shortPrimitiveArray", ArrayType(ShortType, false))
      .add("intPrimitiveArray", ArrayType(IntegerType, false))
      .add("longPrimitiveArray", ArrayType(LongType, false))
      .add("floatPrimitiveArray", ArrayType(FloatType, false))
      .add("doublePrimitiveArray", ArrayType(DoubleType, false))
    val encoder = ExpressionEncoder(schema).resolveAndBind()
    val input = Seq(
      Array(true, false),
      Array(1.toByte, 64.toByte, Byte.MaxValue),
      Array(1.toShort, 255.toShort, Short.MaxValue),
      Array(1, 10000, Int.MaxValue),
      Array(1.toLong, 1000000.toLong, Long.MaxValue),
      Array(1.1.toFloat, 123.456.toFloat, Float.MaxValue),
      Array(11.1111, 123456.7890123, Double.MaxValue)
    )
    val convertedBack = roundTrip(encoder, Row.fromSeq(input))
    input.zipWithIndex.map { case (array, index) =>
      assert(convertedBack.getSeq(index) === array)
    }
  }

  test("RowEncoder should support array as the external type for ArrayType") {
    val schema = new StructType()
      .add("array", ArrayType(IntegerType))
      .add("nestedArray", ArrayType(ArrayType(StringType)))
      .add("deepNestedArray", ArrayType(ArrayType(ArrayType(LongType))))
    val encoder = ExpressionEncoder(schema).resolveAndBind()
    val input = Row(
      Array(1, 2, null),
      Array(Array("abc", null), null),
      Array(Seq(Array(0L, null), null), null))
    val convertedBack = roundTrip(encoder, input)
    assert(convertedBack.getSeq(0) == Seq(1, 2, null))
    assert(convertedBack.getSeq(1) == Seq(Seq("abc", null), null))
    assert(convertedBack.getSeq(2) == Seq(Seq(Seq(0L, null), null), null))
  }

  test("RowEncoder should throw RuntimeException if input row object is null") {
    val schema = new StructType().add("int", IntegerType)
    val encoder = ExpressionEncoder(schema)
    // Check the error class only since the parameters may change depending on how we are running
    // this test case.
    val exception = intercept[SparkRuntimeException](toRow(encoder, null))
    assert(exception.getErrorClass == "EXPRESSION_ENCODING_FAILED")
  }

  test("RowEncoder should validate external type") {
    val e1 = intercept[RuntimeException] {
      val schema = new StructType().add("a", IntegerType)
      val encoder = ExpressionEncoder(schema)
      toRow(encoder, Row(1.toShort))
    }
    assert(e1.getCause.getMessage.contains("java.lang.Short is not a valid external type"))

    val e2 = intercept[RuntimeException] {
      val schema = new StructType().add("a", StringType)
      val encoder = ExpressionEncoder(schema)
      toRow(encoder, Row(1))
    }
    assert(e2.getCause.getMessage.contains("java.lang.Integer is not a valid external type"))

    val e3 = intercept[RuntimeException] {
      val schema = new StructType().add("a",
        new StructType().add("b", IntegerType).add("c", StringType))
      val encoder = ExpressionEncoder(schema)
      toRow(encoder, Row(1 -> "a"))
    }
    assert(e3.getCause.getMessage.contains("scala.Tuple2 is not a valid external type"))

    val e4 = intercept[RuntimeException] {
      val schema = new StructType().add("a", ArrayType(TimestampType))
      val encoder = ExpressionEncoder(schema)
      toRow(encoder, Row(Array("a")))
    }
    assert(e4.getCause.getMessage.contains("java.lang.String is not a valid external type"))
  }

  private def roundTripArray[T](dt: DataType, nullable: Boolean, data: Array[T]): Unit = {
    val schema = new StructType().add("a", ArrayType(dt, nullable))
    test(s"RowEncoder should return mutable.ArraySeq with properly typed array for $schema") {
      val encoder = ExpressionEncoder(schema).resolveAndBind()
      val result = fromRow(encoder, toRow(encoder, Row(data))).getAs[mutable.ArraySeq[_]](0)
      assert(result.array.getClass === data.getClass)
      assert(result === data)
    }
  }

  roundTripArray(IntegerType, nullable = false, Array(1, 2, 3).map(Int.box))
  roundTripArray(StringType, nullable = true, Array("hello", "world", "!", null))

  test("SPARK-25791: Datatype of serializers should be accessible") {
    val udtSQLType = new StructType().add("a", IntegerType)
    val pythonUDT = new PythonUserDefinedType(udtSQLType, "pyUDT", "serializedPyClass")
    val schema = new StructType().add("pythonUDT", pythonUDT, true)
    val encoder = ExpressionEncoder(schema)
    assert(encoder.serializer(0).dataType == pythonUDT.sqlType)
  }

  test("encoding/decoding TimestampType to/from java.time.Instant") {
    withSQLConf(SQLConf.DATETIME_JAVA8API_ENABLED.key -> "true") {
      val schema = new StructType().add("t", TimestampType)
      val encoder = ExpressionEncoder(schema).resolveAndBind()
      val instant = java.time.Instant.parse("2019-02-26T16:56:00Z")
      val row = toRow(encoder, Row(instant))
      assert(row.getLong(0) === DateTimeUtils.instantToMicros(instant))
      val readback = fromRow(encoder, row)
      assert(readback.get(0) === instant)
    }
  }

  test("SPARK-35664: encoding/decoding TimestampNTZType to/from java.time.LocalDateTime") {
    val schema = new StructType().add("t", TimestampNTZType)
    val encoder = ExpressionEncoder(schema).resolveAndBind()
    val localDateTime = java.time.LocalDateTime.parse("2019-02-26T16:56:00")
    val row = toRow(encoder, Row(localDateTime))
    assert(row.getLong(0) === DateTimeUtils.localDateTimeToMicros(localDateTime))
    val readback = fromRow(encoder, row)
    assert(readback.get(0) === localDateTime)
  }

  test("encoding/decoding DateType to/from java.time.LocalDate") {
    withSQLConf(SQLConf.DATETIME_JAVA8API_ENABLED.key -> "true") {
      val schema = new StructType().add("d", DateType)
      val encoder = ExpressionEncoder(schema).resolveAndBind()
      val localDate = java.time.LocalDate.parse("2019-02-27")
      val row = toRow(encoder, Row(localDate))
      assert(row.getInt(0) === DateTimeUtils.localDateToDays(localDate))
      val readback = fromRow(encoder, row)
      assert(readback.get(0).equals(localDate))
    }
  }

  test("SPARK-34605: encoding/decoding DayTimeIntervalType to/from java.time.Duration") {
    dayTimeIntervalTypes.foreach { dayTimeIntervalType =>
      val schema = new StructType().add("d", dayTimeIntervalType)
      val encoder = ExpressionEncoder(schema).resolveAndBind()
      val duration = java.time.Duration.ofDays(1)
      val row = toRow(encoder, Row(duration))
      assert(row.getLong(0) === IntervalUtils.durationToMicros(duration))
      val readback = fromRow(encoder, row)
      assert(readback.get(0).equals(duration))
    }
  }

  test("SPARK-34615: encoding/decoding YearMonthIntervalType to/from java.time.Period") {
    yearMonthIntervalTypes.foreach { yearMonthIntervalType =>
      val schema = new StructType().add("p", yearMonthIntervalType)
      val encoder = ExpressionEncoder(schema).resolveAndBind()
      val period = java.time.Period.ofMonths(1)
      val row = toRow(encoder, Row(period))
      assert(row.getInt(0) === IntervalUtils.periodToMonths(period))
      val readback = fromRow(encoder, row)
      assert(readback.get(0).equals(period))
    }
  }

  for {
    elementType <- Seq(IntegerType, StringType)
    containsNull <- Seq(true, false)
    nullable <- Seq(true, false)
  } {
    test("RowEncoder should preserve array nullability: " +
      s"ArrayType($elementType, containsNull = $containsNull), nullable = $nullable") {
      val schema = new StructType().add("array", ArrayType(elementType, containsNull), nullable)
      val encoder = ExpressionEncoder(schema).resolveAndBind()
      assert(encoder.serializer.length == 1)
      assert(encoder.serializer.head.dataType == ArrayType(elementType, containsNull))
      assert(encoder.serializer.head.nullable == nullable)
    }
  }

  for {
    keyType <- Seq(IntegerType, StringType)
    valueType <- Seq(IntegerType, StringType)
    valueContainsNull <- Seq(true, false)
    nullable <- Seq(true, false)
  } {
    test("RowEncoder should preserve map nullability: " +
      s"MapType($keyType, $valueType, valueContainsNull = $valueContainsNull), " +
      s"nullable = $nullable") {
      val schema = new StructType().add(
        "map", MapType(keyType, valueType, valueContainsNull), nullable)
      val encoder = ExpressionEncoder(schema).resolveAndBind()
      assert(encoder.serializer.length == 1)
      assert(encoder.serializer.head.dataType == MapType(keyType, valueType, valueContainsNull))
      assert(encoder.serializer.head.nullable == nullable)
    }
  }

  private def encodeDecodeTest(schema: StructType): Unit = {
    test(s"encode/decode: ${schema.simpleString}") {
      Seq(false, true).foreach { java8Api =>
        withSQLConf(SQLConf.DATETIME_JAVA8API_ENABLED.key -> java8Api.toString) {
          val encoder = ExpressionEncoder(schema).resolveAndBind()
          val inputGenerator = RandomDataGenerator.forType(schema, nullable = false).get

          var input: Row = null
          try {
            for (_ <- 1 to 5) {
              input = inputGenerator.apply().asInstanceOf[Row]
              val convertedBack = roundTrip(encoder, input)
              assert(input == convertedBack)
            }
          } catch {
            case e: Exception =>
              fail(
                s"""
                   |schema: ${schema.simpleString}
                   |input: ${input}
                 """.stripMargin, e)
          }
        }
      }
    }
  }

  test("SPARK-38437: encoding TimestampType/DateType from any supported datetime Java types") {
    Seq(true, false).foreach { java8Api =>
      withSQLConf(SQLConf.DATETIME_JAVA8API_ENABLED.key -> java8Api.toString) {
        val schema = new StructType()
          .add("t0", TimestampType)
          .add("t1", TimestampType)
          .add("d0", DateType)
          .add("d1", DateType)
        val encoder = ExpressionEncoder(schema, lenient = true).resolveAndBind()
        val instant = java.time.Instant.parse("2019-02-26T16:56:00Z")
        val ld = java.time.LocalDate.parse("2022-03-08")
        val row = encoder.createSerializer().apply(
          Row(instant, java.sql.Timestamp.from(instant), ld, java.sql.Date.valueOf(ld)))
        val expectedMicros = DateTimeUtils.instantToMicros(instant)
        assert(row.getLong(0) === expectedMicros)
        assert(row.getLong(1) === expectedMicros)
        val expectedDays = DateTimeUtils.localDateToDays(ld)
        assert(row.getInt(2) === expectedDays)
        assert(row.getInt(3) === expectedDays)
      }
    }
  }

  test("Encoding an mutable.ArraySeq in scala-2.13") {
    val schema = new StructType()
      .add("headers", ArrayType(new StructType()
        .add("key", StringType)
        .add("value", BinaryType)))
    val encoder = ExpressionEncoder(schema, lenient = true).resolveAndBind()
    val data = Row(mutable.ArraySeq.make(Array(Row("key", "value".getBytes))))
    val row = encoder.createSerializer()(data)
  }
}
