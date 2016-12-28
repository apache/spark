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

import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{RandomDataGenerator, Row}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types._

@SQLUserDefinedType(udt = classOf[ExamplePointUDT])
class ExamplePoint(val x: Double, val y: Double) extends Serializable {
  override def hashCode: Int = 41 * (41 + x.toInt) + y.toInt
  override def equals(that: Any): Boolean = {
    if (that.isInstanceOf[ExamplePoint]) {
      val e = that.asInstanceOf[ExamplePoint]
      (this.x == e.x || (this.x.isNaN && e.x.isNaN) || (this.x.isInfinity && e.x.isInfinity)) &&
        (this.y == e.y || (this.y.isNaN && e.y.isNaN) || (this.y.isInfinity && e.y.isInfinity))
    } else {
      false
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

class RowEncoderSuite extends SparkFunSuite {

  private val structOfString = new StructType().add("str", StringType)
  private val structOfUDT = new StructType().add("udt", new ExamplePointUDT, false)
  private val arrayOfString = ArrayType(StringType)
  private val arrayOfNull = ArrayType(NullType)
  private val mapOfString = MapType(StringType, StringType)
  private val arrayOfUDT = ArrayType(new ExamplePointUDT, false)

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

    val encoder = RowEncoder(schema).resolveAndBind()

    val javaDecimal = new java.math.BigDecimal("1234.5678")
    val scalaDecimal = BigDecimal("1234.5678")
    val catalystDecimal = Decimal("1234.5678")

    val input = Row(100, "test", 0.123, javaDecimal, scalaDecimal, catalystDecimal)
    val row = encoder.toRow(input)
    val convertedBack = encoder.fromRow(row)
    // Decimal will be converted back to Java BigDecimal when decoding.
    assert(convertedBack.getDecimal(3).compareTo(javaDecimal) == 0)
    assert(convertedBack.getDecimal(4).compareTo(scalaDecimal.bigDecimal) == 0)
    assert(convertedBack.getDecimal(5).compareTo(catalystDecimal.toJavaBigDecimal) == 0)
  }

  test("RowEncoder should preserve decimal precision and scale") {
    val schema = new StructType().add("decimal", DecimalType(10, 5), false)
    val encoder = RowEncoder(schema).resolveAndBind()
    val decimal = Decimal("67123.45")
    val input = Row(decimal)
    val row = encoder.toRow(input)

    assert(row.toSeq(schema).head == decimal)
  }

  test("RowEncoder should preserve schema nullability") {
    val schema = new StructType().add("int", IntegerType, nullable = false)
    val encoder = RowEncoder(schema).resolveAndBind()
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
    val encoder = RowEncoder(schema).resolveAndBind()
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
    val encoder = RowEncoder(schema).resolveAndBind()
    val input = Seq(
      Array(true, false),
      Array(1.toByte, 64.toByte, Byte.MaxValue),
      Array(1.toShort, 255.toShort, Short.MaxValue),
      Array(1, 10000, Int.MaxValue),
      Array(1.toLong, 1000000.toLong, Long.MaxValue),
      Array(1.1.toFloat, 123.456.toFloat, Float.MaxValue),
      Array(11.1111, 123456.7890123, Double.MaxValue)
    )
    val row = encoder.toRow(Row.fromSeq(input))
    val convertedBack = encoder.fromRow(row)
    input.zipWithIndex.map { case (array, index) =>
      assert(convertedBack.getSeq(index) === array)
    }
  }

  test("RowEncoder should support array as the external type for ArrayType") {
    val schema = new StructType()
      .add("array", ArrayType(IntegerType))
      .add("nestedArray", ArrayType(ArrayType(StringType)))
      .add("deepNestedArray", ArrayType(ArrayType(ArrayType(LongType))))
    val encoder = RowEncoder(schema).resolveAndBind()
    val input = Row(
      Array(1, 2, null),
      Array(Array("abc", null), null),
      Array(Seq(Array(0L, null), null), null))
    val row = encoder.toRow(input)
    val convertedBack = encoder.fromRow(row)
    assert(convertedBack.getSeq(0) == Seq(1, 2, null))
    assert(convertedBack.getSeq(1) == Seq(Seq("abc", null), null))
    assert(convertedBack.getSeq(2) == Seq(Seq(Seq(0L, null), null), null))
  }

  test("RowEncoder should throw RuntimeException if input row object is null") {
    val schema = new StructType().add("int", IntegerType)
    val encoder = RowEncoder(schema)
    val e = intercept[RuntimeException](encoder.toRow(null))
    assert(e.getMessage.contains("Null value appeared in non-nullable field"))
    assert(e.getMessage.contains("top level row object"))
  }

  test("RowEncoder should validate external type") {
    val e1 = intercept[RuntimeException] {
      val schema = new StructType().add("a", IntegerType)
      val encoder = RowEncoder(schema)
      encoder.toRow(Row(1.toShort))
    }
    assert(e1.getMessage.contains("java.lang.Short is not a valid external type"))

    val e2 = intercept[RuntimeException] {
      val schema = new StructType().add("a", StringType)
      val encoder = RowEncoder(schema)
      encoder.toRow(Row(1))
    }
    assert(e2.getMessage.contains("java.lang.Integer is not a valid external type"))

    val e3 = intercept[RuntimeException] {
      val schema = new StructType().add("a",
        new StructType().add("b", IntegerType).add("c", StringType))
      val encoder = RowEncoder(schema)
      encoder.toRow(Row(1 -> "a"))
    }
    assert(e3.getMessage.contains("scala.Tuple2 is not a valid external type"))

    val e4 = intercept[RuntimeException] {
      val schema = new StructType().add("a", ArrayType(TimestampType))
      val encoder = RowEncoder(schema)
      encoder.toRow(Row(Array("a")))
    }
    assert(e4.getMessage.contains("java.lang.String is not a valid external type"))
  }

  private def encodeDecodeTest(schema: StructType): Unit = {
    test(s"encode/decode: ${schema.simpleString}") {
      val encoder = RowEncoder(schema).resolveAndBind()
      val inputGenerator = RandomDataGenerator.forType(schema, nullable = false).get

      var input: Row = null
      try {
        for (_ <- 1 to 5) {
          input = inputGenerator.apply().asInstanceOf[Row]
          val row = encoder.toRow(input)
          val convertedBack = encoder.fromRow(row)
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
