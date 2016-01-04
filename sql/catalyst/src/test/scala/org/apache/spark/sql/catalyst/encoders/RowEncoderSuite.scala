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
import org.apache.spark.sql.catalyst.util.{GenericArrayData, ArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

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

  override def serialize(obj: Any): GenericArrayData = {
    obj match {
      case p: ExamplePoint =>
        val output = new Array[Any](2)
        output(0) = p.x
        output(1) = p.y
        new GenericArrayData(output)
    }
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
      .add("udt", new ExamplePointUDT, false))

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

  test(s"encode/decode: Product") {
    val schema = new StructType()
      .add("structAsProduct",
        new StructType()
          .add("int", IntegerType)
          .add("string", StringType)
          .add("double", DoubleType))

    val encoder = RowEncoder(schema)

    val input: Row = Row((100, "test", 0.123))
    val row = encoder.toRow(input)
    val convertedBack = encoder.fromRow(row)
    assert(input.getStruct(0) == convertedBack.getStruct(0))
  }

  private def encodeDecodeTest(schema: StructType): Unit = {
    test(s"encode/decode: ${schema.simpleString}") {
      val encoder = RowEncoder(schema)
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
