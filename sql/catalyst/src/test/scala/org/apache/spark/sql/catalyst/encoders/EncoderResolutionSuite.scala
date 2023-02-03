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

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class StringLongClass(a: String, b: Long)

case class StringIntClass(a: String, b: Int)

case class ComplexClass(a: Long, b: StringLongClass)

case class PrimitiveArrayClass(arr: Array[Long])

case class ArrayClass(arr: Seq[StringIntClass])

case class NestedArrayClass(nestedArr: Array[ArrayClass])

class EncoderResolutionSuite extends PlanTest {
  private val str = UTF8String.fromString("hello")

  def testFromRow[T](
      encoder: ExpressionEncoder[T],
      attributes: Seq[Attribute],
      row: InternalRow): Unit = {
    encoder.resolveAndBind(attributes).createDeserializer().apply(row)
  }

  test("real type doesn't match encoder schema but they are compatible: product") {
    val encoder = ExpressionEncoder[StringLongClass]

    // int type can be up cast to long type
    val attrs1 = Seq($"a".string, $"b".int)
    testFromRow(encoder, attrs1, InternalRow(str, 1))

    // int type can be up cast to string type
    val attrs2 = Seq($"a".int, $"b".long)
    testFromRow(encoder, attrs2, InternalRow(1, 2L))
  }

  test("real type doesn't match encoder schema but they are compatible: nested product") {
    val encoder = ExpressionEncoder[ComplexClass]
    val attrs = Seq($"a".int, $"b".struct($"a".int, $"b".long))
    testFromRow(encoder, attrs, InternalRow(1, InternalRow(2, 3L)))
  }

  test("real type doesn't match encoder schema but they are compatible: tupled encoder") {
    val encoder = ExpressionEncoder.tuple(
      ExpressionEncoder[StringLongClass],
      ExpressionEncoder[Long])
    val attrs = Seq($"a".struct($"a".string, $"b".byte), $"b".int)
    testFromRow(encoder, attrs, InternalRow(InternalRow(str, 1.toByte), 2))
  }

  test("real type doesn't match encoder schema but they are compatible: primitive array") {
    val encoder = ExpressionEncoder[PrimitiveArrayClass]
    val attrs = Seq($"arr".array(IntegerType))
    val array = new GenericArrayData(Array(1, 2, 3))
    testFromRow(encoder, attrs, InternalRow(array))
  }

  test("the real type is not compatible with encoder schema: primitive array") {
    val encoder = ExpressionEncoder[PrimitiveArrayClass]
    val attrs = Seq($"arr".array(StringType))
    checkError(
      exception = intercept[AnalysisException](encoder.resolveAndBind(attrs)),
      errorClass = "CANNOT_UP_CAST_DATATYPE",
      parameters = Map("expression" -> "array element",
        "sourceType" -> "\"STRING\"", "targetType" -> "\"BIGINT\"",
        "details" -> (
          s"""
          |The type path of the target object is:
          |- array element class: "long"
          |- field (class: "[J", name: "arr")
          |- root class: "org.apache.spark.sql.catalyst.encoders.PrimitiveArrayClass"
          |You can either add an explicit cast to the input data or choose a higher precision type
          """.stripMargin.trim + " of the field in the target object")))
  }

  test("real type doesn't match encoder schema but they are compatible: array") {
    val encoder = ExpressionEncoder[ArrayClass]
    val attrs = Seq($"arr".array(new StructType().add("a", "int").add("b", "int").add("c", "int")))
    val array = new GenericArrayData(Array(InternalRow(1, 2, 3)))
    testFromRow(encoder, attrs, InternalRow(array))
  }

  test("real type doesn't match encoder schema but they are compatible: nested array") {
    val encoder = ExpressionEncoder[NestedArrayClass]
    val et = new StructType().add("arr", ArrayType(
      new StructType().add("a", "int").add("b", "int").add("c", "int")))
    val attrs = Seq($"nestedArr".array(et))
    val innerArr = new GenericArrayData(Array(InternalRow(1, 2, 3)))
    val outerArr = new GenericArrayData(Array(InternalRow(innerArr)))
    testFromRow(encoder, attrs, InternalRow(outerArr))
  }

  test("the real type is not compatible with encoder schema: non-array field") {
    val encoder = ExpressionEncoder[ArrayClass]
    val attrs = Seq($"arr".int)
    checkError(
      exception = intercept[AnalysisException](encoder.resolveAndBind(attrs)),
      errorClass = "UNSUPPORTED_DESERIALIZER.DATA_TYPE_MISMATCH",
      parameters = Map("desiredType" -> "\"ARRAY\"", "dataType" -> "\"INT\""))
  }

  test("the real type is not compatible with encoder schema: array element type") {
    val encoder = ExpressionEncoder[ArrayClass]
    val attrs = Seq($"arr".array(new StructType().add("c", "int")))
    checkError(
      exception = intercept[AnalysisException](encoder.resolveAndBind(attrs)),
      errorClass = "FIELD_NOT_FOUND",
      parameters = Map("fieldName" -> "`a`", "fields" -> "`c`"))
  }

  test("the real type is not compatible with encoder schema: nested array element type") {
    val encoder = ExpressionEncoder[NestedArrayClass]

    withClue("inner element is not array") {
      val attrs = Seq($"nestedArr".array(new StructType().add("arr", "int")))
      checkError(
        exception = intercept[AnalysisException](encoder.resolveAndBind(attrs)),
        errorClass = "UNSUPPORTED_DESERIALIZER.DATA_TYPE_MISMATCH",
        parameters = Map("desiredType" -> "\"ARRAY\"", "dataType" -> "\"INT\""))
    }

    withClue("nested array element type is not compatible") {
      val attrs = Seq($"nestedArr".array(new StructType()
        .add("arr", ArrayType(new StructType().add("c", "int")))))
      checkError(
        exception = intercept[AnalysisException](encoder.resolveAndBind(attrs)),
        errorClass = "FIELD_NOT_FOUND",
        parameters = Map("fieldName" -> "`a`", "fields" -> "`c`"))
    }
  }

  test("nullability of array type element should not fail analysis") {
    val encoder = ExpressionEncoder[Seq[Int]]
    val attrs = $"a".array(IntegerType) :: Nil

    // It should pass analysis
    val fromRow = encoder.resolveAndBind(attrs).createDeserializer()

    // If no null values appear, it should work fine
    fromRow(InternalRow(new GenericArrayData(Array(1, 2))))

    // If there is null value, it should throw runtime exception
    val e = intercept[RuntimeException] {
      fromRow(InternalRow(new GenericArrayData(Array(1, null))))
    }
    assert(e.getMessage.contains("Null value appeared in non-nullable field"))
  }

  test("the real number of fields doesn't match encoder schema: tuple encoder") {
    val encoder = ExpressionEncoder[(String, Long)]

    {
      val attrs = Seq($"a".string, $"b".long, $"c".int)
      checkError(
        exception = intercept[AnalysisException](encoder.resolveAndBind(attrs)),
        errorClass = "UNSUPPORTED_DESERIALIZER.FIELD_NUMBER_MISMATCH",
        parameters = Map("schema" -> "\"STRUCT<a: STRING, b: BIGINT, c: INT>\"",
          "ordinal" -> "2"))
    }

    {
      val attrs = Seq($"a".string)
      checkError(
        exception = intercept[AnalysisException](encoder.resolveAndBind(attrs)),
        errorClass = "UNSUPPORTED_DESERIALIZER.FIELD_NUMBER_MISMATCH",
        parameters = Map("schema" -> "\"STRUCT<a: STRING>\"",
          "ordinal" -> "2"))
    }
  }

  test("the real number of fields doesn't match encoder schema: nested tuple encoder") {
    val encoder = ExpressionEncoder[(String, (Long, String))]

    {
      val attrs = Seq($"a".string, $"b".struct($"x".long, $"y".string, $"z".int))
      checkError(
        exception = intercept[AnalysisException](encoder.resolveAndBind(attrs)),
        errorClass = "UNSUPPORTED_DESERIALIZER.FIELD_NUMBER_MISMATCH",
        parameters = Map("schema" -> "\"STRUCT<x: BIGINT, y: STRING, z: INT>\"",
          "ordinal" -> "2"))
    }

    {
      val attrs = Seq($"a".string, $"b".struct($"x".long))
      checkError(
        exception = intercept[AnalysisException](encoder.resolveAndBind(attrs)),
        errorClass = "UNSUPPORTED_DESERIALIZER.FIELD_NUMBER_MISMATCH",
        parameters = Map("schema" -> "\"STRUCT<x: BIGINT>\"",
          "ordinal" -> "2"))
    }
  }

  test("nested case class can have different number of fields from the real schema") {
    val encoder = ExpressionEncoder[(String, StringIntClass)]
    val attrs = Seq($"a".string, $"b".struct($"a".string, $"b".int, $"c".int))
    encoder.resolveAndBind(attrs)
  }

  test("SPARK-28497: complex type is not compatible with string encoder schema") {
    val encoder = ExpressionEncoder[String]

    Seq($"a".struct($"x".long), $"a".array(StringType), Symbol("a").map(StringType, StringType))
      .foreach { attr =>
        val attrs = Seq(attr)
        checkError(exception = intercept[AnalysisException](encoder.resolveAndBind(attrs)),
          errorClass = "CANNOT_UP_CAST_DATATYPE",
          parameters = Map("expression" -> "a",
            "sourceType" -> ("\"" + attr.dataType.sql + "\""), "targetType" -> "\"STRING\"",
            "details" -> (
          s"""
          |The type path of the target object is:
          |- root class: "java.lang.String"
          |You can either add an explicit cast to the input data or choose a higher precision type
          """.stripMargin.trim + " of the field in the target object")))
    }
  }

  test("throw exception if real type is not compatible with encoder schema") {
    val e1 = intercept[AnalysisException] {
      ExpressionEncoder[StringIntClass].resolveAndBind(Seq($"a".string, $"b".long))
    }
    checkError(exception = e1,
      errorClass = "CANNOT_UP_CAST_DATATYPE",
      parameters = Map("expression" -> "b",
        "sourceType" -> ("\"BIGINT\""), "targetType" -> "\"INT\"",
        "details" -> (
          s"""
          |The type path of the target object is:
          |- field (class: "int", name: "b")
          |- root class: "org.apache.spark.sql.catalyst.encoders.StringIntClass"
          |You can either add an explicit cast to the input data or choose a higher precision type
          """.stripMargin.trim + " of the field in the target object")))

    val e2 = intercept[AnalysisException] {
      val structType = new StructType().add("a", StringType).add("b", DecimalType.SYSTEM_DEFAULT)
      ExpressionEncoder[ComplexClass].resolveAndBind(Seq($"a".long, $"b".struct(structType)))
    }

    checkError(exception = e2,
      errorClass = "CANNOT_UP_CAST_DATATYPE",
      parameters = Map("expression" -> "b.`b`",
        "sourceType" -> ("\"DECIMAL(38,18)\""), "targetType" -> "\"BIGINT\"",
        "details" -> (
          s"""
          |The type path of the target object is:
          |- field (class: "long", name: "b")
          |- field (class: "org.apache.spark.sql.catalyst.encoders.StringLongClass", name: "b")
          |- root class: "org.apache.spark.sql.catalyst.encoders.ComplexClass"
          |You can either add an explicit cast to the input data or choose a higher precision type
          """.stripMargin.trim + " of the field in the target object")))
  }

  test("SPARK-31750: eliminate UpCast if child's dataType is DecimalType") {
    val encoder = ExpressionEncoder[Seq[BigDecimal]]
    val attr = Seq(AttributeReference("a", ArrayType(DecimalType(38, 0)))())
    // Before SPARK-31750, it will fail because Decimal(38, 0) can not be casted to Decimal(38, 18)
    testFromRow(encoder, attr, InternalRow(ArrayData.toArrayData(Array(Decimal(1.0)))))
  }

  // test for leaf types
  castSuccess[Int, Long]
  castSuccess[java.sql.Date, java.sql.Timestamp]
  castSuccess[Long, String]
  castSuccess[Int, java.math.BigDecimal]
  castSuccess[Long, java.math.BigDecimal]

  castFail[Long, Int]
  castFail[java.sql.Timestamp, java.sql.Date]
  castFail[java.math.BigDecimal, Double]
  castFail[Double, java.math.BigDecimal]
  castFail[java.math.BigDecimal, Int]
  castFail[String, Long]


  private def castSuccess[T: TypeTag, U: TypeTag]: Unit = {
    val from = ExpressionEncoder[T]
    val to = ExpressionEncoder[U]
    val catalystType = from.schema.head.dataType.simpleString
    test(s"cast from $catalystType to ${implicitly[TypeTag[U]].tpe} should success") {
      to.resolveAndBind(from.schema.toAttributes)
    }
  }

  private def castFail[T: TypeTag, U: TypeTag]: Unit = {
    val from = ExpressionEncoder[T]
    val to = ExpressionEncoder[U]
    val catalystType = from.schema.head.dataType.simpleString
    test(s"cast from $catalystType to ${implicitly[TypeTag[U]].tpe} should fail") {
      intercept[AnalysisException](to.resolveAndBind(from.schema.toAttributes))
    }
  }
}
