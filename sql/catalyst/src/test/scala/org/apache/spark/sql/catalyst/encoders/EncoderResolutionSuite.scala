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
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.types._

case class StringLongClass(a: String, b: Long)

case class StringIntClass(a: String, b: Int)

case class ComplexClass(a: Long, b: StringLongClass)

class EncoderResolutionSuite extends PlanTest {
  test("real type doesn't match encoder schema but they are compatible: product") {
    val encoder = ExpressionEncoder[StringLongClass]
    val cls = classOf[StringLongClass]

    {
      val attrs = Seq('a.string, 'b.int)
      val fromRowExpr: Expression = encoder.resolve(attrs, null).fromRowExpression
      val expected: Expression = NewInstance(
        cls,
        toExternalString('a.string) :: 'b.int.cast(LongType) :: Nil,
        false,
        ObjectType(cls))
      compareExpressions(fromRowExpr, expected)
    }

    {
      val attrs = Seq('a.int, 'b.long)
      val fromRowExpr = encoder.resolve(attrs, null).fromRowExpression
      val expected = NewInstance(
        cls,
        toExternalString('a.int.cast(StringType)) :: 'b.long :: Nil,
        false,
        ObjectType(cls))
      compareExpressions(fromRowExpr, expected)
    }
  }

  test("real type doesn't match encoder schema but they are compatible: nested product") {
    val encoder = ExpressionEncoder[ComplexClass]
    val innerCls = classOf[StringLongClass]
    val cls = classOf[ComplexClass]

    val structType = new StructType().add("a", IntegerType).add("b", LongType)
    val attrs = Seq('a.int, 'b.struct(structType))
    val fromRowExpr: Expression = encoder.resolve(attrs, null).fromRowExpression
    val expected: Expression = NewInstance(
      cls,
      Seq(
        'a.int.cast(LongType),
        If(
          'b.struct(structType).isNull,
          Literal.create(null, ObjectType(innerCls)),
          NewInstance(
            innerCls,
            Seq(
              toExternalString(
                GetStructField('b.struct(structType), 0, Some("a")).cast(StringType)),
              GetStructField('b.struct(structType), 1, Some("b"))),
            false,
            ObjectType(innerCls))
        )),
      false,
      ObjectType(cls))
    compareExpressions(fromRowExpr, expected)
  }

  test("real type doesn't match encoder schema but they are compatible: tupled encoder") {
    val encoder = ExpressionEncoder.tuple(
      ExpressionEncoder[StringLongClass],
      ExpressionEncoder[Long])
    val cls = classOf[StringLongClass]

    val structType = new StructType().add("a", StringType).add("b", ByteType)
    val attrs = Seq('a.struct(structType), 'b.int)
    val fromRowExpr: Expression = encoder.resolve(attrs, null).fromRowExpression
    val expected: Expression = NewInstance(
      classOf[Tuple2[_, _]],
      Seq(
        NewInstance(
          cls,
          Seq(
            toExternalString(GetStructField('a.struct(structType), 0, Some("a"))),
            GetStructField('a.struct(structType), 1, Some("b")).cast(LongType)),
          false,
          ObjectType(cls)),
        'b.int.cast(LongType)),
      false,
      ObjectType(classOf[Tuple2[_, _]]))
    compareExpressions(fromRowExpr, expected)
  }

  private def toExternalString(e: Expression): Expression = {
    Invoke(e, "toString", ObjectType(classOf[String]), Nil)
  }

  test("throw exception if real type is not compatible with encoder schema") {
    val msg1 = intercept[AnalysisException] {
      ExpressionEncoder[StringIntClass].resolve(Seq('a.string, 'b.long), null)
    }.message
    assert(msg1 ==
      s"""
         |Cannot up cast `b` from bigint to int as it may truncate
         |The type path of the target object is:
         |- field (class: "scala.Int", name: "b")
         |- root class: "org.apache.spark.sql.catalyst.encoders.StringIntClass"
         |You can either add an explicit cast to the input data or choose a higher precision type
       """.stripMargin.trim + " of the field in the target object")

    val msg2 = intercept[AnalysisException] {
      val structType = new StructType().add("a", StringType).add("b", DecimalType.SYSTEM_DEFAULT)
      ExpressionEncoder[ComplexClass].resolve(Seq('a.long, 'b.struct(structType)), null)
    }.message
    assert(msg2 ==
      s"""
         |Cannot up cast `b.b` from decimal(38,18) to bigint as it may truncate
         |The type path of the target object is:
         |- field (class: "scala.Long", name: "b")
         |- field (class: "org.apache.spark.sql.catalyst.encoders.StringLongClass", name: "b")
         |- root class: "org.apache.spark.sql.catalyst.encoders.ComplexClass"
         |You can either add an explicit cast to the input data or choose a higher precision type
       """.stripMargin.trim + " of the field in the target object")
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
      to.resolve(from.schema.toAttributes, null)
    }
  }

  private def castFail[T: TypeTag, U: TypeTag]: Unit = {
    val from = ExpressionEncoder[T]
    val to = ExpressionEncoder[U]
    val catalystType = from.schema.head.dataType.simpleString
    test(s"cast from $catalystType to ${implicitly[TypeTag[U]].tpe} should fail") {
      intercept[AnalysisException](to.resolve(from.schema.toAttributes, null))
    }
  }
}
