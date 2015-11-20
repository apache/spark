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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.types._

case class StringLongClass(a: String, b: Long)

case class ComplexClass(a: Long, b: StringLongClass)

class EncoderResolveSuite extends PlanTest {
  test("real type doesn't match encoder schema but they are compatible: product") {
    val encoder = ExpressionEncoder[StringLongClass]
    val cls = classOf[StringLongClass]

    var attrs = Seq('a.string, 'b.int)
    var fromRowExpr: Expression = encoder.resolve(attrs, null).fromRowExpression
    var expected: Expression = NewInstance(
      cls,
      toExternalString('a.string) :: 'b.int.cast(LongType) :: Nil,
      false,
      ObjectType(cls))
    compareExpressions(fromRowExpr, expected)

    attrs = Seq('a.int, 'b.long)
    fromRowExpr = encoder.resolve(attrs, null).fromRowExpression
    expected = NewInstance(
      cls,
      toExternalString('a.int.cast(StringType)) :: 'b.long :: Nil,
      false,
      ObjectType(cls))
    compareExpressions(fromRowExpr, expected)
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
              toExternalString(GetStructField(
                'b.struct(structType),
                structType(0),
                0).cast(StringType)),
              GetStructField(
                'b.struct(structType),
                structType(1),
                1)),
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

    val structType = new StructType().add("a", StringType).add("b", ByteType, false)
    val attrs = Seq('a.struct(structType), 'b.int)
    val fromRowExpr: Expression = encoder.resolve(attrs, null).fromRowExpression
    val expected: Expression = NewInstance(
      classOf[Tuple2[_, _]],
      Seq(
        NewInstance(
          cls,
          Seq(
            toExternalString(GetStructField(
              'a.struct(structType),
              structType(0),
              0)),
            GetStructField(
              'a.struct(structType),
              structType(1),
              1).cast(LongType)),
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
}
