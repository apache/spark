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
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class StringLongClass(a: String, b: Long)

case class StringIntClass(a: String, b: Int)

case class ComplexClass(a: Long, b: StringLongClass)

class EncoderResolutionSuite extends PlanTest {
  private val str = UTF8String.fromString("hello")

  test("real type doesn't match encoder schema but they are compatible: product") {
    val encoder = ExpressionEncoder[StringLongClass]

    // int type can be up cast to long type
    val attrs1 = Seq('a.string, 'b.int)
    encoder.resolve(attrs1, null).bind(attrs1).fromRow(InternalRow(str, 1))

    // int type can be up cast to string type
    val attrs2 = Seq('a.int, 'b.long)
    encoder.resolve(attrs2, null).bind(attrs2).fromRow(InternalRow(1, 2L))
  }

  test("real type doesn't match encoder schema but they are compatible: nested product") {
    val encoder = ExpressionEncoder[ComplexClass]
    val attrs = Seq('a.int, 'b.struct('a.int, 'b.long))
    encoder.resolve(attrs, null).bind(attrs).fromRow(InternalRow(1, InternalRow(2, 3L)))
  }

  test("real type doesn't match encoder schema but they are compatible: tupled encoder") {
    val encoder = ExpressionEncoder.tuple(
      ExpressionEncoder[StringLongClass],
      ExpressionEncoder[Long])
    val attrs = Seq('a.struct('a.string, 'b.byte), 'b.int)
    encoder.resolve(attrs, null).bind(attrs).fromRow(InternalRow(InternalRow(str, 1.toByte), 2))
  }

  test("nullability of array type element should not fail analysis") {
    val encoder = ExpressionEncoder[Seq[Int]]
    val attrs = 'a.array(IntegerType) :: Nil

    // It should pass analysis
    val bound = encoder.resolve(attrs, null).bind(attrs)

    // If no null values appear, it should works fine
    bound.fromRow(InternalRow(new GenericArrayData(Array(1, 2))))

    // If there is null value, it should throw runtime exception
    val e = intercept[RuntimeException] {
      bound.fromRow(InternalRow(new GenericArrayData(Array(1, null))))
    }
    assert(e.getMessage.contains("Null value appeared in non-nullable field"))
  }

  test("the real number of fields doesn't match encoder schema: tuple encoder") {
    val encoder = ExpressionEncoder[(String, Long)]

    {
      val attrs = Seq('a.string, 'b.long, 'c.int)
      assert(intercept[AnalysisException](encoder.resolve(attrs, null)).message ==
        "Try to map struct<a:string,b:bigint,c:int> to Tuple2, " +
          "but failed as the number of fields does not line up.\n" +
          " - Input schema: struct<a:string,b:bigint,c:int>\n" +
          " - Target schema: struct<_1:string,_2:bigint>")
    }

    {
      val attrs = Seq('a.string)
      assert(intercept[AnalysisException](encoder.resolve(attrs, null)).message ==
        "Try to map struct<a:string> to Tuple2, " +
          "but failed as the number of fields does not line up.\n" +
          " - Input schema: struct<a:string>\n" +
          " - Target schema: struct<_1:string,_2:bigint>")
    }
  }

  test("the real number of fields doesn't match encoder schema: nested tuple encoder") {
    val encoder = ExpressionEncoder[(String, (Long, String))]

    {
      val attrs = Seq('a.string, 'b.struct('x.long, 'y.string, 'z.int))
      assert(intercept[AnalysisException](encoder.resolve(attrs, null)).message ==
        "Try to map struct<x:bigint,y:string,z:int> to Tuple2, " +
          "but failed as the number of fields does not line up.\n" +
          " - Input schema: struct<a:string,b:struct<x:bigint,y:string,z:int>>\n" +
          " - Target schema: struct<_1:string,_2:struct<_1:bigint,_2:string>>")
    }

    {
      val attrs = Seq('a.string, 'b.struct('x.long))
      assert(intercept[AnalysisException](encoder.resolve(attrs, null)).message ==
        "Try to map struct<x:bigint> to Tuple2, " +
          "but failed as the number of fields does not line up.\n" +
          " - Input schema: struct<a:string,b:struct<x:bigint>>\n" +
          " - Target schema: struct<_1:string,_2:struct<_1:bigint,_2:string>>")
    }
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
