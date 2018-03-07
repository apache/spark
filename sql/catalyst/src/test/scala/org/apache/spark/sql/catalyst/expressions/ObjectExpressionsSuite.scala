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

package org.apache.spark.sql.catalyst.expressions

import scala.collection.JavaConverters._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.objects._
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types._


class ObjectExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("SPARK-16622: The returned value of the called method in Invoke can be null") {
    val inputRow = InternalRow.fromSeq(Seq((false, null)))
    val cls = classOf[Tuple2[Boolean, java.lang.Integer]]
    val inputObject = BoundReference(0, ObjectType(cls), nullable = true)
    val invoke = Invoke(inputObject, "_2", IntegerType)
    checkEvaluationWithGeneratedMutableProjection(invoke, null, inputRow)
  }

  test("MapObjects should make copies of unsafe-backed data") {
    // test UnsafeRow-backed data
    val structEncoder = ExpressionEncoder[Array[Tuple2[java.lang.Integer, java.lang.Integer]]]
    val structInputRow = InternalRow.fromSeq(Seq(Array((1, 2), (3, 4))))
    val structExpected = new GenericArrayData(
      Array(InternalRow.fromSeq(Seq(1, 2)), InternalRow.fromSeq(Seq(3, 4))))
    checkEvalutionWithUnsafeProjection(
      structEncoder.serializer.head, structExpected, structInputRow)

    // test UnsafeArray-backed data
    val arrayEncoder = ExpressionEncoder[Array[Array[Int]]]
    val arrayInputRow = InternalRow.fromSeq(Seq(Array(Array(1, 2), Array(3, 4))))
    val arrayExpected = new GenericArrayData(
      Array(new GenericArrayData(Array(1, 2)), new GenericArrayData(Array(3, 4))))
    checkEvalutionWithUnsafeProjection(
      arrayEncoder.serializer.head, arrayExpected, arrayInputRow)

    // test UnsafeMap-backed data
    val mapEncoder = ExpressionEncoder[Array[Map[Int, Int]]]
    val mapInputRow = InternalRow.fromSeq(Seq(Array(
      Map(1 -> 100, 2 -> 200), Map(3 -> 300, 4 -> 400))))
    val mapExpected = new GenericArrayData(Seq(
      new ArrayBasedMapData(
        new GenericArrayData(Array(1, 2)),
        new GenericArrayData(Array(100, 200))),
      new ArrayBasedMapData(
        new GenericArrayData(Array(3, 4)),
        new GenericArrayData(Array(300, 400)))))
    checkEvalutionWithUnsafeProjection(
      mapEncoder.serializer.head, mapExpected, mapInputRow)
  }

  test("SPARK-23585: UnwrapOption should support interpreted execution") {
    val cls = classOf[Option[Int]]
    val inputObject = BoundReference(0, ObjectType(cls), nullable = true)
    val unwrapObject = UnwrapOption(IntegerType, inputObject)
    Seq((Some(1), 1), (None, null), (null, null)).foreach { case (input, expected) =>
      checkEvaluation(unwrapObject, expected, InternalRow.fromSeq(Seq(input)))
    }
  }

  test("SPARK-23586: WrapOption should support interpreted execution") {
    val cls = ObjectType(classOf[java.lang.Integer])
    val inputObject = BoundReference(0, cls, nullable = true)
    val wrapObject = WrapOption(inputObject, cls)
    Seq((1, Some(1)), (null, None)).foreach { case (input, expected) =>
      checkEvaluation(wrapObject, expected, InternalRow.fromSeq(Seq(input)))
    }
  }

  test("SPARK-23590: CreateExternalRow should support interpreted execution") {
    val schema = new StructType().add("a", IntegerType).add("b", StringType)
    val createExternalRow = CreateExternalRow(Seq(Literal(1), Literal("x")), schema)
    checkEvaluation(createExternalRow, Row.fromSeq(Seq(1, "x")), InternalRow.fromSeq(Seq()))
  }

  test("SPARK-23594 GetExternalRowField should support interpreted execution") {
    val inputObject = BoundReference(0, ObjectType(classOf[Row]), nullable = true)
    val getRowField = GetExternalRowField(inputObject, index = 0, fieldName = "c0")
    Seq((Row(1), 1), (Row(3), 3)).foreach { case (input, expected) =>
      checkEvaluation(getRowField, expected, InternalRow.fromSeq(Seq(input)))
    }

    // If an input row or a field are null, a runtime exception will be thrown
    val errMsg1 = intercept[RuntimeException] {
      evaluate(getRowField, InternalRow.fromSeq(Seq(null)))
    }.getMessage
    assert(errMsg1 === "The input external row cannot be null.")

    val errMsg2 = intercept[RuntimeException] {
      evaluate(getRowField, InternalRow.fromSeq(Seq(Row(null))))
    }.getMessage
    assert(errMsg2 === "The 0th field 'c0' of input row cannot be null.")
  }

  test("SPARK-23587: MapObjects should support interpreted execution") {
    val customCollectionClasses = Seq(classOf[Seq[_]], classOf[scala.collection.Set[_]],
      classOf[java.util.List[_]], classOf[java.util.AbstractList[_]],
      classOf[java.util.AbstractSequentialList[_]], null)
    val function = (lambda: Expression) => Add(lambda, Literal(1))
    val elementType = IntegerType
    val expected = Seq(2, 3, 4)

    val list = new java.util.ArrayList[Int]()
    list.add(1)
    list.add(2)
    list.add(3)
    val arrayData = new GenericArrayData(Array(1, 2, 3))
    val vector = new java.util.Vector[Int]()
    vector.add(1)
    vector.add(2)
    vector.add(3)

    Seq(
      (Seq(1, 2, 3), ObjectType(classOf[Seq[Int]])),
      (list, ObjectType(classOf[java.util.List[Int]])),
      (vector, ObjectType(classOf[java.util.Vector[Int]])),
      (arrayData, ArrayType(IntegerType))
    ).foreach { case (collection, inputType) =>
      val inputObject = BoundReference(0, inputType, nullable = true)

      customCollectionClasses.foreach { customCollectionCls =>
        val optClass = Option(customCollectionCls)
        val mapObj = MapObjects(function, inputObject, elementType, true, optClass)
        val row = InternalRow.fromSeq(Seq(collection))
        val result = mapObj.eval(row)

        customCollectionCls match {
          case null =>
          case l if l.isAssignableFrom(classOf[java.util.AbstractList[_]]) =>
            assert(result.asInstanceOf[java.util.List[_]].asScala.toSeq == expected.toSeq)
          case l if l.isAssignableFrom(classOf[java.util.AbstractSequentialList[_]]) =>
            assert(result.asInstanceOf[java.util.List[_]].asScala.toSeq == expected.toSeq)
          case s if s.isAssignableFrom(classOf[Seq[_]]) =>
            assert(result.asInstanceOf[Seq[_]].toSeq == expected.toSeq)
          case s if s.isAssignableFrom(classOf[scala.collection.Set[_]]) =>
            assert(result.asInstanceOf[scala.collection.Set[_]] == expected.toSet)
        }
        optClass.foreach(_.isAssignableFrom(result.getClass))
      }
    }
  }
}
