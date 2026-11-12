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

package org.apache.spark.sql.catalyst.expressions.ml

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, ExpressionEvalHelper, Literal, UnsafeArrayData, UnwrapUDT}
import org.apache.spark.sql.types._

class VectorToArraySuite extends SparkFunSuite with ExpressionEvalHelper {
  private val vectorSqlType = VectorScaleShift.vectorSqlType

  test("vector to array evaluates dense and sparse vectors") {
    val special = Array(-0.0, Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity)
    val vectors = Seq(
      (InternalRow(1.toByte, null, null, UnsafeArrayData.fromPrimitiveArray(special)),
        special.toSeq),
      (InternalRow(1.toByte, null, null,
        UnsafeArrayData.fromPrimitiveArray(Array.emptyDoubleArray)), Seq.empty[Double]),
      (InternalRow(0.toByte, 5, UnsafeArrayData.fromPrimitiveArray(Array(1, 4)),
        UnsafeArrayData.fromPrimitiveArray(Array(1.1, -2.5))), Seq(0.0, 1.1, 0.0, 0.0, -2.5)),
      (InternalRow(0.toByte, 3, UnsafeArrayData.fromPrimitiveArray(Array.emptyIntArray),
        UnsafeArrayData.fromPrimitiveArray(Array.emptyDoubleArray)), Seq(0.0, 0.0, 0.0)),
      (InternalRow(0.toByte, 0, UnsafeArrayData.fromPrimitiveArray(Array.emptyIntArray),
        UnsafeArrayData.fromPrimitiveArray(Array.emptyDoubleArray)), Seq.empty[Double]))

    for ((vector, expected) <- vectors; elementType <- Seq(DoubleType, FloatType)) {
      val expression = VectorToArray(
        BoundReference(0, vectorSqlType, nullable = true), elementType)
      assert(expression.checkInputDataTypes().isSuccess)
      assert(expression.dataType === ArrayType(elementType, containsNull = false))
      val result = if (elementType == FloatType) expected.map(_.toFloat) else expected
      checkEvaluation(expression, result, InternalRow(vector))
    }
  }

  test("vector to array propagates null vectors and values") {
    val vectors = Seq(null,
      InternalRow(1.toByte, null, null, null),
      InternalRow(0.toByte, 3, null, null))
    for (vector <- vectors; elementType <- Seq(DoubleType, FloatType)) {
      val expression = VectorToArray(
        BoundReference(0, vectorSqlType, nullable = true), elementType)
      assert(expression.nullable)
      checkEvaluation(expression, null, InternalRow(vector))
    }
  }

  test("vector to array accepts unwrapped vector UDTs") {
    val input = BoundReference(0, new VectorToArrayTestUDT, nullable = true)
    val vector = InternalRow(1.toByte, null, null,
      UnsafeArrayData.fromPrimitiveArray(Array(1.1, -2.5)))
    for (elementType <- Seq(DoubleType, FloatType)) {
      assert(VectorToArray(input, elementType).checkInputDataTypes().isFailure)
      val expression = VectorToArray(UnwrapUDT(input), elementType)
      assert(expression.checkInputDataTypes().isSuccess)
      val expected = if (elementType == FloatType) Seq(1.1f, -2.5f) else Seq(1.1, -2.5)
      checkEvaluation(expression, expected, InternalRow(vector))
      checkEvaluation(expression, null, InternalRow(null))
    }
  }

  test("vector to array validates vector schema nullability") {
    val nullableElements = Seq(2, 3).map { index =>
      val field = vectorSqlType(index)
      val elementType = field.dataType.asInstanceOf[ArrayType].elementType
      StructType(vectorSqlType.fields.updated(index,
        field.copy(dataType = ArrayType(elementType, containsNull = true))))
    }
    val invalidTypes = Seq(IntegerType, ArrayType(DoubleType),
      vectorSqlType.add("extra", DoubleType),
      StructType(vectorSqlType.fields.updated(0, vectorSqlType(0).copy(nullable = true))),
      StructType(vectorSqlType.fields.updated(1, StructField("size", LongType)))) ++
      nullableElements
    for (inputType <- invalidTypes; elementType <- Seq(DoubleType, FloatType)) {
      val expression = VectorToArray(
        BoundReference(0, inputType, nullable = true), elementType)
      assert(expression.checkInputDataTypes().isFailure)
    }
  }

  test("vector to array rejects unsupported output types") {
    intercept[IllegalArgumentException] {
      VectorToArray(Literal(null, vectorSqlType), IntegerType)
    }
  }
}

class VectorToArrayTestUDT extends UserDefinedType[InternalRow] {
  override def sqlType: DataType = VectorScaleShift.vectorSqlType
  override def serialize(value: InternalRow): Any = value
  override def deserialize(value: Any): InternalRow = value.asInstanceOf[InternalRow]
  override def userClass: Class[InternalRow] = classOf[InternalRow]
}
