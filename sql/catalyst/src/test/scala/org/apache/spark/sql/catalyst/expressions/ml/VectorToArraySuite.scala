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
import org.apache.spark.sql.catalyst.expressions.{BoundReference, ExpressionEvalHelper, GenericInternalRow, Literal, UnsafeArrayData, UnwrapUDT}
import org.apache.spark.sql.types._

class VectorToArraySuite extends SparkFunSuite with ExpressionEvalHelper {
  private val vectorSqlType = VectorScaleShift.vectorSqlType

  private def dense(values: Double*): InternalRow = {
    new GenericInternalRow(Array[Any](
      1.toByte, null, null, UnsafeArrayData.fromPrimitiveArray(values.toArray)))
  }

  private def sparse(size: Int, indices: Array[Int], values: Array[Double]): InternalRow = {
    new GenericInternalRow(Array[Any](
      0.toByte, size, UnsafeArrayData.fromPrimitiveArray(indices),
      UnsafeArrayData.fromPrimitiveArray(values)))
  }

  test("vector to array interpreted and code-generated evaluation") {
    val vectors = Seq(
      dense(1.1, 0.0, -2.5) -> Seq(1.1, 0.0, -2.5),
      sparse(4, Array(1, 3), Array(1.1, -2.5)) -> Seq(0.0, 1.1, 0.0, -2.5),
      dense() -> Seq.empty[Double],
      sparse(0, Array.emptyIntArray, Array.emptyDoubleArray) -> Seq.empty[Double],
      sparse(3, Array.emptyIntArray, Array.emptyDoubleArray) -> Seq(0.0, 0.0, 0.0),
      dense(Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity) ->
        Seq(Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity))

    for ((vector, expected) <- vectors; elementType <- Seq(DoubleType, FloatType)) {
      val result = if (elementType == FloatType) expected.map(_.toFloat) else expected
      val literal = VectorToArray(Literal(vector, vectorSqlType), elementType)
      assert(literal.checkInputDataTypes().isSuccess)
      assert(literal.dataType === ArrayType(elementType, containsNull = false))
      assert(!literal.nullable)
      checkEvaluation(literal, result)
      checkEvaluation(
        VectorToArray(BoundReference(0, vectorSqlType, nullable = true), elementType),
        result,
        InternalRow(vector))
    }
  }

  test("vector to array accepts UDTs and their SQL struct representation") {
    val udt = new VectorToArrayTestUDT
    val vector = dense(1.1, 2.5)
    val input = BoundReference(0, udt, nullable = true)
    for (elementType <- Seq(DoubleType, FloatType)) {
      val expected = if (elementType == FloatType) Seq(1.1f, 2.5f) else Seq(1.1, 2.5)
      for (child <- Seq(input, UnwrapUDT(input))) {
        val expression = VectorToArray(child, elementType)
        assert(expression.checkInputDataTypes().isSuccess)
        assert(expression.prettyName === "vector_to_array")
        checkEvaluation(expression, expected, InternalRow(vector))
      }
    }
  }

  test("vector to array checks input SQL type nullability") {
    val fieldNullabilityMismatches = vectorSqlType.fields.indices.map { index =>
      val field = vectorSqlType.fields(index)
      StructType(vectorSqlType.fields.updated(index, field.copy(nullable = !field.nullable)))
    }
    val elementNullabilityMismatches = Seq(2, 3).map { index =>
      val field = vectorSqlType.fields(index)
      val elementType = field.dataType.asInstanceOf[ArrayType].elementType
      StructType(vectorSqlType.fields.updated(
        index, field.copy(dataType = ArrayType(elementType, containsNull = true))))
    }
    for (invalidSqlType <- fieldNullabilityMismatches ++ elementNullabilityMismatches;
        inputType <- Seq(invalidSqlType, new VectorToArrayTestUDT {
          override def sqlType: DataType = invalidSqlType
        });
        elementType <- Seq(DoubleType, FloatType)) {
      val expression = VectorToArray(BoundReference(0, inputType, nullable = true), elementType)
      assert(expression.checkInputDataTypes().isFailure)
    }
  }

  test("vector to array rejects null inputs") {
    for (elementType <- Seq(DoubleType, FloatType)) {
      checkExceptionInExpression[IllegalArgumentException](
        VectorToArray(Literal(null, vectorSqlType), elementType),
        "function vector_to_array requires a non-null input argument")
    }
  }

  test("vector to array rejects incompatible input and output types") {
    val invalidTypes = Seq(IntegerType, ArrayType(DoubleType),
      new TestUDT.MyDenseVectorUDT,
      vectorSqlType.add("extra", DoubleType),
      StructType(vectorSqlType.fields.updated(0, StructField("type", IntegerType))))
    for (inputType <- invalidTypes) {
      assert(VectorToArray(BoundReference(0, inputType, nullable = true), DoubleType)
        .checkInputDataTypes().isFailure)
    }
    intercept[IllegalArgumentException] {
      VectorToArray(Literal(dense(1.0), vectorSqlType), IntegerType)
    }
  }

  test("vector to array rejects unknown vector tags") {
    val vector = dense(1.0)
    vector.setByte(0, 2.toByte)
    checkExceptionInExpression[IllegalArgumentException](
      VectorToArray(Literal(vector, vectorSqlType), DoubleType), "Unknown vector type 2")
  }
}

class VectorToArrayTestUDT extends UserDefinedType[InternalRow] {
  override def sqlType: DataType = VectorScaleShift.vectorSqlType
  override def serialize(value: InternalRow): Any = value
  override def deserialize(value: Any): InternalRow = value.asInstanceOf[InternalRow]
  override def userClass: Class[InternalRow] = classOf[InternalRow]
}
