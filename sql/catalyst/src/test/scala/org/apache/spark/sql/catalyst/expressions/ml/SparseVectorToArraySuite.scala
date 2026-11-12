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
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

class SparseVectorToArraySuite extends SparkFunSuite with ExpressionEvalHelper {
  private val vectorSqlType = VectorScaleShift.vectorSqlType

  private def sparseRow(size: Int, indices: Array[Int], values: Array[Double]): InternalRow = {
    new GenericInternalRow(Array[Any](
      0.toByte,
      size,
      UnsafeArrayData.fromPrimitiveArray(indices),
      UnsafeArrayData.fromPrimitiveArray(values)))
  }

  private def sparseToArray(
      size: Int,
      indices: Array[Int],
      values: Array[Double],
      elementType: DataType): SparseVectorToArray = {
    SparseVectorToArray(Literal(sparseRow(size, indices, values), vectorSqlType), elementType)
  }

  test("sparse vector to array interpreted and code-generated evaluation") {
    val vectors = Seq(
      (0, Array.emptyIntArray, Array.emptyDoubleArray, Seq.empty[Double]),
      (4, Array.emptyIntArray, Array.emptyDoubleArray, Seq(0.0, 0.0, 0.0, 0.0)),
      (5, Array(1, 4), Array(1.1, -2.5), Seq(0.0, 1.1, 0.0, 0.0, -2.5)),
      (3, Array(0, 1, 2), Array(Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity),
        Seq(Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity)))

    for ((size, indices, values, expected) <- vectors;
        elementType <- Seq(DoubleType, FloatType)) {
      val expression = sparseToArray(size, indices, values, elementType)
      assert(expression.checkInputDataTypes().isSuccess)
      assert(expression.prettyName === "sparse_vector_to_array")
      assert(expression.dataType === ArrayType(elementType, containsNull = false))
      val result = if (elementType == FloatType) expected.map(_.toFloat) else expected
      checkEvaluation(expression, result)
      checkEvaluation(
        SparseVectorToArray(BoundReference(0, vectorSqlType, nullable = true), elementType),
        result,
        InternalRow(sparseRow(size, indices, values)))
    }
  }

  test("sparse vector to array accepts unwrapped vector UDT inputs") {
    val input = BoundReference(0, new SparseVectorToArrayTestUDT, nullable = true)
    val vector = sparseRow(4, Array(1, 3), Array(1.1, -2.5))
    for (elementType <- Seq(DoubleType, FloatType)) {
      assert(SparseVectorToArray(input, elementType).checkInputDataTypes().isFailure)
      val expression = SparseVectorToArray(UnwrapUDT(input), elementType)
      assert(expression.checkInputDataTypes().isSuccess)
      val expected = if (elementType == FloatType) {
        Seq(0.0f, 1.1f, 0.0f, -2.5f)
      } else {
        Seq(0.0, 1.1, 0.0, -2.5)
      }
      checkEvaluation(expression, expected, InternalRow(vector))
    }
  }

  test("sparse vector to array propagates null structs and sparse fields") {
    val nullFields = Seq(1, 2, 3).map { field =>
      val row = sparseRow(3, Array(1), Array(2.5))
      row.setNullAt(field)
      Literal(row, vectorSqlType)
    }
    for (input <- Literal(null, vectorSqlType) +: nullFields;
        elementType <- Seq(DoubleType, FloatType)) {
      val expression = SparseVectorToArray(input, elementType)
      assert(expression.checkInputDataTypes().isSuccess)
      assert(expression.nullable)
      checkEvaluation(expression, null)
    }
  }

  test("sparse vector to array requires the vector SQL schema including nullability") {
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
    val invalidTypes = Seq(IntegerType, ArrayType(DoubleType),
      vectorSqlType.add("extra", DoubleType),
      StructType(vectorSqlType.fields.updated(1, StructField("size", LongType)))) ++
      fieldNullabilityMismatches ++ elementNullabilityMismatches
    for (inputType <- invalidTypes; elementType <- Seq(DoubleType, FloatType)) {
      val expression = SparseVectorToArray(
        BoundReference(0, inputType, nullable = true), elementType)
      assert(expression.checkInputDataTypes().isFailure)
    }
  }

  test("sparse vector to array preserves signed zero") {
    checkEvaluation(sparseToArray(3, Array(1), Array(-0.0), DoubleType), Seq(0.0, -0.0, 0.0))
    checkEvaluation(sparseToArray(3, Array(1), Array(-0.0), FloatType), Seq(0.0f, -0.0f, 0.0f))
    val doubles = sparseToArray(3, Array(1), Array(-0.0), DoubleType)
      .eval().asInstanceOf[ArrayData]
    val floats = sparseToArray(3, Array(1), Array(-0.0), FloatType)
      .eval().asInstanceOf[ArrayData]
    assert(java.lang.Double.doubleToRawLongBits(doubles.getDouble(1)) === Long.MinValue)
    assert(java.lang.Float.floatToRawIntBits(floats.getFloat(1)) === Int.MinValue)
  }

  test("sparse vector to array rejects unsupported output types") {
    intercept[IllegalArgumentException] {
      sparseToArray(1, Array(0), Array(1.0), IntegerType)
    }
  }
}

class SparseVectorToArrayTestUDT extends UserDefinedType[InternalRow] {
  override def sqlType: DataType = VectorScaleShift.vectorSqlType
  override def serialize(value: InternalRow): Any = value
  override def deserialize(value: Any): InternalRow = value.asInstanceOf[InternalRow]
  override def userClass: Class[InternalRow] = classOf[InternalRow]
}
