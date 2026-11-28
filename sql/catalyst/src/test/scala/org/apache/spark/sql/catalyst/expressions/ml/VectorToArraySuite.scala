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
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, FunctionIdentifier, InternalRow}
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.{BoundReference, EmptyRow, Expression, ExpressionEvalHelper, LeafExpression, Literal, Nondeterministic, UnsafeArrayData, UnsafeProjection, UnwrapUDT, With}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.internal.SQLConf
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
      assert(VectorToArray(input, elementType).checkInputDataTypes().isSuccess)
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
      val udt = new VectorToArrayTestUDT {
        override def sqlType: DataType = inputType
      }
      for (dataType <- Seq(inputType, udt)) {
        val expression = VectorToArray(
          BoundReference(0, dataType, nullable = true), elementType)
        assert(expression.checkInputDataTypes().isFailure)
      }
    }
  }

  test("vector to array is registered only as an internal function") {
    val name = FunctionIdentifier("vector_to_array")
    val input = BoundReference(0, new VectorToArrayTestUDT, nullable = true)
    val vector = InternalRow(1.toByte, null, null,
      UnsafeArrayData.fromPrimitiveArray(Array(1.1, -2.5)))
    assert(!FunctionRegistry.builtin.functionExists(name))
    for ((dtype, elementType) <- Seq("float64" -> DoubleType, "float32" -> FloatType)) {
      val expression = FunctionRegistry.internal.lookupFunction(name, Seq(input, Literal(dtype)))
        .asInstanceOf[VectorToArray]
      assert(expression.child === input)
      assert(expression.dataType === ArrayType(elementType, containsNull = false))
      val expected = if (elementType == FloatType) Seq(1.1f, -2.5f) else Seq(1.1, -2.5)
      checkEvaluation(expression, expected, InternalRow(vector))
    }
  }

  test("internal vector to array validates dtype and argument count") {
    val name = FunctionIdentifier("vector_to_array")
    val input = BoundReference(0, new VectorToArrayTestUDT, nullable = true)
    for (arguments <- Seq(Seq.empty, Seq(input), Seq(input, Literal("float64"), Literal(1)))) {
      val error = intercept[AnalysisException] {
        FunctionRegistry.internal.lookupFunction(name, arguments)
      }
      assert(error.getCondition === "WRONG_NUM_ARGS.WITHOUT_SUGGESTION")
    }
    for ((dtype, condition) <- Seq(
        Literal("float16") -> "INVALID_PARAMETER_VALUE.DTYPE",
        Literal(1) -> "INVALID_PARAMETER_VALUE.STRING",
        BoundReference(1, StringType, nullable = false) ->
          "INVALID_PARAMETER_VALUE.STRING")) {
      val error = intercept[AnalysisException] {
        FunctionRegistry.internal.lookupFunction(name, Seq(input, dtype))
      }
      assert(error.getCondition === condition)
    }
  }

  test("vector to array evaluates nondeterministic inputs once") {
    for (elementType <- Seq(DoubleType, FloatType);
        inputType <- Seq(vectorSqlType, new VectorToArrayTestUDT)) {
      withSQLConf(SQLConf.ALWAYS_INLINE_COMMON_EXPR.key -> "false") {
        def expression: Expression =
          VectorToArray(AlternatingVector(inputType), elementType).replacement
        val expected = if (elementType == FloatType) Seq(1.0f, 2.0f, 3.0f)
          else Seq(1.0, 2.0, 3.0)
        val catalystExpected = CatalystTypeConverters.convertToCatalyst(expected)
        checkEvaluationWithoutCodegen(expression, catalystExpected)
        checkEvaluationWithMutableProjection(expression, catalystExpected)
        val projection = UnsafeProjection.create(Seq(expression))
        projection.initialize(0)
        assert(projection(EmptyRow).getArray(0).toSeq[Any](elementType) === expected)
      }
    }
  }

  test("vector to array honors the common expression inline setting") {
    val vector = InternalRow(1.toByte, null, null,
      UnsafeArrayData.fromPrimitiveArray(Array(1.0, 2.0)))
    for (inline <- Seq(true, false)) {
      withSQLConf(SQLConf.ALWAYS_INLINE_COMMON_EXPR.key -> inline.toString) {
        val expression = VectorToArray(Literal(vector, vectorSqlType), DoubleType)
        assert(expression.replacement.exists(_.isInstanceOf[With]) === !inline)
        checkEvaluation(expression, Seq(1.0, 2.0))
      }
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

case class AlternatingVector(override val dataType: DataType)
  extends LeafExpression with Nondeterministic with CodegenFallback {
  private var calls = 0

  override def nullable: Boolean = false

  override protected def initializeInternal(partitionIndex: Int): Unit = { calls = 0 }

  override protected def evalInternal(input: InternalRow): Any = {
    calls += 1
    if (calls % 2 == 1) {
      InternalRow(1.toByte, null, null,
        UnsafeArrayData.fromPrimitiveArray(Array(1.0, 2.0, 3.0)))
    } else {
      InternalRow(0.toByte, 3, UnsafeArrayData.fromPrimitiveArray(Array(1)),
        UnsafeArrayData.fromPrimitiveArray(Array(7.0)))
    }
  }
}
