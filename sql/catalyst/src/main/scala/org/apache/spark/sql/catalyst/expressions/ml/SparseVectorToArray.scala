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

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.{Expression, TernaryExpression, UnsafeArrayData}
import org.apache.spark.sql.catalyst.expressions.Cast._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

/**
 * Expands a sparse MLlib vector's size, indices, and values into a dense array of doubles or
 * floats. This expression is dedicated only for Spark ML. The indices and values arrays must
 * have non-null elements. Null inputs produce a null result.
 */
case class SparseVectorToArray(
    size: Expression,
    indices: Expression,
    values: Expression,
    elementType: DataType)
  extends TernaryExpression {

  require(elementType == DoubleType || elementType == FloatType,
    "SparseVectorToArray requires a double or float element type.")

  override def first: Expression = size
  override def second: Expression = indices
  override def third: Expression = values

  override def nullIntolerant: Boolean = true

  override def prettyName: String = "sparse_vector_to_array"

  override def dataType: DataType = ArrayType(elementType, containsNull = false)

  override def checkInputDataTypes(): TypeCheckResult = {
    val inputTypes = Seq(
      IntegerType,
      ArrayType(IntegerType, containsNull = false),
      ArrayType(DoubleType, containsNull = false))
    children.zip(inputTypes).zipWithIndex.collectFirst {
      case ((input, expected), index) if input.dataType != expected =>
        DataTypeMismatch(
          errorSubClass = "UNEXPECTED_INPUT_TYPE",
          messageParameters = Map(
            "paramIndex" -> ordinalNumber(index),
            "requiredType" -> toSQLType(expected),
            "inputSql" -> toSQLExpr(input),
            "inputType" -> toSQLType(input.dataType)))
    }.getOrElse(TypeCheckResult.TypeCheckSuccess)
  }

  override protected def nullSafeEval(
      sizeInput: Any,
      indicesInput: Any,
      valuesInput: Any): Any = {
    val vectorSize = sizeInput.asInstanceOf[Int]
    val vectorIndices = indicesInput.asInstanceOf[ArrayData]
    val vectorValues = valuesInput.asInstanceOf[ArrayData]
    if (elementType == FloatType) {
      val result = new Array[Float](vectorSize)
      var i = 0
      while (i < vectorIndices.numElements()) {
        result(vectorIndices.getInt(i)) = vectorValues.getDouble(i).toFloat
        i += 1
      }
      UnsafeArrayData.fromPrimitiveArray(result)
    } else {
      val result = new Array[Double](vectorSize)
      var i = 0
      while (i < vectorIndices.numElements()) {
        result(vectorIndices.getInt(i)) = vectorValues.getDouble(i)
        i += 1
      }
      UnsafeArrayData.fromPrimitiveArray(result)
    }
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val javaType = CodeGenerator.javaType(elementType)
    val result = ctx.freshName("result")
    val index = ctx.freshName("index")
    val unsafeArrayData = classOf[UnsafeArrayData].getName
    nullSafeCodeGen(ctx, ev, (sizeInput, indicesInput, valuesInput) => {
      s"""
         |$javaType[] $result = new $javaType[$sizeInput];
         |for (int $index = 0; $index < $indicesInput.numElements(); $index++) {
         |  $result[$indicesInput.getInt($index)] = ($javaType) $valuesInput.getDouble($index);
         |}
         |${ev.value} = $unsafeArrayData.fromPrimitiveArray($result);
       """.stripMargin
    })
  }

  override protected def withNewChildrenInternal(
      newSize: Expression,
      newIndices: Expression,
      newValues: Expression): SparseVectorToArray = {
    copy(size = newSize, indices = newIndices, values = newValues)
  }
}
