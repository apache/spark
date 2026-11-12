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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression, UnsafeArrayData}
import org.apache.spark.sql.catalyst.expressions.Cast._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode}
import org.apache.spark.sql.types._

/**
 * Expands the SQL struct representation of a sparse MLlib vector into a dense array of doubles
 * or floats. This expression is dedicated only for Spark ML and can be used with `unwrap_udt`.
 * The input must match the vector SQL schema, including field and array element nullability.
 * A null vector, size, indices, or values field produces a null result.
 */
case class SparseVectorToArray(child: Expression, elementType: DataType)
  extends UnaryExpression {

  require(elementType == DoubleType || elementType == FloatType,
    "SparseVectorToArray requires a double or float element type.")

  override def nullIntolerant: Boolean = true

  override def prettyName: String = "sparse_vector_to_array"

  override def dataType: DataType = ArrayType(elementType, containsNull = false)

  // The sparse vector fields are nullable in the vector SQL schema.
  override def nullable: Boolean = true

  override def checkInputDataTypes(): TypeCheckResult = {
    if (child.dataType == VectorScaleShift.vectorSqlType) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(0),
          "requiredType" -> toSQLType(VectorScaleShift.vectorSqlType),
          "inputSql" -> toSQLExpr(child),
          "inputType" -> toSQLType(child.dataType)))
    }
  }

  override protected def nullSafeEval(input: Any): Any = {
    val vector = input.asInstanceOf[InternalRow]
    if (vector.isNullAt(1) || vector.isNullAt(2) || vector.isNullAt(3)) {
      return null
    }
    val vectorSize = vector.getInt(1)
    val vectorIndices = vector.getArray(2)
    val vectorValues = vector.getArray(3)
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
    val vectorSize = ctx.freshName("vectorSize")
    val vectorIndices = ctx.freshName("vectorIndices")
    val vectorValues = ctx.freshName("vectorValues")
    val result = ctx.freshName("result")
    val index = ctx.freshName("index")
    val unsafeArrayData = classOf[UnsafeArrayData].getName
    nullSafeCodeGen(ctx, ev, vector => {
      s"""
         |${ev.isNull} = $vector.isNullAt(1) || $vector.isNullAt(2) || $vector.isNullAt(3);
         |if (!${ev.isNull}) {
         |  int $vectorSize = $vector.getInt(1);
         |  ArrayData $vectorIndices = $vector.getArray(2);
         |  ArrayData $vectorValues = $vector.getArray(3);
         |  $javaType[] $result = new $javaType[$vectorSize];
         |  for (int $index = 0; $index < $vectorIndices.numElements(); $index++) {
         |    $result[$vectorIndices.getInt($index)] = ($javaType) $vectorValues.getDouble($index);
         |  }
         |  ${ev.value} = $unsafeArrayData.fromPrimitiveArray($result);
         |}
       """.stripMargin
    })
  }

  override protected def withNewChildInternal(newChild: Expression): SparseVectorToArray = {
    copy(child = newChild)
  }
}
