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
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, ExpectsInputTypes, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types._

/**
 * Computes the dot product of two SQL struct representations of MLlib vectors. This expression is
 * dedicated only for Spark ML and should be used together with `unwrap_udt`.
 */
case class VectorDotProduct(left: Expression, right: Expression)
  extends BinaryExpression with ExpectsInputTypes {

  override def nullIntolerant: Boolean = true

  override def prettyName: String = "ml_vector_dot_product"

  override def inputTypes: Seq[AbstractDataType] =
    Seq(VectorDotProduct.vectorSqlType, VectorDotProduct.vectorSqlType)

  override def dataType: DataType = DoubleType

  override def nullable: Boolean = left.nullable || right.nullable

  override protected def nullSafeEval(leftInput: Any, rightInput: Any): Any = {
    MLExpressionUtils.dotProduct(
      leftInput.asInstanceOf[InternalRow],
      rightInput.asInstanceOf[InternalRow])
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val utils = classOf[MLExpressionUtils].getName
    nullSafeCodeGen(ctx, ev, (leftInput, rightInput) => {
      s"${ev.value} = $utils.dotProduct($leftInput, $rightInput);"
    })
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression,
      newRight: Expression): VectorDotProduct = copy(left = newLeft, right = newRight)
}

object VectorDotProduct {
  private[ml] val vectorSqlType = StructType(Array(
    StructField("type", ByteType, nullable = false),
    StructField("size", IntegerType, nullable = true),
    StructField("indices", ArrayType(IntegerType, containsNull = false), nullable = true),
    StructField("values", ArrayType(DoubleType, containsNull = false), nullable = true)))
}
