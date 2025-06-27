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

import org.apache.datasketches.frequencies.ItemsSketch
import org.apache.datasketches.memory.Memory

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.aggregate.ApproxTopK
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

case class ApproxTopKEstimate(expr: Expression, k: Expression)
  extends BinaryExpression
  with CodegenFallback
  with ImplicitCastInputTypes {

  def this(child: Expression, topK: Int) = this(child, Literal(topK))

  def this(child: Expression) = this(child, Literal(ApproxTopK.DEFAULT_K))

  private lazy val itemDataType: DataType = {
    // itemDataType is the type of the "ItemTypeNull" field of the output of ACCUMULATE or COMBINE
    expr.dataType.asInstanceOf[StructType]("ItemTypeNull").dataType
  }

  override def left: Expression = expr

  override def right: Expression = k

  override def inputTypes: Seq[AbstractDataType] = Seq(StructType, IntegerType)

  override def dataType: DataType = ApproxTopK.getResultDataType(itemDataType)

  override def nullSafeEval(input1: Any, input2: Any): Any = {
    val dataSketchBytes = input1.asInstanceOf[InternalRow].getBinary(0)
    val maxItemsTrackedVal = input1.asInstanceOf[InternalRow].getInt(2)
    ApproxTopK.checkExpressionNotNull(k, "k")
    val kVal = input2.asInstanceOf[Int]
    ApproxTopK.checkK(kVal)
    ApproxTopK.checkMaxItemsTracked(maxItemsTrackedVal, kVal)
    val itemsSketch = ItemsSketch.getInstance(
      Memory.wrap(dataSketchBytes), ApproxTopK.genSketchSerDe(itemDataType))
    ApproxTopK.genEvalResult(itemsSketch, kVal, itemDataType)
  }

  override protected def withNewChildrenInternal(newExpr: Expression, newK: Expression)
  : Expression = copy(expr = newExpr, k = newK)

  override def nullIntolerant: Boolean = false

  override def prettyName: String =
    getTagValue(FunctionRegistry.FUNC_ALIAS).getOrElse("approx_top_k_estimate")
}
