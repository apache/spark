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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction
import org.apache.spark.sql.types.{AbstractDataType, DataType}

case class ApplyFunctionExpression(
    function: ScalarFunction[_],
    children: Seq[Expression])
  extends Expression with UserDefinedExpression with CodegenFallback with ImplicitCastInputTypes {

  override def nullable: Boolean = function.isResultNullable
  override def name: String = function.name()
  override def dataType: DataType = function.resultType()
  override def inputTypes: Seq[AbstractDataType] = function.inputTypes().toSeq
  override lazy val deterministic: Boolean = function.isDeterministic &&
      children.forall(_.deterministic)

  private lazy val reusedRow = new SpecificInternalRow(function.inputTypes())

  /** Returns the result of evaluating this expression on a given input Row */
  override def eval(input: InternalRow): Any = {
    var i = 0
    while (i < children.length) {
      val expr = children(i)
      reusedRow.update(i, expr.eval(input))
      i += 1
    }

    function.produceResult(reusedRow)
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)
}
