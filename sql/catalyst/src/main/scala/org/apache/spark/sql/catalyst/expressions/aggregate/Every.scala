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

package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns true if all values of `expr` are true.")
case class Every(child: Expression) extends DeclarativeAggregate with ImplicitCastInputTypes {

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  override def dataType: DataType = BooleanType

  override def inputTypes: Seq[AbstractDataType] = Seq(BooleanType)

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForBooleanExpr(child.dataType, "function every")

  private lazy val every = AttributeReference("every", BooleanType)()

  private lazy val emptySet = AttributeReference("emptySet", BooleanType)()

  override lazy val aggBufferAttributes = every :: emptySet :: Nil

  override lazy val initialValues: Seq[Expression] = Seq(
    Literal(true),
    Literal(true)
  )

  override lazy val updateExpressions: Seq[Expression] = Seq(
    And(every, Coalesce(Seq(child, Literal(false)))),
    Literal(false)
  )

  override lazy val mergeExpressions: Seq[Expression] = Seq(
    And(every.left, every.right),
    And(emptySet.left, emptySet.right)
  )

  override lazy val evaluateExpression: Expression = And(!emptySet, every)
}
