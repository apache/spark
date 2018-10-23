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
  usage = "_FUNC_(expr) - Returns the maximum value of `expr`.")
case class Max(child: Expression) extends DeclarativeAggregate {

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = child.dataType

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForOrderingExpr(child.dataType, "function max")

  private lazy val max = AttributeReference("max", child.dataType)()

  override lazy val aggBufferAttributes: Seq[AttributeReference] = max :: Nil

  override lazy val initialValues: Seq[Literal] = Seq(
    /* max = */ Literal.create(null, child.dataType)
  )

  override lazy val updateExpressions: Seq[Expression] = Seq(
    /* max = */ greatest(max, child)
  )

  override lazy val mergeExpressions: Seq[Expression] = {
    Seq(
      /* max = */ greatest(max.left, max.right)
    )
  }

  override lazy val evaluateExpression: AttributeReference = max
}

abstract class AnyAggBase(arg: Expression)
  extends UnevaluableAggrgate with ImplicitCastInputTypes {

  override def children: Seq[Expression] = arg :: Nil

  override def dataType: DataType = BooleanType

  override def inputTypes: Seq[AbstractDataType] = Seq(BooleanType)

  override def checkInputDataTypes(): TypeCheckResult = {
    arg.dataType match {
      case dt if dt != BooleanType =>
        TypeCheckResult.TypeCheckFailure(s"Input to function '$prettyName' should have been " +
          s"${BooleanType.simpleString}, but it's [${arg.dataType.catalogString}].")
      case _ => TypeCheckResult.TypeCheckSuccess
    }
  }
}

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns true if at least one value of `expr` is true.")
case class AnyAgg(arg: Expression) extends AnyAggBase(arg) {
  override def nodeName: String = "Any"
}

@ExpressionDescription(
  usage = "_FUNC_(expr) - Returns true if at least one value of `expr` is true.")
case class SomeAgg(arg: Expression) extends AnyAggBase(arg) {
  override def nodeName: String = "Some"
}
