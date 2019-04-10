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
import org.apache.spark.sql.types._

@ExpressionDescription(
  usage = """
    _FUNC_(expr) - Returns the number of rows that the supplied expression is non-null and true.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(col % 2 = 0) FROM VALUES (NULL), (0), (1), (2), (3) AS tab(col);
       2
      > SELECT _FUNC_(col IS NULL) FROM VALUES (NULL), (0), (1), (2), (3) AS tab(col);
       1
  """,
  since = "3.0.0")
case class CountIf(child: Expression) extends DeclarativeAggregate {

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = false

  override def dataType: DataType = LongType

  protected lazy val count: AttributeReference =
    AttributeReference("count", LongType, nullable = false)()

  override lazy val aggBufferAttributes: Seq[AttributeReference] = count :: Nil

  override lazy val initialValues: Seq[Expression] = Seq(
    /* count = */ Literal(0L)
  )

  override lazy val mergeExpressions: Seq[Expression] = Seq(
    /* count = */ count.left + count.right
  )

  override lazy val evaluateExpression: AttributeReference = count

  override def defaultResult: Option[Literal] = Option(Literal(0L))

  override def checkInputDataTypes(): TypeCheckResult = child.dataType match {
    case BooleanType => TypeCheckResult.TypeCheckSuccess
    case _ =>
      TypeCheckResult.TypeCheckFailure(
        s"function count_if requires boolean type, not ${child.dataType.catalogString}"
      )
  }

  override lazy val updateExpressions: Seq[Expression] = Seq(
    /* count = */ If(child, count + 1L, count)
  )
}
