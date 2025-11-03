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
import org.apache.spark.sql.catalyst.trees.TreePattern.{COUNT, TreePattern}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryErrorsBase}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(*) - Returns the total number of retrieved rows, including rows containing null.

    _FUNC_(expr[, expr...]) - Returns the number of rows for which the supplied expression(s) are all non-null.

    _FUNC_(DISTINCT expr[, expr...]) - Returns the number of rows for which the supplied expression(s) are unique and non-null.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(*) FROM VALUES (NULL), (5), (5), (20) AS tab(col);
       4
      > SELECT _FUNC_(col) FROM VALUES (NULL), (5), (5), (20) AS tab(col);
       3
      > SELECT _FUNC_(DISTINCT col) FROM VALUES (NULL), (5), (5), (10) AS tab(col);
       2
  """,
  group = "agg_funcs",
  since = "1.0.0")
// scalastyle:on line.size.limit
case class Count(children: Seq[Expression]) extends DeclarativeAggregate
  with QueryErrorsBase {

  override def nullable: Boolean = false

  final override val nodePatterns: Seq[TreePattern] = Seq(COUNT)

  // Return data type.
  override def dataType: DataType = LongType

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.isEmpty && !SQLConf.get.getConf(SQLConf.ALLOW_PARAMETERLESS_COUNT)) {
      throw QueryCompilationErrors.wrongNumArgsError(
        toSQLId(prettyName),
        Seq(" >= 1"),
        0,
        "0",
        toSQLConf(SQLConf.ALLOW_PARAMETERLESS_COUNT.key),
        toSQLConfVal(true.toString))
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  protected lazy val count = AttributeReference("count", LongType, nullable = false)()

  override lazy val aggBufferAttributes = count :: Nil

  override lazy val initialValues = Seq(
    /* count = */ Literal(0L)
  )

  override lazy val mergeExpressions = Seq(
    /* count = */ count.left + count.right
  )

  override lazy val evaluateExpression = count

  override def defaultResult: Option[Literal] = Option(Literal(0L))

  override lazy val updateExpressions = {
    val nullableChildren = children.filter(_.nullable)
    if (nullableChildren.isEmpty) {
      Seq(
        /* count = */ count + 1L
      )
    } else {
      Seq(
        /* count = */ If(nullableChildren.map(IsNull).reduce(Or), count, count + 1L)
      )
    }
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Count =
    copy(children = newChildren)
}

object Count {
  def apply(child: Expression): Count = Count(child :: Nil)
}
