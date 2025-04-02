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
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.TypeCheckSuccess
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types._

/**
 * Returns the last value of `child` for a group of rows. If the last value of `child`
 * is `null`, it returns `null` (respecting nulls). Even if [[Last]] is used on an already
 * sorted column, if we do partial aggregation and final aggregation (when mergeExpression
 * is used) its result will not be deterministic (unless the input table is sorted and has
 * a single partition, and we use a single reducer to do the aggregation.).
 */
@ExpressionDescription(
  usage = """
    _FUNC_(expr[, isIgnoreNull]) - Returns the last value of `expr` for a group of rows.
      If `isIgnoreNull` is true, returns only non-null values""",
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES (10), (5), (20) AS tab(col);
       20
      > SELECT _FUNC_(col) FROM VALUES (10), (5), (NULL) AS tab(col);
       NULL
      > SELECT _FUNC_(col, true) FROM VALUES (10), (5), (NULL) AS tab(col);
       5
  """,
  note = """
    The function is non-deterministic because its results depends on the order of the rows
    which may be non-deterministic after a shuffle.
  """,
  group = "agg_funcs",
  since = "2.0.0")
case class Last(child: Expression, ignoreNulls: Boolean)
  extends DeclarativeAggregate with ExpectsInputTypes with UnaryLike[Expression] {

  def this(child: Expression) = this(child, false)

  def this(child: Expression, ignoreNullsExpr: Expression) = {
    this(child, FirstLast.validateIgnoreNullExpr(ignoreNullsExpr, "last"))
  }

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = child.dataType

  // Expected input data type.
  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType, BooleanType)

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else {
      TypeCheckSuccess
    }
  }

  private lazy val last = AttributeReference("last", child.dataType)()

  private lazy val valueSet = AttributeReference("valueSet", BooleanType)()

  override lazy val aggBufferAttributes: Seq[AttributeReference] = last :: valueSet :: Nil

  override lazy val initialValues: Seq[Literal] = Seq(
    /* last = */ Literal.create(null, child.dataType),
    /* valueSet = */ Literal.create(false, BooleanType)
  )

  override lazy val updateExpressions: Seq[Expression] = {
    if (ignoreNulls) {
      Seq(
        /* last = */ If(child.isNull, last, child),
        /* valueSet = */ valueSet || child.isNotNull
      )
    } else {
      Seq(
        /* last = */ child,
        /* valueSet = */ Literal.create(true, BooleanType)
      )
    }
  }

  override lazy val mergeExpressions: Seq[Expression] = {
    // Prefer the right hand expression if it has been set.
    Seq(
      /* last = */ If(valueSet.right, last.right, last.left),
      /* valueSet = */ valueSet.right || valueSet.left
    )
  }

  override lazy val evaluateExpression: AttributeReference = last

  override def toString: String = s"$prettyName($child)${if (ignoreNulls) " ignore nulls"}"

  override protected def withNewChildInternal(newChild: Expression): Last = copy(child = newChild)
}
