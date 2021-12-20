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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, If, Literal}
import org.apache.spark.sql.types.DoubleType

@ExpressionDescription(
  usage = """
    _FUNC_(expr1, expr2) - Returns the number of non-null number pairs in a group.
  """,
  examples = """
    Examples:
      > SELECT _FUNC_(y, x) FROM VALUES (1, 2), (2, 2), (2, 3), (2, 4) AS tab(y, x);
       0.2727272727272727
      > SELECT _FUNC_(y, x) FROM VALUES (1, 2), (2, null), (2, 3), (2, 4) AS tab(y, x);
       0.7500000000000001
      > SELECT _FUNC_(y, x) FROM VALUES (1, 2), (2, null), (null, 3), (2, 4) AS tab(y, x);
       1.0
  """,
  group = "agg_funcs",
  since = "3.3.0")
case class RegrR2(x: Expression, y: Expression)
  extends PearsonCorrelation(x, y, true) {

  override def prettyName: String = "regr_r2"

  override val evaluateExpression: Expression = {
    lazy val corr = ck / sqrt(xMk * yMk)
    If(n === 0.0, Literal.create(null, DoubleType),
      If(n === 1.0, divideByZeroEvalResult, corr * corr))
  }

  override protected def withNewChildrenInternal(
      newLeft: Expression, newRight: Expression): RegrR2 =
    this.copy(x = newLeft, y = newRight)
}
