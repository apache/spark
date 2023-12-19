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

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "BETWEEN(projection, lower, upper) - Returns true if `projection` > `lower` and `projection` <  `upper`.",
  examples = """
    Examples:
      > SELECT _FUNC_(0.5, 0.1, 1.0);
        TRUE
  """,
  arguments = """
    Arguments:
      * projection - An expression that is being compared with lower and upper bound.
      * lower - Lower bound of the between check.
      * upper - Upper bound of the between check.
  """,
  since = "4.0.0",
  group = "conditional_funcs")
case class Between(proj: Expression, lower: Expression, upper: Expression, replacement: Expression)
  extends RuntimeReplaceable with InheritAnalysisRules  {
  def this(proj: Expression, lower: Expression, upper: Expression) = {
    this(proj, lower, upper, {
        val commonExpr = CommonExpressionDef(proj)
        val ref = new CommonExpressionRef(commonExpr)
        With(And(
            GreaterThanOrEqual(ref, lower),
            LessThanOrEqual(ref, upper)),
            Seq(commonExpr))
    })
  };

  override def parameters: Seq[Expression] = Seq(proj, lower, upper)

  override protected def withNewChildInternal(newChild: Expression): Between = {
    copy(replacement = newChild)
  }
}

object Between {
  def apply(proj: Expression, lower: Expression, upper: Expression): Between = {
    new Between(proj, lower, upper)
  }
}
