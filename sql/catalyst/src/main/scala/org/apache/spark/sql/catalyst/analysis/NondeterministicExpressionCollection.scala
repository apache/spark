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

package org.apache.spark.sql.catalyst.analysis

import java.util.LinkedHashMap

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.trees.TreePattern._

object NondeterministicExpressionCollection {
  private[sql] val EXTRACTED_SUBQUERY = TreeNodeTag[Unit]("extracted-nondeterministic-subquery")

  private[analysis] def getExpressionKey(expression: Expression): (Expression, Seq[ExprId]) = {
    // Canonicalization removes subquery IDs, but separate nondeterministic subqueries must
    // retain independent evaluations, including when they are children of a collected UDF.
    val subqueryIds = if (expression.containsAnyPattern(
        SCALAR_SUBQUERY, EXISTS_SUBQUERY, IN_SUBQUERY)) {
      expression.collect { case subquery: SubqueryExpression if !subquery.deterministic =>
        subquery.exprId
      }
    } else Seq.empty
    (expression.canonicalized, subqueryIds)
  }

  def getNondeterministicToAttributes(
      expressions: Seq[Expression]): LinkedHashMap[(Expression, Seq[ExprId]), NamedExpression] = {
    val nonDeterministicToAttributes =
      new LinkedHashMap[(Expression, Seq[ExprId]), NamedExpression]

    for (expr <- expressions) {
      if (!expr.deterministic) {
        val leafNondeterministic = expr.collect {
          case nondeterministicExpr: Nondeterministic => nondeterministicExpr
          case scalar: ScalarSubquery if !scalar.deterministic => scalar
          case exists: Exists if !exists.deterministic => exists
          case not @ Not(in: InSubquery) if !in.deterministic => not
          case in: InSubquery if !in.deterministic => in
          case udf: UserDefinedExpression if !udf.deterministic => udf
          case udf: ExternalUserDefinedFunction if !udf.deterministic => udf
        }

        for (nondeterministicExpr <- leafNondeterministic.distinct) {
          val namedExpression = nondeterministicExpr match {
            case namedExpression: NamedExpression => namedExpression
            case _ => Alias(nondeterministicExpr, "_nondeterministic")()
          }
          if (nondeterministicExpr.containsAnyPattern(
              SCALAR_SUBQUERY, EXISTS_SUBQUERY, IN_SUBQUERY)) {
            namedExpression.setTagValue(EXTRACTED_SUBQUERY, ())
          }
          nonDeterministicToAttributes.put(getExpressionKey(nondeterministicExpr), namedExpression)
        }
      }
    }

    nonDeterministicToAttributes
  }
}
