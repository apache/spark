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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, Expression, NamedExpression, PythonUDF}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, Zip}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.ZIP

/**
 * Resolves a [[Zip]] node by rewriting it into a single [[Project]] over the shared base plan.
 *
 * Both children of Zip must derive from the same base plan through chains of scalar Project
 * nodes (1:1 row mapping). `Project.resolved` already rejects Generator, AggregateExpression,
 * and WindowExpression. This rule additionally rejects non-scalar Python UDFs (e.g.
 * GROUPED_MAP), which are not caught by `Project.resolved`.
 *
 * This rule:
 * 1. Waits for both children to be resolved
 * 2. Strips Project layers from each side to find the base plan, composing alias
 *    substitutions so the resulting expressions reference the base plan's attributes directly
 * 3. Verifies the base plans produce the same result (via `sameResult`)
 * 4. Verifies neither side contains a non-scalar Python UDF
 * 5. Remaps the right side's attribute references to the left base plan's output
 * 6. Produces a single Project that combines both sides' expressions
 *
 * If the base plans do not match, or a non-scalar Python UDF is present, the Zip node remains
 * unresolved and CheckAnalysis will report a `ZIP_PLANS_NOT_MERGEABLE` error.
 */
object ResolveZip extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
    _.containsPattern(ZIP), ruleId) {
    case z: Zip if z.childrenResolved =>
      val (leftExprs, leftBase) = extractProjectAndBase(z.left)
      val (rightExprs, rightBase) = extractProjectAndBase(z.right)
      if (leftBase.sameResult(rightBase) && allScalar(leftExprs ++ rightExprs)) {
        // Build an attribute mapping from rightBase output to leftBase output (by position)
        val attrMapping = AttributeMap(rightBase.output.zip(leftBase.output))
        // Remap right expressions to reference leftBase's attributes
        val remappedRightExprs = rightExprs.map { expr =>
          expr.transform {
            case a: Attribute => attrMapping.getOrElse(a, a)
          }.asInstanceOf[NamedExpression]
        }
        Project(leftExprs ++ remappedRightExprs, leftBase)
      } else {
        z
      }
  }

  /**
   * Walks down a chain of [[Project]] nodes, composing alias substitutions so the returned
   * expressions reference the deepest non-Project base directly. Necessary because chains of
   * `select`/`withColumn` produce nested Projects, and merging two Zip sides requires both to
   * reach the same `sameResult` base regardless of chain depth.
   */
  private def extractProjectAndBase(
      plan: LogicalPlan): (Seq[NamedExpression], LogicalPlan) = plan match {
    case Project(projectList, child) => stripProjects(child, projectList)
    case other => (other.output, other)
  }

  @scala.annotation.tailrec
  private def stripProjects(
      plan: LogicalPlan,
      outerExprs: Seq[NamedExpression]): (Seq[NamedExpression], LogicalPlan) = plan match {
    case Project(innerExprs, child) =>
      val aliasMap = AttributeMap(innerExprs.collect {
        case a: Alias => a.toAttribute -> a.child
      })
      val composed = outerExprs.map(substitute(_, aliasMap))
      stripProjects(child, composed)
    case other => (outerExprs, other)
  }

  /**
   * Replaces references to inner aliases inside `expr` with the underlying expressions. When
   * `expr` is a bare [[Attribute]] that matches an inner alias, wraps the substituted expression
   * in a fresh [[Alias]] preserving the outer name and exprId so downstream references stay
   * stable.
   */
  private def substitute(
      expr: NamedExpression, aliasMap: AttributeMap[Expression]): NamedExpression = expr match {
    case attr: Attribute if aliasMap.contains(attr) =>
      Alias(aliasMap(attr), attr.name)(exprId = attr.exprId)
    case _ =>
      expr.transform {
        case a: Attribute if aliasMap.contains(a) => aliasMap(a)
      }.asInstanceOf[NamedExpression]
  }

  /**
   * Returns true if all expressions are scalar (1:1 row mapping).
   * `Project.resolved` already rejects Generator, AggregateExpression, and WindowExpression.
   * This additionally rejects non-scalar Python UDFs (e.g. GROUPED_MAP) that can break
   * the 1:1 row mapping.
   */
  private def allScalar(exprs: Seq[NamedExpression]): Boolean = {
    !exprs.exists(_.exists {
      case udf: PythonUDF => !PythonUDF.isScalarPythonUDF(udf)
      case _ => false
    })
  }
}
