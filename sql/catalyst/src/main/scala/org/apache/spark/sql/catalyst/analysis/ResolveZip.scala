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

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, Zip}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.ZIP

/**
 * Resolves a [[Zip]] node by rewriting it into a single [[Project]] over the shared base plan.
 *
 * Both children of Zip must derive from the same base plan through chains of Project nodes.
 * This rule:
 * 1. Waits for both children to be resolved
 * 2. Strips Project layers from each side to find the base plan
 * 3. Verifies the base plans produce the same result (via `sameResult`)
 * 4. Remaps the right side's attribute references to the left base plan's output
 * 5. Produces a single Project that combines both sides' expressions
 *
 * If the base plans do not match, the Zip node remains unresolved and CheckAnalysis
 * will report a [[ZIP_PLANS_NOT_MERGEABLE]] error.
 */
object ResolveZip extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
    _.containsPattern(ZIP), ruleId) {
    case z: Zip if z.childrenResolved =>
      val (leftExprs, leftBase) = extractProjectAndBase(z.left)
      val (rightExprs, rightBase) = extractProjectAndBase(z.right)
      if (leftBase.sameResult(rightBase)) {
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

  private def extractProjectAndBase(
      plan: LogicalPlan): (Seq[NamedExpression], LogicalPlan) = plan match {
    case Project(projectList, child) => (projectList, child)
    case other => (other.output, other)
  }
}
