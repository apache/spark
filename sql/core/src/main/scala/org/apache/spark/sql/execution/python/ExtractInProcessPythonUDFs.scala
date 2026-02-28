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

package org.apache.spark.sql.execution.python

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression,
  NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Extracts [[InProcessPythonUDF]] expressions from logical plan nodes, rewriting the plan
 * so each batch of UDFs is evaluated in a dedicated [[InProcessEvalPython]] node.
 *
 * Simpler than [[ExtractPythonUDFs]]: in-process UDFs are scalar and don't support
 * iterator mode, aggregate mode, or nested chaining in Phase 1.
 *
 * Example rewrite:
 *   Project [double(a), triple(b)]
 *     Scan
 *   becomes:
 *   Project [inprocessUDF0, inprocessUDF1]
 *     InProcessEvalPython [double(a), triple(b)] to [inprocessUDF0, inprocessUDF1]
 *       Scan
 */
object ExtractInProcessPythonUDFs extends Rule[LogicalPlan] {

  private def hasInProcessUDF(e: Expression): Boolean =
    e.exists(_.isInstanceOf[InProcessPythonUDF])

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformUp {
    // Already extracted — skip to avoid double-wrapping
    case p: InProcessEvalPython => p

    case node: LogicalPlan if node.expressions.exists(hasInProcessUDF) =>
      extract(node)
  }

  private def extract(plan: LogicalPlan): LogicalPlan = {
    // Collect all distinct InProcessPythonUDFs from this plan's expressions
    val udfs = plan.expressions
      .flatMap(_.collect { case u: InProcessPythonUDF => u })
      .distinct

    if (udfs.isEmpty) return plan

    // Map each UDF to a fresh AttributeReference that will hold its result
    val attributeMap = mutable.LinkedHashMap[InProcessPythonUDF, NamedExpression]()

    // For each child plan, find UDFs whose inputs are fully satisfied by that child
    val newChildren = plan.children.map { child =>
      val validUdfs = udfs.filter(_.references.subsetOf(child.outputSet))
      if (validUdfs.nonEmpty) {
        val resultAttrs: Seq[Attribute] = validUdfs.zipWithIndex.map { case (u, i) =>
          AttributeReference(s"inprocessUDF$i", u.dataType)()
        }
        attributeMap ++= validUdfs.zip(resultAttrs)
        InProcessEvalPython(validUdfs, resultAttrs, child)
      } else {
        child
      }
    }

    // Replace InProcessPythonUDF expressions with their result attributes
    val rewritten = plan.withNewChildren(newChildren).transformExpressions {
      case u: InProcessPythonUDF => attributeMap.getOrElse(u, u)
    }

    // Trim the added UDF result attributes if not in the original output
    if (rewritten.output != plan.output) Project(plan.output, rewritten) else rewritten
  }
}
