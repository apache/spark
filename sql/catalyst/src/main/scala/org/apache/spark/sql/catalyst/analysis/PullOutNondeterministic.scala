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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Pulls out nondeterministic expressions from LogicalPlan which is not Project or Filter,
 * put them into an inner Project and finally project them away at the outer Project.
 */
object PullOutNondeterministic extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsUp applyLocally

  val applyLocally: PartialFunction[LogicalPlan, LogicalPlan] = {
    case p if !p.resolved => p // Skip unresolved nodes.
    case p: Project => p
    case f: Filter => f

    case a: Aggregate if a.groupingExpressions.exists(!_.deterministic) =>
      val nondeterToAttr = getNondeterToAttr(a.groupingExpressions)
      val newChild = Project(a.child.output ++ nondeterToAttr.values, a.child)
      a.transformExpressions { case e =>
        nondeterToAttr.get(e).map(_.toAttribute).getOrElse(e)
      }.copy(child = newChild)

    // Don't touch collect metrics. Top-level metrics are not supported (check analysis will fail)
    // and we want to retain them inside the aggregate functions.
    case m: CollectMetrics => m

    // Skip PythonUDTF as it will be planned as its own dedicated logical and physical node.
    case g @ Generate(_: PythonUDTF, _, _, _, _, _) => g

    // todo: It's hard to write a general rule to pull out nondeterministic expressions
    // from LogicalPlan, currently we only do it for UnaryNode which has same output
    // schema with its child.
    case p: UnaryNode if p.output == p.child.output && p.expressions.exists(!_.deterministic) =>
      val nondeterToAttr = getNondeterToAttr(p.expressions)
      val newPlan = p.transformExpressions { case e =>
        nondeterToAttr.get(e).map(_.toAttribute).getOrElse(e)
      }
      val newChild = Project(p.child.output ++ nondeterToAttr.values, p.child)
      Project(p.output, newPlan.withNewChildren(newChild :: Nil))
  }

  private def getNondeterToAttr(exprs: Seq[Expression]): Map[Expression, NamedExpression] = {
    exprs.filterNot(_.deterministic).flatMap { expr =>
      val leafNondeterministic = expr.collect {
        case n: Nondeterministic => n
        case udf: UserDefinedExpression if !udf.deterministic => udf
      }
      leafNondeterministic.distinct.map { e =>
        val ne = e match {
          case n: NamedExpression => n
          case _ => Alias(e, "_nondeterministic")()
        }
        e -> ne
      }
    }.toMap
  }
}
