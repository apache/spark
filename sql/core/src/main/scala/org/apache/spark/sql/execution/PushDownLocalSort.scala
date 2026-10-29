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

package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, AttributeReference, AttributeSet, SortOrder}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.window.{WindowExecBase, WindowGroupLimitExec}
import org.apache.spark.sql.internal.SQLConf

/**
 * Pushes a wider local sort down through order-preserving operators onto a narrower local sort
 * below, widening it so that a single sort satisfies several operators' ordering requirements
 * instead of re-sorting once per operator.
 *
 * `EnsureRequirements` adds one local `SortExec` (`global = false`) above every operator whose
 * `requiredChildOrdering` is not already satisfied. When such requirements are in a prefix-cover
 * relationship, this produces multiple local sorts that only differ in width. A canonical case is a
 * sort aggregate stacked on a window over the same clustering keys, where the aggregate needs a
 * wider ordering than the window:
 *
 * {{{
 *   SortAggregate(key = [a, b, c])
 *     Sort([a, b, c], global = false)   <- upper, wider
 *       Window([a], [b])
 *         Sort([a, b], global = false)  <- lower, narrower
 *           Exchange(hashpartitioning([a]))
 * }}}
 *
 * Because every operator between the two sorts is order-preserving and the upper ordering
 * prefix-covers everything required along the way, the wider ordering can be pushed down to widen
 * the lower sort, and the upper sort then dropped entirely:
 *
 * {{{
 *   SortAggregate(key = [a, b, c])
 *     Window([a], [b])                  requiredChildOrdering [a, b] is satisfied by [a, b, c]
 *       Sort([a, b, c], global = false) <- single sort now serves both operators
 *         Exchange(hashpartitioning([a]))
 * }}}
 *
 * When a `ProjectExec` on the path renames an ordering column in its output (`b AS x`), the
 * ordering is rewritten from the project's output space back to its child's space (`x` -> `b`) as
 * it is pushed through, so a sort over the renamed column is still matched below. Only plain
 * renames are followed, and the rule never crosses a shuffle or a non-order-preserving operator.
 */
object PushDownLocalSort extends Rule[SparkPlan] {

  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(SQLConf.PUSH_DOWN_LOCAL_SORT_ENABLED)) {
      return plan
    }
    val throughCardinalityReducer =
      conf.getConf(SQLConf.PUSH_DOWN_LOCAL_SORT_THROUGH_CARDINALITY_REDUCER_ENABLED)

    plan.transform {
      case upper @ SortExec(upperOrder, false, child, _) =>
        pushDown(child, upperOrder, throughCardinalityReducer).getOrElse(upper)
    }
  }

  /**
   * Walks down from `plan` through a chain of order-preserving unary operators, looking for a
   * lower local `SortExec` that `upperOrder` strictly covers. When found, widens that lower sort
   * to `upperOrder` and returns the rebuilt subtree (which re-exposes `upperOrder` at its top);
   * returns `None` if no safe widening applies, leaving the plan untouched. As it crosses an
   * operator that renames ordering columns, `upperOrder` is rewritten into that operator's child
   * space so the search continues against the child's own attributes. `throughCardinalityReducer`
   * controls whether cardinality-reducing operators (`FilterExec`, `WindowGroupLimitExec`) may be
   * crossed.
   */
  private def pushDown(
      plan: SparkPlan,
      upperOrder: Seq[SortOrder],
      throughCardinalityReducer: Boolean): Option[SparkPlan] = plan match {
    case lower @ SortExec(lowerOrder, false, _, _)
        // Only widen when the upper ordering strictly covers the lower one. When they are
        // equivalent the upper sort is plainly redundant and is left to `RemoveRedundantSorts`; a
        // non-covering ordering cannot serve the lower requirement. The column check keeps the
        // widened sort well-formed (every key of `upperOrder` is available below the lower sort).
        if SortOrder.orderingSatisfies(upperOrder, lowerOrder) &&
          !SortOrder.orderingSatisfies(lowerOrder, upperOrder) &&
          AttributeSet(upperOrder.flatMap(_.references)).subsetOf(lower.child.outputSet) =>
      Some(SortExec(upperOrder, global = false, child = lower.child))

    case op: UnaryExecNode if isOrderPreserving(op, throughCardinalityReducer) =>
      // A `ProjectExec` may rename ordering columns in its output (`b AS x`). Rewrite `upperOrder`
      // from the operator's output space back to its child's space before pushing further down, so
      // a sort over the renamed column is still matched below. Only plain renames are followed; an
      // expression alias leaves the sort key referencing an output attribute the child does not
      // produce, so the `subsetOf(op.child.outputSet)` check below rejects it.
      val outputExprs = plan match {
        case p: ProjectExec => p.projectList
        case _ => Nil
      }
      val rewrittenUpperOrder = if (outputExprs.isEmpty) {
        upperOrder
      } else {
        val aliasToAttributeMap = AttributeMap(outputExprs.collect {
          case a @ Alias(child: AttributeReference, _) => (a.toAttribute, child: Attribute)
        })
        upperOrder.map { _.transformUp {
            case a: Attribute => aliasToAttributeMap.getOrElse(a, a)
          }.asInstanceOf[SortOrder]
        }
      }
      if (SortOrder.orderingSatisfies(rewrittenUpperOrder, op.requiredChildOrdering.head) &&
          AttributeSet(rewrittenUpperOrder.flatMap(_.references)).subsetOf(op.child.outputSet)) {
        pushDown(op.child, rewrittenUpperOrder, throughCardinalityReducer)
          .map(newChild => op.withNewChildren(Seq(newChild)))
      } else {
        None
      }

    case _ => None
  }

  private def isOrderPreserving(
      plan: UnaryExecNode,
      throughCardinalityReducer: Boolean): Boolean = plan match {
    // A non-deterministic project/filter must not be crossed: moving the sort below it changes
    // which rows a seeded non-deterministic expression is evaluated over (a different row-value
    // association for a project, a different surviving set for a filter). This mirrors the
    // determinism guard in the logical `EliminateSorts.canEliminateSort`.
    case p: ProjectExec => p.projectList.forall(_.deterministic)
    // `FilterExec` and `WindowGroupLimitExec` are cardinality reducers: pushing the wider sort
    // below them sorts the full input instead of only the surviving rows, which can outweigh the
    // saved sort. Only cross them when explicitly enabled. `FilterExec` additionally needs the
    // determinism guard, for the same reason as the project above.
    case f: FilterExec => throughCardinalityReducer && f.condition.deterministic
    case _: WindowGroupLimitExec => throughCardinalityReducer
    // Pushing a wider sort under a window refines the input order the window sees within ties of
    // its own `ORDER BY`. For order-sensitive window functions (`first_value`/`last_value`/
    // `collect_list`, `lead`/`lag`, `ROWS`-frame aggregates) that can change the result -- but only
    // for a non-unique window `ORDER BY`, where the result is already non-deterministic by Spark's
    // contract, so this does not change any deterministic result. This differs from
    // `CollectMetricsExec`, which is deliberately excluded: its observed metric is a documented,
    // expected-stable side output, not a non-deterministic query result, so refining the order it
    // sees would be a real surprise.
    case _: WindowExecBase => true
    case _ => false
  }
}
