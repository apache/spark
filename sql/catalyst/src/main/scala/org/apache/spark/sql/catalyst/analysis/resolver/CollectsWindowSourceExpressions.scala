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

package org.apache.spark.sql.catalyst.analysis.resolver

import org.apache.spark.sql.catalyst.expressions.{Alias, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project, Window}

/**
 * [[CollectsWindowSourceExpressions]] is extended by resolvers that need to collect expressions
 * that will be placed in a base operator below newly constructed [[Window]] operator.
 *
 * Consider a query in which there is a [[WindowExpression]] in a [[Project]], along with a
 * correlated [[Exists]] subquery, for example:
 *
 * {{{
 * SELECT
 *   SUM(salary) OVER (PARTITION BY EXISTS (
 *     SELECT 1 FROM dept WHERE emp.dept_id = dept.dept_id
 *   ))
 * FROM
 *   emp;
 * }}}
 *
 * Since there are window expressions in the [[Project]], [[ProjectResolver]] will replace this
 * [[Project]] with [[Window]] operator. Because [[Window.projectList]] needs to retain the same
 * output schema as the original [[Project]], we  need to place additional [[Project]] node below
 * [[Window]], in order to extract window's source expressions.
 *
 * Analyzed logical plan will contain a [[Window]] and a [[Project]] operator:
 *
 * Window [sum(salary#1) windowspecdefinition(...)#2], [exists# [dept_id#0]]
 * +- Project [dept_id#0, salary#1]
 *    +- SubqueryAlias emp
 *       +- ...
 *
 * Collected window source expressions for outer `SELECT`: [dept_id#0, salary#1].
 * Collected window source expressions for inner `SELECT`: [dept_id#2], expressions collected here
 * are not used for window construction since there are no window expressions in the subquery's
 * project list.
 *
 * Here is an example showcasing the behavior in an [[Aggregate]] with [[WindowExpression]]:
 *
 * {{{
 * SELECT
 *   SUM(1 + SUM(col1)) OVER (PARTITION BY ABS(col2 + col3))
 * FROM
 *   VALUES (1, 2, 1)
 * GROUP BY col2 + col3
 * }}}
 *
 * [[AggregateResolver]] will replace [[Aggregate]] with [[Window]] operator. For
 * [[Window.projectList]] to retain the same output schema as the original [[Aggregate]],
 * we will place an [[Aggregate]] node below [[Window]], in order to extract window's source
 * expressions. The aggregate expression list will contain internal aliases for `col2 + col3`
 * and `sum(col1)`:
 *
 * Window [sum((1 + _w1#4)) windowspecdefinition(abs(_w0#3), ...)#5], [abs(_w0#3)]
 * +- Aggregate [(col2#1 + col3#2)], [(col2#1 + col3#2) AS _w0#3, sum(col1#0) as _w1#4]
 *
 * Collected window source expressions: [(col2 + col3), col1#0]
 *
 * Next example is similar, the difference being we also need to take care of the grouping
 * expression aliases:
 *
 * {{{
 * SELECT
 *   (col1 + col2) as a,
 *   SUM(col1 + col2) OVER ()
 * FROM VALUES (1, 2, 3)
 * GROUP BY col1 + col2;
 * }}}
 *
 * Parsed plan:
 *
 * 'Aggregate [('col1 + 'col2)],
 *            [('col1 + 'col2) AS a#465074, unresolvedalias('SUM(('col1 + 'col2)) windowspec]
 *
 * The aggregate expression list now contains both the internal `col1 + col2` alias `_w0` and its
 * explicit alias `a`:
 *
 * Window [a#2, sum(_w0#3) windowspec...#4]
 * +- Aggregate [(col1#0 + col2#1)], [(col1#0 + col2#1) AS a#2, (col1#0 + col2#1) AS _w0#3]
 *
 * Collected window source expressions: [(col1 + col2) as a, col1 + col2]
 *
 * We primarily differentiate between four cases which need to be handled:
 *  - Attributes in the project list:
 *     - [[AttributeReference]] handled by [[ExpressionResolver.handleResolvedAttributeReference]].
 *     - [[Attribute]] handled by [[ExpressionResolver.resolveAttribute]].
 *     - [[OuterReference]] handled by [[ExpressionResolver.resolveAttribute]].
 *     - [[ExtractValue]] needs special handling, e.g. in the case of nesting such as
 *       `col.a`, we should collect the [[AttributeReference]] of (`col`), not the [[ExtractValue]]
 *       expression. This is handled by [[ExpressionResolver.handleResolvedExtractValue]].
 *  - Outer expressions within [[SubqueryExpression]]:
 *     - In the first example, emp.dept_id comes from the outside of the subquery, thus it needs
 *       special handling compared to the other attributes. Since it does not appear in the outer
 *       [[Project]] we collect it once we finish resolution of the subquery.
 *  - Aggregate expressions:
 *     - [[AggregateExpression]] handled by [[AggregateExpressionResolver]].
 *     - Aggregate expression under [[Alias]] handled by [[AliasResolver]].
 *  - Grouping expressions:
 *     - In the second example, `col2 + col3` appears both in the aggregate expressions list and the
 *       grouping expressions list, so final [[Window]] will reference it from child [[Aggregate]].
 */
trait CollectsWindowSourceExpressions {
  protected val windowResolutionContextStack: WindowResolutionContextStack

  /**
   * Adds the expression to the current [[WindowResolutionContext]] if it comes from a project list
   * or an aggregate expression list.
   */
  protected def collectWindowSourceExpression(
      expression: Expression,
      parentOperator: LogicalPlan): Unit = {
    parentOperator match {
      case _: Project | _: Aggregate =>
        windowResolutionContextStack.current.windowSourceExpressions.add(expression.canonicalized)
      case _ =>
    }
  }

  /**
   * Retrieves [[SemanticComparator]] containing expressions collected in the current context.
   */
  protected def getWindowSourceExpressionsSemanticComparator(): SemanticComparator =
    windowResolutionContextStack.current.windowSourceExpressions

  /**
   * Collects grouping expressions and their aliases from aggregate expression list.
   *
   * Unlike the other types of window source expressions, grouping expressions and their aliases are
   * collected after the aggregate expression list has been fully resolved. This is because their
   * resolution is dependent on the former.
   *
   * Grouping expressions should only be extracted if they appear in the aggregate expression
   * list. Notice that if the aliases do exist they will always occur as top-level expressions,
   * so it is enough to traverse the aggregate expression list without recursively traversing the
   * expression trees.
   *
   * Consider the following query:
   *
   * {{{ SELECT col1 + 1, SUM(SUM(col2)) OVER () FROM VALUES (1, 2) GROUP BY col1 + 1, col2; }}}
   *
   * Parsed logical plan before the resolution will have this aggregate:
   *
   * 'Aggregate [('col1 + 1), 'col2],
   *            [unresolvedalias(('col1 + 1)), unresolvedalias('SUM('SUM('col2)) windowspec]
   *
   * The resolved plan will contain a [[Window]] and a child [[Aggregate]] to provide `SUM(col2)`
   * and `col1 + 1`. It should not include `col2` though since it does not appear as a top-level
   * expression in the aggregate expression list.
   *
   * Window [(col1 + 1)#2, sum(_w0#3) windowspec...#4]
   * +- Aggregate [(col1#0 + 1), col2#1], [(col1#0 + 1) AS (col1 + 1)#2, sum(col2#1) AS _w0#3]
   *
   * Here's another example demonstrating the same behavior but with grouping expression aliases:
   *
   * {{{
   * SELECT
   *   (col1 + col2) as a,
   *   SUM(col1 + col2) OVER ()
   * FROM VALUES (1, 2, 3)
   * GROUP BY col1 + col2;
   * }}}
   *
   * Parsed plan:
   *
   * 'Aggregate [('col1 + 'col2)],
   *            [('col1 + 'col2) AS a#2, unresolvedalias('SUM(('col1 + 'col2)) windowspec]
   *
   * Analyzed plan:
   *
   * Window [a#2, sum(_w1#3) windowspec...#4]
   * +- Aggregate [(col1#0 + col2#1)], [(col1#0 + col2#1) AS a#2, (col1#0 + col2#1) AS _w1#3]
   */
  protected def collectGroupingAndAggregateSourceExpressions(resolvedAggregate: Aggregate): Unit = {
    for (expression <- resolvedAggregate.groupingExpressions) {
      collectWindowSourceExpression(
        expression = expression,
        parentOperator = resolvedAggregate
      )
    }

    val groupingExpressionsSemanticComparator =
      new SemanticComparator(resolvedAggregate.groupingExpressions)

    for (expression <- resolvedAggregate.aggregateExpressions) {
      expression match {
        case alias @ Alias(child, _) =>
          groupingExpressionsSemanticComparator.collectFirst(child) match {
            case Some(_) =>
              collectWindowSourceExpression(
                expression = alias,
                parentOperator = resolvedAggregate
              )
            case _ =>
          }
        case _ =>
      }
    }
  }
}
