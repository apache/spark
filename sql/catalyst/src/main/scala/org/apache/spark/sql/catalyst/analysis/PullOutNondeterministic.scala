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

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.ExprUtils.toSQLExpr
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Pulls out nondeterministic expressions from LogicalPlan which is not Project or Filter,
 * put them into an inner Project and finally project them away at the outer Project.
 */
object PullOutNondeterministic extends Rule[LogicalPlan] with PredicateHelper {
  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsUp applyLocally

  val applyLocally: PartialFunction[LogicalPlan, LogicalPlan] = {
    case p if !p.resolved => p // Skip unresolved nodes.
    case p: Project => p
    case f: Filter => f

    case a: Aggregate =>
      val expressionsToExtract = a.groupingExpressions ++ nondeterministicSubqueriesInInputs(a)
      if (expressionsToExtract.forall(_.deterministic)) a else {
        validateNondeterministicSubqueries(a, expressionsToExtract)
        val nondeterToAttr =
          NondeterministicExpressionCollection.getNondeterministicToAttributes(expressionsToExtract)
        val newChild = Project(a.child.output ++ nondeterToAttr.values.asScala.toSeq, a.child)
        val deterministicAggregate = a.transformExpressions { case e =>
          Option(nondeterToAttr.get(NondeterministicExpressionCollection.getExpressionKey(e)))
            .map(_.toAttribute).getOrElse(e)
        }.copy(child = newChild)

        deterministicAggregate.groupingExpressions.foreach(expr => if (!expr.deterministic) {
          throw SparkException.internalError(
            msg = s"Non-deterministic expression '${toSQLExpr(expr)}' should not appear in " +
              "grouping expression.",
            context = expr.origin.getQueryContext,
            summary = expr.origin.context.summary)
        })

        deterministicAggregate
      }

    case join: Join if join.condition.exists(_.exists {
        case scalar: ScalarSubquery => !scalar.deterministic && !scalar.isCorrelated
        case _ => false
      }) =>
      validateNondeterministicSubqueries(join, join.condition.toSeq)
      val scalars = join.condition.toSeq.flatMap(_.collect {
        case scalar: ScalarSubquery if !scalar.deterministic && !scalar.isCorrelated => scalar
      })
      val nondeterToAttr =
        NondeterministicExpressionCollection.getNondeterministicToAttributes(scalars)
      val condition = join.condition.map(_.transform {
        case scalar: ScalarSubquery =>
          Option(nondeterToAttr.get(NondeterministicExpressionCollection.getExpressionKey(scalar)))
            .map(_.toAttribute).getOrElse(scalar)
      })
      // An uncorrelated scalar does not depend on either input. Preserve the join's original
      // output, including outer-join nullability and semi/anti-join column visibility.
      val uses = mutable.HashMap.empty[ExprId, AttributeSet]
      join.condition.toSeq.flatMap(splitConjunctivePredicates).foreach { conjunct =>
        val references = conjunct.references
        conjunct.foreach {
          case scalar: ScalarSubquery if !scalar.deterministic && !scalar.isCorrelated =>
            uses(scalar.exprId) = uses.getOrElse(scalar.exprId, AttributeSet.empty) ++ references
          case _ =>
        }
      }
      val (rightScalars, leftScalars) = nondeterToAttr.values.asScala.toSeq.partition {
        case Alias(scalar: ScalarSubquery, _) =>
          val references = uses(scalar.exprId)
          references.nonEmpty && references.subsetOf(join.right.outputSet)
        case _ => false
      }
      def project(child: LogicalPlan, scalars: Seq[NamedExpression]): LogicalPlan =
        if (scalars.isEmpty) child else Project(child.output ++ scalars, child)
      Project(join.output, join.copy(left = project(join.left, leftScalars),
        right = project(join.right, rightScalars), condition = condition))

    // Don't touch collect metrics. Top-level metrics are not supported (check analysis will fail)
    // and we want to retain them inside the aggregate functions.
    case m: CollectMetrics => m

    // Skip PythonUDTF as it will be planned as its own dedicated logical and physical node.
    case g @ Generate(_: PythonUDTF, _, _, _, _, _) => g

    // todo: It's hard to write a general rule to pull out nondeterministic expressions
    // from LogicalPlan, currently we only do it for UnaryNode which has same output
    // schema with its child.
    case p: UnaryNode if p.output == p.child.output && p.expressions.exists(!_.deterministic) =>
      validateNondeterministicSubqueries(p, p.expressions)
      val nondeterToAttr =
        NondeterministicExpressionCollection.getNondeterministicToAttributes(p.expressions)
      val newPlan = p.transformExpressions { case e =>
        Option(nondeterToAttr.get(NondeterministicExpressionCollection.getExpressionKey(e)))
          .map(_.toAttribute).getOrElse(e)
      }
      val newChild = Project(p.child.output ++ nondeterToAttr.values.asScala.toSeq, p.child)
      Project(p.output, newPlan.withNewChildren(newChild :: Nil))
  }

  private def nondeterministicSubqueriesInInputs(
      aggregate: Aggregate): Seq[Expression] = {
    // Only complete subquery expressions may be extracted. Direct nondeterministic
    // inputs, such as rand() and nondeterministic UDFs, must retain their validation errors.
    aggregate.aggregateExpressions
      .flatMap(_.collect { case expression: AggregateExpression => expression })
      .flatMap(expression => expression.aggregateFunction.children ++ expression.filter.toSeq)
      .flatMap(_.collect {
        case scalar: ScalarSubquery if !scalar.deterministic => scalar
        case exists: Exists if !exists.deterministic => exists
        case not @ Not(in: InSubquery)
            if !in.deterministic &&
              in.values.forall(ExprUtils.isDeterministicAfterSubqueryExtraction) => not
        case in: InSubquery if !in.deterministic &&
            in.values.forall(ExprUtils.isDeterministicAfterSubqueryExtraction) => in
      })
  }

  private def validateNondeterministicSubqueries(
      plan: LogicalPlan,
      expressions: Seq[Expression]): Unit = {
    expressions.foreach(_.foreach {
      // Moving a correlated scalar into a Project must not bypass its original host's
      // restrictions, including grouping-only correlated scalar subqueries.
      case subquery: SubqueryExpression if !subquery.deterministic =>
        ValidateSubqueryExpression(plan, subquery)
      case _ =>
    })
  }
}
