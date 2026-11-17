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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.ExprUtils.toSQLExpr
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction}
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

    // Let ordinary aggregate validation report the original invalid grouping expression.
    case a: Aggregate if a.groupingExpressions.exists(_.exists {
        case _: AggregateExpression | _: AggregateFunction => true
        case _ => false
      }) => a

    case a: Aggregate =>
      val expressionsToExtract = a.groupingExpressions ++ nondeterministicSubqueriesInInputs(a)
      if (expressionsToExtract.forall(_.deterministic)) a else {
        validateNondeterministicSubqueries(a, expressionsToExtract)
        val keys = new NondeterministicExpressionCollection.ExpressionKeys
        val nondeterToAttr =
          NondeterministicExpressionCollection.getNondeterministicToAttributes(
            expressionsToExtract, keys)
        val newChild = NondeterministicExpressionCollection.project(a.child, nondeterToAttr, keys)
        val deterministicAggregate = a.transformExpressions { case e =>
          Option(nondeterToAttr.get(keys(e)))
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

    case join: LateralJoin if join.condition.exists(_.exists {
        case scalar: ScalarSubquery =>
          ValidateSubqueryExpression.isIndependentKllScalar(scalar)
        case _ => false
      }) =>
      val scalars = join.condition.toSeq.flatMap(_.collect {
        case scalar: ScalarSubquery
            if ValidateSubqueryExpression.isIndependentKllScalar(scalar) => scalar
      })
      validateNondeterministicSubqueries(join, scalars)
      val keys = new NondeterministicExpressionCollection.ExpressionKeys
      val bindings =
        NondeterministicExpressionCollection.getNondeterministicToAttributes(scalars, keys)
      val condition = join.condition.map(_.transformDown {
        case scalar: ScalarSubquery if bindings.containsKey(keys(scalar)) =>
          bindings.get(keys(scalar)).toAttribute
      })
      Project(join.output, join.copy(
        left = NondeterministicExpressionCollection.project(join.left, bindings, keys),
        condition = condition))

    case join: Join if join.condition.exists(_.exists {
        case scalar: ScalarSubquery => !scalar.deterministic && !scalar.isCorrelated
        case _ => false
      }) =>
      validateNondeterministicSubqueries(join, join.condition.toSeq)
      val scalars = join.condition.toSeq.flatMap(_.collect {
        case scalar: ScalarSubquery if !scalar.deterministic && !scalar.isCorrelated => scalar
      })
      val keys = new NondeterministicExpressionCollection.ExpressionKeys
      val nondeterToAttr =
        NondeterministicExpressionCollection.getNondeterministicToAttributes(scalars, keys)
      val leftBindings = mutable.LinkedHashMap.empty[
        NondeterministicExpressionCollection.ExpressionKey, NamedExpression]
      val rightBindings = mutable.LinkedHashMap.empty[
        NondeterministicExpressionCollection.ExpressionKey, NamedExpression]
      val uses = mutable.HashMap.empty[ExprId, AttributeSet]
      val rightOutput = join.right.outputSet
      def rewrite(expression: Expression, references: AttributeSet): Expression = expression match {
        case scalar: ScalarSubquery if nondeterToAttr.containsKey(keys(scalar)) =>
          val key = keys(scalar)
          val side = if (references.nonEmpty && references.subsetOf(rightOutput)) {
            rightBindings
          } else leftBindings
          val binding = side.getOrElseUpdate(key, {
            // One scalar identity may occur in both equi-key operands. Keep attributes local to
            // each input while preserving the scalar's original identity and physical reuse policy.
            val alias = nondeterToAttr.get(key).newInstance()
            alias.copyTagsFrom(nondeterToAttr.get(key))
            alias
          })
          uses.get(binding.exprId) match {
            case Some(previous) if !(previous eq references) =>
              uses(binding.exprId) = previous ++ references
            case None => uses(binding.exprId) = references
            case _ =>
          }
          binding.toAttribute
        case comparison: BinaryComparison =>
          comparison.mapChildren { operand =>
            val local = operand.references
            rewrite(operand, if (local.nonEmpty) local else references)
          }
        case other => other.mapChildren(rewrite(_, references))
      }
      // Locate comparisons beneath Boolean and SQL-function wrappers, not only at the conjunct
      // root. Attribute-free operands use their surrounding predicate as a placement fallback.
      val condition = join.condition.map { condition =>
        splitConjunctivePredicates(condition).map { conjunct =>
          rewrite(conjunct, conjunct.references)
        }.reduceLeft(And)
      }
      def project(child: LogicalPlan, bindings: Iterable[NamedExpression]): LogicalPlan = {
        if (bindings.isEmpty) child else child match {
          // Independent scalars can be attached within an unhinted inner component. Keeping
          // its relations visible lets both join-ordering rules choose selective joins first.
          case inner: Join
              if inner.joinType.isInstanceOf[org.apache.spark.sql.catalyst.plans.InnerLike]
              && inner.hint == JoinHint.NONE =>
            val rightOutput = inner.right.outputSet
            val (right, left) = bindings.partition { binding =>
              val references = uses(binding.exprId)
              references.nonEmpty && references.subsetOf(rightOutput)
            }
            inner.copy(left = project(inner.left, left), right = project(inner.right, right))
          case _ => Project(child.output ++ bindings.toSeq, child)
        }
      }
      Project(join.output, join.copy(left = project(join.left, leftBindings.values),
        right = project(join.right, rightBindings.values), condition = condition))

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
      val keys = new NondeterministicExpressionCollection.ExpressionKeys
      val nondeterToAttr =
        NondeterministicExpressionCollection.getNondeterministicToAttributes(p.expressions, keys)
      val newPlan = p.transformExpressions { case e =>
        Option(nondeterToAttr.get(keys(e)))
          .map(_.toAttribute).getOrElse(e)
      }
      val newChild = NondeterministicExpressionCollection.project(p.child, nondeterToAttr, keys)
      Project(p.output, newPlan.withNewChildren(newChild :: Nil))
  }

  private def nondeterministicSubqueriesInInputs(
      aggregate: Aggregate): Seq[Expression] = {
    val deterministic = new ExprUtils.SubqueryExtractionDeterminism
    // Only complete subquery expressions may be extracted. Direct nondeterministic
    // inputs, such as rand() and nondeterministic UDFs, must retain their validation errors.
    aggregate.aggregateExpressions
      .flatMap(_.collect { case expression: AggregateExpression => expression })
      .flatMap(expression => expression.aggregateFunction.children ++ expression.filter.toSeq)
      // Retain invalid direct-random arguments verbatim for the original error rendering.
      .filter(deterministic.apply)
      .flatMap(_.collect {
        case scalar: ScalarSubquery if !scalar.deterministic => scalar
        case exists: Exists if !exists.deterministic => exists
        case not @ Not(in: InSubquery)
            if !in.deterministic &&
              in.values.forall(deterministic.apply) => not
        case in: InSubquery if !in.deterministic &&
            in.values.forall(deterministic.apply) => in
      })
  }

  private def validateNondeterministicSubqueries(
      plan: LogicalPlan,
      expressions: Seq[Expression]): Unit = {
    val visited = new java.util.IdentityHashMap[Expression, java.lang.Boolean]
    def validate(expression: Expression): Unit = {
      if (visited.put(expression, true) == null) {
        expression match {
          // Extraction must retain the restrictions of the subquery's original host.
          case subquery: SubqueryExpression if !subquery.deterministic =>
            ValidateSubqueryExpression(plan, subquery)
          case _ =>
        }
        expression.children.foreach(validate)
      }
    }
    expressions.foreach(validate)
  }
}
