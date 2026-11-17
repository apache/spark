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
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{EXISTS_SUBQUERY, IN_SUBQUERY, KLL_CHECKPOINT_ALTERNATIVE, RUNTIME_REPLACEABLE, SCALAR_SUBQUERY, TreePattern}
import org.apache.spark.sql.types.{DataType, LongType}

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

    // Let ordinary aggregate validation report the original invalid grouping expression.
    case a: Aggregate if a.groupingExpressions.exists(e => !e.deterministic && e.exists {
        case _: AggregateExpression | _: AggregateFunction => true
        case _ => false
      }) => a

    case a: Aggregate =>
      val guarded = guardAggregateInputs(a)
      val expressionsToExtract =
        guarded.groupingExpressions ++ nondeterministicSubqueriesInInputs(guarded)
      if (expressionsToExtract.forall(_.deterministic)) a else {
        validateNondeterministicSubqueries(a, expressionsToExtract)
        val keys = new NondeterministicExpressionCollection.ExpressionKeys
        val nondeterToAttr =
          NondeterministicExpressionCollection.getNondeterministicToAttributes(
            expressionsToExtract, keys)
        val newChild =
          NondeterministicExpressionCollection.project(guarded.child, nondeterToAttr, keys)
        val deterministicAggregate = guarded.mapExpressionsWithReferenceEquality { expression =>
          NondeterministicExpressionCollection.replaceBindings(expression, nondeterToAttr, keys)
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

    case join: LateralJoin if join.condition.exists(_.containsPattern(SCALAR_SUBQUERY)) =>
      val independent = new ValidateSubqueryExpression.KllSubqueryIndependence
      val scalars = join.condition.toSeq.flatMap(_.collect {
        case scalar: ScalarSubquery if independent(scalar) => scalar
      })
      if (scalars.isEmpty) join else {
        validateNondeterministicSubqueries(join, scalars)
        val keys = new NondeterministicExpressionCollection.ExpressionKeys
        val bindings =
          NondeterministicExpressionCollection.getNondeterministicToAttributes(scalars, keys)
        val condition = join.condition.map(_.transformDownWithReferenceEquality {
          case scalar: ScalarSubquery if bindings.containsKey(keys(scalar)) =>
            bindings.get(keys(scalar)).toAttribute
        })
        Project(join.output, join.copy(
          left = NondeterministicExpressionCollection.project(join.left, bindings, keys),
          condition = condition))
      }

    case join: Join if join.condition.exists(e => e.containsPattern(SCALAR_SUBQUERY) && e.exists {
        case scalar: ScalarSubquery => !scalar.deterministic && !scalar.isCorrelated
        case _ => false
      }) =>
      validateNondeterministicSubqueries(join, join.condition.toSeq)
      val keys = new NondeterministicExpressionCollection.ExpressionKeys
      val nondeterToAttr = new java.util.LinkedHashMap[
        NondeterministicExpressionCollection.ExpressionKey, NamedExpression]
      def collect(expression: Expression): Unit = {
        val candidate = expression match {
          // Store the consumed count, rather than copying a binary sketch into every input row
          // and state record. Keep the scalar identity inside the getter for physical reuse.
          case getter: KllSketchGetNBase if safeKllCount(getter) => true
          case scalar: ScalarSubquery => !scalar.deterministic && !scalar.isCorrelated
          case _ => false
        }
        if (candidate) {
          val key = keys(expression)
          if (!nondeterToAttr.containsKey(key)) {
            // TODO: Avoid materializing binary KLL scalars in input/shuffle/state payloads.
            // Retaining cached scalars requires join validation, residual placement and identity
            // handling beyond this metadata fix. Unproven decoders must keep their guards.
            nondeterToAttr.put(key,
              Alias(expression, "_nondeterministic")().asNondeterministicSubqueryBinding)
          }
        } else expression.children.foreach(collect)
      }
      join.condition.foreach(collect)
      val destinations = new java.util.IdentityHashMap[LogicalPlan, java.lang.Integer]
      val completedDestinations = new java.util.IdentityHashMap[
        LogicalPlan, java.util.IdentityHashMap[AttributeSet, java.lang.Integer]]
      val membershipDestinations = new java.util.IdentityHashMap[
        LogicalPlan, mutable.Map[AttributeSet, Int]]
      val forwarding = new java.util.IdentityHashMap[Project, java.lang.Boolean]
      def forwardsAttributes(project: Project): Boolean = {
        val cached = forwarding.get(project)
        if (cached != null) cached.booleanValue() else {
          val result = project.projectList.forall(_.isInstanceOf[Attribute])
          forwarding.put(project, result)
          result
        }
      }
      def destination(child: LogicalPlan, references: AttributeSet): Int = child match {
        case alias: SubqueryAlias => destination(alias.child, references)
        case projection: Project if forwardsAttributes(projection) =>
          destination(projection.child, references)
        case inner: Join
            if inner.joinType.isInstanceOf[org.apache.spark.sql.catalyst.plans.InnerLike] &&
              inner.hint == JoinHint.NONE =>
          val next = if (references.nonEmpty && references.subsetOf(inner.right.outputSet)) {
            inner.right
          } else inner.left
          destination(next, references)
        case other =>
          val cached = destinations.get(other)
          if (cached != null) cached.intValue() else {
            val id = destinations.size()
            destinations.put(other, id)
            id
          }
      }
      def cachedDestination(child: LogicalPlan, references: AttributeSet): Int = {
        val completed = completedDestinations.computeIfAbsent(child,
          _ => new java.util.IdentityHashMap[AttributeSet, java.lang.Integer])
        completed.computeIfAbsent(references, _ => {
          val memberships = membershipDestinations.computeIfAbsent(child,
            _ => mutable.HashMap.empty[AttributeSet, Int])
          memberships.getOrElseUpdate(references, destination(child, references))
        }).intValue()
      }
      // References to one scalar can belong to different relations within the same join input.
      // Share an alias at its actual destination, keeping each selective join edge independent.
      val leftBindings = mutable.LinkedHashMap.empty[
        (NondeterministicExpressionCollection.ExpressionKey, Int), NamedExpression]
      val rightBindings = mutable.LinkedHashMap.empty[
        (NondeterministicExpressionCollection.ExpressionKey, Int), NamedExpression]
      val referenceSets = mutable.HashMap.empty[ExprId, mutable.ArrayBuffer[AttributeSet]]
      val rightOutput = join.right.outputSet
      val rightMemberships = new java.util.IdentityHashMap[AttributeSet, java.lang.Boolean]
      def rewrite(expression: Expression, references: AttributeSet): Expression = expression match {
        case value @ (_: ScalarSubquery | _: KllSketchGetNBase)
            if nondeterToAttr.containsKey(keys(value)) =>
          val key = keys(value)
          val side = if (rightMemberships.computeIfAbsent(references,
              refs => refs.nonEmpty && refs.subsetOf(rightOutput)).booleanValue()) {
            rightBindings
          } else leftBindings
          val input = if (side eq rightBindings) join.right else join.left
          val binding = side.getOrElseUpdate((key, cachedDestination(input, references)), {
            // One scalar identity may occur in both equi-key operands. Keep attributes local to
            // each input while preserving the scalar's original identity and physical reuse policy.
            val alias = nondeterToAttr.get(key).newInstance()
            alias.copyTagsFrom(nondeterToAttr.get(key))
            alias
          })
          val sets = referenceSets.getOrElseUpdate(binding.exprId, mutable.ArrayBuffer.empty)
          if (sets.isEmpty || !(sets.last eq references)) sets += references
          binding.toAttribute
        case comparison: BinaryComparison =>
          comparison.mapChildrenWithReferenceEquality { operand =>
            val local = operand.references
            rewrite(operand, if (local.nonEmpty) local else references)
          }
        case other => other.mapChildrenWithReferenceEquality(rewrite(_, references))
      }
      // Locate comparisons beneath Boolean and SQL-function wrappers, not only at the conjunct
      // root. Attribute-free operands use their surrounding predicate as a placement fallback.
      val condition = join.condition.map { condition =>
        val conjuncts = mutable.ArrayBuffer.empty[Expression]
        val pending = mutable.Stack[Expression](condition)
        while (pending.nonEmpty) {
          pending.pop() match {
            case And(left, right) => pending.push(right).push(left)
            case conjunct => conjuncts += conjunct
          }
        }
        conjuncts.map { conjunct =>
          rewrite(conjunct, conjunct.references)
        }.reduceLeft(And)
      }
      // Union once per binding. Incremental immutable unions repeatedly copied growing prefixes.
      val uses = referenceSets.iterator.map { case (id, sets) =>
        id -> AttributeSet.fromAttributeSets(sets.toSeq)
      }.toMap
      def project(child: LogicalPlan, bindings: Iterable[NamedExpression]): LogicalPlan = {
        if (bindings.isEmpty) child else child match {
          case alias: SubqueryAlias => alias.copy(child = project(alias.child, bindings))
          case projection: Project if forwardsAttributes(projection) =>
            projection.copy(projectList = projection.projectList ++ bindings.map(_.toAttribute),
              child = project(projection.child, bindings))
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
      val newPlan = p.mapExpressionsWithReferenceEquality { expression =>
        NondeterministicExpressionCollection.replaceBindings(expression, nondeterToAttr, keys)
      }
      val newChild = NondeterministicExpressionCollection.project(p.child, nondeterToAttr, keys)
      Project(p.output, newPlan.withNewChildren(newChild :: Nil))
  }

  private def safeKllCount(getter: KllSketchGetNBase): Boolean = getter.child match {
    case scalar: ScalarSubquery if !scalar.deterministic && !scalar.isCorrelated =>
      // Hoisting an arbitrary decoder can expose an invalid buffer in an untaken CASE branch,
      // or before a join produces any candidate row. Only a matching aggregate's direct result
      // proves decoding safe; transformed buffers and other getters stay in their original scope.
      scalar.plan match {
        case Aggregate(_, Seq(Alias(result: AggregateExpression, _)), _, _) =>
          (getter, result.aggregateFunction) match {
            case (_: KllSketchGetNBigint, _: KllSketchAggBigint | _: KllMergeAggBigint) => true
            case (_: KllSketchGetNFloat, _: KllSketchAggFloat | _: KllMergeAggFloat) => true
            case (_: KllSketchGetNDouble, _: KllSketchAggDouble | _: KllMergeAggDouble) => true
            case _ => false
          }
        case _ => false
      }
    case _ => false
  }

  private def nondeterministicSubqueriesInInputs(
      aggregate: Aggregate): Seq[Expression] = {
    if (!aggregate.containsAnyPattern(SCALAR_SUBQUERY, EXISTS_SUBQUERY, IN_SUBQUERY)) {
      return Nil
    }
    val deterministic = new ExprUtils.SubqueryExtractionDeterminism
    // Collect eligible input expressions intact so the binding collector can preserve enclosing
    // CASE, Boolean and NULL guards. Direct randomness must retain its validation errors.
    aggregate.aggregateExpressions
      .filter(_.containsAnyPattern(SCALAR_SUBQUERY, EXISTS_SUBQUERY, IN_SUBQUERY))
      .flatMap(_.collect { case expression: AggregateExpression => expression })
      .flatMap(expression => expression.aggregateFunction.children ++ expression.filter.toSeq)
      // Retain invalid direct-random arguments verbatim for the original error rendering.
      .filter(e => !e.deterministic && deterministic(e))
  }

  private def guardAggregateInputs(aggregate: Aggregate): Aggregate = {
    if (!aggregate.containsAnyPattern(SCALAR_SUBQUERY, EXISTS_SUBQUERY, IN_SUBQUERY)) {
      return aggregate
    }
    val deterministic = new ExprUtils.SubqueryExtractionDeterminism
    val keys = new NondeterministicExpressionCollection.ExpressionKeys
    val filters = mutable.LinkedHashMap.empty[
      NondeterministicExpressionCollection.ExpressionKey, Alias]
    val guard: PartialFunction[Expression, Expression] = {
      case expression: AggregateExpression =>
        def needsGuard(input: Expression): Boolean = !input.deterministic && deterministic(input)
        val function = expression.aggregateFunction match {
          // Count tests only nullable arguments, in order, and skips the rest after the first
          // NULL. Stage those checks together rather than evaluating its arguments eagerly.
          // DISTINCT still needs the original argument values for its distinct keys.
          case count: Count if !expression.isDistinct && count.children.exists(needsGuard) &&
              count.children.forall(deterministic.apply) =>
            Count(KllCountAggregateInput(count.children))
          case other => other
        }
        val prepared = if (function eq expression.aggregateFunction) expression else {
          expression.copy(aggregateFunction = function)
        }
        if (!expression.filter.exists(deterministic.apply) ||
            !function.children.exists(needsGuard)) {
          prepared
        } else {
          val filter = expression.filter.get
          // Both the argument guard and FILTER consume this lower binding. Deterministic
          // predicates can also be expensive, so do not duplicate their evaluation.
          val predicate = filters.getOrElseUpdate(keys(filter),
            Alias(filter, "_aggregate_filter")().asNondeterministicSubqueryBinding).toAttribute
          val inputs = function.children.map {
            case input if needsGuard(input) => KllFilteredAggregateInput(predicate, input)
            case input => input
          }
          prepared.copy(
            aggregateFunction = function.withNewChildren(inputs).asInstanceOf[AggregateFunction],
            filter = Some(predicate))
        }
    }
    def rewrite(expression: Expression): Expression = {
      if (!expression.containsAnyPattern(SCALAR_SUBQUERY, EXISTS_SUBQUERY, IN_SUBQUERY)) {
        expression
      } else {
        guard.applyOrElse(expression, identity[Expression])
          .mapChildrenWithReferenceEquality(rewrite)
      }
    }
    val guarded = aggregate.mapExpressionsWithReferenceEquality(rewrite).asInstanceOf[Aggregate]
    if (filters.isEmpty) guarded else {
      guarded.copy(child = Project(guarded.child.output ++ filters.values.toSeq, guarded.child))
    }
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

/** Keeps Count's input checks intact until NULL handling for primitive UDFs has run. */
private[analysis] case class KllCountAggregateInput(override val children: Seq[Expression])
  extends RuntimeReplaceable {
  // Be conservative before HandleNullInputsForUDF: it can make an argument nullable later.
  override def nullable: Boolean = true
  override def dataType: DataType = LongType
  override lazy val replacement: Expression = {
    val nullableInputs = children.filter(_.nullable)
    if (nullableInputs.isEmpty) Literal(1L) else {
      If(nullableInputs.map(IsNull).reduceLeft(Or), Literal.create(null, LongType), Literal(1L))
    }
  }
  override val nodePatterns: Seq[TreePattern] =
    Seq(RUNTIME_REPLACEABLE, KLL_CHECKPOINT_ALTERNATIVE)
  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression = copy(children = newChildren)
}
