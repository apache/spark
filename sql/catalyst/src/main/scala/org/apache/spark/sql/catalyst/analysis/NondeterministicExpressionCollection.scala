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

import java.util.{IdentityHashMap, LinkedHashMap}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.trees.TreePattern._

object NondeterministicExpressionCollection {
  /** Snapshot structurally owned compiler bindings before normalization and cache substitution. */
  private[sql] def extractedBindingIds(source: LogicalPlan): Set[ExprId] = {
    if (!source.containsPattern(NONDETERMINISTIC_SUBQUERY_BINDING)) return Set.empty
    source.collectWithSubqueries {
      case project: Project => project.projectList.collect {
        case alias: Alias if alias.isNondeterministicSubqueryBinding => alias.exprId
      }
    }.flatten.toSet
  }

  private[sql] def restoreBindingProvenance(
      bindings: Set[ExprId],
      prepared: LogicalPlan): LogicalPlan = {
    if (bindings.isEmpty) prepared else prepared.transformAllExpressionsWithSubqueries {
      case alias: Alias if bindings.contains(alias.exprId) &&
          !alias.isNondeterministicSubqueryBinding =>
        alias.asNondeterministicSubqueryBinding
    }
  }

  /** Substitute compiler bindings whose values have already been expanded in dependency order. */
  private[sql] def restoreBindingReferences(
      expression: Expression,
      bindings: scala.collection.Map[ExprId, Expression],
      stripFilterGuards: Boolean = false): Expression = {
    if (bindings.isEmpty && !stripFilterGuards) return expression
    // The caller's binding map is fixed for this invocation but can grow between calls. Only
    // unchanged results are reusable: changed occurrences retain their own evaluation identities.
    val unchanged = new IdentityHashMap[Expression, java.lang.Boolean]
    def restore(value: Expression): Expression = {
      if (unchanged.containsKey(value)) return value
      val result = value match {
        case guard: KllFilteredAggregateInput if stripFilterGuards && guard.deterministic =>
          restore(guard.input)
        case count: Count if stripFilterGuards =>
          val inputs = count.children.map(restore)
          inputs match {
            case Seq(input: KllCountAggregateInput) => count.copy(children = input.children)
            case _ if count.children.lazyZip(inputs).forall(_ eq _) => count
            case _ => count.copy(children = inputs)
          }
        // Values are already expanded child-first. Keep bound replacements opaque.
        case attribute: Attribute => bindings.getOrElse(attribute.exprId, attribute)
        case other => other.mapChildrenWithReferenceEquality(restore)
      }
      if (result eq value) unchanged.put(value, true)
      result
    }
    restore(expression)
  }

  private[analysis] final class SubqueryIdentities private (
      val ids: Vector[ExprId],
      private val hash: Int,
      private val power: Int) {
    def size: Int = ids.size

    def ++(other: SubqueryIdentities): SubqueryIdentities = {
      if (ids.isEmpty) other else if (other.ids.isEmpty) this else {
        new SubqueryIdentities(ids ++ other.ids, hash * other.power + other.hash,
          power * other.power)
      }
    }

    override def hashCode(): Int = hash
    override def equals(other: Any): Boolean = other match {
      case that: SubqueryIdentities =>
        (this eq that) || (hash == that.hash && ids == that.ids)
      case _ => false
    }
  }

  private[analysis] object SubqueryIdentities {
    val empty: SubqueryIdentities = new SubqueryIdentities(Vector.empty, 0, 1)
    def apply(id: ExprId): SubqueryIdentities =
      new SubqueryIdentities(Vector(id), id.hashCode(), 31)
  }

  private[analysis] type ExpressionKey = (Expression, SubqueryIdentities)

  private[analysis] class ExpressionKeys {
    private val summaries = new IdentityHashMap[Expression, SubqueryIdentities]
    private val stagingRequirements = new IdentityHashMap[Expression, java.lang.Boolean]
    private[analysis] val liveOrdinaryBindings = mutable.HashSet.empty[ExprId]

    private def subqueryIds(expression: Expression): SubqueryIdentities = {
      if (!expression.containsPattern(PLAN_EXPRESSION)) return SubqueryIdentities.empty
      val cached = summaries.get(expression)
      if (cached != null) return cached
      // Persistent vectors share prefixes when successive wrappers add another subquery.
      val children = expression.children.foldLeft(SubqueryIdentities.empty) { (ids, child) =>
        ids ++ subqueryIds(child)
      }
      val ids = expression match {
        case subquery: SubqueryExpression if !subquery.deterministic =>
          SubqueryIdentities(subquery.exprId) ++ children
        case _ => children
      }
      summaries.put(expression, ids)
      ids
    }

    def requiresSubqueryStaging(expression: Expression): Boolean = {
      if (!expression.containsAnyPattern(SCALAR_SUBQUERY, EXISTS_SUBQUERY, IN_SUBQUERY)) {
        return false
      }
      val cached = stagingRequirements.get(expression)
      if (cached != null) return cached.booleanValue()
      val required = expression match {
        case subquery: SubqueryExpression => !subquery.deterministic
        // Even a deterministic IN query can have random operands that need input-row staging.
        case in: InSubquery => !in.deterministic
        case other => other.children.exists(requiresSubqueryStaging)
      }
      stagingRequirements.put(expression, required)
      required
    }

    def apply(expression: Expression): ExpressionKey = {
      // Preserve analyzer bindings, including subqueries inside a collected UDF. This does not
      // define physical reuse equivalence: physical subquery/exchange canonicalization can still
      // merge independent KLL evaluations, as it did before the aggregate metadata correction.
      // TODO: Carry evaluation domains through ordinary and adaptive physical reuse separately,
      // preserving the opposite requirement that references to one retained CTE remain shared.
      val ids = if (expression.containsAnyPattern(
        SCALAR_SUBQUERY, EXISTS_SUBQUERY, IN_SUBQUERY)) {
        subqueryIds(expression)
      } else SubqueryIdentities.empty
      (expression.canonicalized, ids)
    }
  }

  private def orderedNullGuard(expression: Expression): Boolean = expression match {
    // UDF dependency staging and IN's input-row operands have explicit handling below. Other
    // multi-input evaluators can guard later inputs, including variable-arity expressions.
    // Conservatively keep them whole even when current nullable metadata says no guard is needed:
    // HandleNullInputsForUDF can introduce NULLs later, and nullable can rescan whole prefixes.
    case _: UserDefinedExpression | _: ExternalUserDefinedFunction |
        _: InSubquery | _: SubqueryExpression => false
    case other => other.children.sizeCompare(1) > 0
  }

  private def guardedSubqueries(expression: Expression, keys: ExpressionKeys): Boolean = {
    // Test cached properties before inspecting the expression's evaluation domain.
    if (expression.deterministic || !keys.requiresSubqueryStaging(expression)) {
      return false
    }
    expression match {
      case _: ConditionalExpression | _: And | _: Or | _: TryEval |
          _: KllFilteredAggregateInput | _: KllCountAggregateInput => true
      case udf: ScalaUDF if udf.inputPrimitives.contains(true) => true
      case other => orderedNullGuard(other)
    }
  }

  private def bindingRoot(expression: Expression, keys: ExpressionKeys): Boolean = {
    expression match {
      // Bind an evaluation guard whole; its descendants must not become unconditional roots.
      case expression if guardedSubqueries(expression, keys) => true
      case _: Nondeterministic => true
      case scalar: ScalarSubquery => !scalar.deterministic
      case exists: Exists => !exists.deterministic
      case Not(in: InSubquery) => !in.deterministic
      case in: InSubquery => !in.deterministic
      case udf: UserDefinedExpression => !udf.deterministic
      case udf: ExternalUserDefinedFunction => !udf.deterministic
      case _ => false
    }
  }

  private def bindingFor(
      expression: Expression,
      bindings: LinkedHashMap[ExpressionKey, NamedExpression],
      keys: ExpressionKeys): NamedExpression = {
    if (bindingRoot(expression, keys)) bindings.get(keys(expression)) else null
  }

  private[analysis] def replaceBindings(
      expression: Expression,
      bindings: LinkedHashMap[ExpressionKey, NamedExpression],
      keys: ExpressionKeys): Expression = {
    if (expression.deterministic) return expression
    val binding = bindingFor(expression, bindings, keys)
    if (binding != null) binding.toAttribute else {
      expression.mapChildrenWithReferenceEquality(replaceBindings(_, bindings, keys))
    }
  }

  private[analysis] def getNondeterministicToAttributes(
      expressions: Seq[Expression],
      keys: ExpressionKeys = new ExpressionKeys)
    : LinkedHashMap[ExpressionKey, NamedExpression] = {
    val nonDeterministicToAttributes =
      new LinkedHashMap[ExpressionKey, NamedExpression]
    val visited = new IdentityHashMap[Expression, java.lang.Boolean]

    for (expr <- expressions) {
      if (!expr.deterministic) {
        val leafNondeterministic = mutable.ArrayBuffer.empty[Expression]
        def collect(expression: Expression): Unit = {
          if (visited.put(expression, true) == null) {
            if (bindingRoot(expression, keys)) leafNondeterministic += expression
            if (!guardedSubqueries(expression, keys)) expression.children.foreach(collect)
          }
        }
        collect(expr)

        for (nondeterministicExpr <- leafNondeterministic.distinct) {
          val namedExpression = nondeterministicExpr match {
            case namedExpression: NamedExpression => namedExpression
            case _ => Alias(nondeterministicExpr, "_nondeterministic")()
          }
          val binding = namedExpression match {
            case alias: Alias if nondeterministicExpr.containsAnyPattern(
                SCALAR_SUBQUERY, EXISTS_SUBQUERY, IN_SUBQUERY) =>
              alias.asNondeterministicSubqueryBinding
            case other => other
          }
          val key = keys(nondeterministicExpr)
          if (!nonDeterministicToAttributes.containsKey(key)) {
            nonDeterministicToAttributes.put(key, binding)
          }
        }
      }
    }

    if (nonDeterministicToAttributes.values.asScala.exists {
        case alias: Alias => guardedSubqueries(alias.child, keys)
        case other => guardedSubqueries(other, keys)
      }) {
      recordLiveOrdinaryBindings(expressions, nonDeterministicToAttributes, keys)
    }
    nonDeterministicToAttributes
  }

  private def recordLiveOrdinaryBindings(
      expressions: Seq[Expression],
      bindings: LinkedHashMap[ExpressionKey, NamedExpression],
      keys: ExpressionKeys): Unit = {
    val live = mutable.HashSet.empty[ExprId]
    val pending = mutable.Queue.empty[NamedExpression]
    val visitedHosts = new IdentityHashMap[Expression, java.lang.Boolean]
    val visitedDependencies = new IdentityHashMap[Expression, java.lang.Boolean]
    def demand(binding: NamedExpression): Unit = {
      if (live.add(binding.exprId)) pending.enqueue(binding)
    }
    def visit(expression: Expression, host: Boolean): Unit = {
      if (expression.deterministic) return
      // A host consumes its whole expression; dependencies observe their evaluation guards.
      // Shared prefixes need one visit in each mode, with the original first-visit ordering.
      val visited = if (host) visitedHosts else visitedDependencies
      if (visited.put(expression, true) != null) return
      val binding = bindingFor(expression, bindings, keys)
      if (binding != null) demand(binding) else if (host) {
        expression.children.foreach(visit(_, host = true))
      } else expression match {
        case conditional: ConditionalExpression =>
          conditional.alwaysEvaluatedInputs.foreach(visit(_, host = false))
        case And(left, _) => visit(left, host = false)
        case Or(left, _) => visit(left, host = false)
        case other => other.children.foreach(visit(_, host = false))
      }
    }
    expressions.foreach(visit(_, host = true))
    while (pending.nonEmpty) {
      val binding = pending.dequeue()
      val expression = binding match {
        case alias: Alias => alias.child
        case other => other
      }
      if (!keys.requiresSubqueryStaging(expression)) {
        keys.liveOrdinaryBindings += binding.exprId
      } else if (!guardedSubqueries(expression, keys)) {
        expression match {
          case Not(in: InSubquery) => in.children.foreach(visit(_, host = false))
          case other => other.children.foreach(visit(_, host = false))
        }
      }
    }
  }

  private[analysis] def project(
      child: LogicalPlan,
      bindings: LinkedHashMap[ExpressionKey, NamedExpression],
      keys: ExpressionKeys): LogicalPlan = {
    val levels = mutable.HashMap.empty[ExprId, Int]
    val rewritten = mutable.HashMap.empty[ExprId, NamedExpression]
    val unchangedLiveArms = new IdentityHashMap[Expression, java.lang.Boolean]

    def stage(binding: NamedExpression): Int = levels.getOrElseUpdate(binding.exprId, {
      var level = 0
      def replace(expression: Expression): Expression = {
        // Deterministic arguments cannot contain a collected dependency. In particular, avoid
        // canonicalizing every prefix of arithmetic expressions in ordinary UDF arguments.
        if (expression.deterministic) return expression
        val dependency = bindingFor(expression, bindings, keys)
        if (dependency != null) {
          level = math.max(level, stage(dependency) + 1)
          dependency.toAttribute
        } else expression match {
          // Dependencies in untaken branches must remain inside their conditional evaluation.
          case conditional: ConditionalExpression =>
            conditional.withNewAlwaysEvaluatedInputs(conditional.alwaysEvaluatedInputs.map(replace))
          case And(left, right) => And(replace(left), right)
          case Or(left, right) => Or(replace(left), right)
          case other if guardedSubqueries(other, keys) => other
          case other => other.mapChildrenWithReferenceEquality(replace)
        }
      }
      // Replacing only the children of the bound root keeps its own evaluation. Its
      // dependencies must be computed on the original input rows in a lower Project.
      val replacement = binding match {
        case alias: Alias =>
          val child = alias.child match {
            // Ordinary nondeterministic UDFs retain their historical single Project. Dependency
            // layers are needed only for the subqueries introduced by this extraction path.
            case expression if !keys.requiresSubqueryStaging(expression) => expression
            case expression if guardedSubqueries(expression, keys) =>
              // Reuse only ordinary producers already consumed by the host or an unconditional
              // dependency. Collected but unused descendants of ordinary UDFs are not live.
              def reuseLive(value: Expression): Expression = {
                if (value.deterministic || unchangedLiveArms.containsKey(value)) return value
                val dependency = bindingFor(value, bindings, keys)
                val result = if (dependency != null &&
                    keys.liveOrdinaryBindings.contains(dependency.exprId)) {
                  level = math.max(level, stage(dependency) + 1)
                  dependency.toAttribute
                } else value match {
                  case _: SubqueryExpression => value
                  case other => other.mapChildrenWithReferenceEquality(reuseLive)
                }
                // Changed arms must still update each binding's dependency level on every use.
                if (result eq value) unchangedLiveArms.put(value, true)
                result
              }
              if (keys.liveOrdinaryBindings.isEmpty) expression else {
                expression.mapChildrenWithReferenceEquality(reuseLive)
              }
            // NOT IN must reach the existing NULL-aware predicate-subquery rewrite as a whole.
            // Only stage its operands; extraction must not turn it into NOT of an IN binding.
            case not @ Not(in: InSubquery) =>
              not.copy(child = in.mapChildrenWithReferenceEquality(replace))
            case other => other.mapChildrenWithReferenceEquality(replace)
          }
          alias.copy(child = child)(alias.exprId, alias.qualifier, alias.explicitMetadata,
            alias.nonInheritableMetadataKeys, alias.nondeterministicSubqueryOrigin)
        case other =>
          other.mapChildrenWithReferenceEquality(replace).asInstanceOf[NamedExpression]
      }
      replacement.copyTagsFrom(binding)
      rewritten(binding.exprId) = replacement
      level
    })

    val ordered = bindings.values.asScala.toSeq
    ordered.foreach(stage)
    var output = child.output.toVector
    // TODO: Prune forwarded bindings after their last use. Full forwarding keeps dependency
    // construction simple, but logical width accumulates across dependency levels.
    ordered.groupBy(binding => levels(binding.exprId)).toSeq.sortBy(_._1).foldLeft(child) {
      case (input, (_, expressions)) =>
        val additions = expressions.iterator.map(e => rewritten(e.exprId)).toVector
        val result = Project(output ++ additions, input)
        output = output ++ additions.map(_.toAttribute)
        result
    }
  }
}
