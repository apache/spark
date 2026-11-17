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
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.trees.TreePattern._

object NondeterministicExpressionCollection {
  /** Snapshot structurally owned compiler bindings before normalization and cache substitution. */
  private[sql] def extractedBindingIds(source: LogicalPlan): Set[ExprId] = {
    if (!source.containsPattern(PLAN_EXPRESSION)) return Set.empty
    source.collectWithSubqueries {
      case project: Project => project.projectList.collect {
        case alias: Alias if alias.isNondeterministicSubqueryBinding => alias.exprId
      }
    }.flatten.toSet
  }

  private[sql] def restoreBindingProvenance(
      source: LogicalPlan,
      prepared: LogicalPlan): LogicalPlan = {
    restoreBindingProvenance(extractedBindingIds(source), prepared)
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
      bindings: scala.collection.Map[ExprId, Expression]): Expression = {
    if (bindings.isEmpty) expression else expression match {
      case attribute: Attribute => bindings.getOrElse(attribute.exprId, attribute)
      // Values are expanded child-first. Returning a bound value directly avoids revisiting it
      // once for every reference, and preserves its evaluation identity.
      case other => other.mapChildrenWithReferenceEquality(restoreBindingReferences(_, bindings))
    }
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

  private[analysis] def getExpressionKey(expression: Expression): ExpressionKey = {
    new ExpressionKeys().apply(expression)
  }

  private[analysis] class ExpressionKeys {
    private val summaries = new IdentityHashMap[Expression, SubqueryIdentities]
    private val stagingRequirements = new IdentityHashMap[Expression, java.lang.Boolean]

    private def subqueryIds(expression: Expression): SubqueryIdentities = {
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

  private def guardedSubqueries(expression: Expression): Boolean = {
    val guarded = expression match {
      case _: ConditionalExpression | _: And | _: Or => true
      case binary: BinaryExpression => binary.left.nullable
      case _ => false
    }
    guarded && !expression.deterministic && expression.containsAnyPattern(
      SCALAR_SUBQUERY, EXISTS_SUBQUERY, IN_SUBQUERY)
  }

  private val bindingRoots: PartialFunction[Expression, Expression] = {
    // Bind the evaluation guard as a whole; its descendants must not become unconditional roots.
    case expression if guardedSubqueries(expression) => expression
    case expression: Nondeterministic => expression
    case scalar: ScalarSubquery if !scalar.deterministic => scalar
    case exists: Exists if !exists.deterministic => exists
    case not @ Not(in: InSubquery) if !in.deterministic => not
    case in: InSubquery if !in.deterministic => in
    case udf: UserDefinedExpression if !udf.deterministic => udf
    case udf: ExternalUserDefinedFunction if !udf.deterministic => udf
  }

  private def bindingFor(
      expression: Expression,
      bindings: LinkedHashMap[ExpressionKey, NamedExpression],
      keys: ExpressionKeys): NamedExpression = {
    if (bindingRoots.isDefinedAt(expression)) bindings.get(keys(expression)) else null
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
            bindingRoots.lift(expression).foreach(leafNondeterministic += _)
            if (!guardedSubqueries(expression)) expression.children.foreach(collect)
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

    nonDeterministicToAttributes
  }

  private[analysis] def project(
      child: LogicalPlan,
      bindings: LinkedHashMap[ExpressionKey, NamedExpression],
      keys: ExpressionKeys): LogicalPlan = {
    val levels = mutable.HashMap.empty[ExprId, Int]
    val rewritten = mutable.HashMap.empty[ExprId, NamedExpression]

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
          case binary: BinaryExpression if binary.left.nullable =>
            binary.withNewChildren(Seq(replace(binary.left), binary.right))
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
            case expression if guardedSubqueries(expression) => expression
            // HandleNullInputsForUDF adds its guard after extraction in iterative analysis.
            // Keep all arguments under that future guard, including throwable random arguments.
            case udf: ScalaUDF if udf.inputPrimitives.zip(udf.children).exists {
                case (primitive, input) => primitive && input.nullable
              } => udf
            // NOT IN must reach predicate-subquery rewriting as a whole so that NULLs on
            // either side retain their three-valued semantics. Only stage its operands.
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
    ordered.groupBy(binding => levels(binding.exprId)).toSeq.sortBy(_._1).foldLeft(child) {
      case (input, (_, expressions)) =>
        Project(input.output ++ expressions.map(e => rewritten(e.exprId)), input)
    }
  }
}
