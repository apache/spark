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

package org.apache.spark.sql.execution.streaming.runtime

import scala.collection.mutable.{Map => MutableMap, Set => MutableSet}

import org.apache.spark.sql.catalyst.analysis.{KllAggregatePreparation, NondeterministicExpressionCollection, PullOutNondeterministic}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{KllMergeAggBigint, KllMergeAggDouble, KllMergeAggFloat, KllSketchAggBigint, KllSketchAggDouble, KllSketchAggFloat}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.TreePattern._

/** Checkpoint-owned logical semantics shared by startup scans and micro-batch normalization. */
private[sql] object KllCheckpointPreparation {
  def apply(
      preOptimized: LogicalPlan,
      original: LogicalPlan,
      legacySharing: Boolean): LogicalPlan = {
    // Ordinary scalar/UDF extraction also creates compiler bindings. Their provenance alone
    // does not require KLL restoration. Check both inputs before normalization can hide a producer.
    if (!preOptimized.containsAnyPattern(KLL_AGGREGATE, KLL_CHECKPOINT_ALTERNATIVE) &&
        !original.containsAnyPattern(KLL_AGGREGATE, KLL_CHECKPOINT_ALTERNATIVE)) {
      return preOptimized
    }
    // Checkpoints encode aggregate buffers by ordinal. Their recorded sharing mode takes
    // precedence over the analysis/batch rollback setting captured on KLL expressions.
    var needsExtraction = false
    var kllChanged = false
    val unchosen = MutableSet.empty[ExprId]
    val reclassify: PartialFunction[Expression, Expression] = {
      case k: KllSketchAggBigint if k.legacyDeterministic != legacySharing =>
        kllChanged = true
        needsExtraction = !legacySharing
        k.copy(legacyDeterministic = legacySharing)
      case k: KllSketchAggFloat if k.legacyDeterministic != legacySharing =>
        kllChanged = true
        needsExtraction = !legacySharing
        k.copy(legacyDeterministic = legacySharing)
      case k: KllSketchAggDouble if k.legacyDeterministic != legacySharing =>
        kllChanged = true
        needsExtraction = !legacySharing
        k.copy(legacyDeterministic = legacySharing)
      case k: KllMergeAggBigint if k.legacyDeterministic != legacySharing =>
        kllChanged = true
        needsExtraction = !legacySharing
        k.copy(legacyDeterministic = legacySharing)
      case k: KllMergeAggFloat if k.legacyDeterministic != legacySharing =>
        kllChanged = true
        needsExtraction = !legacySharing
        k.copy(legacyDeterministic = legacySharing)
      case k: KllMergeAggDouble if k.legacyDeterministic != legacySharing =>
        kllChanged = true
        needsExtraction = !legacySharing
        k.copy(legacyDeterministic = legacySharing)
    }
    def selectExpression(expression: Expression): Expression = {
      if (!expression.containsAnyPattern(KLL_AGGREGATE, KLL_CHECKPOINT_ALTERNATIVE)) {
        return expression
      }
      expression match {
        case resolution: KllAggregateResolution =>
          val chosen = if (legacySharing) resolution.legacy else resolution.corrected
          unchosen ++= KllAggregatePreparation.unchosenOutputs(resolution, chosen)
          selectExpression(chosen)
        case other =>
          reclassify.applyOrElse(other, identity[Expression]) match {
            case query: SubqueryExpression =>
              val children = query.mapChildrenWithReferenceEquality(selectExpression)
                .asInstanceOf[SubqueryExpression]
              val nested = selectPlan(query.plan)
              if (nested eq query.plan) children else children.withNewPlan(nested)
            case updated => updated.mapChildrenWithReferenceEquality(selectExpression)
          }
      }
    }
    def selectPlan(input: LogicalPlan): LogicalPlan = {
      if (!input.containsAnyPattern(KLL_AGGREGATE, KLL_CHECKPOINT_ALTERNATIVE)) return input
      input.mapChildrenWithReferenceEquality(selectPlan)
        .mapExpressionsWithReferenceEquality(selectExpression)
    }
    val selected = selectPlan(preOptimized)
    // Alternative resolution outputs are compiler-owned state demands. Retire those that
    // the selected consumer no longer uses even when optional ColumnPruning is disabled.
    val adjusted = if (unchosen.isEmpty) selected else {
      KllAggregatePreparation.retireUnchosenOutputs(selected, unchosen.toSet)
    }
    // Retain original compiler identities even if custom normalization replaces a producer
    // or its provenance before checkpoint preparation.
    val originalBindings = if (legacySharing) {
      NondeterministicExpressionCollection.extractedBindingIds(original)
    } else Set.empty[ExprId]
    val optimizerInput = if (legacySharing && (kllChanged || originalBindings.nonEmpty ||
        adjusted.containsPattern(KLL_CHECKPOINT_ALTERNATIVE))) {
      // Undo only compiler bindings exposed by the KLL metadata change. Restore the original
      // expressions before matching historical UDF bindings: separate IN predicates acquire
      // separate existence attributes during rewriting, while equal scalar SUMs historically share.
      def restoreInput(
          input: LogicalPlan,
          independentOnly: Boolean): (LogicalPlan, MutableMap[ExprId, Expression]) = {
        val bindings = MutableMap.empty[ExprId, Expression]
        if (!input.containsAnyPattern(
            NONDETERMINISTIC_SUBQUERY_BINDING, KLL_CHECKPOINT_ALTERNATIVE)) {
          return (input, bindings)
        }
        def restore(expression: Expression): Expression = {
          NondeterministicExpressionCollection.restoreBindingReferences(
            expression, bindings, stripFilterGuards = true)
        }
        def isUdfBinding(expression: Expression): Boolean = expression match {
          case _: UserDefinedExpression | _: ExternalUserDefinedFunction => true
          case If(check, Literal(null, _), udf: ScalaUDF) =>
            // HandleNullInputsForUDF wraps nullable primitive inputs after binding extraction.
            // Recognize its exact NULL guard before recovering historical UDF key sharing.
            udf.inputPrimitives.zip(udf.children).collect {
              case (true, KnownNotNull(child)) => IsNull(child): Expression
            }.reduceLeftOption(Or).exists(_.semanticEquals(check))
          case _ => false
        }
        def visit(plan: LogicalPlan): LogicalPlan = plan match {
          case project: Project =>
            val child = visit(project.child)
            val udfBindings = MutableMap.empty[Expression, Attribute]
            val retained = project.projectList.flatMap {
              case a: Attribute if bindings.contains(a.exprId) => None
              case original =>
                val expression = restore(original).asInstanceOf[NamedExpression]
                expression match {
                  case alias: Alias if alias.isNondeterministicSubqueryBinding &&
                      alias.child.deterministic &&
                      (!independentOnly || alias.child.references.isEmpty) =>
                    bindings(alias.exprId) = alias.child
                    None
                  case alias: Alias if alias.isNondeterministicSubqueryBinding &&
                      isUdfBinding(alias.child) &&
                      !alias.child.exists(e =>
                        e.isInstanceOf[SubqueryExpression] && !e.deterministic) =>
                    // Restoring deterministic scalar inputs can make UDF bindings canonically
                    // equal again. Retain one evaluation, as historical extraction did. Random
                    // inputs remain attributes, and independent random subqueries stay distinct.
                    val key = alias.child.canonicalized
                    udfBindings.get(key) match {
                      case Some(attribute) =>
                        bindings(alias.exprId) = attribute
                        None
                      case None =>
                        udfBindings(key) = alias.toAttribute
                        Some(alias)
                    }
                  case other => Some(other)
                }
            }
            project.copy(projectList = retained, child = child)
          // These operators forward every input attribute. Avoid recursive output comparisons
          // and keep one mutable binding environment for the entire chain of Projects.
          case unary: UnaryNode if unary.isInstanceOf[Filter] || unary.isInstanceOf[Sort] ||
              unary.isInstanceOf[RepartitionOperation] || unary.isInstanceOf[SubqueryAlias] ||
              unary.isInstanceOf[ResolvedHint] =>
            val child = visit(unary.child)
            if (bindings.isEmpty) unary else {
              unary.withNewChildren(Seq(child)).mapExpressionsWithReferenceEquality(restore)
            }
          case other => other
        }
        (visit(input), bindings)
      }
      NondeterministicExpressionCollection.restoreBindingProvenance(originalBindings, adjusted)
        .transformUpWithSubqueriesAndPruning(_.containsAnyPattern(
          NONDETERMINISTIC_SUBQUERY_BINDING, KLL_CHECKPOINT_ALTERNATIVE)) {
        case join: Join =>
          val (left, leftBindings) = restoreInput(join.left, independentOnly = true)
          val (right, rightBindings) = restoreInput(join.right, independentOnly = true)
          leftBindings ++= rightBindings
          val bindings = leftBindings
          if (bindings.isEmpty) join else {
            def restore(expression: Expression): Expression =
              NondeterministicExpressionCollection.restoreBindingReferences(
                expression, bindings, stripFilterGuards = true)
            val restored = join.copy(left = left, right = right,
              condition = join.condition.map(restore))
            Project(join.output.map { attribute =>
              if (bindings.contains(attribute.exprId)) {
                Alias(restore(attribute), attribute.name)(attribute.exprId,
                  attribute.qualifier, Some(attribute.metadata)).asNondeterministicSubqueryBinding
              } else attribute
            }, restored)
          }
        case aggregate: Aggregate =>
          val (child, bindings) = restoreInput(aggregate.child, independentOnly = false)
          if (bindings.isEmpty && !aggregate.containsPattern(KLL_CHECKPOINT_ALTERNATIVE)) {
            aggregate
          } else {
            def restore(expression: Expression): Expression =
              NondeterministicExpressionCollection.restoreBindingReferences(
                expression, bindings, stripFilterGuards = true)
            val restored = aggregate.copy(
              groupingExpressions = aggregate.groupingExpressions.map(restore),
              aggregateExpressions = aggregate.aggregateExpressions.map {
                case a: Attribute if bindings.contains(a.exprId) =>
                  Alias(restore(a), a.name)(a.exprId, a.qualifier, Some(a.metadata))
                case other => restore(other).asInstanceOf[NamedExpression]
              }, child = child)
            restored
          }
      }
    } else if (needsExtraction) {
      // Analysis in rollback mode did not stage newly nondeterministic subqueries. Apply the
      // same extraction before optimizing, including within scalar subquery plans.
      adjusted.transformUpWithSubqueriesAndPruning(_.containsAnyPattern(
        KLL_AGGREGATE, KLL_CHECKPOINT_ALTERNATIVE, NONDETERMINISTIC_SUBQUERY_BINDING))(
        PullOutNondeterministic.applyLocally)
    } else {
      adjusted
    }
    optimizerInput
  }
}
