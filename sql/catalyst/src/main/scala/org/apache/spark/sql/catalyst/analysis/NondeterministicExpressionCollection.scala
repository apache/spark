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
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.trees.TreePattern._

object NondeterministicExpressionCollection {
  private[sql] val EXTRACTED_SUBQUERY = TreeNodeTag[Unit]("extracted-nondeterministic-subquery")

  /**
   * Extraction owns this annotation until micro-batch state preparation. TreeNode copies and
   * explicit Alias forwarding preserve it. Reconcile the original analyzed binding identities
   * after normalization and cache processing, before using provenance to restore state layouts.
   * A user alias has a different identity and must never acquire provenance by matching its child.
   */
  private[sql] def extractedBindingIds(source: LogicalPlan): Set[ExprId] = {
    if (!source.containsPattern(PLAN_EXPRESSION)) return Set.empty
    source.collectWithSubqueries {
      case project: Project => project.projectList.collect {
        case alias: Alias if alias.containsTag(EXTRACTED_SUBQUERY) => alias.exprId
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
          !alias.containsTag(EXTRACTED_SUBQUERY) =>
        val restored = alias.copy(child = alias.child)(alias.exprId, alias.qualifier,
          alias.explicitMetadata, alias.nonInheritableMetadataKeys)
        restored.copyTagsFrom(alias)
        restored.setTagValue(EXTRACTED_SUBQUERY, ())
        restored
    }
  }

  private[analysis] def getExpressionKey(expression: Expression): (Expression, Seq[ExprId]) = {
    new ExpressionKeys().apply(expression)
  }

  private[analysis] class ExpressionKeys {
    private val summaries = new IdentityHashMap[Expression, Vector[ExprId]]

    private def subqueryIds(expression: Expression): Vector[ExprId] = {
      val cached = summaries.get(expression)
      if (cached != null) return cached
      // Persistent vectors share prefixes when successive wrappers add another subquery.
      val children = expression.children.foldLeft(Vector.empty[ExprId]) { (ids, child) =>
        ids ++ subqueryIds(child)
      }
      val ids = expression match {
        case subquery: SubqueryExpression if !subquery.deterministic =>
          subquery.exprId +: children
        case _ => children
      }
      summaries.put(expression, ids)
      ids
    }

    def apply(expression: Expression): (Expression, Seq[ExprId]) = {
      // Preserve analyzer bindings, including subqueries inside a collected UDF. This does not
      // define physical reuse equivalence: physical subquery/exchange canonicalization can still
      // merge independent KLL evaluations, as it did before the aggregate metadata correction.
      // TODO: Carry evaluation domains through ordinary and adaptive physical reuse separately,
      // preserving the opposite requirement that references to one retained CTE remain shared.
      val ids = if (expression.containsAnyPattern(
        SCALAR_SUBQUERY, EXISTS_SUBQUERY, IN_SUBQUERY)) {
        subqueryIds(expression)
      } else Seq.empty
      (expression.canonicalized, ids)
    }
  }

  private[analysis] def getNondeterministicToAttributes(
      expressions: Seq[Expression],
      keys: ExpressionKeys = new ExpressionKeys)
    : LinkedHashMap[(Expression, Seq[ExprId]), NamedExpression] = {
    val nonDeterministicToAttributes =
      new LinkedHashMap[(Expression, Seq[ExprId]), NamedExpression]

    for (expr <- expressions) {
      if (!expr.deterministic) {
        val leafNondeterministic = expr.collect {
          case nondeterministicExpr: Nondeterministic => nondeterministicExpr
          case scalar: ScalarSubquery if !scalar.deterministic => scalar
          case exists: Exists if !exists.deterministic => exists
          case not @ Not(in: InSubquery) if !in.deterministic => not
          case in: InSubquery if !in.deterministic => in
          case udf: UserDefinedExpression if !udf.deterministic => udf
          case udf: ExternalUserDefinedFunction if !udf.deterministic => udf
        }

        for (nondeterministicExpr <- leafNondeterministic.distinct) {
          val namedExpression = nondeterministicExpr match {
            case namedExpression: NamedExpression => namedExpression
            case _ => Alias(nondeterministicExpr, "_nondeterministic")()
          }
          if (nondeterministicExpr.containsAnyPattern(
              SCALAR_SUBQUERY, EXISTS_SUBQUERY, IN_SUBQUERY)) {
            namedExpression.setTagValue(EXTRACTED_SUBQUERY, ())
          }
          val key = keys(nondeterministicExpr)
          if (!nonDeterministicToAttributes.containsKey(key)) {
            nonDeterministicToAttributes.put(key, namedExpression)
          }
        }
      }
    }

    nonDeterministicToAttributes
  }

  private[analysis] def project(
      child: LogicalPlan,
      bindings: LinkedHashMap[(Expression, Seq[ExprId]), NamedExpression],
      keys: ExpressionKeys): LogicalPlan = {
    val levels = mutable.HashMap.empty[ExprId, Int]
    val rewritten = mutable.HashMap.empty[ExprId, NamedExpression]

    def stage(binding: NamedExpression): Int = levels.getOrElseUpdate(binding.exprId, {
      var level = 0
      def replace(expression: Expression): Expression = {
        if (bindings.containsKey(keys(expression))) {
          val dependency = bindings.get(keys(expression))
          level = math.max(level, stage(dependency) + 1)
          dependency.toAttribute
        } else expression match {
          // Dependencies in untaken branches must remain inside their conditional evaluation.
          case conditional: ConditionalExpression =>
            conditional.withNewAlwaysEvaluatedInputs(conditional.alwaysEvaluatedInputs.map(replace))
          case other => other.mapChildren(replace)
        }
      }
      // Replacing only the children of the bound root keeps its own evaluation. Its
      // dependencies must be computed on the original input rows in a lower Project.
      val replacement = binding match {
        case alias: Alias =>
          val child = alias.child match {
            // NOT IN must reach predicate-subquery rewriting as a whole so that NULLs on
            // either side retain their three-valued semantics. Only stage its operands.
            case not @ Not(in: InSubquery) => not.copy(child = in.mapChildren(replace))
            case other => other.mapChildren(replace)
          }
          alias.copy(child = child)(alias.exprId, alias.qualifier, alias.explicitMetadata,
            alias.nonInheritableMetadataKeys)
        case other => other.mapChildren(replace).asInstanceOf[NamedExpression]
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
