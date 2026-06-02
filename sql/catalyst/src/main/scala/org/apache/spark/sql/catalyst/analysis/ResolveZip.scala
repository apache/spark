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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, Expression, ExprId, NamedExpression, PythonUDF}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, Zip}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.ZIP

/**
 * Resolves a [[Zip]] node by rewriting it into a chain of [[Project]] nodes over the shared
 * base plan.
 *
 * The two children of `Zip` must produce the same canonicalized plan after stripping outer
 * `Project` chains, and the chains themselves must contain only scalar expressions
 * (`Project.resolved` already rejects Generator, AggregateExpression, and WindowExpression;
 * this rule additionally rejects non-scalar Python UDFs that break the 1:1 row mapping).
 *
 * The rewrite collects every alias introduced by either chain, deduplicates aliases that
 * share the same canonicalized child (a shared parent that feeds both sides is re-instanced
 * by the analyzer, so its producer surfaces twice), groups them by dependency depth (depth 1
 * = references only base attributes; depth k = references at least one depth-(k-1) alias), and
 * emits one `Project` layer per depth so each user-written alias stays in its own `Alias`.
 * `CollapseProject` runs later with its existing safety guards (`canCollapseExpressions`), so
 * nondeterministic producers (`rand()`, `uuid()`) and expensive producers referenced more than
 * once stay separate -- avoiding the double evaluation that an unguarded inline would cause.
 *
 * If the two sides cannot be merged, the `Zip` node remains unresolved and `CheckAnalysis`
 * reports a `ZIP_PLANS_NOT_MERGEABLE` error.
 */
object ResolveZip extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUpWithPruning(
    _.containsPattern(ZIP), ruleId) {
    case z: Zip if z.childrenResolved => tryMerge(z).getOrElse(z)
  }

  private def tryMerge(z: Zip): Option[LogicalPlan] = {
    val (leftAliases, leftTopList, leftBase) = analyzeChain(z.left)
    val (rightAliases, rightTopList, rightBase) = analyzeChain(z.right)

    if (!leftBase.sameResult(rightBase)) return None
    if (!allScalar(leftAliases ++ rightAliases)) return None

    // Right base's attributes may have different exprIds than the left base's even when the
    // two bases are `sameResult`. Map positionally so right-side references resolve against
    // the left base in the merged plan.
    val attrMapping: AttributeMap[Attribute] =
      AttributeMap(rightBase.output.zip(leftBase.output))
    val remappedRightAliases = rightAliases.map(remapAlias(_, attrMapping))
    val remappedRightTopList = rightTopList.map(remapNamedExpr(_, attrMapping))

    // When both sides walk through a shared parent `Project`, the analyzer re-instances the
    // right side, so the shared producer surfaces as two aliases with different exprIds but the
    // same child expression (e.g. two `rand(sameSeed)`). Keeping both would evaluate the producer
    // twice per row. Deduplicate by canonicalized child: keep the first alias for each distinct
    // child, and remap references to the dropped aliases (in surviving alias bodies and in the
    // output lists) to the survivor. A freshly written producer gets a distinct seed, so its
    // canonical form differs and it is never merged with an unrelated one.
    val canonToKept = mutable.LinkedHashMap.empty[Expression, Alias]
    val droppedToSurvivor = mutable.HashMap.empty[ExprId, Attribute]
    (leftAliases ++ remappedRightAliases).foreach { a =>
      canonToKept.get(a.child.canonicalized) match {
        case Some(kept) => droppedToSurvivor(a.exprId) = kept.toAttribute
        case None => canonToKept(a.child.canonicalized) = a
      }
    }

    def remapDropped[E <: Expression](e: E): E = if (droppedToSurvivor.isEmpty) {
      e
    } else {
      e.transform { case a: Attribute => droppedToSurvivor.getOrElse(a.exprId, a) }
        .asInstanceOf[E]
    }

    // Rewrite surviving alias bodies so references to dropped aliases point at the survivors.
    val dedupedAliases = canonToKept.values.toSeq.map { a =>
      remapAliasChild(a, remapDropped(a.child))
    }
    val layered = buildLayeredChain(dedupedAliases, leftBase)
    // Build the top-level output list. For each top-level expression:
    //   - If it references a dropped alias, use the survivor attribute -- but re-alias it to the
    //     dropped column's own name, exprId, and metadata so the schema stays correct. Two sides
    //     may expose the same deterministic producer under different names (e.g.
    //     df.select($"a".as("x")).zip(df.select($"a".as("y")))); dedup keeps the survivor's
    //     identity only for internal references, not for user-visible output columns. Only the
    //     producer (value) is shared; the dropped column keeps its own name and metadata.
    //   - Otherwise pass the attribute through unchanged.
    val finalProjectList: Seq[NamedExpression] =
      (leftTopList ++ remappedRightTopList).map { ne =>
        val attr = ne.toAttribute
        droppedToSurvivor.get(attr.exprId) match {
          case Some(survivorAttr) =>
            // Force explicitMetadata so the output column keeps the dropped column's own metadata
            // even though its value now comes from the survivor attribute.
            Alias(survivorAttr, attr.name)(
              exprId = attr.exprId, explicitMetadata = Some(attr.metadata))
          case None => remapDropped(attr)
        }
      }
    Some(Project(finalProjectList, layered))
  }

  /**
   * Walks a chain of `Project` nodes and returns:
   *   - every `Alias` introduced anywhere in the chain (deepest first, then outward),
   *   - the topmost `Project`'s projection list (or the plan's output if there is no top
   *     `Project`), used to drive the final output column list, and
   *   - the chain's base plan (first non-`Project` node).
   */
  private def analyzeChain(
      plan: LogicalPlan): (Seq[Alias], Seq[NamedExpression], LogicalPlan) = plan match {
    case Project(exprs, child) =>
      val (childAliases, _, base) = analyzeChain(child)
      val newAliases = exprs.collect { case a: Alias => a }
      (childAliases ++ newAliases, exprs, base)
    case other =>
      (Seq.empty, other.output, other)
  }

  /** Rewrites a single `Alias` so its body references the left base's attributes. */
  private def remapAlias(a: Alias, attrMapping: AttributeMap[Attribute]): Alias = {
    val newChild = a.child.transform {
      case attr: Attribute => attrMapping.getOrElse(attr, attr)
    }
    remapAliasChild(a, newChild)
  }

  /** Returns a copy of `a` with `newChild` as its body, preserving name, exprId, and metadata. */
  private def remapAliasChild(a: Alias, newChild: Expression): Alias = {
    Alias(newChild, a.name)(
      exprId = a.exprId,
      qualifier = a.qualifier,
      explicitMetadata = a.explicitMetadata,
      nonInheritableMetadataKeys = a.nonInheritableMetadataKeys)
  }

  private def remapNamedExpr(
      ne: NamedExpression, attrMapping: AttributeMap[Attribute]): NamedExpression = ne match {
    case a: Alias => remapAlias(a, attrMapping)
    case attr: Attribute => attrMapping.getOrElse(attr, attr)
    case other =>
      other.transform { case attr: Attribute => attrMapping.getOrElse(attr, attr) }
        .asInstanceOf[NamedExpression]
  }

  /**
   * Builds a chain of `Project`s over `base`, with one layer per dependency depth so each
   * user-written alias stays in its own `Alias`. Each layer carries every attribute from the
   * previous layer (full passthrough) so deeper layers and the top can still reference
   * earlier columns; `ColumnPruning` removes the unused passthroughs in the optimizer.
   */
  private def buildLayeredChain(aliases: Seq[Alias], base: LogicalPlan): LogicalPlan = {
    if (aliases.isEmpty) return base

    val aliasByExprId: Map[ExprId, Alias] = aliases.map(a => a.exprId -> a).toMap
    val depthCache = mutable.Map.empty[ExprId, Int]
    def depthOf(exprId: ExprId): Int = depthCache.getOrElseUpdate(exprId, {
      val alias = aliasByExprId(exprId)
      val refDepths = alias.child.collect { case a: Attribute => a }
        .flatMap(r => aliasByExprId.get(r.exprId).map(_ => depthOf(r.exprId)))
      if (refDepths.isEmpty) 1 else refDepths.max + 1
    })
    aliases.foreach(a => depthOf(a.exprId))
    val byDepth = aliases.groupBy(a => depthCache(a.exprId)).toSeq.sortBy(_._1)

    byDepth.foldLeft[LogicalPlan](base) { case (acc, (_, depthAliases)) =>
      Project(acc.output ++ depthAliases, acc)
    }
  }

  /**
   * Returns true if no alias contains a non-scalar Python UDF. `Project.resolved` already
   * rejects Generator, AggregateExpression, and WindowExpression; this additionally rejects
   * non-scalar Python UDFs (e.g. `GROUPED_MAP`) that would break the 1:1 row mapping.
   */
  private def allScalar(aliases: Seq[Alias]): Boolean = {
    !aliases.exists(_.exists {
      case udf: PythonUDF => !PythonUDF.isScalarPythonUDF(udf)
      case _ => false
    })
  }
}
