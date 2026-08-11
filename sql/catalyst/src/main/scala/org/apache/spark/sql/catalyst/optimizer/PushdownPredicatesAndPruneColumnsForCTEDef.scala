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

package org.apache.spark.sql.catalyst.optimizer

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeSet}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, Or, PredicateHelper, SubqueryExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.CTE
import org.apache.spark.util.collection.Utils

/**
 * Infer predicates and column pruning for [[CTERelationDef]] from its reference points, and push
 * the disjunctive predicates as well as the union of attributes down the CTE plan.
 */
object PushdownPredicatesAndPruneColumnsForCTEDef extends Rule[LogicalPlan] with PredicateHelper {

  // CTE_id - (CTE_definition, precedence, predicates_to_push_down, attributes_to_prune)
  private type CTEMap = mutable.HashMap[Long, (CTERelationDef, Int, Seq[Expression], AttributeSet)]

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!plan.isInstanceOf[Subquery] && plan.containsPattern(CTE)) {
      val cteMap = new CTEMap
      gatherPredicatesAndAttributes(plan, cteMap)
      pushdownPredicatesAndAttributes(plan, cteMap)
    } else {
      plan
    }
  }

  private def restoreCTEDefAttrs(
      input: Seq[Expression],
      mapping: Map[Attribute, Expression]): Seq[Expression] = {
    input.map(e => e.transform {
      case a: Attribute =>
        mapping.keys.find(_.semanticEquals(a)).map(mapping).getOrElse(a)
    })
  }

  /**
   * Gather all the predicates and referenced attributes on different points of CTE references
   * using pattern `ScanOperation` (which takes care of determinism) and combine those predicates
   * and attributes that belong to the same CTE definition.
   * For the same CTE definition, if any of its references does not have predicates, the combined
   * predicate will be a TRUE literal, which means there will be no predicate push-down.
   */
  private def gatherPredicatesAndAttributes(plan: LogicalPlan, cteMap: CTEMap): Unit = {
    plan match {
      case WithCTE(child, cteDefs) =>
        cteDefs.zipWithIndex.foreach { case (cteDef, precedence) =>
          gatherPredicatesAndAttributes(cteDef.child, cteMap)
          cteMap.put(cteDef.id, (cteDef, precedence, Seq.empty, AttributeSet.empty))
        }
        gatherPredicatesAndAttributes(child, cteMap)

      case PhysicalOperation(projects, predicates, ref: CTERelationRef) =>
        val (cteDef, precedence, preds, attrs) = cteMap(ref.cteId)
        val attrMapping = Utils.toMap(ref.output, cteDef.output)
        val newPredicates = if (isTruePredicate(preds)) {
          preds
        } else {
          // Make sure we only push down predicates that do not contain forward CTE references.
          val filteredPredicates = restoreCTEDefAttrs(predicates.filter(_.find {
            case s: SubqueryExpression => s.plan.find {
              case r: CTERelationRef =>
                // If the ref's ID does not exist in the map or if ref's corresponding precedence
                // is bigger than that of the current CTE we are pushing predicates for, it
                // indicates a forward reference and we should exclude this predicate.
                !cteMap.contains(r.cteId) || cteMap(r.cteId)._2 >= precedence
              case _ => false
            }.nonEmpty
            case _ => false
          }.isEmpty), attrMapping).filter(_.references.forall(cteDef.outputSet.contains))
          if (filteredPredicates.isEmpty) {
            Seq(Literal.TrueLiteral)
          } else {
            preds :+ filteredPredicates.reduce(And)
          }
        }
        val newAttributes = attrs ++
          AttributeSet(restoreCTEDefAttrs(projects.flatMap(_.references), attrMapping)) ++
          AttributeSet(restoreCTEDefAttrs(predicates.flatMap(_.references), attrMapping))

        cteMap.update(ref.cteId, (cteDef, precedence, newPredicates, newAttributes))
        plan.subqueriesAll.foreach(s => gatherPredicatesAndAttributes(s, cteMap))

      case _ =>
        plan.children.foreach(c => gatherPredicatesAndAttributes(c, cteMap))
        plan.subqueries.foreach(s => gatherPredicatesAndAttributes(s, cteMap))
    }
  }

  /**
   * Push down the combined predicate and attribute references to each CTE definition plan.
   *
   * In order to guarantee idempotency, we keep the predicates (if any) being pushed down by the
   * last iteration of this rule in a temporary field of `CTERelationDef`, so that on the current
   * iteration, we only push down predicates for a CTE def if there exists any new predicate that
   * has not been pushed before. When such a new predicate push-down happens, the CTE definition
   * is rebuilt from its CURRENT child: the push-down filter this rule placed in the previous
   * iteration is removed (wherever it sits) and the result is wrapped with the latest combined
   * predicate. This preserves any change other rules made to the CTE definition's child in
   * between (e.g. filters injected by `InferFiltersFromConstraints`, which runs in the `Once`
   * batch sandwiched between the two fixedPoint batches containing this rule). If the previous
   * push-down can no longer be found (another rule rewrote or merged it with other filters),
   * the current child is used as-is: re-pushing the combined predicate is redundant but always
   * semantics-preserving, since the disjunction of the reference predicates is valid for every
   * row of the CTE definition.
   */
  private def pushdownPredicatesAndAttributes(
      plan: LogicalPlan,
      cteMap: CTEMap): LogicalPlan = plan.transformWithSubqueries {
    case cteDef @ CTERelationDef(child, id, originalPlanWithPredicates, _, _, _) =>
      val (_, _, newPreds, newAttrSet) = cteMap(id)
      val preds = originalPlanWithPredicates.map(_._2).getOrElse(Seq.empty)
      if (!isTruePredicate(newPreds) &&
          newPreds.exists(newPred => !preds.exists(_.semanticEquals(newPred)))) {
        val basePlan = originalPlanWithPredicates match {
          case Some((_, prevPreds)) if prevPreds.nonEmpty =>
            // Remove the push-down filter this rule placed in the previous iteration. It is
            // usually the top-level node of the child, but rules sharing the fixedPoint
            // batches with this rule (e.g. `PushDownPredicates`) may have moved it deeper,
            // possibly across attribute-renaming projections - hence the comparison is done
            // on canonicalized conditions. If the previous push-down can no longer be found
            // (another rule rewrote or merged it with other filters), the current child is
            // used as-is: re-pushing the combined predicate is redundant but always
            // semantics-preserving, since the disjunction of the reference predicates is
            // valid for every row of the CTE definition.
            removePushedDownFilter(child, prevPreds.reduce(Or))
          case _ => child
        }
        val newCombinedPred = newPreds.reduce(Or)
        val newChild = if (needsPruning(basePlan, newAttrSet)) {
          Project(newAttrSet.toSeq, basePlan)
        } else {
          basePlan
        }
        cteDef.copy(child = Filter(newCombinedPred, newChild),
          // The plan component of `originalPlanWithPredicates` is recorded but never read
          // back: on the next iteration only the pushed predicates are consulted (see the
          // `basePlan` computation above).
          originalPlanWithPredicates = Some((basePlan, newPreds)))
      } else if (needsPruning(cteDef.child, newAttrSet)) {
        cteDef.copy(child = Project(newAttrSet.toSeq, cteDef.child))
      } else {
        cteDef
      }

    case cteRef @ CTERelationRef(cteId, _, output, _, _, _, _, _) =>
      val (cteDef, _, _, newAttrSet) = cteMap(cteId)
      if (needsPruning(cteDef.child, newAttrSet)) {
        val indices = newAttrSet.toSeq.map(cteDef.output.indexOf)
        val newOutput = indices.map(output)
        cteRef.copy(output = newOutput)
      } else {
        // Do not change the order of output columns if no column is pruned, in which case there
        // might be no Project and the order is important.
        cteRef
      }
  }

  /**
   * Removes the previous push-down filter (identified by its condition, `predicate`) from
   * `plan`, wherever predicate push-down rules sharing the fixedPoint batches with this rule
   * (e.g. `PushDownPredicates`) may have moved it. The descent mirrors the cases of
   * `PushPredicateThroughNonJoin` and `PushPredicateThroughJoin`, translating `predicate` back
   * the same way they translate the pushed condition (for `Project` and `Aggregate` it uses
   * the very same `AliasHelper` utilities, so the two cannot drift apart): through projection
   * and grouping-key aliases, positionally into each branch (`Union`), and unchanged
   * through operators that pass the referenced attributes verbatim (`Join`, `Window`, and
   * output-preserving unary nodes like `Filter`, `Sort`, `Repartition`). Descent stops at
   * operators that remap attributes in other ways (e.g. `Generate`, `Expand`): if the filter
   * cannot be located, the input plan is returned unchanged, and the caller re-pushes on top,
   * which is redundant but always semantics-preserving.
   *
   * Ancestors of the removed filter are rebuilt with `withNewChildren` rather than direct
   * case-class copies so that `TreeNode` tags (e.g. `Project.hiddenOutputTag`, which the
   * analyzer sets on the projection above a natural/USING join) survive the rebuild.
   */
  private def removePushedDownFilter(plan: LogicalPlan, predicate: Expression): LogicalPlan = {
    def remove(current: LogicalPlan, target: Expression): (LogicalPlan, Boolean) = current match {
      case Filter(cond, inner) if cond.canonicalized == target.canonicalized =>
        (inner, true)
      case p: Project =>
        // Mirror PushPredicateThroughNonJoin: translate the target through the projection's
        // aliases with the same helper it uses to move the filter below the projection.
        val translated = replaceAlias(target, getAliasMap(p))
        val (newChild, removed) = remove(p.child, translated)
        if (removed) (p.withNewChildren(Seq(newChild)), true) else (p, false)
      case j: Join =>
        val (newLeft, removedFromLeft) = remove(j.left, target)
        if (removedFromLeft) {
          (j.withNewChildren(Seq(newLeft, j.right)), true)
        } else {
          val (newRight, removedFromRight) = remove(j.right, target)
          if (removedFromRight) (j.withNewChildren(Seq(j.left, newRight)), true) else (j, false)
        }
      case u: Union =>
        // PushDownPredicates copies the filter into every branch, mapping the union output
        // attributes to each branch's output positionally; remove it from every branch where
        // it is found. The ExprId-to-output-index map is built once for all branches.
        val outputIndexByExprId = u.output.map(_.exprId).zipWithIndex.toMap
        var removedAny = false
        val newChildren = u.children.map { branch =>
          val branchTarget = target.transform {
            case a: Attribute if outputIndexByExprId.contains(a.exprId) =>
              branch.output(outputIndexByExprId(a.exprId))
          }
          val (newBranch, removed) = remove(branch, branchTarget)
          if (removed) {
            removedAny = true
            newBranch
          } else {
            branch
          }
        }
        if (removedAny) (u.withNewChildren(newChildren), true) else (u, false)
      case agg: Aggregate =>
        // Mirror PushPredicateThroughNonJoin: translate the target through the grouping
        // aliases with the same helper it uses to move grouping-key filters below the
        // aggregate (filters referencing aggregate expressions stay up, so they are always
        // found above this node).
        val translated = replaceAlias(target, getAliasMap(agg))
        val (newChild, removed) = remove(agg.child, translated)
        if (removed) (agg.withNewChildren(Seq(newChild)), true) else (agg, false)
      case w: Window =>
        // PushDownPredicates pushes filters referencing only partition columns below the
        // window unchanged (partition columns are input attributes, so no translation).
        val (newChild, removed) = remove(w.child, target)
        if (removed) (w.withNewChildren(Seq(newChild)), true) else (w, false)
      case other if other.children.length == 1 &&
          other.outputSet == other.children.head.outputSet =>
        val (newChild, removed) = remove(other.children.head, target)
        if (removed) (other.withNewChildren(Seq(newChild)), true) else (other, false)
      case _ => (current, false)
    }
    remove(plan, predicate)._1
  }

  private def isTruePredicate(predicates: Seq[Expression]): Boolean = {
    predicates.length == 1 && predicates.head == Literal.TrueLiteral
  }

  private def needsPruning(sourcePlan: LogicalPlan, attributeSet: AttributeSet): Boolean = {
    attributeSet.size < sourcePlan.outputSet.size && attributeSet.subsetOf(sourcePlan.outputSet)
  }
}

/**
 * Clean up temporary info from [[CTERelationDef]] nodes. This rule should be called after all
 * iterations of [[PushdownPredicatesAndPruneColumnsForCTEDef]] are done.
 */
object CleanUpTempCTEInfo extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan =
    plan.transformWithPruning(_.containsPattern(CTE)) {
      case cteDef @ CTERelationDef(_, _, Some(_), _, _, _) =>
        cteDef.copy(originalPlanWithPredicates = None)
    }
}
