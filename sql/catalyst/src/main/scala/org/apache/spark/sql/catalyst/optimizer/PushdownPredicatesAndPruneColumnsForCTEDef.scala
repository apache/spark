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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeMap, AttributeSet}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, Or, PredicateHelper, SubqueryExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{CTE, PLAN_EXPRESSION}

/**
 * Infer predicates and column pruning for [[CTERelationDef]] from its reference points, and push
 * the disjunctive predicates as well as the union of attributes down the CTE plan.
 */
object PushdownPredicatesAndPruneColumnsForCTEDef extends Rule[LogicalPlan] with PredicateHelper {

  // A definition occurrence is identified by its path from the root, not by its CTE ID: inlining
  // can leave distinct occurrences with the same ID. Child and subquery steps use different tags.
  private type PlanPath = List[(Boolean, Int)]
  private type CTEIdScope = List[(Long, CTEState)]
  private type CTEMap = mutable.HashMap[PlanPath, CTEState]

  private case class CTEState(
      cteDef: CTERelationDef,
      definitionSiteScope: CTEIdScope,
      var predicates: Seq[Expression] = Seq.empty,
      var attributes: AttributeSet = AttributeSet.empty)

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!plan.isInstanceOf[Subquery] && plan.containsPattern(CTE)) {
      val cteMap = new CTEMap
      gatherPredicatesAndAttributes(plan, cteMap, Nil, Nil)
      pushdownPredicatesAndAttributes(plan, cteMap)
    } else {
      plan
    }
  }

  private def childPath(path: PlanPath, index: Int): PlanPath = (false, index) :: path

  private def subqueryPath(path: PlanPath, index: Int): PlanPath = (true, index) :: path

  // A CTERelationDef is unary, so its definition query is always child 0.
  private def cteDefQueryPath(cteDefPath: PlanPath): PlanPath =
    childPath(cteDefPath, 0)

  private def resolve(scope: CTEIdScope, cteId: Long): Option[CTEState] =
    scope.collectFirst { case (id, state) if id == cteId => state }

  private def resolveState(scope: CTEIdScope, ref: CTERelationRef): CTEState = {
    resolve(scope, ref.cteId).getOrElse {
      val visibleIds = scope.map(_._1).mkString("[", ", ", "]")
      throw SparkException.internalError(
        s"CTE reference ${ref.cteId} is not visible in lexical scope $visibleIds.")
    }
  }

  private def restoreCTEDefAttrs(
      input: Seq[Expression],
      mapping: Map[Attribute, Expression]): Seq[Expression] = {
    input.map(e => e.transform {
      case a: Attribute => mapping.getOrElse(a, a)
    })
  }

  /**
   * Gather predicates and referenced attributes into the state of each lexical definition
   * occurrence. `PhysicalOperation` still returns a single filter even when it is
   * non-deterministic, so such predicates are excluded below. For one occurrence, if any of its
   * references has no pushable predicate, TRUE disables predicate pushdown for that occurrence.
   */
  private def gatherPredicatesAndAttributes(
      plan: LogicalPlan, cteMap: CTEMap, scope: CTEIdScope, path: PlanPath): Unit = {
    plan match {
      case WithCTE(child, cteDefs) =>
        var scopeSoFar = scope
        cteDefs.zipWithIndex.foreach { case (cteDef, index) =>
          val cteDefPath = childPath(path, index)
          val state = cteMap.getOrElseUpdate(cteDefPath, CTEState(cteDef, scopeSoFar))
          gatherPredicatesAndAttributes(
            cteDef.child, cteMap, scopeSoFar, cteDefQueryPath(cteDefPath))
          scopeSoFar = (cteDef.id, state) :: scopeSoFar
        }
        gatherPredicatesAndAttributes(
          child, cteMap, scopeSoFar, childPath(path, cteDefs.size))

      case PhysicalOperation(projects, predicates, ref: CTERelationRef) =>
        val state = resolveState(scope, ref)
        val cteDef = state.cteDef
        val attrMapping = AttributeMap(ref.output.zip(cteDef.output))
        val preds = state.predicates
        state.predicates = if (isTruePredicate(preds)) {
          preds
        } else {
          // A reference keeps its own predicates, so a non-deterministic one pushed into the
          // definition as well would be evaluated a second time.
          val deterministicPredicates = predicates.filter(_.deterministic)
          val filteredPredicates = restoreCTEDefAttrs(deterministicPredicates.filter { predicate =>
            predicate.find {
              case s: SubqueryExpression => !canMoveSubquery(s.plan, scope, state)
              case _ => false
            }.isEmpty
          }, attrMapping).filter(_.references.forall(cteDef.outputSet.contains))
          if (filteredPredicates.isEmpty) {
            Seq(Literal.TrueLiteral)
          } else {
            val newPred = filteredPredicates.reduce(And)
            if (preds.exists(_.semanticEquals(newPred))) preds else preds :+ newPred
          }
        }
        state.attributes ++=
          AttributeSet(restoreCTEDefAttrs(projects.flatMap(_.references), attrMapping)) ++
          AttributeSet(restoreCTEDefAttrs(predicates.flatMap(_.references), attrMapping))

        gatherFromOperationChainSubqueries(plan, cteMap, scope, path)

      case _ =>
        plan.children.zipWithIndex.foreach { case (child, index) =>
          gatherPredicatesAndAttributes(child, cteMap, scope, childPath(path, index))
        }
        plan.subqueries.zipWithIndex.foreach { case (subquery, index) =>
          gatherPredicatesAndAttributes(subquery, cteMap, scope, subqueryPath(path, index))
        }
    }
  }

  /**
   * Walks an already-processed PhysicalOperation chain and delegates each immediate subquery to
   * gatherPredicatesAndAttributes. The operation's terminal reference is not processed again.
   */
  private def gatherFromOperationChainSubqueries(
      plan: LogicalPlan, cteMap: CTEMap, scope: CTEIdScope, path: PlanPath): Unit = {
    // Fully gather each immediate subquery, including its nested subqueries.
    plan.subqueries.zipWithIndex.foreach { case (subquery, index) =>
      gatherPredicatesAndAttributes(subquery, cteMap, scope, subqueryPath(path, index))
    }

    // Walk the rest of the collapsed chain without reprocessing its terminal CTE reference.
    plan.children.zipWithIndex.foreach { case (child, index) =>
      gatherFromOperationChainSubqueries(child, cteMap, scope, childPath(path, index))
    }
  }

  /**
   * Checks that every reference in a moved subquery remains valid at the target definition site.
   * Definitions brought along by the subquery are local and need no outer visibility. Any other
   * reference must resolve to the same definition occurrence in both the reference and target
   * scopes, even when distinct occurrences share a CTE ID.
   */
  private def canMoveSubquery(
      subquery: LogicalPlan, referenceScope: CTEIdScope, target: CTEState): Boolean = {
    def check(current: LogicalPlan, scope: CTEIdScope, localStates: List[CTEState]): Boolean = {
      current match {
        case w: WithCTE =>
          var scopeSoFar = scope
          var locals = localStates
          val definitionsValid = w.cteDefs.forall { cteDef =>
            val valid = check(cteDef.child, scopeSoFar, locals)
            val localState = CTEState(cteDef, scopeSoFar)
            locals = localState :: locals
            scopeSoFar = (cteDef.id, localState) :: scopeSoFar
            valid
          }
          definitionsValid && check(w.plan, scopeSoFar, locals)

        case ref: CTERelationRef =>
          resolve(scope, ref.cteId) match {
            case Some(state) if localStates.exists(_ eq state) => true
            case Some(state) => resolve(target.definitionSiteScope, ref.cteId).exists(_ eq state)
            case None => false
          }

        case _ =>
          current.children.forall(check(_, scope, localStates)) &&
            current.subqueries.forall(check(_, scope, localStates))
      }
    }

    check(subquery, referenceScope, Nil)
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
      plan: LogicalPlan, cteMap: CTEMap): LogicalPlan = {
    def rewrite(current: LogicalPlan, scope: CTEIdScope, path: PlanPath): LogicalPlan = {
      current match {
        case w: WithCTE =>
          var scopeSoFar = scope
          val newDefs = w.cteDefs.zipWithIndex.map { case (cteDef, index) =>
            val cteDefPath = childPath(path, index)
            val state = cteMap(cteDefPath)
            val newChild = rewrite(cteDef.child, scopeSoFar, cteDefQueryPath(cteDefPath))
            scopeSoFar = (cteDef.id, state) :: scopeSoFar
            pushdownDefinition(cteDef, newChild, state)
          }
          w.copy(
            plan = rewrite(w.plan, scopeSoFar, childPath(path, newDefs.size)), cteDefs = newDefs)

        case ref: CTERelationRef =>
          val state = resolveState(scope, ref)
          val cteDef = state.cteDef
          if (needsPruning(cteDef.child, state.attributes)) {
            val indices = state.attributes.toSeq.map(cteDef.output.indexOf)
            ref.copy(output = indices.map(ref.output))
          } else {
            // Do not change output order when no column is pruned; there might be no Project.
            ref
          }

        case _ =>
          var childIndex = 0
          val withNewChildren = current.mapChildren { child =>
            val newChild = rewrite(child, scope, childPath(path, childIndex))
            childIndex += 1
            newChild
          }
          var subqueryIndex = 0
          withNewChildren.transformExpressionsWithPruning(_.containsPattern(PLAN_EXPRESSION)) {
            case s: SubqueryExpression =>
              val newPlan = rewrite(
                s.plan, scope, subqueryPath(path, subqueryIndex))
              subqueryIndex += 1
              if (newPlan == s.plan) s else s.withNewPlan(newPlan)
          }
      }
    }

    rewrite(plan, Nil, Nil)
  }

  private def pushdownDefinition(
      cteDef: CTERelationDef, child: LogicalPlan, state: CTEState): CTERelationDef = {
    val newPreds = state.predicates
    val newAttrSet = state.attributes
    val originalPlanWithPredicates = cteDef.originalPlanWithPredicates
    val preds = originalPlanWithPredicates.map(_._2).getOrElse(Seq.empty)
    if (!isTruePredicate(newPreds) &&
        newPreds.exists(newPred => !preds.exists(_.semanticEquals(newPred)))) {
      val basePlan = originalPlanWithPredicates match {
        case Some((_, prevPreds)) if prevPreds.nonEmpty =>
          // Remove the previous push-down wherever shared predicate-pushdown rules moved it. If
          // it cannot be found, re-pushing the disjunction is redundant but semantics-preserving.
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
        // Only the predicates are read back on the next iteration, not the recorded plan.
        originalPlanWithPredicates = Some((basePlan, newPreds)))
    } else if (needsPruning(child, newAttrSet)) {
      cteDef.copy(child = Project(newAttrSet.toSeq, child))
    } else if (child != cteDef.child) {
      cteDef.copy(child = child)
    } else {
      cteDef
    }
  }

  /**
   * Removes the previous push-down filter (identified by its condition, `predicate`) from
   * `plan`, wherever predicate push-down rules sharing the fixedPoint batches with this rule
   * (e.g. `PushDownPredicates`) may have moved it. The descent mirrors the cases of
   * `PushPredicateThroughNonJoin` and `PushPredicateThroughJoin`, translating `predicate` back
   * the same way they translate the pushed condition (for `Project` it uses the very same
   * `AliasHelper` utilities, and for `Aggregate` it calls
   * `PushPredicateThroughNonJoin.pushBelowAggregate`, so the two cannot drift apart): through
   * projection aliases, through grouping-key aliases with float/double grouping keys normalized
   * as `pushBelowAggregate` does, positionally into each branch (`Union`), and unchanged
   * through operators that pass the referenced attributes verbatim (`Join`, `Window`, and
   * output-preserving unary nodes like `Filter`, `Sort`, `Repartition`). The descent is recursive
   * because the filter can end up several operators down -- in particular between the projections
   * `PushPredicateThroughNonJoin` splits a `Project` into for expensive projected expressions.
   * Descent stops at operators that remap attributes in other ways (e.g. `Generate`, `Expand`):
   * if the filter cannot be located, the input plan is returned unchanged, and the caller
   * re-pushes on top, which is redundant but always semantics-preserving.
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
        // Mirror PushPredicateThroughNonJoin: split and translate the target with the same helper
        // it uses to move filters below the aggregate. A filter it keeps entirely above this node
        // has been found above already, and one it splits is not entirely below it.
        val (pushDown, stayUp) =
          PushPredicateThroughNonJoin.pushBelowAggregate(splitConjunctivePredicates(target), agg)
        if (pushDown.isEmpty || stayUp.nonEmpty) {
          (agg, false)
        } else {
          val (newChild, removed) = remove(agg.child, pushDown.reduce(And))
          if (removed) (agg.withNewChildren(Seq(newChild)), true) else (agg, false)
        }
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
      case cteDef @ CTERelationDef(_, _, Some(_), _, _, _, _) =>
        cteDef.copy(originalPlanWithPredicates = None)
    }
}
