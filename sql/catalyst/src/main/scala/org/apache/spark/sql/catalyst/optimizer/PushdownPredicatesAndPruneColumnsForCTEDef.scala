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

import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeSet, Expression, Literal, Or, SubqueryExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.CTE

/**
 * Infer predicates and column pruning for [[CTERelationDef]] from its reference points, and push
 * the disjunctive predicates as well as the union of attributes down the CTE plan.
 */
object PushdownPredicatesAndPruneColumnsForCTEDef extends Rule[LogicalPlan] {

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
        val attrMapping = ref.output.zip(cteDef.output).map{ case (r, d) => r -> d }.toMap
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
   * has not been pushed before. Also, since part of a new predicate might overlap with some
   * existing predicate and it can be hard to extract only the non-overlapping part, we also keep
   * the original CTE definition plan without any predicate push-down in that temporary field so
   * that when we do a new predicate push-down, we can construct a new plan with all latest
   * predicates over the original plan without having to figure out the exact predicate difference.
   */
  private def pushdownPredicatesAndAttributes(
      plan: LogicalPlan,
      cteMap: CTEMap): LogicalPlan = plan.transformWithSubqueries {
    case cteDef @ CTERelationDef(child, id, originalPlanWithPredicates, _) =>
      val (_, _, newPreds, newAttrSet) = cteMap(id)
      val originalPlan = originalPlanWithPredicates.map(_._1).getOrElse(child)
      val preds = originalPlanWithPredicates.map(_._2).getOrElse(Seq.empty)
      if (!isTruePredicate(newPreds) &&
          newPreds.exists(newPred => !preds.exists(_.semanticEquals(newPred)))) {
        val newCombinedPred = newPreds.reduce(Or)
        val newChild = if (needsPruning(originalPlan, newAttrSet)) {
          Project(newAttrSet.toSeq, originalPlan)
        } else {
          originalPlan
        }
        CTERelationDef(Filter(newCombinedPred, newChild), id, Some((originalPlan, newPreds)))
      } else if (needsPruning(cteDef.child, newAttrSet)) {
        CTERelationDef(Project(newAttrSet.toSeq, cteDef.child), id, Some((originalPlan, preds)))
      } else {
        cteDef
      }

    case cteRef @ CTERelationRef(cteId, _, output, _) =>
      val (cteDef, _, _, newAttrSet) = cteMap(cteId)
      if (newAttrSet.size < output.size) {
        val indices = newAttrSet.toSeq.map(cteDef.output.indexOf)
        val newOutput = indices.map(output)
        cteRef.copy(output = newOutput)
      } else {
        // Do not change the order of output columns if no column is pruned, in which case there
        // might be no Project and the order is important.
        cteRef
      }
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
      case cteDef @ CTERelationDef(_, _, Some(_), _) =>
        cteDef.copy(originalPlanWithPredicates = None)
    }
}
