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

import org.apache.spark.sql.catalyst.expressions.{Alias, Literal, ScalarSubquery, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{CTERelationDef, CTERelationRef, LogicalPlan, OneRowRelation, Project, WithCTE}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.RELATION_TIME_TRAVEL
import org.apache.spark.sql.classic.SparkSession

/**
 * Evaluates the scalar subqueries in the timestamp of a time travel, so that the relation can be
 * resolved as of that timestamp.
 *
 * A subquery is evaluated as a query of its own. The CTEs that it reads from the query around
 * it, and the CTEs that those read in turn, are defined in the evaluated subquery, as if its
 * WITH clause defined them. The analyzer resolves the plan of a subquery expression on its own,
 * without the CTE definitions of the query around it. A time travel in such a plan that reads
 * one of these CTEs is therefore left for the analysis of the query around it, and this rule also
 * visits the plans of subquery expressions.
 */
class EvalSubqueriesForTimeTravel extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    // CTE substitution puts a definition after the definitions of its WITH clause that it reads,
    // and a subquery plan is collected after the plan around it, so a definition comes after
    // the definitions that it reads.
    lazy val cteDefs =
      plan.collectWithSubqueries { case cteDef: CTERelationDef => cteDef }.distinctBy(_.id)
    plan.resolveOperatorsUpWithSubqueriesAndPruning(_.containsPattern(RELATION_TIME_TRAVEL)) {
      case r @ RelationTimeTravel(_, Some(ts), _)
          if ts.resolved && SubqueryExpression.hasSubquery(ts) =>
        val readCTEDefs = ts.collect {
          case s: ScalarSubquery => s.exprId -> cteDefsReadBy(s.plan, cteDefs)
        }.toMap
        val hasReadCTEDefs = readCTEDefs.values.forall { case (_, complete) => complete }
        if (!hasReadCTEDefs && AnalysisContext.get.isSubqueryPlan) {
          // This is the plan of a subquery expression, and the query around it defines a CTE that
          // the timestamp reads. Leave the time travel for the analysis of that query.
          r
        } else {
          val subqueryEvaluated = ts.transform {
            case s: ScalarSubquery =>
              // `RelationTimeTravel` is a leaf node. Subqueries in it cannot resolve any
              // outer references and should not be correlated.
              assert(!s.isCorrelated, "Correlated subquery should not appear in " +
                classOf[EvalSubqueriesForTimeTravel].getSimpleName)
              val (defs, _) = readCTEDefs(s.exprId)
              val subquery = if (defs.isEmpty) s else s.withNewPlan(WithCTE(s.plan, defs))
              // Wrap the scalar subquery in a Project over OneRowRelation to execute it
              // through the normal query execution path. This properly handles table
              // references in the subquery (e.g., V2 tables).
              val wrappedPlan = Project(Seq(Alias(subquery, "result")()), OneRowRelation())
              val spark = SparkSession.active
              val qe = spark.sessionState.executePlan(wrappedPlan)
              val result = qe.executedPlan.executeCollect().head.get(0, s.dataType)
              Literal(result, s.dataType)
          }
          r.copy(timestamp = Some(subqueryEvaluated))
        }
    }
  }

  /**
   * Returns the definitions of the CTEs that `plan` reads but does not define, and of the CTEs
   * that those read in turn, in the order of `cteDefs`, and whether `cteDefs` has all of them.
   * The ones that it does not have are left out, so executing the plan fails on them as before.
   */
  private def cteDefsReadBy(
      plan: LogicalPlan,
      cteDefs: => Seq[CTERelationDef]): (Seq[CTERelationDef], Boolean) = {
    val pending = mutable.Queue.from(outerCTEIds(plan))
    val readIds = mutable.HashSet.empty[Long]
    lazy val cteDefsById = cteDefs.map(cteDef => cteDef.id -> cteDef).toMap
    var complete = true
    while (pending.nonEmpty) {
      val id = pending.dequeue()
      if (readIds.add(id)) {
        cteDefsById.get(id) match {
          case Some(cteDef) => pending ++= outerCTEIds(cteDef.child)
          case None => complete = false
        }
      }
    }
    if (readIds.isEmpty) (Nil, true) else (cteDefs.filter(d => readIds.contains(d.id)), complete)
  }

  /** Returns the ids of the CTEs that `plan` reads but does not define. */
  private def outerCTEIds(plan: LogicalPlan): Seq[Long] = {
    val definedIds = plan.collectWithSubqueries { case cteDef: CTERelationDef => cteDef.id }.toSet
    plan.collectWithSubqueries {
      case ref: CTERelationRef if !definedIds.contains(ref.cteId) => ref.cteId
    }.distinct
  }
}
