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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Alias, Literal}
import org.apache.spark.sql.catalyst.plans.logical._

class ResolveRecursiveCTESuite extends AnalysisTest {
  // Motivated by:
  // WITH RECURSIVE t AS (SELECT 1 UNION ALL SELECT * FROM t) SELECT * FROM t;
  test("ResolveWithCTE rule on recursive CTE without UnresolvedSubqueryColumnAliases") {
    val cteId = 0
    val anchor = Project(Seq(Alias(Literal(1), "c")()), OneRowRelation())

    def getBeforePlan(): LogicalPlan = {
      val cteRef = CTERelationRef(
        cteId,
        _resolved = false,
        output = Seq(),
        isStreaming = false)
      val recursion = cteRef.copy(recursive = true).subquery("t")
      WithCTE(
        cteRef.copy(recursive = false),
        Seq(CTERelationDef(anchor.union(recursion).subquery("t"), cteId)))
    }

    val analyzed = getAnalyzer.execute(getBeforePlan())

    val outputExprIds = analyzed match {
      case WithCTE(_, cteDefs) =>
        cteDefs.head.child match {
          case SubqueryAlias(_, UnionLoop(_, _, _, exprIds, _, _)) =>
            exprIds
        }
    }

    def getAfterPlan(): LogicalPlan = {
      val recursion = UnionLoopRef(cteId, anchor.output.map(_.withNullability(true)),
        accumulated = false).subquery("t")
      val cteDef = CTERelationDef(UnionLoop(cteId, anchor, recursion,
        outputExprIds).subquery("t"), cteId)
      val cteRef = CTERelationRef(
        cteId,
        _resolved = true,
        output = cteDef.output,
        isStreaming = false)
      WithCTE(cteRef, Seq(cteDef))
    }

    comparePlans(analyzed, getAfterPlan())
  }

  // Motivated by:
  // WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT * FROM t) SELECT * FROM t;
  test("ResolveWithCTE rule on recursive CTE with UnresolvedSubqueryColumnAliases") {
    val cteId = 0
    val anchor = Project(Seq(Alias(Literal(1), "c")()), OneRowRelation())

    def getBeforePlan(): LogicalPlan = {
      val cteRef = CTERelationRef(
        cteId,
        _resolved = false,
        output = Seq(),
        isStreaming = false)
      val recursion = cteRef.copy(recursive = true).subquery("t")
      val cteDef = CTERelationDef(
        UnresolvedSubqueryColumnAliases(Seq("n"), anchor.union(recursion)).subquery("t"),
        cteId)
      WithCTE(cteRef.copy(recursive = false), Seq(cteDef))
    }

    val analyzed = getAnalyzer.execute(getBeforePlan())

    val outputExprIds = analyzed match {
      case WithCTE(_, cteDefs) =>
        cteDefs.head.child match {
          case SubqueryAlias(_, Project(_, UnionLoop(_, _, _, exprIds, _, _))) =>
            exprIds
        }
    }

    def getAfterPlan(): LogicalPlan = {
      val col = anchor.output.head.withNullability(true)
      val recursion = UnionLoopRef(cteId, anchor.output.map(_.withNullability(true)),
        accumulated = false)
        .select(col.as("n"))
        .subquery("t")
      val cteDef = CTERelationDef(
        UnionLoop(cteId, anchor, recursion, outputExprIds)
          .select(col.as("n")).subquery("t"), cteId)
      val cteRef = CTERelationRef(
        cteId,
        _resolved = true,
        output = cteDef.output,
        isStreaming = false)
      WithCTE(cteRef, Seq(cteDef))
    }

    comparePlans(analyzed, getAfterPlan())
  }
}
