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

import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer.ResolveSubqueryColumnAliases
import org.apache.spark.sql.catalyst.expressions.{Alias, Literal}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor

class ResolveRecursiveCTESuite extends AnalysisTest {
  // Motivated by:
  // WITH RECURSIVE t AS (SELECT 1 UNION ALL SELECT * FROM t) SELECT * FROM t;
  test("ResolveWithCTE rule on recursive CTE without UnresolvedSubqueryColumnAliases") {
    // The analyzer will repeat ResolveWithCTE rule twice.
    val rules = Seq(ResolveWithCTE, ResolveWithCTE)
    val analyzer = new RuleExecutor[LogicalPlan] {
      override val batches = Seq(Batch("Resolution", Once, rules: _*))
    }
    // Since cteDef IDs need to be the same, cteDef for each case will be created by copying
    // this one with its child replaced.
    val cteDef = CTERelationDef(OneRowRelation())
    val anchor = Project(Seq(Alias(Literal(1), "1")()), OneRowRelation())

    def getBeforePlan(cteDef: CTERelationDef): LogicalPlan = {
      val recursionPart = SubqueryAlias("t",
          CTERelationRef(cteDef.id, false, Seq(), false, recursive = true))

      val cteDefFinal = cteDef.copy(child =
        SubqueryAlias("t", Union(Seq(anchor, recursionPart))))

      WithCTE(
        SubqueryAlias("t", CTERelationRef(cteDefFinal.id, false, Seq(), false, recursive = false)),
        Seq(cteDefFinal))
    }

    def getAfterPlan(cteDef: CTERelationDef): LogicalPlan = {
      val saRecursion = SubqueryAlias("t",
        UnionLoopRef(cteDef.id, anchor.output, false))

      val cteDefFinal = cteDef.copy(child =
        SubqueryAlias("t", UnionLoop(cteDef.id, anchor, saRecursion)))

      val outerCteRef = CTERelationRef(cteDefFinal.id, true, cteDefFinal.output, false,
        recursive = false)

      WithCTE(SubqueryAlias("t", outerCteRef), Seq(cteDefFinal))
    }

    val beforePlan = getBeforePlan(cteDef)
    val afterPlan = getAfterPlan(cteDef)

    comparePlans(analyzer.execute(beforePlan), afterPlan)
  }

  // Motivated by:
  // WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT * FROM t) SELECT * FROM t;
  test("ResolveWithCTE rule on recursive CTE with UnresolvedSubqueryColumnAliases") {
    // The analyzer will repeat ResolveWithCTE rule twice.
    val rules = Seq(ResolveWithCTE, ResolveSubqueryColumnAliases, ResolveWithCTE)
    val analyzer = new RuleExecutor[LogicalPlan] {
      override val batches = Seq(Batch("Resolution", Once, rules: _*))
    }
    // Since cteDef IDs need to be the same, cteDef for each case will be created by copying
    // this one with its child replaced.
    val cteDef = CTERelationDef(OneRowRelation())
    val anchor = Project(Seq(Alias(Literal(1), "1")()), OneRowRelation())

    def getBeforePlan(cteDef: CTERelationDef): LogicalPlan = {
      val recursionPart = SubqueryAlias("t",
          CTERelationRef(cteDef.id, false, Seq(), false, recursive = true))

      val cteDefFinal = cteDef.copy(child =
        SubqueryAlias("t",
          UnresolvedSubqueryColumnAliases(Seq("n"),
            Union(Seq(anchor, recursionPart)))))

      WithCTE(
        SubqueryAlias("t", CTERelationRef(cteDefFinal.id, false, Seq(), false, recursive = false)),
        Seq(cteDefFinal))
    }

    def getAfterPlan(cteDef: CTERelationDef): LogicalPlan = {
      val saRecursion = SubqueryAlias("t",
        Project(Seq(Alias(anchor.output.head, "n")()),
          UnionLoopRef(cteDef.id, anchor.output, false)))

      val cteDefFinal = cteDef.copy(child =
        SubqueryAlias("t",
          Project(Seq(Alias(anchor.output.head, "n")()),
            UnionLoop(cteDef.id, anchor, saRecursion))))

      val outerCteRef = CTERelationRef(cteDefFinal.id, true, cteDefFinal.output, false,
        recursive = false)

      WithCTE(SubqueryAlias("t", outerCteRef), Seq(cteDefFinal))
    }

    val beforePlan = getBeforePlan(cteDef)
    val afterPlan = getAfterPlan(cteDef)

    comparePlans(analyzer.execute(beforePlan), afterPlan)
  }
}
