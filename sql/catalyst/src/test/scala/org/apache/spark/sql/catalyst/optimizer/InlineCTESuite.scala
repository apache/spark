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

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf

class InlineCTESuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Inline CTE", FixedPoint(100),
      InlineCTE(),
      ColumnPruning,
      RemoveNoopOperators,
      CollapseProject,
      RemoveRedundantAliases) :: Nil
  }

  private val localRelation = LocalRelation($"a".int, $"b".int, $"c".int)

  Seq(true, false).foreach { exchangeReuseEnabled =>
    Seq(1, 2, 3, 4).foreach { threshold =>
      test(s"Test CTE inline when exchangeReuse = $exchangeReuseEnabled and " +
        s"notInlineCTEThreshold = $threshold") {
        withSQLConf(
          SQLConf.EXCHANGE_REUSE_ENABLED.key -> exchangeReuseEnabled.toString,
          SQLConf.NOT_INLINE_CTE_THRESHOLD.key -> threshold.toString) {
          val originalQuery = UnresolvedWith(
            Union(Seq(
              UnresolvedRelation(Seq("results")),
              UnresolvedRelation(Seq("results")),
              UnresolvedRelation(Seq("results")))),
            Seq(("results", SubqueryAlias("results", localRelation)))
          ).analyze

          val correctAnswer = if ((threshold == 1 || threshold == 4) || !exchangeReuseEnabled) {
            Union(Seq(
              localRelation,
              localRelation,
              localRelation)).analyze
          } else {
            RemoveNoopOperators(RemoveRedundantAliases(WithCTE(
              Union(Seq(
                CTERelationRef(1, true, localRelation.output),
                CTERelationRef(1, true, localRelation.output),
                CTERelationRef(1, true, localRelation.output))),
              Seq(CTERelationDef(localRelation, 1))).analyze))
          }
          comparePlans(Optimize.execute(originalQuery), correctAnswer)
        }
      }
    }
  }
}
