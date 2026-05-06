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

package org.apache.spark.sql.catalyst.normalizer

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._

class NormalizeCTEIdsSuite extends SparkFunSuite with PlanTest {

  // Build a sub-tree containing an orphan CTERelationRef (no enclosing WithCTE).
  // Use a single AttributeReference shared between Filter/Project and the ref's output
  // so ExprId equality within the sub-tree doesn't pollute the canonicalized comparison.
  private def buildOrphan(cteId: Long): LogicalPlan = {
    val attr = $"x".int
    val ref = CTERelationRef(
      cteId = cteId,
      _resolved = true,
      output = Seq(attr),
      isStreaming = false)
    Project(Seq(attr), Filter(attr > Literal(0), ref))
  }

  test("SPARK-56738: normalizes CTERelationRef inside WithCTE (existing behaviour)") {
    val cteDef1 = CTERelationDef(child = LocalRelation($"x".int), id = 100L)
    val ref1 = CTERelationRef(100L, _resolved = true, Seq($"x".int), isStreaming = false)
    val plan1 = WithCTE(Project(Seq($"x".int), ref1), Seq(cteDef1))

    val cteDef2 = CTERelationDef(child = LocalRelation($"x".int), id = 999L)
    val ref2 = CTERelationRef(999L, _resolved = true, Seq($"x".int), isStreaming = false)
    val plan2 = WithCTE(Project(Seq($"x".int), ref2), Seq(cteDef2))

    val n1 = NormalizeCTEIds(plan1)
    val n2 = NormalizeCTEIds(plan2)
    assert(n1.canonicalized == n2.canonicalized,
      s"WithCTE-wrapped plans should canonicalize identically.\nn1=$n1\nn2=$n2")
  }

  test("SPARK-56738: normalizes orphan CTERelationRef (no enclosing WithCTE)") {
    val orphan1 = buildOrphan(cteId = 100L)
    val orphan2 = buildOrphan(cteId = 999L)

    val n1 = NormalizeCTEIds(orphan1)
    val n2 = NormalizeCTEIds(orphan2)

    // Two orphan sub-trees with structurally identical bodies but different
    // per-parse cteId values should canonicalize identically after NormalizeCTEIds.
    assert(n1.canonicalized == n2.canonicalized,
      "Orphan CTERelationRef cteIds should be normalized; canonical-equal expected.")

    // sameResult on the original plans does NOT run NormalizeCTEIds; it only consults
    // LogicalPlan.canonicalized. CacheManager always runs QueryExecution.normalize first,
    // so assert sameResult on the normalized plans, which mirrors the production path.
    assert(n1.sameResult(n2),
      "NormalizeCTEIds-normalized orphan CTERelationRef sub-trees with structurally " +
        "identical bodies should be considered sameResult.")
  }
}
