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
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{CTERelationDef, CTERelationRef, LocalRelation, Union, WithCTE}
import org.apache.spark.sql.types.IntegerType

class NormalizeCTEIdsSuite extends SparkFunSuite {
  test("SPARK-56739: orphan CTERelationRef outside WithCTE should be normalized") {
    val attr = AttributeReference("id", IntegerType)()
    val relation = LocalRelation(attr)

    // CTE def with a high ID (simulates real usage where IDs are large)
    val cteDef = CTERelationDef(relation, id = 100L)

    // WithCTE with a ref inside
    val innerRef = CTERelationRef(100L, _resolved = true, output = Seq(attr), isStreaming = false)
    val withCTE = WithCTE(innerRef, Seq(cteDef))

    // Orphan ref OUTSIDE the WithCTE (simulates post-InlineCTE/MergeSubplans state)
    val orphanRef = CTERelationRef(100L, _resolved = true, output = Seq(attr), isStreaming = false)

    // Plan: Union of WithCTE and orphan ref
    val plan = Union(Seq(withCTE, orphanRef))

    val normalized = NormalizeCTEIds(plan)

    // Collect all CTERelationRef IDs in the normalized plan
    val refIds = normalized.collect {
      case ref: CTERelationRef => ref.cteId
    }

    // The ref inside WithCTE gets normalized to 0. The orphan should also be 0.
    assert(refIds.nonEmpty, "Should have CTERelationRefs in the plan")
    assert(refIds.forall(_ == 0L),
      s"All CTERelationRef IDs should be normalized to 0 but got: $refIds")
  }
}
