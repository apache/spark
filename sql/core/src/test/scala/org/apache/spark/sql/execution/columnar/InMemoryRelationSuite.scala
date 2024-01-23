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

package org.apache.spark.sql.execution.columnar

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSparkSessionBase
import org.apache.spark.storage.StorageLevel

class InMemoryRelationSuite extends SparkFunSuite with SharedSparkSessionBase {
  test("SPARK-43157: Clone innerChildren cached plan") {
    val d = spark.range(1)
    val relation = InMemoryRelation(StorageLevel.MEMORY_ONLY, d.queryExecution, None)
    val cloned = relation.clone().asInstanceOf[InMemoryRelation]

    val relationCachedPlan = relation.innerChildren.head
    val clonedCachedPlan = cloned.innerChildren.head

    // verify the plans are not the same object but are logically equivalent
    assert(!relationCachedPlan.eq(clonedCachedPlan))
    assert(relationCachedPlan === clonedCachedPlan)
  }

  test("SPARK-46779: InMemoryRelations with the same cached plan are semantically equivalent") {
    val d = spark.range(1)
    val r1 = InMemoryRelation(StorageLevel.MEMORY_ONLY, d.queryExecution, None)
    val r2 = r1.withOutput(r1.output.map(_.newInstance()))
    assert(r1.sameResult(r2))
  }
}
