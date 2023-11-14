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

package org.apache.spark.sql.execution

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.storage.StorageLevel

class SubqueryBroadcastExecSuite extends SparkPlanTest with SharedSparkSession {

  test("SPARK-45925: Test equivalence with SubqueryAdaptiveBroadcastExec") {
    val d = spark.range(10).select(Column($"id".as("b1")), Column((- $"id").as("b2")))
    val relation = InMemoryRelation(StorageLevel.MEMORY_ONLY, d.queryExecution, None)
    val cloned = relation.clone().asInstanceOf[InMemoryRelation]

    val df1 = relation.select(Column($"b1".as("b111")))
    val lp1 = df1.queryExecution.optimizedPlan
    val sp1 = df1.queryExecution.sparkPlan

    val df2 = cloned.select(Column($"b1".as("b11")))
    val lp2 = df2.queryExecution.optimizedPlan
    val sp2 = df2.queryExecution.sparkPlan
    val sbe1 = SubqueryBroadcastExec("one", 1, lp1.output, sp1)
    val sabe1 = SubqueryAdaptiveBroadcastExec("one", 1, true, lp1, lp1.output, sp1)
    assert(sbe1 == sabe1)

    val sabe2 = SubqueryAdaptiveBroadcastExec("one", 1, true, lp2, lp2.output, sp2)

    // check canonicalized equivalence too
    assert( sbe1.canonicalized== sabe2.canonicalized)
  }
}
