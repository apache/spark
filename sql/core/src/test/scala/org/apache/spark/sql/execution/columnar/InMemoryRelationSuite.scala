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
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.test.SharedSparkSessionBase
import org.apache.spark.storage.StorageLevel

class InMemoryRelationSuite extends SparkFunSuite
  with SharedSparkSessionBase with AdaptiveSparkPlanHelper {

  test("SPARK-46779: InMemoryRelations with the same cached plan are semantically equivalent") {
    val d = spark.range(1)
    val r1 = InMemoryRelation(StorageLevel.MEMORY_ONLY, d.queryExecution, None)
    val r2 = r1.withOutput(r1.output.map(_.newInstance()))
    assert(r1.sameResult(r2))
  }

  test("SPARK-47177: Cached SQL plan do not display final AQE plan in explain string") {
    def findIMRInnerChild(p: SparkPlan): SparkPlan = {
      val tableCache = find(p) {
        case _: InMemoryTableScanExec => true
        case _ => false
      }
      assert(tableCache.isDefined)
      tableCache.get.asInstanceOf[InMemoryTableScanExec].relation.innerChildren.head
    }

    val d1 = spark.range(1).withColumn("key", expr("id % 100"))
      .groupBy("key").agg(Map("key" -> "count"))
    val cached_d2 = d1.cache()
    val df = cached_d2.withColumn("key2", expr("key % 10"))
      .groupBy("key2").agg(Map("key2" -> "count"))

    assert(findIMRInnerChild(df.queryExecution.executedPlan).treeString
      .contains("AdaptiveSparkPlan isFinalPlan=false"))
    df.collect()
    assert(findIMRInnerChild(df.queryExecution.executedPlan).treeString
      .contains("AdaptiveSparkPlan isFinalPlan=true"))
  }
}
