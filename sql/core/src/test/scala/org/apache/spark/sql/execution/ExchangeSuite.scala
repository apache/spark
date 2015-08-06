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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.execution.joins.SortMergeJoin

class ExchangeSuite extends SparkPlanTest {
  test("shuffling UnsafeRows in exchange") {
    val input = (1 to 1000).map(Tuple1.apply)
    checkAnswer(
      input.toDF(),
      plan => ConvertToSafe(Exchange(SinglePartition, ConvertToUnsafe(plan))),
      input.map(Row.fromTuple)
    )
  }

  test("EnsureRequirements shouldn't add exchange to SMJ inputs if both are SinglePartition") {
    val df = (1 to 10).map(Tuple1.apply).toDF("a").repartition(1)
    val keys = Seq(df.col("a").expr)
    val smj = SortMergeJoin(keys, keys, df.queryExecution.sparkPlan, df.queryExecution.sparkPlan)
    val afterEnsureRequirements = EnsureRequirements(df.sqlContext).apply(smj)
    if (afterEnsureRequirements.collect { case Exchange(_, _) => true }.nonEmpty) {
      fail(s"No Exchanges should have been added:\n$afterEnsureRequirements")
    }
  }
}
