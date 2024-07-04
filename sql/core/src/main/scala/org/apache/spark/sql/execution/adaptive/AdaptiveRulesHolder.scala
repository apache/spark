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

package org.apache.spark.sql.execution.adaptive

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

/**
 * A holder to warp the SQL extension rules of adaptive query execution.
 *
 * @param queryStagePrepRules applied before creation of query stages
 * @param runtimeOptimizerRules applied to tune logical plan based on the runtime statistics of
 *                              query stage
 * @param queryStageOptimizerRules applied to a new query stage before its execution. It makes sure
 *                                 all children query stages are materialized
 * @param queryPostPlannerStrategyRules applied between `plannerStrategy` and `queryStagePrepRules`,
 *                                      so it can get the whole plan before injecting exchanges.
 */
class AdaptiveRulesHolder(
    val queryStagePrepRules: Seq[Rule[SparkPlan]],
    val runtimeOptimizerRules: Seq[Rule[LogicalPlan]],
    val queryStageOptimizerRules: Seq[Rule[SparkPlan]],
    val queryPostPlannerStrategyRules: Seq[Rule[SparkPlan]]) {
}
