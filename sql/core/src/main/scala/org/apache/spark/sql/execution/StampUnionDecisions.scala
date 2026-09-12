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

import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Fixes each [[UnionExec]]'s partitioning decision and codegen conf snapshot at one defined point.
 *
 * `UnionExec` derives both from state that moves: its children's `outputPartitioning` sharpens as
 * AQE finalises the plans behind them, and `conf` is the live session conf. Whoever asked first
 * used to decide, which made the answer depend on when it was observed. This rule asks once,
 * right after `EnsureRequirements`, so the partitioning a parent's exchange decision was taken
 * from is the one `unionRDDs` and the codegen gate use.
 *
 * It only writes what is not there yet, so re-running it (AQE re-optimizes each round) keeps the
 * first answer, and a node rebuilt from a stamped one keeps the tags `copyTagsFrom` gave it.
 */
object StampUnionDecisions extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    plan.foreach {
      case u: UnionExec => u.stampDecisions()
      case _ =>
    }
    plan
  }
}
