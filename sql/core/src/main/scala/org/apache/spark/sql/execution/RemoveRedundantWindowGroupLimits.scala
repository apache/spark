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
import org.apache.spark.sql.execution.window.{Final, Partial, WindowGroupLimitExec}

/**
 * Remove redundant partial WindowGroupLimitExec node from the spark plan. A partial
 * WindowGroupLimitExec node is redundant when its child satisfies its required child distribution.
 */
object RemoveRedundantWindowGroupLimits extends Rule[SparkPlan] {

  def apply(plan: SparkPlan): SparkPlan = plan transform {
    case outer @ WindowGroupLimitExec(
    _, _, _, _, Final, WindowGroupLimitExec(_, _, _, _, Partial, child))
      if child.outputPartitioning.satisfies(outer.requiredChildDistribution.head) =>
      val newOuter = outer.withNewChildren(Seq(child))
      newOuter
  }

}
