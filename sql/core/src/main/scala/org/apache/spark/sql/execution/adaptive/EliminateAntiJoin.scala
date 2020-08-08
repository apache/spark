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

import org.apache.spark.sql.catalyst.plans.LeftAnti
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf

/**
 * This optimization rule detects and eliminate a LeftAntiJoin when buildSide is empty.
 */
case class EliminateAntiJoin(conf: SQLConf) extends Rule[LogicalPlan] {

  private def shouldEliminate(plan: LogicalPlan): Boolean = plan match {
    case LogicalQueryStage(_, stage: QueryStageExec) if stage.resultOption.get().isDefined
      && stage.getRuntimeStatistics.rowCount.isDefined
      && stage.getRuntimeStatistics.rowCount.get == 0L => true
    case _ => false
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformDown {
    // If the right side is empty, LeftAntiJoin simply returns the left side.
    // Eliminate Join with left LogicalPlan instead.
    case Join(left, right, LeftAnti, _, _)
        if shouldEliminate(right) =>
      left
  }
}
