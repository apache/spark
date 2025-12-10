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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, OptimizePartitionsCommand, Repartition}
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Proactively optimizes the partition count of a Dataset based on its estimated size.
 * This rule transforms the custom OptimizePartitionsCommand into standard Spark operations.
 */
object OptimizePartitionsRule extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transform {
    case OptimizePartitionsCommand(child, targetMB, currentPartitions) =>

      val targetBytes = targetMB.toLong * 1024L * 1024L

      // Get the estimated size from Catalyst Statistics
      val sizeInBytes = child.stats.sizeInBytes

      // Calculate Optimal Partition Count (N)
      val count = math.ceil(sizeInBytes.toDouble / targetBytes).toInt
      val calculatedN: Int = if (count <= 1) 1 else count

      // Smart Switch: Coalesce vs Repartition
      if (calculatedN < currentPartitions) {
        // DOWNSCALING: Use Coalesce (shuffle = false)
        Repartition(calculatedN, shuffle = false, child)
      } else if (calculatedN > currentPartitions) {
        // UPSCALING: Use Repartition (shuffle = true)
        Repartition(calculatedN, shuffle = true, child)
      } else {
        // OPTIMAL
        child
      }
  }
}
