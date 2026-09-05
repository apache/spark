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

package org.apache.spark.scheduler

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode

/**
 * :: DeveloperApi ::
 * An immutable snapshot of the scheduling-relevant properties of a schedulable entity (a pool or
 * a task set), captured at the moment its enclosing pool is sorted.
 *
 * This is the value type handed to a custom `java.util.Comparator[SchedulableInfo]` installed via
 * `spark.scheduler.rootPool.comparator.class`. Exposing a snapshot rather than Spark's internal
 * `Schedulable` keeps the scheduler's live, mutable Pool/TaskSetManager hierarchy fully internal
 * and free to evolve.
 *
 * The values only carry their full meaning with some scheduler knowledge: `priority` is the job id
 * for a task set, `stageId` is -1 for a pool, and `minShare`/`weight` only influence the ordering
 * in FAIR mode.
 *
 * @param schedulingMode the scheduling mode (FAIR, FIFO or NONE) of the entity
 * @param weight the relative weight used when sharing resources in FAIR mode
 * @param minShare the minimum share of resources requested in FAIR mode
 * @param runningTasks the number of currently running tasks
 * @param priority the scheduling priority; the job id for a task set
 * @param stageId the stage id for a task set, or -1 for a pool
 * @param name the name of the schedulable entity
 */
@DeveloperApi
case class SchedulableInfo(
    schedulingMode: SchedulingMode,
    weight: Int,
    minShare: Int,
    runningTasks: Int,
    priority: Int,
    stageId: Int,
    name: String)

object SchedulableInfo {
  /** Creates an immutable snapshot of the given schedulable's ordering properties. */
  private[spark] def apply(schedulable: Schedulable): SchedulableInfo = {
    SchedulableInfo(
      schedulable.schedulingMode,
      schedulable.weight,
      schedulable.minShare,
      schedulable.runningTasks,
      schedulable.priority,
      schedulable.stageId,
      schedulable.name)
  }
}
