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

package org.apache.spark

import java.util.Properties

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.metrics.MetricsSystem

/** A [[TaskContext]] with extra info and tooling for a barrier stage. */
class BarrierTaskContext(
    override val stageId: Int,
    override val stageAttemptNumber: Int,
    override val partitionId: Int,
    override val taskAttemptId: Long,
    override val attemptNumber: Int,
    override val taskMemoryManager: TaskMemoryManager,
    localProperties: Properties,
    @transient private val metricsSystem: MetricsSystem,
    // The default value is only used in tests.
    override val taskMetrics: TaskMetrics = TaskMetrics.empty)
  extends TaskContextImpl(stageId, stageAttemptNumber, partitionId, taskAttemptId, attemptNumber,
      taskMemoryManager, localProperties, metricsSystem, taskMetrics) {

  /**
   * :: Experimental ::
   * Sets a global barrier and waits until all tasks in this stage hit this barrier. Similar to
   * MPI_Barrier function in MPI, the barrier() function call blocks until all tasks in the same
   * stage have reached this routine.
   *
   * CAUTION! In a barrier stage, each task must have the same number of barrier() calls, in all
   * possible code branches. Otherwise, you may get the job hanging or a SparkException after
   * timeout. Some examples of misuses listed below:
   * 1. Only call barrier() function on a subset of all the tasks in the same barrier stage, it
   * shall lead to timeout of the function call.
   * {{{
   *   rdd.barrier().mapPartitions { (iter, context) =>
   *       if (context.partitionId() == 0) {
   *           // Do nothing.
   *       } else {
   *           context.barrier()
   *       }
   *       iter
   *   }
   * }}}
   *
   * 2. Include barrier() function in a try-catch code block, this may lead to timeout of the
   * second function call.
   * {{{
   *   rdd.barrier().mapPartitions { (iter, context) =>
   *       try {
   *           // Do something that might throw an Exception.
   *           doSomething()
   *           context.barrier()
   *       } catch {
   *           case e: Exception => logWarning("...", e)
   *       }
   *       context.barrier()
   *       iter
   *   }
   * }}}
   */
  @Experimental
  @Since("2.4.0")
  def barrier(): Unit = {
    // TODO SPARK-24817 implement global barrier.
  }

  /**
   * :: Experimental ::
   * Returns the all task infos in this barrier stage, the task infos are ordered by partitionId.
   */
  @Experimental
  @Since("2.4.0")
  def getTaskInfos(): Array[BarrierTaskInfo] = {
    val addressesStr = localProperties.getProperty("addresses", "")
    addressesStr.split(",").map(_.trim()).map(new BarrierTaskInfo(_))
  }
}
