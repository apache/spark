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
package org.apache.spark.dynamicallocation

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.util.{Clock, SystemClock}


/**
 * Base class for dynamic allocation strategies.
 */
abstract class AllocationStrategy(val conf: SparkConf) extends EventListener {

  // Lower and upper bounds on the number of executors.
  protected val minNumExecutors = conf.getInt("spark.dynamicAllocation.minExecutors", 0)
  protected val maxNumExecutors = conf.getInt("spark.dynamicAllocation.maxExecutors",
    Integer.MAX_VALUE)
  protected val initialNumExecutors = conf.getInt("spark.dynamicAllocation.initialExecutors",
    minNumExecutors)

  // How long there must be backlogged tasks for before an addition is triggered (seconds)
  protected val schedulerBacklogTimeoutS = conf.getTimeAsSeconds(
    "spark.dynamicAllocation.schedulerBacklogTimeout", "1s")

  // Same as above, but used only after `schedulerBacklogTimeoutS` is exceeded
  protected val sustainedSchedulerBacklogTimeoutS = conf.getTimeAsSeconds(
    "spark.dynamicAllocation.sustainedSchedulerBacklogTimeout", s"${schedulerBacklogTimeoutS}s")

  // How long an executor must be idle for before it is removed (seconds)
  protected val executorIdleTimeoutS = conf.getTimeAsSeconds(
    "spark.dynamicAllocation.executorIdleTimeout", "60s")

  protected val cachedExecutorIdleTimeoutS = conf.getTimeAsSeconds(
    "spark.dynamicAllocation.cachedExecutorIdleTimeout", s"${Integer.MAX_VALUE}s")

  // During testing, the methods to actually kill and add executors are mocked out
  protected val testing = conf.getBoolean("spark.dynamicAllocation.testing", false)

  // TODO: The default value of 1 for spark.executor.cores works right now because dynamic
  // allocation is only supported for YARN and the default number of cores per executor in YARN is
  // 1, but it might need to be attained differently for different cluster managers
  protected val tasksPerExecutor =
    conf.getInt("spark.executor.cores", 1) / conf.getInt("spark.task.cpus", 1)

  validateSettings()

  protected var clock: Clock = new SystemClock()

  /**
   * Verify that the settings specified through the config are valid.
   * If not, throw an appropriate exception.
   */
  private def validateSettings(): Unit = {
    if (minNumExecutors < 0 || maxNumExecutors < 0) {
      throw new SparkException("spark.dynamicAllocation.{min/max}Executors must be positive!")
    }
    if (maxNumExecutors == 0) {
      throw new SparkException("spark.dynamicAllocation.maxExecutors cannot be 0!")
    }
    if (minNumExecutors > maxNumExecutors) {
      throw new SparkException(s"spark.dynamicAllocation.minExecutors ($minNumExecutors) must " +
        s"be less than or equal to spark.dynamicAllocation.maxExecutors ($maxNumExecutors)!")
    }
    if (schedulerBacklogTimeoutS <= 0) {
      throw new SparkException("spark.dynamicAllocation.schedulerBacklogTimeout must be > 0!")
    }
    if (sustainedSchedulerBacklogTimeoutS <= 0) {
      throw new SparkException(
        "spark.dynamicAllocation.sustainedSchedulerBacklogTimeout must be > 0!")
    }
    if (executorIdleTimeoutS <= 0) {
      throw new SparkException("spark.dynamicAllocation.executorIdleTimeout must be > 0!")
    }
    // Require external shuffle service for dynamic allocation
    // Otherwise, we may lose shuffle files when killing executors
    if (!conf.getBoolean("spark.shuffle.service.enabled", false) && !testing) {
      throw new SparkException("Dynamic allocation of executors requires the external " +
        "shuffle service. You may enable this through spark.shuffle.service.enabled.")
    }
    if (tasksPerExecutor == 0) {
      throw new SparkException("spark.executor.cores must not be less than spark.task.cpus.")
    }
  }

  /**
   * Use a different clock for this allocation heuristic. This is mainly used for testing.
   */
  def setClock(newClock: Clock): Unit = {
    clock = newClock
  }

  /**
   * The [[org.apache.spark.ExecutorAllocationManager]] will call allocate() and
   * release() periodically one after another with the same time value.
   *
   * @param now current time
   */
  def allocate(now: Long): Unit

  /**
   * @return whether an executor has been released.
   */
  def release(now: Long): Boolean

  /**
   * For resetting the internal state of the heuristic.
   */
  def reset(): Unit

  /**
   * @return the metrics to be registered to the metric subsystem.
   */
  def getMetricsToRegister(): Seq[Metric[Any]]
}
