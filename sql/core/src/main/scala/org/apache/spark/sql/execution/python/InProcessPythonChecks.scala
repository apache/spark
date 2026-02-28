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

package org.apache.spark.sql.execution.python

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf

/**
 * Validates that in-process Python UDFs are only used when exactly one task can run per
 * executor, preventing GIL contention on [[InProcessPythonRuntime]]'s shared interpreter.
 *
 * The constraint: spark.executor.cores / spark.task.cpus == 1
 *
 * Typical correct configuration:
 *   spark.executor.cores=1  (one core per executor, parallelism via more executors)
 *
 * Runs after [[ExtractInProcessPythonUDFs]] in the "Extract InProcess Python UDFs" optimizer
 * batch, so it sees [[InProcessEvalPython]] nodes.
 */
object InProcessPythonChecks extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.foreach {
      case _: InProcessEvalPython => checkConcurrencyConfig()
      case _ =>
    }
    plan
  }

  private def checkConcurrencyConfig(): Unit = {
    val conf = SQLConf.get
    val executorCores =
      conf.getConfString("spark.executor.cores", "1").toInt
    val taskCpus =
      conf.getConfString("spark.task.cpus", "1").toInt
    val maxConcurrentTasks = executorCores / taskCpus

    if (maxConcurrentTasks != 1) {
      throw new IllegalArgumentException(
        s"In-process Python UDFs require exactly one concurrent task per executor to " +
        s"avoid GIL contention on the shared jep interpreter. " +
        s"Current configuration allows $maxConcurrentTasks concurrent tasks " +
        s"(spark.executor.cores=$executorCores, spark.task.cpus=$taskCpus). " +
        s"Set spark.executor.cores == spark.task.cpus (e.g. both to 1).")
    }
  }
}
