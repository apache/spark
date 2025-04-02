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

import org.apache.spark.SparkConf
import org.apache.spark.annotation.Unstable
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/**
 * An interface to represent the cost of a plan.
 *
 * @note This class is subject to be changed and/or moved in the near future.
 */
@Unstable
trait Cost extends Ordered[Cost]

/**
 * An interface to evaluate the cost of a physical plan.
 *
 * @note This class is subject to be changed and/or moved in the near future.
 */
@Unstable
trait CostEvaluator {
  def evaluateCost(plan: SparkPlan): Cost
}

object CostEvaluator extends Logging {

  /**
   * Instantiates a [[CostEvaluator]] using the given className.
   */
  def instantiate(className: String, conf: SparkConf): CostEvaluator = {
    logDebug(s"Creating CostEvaluator $className")
    val evaluators = Utils.loadExtensions(classOf[CostEvaluator], Seq(className), conf)
    require(evaluators.nonEmpty, "A valid AQE cost evaluator must be specified by config " +
      s"${SQLConf.ADAPTIVE_CUSTOM_COST_EVALUATOR_CLASS.key}, but $className resulted in zero " +
      "valid evaluator.")
    evaluators.head
  }
}
