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

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike

/**
 * A simple implementation of [[Cost]], which takes a number of [[Long]] as the cost value.
 */
case class SimpleCost(value: Long) extends Cost {

  override def compare(that: Cost): Int = that match {
    case SimpleCost(thatValue) =>
      if (value < thatValue) -1 else if (value > thatValue) 1 else 0
    case _ =>
      throw new IllegalArgumentException(s"Could not compare cost with $that")
  }
}

/**
 * A simple implementation of [[CostEvaluator]], which counts the number of
 * [[ShuffleExchangeLike]] nodes in the plan.
 */
object SimpleCostEvaluator extends CostEvaluator {

  override def evaluateCost(plan: SparkPlan): Cost = {
    val cost = plan.collect {
      case s: ShuffleExchangeLike => s
    }.size
    SimpleCost(cost)
  }
}
