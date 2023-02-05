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

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{DeserializeToObjectExec, SparkPlan}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike

/**
 * This rule is used to adjust the shuffle exchange with special SparkPlan who
 * does not allow a shuffle on top of it.
 */
object AdjustShuffleExchangePosition extends Rule[SparkPlan] {
  private def shouldAdjust(plan: SparkPlan): Boolean = plan match {
    // `DeserializeToObjectExec` is used by Spark internally e.g. `Dataset.rdd`. It produces
    // safe rows and must be root node because SQL operators only accept unsafe rows as input.
    // This conflicts with AQE framework since we may add shuffle back during re-optimize
    // to preserve the user-specified repartition, so here we adjust the position with shuffle.
    case _: DeserializeToObjectExec => true
    case _ => false
  }

  override def apply(plan: SparkPlan): SparkPlan = plan match {
    case shuffle: ShuffleExchangeLike if shouldAdjust(shuffle.child) =>
      shuffle.child.withNewChildren(shuffle.withNewChildren(shuffle.child.children) :: Nil)
    case _ => plan
  }
}
