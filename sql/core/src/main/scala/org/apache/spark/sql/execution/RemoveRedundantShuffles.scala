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
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec

/**
 * Remove redundant ShuffleExchangeExec node from the spark plan. A shuffle node is redundant when
 * its child is also a shuffle.
 */
object RemoveRedundantShuffles extends Rule[SparkPlan] {
  def apply(plan: SparkPlan): SparkPlan = plan transform {
    case s1 @ ShuffleExchangeExec(_, s2: ShuffleExchangeExec, _, _) =>
      s1.copy(child = s2.child)
  }
}
