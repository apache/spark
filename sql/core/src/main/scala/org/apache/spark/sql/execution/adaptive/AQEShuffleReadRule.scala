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
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.{ShuffleExchangeLike, ShuffleOrigin}

/**
 * A rule that may create [[AQEShuffleReadExec]] on top of [[ShuffleQueryStageExec]] and change the
 * plan output partitioning. The AQE framework will skip the rule if it leads to extra shuffles.
 */
trait AQEShuffleReadRule extends Rule[SparkPlan] {
  /**
   * Returns the list of [[ShuffleOrigin]]s supported by this rule.
   */
  protected def supportedShuffleOrigins: Seq[ShuffleOrigin]

  protected def isSupported(shuffle: ShuffleExchangeLike): Boolean = {
    supportedShuffleOrigins.contains(shuffle.shuffleOrigin)
  }
}
