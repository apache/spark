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

package org.apache.spark.sql.execution.streaming

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution, StatefulOpClusteredDistribution}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.STATEFUL_OPERATOR_USE_STRICT_DISTRIBUTION

/**
 * This object is to provide clustered distribution for stateful operator with ensuring backward
 * compatibility. Please read through the NOTE on the classdoc of
 * [[StatefulOpClusteredDistribution]] before making any changes. Please refer SPARK-38204
 * for details.
 *
 * Do not use methods in this object for stateful operators which already uses
 * [[StatefulOpClusteredDistribution]] as its required child distribution.
 */
object StatefulOperatorPartitioning {

  def getCompatibleDistribution(
      expressions: Seq[Expression],
      stateInfo: StatefulOperatorStateInfo,
      conf: SQLConf): Distribution = {
    getCompatibleDistribution(expressions, stateInfo.numPartitions, conf)
  }

  def getCompatibleDistribution(
      expressions: Seq[Expression],
      numPartitions: Int,
      conf: SQLConf): Distribution = {
    if (conf.getConf(STATEFUL_OPERATOR_USE_STRICT_DISTRIBUTION)) {
      StatefulOpClusteredDistribution(expressions, numPartitions)
    } else {
      ClusteredDistribution(expressions, requiredNumPartitions = Some(numPartitions))
    }
  }
}
