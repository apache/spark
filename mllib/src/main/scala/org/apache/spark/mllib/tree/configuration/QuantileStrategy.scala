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

package org.apache.spark.mllib.tree.configuration

import org.apache.spark.annotation.Experimental

/**
 * :: Experimental ::
 * Enum for selecting the quantile calculation strategy
 */
@Experimental
private[mllib] object QuantileStrategy extends Enumeration {
  type QuantileStrategy = Value
  val Sort, MinMax, ApproxHist = Value
}

/**
 * :: Experimental ::
 * Factory for creating [[org.apache.spark.mllib.tree.configuration.QuantileStrategy]] instances.
 */
@Experimental
private[mllib] object QuantileStrategies {

  import org.apache.spark.mllib.tree.configuration.QuantileStrategy._

  /**
   * Mapping used for strategy names.
   * If you add a new strategy type, add it here.
   */
  val nameToStrategyMap: Map[String, QuantileStrategy] = Map(
    "sort" -> Sort)

  /**
   * Given a string with the name of a quantile strategy, get the QuantileStrategy type.
   */
  def strategy(name: String): QuantileStrategy = {
    if (nameToStrategyMap.contains(name)) {
      nameToStrategyMap(name)
    } else {
      throw new IllegalArgumentException(s"Bad QuantileStrategy parameter: $name")
    }
  }

}
