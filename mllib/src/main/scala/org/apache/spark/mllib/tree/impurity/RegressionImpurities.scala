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

package org.apache.spark.mllib.tree.impurity

import org.apache.spark.annotation.Experimental

/**
 * :: Experimental ::
 * Factory class for constructing a [[org.apache.spark.mllib.tree.impurity.RegressionImpurity]]
 * type based on its name.
 */
@Experimental
private[mllib] object RegressionImpurities {

  /**
   * Mapping used for impurity names, used for parsing impurity settings.
   * If you add a new impurity class, add it here.
   */
  val nameToImpurityMap: Map[String, RegressionImpurity] = Map(
    "variance" -> Variance)

  val names: List[String] = nameToImpurityMap.keys.toList

  /**
   * Given impurity name, return type.
   */
  def impurity(name: String): RegressionImpurity = {
    if (nameToImpurityMap.contains(name)) {
      nameToImpurityMap(name)
    } else {
      throw new IllegalArgumentException(s"Bad impurity parameter for regression: $name")
    }
  }

}
