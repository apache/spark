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

package org.apache.spark.mllib.impl.tree

import org.apache.spark.mllib.tree.configuration.Algo.Algo

sealed trait FeatureSubsetStrategy {
//  private[mllib] def featuresPerNode(numFeatures: Int, algo: Algo): Int
}

object FeatureSubsetStrategy {

  /**
   * This option sets the [[FeatureSubsetStrategy]] based on numTrees and the task.
   *  - If numTrees == 1, set to [[FeatureSubsetStrategy.All]].
   *  - If numTrees > 1 (forest),
   *     - For Classification, set to [[FeatureSubsetStrategy.Sqrt]].
   *     - For Regression, set to [[FeatureSubsetStrategy.OneThird]].
   */
  case object Auto extends FeatureSubsetStrategy

  case object All extends FeatureSubsetStrategy

  case object Sqrt extends FeatureSubsetStrategy

  case object OneThird extends FeatureSubsetStrategy

  case object Log2 extends FeatureSubsetStrategy

  case class Fraction(fraction: Double) extends FeatureSubsetStrategy {
    require(fraction > 0.0 && fraction <= 1.0,
      s"FeatureSubsetStrategy.Fraction($fraction) is invalid; fraction must be in range (0,1].")
  }

}

object FeatureSubsetStrategies {

  final val Auto: FeatureSubsetStrategy = FeatureSubsetStrategy.Auto

  final val All: FeatureSubsetStrategy = FeatureSubsetStrategy.All

  final val Sqrt: FeatureSubsetStrategy = FeatureSubsetStrategy.Sqrt

  final val OneThird: FeatureSubsetStrategy = FeatureSubsetStrategy.OneThird

  final val Log2: FeatureSubsetStrategy = FeatureSubsetStrategy.Log2

  def Fraction(fraction: Double): FeatureSubsetStrategy = FeatureSubsetStrategy.Fraction(fraction)

}
