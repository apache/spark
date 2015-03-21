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


// TODO: Once we move the tree implementation to be in this new API,
//       create an Algo enum under the new API.
sealed abstract class FeatureSubsetStrategy {
  /** Compute the number of features to test at each node */
  private[mllib] def featuresPerNode(numFeatures: Int): Int
}

case class FunctionalSubset(f: Int => Int) extends FeatureSubsetStrategy {
  override private[mllib] def featuresPerNode(numFeatures: Int): Int = f(numFeatures)
}

case class FractionalSubset(fraction: Double) extends FeatureSubsetStrategy {

  require(fraction > 0.0 && fraction <= 1.0,
    s"FeatureSubsetStrategy.Fraction($fraction) is invalid; fraction must be in range (0,1].")

  override private[mllib] def featuresPerNode(numFeatures: Int): Int = {
    (fraction * numFeatures).ceil.toInt
  }
}

object FeatureSubsetStrategy {

  /**
   * This option sets the [[FeatureSubsetStrategy]] based on numTrees and the task.
   *  - If numTrees == 1, set to "all".
   *  - If numTrees > 1 (forest),
   *     - For Classification, set to "sqrt".
   *     - For Regression, set to "onethird".
   */
  final val Auto: FeatureSubsetStrategy = {
    case object Auto extends FeatureSubsetStrategy {
      override private[mllib] def featuresPerNode(numFeatures: Int): Int = {
        throw new RuntimeException("FeatureSubsetStrategy.Auto.featuresPerNode was called," +
          " but this should never happen since Auto should be replaced within each algorithm.")
      }
    }
    Auto
  }

  def Functional(f: Int => Int): FeatureSubsetStrategy = FunctionalSubset(f)

  def Fractional(fraction: Double): FeatureSubsetStrategy = FractionalSubset(fraction)

}

private[mllib] object FeatureSubsetStrategies {

  import FeatureSubsetStrategy._

  private val supportedValues = Array("auto", "all", "onethird", "sqrt", "log2")

  /**
   * Parse a string-valued FeatureSubsetStrategy.
   * This method is case-insensitive.
   */
  def fromString(featureSubsetStrategy: String): FeatureSubsetStrategy = {
    featureSubsetStrategy.toLowerCase match {
      case "auto" => Auto
      case "all" => Fractional(1.0)
      case "onethird" => Fractional(1.0 / 3.0)
      case "sqrt" => Functional(numFeatures => math.sqrt(numFeatures).ceil.toInt)
      case "log2" =>
        Functional(numFeatures => math.max(1, (math.log(numFeatures) / math.log(2)).ceil.toInt))
      case _ =>
        throw new IllegalArgumentException(s"Tree FeatureSubsetStrategy not recognized:" +
          s" $featureSubsetStrategy\n" +
          s"  Supported values: ${supportedValues.mkString(", ")}")
    }
  }
}
