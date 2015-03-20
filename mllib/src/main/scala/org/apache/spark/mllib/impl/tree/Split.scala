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

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.tree.configuration.{FeatureType => OldFeatureType}
import org.apache.spark.mllib.tree.model.{Split => OldSplit}


trait Split {

  /** Index of feature which this split tests */
  def feature: Int

  /** Return true (split to left) or false (split to right) */
  private[mllib] def goLeft(features: Vector): Boolean

}

private[mllib] object Split {

  def fromOld(oldSplit: OldSplit): Split = {
    oldSplit.featureType match {
      case OldFeatureType.Categorical =>
        new CategoricalSplit(feature = oldSplit.feature,
          categories = oldSplit.categories.map(_.toInt).toSet)
      case OldFeatureType.Continuous =>
        new ContinuousSplit(feature = oldSplit.feature, threshold = oldSplit.threshold)
    }
  }

}

class CategoricalSplit(override val feature: Int, val categories: Set[Int]) extends Split {

  override private[mllib] def goLeft(features: Vector): Boolean = {
    categories.contains(features(feature).toInt)
  }
}

class ContinuousSplit(override val feature: Int, val threshold: Double) extends Split {

  override private[mllib] def goLeft(features: Vector): Boolean = {
    features(feature) <= threshold
  }
}
