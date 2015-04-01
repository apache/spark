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


/**
 * Interface for a "Split," which specifies a test made at a decision tree node
 * to choose the left or right path.
 */
trait Split extends Serializable {

  /** Index of feature which this split tests */
  def feature: Int

  /** Return true (split to left) or false (split to right) */
  private[mllib] def goLeft(features: Vector): Boolean

  /** Convert to old Split format */
  private[tree] def toOld: OldSplit
}

private[mllib] object Split {

  def fromOld(oldSplit: OldSplit): Split = {
    oldSplit.featureType match {
      case OldFeatureType.Categorical =>
        new CategoricalSplit(feature = oldSplit.feature,
          categories = oldSplit.categories.toSet)
      case OldFeatureType.Continuous =>
        new ContinuousSplit(feature = oldSplit.feature, threshold = oldSplit.threshold)
    }
  }

}

/**
 * Split which tests a categorical feature.
 * @param feature  Index of the feature to test
 * @param categories  If the feature value is in this set of categories, then the split goes left.
 *                    Otherwise, it goes right.
 */
class CategoricalSplit(override val feature: Int, val categories: Set[Double]) extends Split {

  override private[mllib] def goLeft(features: Vector): Boolean = {
    categories.contains(features(feature))
  }

  override def equals(o: Any): Boolean = {
    o match {
      case other: CategoricalSplit =>
        feature == other.feature && categories == other.categories
      case _ =>
        false
    }
  }

  override private[tree] def toOld: OldSplit = {
    OldSplit(feature, threshold = 0.0, OldFeatureType.Categorical, categories.toList)
  }
}

/**
 * Split which tests a continuous feature.
 * @param feature  Index of the feature to test
 * @param threshold  If the feature value is <= this threshold, then the split goes left.
 *                    Otherwise, it goes right.
 */
class ContinuousSplit(override val feature: Int, val threshold: Double) extends Split {

  override private[mllib] def goLeft(features: Vector): Boolean = {
    features(feature) <= threshold
  }

  override def equals(o: Any): Boolean = {
    o match {
      case other: ContinuousSplit =>
        feature == other.feature && threshold == other.threshold
      case _ =>
        false
    }
  }

  override private[tree] def toOld: OldSplit = {
    OldSplit(feature, threshold, OldFeatureType.Continuous, List.empty[Double])
  }
}
