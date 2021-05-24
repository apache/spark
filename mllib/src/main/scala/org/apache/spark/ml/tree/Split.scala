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

package org.apache.spark.ml.tree

import java.util.Objects

import org.apache.spark.annotation.Since
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.tree.configuration.{FeatureType => OldFeatureType}
import org.apache.spark.mllib.tree.model.{Split => OldSplit}


/**
 * Interface for a "Split," which specifies a test made at a decision tree node
 * to choose the left or right path.
 */
sealed trait Split extends Serializable {

  /** Index of feature which this split tests */
  def featureIndex: Int

  /**
   * Return true (split to left) or false (split to right).
   * @param features  Vector of features (original values, not binned).
   */
  private[ml] def shouldGoLeft(features: Vector): Boolean

  /**
   * Return true (split to left) or false (split to right).
   * @param binnedFeature Binned feature value.
   * @param splits All splits for the given feature.
   */
  private[ml] def shouldGoLeft(binnedFeature: Int, splits: Array[Split]): Boolean

  /** Convert to old Split format */
  private[tree] def toOld: OldSplit
}

private[tree] object Split {

  def fromOld(oldSplit: OldSplit, categoricalFeatures: Map[Int, Int]): Split = {
    oldSplit.featureType match {
      case OldFeatureType.Categorical =>
        new CategoricalSplit(featureIndex = oldSplit.feature,
          _leftCategories = oldSplit.categories.toArray, categoricalFeatures(oldSplit.feature))
      case OldFeatureType.Continuous =>
        new ContinuousSplit(featureIndex = oldSplit.feature, threshold = oldSplit.threshold)
    }
  }
}

/**
 * Split which tests a categorical feature.
 * @param featureIndex  Index of the feature to test
 * @param _leftCategories  If the feature value is in this set of categories, then the split goes
 *                         left. Otherwise, it goes right.
 * @param numCategories  Number of categories for this feature.
 */
class CategoricalSplit private[ml] (
    override val featureIndex: Int,
    _leftCategories: Array[Double],
    @Since("2.0.0") val numCategories: Int)
  extends Split {

  require(_leftCategories.forall(cat => 0 <= cat && cat < numCategories), "Invalid leftCategories" +
    s" (should be in range [0, $numCategories)): ${_leftCategories.mkString(",")}")

  /**
   * If true, then "categories" is the set of categories for splitting to the left, and vice versa.
   */
  private val isLeft: Boolean = _leftCategories.length <= numCategories / 2

  /** Set of categories determining the splitting rule, along with [[isLeft]]. */
  private val categories: Set[Double] = {
    if (isLeft) {
      _leftCategories.toSet
    } else {
      setComplement(_leftCategories.toSet)
    }
  }

  override private[ml] def shouldGoLeft(features: Vector): Boolean = {
    if (isLeft) {
      categories.contains(features(featureIndex))
    } else {
      !categories.contains(features(featureIndex))
    }
  }

  override private[ml] def shouldGoLeft(binnedFeature: Int, splits: Array[Split]): Boolean = {
    if (isLeft) {
      categories.contains(binnedFeature.toDouble)
    } else {
      !categories.contains(binnedFeature.toDouble)
    }
  }

  override def hashCode(): Int = {
    val state = Seq(featureIndex, isLeft, categories)
    state.map(Objects.hashCode).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def equals(o: Any): Boolean = o match {
    case other: CategoricalSplit => featureIndex == other.featureIndex &&
      isLeft == other.isLeft && categories == other.categories
    case _ => false
  }

  override private[tree] def toOld: OldSplit = {
    val oldCats = if (isLeft) {
      categories
    } else {
      setComplement(categories)
    }
    OldSplit(featureIndex, threshold = 0.0, OldFeatureType.Categorical, oldCats.toList)
  }

  /** Get sorted categories which split to the left */
  def leftCategories: Array[Double] = {
    val cats = if (isLeft) categories else setComplement(categories)
    cats.toArray.sorted
  }

  /** Get sorted categories which split to the right */
  def rightCategories: Array[Double] = {
    val cats = if (isLeft) setComplement(categories) else categories
    cats.toArray.sorted
  }

  /** [0, numCategories) \ cats */
  private def setComplement(cats: Set[Double]): Set[Double] = {
    Range(0, numCategories).map(_.toDouble).filter(cat => !cats.contains(cat)).toSet
  }
}

/**
 * Split which tests a continuous feature.
 * @param featureIndex  Index of the feature to test
 * @param threshold  If the feature value is less than or equal to this threshold, then the
 *                   split goes left. Otherwise, it goes right.
 */
class ContinuousSplit private[ml] (override val featureIndex: Int, val threshold: Double)
  extends Split {

  override private[ml] def shouldGoLeft(features: Vector): Boolean = {
    features(featureIndex) <= threshold
  }

  override private[ml] def shouldGoLeft(binnedFeature: Int, splits: Array[Split]): Boolean = {
    if (binnedFeature == splits.length) {
      // > last split, so split right
      false
    } else {
      val featureValueUpperBound = splits(binnedFeature).asInstanceOf[ContinuousSplit].threshold
      featureValueUpperBound <= threshold
    }
  }

  override def equals(o: Any): Boolean = {
    o match {
      case other: ContinuousSplit =>
        featureIndex == other.featureIndex && threshold == other.threshold
      case _ =>
        false
    }
  }

  override def hashCode(): Int = {
    val state = Seq(featureIndex, threshold)
    state.map(Objects.hashCode).foldLeft(0)((a, b) => 31 * a + b)
  }

  override private[tree] def toOld: OldSplit = {
    OldSplit(featureIndex, threshold, OldFeatureType.Continuous, List.empty[Double])
  }
}
