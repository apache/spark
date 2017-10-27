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

package org.apache.spark.ml.tree.impl

import org.apache.spark.util.collection.BitSet

/**
 * Stores values for a single training data column (a single continuous or categorical feature).
 *
 * Values are currently stored in a dense representation only.
 * TODO: Support sparse storage (to optimize deeper levels of the tree), and maybe compressed
 *       storage (to optimize upper levels of the tree).
 *
 * TODO: Sort feature values to support more complicated splitting logic (e.g. considering every
 *       possible continuous split instead of discretizing continuous features).
 *
 * TODO: Consider sorting feature values; the only changed required would be to
 * sort values at construction-time. Sorting might improve locality during stats
 * aggregation (we'd frequently update the same O(statsSize) array for a (feature, bin),
 * instead of frequently updating for the same feature).
 *
 */
private[impl] class FeatureColumn(
    val featureIndex: Int,
    val values: Array[Int])
  extends Serializable {

  /** For debugging */
  override def toString: String = {
    "  FeatureVector(" +
      s"    featureIndex: $featureIndex,\n" +
      s"    values: ${values.mkString(", ")},\n" +
      "  )"
  }

  def deepCopy(): FeatureColumn = new FeatureColumn(featureIndex, values.clone())

  override def equals(other: Any): Boolean = {
    other match {
      case o: FeatureColumn =>
        featureIndex == o.featureIndex && values.sameElements(o.values)
      case _ => false
    }
  }

  override def hashCode: Int = {
    com.google.common.base.Objects.hashCode(
      featureIndex: java.lang.Integer,
      values)
  }

  /**
   * Reorders the subset of feature values at indices [from, to) in the passed-in column
   * according to the split information encoded in instanceBitVector (feature values for rows
   * that split left appear before feature values for rows that split right).
   *
   * @param numLeftRows Number of rows on the left side of the split
   * @param tempVals Destination buffer for reordered feature values
   * @param instanceBitVector instanceBitVector(i) = true if the row for the (from + i)th feature
   *                          value splits right, false otherwise
   */
  private[ml] def updateForSplit(
      from: Int,
      to: Int,
      numLeftRows: Int,
      tempVals: Array[Int],
      instanceBitVector: BitSet): Unit = {
    LocalDecisionTreeUtils.updateArrayForSplit(values, from, to, numLeftRows, tempVals,
      instanceBitVector)
  }
}

private[impl] object FeatureColumn {
  /**
   * Store column values sorted by decision tree node (i.e. all column values for a node occur
   * in a contiguous subarray).
   */
  private[impl] def apply(featureIndex: Int, values: Array[Int]) = {
    new FeatureColumn(featureIndex, values)
  }

}
