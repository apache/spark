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
 * NOTE: We could add sorting of feature values in this PR; the only changed required would be to
 * sort feature values at construction-time. Sorting might improve locality during stats
 * aggregation (we'd frequently update the same O(statsSize) array for a (feature, bin),
 * instead of frequently updating for the same feature).
 *
 * @param featureArity  For categorical features, this gives the number of categories.
 *                      For continuous features, this should be set to 0.
 * @param rowIndices Optional: rowIndices(i) is the row index of the ith feature value (values(i))
 *                   If unspecified, feature values are assumed to be ordered by row (i.e. values(i)
 *                   is a feature value from the ith row).
 */
private[impl] class FeatureVector(
    val featureIndex: Int,
    val featureArity: Int,
    val values: Array[Int],
    private val rowIndices: Option[Array[Int]])
  extends Serializable {
  // Associates feature values with training point rows. indices(i) = training point index
  // (row index) of ith feature value
  val indices = rowIndices.getOrElse(values.indices.toArray)

  def isCategorical: Boolean = featureArity > 0

  /** For debugging */
  override def toString: String = {
    "  FeatureVector(" +
      s"    featureIndex: $featureIndex,\n" +
      s"    featureType: ${if (featureArity == 0) "Continuous" else "Categorical"},\n" +
      s"    featureArity: $featureArity,\n" +
      s"    values: ${values.mkString(", ")},\n" +
      s"    indices: ${indices.mkString(", ")},\n" +
      "  )"
  }

  def deepCopy(): FeatureVector =
    new FeatureVector(featureIndex, featureArity, values.clone(), Some(indices.clone()))

  override def equals(other: Any): Boolean = {
    other match {
      case o: FeatureVector =>
        featureIndex == o.featureIndex && featureArity == o.featureArity &&
          values.sameElements(o.values) && indices.sameElements(o.indices)
      case _ => false
    }
  }

  /**
   * Reorders the subset of feature values at indices [from, to) in the passed-in column
   * according to the split information encoded in instanceBitVector (feature values for rows
   * that split left appear before feature values for rows that split right).
   *
   * @param numLeftRows Number of rows on the left side of the split
   * @param tempVals Destination buffer for reordered feature values
   * @param tempIndices Destination buffer for row indices corresponding to reordered feature values
   * @param instanceBitVector instanceBitVector(i) = true if the row for the ith feature
   *                          value splits right, false otherwise
   */
  private[ml] def updateForSplit(
      from: Int,
      to: Int,
      numLeftRows: Int,
      tempVals: Array[Int],
      tempIndices: Array[Int],
      instanceBitVector: BitSet): Unit = {

    // BEGIN SORTING
    // We sort the [from, to) slice of col based on instance bit.
    // All instances going "left" in the split (which are false)
    // should be ordered before the instances going "right". The instanceBitVector
    // gives us the split bit value for each instance based on the instance's index.
    // We copy our feature values into @tempVals and @tempIndices either:
    // 1) in the [from, numLeftRows) range if the bit is false, or
    // 2) in the [numLeftRows, to) range if the bit is true.
    var (leftInstanceIdx, rightInstanceIdx) = (0, numLeftRows)
    var idx = from
    while (idx < to) {
      val indexForVal = indices(idx)
      val bit = instanceBitVector.get(idx - from)
      if (bit) {
        tempVals(rightInstanceIdx) = values(idx)
        tempIndices(rightInstanceIdx) = indexForVal
        rightInstanceIdx += 1
      } else {
        tempVals(leftInstanceIdx) = values(idx)
        tempIndices(leftInstanceIdx) = indexForVal
        leftInstanceIdx += 1
      }
      idx += 1
    }
    // END SORTING
    // update the column values and indices
    // with the corresponding indices
    System.arraycopy(tempVals, 0, values, from, to - from)
    System.arraycopy(tempIndices, 0, indices, from, to - from)
  }

  override def hashCode: Int = {
    com.google.common.base.Objects.hashCode(
      featureIndex: java.lang.Integer,
      featureArity: java.lang.Integer,
      values,
      indices)
  }
}

private[impl] object FeatureVector {
  /**
   * Store column values sorted by decision tree node (i.e. all column values for a node occur
   * in a contiguous subarray).
   */
  private[impl] def apply(
      featureIndex: Int,
      featureArity: Int,
      values: Array[Int]): FeatureVector = {
    new FeatureVector(featureIndex, featureArity, values, rowIndices = None)
  }
}
