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

import org.apache.spark.internal.Logging

/**
 * Utility methods specific to local decision tree training.
 */
private[ml] object LocalDecisionTreeUtils extends Logging {

  /**
   * Convert a dataset of binned feature values from row storage to column storage.
   * Stores data as [[org.apache.spark.ml.linalg.DenseVector]].
   *
   *
   * @param rowStore  An array of input data rows, each represented as an
   *                  int array of binned feature values
   * @return Transpose of rowStore as an array of columns consisting of binned feature values.
   *
   * TODO: Add implementation for sparse data.
   *       For sparse data, distribute more evenly based on number of non-zeros.
   *       (First collect stats to decide how to partition.)
   */
  private[impl] def rowToColumnStoreDense(rowStore: Array[Array[Int]]): Array[Array[Int]] = {
    // Compute the number of rows in the data
    val numRows = {
      val longNumRows: Long = rowStore.length
      require(longNumRows < Int.MaxValue, s"rowToColumnStore given RDD with $longNumRows rows," +
        s" but can handle at most ${Int.MaxValue} rows")
      longNumRows.toInt
    }

    // Check that the input dataset isn't empty (0 rows) or featureless (rows with 0 features)
    require(numRows > 0, "Local decision tree training requires numRows > 0.")
    val numFeatures = rowStore(0).length
    require(numFeatures > 0, "Local decision tree training requires numFeatures > 0.")
    // Return the transpose of the rowStore matrix
    rowStore.transpose
  }

}
