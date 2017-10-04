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

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.tree.ContinuousSplit
import org.apache.spark.util.collection.BitSet

/** Unit tests for helper classes/methods specific to local tree training */
class LocalTreeUtilsSuite extends SparkFunSuite {

  test("rowToColumnStoreDense: transforms row-major data into a column-major representation") {
    // Attempt to transform an empty training dataset
    intercept[IllegalArgumentException] {
      LocalDecisionTreeUtils.rowToColumnStoreDense(Array.empty)
    }

    // Transform a training dataset consisting of a single row
    {
      val rowLength = 10
      val data = Array(0.until(rowLength).toArray)
      val transposed = LocalDecisionTreeUtils.rowToColumnStoreDense(data)
      assert(transposed.length == rowLength,
        s"Column-major representation of $rowLength-element row " +
          s"contained ${transposed.length} elements")
      transposed.foreach { col =>
        assert(col.length == 1, s"Column-major representation of a single row " +
          s"contained column of length ${col.length}, expected length: 1")
      }
    }

    // Transform a dataset consisting of a single column
    {
      val colSize = 10
      val data = Array.tabulate[Array[Int]](colSize)(Array(_))
      val transposed = LocalDecisionTreeUtils.rowToColumnStoreDense(data)
      assert(transposed.length > 0, s"Column-major representation of $colSize-element column " +
        s"was empty.")
      assert(transposed.length == 1, s"Column-major representation of $colSize-element column " +
        s"should be a single array but was ${transposed.length} arrays.")
      assert(transposed(0).length == colSize,
        s"Column-major representation of $colSize-element column contained " +
          s"${transposed(0).length} elements")
    }

    // Transform a 2x3 (non-square) dataset
    {
      val data = Array(Array(0, 1, 2), Array(3, 4, 5))
      val expected = Array(Array(0, 3), Array(1, 4), Array(2, 5))
      val transposed = LocalDecisionTreeUtils.rowToColumnStoreDense(data)
      transposed.zip(expected).foreach { case (resultCol, expectedCol) =>
        assert(resultCol.sameElements(expectedCol), s"Result column" +
          s"${resultCol.mkString(", ")} differed from expected col ${expectedCol.mkString(", ")}")
      }
    }
  }
}
