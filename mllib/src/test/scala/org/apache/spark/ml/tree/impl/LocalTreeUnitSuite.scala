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

import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.util.collection.BitSet

/** Tests for equivalence/performance of local tree training vs distributed training. */
class LocalTreeUnitSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  test("Transform row-major training data into a column-major representation") {
    // Transform an empty training dataset
    {
      val result = LocalDecisionTreeUtils.rowToColumnStoreDense(Array.empty)
      val expected: Array[Array[Int]] = Array.empty
      assert(result.sameElements(expected))
    }

    // Transform a training dataset consisting of a single row
    {
      val rowLength = 10
      val data = Array(0.until(rowLength).toArray)
      val transposed = LocalDecisionTreeUtils.rowToColumnStoreDense(data)
      assert(transposed.length == rowLength,
        s"Transpose of row of length ${rowLength} only contained ${transposed.length} columns")
      transposed.foreach { col =>
        assert(col.length == 1, s"Column had length ${col.length}, expected length: 1")
      }
    }

    // Transform a 2x2 dataset
    {
      val data = Array(Array(0, 1), Array(2, 3))
      val expected = Array(Array(0, 2), Array(1, 3))
      val transposed = LocalDecisionTreeUtils.rowToColumnStoreDense(data)
      transposed.zip(expected).foreach { case (resultCol, expectedCol) =>
        assert(resultCol.sameElements(expectedCol), s"Result column" +
          s"${resultCol.mkString(", ")} differed from expected col ${expectedCol.mkString(", ")}")
      }
    }
  }

  test("Methods for sorting columns work properly") {
    val numEntries = 100

    val tempVals = new Array[Int](numEntries)
    val tempIndices = new Array[Int](numEntries)

    // Create a column of numEntries values
    val values = 0.until(numEntries).toArray
    val col = new FeatureVector(-1, 0, values, values.indices.toArray)

    // TODO(smurching): Test different bounds here?
    val from = 0
    val to = numEntries

    // Pick a random subset of indices to split left
    val rng = new Random(seed = 42)
    val leftProb = 0.5
    val (leftIdxs, rightIdxs) = values.indices.partition(i => rng.nextDouble() < leftProb)

    // Determine our expected result after sorting
    val expected = leftIdxs.map(values(_)) ++ rightIdxs.map(values(_))

    // Create a bitset indicating whether each of our values splits left or right
    val instanceBitVector = new BitSet(values.length)
    rightIdxs.foreach(instanceBitVector.set(_))

    LocalDecisionTreeUtils.sortCol(col, from, to, leftIdxs.length, tempVals,
      tempIndices, instanceBitVector)

    assert(col.values.sameElements(expected))
  }

//  test("Choose splits for a dataset consisting of a single label") {
//    val constLabel = 1
//    val data = sc.parallelize(Range(0, 8).map(x => LabeledPoint(constLabel, Vectors.dense(x))))
//    val df = spark.sqlContext.createDataFrame(data)
//
//    LocalDecisionTree.chooseSplit(col, labels, from, to, fullImpurityAgg, metadata, splits)
//
//  }

}
