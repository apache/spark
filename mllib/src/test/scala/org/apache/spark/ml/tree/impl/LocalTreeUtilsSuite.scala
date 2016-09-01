///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.spark.ml.tree.impl
//
///**
// * Test suite for [[YggdrasilUtil]].
// */
//class YggdrasilUtilSuite extends SparkFunSuite with MLlibTestSparkContext  {
//
//  private def checkDense(rows: Seq[Vector]): Unit = {
//    val numRowPartitions = 2
//    val rowStore = sc.parallelize(rows, numRowPartitions)
//    val colStore = rowToColumnStoreDense(rowStore)
//    val numColPartitions = colStore.partitions.length
//    val cols: Map[Int, Array[Double]] = colStore.collect().toMap
//    val numRows = rows.size
//    if (numRows == 0) {
//      assert(cols.isEmpty)
//      return
//    }
//    val numCols = rows.head.size
//    if (numCols == 0) {
//      assert(cols.isEmpty)
//      return
//    }
//    rows.zipWithIndex.foreach { case (row, i) =>
//      var j = 0
//      while (j < numCols) {
//        assert(row(j) == cols(j)(i))
//        j += 1
//      }
//    }
//    val expectedNumColPartitions = math.min(rowStore.partitions.length, numCols)
//    assert(numColPartitions === expectedNumColPartitions)
//  }
//
//  private def checkSparse(rows: Seq[Vector]): Unit = {
//    val numRowPartitions = 2
//    val overPartitionFactor = 2
//    val rowStore = sc.parallelize(rows, numRowPartitions)
//    val colStore = rowToColumnStoreSparse(rowStore, overPartitionFactor)
//    val cols: Map[Int, Vector] = colStore.collect().toMap
//    val numRows = rows.size
//    // Check cases with 0 rows or cols
//    if (numRows == 0) {
//      assert(cols.isEmpty)
//      return
//    }
//    val numCols = rows.head.size
//    if (numCols == 0) {
//      assert(cols.isEmpty)
//      return
//    }
//    // Check values (and count non-zeros too)
//    var expectedNumNonZeros = 0
//    rows.zipWithIndex.foreach { case (row, i) =>
//      var j = 0
//      while (j < numCols) {
//        assert(row(j) == cols(j)(i))
//        if (row(j) != 0) expectedNumNonZeros += 1
//        j += 1
//      }
//    }
//    // Check sparsity
//    val numNonZeros = cols.values.map {
//      case sv: SparseVector => sv.indices.length
//      case _ => throw new RuntimeException(
//        "checkSparse() found column which was not converted to SparseVector.")
//    }.sum
//    assert(numNonZeros === expectedNumNonZeros)
//    // Check partitions to make sure they each contain consecutive columns.
//    val colsByPartition: Array[(Int, Array[(Int, Vector)])] = colStore.mapPartitionsWithIndex {
//      case (partitionIndex, iterator) =>
//        val partCols = new mutable.ArrayBuffer[(Int, Vector)]
//        iterator.foreach(col => partCols += col)
//        Iterator((partitionIndex, iterator.toArray))
//    }.collect()
//    colsByPartition.foreach { case (partitionIndex, partCols) =>
//      var j = 0
//      while (j + 1 < partCols.length) {
//        val curColIndex = partCols(j)._1
//        val nextColIndex = partCols(j + 1)._1
//        assert(curColIndex + 1 == nextColIndex)
//        j += 1
//      }
//    }
//  }
//
//  test("rowToColumnStore: small dense") {
//    val rows = Seq(
//      Vectors.dense(1.0, 2.0, 3.0, 4.0),
//      Vectors.dense(1.1, 2.1, 3.1, 4.1),
//      Vectors.dense(1.2, 2.2, 3.2, 4.2)
//    )
//    checkDense(rows)
//    checkSparse(rows)
//  }
//
//  test("rowToColumnStore: small sparse") {
//    val rows = Seq(
//      Vectors.sparse(4, Array(0, 1), Array(1.0, 2.0)),
//      Vectors.sparse(4, Array(1, 2), Array(1.1, 2.1)),
//      Vectors.sparse(4, Array(2, 3), Array(1.2, 2.2))
//    )
//    checkDense(rows)
//    checkSparse(rows)
//  }
//
//  test("rowToColumnStore: large dense") {
//    // Note: All values must be non-zero since rowToColumnStoreSparse() automatically ignores
//    //       zero-valued elements.
//    val numRows = 100
//    val numCols = 90
//    val rows = Range(0, numRows).map { i =>
//      Vectors.dense(Range(0, numCols).map(_ + numCols * i + 1.0).toArray)
//    }
//    checkDense(rows)
//    checkSparse(rows)
//  }
//
//  test("rowToColumnStore: mixed dense and sparse") {
//    val rows = Seq(
//      Vectors.dense(1.0, 2.0, 3.0, 4.0),
//      Vectors.sparse(4, Array(1, 2), Array(1.1, 2.1)),
//      Vectors.dense(1.2, 2.2, 3.2, 4.2),
//      Vectors.sparse(4, Array(0, 2), Array(1.3, 2.3))
//    )
//    checkDense(rows)
//    checkSparse(rows)
//  }
//
//  test("rowToColumnStore: 0 rows") {
//    val rows = Seq.empty[Vector]
//    checkDense(rows)
//    checkSparse(rows)
//  }
//
//  test("rowToColumnStore: 0 cols") {
//    val rows = Seq(
//      Vectors.dense(Array.empty[Double]),
//      Vectors.dense(Array.empty[Double]),
//      Vectors.dense(Array.empty[Double])
//    )
//    checkDense(rows)
//    checkSparse(rows)
//  }
//}