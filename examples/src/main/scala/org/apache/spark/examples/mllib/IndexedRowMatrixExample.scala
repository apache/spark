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

// scalastyle:off println
package org.apache.spark.examples.mllib

import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
// $example off$

object IndexedRowMatrixExample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("IndexedRowMatrixExample")
    val sc = new SparkContext(conf)

    // $example on$
    val r0 = IndexedRow(0, Vectors.dense(1, 2, 3))
    val r1 = IndexedRow(1, Vectors.dense(4, 5, 6))
    val r2 = IndexedRow(2, Vectors.dense(7, 8, 9))
    val r3 = IndexedRow(3, Vectors.dense(10, 11, 12))

    val rows: RDD[IndexedRow] = sc.parallelize(Seq(r0, r1, r2, r3))  // an RDD of indexed rows
    // Create an IndexedRowMatrix from an RDD[IndexedRow].
    val mat: IndexedRowMatrix = new IndexedRowMatrix(rows)

    // Get its size.
    val m = mat.numRows()
    val n = mat.numCols()

    // Drop its row indices.
    val rowMat: RowMatrix = mat.toRowMatrix()
    // $example off$

    sc.stop()
  }
}
// scalastyle:on println
