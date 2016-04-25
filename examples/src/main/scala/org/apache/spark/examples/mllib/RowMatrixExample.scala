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
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
// $example off$

object RowMatrixExample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RowMatrixExample")
    val sc = new SparkContext(conf)

    // $example on$
    val v1 = Vectors.dense(1.0, 10.0, 100.0)
    val v2 = Vectors.dense(2.0, 20.0, 200.0)
    val v3 = Vectors.dense(3.0, 30.0, 300.0)

    val rows: RDD[Vector] = sc.parallelize(Seq(v1, v2, v3))  // an RDD of local vectors
    // Create a RowMatrix from an RDD[Vector].
    val mat: RowMatrix = new RowMatrix(rows)

    // Get its size.
    val m = mat.numRows()
    val n = mat.numCols()

    // QR decomposition
    val qrResult = mat.tallSkinnyQR(true)
    // $example off$

    sc.stop()
  }
}
// scalastyle:on println
