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

package org.apache.spark.ml.feature

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLlibTestSparkContext

class MinHashSuite extends SparkFunSuite with MLlibTestSparkContext {
  test("MinHash") {
    val data = {
      for (i <- 0 to 95) yield Vectors.sparse(100, (i until i + 5).map((_, 1.0)))
    }
    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("keys")

    val mh = new MinHash()
      .setOutputDim(1)
      .setInputCol("keys")
      .setOutputCol("values")

    val (falsePositive, falseNegative) = LSHTest.calculateLSHProperty(df, mh, 0.75, 0.5)
    assert(falsePositive < 0.3)
    assert(falseNegative < 0.1)
  }

  test("approxNearestNeighbors for min hash") {
    val data = {
      for (i <- 0 to 95) yield Vectors.sparse(100, (i until i + 5).map((_, 1.0)))
    }
    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("keys")

    val mh = new MinHash()
      .setOutputDim(20)
      .setInputCol("keys")
      .setOutputCol("values")

    val key: Vector = Vectors.sparse(100,
      (0 until 100).filter(_.toString.contains("1")).map((_, 1.0)))

    val (precision, recall) = LSHTest.calculateApproxNearestNeighbors(mh, df, key, 20,
      singleProbing = true)
    assert(precision >= 0.7)
    assert(recall >= 0.7)
  }

  test("approxSimilarityJoin for minhash on different dataset") {
    val dataA = {
      for (i <- 0 until 20) yield Vectors.sparse(100, (5 * i until 5 * i + 5).map((_, 1.0)))
    }
    val dfA = spark.createDataFrame(dataA.map(Tuple1.apply)).toDF("keys")

    val dataB = {
      for (i <- 0 until 30) yield Vectors.sparse(100, (3 * i until 3 * i + 3).map((_, 1.0)))
    }
    val dfB = spark.createDataFrame(dataB.map(Tuple1.apply)).toDF("keys")

    val mh = new MinHash()
      .setOutputDim(20)
      .setInputCol("keys")
      .setOutputCol("values")

    val (precision, recall) = LSHTest.calculateApproxSimilarityJoin(mh, dfA, dfB, 0.5)
    assert(precision == 1.0)
    assert(recall >= 0.9)
  }
}
