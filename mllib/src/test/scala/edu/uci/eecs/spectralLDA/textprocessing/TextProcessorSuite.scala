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

package edu.uci.eecs.spectralLDA.textprocessing

import breeze.linalg._

import org.apache.spark.SparkFunSuite
import org.apache.spark.mllib.util.MLlibTestSparkContext


class TextProcessorSuite extends SparkFunSuite with MLlibTestSparkContext {
  test("Inverse Document Frequency") {
    val docs = Seq[(Long, SparseVector[Double])](
      (1000L, SparseVector[Double](0.0, 0.0, 7.0)),
      (1001L, SparseVector[Double](0.0, 3.0, 5.0)),
      (1002L, SparseVector[Double](1.0, 3.0, 0.5))
    )
    val docsRDD = sc.parallelize[(Long, SparseVector[Double])](docs)

    val idf = TextProcessor.inverseDocumentFrequency(docsRDD)

    assert(norm(idf - DenseVector[Double](3.0, 1.5, 1.0)) <= 1e-8)
  }
}

