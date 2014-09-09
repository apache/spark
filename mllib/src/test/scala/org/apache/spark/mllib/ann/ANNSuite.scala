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

package org.apache.spark.mllib.ann

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.LocalSparkContext
import org.scalatest.FunSuite

class ANNSuite extends FunSuite with LocalSparkContext {
  private val inputs = Array[Array[Double]](
    Array[Double](0,0),
    Array[Double](0,1),
    Array[Double](1,0),
    Array[Double](1,1)
  )
  private val outputs = Array[Double](0, 1, 1, 0)
  private val inputSize = 2
  private val hiddenSize = 5
  private val outputSize = 1
  test("ANN learns XOR function") {
    val data = inputs.zip(outputs).map { case(features, label) =>
      (Vectors.dense(features), Vectors.dense(Array(label)))}
    val rddData = sc.parallelize(data, 2)
    val topology = Array[Int](inputSize, hiddenSize, outputSize)
    val model = ArtificialNeuralNetwork.train(rddData, topology, 2000, 2.0, 1.0)
    val predictionAndLabels = rddData.map { case(input, label) =>
      (model.predictV(input)(0), label(0)) }.collect()
    assert(predictionAndLabels.forall { case(p, l) => (math.round(p) - l) == 0 })
  }
}
