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
import org.apache.spark.util.random.XORShiftRandom

import org.scalatest.FunSuite

class ANNSuite extends FunSuite with LocalSparkContext {

  test("ANN learns XOR function") {
    val inputs = Array[Array[Double]](
      Array[Double](0,0),
      Array[Double](0,1),
      Array[Double](1,0),
      Array[Double](1,1)
    )
    val outputs = Array[Double](0, 1, 1, 0)
    val hiddenSize = 5
    val data = inputs.zip(outputs).map { case(features, label) =>
      (Vectors.dense(features), Vectors.dense(Array(label)))}
    val rddData = sc.parallelize(data, 2)
    val hiddenLayersTopology = Array[Int](hiddenSize)
    val model = ArtificialNeuralNetwork.train(rddData, hiddenLayersTopology, 100, 1e-5)
    val predictionAndLabels = rddData.map { case(input, label) =>
      (model.predict(input)(0), label(0)) }.collect()
    assert(predictionAndLabels.forall { case(p, l) => (math.round(p) - l) == 0 })
  }

  test("Gradient of ANN") {

    val eps = 1e-6
    val accept = 1e-7

    val topologyArr = Array[Array[Int]](
      Array[Int](1, 5, 1),
      Array[Int](5, 10, 5, 3),
      Array[Int](128, 256, 128)
    )

    val rnd = new XORShiftRandom(0)

    var cnt = 0
    while( cnt<topologyArr.length ) {

      val topology = topologyArr(cnt)
      val L = topology.length - 1
      val noInp = topology(0)
      val noOut = topology(L)
      val annGradient = new ANNLeastSquaresGradient(topology)
      var noWeights = 0

      var l = 1
      while(l <= L) {
        noWeights += (topology(l - 1) + 1) * topology(l)
        l += 1
      }

      val arrWeights = new Array[Double](noWeights)

      var w = 0
      while(w < noWeights) {
        arrWeights(w) = rnd.nextDouble()
        w += 1
      }

      val arrInp = new Array[Double](noInp)
      val arrOut = new Array[Double](noOut)
      val arrData = new Array[Double](noInp + noOut)

      w = 0
      while(w < noInp) {
        arrInp(w) = rnd.nextDouble()
        arrData(w) = arrInp(w)
        w += 1
      }

      w = 0
      while(w < noOut) {
        arrOut(w) = rnd.nextDouble()
        arrData(noInp + w) = arrOut(w)
        w += 1
      }

      val data = Vectors.dense( arrData )
      val brzOut = Vectors.dense( arrOut ).toBreeze
      val weights = Vectors.dense( arrWeights )
      val gradient = annGradient.compute( data, 0.0, weights )._1

      val arrTmpWeights = new Array[Double]( noWeights )
      Array.copy(arrWeights, 0, arrTmpWeights, 0, noWeights )
      val tmpWeights = Vectors.dense( arrTmpWeights )

      w = 0
      while(w < noWeights)
      {
        arrTmpWeights(w) = arrTmpWeights(w) + eps

        val annModel1 = new ArtificialNeuralNetworkModel(weights, topology)
        val brzO1 = annModel1.predict(data).toBreeze

        val annModel2 = new ArtificialNeuralNetworkModel(tmpWeights, topology)
        val brzO2 = annModel2.predict(data).toBreeze

        val E1 = .5* (brzO1 - brzOut).dot(brzO1 - brzOut)
        val E2 = .5* (brzO2 - brzOut).dot(brzO2 - brzOut)
        val dEdW = ( E2 - E1 ) / eps

        val gradw = gradient(w)
        val err = dEdW - gradw
        assert(math.abs(err) < accept, 
      s"Difference between calculated and approximated gradient too large ($dEdW - $gradw = $err)"
        )

        arrTmpWeights(w) = arrTmpWeights(w) - eps

        w += 1
      }

      cnt += 1
    }

  }

}
