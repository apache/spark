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

import org.apache.spark.mllib.linalg.{DenseVector, Vectors, Vector}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.util.random.XORShiftRandom
import breeze.linalg.{DenseVector => BDV}

import org.scalatest.FunSuite

class ANNSuite extends FunSuite with MLlibTestSparkContext {

  test("ANN learns XOR function") {
    val inputs = Array[Array[Double]](
      Array[Double](0,0),
      Array[Double](0,1),
      Array[Double](1,0),
      Array[Double](1,1)
    )
    val outputs = Array[Double](0, 1, 1, 0)
    val data = inputs.zip(outputs).map { case(features, label) =>
      (Vectors.dense(features), Vectors.dense(Array(label)))}
    val rddData = sc.parallelize(data, 2)
    val hiddenLayersTopology = Array[Int](5)
    val initialWeights = ArtificialNeuralNetwork.
      randomWeights(rddData, hiddenLayersTopology, 0x01234567)
    val model = ArtificialNeuralNetwork.
      train(rddData, 4, hiddenLayersTopology, initialWeights, 200, 1e-4)
    val predictionAndLabels = rddData.map { case(input, label) =>
      (model.predict(input)(0), label(0)) }.collect()
    assert(predictionAndLabels.forall { case(p, l) => (math.round(p) - l) == 0 })
  }
  
  /*
  This test compares the output of the annGradient.compute function with the following
  approximations:
    
  dE / dw_k ~= ( E(w + eps*e_k, x) - E(w, x) ) / eps
    
  where E(w, x) is the summed squared error multiplied by a factor 0.5, given weight vector w
  and input x, w_k the k-th element in the weight vector (starting with k=0) and e_k the
  associated k-th cartesian unit vector.
  
  The test is passed when the difference is less than accept=1e-7 with eps=1e-6.
  */
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
      val nextTopology = topologyArr(cnt)
      val L = nextTopology.length - 1
      val noInp = nextTopology(0)
      val noOut = nextTopology(L)
      val annGradient = new ANNLeastSquaresGradient(nextTopology)
      var noWeights = 0
      var l = 1
      while(l <= L) {
        noWeights += (nextTopology(l - 1) + 1) * nextTopology(l)
        l += 1
      }
      val arrWeights = new Array[Double](noWeights)
      var w = 0
      while(w < noWeights) {
        arrWeights(w) = rnd.nextDouble() * 4.8 - 2.4
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
        val iData = Vectors.dense(arrData.slice(0, noInp))
        val annModel1 = new ArtificialNeuralNetworkModel(weights, nextTopology)
        val brzO1 = annModel1.predict(iData).toBreeze
        val annModel2 = new ArtificialNeuralNetworkModel(tmpWeights, nextTopology)
        val brzO2 = annModel2.predict(iData).toBreeze
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
