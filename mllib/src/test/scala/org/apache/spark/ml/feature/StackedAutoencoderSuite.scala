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
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.Row

class StackedAutoencoderSuite extends SparkFunSuite with MLlibTestSparkContext {

  // using data similar to https://inst.eecs.berkeley.edu/~cs182/sp08/assignments/a3-tlearn.html
  val binaryData = Seq(
    Vectors.dense(Array(1.0, 0.0, 0.0, 0.0)),
    Vectors.dense(Array(0.0, 1.0, 0.0, 0.0)),
    Vectors.dense(Array(0.0, 0.0, 1.0, 0.0)),
    Vectors.dense(Array(0.0, 0.0, 0.0, 1.0)))

  val real01Data = Seq(
    Vectors.dense(Array(0.5, 0.1, 0.1, 0.1)),
    Vectors.dense(Array(0.1, 0.6, 0.5, 0.5)),
    Vectors.dense(Array(0.5, 0.5, 0.5, 0.5)),
    Vectors.dense(Array(0.9, 0.9, 0.9, 0.9)))

  val realData = Seq(
    Vectors.dense(Array(10.0, 0.0, 0.0, 0.0)),
    Vectors.dense(Array(0.0, 1.0, 0.0, 0.0)),
    Vectors.dense(Array(0.0, 0.0, 10.0, 0.0)),
    Vectors.dense(Array(0.0, 0.0, 0.0, 10.0)))

  test("Autoencoder reconstructs the original data by encoding and decoding") {
    val dataSets = Seq(binaryData, real01Data, realData)
    val dataTypes = Seq(true, true, false)
    val dataSetAndTypes = dataSets.zip(dataTypes)
    for ((data, is01) <- dataSetAndTypes) {
      val rdd = sc.parallelize(data, 1).map(x => Tuple1(x))
      val df = spark.createDataFrame(rdd).toDF("input")
      val stackedAutoencoder = new StackedAutoencoder()
        .setLayers(Array(4, 3, 3))
        .setBlockSize(1)
        .setMaxIter(100)
        .setSeed(11L)
        .setTol(1e-6)
        .setInputCol("input")
        .setOutputCol("output")
        .setDataIn01Interval(is01)
        .setBuildDecoder(true)
      // TODO: find a way to inherit the input and output parameter value from estimator
      val saModel = stackedAutoencoder.fit(df)
      saModel.setInputCol("input").setOutputCol("encoded")
      // encoding
      val encodedData = saModel.transform(df)
      // decoding
      saModel.setInputCol("encoded").setOutputCol("decoded")
      val decodedData = saModel.decode(encodedData)
      // epsilon == 1/100 of the maximum value
      val eps = if (is01) 1.0 / 100 else 10.0 / 100
      decodedData.collect.foreach { case Row(input: Vector, _: Vector, decoded: Vector) =>
        assert(input ~== decoded absTol eps)
      }
    }
  }

  test("Autoencoder use for pre-training") {
    val dataFrame = spark.createDataFrame(Seq(
      (Vectors.dense(0.0, 0.0), 0.0),
      (Vectors.dense(0.0, 1.0), 1.0),
      (Vectors.dense(1.0, 0.0), 1.0),
      (Vectors.dense(1.0, 1.0), 0.0))
    ).toDF("features", "label")
    val layers = Array[Int](2, 7, 6, 5, 4, 3, 2)
    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(1)
      .setSeed(12L)
      .setMaxIter(1)
      .setTol(1e-6)
    val initialWeights = trainer.fit(dataFrame).weights
    trainer
      .setInitialWeights(initialWeights.copy)
      .setMaxIter(10)
    val badModel = trainer.fit(dataFrame)
    val badResult = badModel.transform(dataFrame)
    val badPredictionAndLabels = badResult.select("prediction", "label").collect()
    // solution converged to a bad optimum
    assert(!badPredictionAndLabels.forall { case Row(p: Double, l: Double) =>
      p == l
    }, "Model should not predict as expected")

    // pre-train all layers except last as stacked autoencoder
    val encoderLayers = layers.init
    val autoEncoder = new StackedAutoencoder("stackedAutoencoder")
      .setBlockSize(1)
      .setBuildDecoder(false)
      .setDataIn01Interval(true)
      .setInputCol("features")
      .setLayers(encoderLayers)
      .setMaxIter(10)
      .setSeed(12L)
      .setTol(1e-6)
    val autoEncoderModel = autoEncoder.fit(dataFrame)
    val autoEncoderWeights = autoEncoderModel.encoderWeights
    // initialize weights for the classifier and copy pre-trained weights
    System.arraycopy(
      autoEncoderWeights.toArray, 0, initialWeights.toArray, 0, autoEncoderWeights.toArray.length)
    val preTrainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(1)
      .setInitialWeights(initialWeights)
      .setMaxIter(10)
      .setTol(1e-6)
    val preModel = preTrainer.fit(dataFrame)
    val preResult = preModel.transform(dataFrame)
    val predictionAndLabels = preResult.select("prediction", "label").collect()
    predictionAndLabels.foreach { case Row(p: Double, l: Double) =>
      assert(p == l)
    }
  }
}
