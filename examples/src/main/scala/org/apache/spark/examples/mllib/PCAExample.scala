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

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
// $example on$
import org.apache.spark.mllib.feature.PCA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
// $example off$

@deprecated("Deprecated since LinearRegressionWithSGD is deprecated.  Use ml.feature.PCA", "2.0.0")
object PCAExample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("PCAExample")
    val sc = new SparkContext(conf)

    // $example on$
    val data = sc.textFile("data/mllib/ridge-data/lpsa.data").map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }.cache()

    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    val pca = new PCA(training.first().features.size / 2).fit(data.map(_.features))
    val training_pca = training.map(p => p.copy(features = pca.transform(p.features)))
    val test_pca = test.map(p => p.copy(features = pca.transform(p.features)))

    val numIterations = 100
    val model = LinearRegressionWithSGD.train(training, numIterations)
    val model_pca = LinearRegressionWithSGD.train(training_pca, numIterations)

    val valuesAndPreds = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    val valuesAndPreds_pca = test_pca.map { point =>
      val score = model_pca.predict(point.features)
      (score, point.label)
    }

    val MSE = valuesAndPreds.map { case (v, p) => math.pow((v - p), 2) }.mean()
    val MSE_pca = valuesAndPreds_pca.map { case (v, p) => math.pow((v - p), 2) }.mean()

    println("Mean Squared Error = " + MSE)
    println("PCA Mean Squared Error = " + MSE_pca)
    // $example off$

    sc.stop()
  }
}
// scalastyle:on println
