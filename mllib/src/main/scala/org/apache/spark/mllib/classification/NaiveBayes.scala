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

package org.apache.spark.mllib.classification

import scala.collection.mutable

import org.jblas.DoubleMatrix

import org.apache.spark.Logging
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
 * Model for Naive Bayes Classifiers.
 *
 * @param pi Log of class priors, whose dimension is C.
 * @param theta Log of class conditional probabilities, whose dimension is CXD.
 */
class NaiveBayesModel(pi: Array[Double], theta: Array[Array[Double]])
  extends ClassificationModel with Serializable {

  // Create a column vector that can be used for predictions
  private val _pi = new DoubleMatrix(pi.length, 1, pi: _*)
  private val _theta = new DoubleMatrix(theta)

  def predict(testData: RDD[Array[Double]]): RDD[Double] = testData.map(predict)

  def predict(testData: Array[Double]): Double = {
    val dataMatrix = new DoubleMatrix(testData.length, 1, testData: _*)
    val result = _pi.add(_theta.mmul(dataMatrix))
    result.argmax()
  }
}

/**
 * Trains a Naive Bayes model given an RDD of `(label, features)` pairs.
 *
 * @param lambda The smooth parameter
 */
class NaiveBayes private (val lambda: Double = 1.0)
  extends Serializable with Logging {

  /**
   * Run the algorithm with the configured parameters on an input RDD of LabeledPoint entries.
   *
   * @param data RDD of (label, array of features) pairs.
   */
  def run(data: RDD[LabeledPoint]) = {
    // Aggregates all sample points to driver side to get sample count and summed feature vector
    // for each label.  The shape of `zeroCombiner` & `aggregated` is:
    //
    //    label: Int -> (count: Int, featuresSum: DoubleMatrix)
    val zeroCombiner = mutable.Map.empty[Int, (Int, DoubleMatrix)]
    val aggregated = data.aggregate(zeroCombiner)({ (combiner, point) =>
      point match {
        case LabeledPoint(label, features) =>
          val (count, featuresSum) = combiner.getOrElse(label.toInt, (0, DoubleMatrix.zeros(1)))
          val fs = new DoubleMatrix(features.length, 1, features: _*)
          combiner += label.toInt -> (count + 1, featuresSum.addi(fs))
      }
    }, { (lhs, rhs) =>
      for ((label, (c, fs)) <- rhs) {
        val (count, featuresSum) = lhs.getOrElse(label, (0, DoubleMatrix.zeros(1)))
        lhs(label) = (count + c, featuresSum.addi(fs))
      }
      lhs
    })

    // Kinds of label
    val C = aggregated.size
    // Total sample count
    val N = aggregated.values.map(_._1).sum

    val pi = new Array[Double](C)
    val theta = new Array[Array[Double]](C)
    val piLogDenom = math.log(N + C * lambda)

    for ((label, (count, fs)) <- aggregated) {
      val thetaLogDenom = math.log(fs.sum() + fs.length * lambda)
      pi(label) = math.log(count + lambda) - piLogDenom
      theta(label) = fs.toArray.map(f => math.log(f + lambda) - thetaLogDenom)
    }

    new NaiveBayesModel(pi, theta)
  }
}

object NaiveBayes {
  /**
   * Trains a Naive Bayes model given an RDD of `(label, features)` pairs.
   *
   * This is the Multinomial NB ([[http://tinyurl.com/lsdw6p]]) which can handle all kinds of
   * discrete data.  For example, by converting documents into TF-IDF vectors, it can be used for
   * document classification.  By making every vector a 0-1 vector. it can also be used as
   * Bernoulli NB ([[http://tinyurl.com/p7c96j6]]).
   *
   * @param input RDD of `(label, array of features)` pairs.  Every vector should be a frequency
   *              vector or a count vector.
   * @param lambda The smooth parameter
   */
  def train(input: RDD[LabeledPoint], lambda: Double = 1.0): NaiveBayesModel = {
    new NaiveBayes(lambda).run(input)
  }
}
