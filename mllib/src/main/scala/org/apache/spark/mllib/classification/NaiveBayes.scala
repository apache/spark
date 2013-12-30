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

import org.jblas.DoubleMatrix

import org.apache.spark.Logging
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

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
    // Prepares input data, the shape of resulted RDD is:
    //
    //    label: Int -> (count: Int, features: DoubleMatrix)
    //
    // The added count field is initialized to 1 to enable the following `foldByKey` transformation.
    val mappedData = data.map { case LabeledPoint(label, features) =>
      label.toInt -> (1, new DoubleMatrix(features.length, 1, features: _*))
    }

    // Gets a map from labels to their corresponding sample point counts and summed feature vectors.
    // Shape of resulted RDD is:
    //
    //    label: Int -> (count: Int, summedFeatureVector: DoubleMatrix)
    //
    // Two tricky parts worth explaining:
    //
    // 1. Feature vectors are summed with the inplace jblas matrix addition operation, thus we
    //    chose `foldByKey` instead of `reduceByKey` to avoid modifying original input data.
    //
    // 2. The zero value passed to `foldByKey` contains a `null` rather than a zero vector because
    //    the dimension of the feature vector is unknown.  Calling `data.first.length` to get the
    //    dimension is not preferable since it requires an expensive RDD action.
    val countsAndSummedFeatures = mappedData.foldByKey((0, null)) { (lhs, rhs) =>
      if (lhs._1 == 0) {
        (rhs._1, new DoubleMatrix().copy(rhs._2))
      } else {
        (lhs._1 + rhs._1, lhs._2.addi(rhs._2))
      }
    }

    val collected = countsAndSummedFeatures.mapValues { case (count, summedFeatureVector) =>
      val p = math.log(count + lambda)
      val logDenom = math.log(summedFeatureVector.sum + summedFeatureVector.length * lambda)
      val t = summedFeatureVector
      var i = 0
      while (i < t.length) {
        t.put(i, math.log(t.get(i) + lambda) - logDenom)
        i += 1
      }
      (count, p, t)
    }.collectAsMap()

    // Total sample count.  Calling `data.count` to get `N` is not preferable since it triggers
    // an expensive RDD action
    val N = collected.values.map(_._1).sum

    // Kinds of label.
    val C = collected.size

    val logDenom = math.log(N + C * lambda)
    val pi = new Array[Double](C)
    val theta = new Array[Array[Double]](C)

    for ((label, (_, p, t)) <- collected) {
      pi(label) = p - logDenom
      theta(label) = t.toArray
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
