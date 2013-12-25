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

import org.apache.spark.Logging
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.jblas.DoubleMatrix

/**
 * Model for Naive Bayes Classifiers.
 *
 * @param weightPerLabel Weights computed for every label, which's dimension is C.
 * @param weightMatrix Weights computed for every label and feature, which's dimension is CXD
 */
class NaiveBayesModel(val weightPerLabel: Array[Double],
    val weightMatrix: Array[Array[Double]])
  extends ClassificationModel with Serializable {

  // Create a column vector that can be used for predictions
  private val _weightPerLabel = new DoubleMatrix(weightPerLabel.length, 1, weightPerLabel:_*)
  private val _weightMatrix = new DoubleMatrix(weightMatrix)

  def predict(testData: RDD[Array[Double]]): RDD[Double] = testData.map(predict)

  def predict(testData: Array[Double]): Double = {
    val dataMatrix = new DoubleMatrix(testData.length, 1, testData: _*)
    val result = _weightPerLabel.add(_weightMatrix.mmul(dataMatrix))
    result.argmax()
  }
}

class NaiveBayes private (val lambda: Double = 1.0) // smoothing parameter
  extends Serializable with Logging {

  private[this] def vectorAdd(v1: Array[Double], v2: Array[Double]) =
    v1.zip(v2).map(pair => pair._1 + pair._2)

  /**
   * Run the algorithm with the configured parameters on an input
   * RDD of LabeledPoint entries.
   *
   * @param C kind of labels, labels are continuous integers and the maximal label is C-1
   * @param D dimension of feature vectors
   * @param data RDD of (label, array of features) pairs.
   */
  def run(C: Int, D: Int, data: RDD[LabeledPoint]) = {
    val locallyReduced = data.mapPartitions { iterator =>
      val localLabelCounts = mutable.Map.empty[Int, Int].withDefaultValue(0)
      val localSummedObservations =
        mutable.Map.empty[Int, Array[Double]].withDefaultValue(Array.fill(D)(0.0))

      for (LabeledPoint(label, features) <- iterator; i = label.toInt) {
        localLabelCounts(i) += 1
        localSummedObservations(i) = vectorAdd(localSummedObservations(i), features)
      }

      for ((label, count) <- localLabelCounts.toIterator) yield {
        label -> (count, localSummedObservations(label))
      }
    }

    val reduced = locallyReduced.reduceByKey { (lhs, rhs) =>
      (lhs._1 + rhs._1, vectorAdd(lhs._2, rhs._2))
    }

    val collected = reduced.mapValues { case (count, summed) =>
      val labelWeight = math.log(count + lambda)
      val logDenom = math.log(summed.sum + D * lambda)
      val weights = summed.map(w => math.log(w + lambda) - logDenom)
      (count, labelWeight, weights)
    }.collectAsMap()

    val weightPerLabel = {
      val N = collected.values.map(_._1).sum
      val logDenom = math.log(N + C * lambda)
      collected.mapValues(_._2 - logDenom).toArray.sortBy(_._1).map(_._2)
    }

    val weightMatrix = collected.mapValues(_._3).toArray.sortBy(_._1).map(_._2)

    new NaiveBayesModel(weightPerLabel, weightMatrix)
  }
}

object NaiveBayes {
  /**
   * Train a naive bayes model given an RDD of (label, features) pairs.
   *
   * @param C kind of labels, the maximal label is C-1
   * @param D dimension of feature vectors
   * @param input RDD of (label, array of features) pairs.
   * @param lambda smooth parameter
   */
  def train(C: Int, D: Int, input: RDD[LabeledPoint],
      lambda: Double = 1.0): NaiveBayesModel = {
    new NaiveBayes(lambda).run(C, D, input)
  }
}
