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


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.regression._
import org.apache.spark.rdd.RDD

trait BinaryClassificationModel extends ClassificationModel {
  /**
   * Return true labels and prediction scores in an RDD
   *
   * @param input RDD with labelled points to use for the evaluation
   * @return RDD[(Double, Double)] Contains a pair of (label, probability)
   *         where probability is the probability the model assigns to
   *         the label being 1.
   */ 
  def scoreForEval(input: RDD[LabeledPoint]) : RDD[(Double, Double)] = {
    val predictionAndLabel = input.map { point =>
        val scores = score(point.features)
        (scores, point.label)
    }
    predictionAndLabel
  }

  /**
   * Evaluate the performance of the model using the score assigned by the model
   * to observations and the true label.
   * Returns the Receiver operating characteristic area under the curve.
   * Note that we consider the prediction of a label to be 0 if the score is less than 0,
   * and we predict label 1 if the score is larger than 0.
   *
   * @param predictionAndLabel RDD with (score by model, true label)
   * @return Double Area under curve of ROC
   */ 
  def areaUnderROC(predictionAndLabel: RDD[(Double, Double)]) : Double = {
    val nObs = predictionAndLabel.count
    val nPos = predictionAndLabel.filter(x => x._2 == 1.0).count
    // sort according to the predicted score and add indices
    val sortedPredictionsWithIndex = predictionAndLabel.sortByKey(true).zipWithIndex
    // sum of the positive ranks
    val sumPosRanks = sortedPredictionsWithIndex.filter(x => (x._1)._2 > 0).map(x => x._2 + 1).sum
    // if there are no positive or no negative labels, the area under the curve is not defined.
    // Return 0 in that case.
    if ((nPos > 0) && (nObs > nPos)) {
          (sumPosRanks - nPos * (nPos + 1) / 2) / (nPos * (nObs - nPos))
        } else {
          0
        }
  }
}
