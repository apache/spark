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

package org.apache.spark.examples.mllib

import scala.language.reflectiveCalls

import scopt.OptionParser

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.{RandomForestClassificationModel, RandomForestClassifier, DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils


object NewDT {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName(s"NewDT")
    val sc = new SparkContext(conf)

    val data = sc.parallelize(Seq(
      LabeledPoint(1.0, Vectors.dense(1,2,3)),
      LabeledPoint(0.0, Vectors.dense(2,3,4))))

    /****************** EXAMPLE OF Decision Tree **************************/
    val dt = new DecisionTreeClassifier()
      .setImpurity(DecisionTreeClassifier.Impurities.Gini)
      .setCheckpointInterval(5)
      .setImpurity(DecisionTreeClassifier.Impurities.Gini)
      .setCheckpointInterval(5)
    val dtModel: DecisionTreeClassificationModel = dt.run(data)

    /****************** EXAMPLE OF Ensemble **************************/
    val rf = new RandomForestClassifier()
      .setImpurity(RandomForestClassifier.supportedImpurities.Gini)
      .setCheckpointInterval(5)
      .setFeatureSubsetStrategy("auto")
      .setImpurity(RandomForestClassifier.supportedImpurities.Gini)
      .setCheckpointInterval(5)
      .setFeatureSubsetStrategy("onethird")
    val rfModel: RandomForestClassificationModel = rf.run(data)

    sc.stop()
  }

}
