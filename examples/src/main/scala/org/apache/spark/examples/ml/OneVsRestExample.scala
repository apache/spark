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
package org.apache.spark.examples.ml

// $example on$
import org.apache.spark.ml.classification.{LogisticRegression, OneVsRest}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.{DataFrame, Dataset}
// $example off$
import org.apache.spark.sql.SparkSession

/**
 * An example runner for Multiclass to Binary Reduction with One Vs Rest.
 * The example uses Logistic Regression as the base classifier.
 * Run with
 * {{{
 * ./bin/run-example ml.OneVsRestExample [options]
 * }}}
 */

object OneVsRestExample {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName(s"OneVsRestExample")
      .getOrCreate()

    // $example on$
    // load data file.
    val inputData: DataFrame = spark.read.format("libsvm")
      .load("data/mllib/sample_multiclass_classification_data.txt")

    // generate the train/test split.
    val Array(train, test) = inputData.randomSplit(Array(0.8, 0.2))

    // instantiate the base classifier
    val classifier = new LogisticRegression()
      .setMaxIter(10)
      .setTol(1E-6)
      .setFitIntercept(true)

    // instantiate the One Vs Rest Classifier.
    val ovr = new OneVsRest()
    ovr.setClassifier(classifier)

    // train the multiclass model.
    val ovrModel = ovr.fit(train)

    // score the model on test data.
    val predictions = ovrModel.transform(test)

    // obtain metrics.
    val metrics = new MulticlassMetrics(predictions.as[(Double, Double)].rdd)

    val confusionMatrix = metrics.confusionMatrix

    // compute the false positive rate per label.
    val numClasses = confusionMatrix.numRows
    val fprs = Range(0, numClasses).map(p => (p, metrics.falsePositiveRate(p.toDouble)))

    println(s" Confusion Matrix\n ${confusionMatrix.toString}\n")
    println("label\tfpr")
    println(fprs.map {case (label, fpr) => label + "\t" + fpr}.mkString("\n"))
    // $example off$

    spark.stop()
  }

}
// scalastyle:on println
