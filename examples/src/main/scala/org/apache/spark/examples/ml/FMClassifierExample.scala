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
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{FMClassificationModel, FMClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, MinMaxScaler, StringIndexer}
// $example off$
import org.apache.spark.sql.SparkSession

object FMClassifierExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("FMClassifierExample")
      .getOrCreate()

    // $example on$
    // Load and parse the data file, converting it to a DataFrame.
    val data = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(data)
    // Scale features.
    val featureScaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .fit(data)

    // Split the data into training and test sets (30% held out for testing).
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    // Train a FM model.
    val fm = new FMClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("scaledFeatures")
      .setStepSize(0.001)

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labelsArray(0))

    // Create a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureScaler, fm, labelConverter))

    // Train model.
    val model = pipeline.fit(trainingData)

    // Make predictions.
    val predictions = model.transform(testData)

    // Select example rows to display.
    predictions.select("predictedLabel", "label", "features").show(5)

    // Select (prediction, true label) and compute test accuracy.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println(s"Test set accuracy = $accuracy")

    val fmModel = model.stages(2).asInstanceOf[FMClassificationModel]
    println(s"Factors: ${fmModel.factors} Linear: ${fmModel.linear} " +
      s"Intercept: ${fmModel.intercept}")
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println
