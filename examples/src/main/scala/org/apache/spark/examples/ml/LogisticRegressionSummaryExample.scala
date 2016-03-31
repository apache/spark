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

import java.io.File

import com.google.common.io.Files

import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
// $example off$
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext

object LogisticRegressionSummaryExample {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LogisticRegressionSummaryExample")
    val sc = new SparkContext(conf)
    val sqlCtx = new SQLContext(sc)
    import sqlCtx.implicits._

    // Load training data
    val training = sqlCtx.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    // Show statistical summary of labels.
    val labelSummary = training.describe("label")
    labelSummary.show()

    // Convert features column to an RDD of vectors.
    val features = training.select("features").rdd.map { case Row(v: Vector) => v }
    val featureSummary = features.aggregate(new MultivariateOnlineSummarizer())(
      (summary, feat) => summary.add(feat),
      (sum1, sum2) => sum1.merge(sum2))
    println(s"Selected features column with average values:\n ${featureSummary.mean.toString}")

    // Save the records in a parquet file.
    val tmpDir = Files.createTempDir()
    tmpDir.deleteOnExit()
    val outputDir = new File(tmpDir, "dataframe").toString
    println(s"Saving to $outputDir as Parquet file.")
    training.write.parquet(outputDir)

    // Load the records back.
    println(s"Loading Parquet file with UDT from $outputDir.")
    val newDF = sqlCtx.read.parquet(outputDir)
    println(s"Schema from Parquet:")
    newDF.printSchema()

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    // Fit the model
    val lrModel = lr.fit(training)

    // $example on$
    // Extract the summary from the returned LogisticRegressionModel instance.
    val trainingSummary = lrModel.summary

    // Obtain the objective per iteration.
    val objectiveHistory = trainingSummary.objectiveHistory
    objectiveHistory.foreach(loss => println(loss))

    // Obtain the metrics useful to judge performance on test data.
    // We cast the summary to a BinaryLogisticRegressionSummary since the problem is a binary
    // classification problem.
    val binarySummary = trainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]

    // Obtain the receiver-operating characteristic as a DataFrame and areaUnderROC.
    val roc = binarySummary.roc
    roc.show()
    println(binarySummary.areaUnderROC)

    // Set the model threshold to maximize F-Measure.
    val fMeasure = binarySummary.fMeasureByThreshold
    val maxFMeasure = fMeasure.select(max("F-Measure")).head().getDouble(0)
    val bestThreshold = fMeasure.where($"F-Measure" === maxFMeasure)
      .select("threshold").head().getDouble(0)
    lrModel.setThreshold(bestThreshold)
    // $example off$

    // Use extra params

    // We may alternatively specify parameters using a ParamMap, which supports several methods for
    // specifying parameters.
    val paramMap = ParamMap(lr.maxIter -> 20)
    // Specify 1 Param.  This overwrites the original maxIter.
    paramMap.put(lr.maxIter, 30)
    // Specify multiple Params.
    paramMap.put(lr.regParam -> 0.1, lr.thresholds -> Array(0.45, 0.55))

    // One can also combine ParamMaps.
    val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability")  // Change output column name.
    val paramMapCombined = paramMap ++ paramMap2

    // Now learn a new model using the paramMapCombined parameters.
    // paramMapCombined overrides all parameters set earlier via lr.set* methods.
    val model2 = lr.fit(training.toDF(), paramMapCombined)
    println("Model 2 was fit using parameters: " + model2.parent.extractParamMap())

    sc.stop()
  }
}
// scalastyle:on println
