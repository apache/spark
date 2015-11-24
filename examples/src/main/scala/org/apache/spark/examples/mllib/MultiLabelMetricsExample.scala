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

// $example on$
import org.apache.spark.mllib.evaluation.MultilabelMetrics
import org.apache.spark.rdd.RDD
// $example off$
import org.apache.spark.{SparkContext, SparkConf}

object MultiLabelMetricsExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MultiLabelMetricsExample")
    val sc = new SparkContext(conf)
    // $example on$
    val scoreAndLabels: RDD[(Array[Double], Array[Double])] = sc.parallelize(
      Seq((Array(0.0, 1.0), Array(0.0, 2.0)),
        (Array(0.0, 2.0), Array(0.0, 1.0)),
        (Array.empty[Double], Array(0.0)),
        (Array(2.0), Array(2.0)),
        (Array(2.0, 0.0), Array(2.0, 0.0)),
        (Array(0.0, 1.0, 2.0), Array(0.0, 1.0)),
        (Array(1.0), Array(1.0, 2.0))), 2)

    // Instantiate metrics object
    val metrics = new MultilabelMetrics(scoreAndLabels)

    // Summary stats
    println(s"Recall = ${metrics.recall}")
    println(s"Precision = ${metrics.precision}")
    println(s"F1 measure = ${metrics.f1Measure}")
    println(s"Accuracy = ${metrics.accuracy}")

    // Individual label stats
    metrics.labels.foreach(label =>
      println(s"Class $label precision = ${metrics.precision(label)}"))
    metrics.labels.foreach(label => println(s"Class $label recall = ${metrics.recall(label)}"))
    metrics.labels.foreach(label => println(s"Class $label F1-score = ${metrics.f1Measure(label)}"))

    // Micro stats
    println(s"Micro recall = ${metrics.microRecall}")
    println(s"Micro precision = ${metrics.microPrecision}")
    println(s"Micro F1 measure = ${metrics.microF1Measure}")

    // Hamming loss
    println(s"Hamming loss = ${metrics.hammingLoss}")

    // Subset accuracy
    println(s"Subset accuracy = ${metrics.subsetAccuracy}")
    // $example off$
  }
}
// scalastyle:on println
