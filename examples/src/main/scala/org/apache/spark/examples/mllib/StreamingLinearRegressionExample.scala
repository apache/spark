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
// $example on$
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.StreamingLinearRegressionWithSGD
// $example off$
import org.apache.spark.streaming._

/**
 * Train a linear regression model on one stream of data and make predictions
 * on another stream, where the data streams arrive as text files
 * into two different directories.
 *
 * The rows of the text files must be labeled data points in the form
 * `(y,[x1,x2,x3,...,xn])`
 * Where n is the number of features. n must be the same for train and test.
 *
 * Usage: StreamingLinearRegressionExample <trainingDir> <testDir>
 *
 * To run on your local machine using the two directories `trainingDir` and `testDir`,
 * with updates every 5 seconds, and 2 features per data point, call:
 *    $ bin/run-example mllib.StreamingLinearRegressionExample trainingDir testDir
 *
 * As you add text files to `trainingDir` the model will continuously update.
 * Anytime you add text files to `testDir`, you'll see predictions from the current model.
 *
 */
object StreamingLinearRegressionExample {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: StreamingLinearRegressionExample <trainingDir> <testDir>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("StreamingLinearRegressionExample")
    val ssc = new StreamingContext(conf, Seconds(1))

    // $example on$
    val trainingData = ssc.textFileStream(args(0)).map(LabeledPoint.parse).cache()
    val testData = ssc.textFileStream(args(1)).map(LabeledPoint.parse)

    val numFeatures = 3
    val model = new StreamingLinearRegressionWithSGD()
      .setInitialWeights(Vectors.zeros(numFeatures))

    model.trainOn(trainingData)
    model.predictOnValues(testData.map(lp => (lp.label, lp.features))).print()

    ssc.start()
    ssc.awaitTermination()
    // $example off$

    ssc.stop()
  }
}
// scalastyle:on println
