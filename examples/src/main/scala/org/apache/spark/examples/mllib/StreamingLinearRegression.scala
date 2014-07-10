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

import org.apache.spark.SparkConf
import org.apache.spark.mllib.util.MLStreamingUtils
import org.apache.spark.mllib.regression.StreamingLinearRegressionWithSGD
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Continually update a model on one stream of data using streaming linear regression,
 * while making predictions on another stream of data
 *
 */
object StreamingLinearRegression {

  def main(args: Array[String]) {

    if (args.length != 4) {
      System.err.println("Usage: StreamingLinearRegression <trainingData> <testData> <batchDuration> <numFeatures>")
      System.exit(1)
    }

    val conf = new SparkConf().setMaster("local").setAppName("StreamingLinearRegression")
    val ssc = new StreamingContext(conf, Seconds(args(2).toLong))

    val trainingData = MLStreamingUtils.loadLabeledPointsFromText(ssc, args(0))
    val testData = MLStreamingUtils.loadLabeledPointsFromText(ssc, args(1))

    val model = StreamingLinearRegressionWithSGD.start(args(3).toInt)

    model.trainOn(trainingData)
    model.predictOn(testData).print()

    ssc.start()
    ssc.awaitTermination()

  }

}
