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

package org.apache.spark.examples.ml

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.sql.{SQLContext, DataFrame}

/**
 * An example runner for binarizer. Run with
 * {{{
 * ./bin/run-example ml.BinarizerExample [options]
 * }}}
 */
object BinarizerExample {

  val conf = new SparkConf().setAppName("BinarizerExample")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  val data = Array(
    (0, 0.1),
    (1, 0.8),
    (2, 0.2)
  )
  val dataFrame: DataFrame = sqlContext.createDataFrame(data).toDF("label", "feature")

  val binarizer: Binarizer = new Binarizer()
    .setInputCol("feature")
    .setOutputCol("binarized_feature")
    .setThreshold(0.5)

  val binarizedDataFrame = binarizer.transform(dataFrame)
  val binarizedFeatures = binarizedDataFrame.select("binarized_feature")
  binarizedFeatures.collect().foreach(println)
}
