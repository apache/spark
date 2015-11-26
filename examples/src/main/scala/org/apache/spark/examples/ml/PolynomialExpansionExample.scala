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

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.ml.feature.PolynomialExpansion
import org.apache.spark.mllib.linalg.Vectors

/**
 * An example runner for polynomial expansion. Run with
 * {{{
 * ./bin/run-example ml.PolynomialExpansionExample [options]
 * }}}
 */
object PolynomialExpansionExample {

  val conf = new SparkConf().setAppName("PolynomialExpansionExample")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  val data = Array(
    Vectors.dense(-2.0, 2.3),
    Vectors.dense(0.0, 0.0),
    Vectors.dense(0.6, -1.1)
  )
  val df = sqlContext.createDataFrame(data.map(Tuple1.apply)).toDF("features")
  val polynomialExpansion = new PolynomialExpansion()
    .setInputCol("features")
    .setOutputCol("polyFeatures")
    .setDegree(3)
  val polyDF = polynomialExpansion.transform(df)
  polyDF.select("polyFeatures").take(3).foreach(println)
}


