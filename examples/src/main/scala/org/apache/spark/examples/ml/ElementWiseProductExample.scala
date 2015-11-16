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
import org.apache.spark.ml.feature.ElementwiseProduct
import org.apache.spark.mllib.linalg.Vectors

/**
 * An example runner for element wise product. Run with
 * {{{
 * ./bin/run-example ml.ElementWiseProductExample [options]
 * }}}
 */
object ElementWiseProductExample {

  val conf = new SparkConf().setAppName("OneHotEncoderExample")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  // Create some vector data; also works for sparse vectors
  val dataFrame = sqlContext.createDataFrame(Seq(
    ("a", Vectors.dense(1.0, 2.0, 3.0)),
    ("b", Vectors.dense(4.0, 5.0, 6.0)))).toDF("id", "vector")

  val transformingVector = Vectors.dense(0.0, 1.0, 2.0)
  val transformer = new ElementwiseProduct()
    .setScalingVec(transformingVector)
    .setInputCol("vector")
    .setOutputCol("transformedVector")

  // Batch transform the vectors to create new column:
  transformer.transform(dataFrame).show()
}

