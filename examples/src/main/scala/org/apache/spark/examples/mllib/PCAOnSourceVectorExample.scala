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
import org.apache.spark.SparkContext
// $example on$
import org.apache.spark.mllib.feature.PCA
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
// $example off$

object PCAOnSourceVectorExample {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("PCAOnSourceVectorExample")
    val sc = new SparkContext(conf)

    // $example on$
    val data: RDD[LabeledPoint] = sc.parallelize(Seq(
      new LabeledPoint(0, Vectors.dense(1, 0, 0, 0, 1)),
      new LabeledPoint(1, Vectors.dense(1, 1, 0, 1, 0)),
      new LabeledPoint(1, Vectors.dense(1, 1, 0, 0, 0)),
      new LabeledPoint(0, Vectors.dense(1, 0, 0, 0, 0)),
      new LabeledPoint(1, Vectors.dense(1, 1, 0, 0, 0))))

    // Compute the top 5 principal components.
    val pca = new PCA(5).fit(data.map(_.features))

    // Project vectors to the linear space spanned by the top 5 principal
    // components, keeping the label
    val projected = data.map(p => p.copy(features = pca.transform(p.features)))
    // $example off$
    val collect = projected.collect()
    println("Projected vector of principal component:")
    collect.foreach { vector => println(vector) }

    sc.stop()
  }
}
// scalastyle:on println
