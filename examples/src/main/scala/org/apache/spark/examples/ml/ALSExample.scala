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

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.Row
// $example off$

object ALSExample {

  // $example on$
  case class Rating(userId: Int, itemId: Int, rating: Float)
  // $example off$

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ALSExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    // $example on$
    // Load the data stored in csv format as a DataFrame.
    val data = sc.textFile("data/mllib/als/test.data")
    val ratings = data.map(_.split(',') match { case Array(user, item, rating) =>
      Rating(user.toInt, item.toInt, rating.toFloat)
    }).toDF("user", "item", "rating")

    // Build the recommandation model using ALS
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("user")
      .setItemCol("item")
      .setRatingCol("rating")
    val model = als.fit(ratings)

    // Evaluating the model by computing the RMSE on the same dataset
    val predictions = model.transform(ratings)
    val mse = predictions
      .select("rating", "prediction")
      .map { case Row(rating: Float, prediction: Float) =>
        val err = rating.toDouble - prediction.toDouble
        err * err
      }.mean()
    val rmse = math.sqrt(mse)
    println(s"Root-mean-square error = $rmse")
    // $example off$
    sc.stop()
  }
}
// scalastyle:on println

