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
import org.apache.spark.mllib.evaluation.{RankingMetrics, RegressionMetrics}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
// $example off$
import org.apache.spark.sql.SparkSession

object RankingMetricsExample {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("RankingMetricsExample")
      .getOrCreate()
    import spark.implicits._
    // $example on$
    // Read in the ratings data
    val ratings = spark.read.textFile("data/mllib/sample_movielens_data.txt").rdd.map { line =>
      val fields = line.split("::")
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble - 2.5)
    }.cache()

    // Map ratings to 1 or 0, 1 indicating a movie that should be recommended
    val binarizedRatings = ratings.map(r => Rating(r.user, r.product,
      if (r.rating > 0) 1.0 else 0.0)).cache()

    // Summarize ratings
    val numRatings = ratings.count()
    val numUsers = ratings.map(_.user).distinct().count()
    val numMovies = ratings.map(_.product).distinct().count()
    println(s"Got $numRatings ratings from $numUsers users on $numMovies movies.")

    // Build the model
    val numIterations = 10
    val rank = 10
    val lambda = 0.01
    val model = ALS.train(ratings, rank, numIterations, lambda)

    // Define a function to scale ratings from 0 to 1
    def scaledRating(r: Rating): Rating = {
      val scaledRating = math.max(math.min(r.rating, 1.0), 0.0)
      Rating(r.user, r.product, scaledRating)
    }

    // Get sorted top ten predictions for each user and then scale from [0, 1]
    val userRecommended = model.recommendProductsForUsers(10).map { case (user, recs) =>
      (user, recs.map(scaledRating))
    }

    // Assume that any movie a user rated 3 or higher (which maps to a 1) is a relevant document
    // Compare with top ten most relevant documents
    val userMovies = binarizedRatings.groupBy(_.user)
    val relevantDocuments = userMovies.join(userRecommended).map { case (user, (actual,
    predictions)) =>
      (predictions.map(_.product), actual.filter(_.rating > 0.0).map(_.product).toArray)
    }

    // Instantiate metrics object
    val metrics = new RankingMetrics(relevantDocuments)

    // Precision at K
    Array(1, 3, 5).foreach { k =>
      println(s"Precision at $k = ${metrics.precisionAt(k)}")
    }

    // Mean average precision
    println(s"Mean average precision = ${metrics.meanAveragePrecision}")

    // Mean average precision at k
    println(s"Mean average precision at 2 = ${metrics.meanAveragePrecisionAt(2)}")

    // Normalized discounted cumulative gain
    Array(1, 3, 5).foreach { k =>
      println(s"NDCG at $k = ${metrics.ndcgAt(k)}")
    }

    // Recall at K
    Array(1, 3, 5).foreach { k =>
      println(s"Recall at $k = ${metrics.recallAt(k)}")
    }

    // Get predictions for each data point
    val allPredictions = model.predict(ratings.map(r => (r.user, r.product))).map(r => ((r.user,
      r.product), r.rating))
    val allRatings = ratings.map(r => ((r.user, r.product), r.rating))
    val predictionsAndLabels = allPredictions.join(allRatings).map { case ((user, product),
    (predicted, actual)) =>
      (predicted, actual)
    }

    // Get the RMSE using regression metrics
    val regressionMetrics = new RegressionMetrics(predictionsAndLabels)
    println(s"RMSE = ${regressionMetrics.rootMeanSquaredError}")

    // R-squared
    println(s"R-squared = ${regressionMetrics.r2}")
    // $example off$
  }
}
// scalastyle:on println
