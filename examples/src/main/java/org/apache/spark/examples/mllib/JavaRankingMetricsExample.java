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

package org.apache.spark.examples.mllib;

// $example on$
import java.util.*;

import scala.Tuple2;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.evaluation.RegressionMetrics;
import org.apache.spark.mllib.evaluation.RankingMetrics;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
// $example off$
import org.apache.spark.SparkConf;

public class JavaRankingMetricsExample {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("Java Ranking Metrics Example");
    JavaSparkContext sc = new JavaSparkContext(conf);
    // $example on$
    String path = "data/mllib/sample_movielens_data.txt";
    JavaRDD<String> data = sc.textFile(path);
    JavaRDD<Rating> ratings = data.map(
      new Function<String, Rating>() {
        @Override
        public Rating call(String line) {
          String[] parts = line.split("::");
            return new Rating(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]), Double
              .parseDouble(parts[2]) - 2.5);
        }
      }
    );
    ratings.cache();

    // Train an ALS model
    final MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(ratings), 10, 10, 0.01);

    // Get top 10 recommendations for every user and scale ratings from 0 to 1
    JavaRDD<Tuple2<Object, Rating[]>> userRecs = model.recommendProductsForUsers(10).toJavaRDD();
    JavaRDD<Tuple2<Object, Rating[]>> userRecsScaled = userRecs.map(
      new Function<Tuple2<Object, Rating[]>, Tuple2<Object, Rating[]>>() {
        @Override
        public Tuple2<Object, Rating[]> call(Tuple2<Object, Rating[]> t) {
          Rating[] scaledRatings = new Rating[t._2().length];
          for (int i = 0; i < scaledRatings.length; i++) {
            double newRating = Math.max(Math.min(t._2()[i].rating(), 1.0), 0.0);
            scaledRatings[i] = new Rating(t._2()[i].user(), t._2()[i].product(), newRating);
          }
          return new Tuple2<>(t._1(), scaledRatings);
        }
      }
    );
    JavaPairRDD<Object, Rating[]> userRecommended = JavaPairRDD.fromJavaRDD(userRecsScaled);

    // Map ratings to 1 or 0, 1 indicating a movie that should be recommended
    JavaRDD<Rating> binarizedRatings = ratings.map(
      new Function<Rating, Rating>() {
        @Override
        public Rating call(Rating r) {
          double binaryRating;
          if (r.rating() > 0.0) {
            binaryRating = 1.0;
          } else {
            binaryRating = 0.0;
          }
          return new Rating(r.user(), r.product(), binaryRating);
        }
      }
    );

    // Group ratings by common user
    JavaPairRDD<Object, Iterable<Rating>> userMovies = binarizedRatings.groupBy(
      new Function<Rating, Object>() {
        @Override
        public Object call(Rating r) {
          return r.user();
        }
      }
    );

    // Get true relevant documents from all user ratings
    JavaPairRDD<Object, List<Integer>> userMoviesList = userMovies.mapValues(
      new Function<Iterable<Rating>, List<Integer>>() {
        @Override
        public List<Integer> call(Iterable<Rating> docs) {
          List<Integer> products = new ArrayList<>();
          for (Rating r : docs) {
            if (r.rating() > 0.0) {
              products.add(r.product());
            }
          }
          return products;
        }
      }
    );

    // Extract the product id from each recommendation
    JavaPairRDD<Object, List<Integer>> userRecommendedList = userRecommended.mapValues(
      new Function<Rating[], List<Integer>>() {
        @Override
        public List<Integer> call(Rating[] docs) {
          List<Integer> products = new ArrayList<>();
          for (Rating r : docs) {
            products.add(r.product());
          }
          return products;
        }
      }
    );
    JavaRDD<Tuple2<List<Integer>, List<Integer>>> relevantDocs = userMoviesList.join(
      userRecommendedList).values();

    // Instantiate the metrics object
    RankingMetrics<Integer> metrics = RankingMetrics.of(relevantDocs);

    // Precision and NDCG at k
    Integer[] kVector = {1, 3, 5};
    for (Integer k : kVector) {
      System.out.format("Precision at %d = %f\n", k, metrics.precisionAt(k));
      System.out.format("NDCG at %d = %f\n", k, metrics.ndcgAt(k));
    }

    // Mean average precision
    System.out.format("Mean average precision = %f\n", metrics.meanAveragePrecision());

    // Evaluate the model using numerical ratings and regression metrics
    JavaRDD<Tuple2<Object, Object>> userProducts = ratings.map(
      new Function<Rating, Tuple2<Object, Object>>() {
        @Override
        public Tuple2<Object, Object> call(Rating r) {
          return new Tuple2<Object, Object>(r.user(), r.product());
        }
      }
    );
    JavaPairRDD<Tuple2<Integer, Integer>, Object> predictions = JavaPairRDD.fromJavaRDD(
      model.predict(JavaRDD.toRDD(userProducts)).toJavaRDD().map(
        new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Object>>() {
          @Override
          public Tuple2<Tuple2<Integer, Integer>, Object> call(Rating r) {
            return new Tuple2<Tuple2<Integer, Integer>, Object>(
              new Tuple2<>(r.user(), r.product()), r.rating());
          }
        }
      ));
    JavaRDD<Tuple2<Object, Object>> ratesAndPreds =
      JavaPairRDD.fromJavaRDD(ratings.map(
        new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Object>>() {
          @Override
          public Tuple2<Tuple2<Integer, Integer>, Object> call(Rating r) {
            return new Tuple2<Tuple2<Integer, Integer>, Object>(
              new Tuple2<>(r.user(), r.product()), r.rating());
          }
        }
      )).join(predictions).values();

    // Create regression metrics object
    RegressionMetrics regressionMetrics = new RegressionMetrics(ratesAndPreds.rdd());

    // Root mean squared error
    System.out.format("RMSE = %f\n", regressionMetrics.rootMeanSquaredError());

    // R-squared
    System.out.format("R-squared = %f\n", regressionMetrics.r2());
    // $example off$

    sc.stop();
  }
}
