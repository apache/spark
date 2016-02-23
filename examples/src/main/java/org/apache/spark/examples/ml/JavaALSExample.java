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

package org.apache.spark.examples.ml;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

// $example on$
import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.types.DataTypes;
// $example off$

public class JavaALSExample {

  // $example on$
  public static class Rating implements Serializable {
    private int userId;
    private int movieId;
    private float rating;
    private long timestamp;

    public Rating() {}

    public Rating(int userId, int movieId, float rating, long timestamp) {
      this.userId = userId;
      this.movieId = movieId;
      this.rating = rating;
      this.timestamp = timestamp;
    }

    public int getUserId() {
      return userId;
    }

    public int getMovieId() {
      return movieId;
    }

    public float getRating() {
      return rating;
    }

    public long getTimestamp() {
      return timestamp;
    }

    public static Rating parseRating(String str) {
      String[] fields = str.split("::");
      if (fields.length != 4) {
        throw new IllegalArgumentException("Each line must contain 4 fields");
      }
      int userId = Integer.parseInt(fields[0]);
      int movieId = Integer.parseInt(fields[1]);
      float rating = Float.parseFloat(fields[2]);
      long timestamp = Long.parseLong(fields[3]);
      return new Rating(userId, movieId, rating, timestamp);
    }
  }
  // $example off$

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("JavaALSExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(jsc);

    // $example on$
    JavaRDD<Rating> ratingsRDD = jsc.textFile("data/mllib/als/sample_movielens_ratings.txt")
      .map(new Function<String, Rating>() {
        public Rating call(String str) {
          return Rating.parseRating(str);
        }
      });
    DataFrame ratings = sqlContext.createDataFrame(ratingsRDD, Rating.class);
    DataFrame[] splits = ratings.randomSplit(new double[]{0.8, 0.2});
    DataFrame training = splits[0];
    DataFrame test = splits[1];

    // Build the recommendation model using ALS on the training data
    ALS als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating");
    ALSModel model = als.fit(training);

    // Evaluate the model by computing the RMSE on the test data
    DataFrame rawPredictions = model.transform(test);
    DataFrame predictions = rawPredictions
      .withColumn("rating", rawPredictions.col("rating").cast(DataTypes.DoubleType))
      .withColumn("prediction", rawPredictions.col("prediction").cast(DataTypes.DoubleType));

    RegressionEvaluator evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction");
    Double rmse = evaluator.evaluate(predictions);
    System.out.println("Root-mean-square error = " + rmse);
    // $example off$
    jsc.stop();
  }
}
