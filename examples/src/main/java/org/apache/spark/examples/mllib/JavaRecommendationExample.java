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
import scala.Tuple2;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.SparkConf;
// $example off$

public class JavaRecommendationExample {
  public static void main(String[] args) {
    // $example on$
    SparkConf conf = new SparkConf().setAppName("Java Collaborative Filtering Example");
    JavaSparkContext jsc = new JavaSparkContext(conf);

    // Load and parse the data
    String path = "data/mllib/als/test.data";
    JavaRDD<String> data = jsc.textFile(path);
    JavaRDD<Rating> ratings = data.map(
      new Function<String, Rating>() {
        public Rating call(String s) {
          String[] sarray = s.split(",");
          return new Rating(Integer.parseInt(sarray[0]), Integer.parseInt(sarray[1]),
            Double.parseDouble(sarray[2]));
        }
      }
    );

    // Build the recommendation model using ALS
    int rank = 10;
    int numIterations = 10;
    MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(ratings), rank, numIterations, 0.01);

    // Evaluate the model on rating data
    JavaRDD<Tuple2<Object, Object>> userProducts = ratings.map(
      new Function<Rating, Tuple2<Object, Object>>() {
        public Tuple2<Object, Object> call(Rating r) {
          return new Tuple2<Object, Object>(r.user(), r.product());
        }
      }
    );
    JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD.fromJavaRDD(
      model.predict(JavaRDD.toRDD(userProducts)).toJavaRDD().map(
        new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {
          public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating r){
            return new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating());
          }
        }
      ));
    JavaRDD<Tuple2<Double, Double>> ratesAndPreds =
      JavaPairRDD.fromJavaRDD(ratings.map(
        new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {
          public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating r){
            return new Tuple2<>(new Tuple2<>(r.user(), r.product()), r.rating());
          }
        }
      )).join(predictions).values();
    double MSE = JavaDoubleRDD.fromRDD(ratesAndPreds.map(
      new Function<Tuple2<Double, Double>, Object>() {
        public Object call(Tuple2<Double, Double> pair) {
          Double err = pair._1() - pair._2();
          return err * err;
        }
      }
    ).rdd()).mean();
    System.out.println("Mean Squared Error = " + MSE);

    // Save and load model
    model.save(jsc.sc(), "target/tmp/myCollaborativeFilter");
    MatrixFactorizationModel sameModel = MatrixFactorizationModel.load(jsc.sc(),
      "target/tmp/myCollaborativeFilter");
    // $example off$

    jsc.stop();
  }
}
