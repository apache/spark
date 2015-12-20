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
import java.util.Arrays;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
// $example off$

public class JavaALSExample {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("JavaALSExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(jsc);

    // $example on$
    JavaRDD<Row> jrdd = jsc.parallelize(Arrays.asList(
      RowFactory.create(1, 1, 5.0),
      RowFactory.create(1, 2, 1.0),
      RowFactory.create(1, 4, 1.0),
      RowFactory.create(2, 1, 5.0),
      RowFactory.create(2, 2, 1.0),
      RowFactory.create(2, 3, 5.0),
      RowFactory.create(3, 2, 5.0),
      RowFactory.create(3, 3, 1.0),
      RowFactory.create(3, 4, 5.0),
      RowFactory.create(4, 1, 1.0),
      RowFactory.create(4, 3, 1.0),
      RowFactory.create(4, 4, 5.0)
    ));
    StructType schema = new StructType(new StructField[]{
      new StructField("user", DataTypes.IntegerType, false, Metadata.empty()),
      new StructField("item", DataTypes.IntegerType, false, Metadata.empty()),
      new StructField("rating", DataTypes.FloatType, false, Metadata.empty())
    });
    DataFrame data = sqlContext.createDataFrame(jrdd, schema);

    // Build the recommendation model using ALS
    ALS als = new ALS()
        .setMaxIter(5)
        .setRegParam(0.01)
        .setUserCol("user")
        .setItemCol("item")
        .setRatingCol("rating");
    ALSModel model = als.fit(data);

    // Evaluate the model by computing the RMSE on the same dataset
    DataFrame predictions = model.transform(data);
    double mse = JavaDoubleRDD.fromRDD(predictions.javaRDD()
      .map(new Function<Row, Object>() {
        public Double call(Row row) {
          // Difference between rating and prediction
          Double err = (double) row.getFloat(0) - (double) row.getFloat(1);
          return err * err;
        }
      }).rdd()).mean();
    double rmse = Math.sqrt(mse);
    System.out.println("Root-mean-square error = " + rmse);
    // $example off$
    jsc.stop();
  }
}
