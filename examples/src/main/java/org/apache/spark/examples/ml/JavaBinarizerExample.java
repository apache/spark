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

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.Binarizer;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
// $example off$

public class JavaBinarizerExample {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("JavaBinarizerExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext jsql = new SQLContext(jsc);

    // $example on$
    JavaRDD<Row> jrdd = jsc.parallelize(Arrays.asList(
      RowFactory.create(0, 0.1),
      RowFactory.create(1, 0.8),
      RowFactory.create(2, 0.2)
    ));
    StructType schema = new StructType(new StructField[]{
      new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
      new StructField("feature", DataTypes.DoubleType, false, Metadata.empty())
    });
    DataFrame continuousDataFrame = jsql.createDataFrame(jrdd, schema);
    Binarizer binarizer = new Binarizer()
      .setInputCol("feature")
      .setOutputCol("binarized_feature")
      .setThreshold(0.5);
    DataFrame binarizedDataFrame = binarizer.transform(continuousDataFrame);
    DataFrame binarizedFeatures = binarizedDataFrame.select("binarized_feature");
    for (Row r : binarizedFeatures.collect()) {
      Double binarized_value = r.getDouble(0);
      System.out.println(binarized_value);
    }
    // $example off$
    jsc.stop();
  }
}