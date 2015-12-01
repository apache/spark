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
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
// $example off$

public class JavaStopWordsRemoverExample {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("JavaStopWordsRemoverExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext jsql = new SQLContext(jsc);

    // $example on$
    StopWordsRemover remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered");

    JavaRDD<Row> rdd = jsc.parallelize(Arrays.asList(
      RowFactory.create(Arrays.asList("I", "saw", "the", "red", "baloon")),
      RowFactory.create(Arrays.asList("Mary", "had", "a", "little", "lamb"))
    ));

    StructType schema = new StructType(new StructField[]{
      new StructField(
        "raw", DataTypes.createArrayType(DataTypes.StringType), false, Metadata.empty())
    });

    DataFrame dataset = jsql.createDataFrame(rdd, schema);
    remover.transform(dataset).show();
    // $example off$
    jsc.stop();
  }
}
