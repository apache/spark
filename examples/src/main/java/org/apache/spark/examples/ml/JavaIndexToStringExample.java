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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

// $example on$
import java.util.Arrays;

import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
// $example off$

public class JavaIndexToStringExample {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("JavaIndexToStringExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(jsc);

    // $example on$
    JavaRDD<Row> jrdd = jsc.parallelize(Arrays.asList(
      RowFactory.create(0, "a"),
      RowFactory.create(1, "b"),
      RowFactory.create(2, "c"),
      RowFactory.create(3, "a"),
      RowFactory.create(4, "a"),
      RowFactory.create(5, "c")
    ));
    StructType schema = new StructType(new StructField[]{
      new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
      new StructField("category", DataTypes.StringType, false, Metadata.empty())
    });
    DataFrame df = sqlContext.createDataFrame(jrdd, schema);

    StringIndexerModel indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")
      .fit(df);
    DataFrame indexed = indexer.transform(df);

    IndexToString converter = new IndexToString()
      .setInputCol("categoryIndex")
      .setOutputCol("originalCategory");
    DataFrame converted = converter.transform(indexed);
    converted.select("id", "originalCategory").show();
    // $example off$
    jsc.stop();
  }
}
