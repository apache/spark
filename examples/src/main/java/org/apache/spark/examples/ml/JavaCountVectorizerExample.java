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

// $example on$
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.*;
// $example off$

public class JavaCountVectorizerExample {
  public static void main(String[] args) {

    SparkConf conf = new SparkConf().setAppName("JavaCountVectorizerExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(jsc);

    // $example on$
    // Input data: Each row is a bag of words from a sentence or document.
    JavaRDD<Row> jrdd = jsc.parallelize(Arrays.asList(
      RowFactory.create(Arrays.asList("a", "b", "c")),
      RowFactory.create(Arrays.asList("a", "b", "b", "c", "a"))
    ));
    StructType schema = new StructType(new StructField [] {
      new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
    });
    DataFrame df = sqlContext.createDataFrame(jrdd, schema);

    // fit a CountVectorizerModel from the corpus
    CountVectorizerModel cvModel = new CountVectorizer()
      .setInputCol("text")
      .setOutputCol("feature")
      .setVocabSize(3)
      .setMinDF(2)
      .fit(df);

    // alternatively, define CountVectorizerModel with a-priori vocabulary
    CountVectorizerModel cvm = new CountVectorizerModel(new String[]{"a", "b", "c"})
      .setInputCol("text")
      .setOutputCol("feature");

    cvModel.transform(df).show();
    // $example off$
  }
}
