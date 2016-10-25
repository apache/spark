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
import java.util.List;

import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
// $example off$

public class JavaCountVectorizerExample {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaCountVectorizerExample")
      .getOrCreate();

    // $example on$
    // Input data: Each row is a bag of words from a sentence or document.
    List<Row> data = Arrays.asList(
      RowFactory.create(Arrays.asList("a", "b", "c")),
      RowFactory.create(Arrays.asList("a", "b", "b", "c", "a"))
    );
    StructType schema = new StructType(new StructField [] {
      new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
    });
    Dataset<Row> df = spark.createDataFrame(data, schema);

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

    cvModel.transform(df).show(false);
    // $example off$

    spark.stop();
  }
}
