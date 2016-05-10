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

import org.apache.spark.sql.SparkSession;

// $example on$
import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
// $example off$

public class JavaTokenizerExample {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaTokenizerExample")
      .getOrCreate();

    // $example on$
    List<Row> data = Arrays.asList(
      RowFactory.create(0, "Hi I heard about Spark"),
      RowFactory.create(1, "I wish Java could use case classes"),
      RowFactory.create(2, "Logistic,regression,models,are,neat")
    );

    StructType schema = new StructType(new StructField[]{
      new StructField("label", DataTypes.IntegerType, false, Metadata.empty()),
      new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
    });

    Dataset<Row> sentenceDataFrame = spark.createDataFrame(data, schema);

    Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");

    Dataset<Row> wordsDataFrame = tokenizer.transform(sentenceDataFrame);
    for (Row r : wordsDataFrame.select("words", "label").takeAsList(3)) {
      java.util.List<String> words = r.getList(0);
      for (String word : words) System.out.print(word + " ");
      System.out.println();
    }

    RegexTokenizer regexTokenizer = new RegexTokenizer()
      .setInputCol("sentence")
      .setOutputCol("words")
      .setPattern("\\W");  // alternatively .setPattern("\\w+").setGaps(false);
    // $example off$
    spark.stop();
  }
}
