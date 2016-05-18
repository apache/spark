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

import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
// $example off$

public class JavaWord2VecExample {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaWord2VecExample")
      .getOrCreate();

    // $example on$
    // Input data: Each row is a bag of words from a sentence or document.
    List<Row> data = Arrays.asList(
      RowFactory.create(Arrays.asList("Hi I heard about Spark".split(" "))),
      RowFactory.create(Arrays.asList("I wish Java could use case classes".split(" "))),
      RowFactory.create(Arrays.asList("Logistic regression models are neat".split(" ")))
    );
    StructType schema = new StructType(new StructField[]{
      new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
    });
    Dataset<Row> documentDF = spark.createDataFrame(data, schema);

    // Learn a mapping from words to Vectors.
    Word2Vec word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0);
    Word2VecModel model = word2Vec.fit(documentDF);
    Dataset<Row> result = model.transform(documentDF);
    for (Row r : result.select("result").takeAsList(3)) {
      System.out.println(r);
    }
    // $example off$

    spark.stop();
  }
}
