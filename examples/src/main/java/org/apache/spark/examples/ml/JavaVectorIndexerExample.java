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
import java.util.Map;

import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
// $example off$

public class JavaVectorIndexerExample {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaVectorIndexerExample")
      .getOrCreate();

    // $example on$
    Dataset<Row> data = spark.read().format("libsvm").load("data/mllib/sample_libsvm_data.txt");

    VectorIndexer indexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexed")
      .setMaxCategories(10);
    VectorIndexerModel indexerModel = indexer.fit(data);

    Map<Integer, Map<Double, Integer>> categoryMaps = indexerModel.javaCategoryMaps();
    System.out.print("Chose " + categoryMaps.size() + " categorical features:");

    for (Integer feature : categoryMaps.keySet()) {
      System.out.print(" " + feature);
    }
    System.out.println();

    // Create new column "indexed" with categorical values transformed to indices
    Dataset<Row> indexedData = indexerModel.transform(data);
    indexedData.show();
    // $example off$
    spark.stop();
  }
}
