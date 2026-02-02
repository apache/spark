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

import org.apache.spark.ml.fpm.FPGrowth;
import org.apache.spark.ml.fpm.FPGrowthModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
// $example off$

/**
 * An example demonstrating FPGrowth.
 * Run with
 * <pre>
 * bin/run-example ml.JavaFPGrowthExample
 * </pre>
 */
public class JavaFPGrowthExample {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaFPGrowthExample")
      .getOrCreate();

    // $example on$
    List<Row> data = Arrays.asList(
      RowFactory.create(Arrays.asList("1 2 5".split(" "))),
      RowFactory.create(Arrays.asList("1 2 3 5".split(" "))),
      RowFactory.create(Arrays.asList("1 2".split(" ")))
    );
    StructType schema = new StructType(new StructField[]{ new StructField(
      "items", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
    });
    Dataset<Row> itemsDF = spark.createDataFrame(data, schema);

    FPGrowthModel model = new FPGrowth()
      .setItemsCol("items")
      .setMinSupport(0.5)
      .setMinConfidence(0.6)
      .fit(itemsDF);

    // Display frequent itemsets.
    model.freqItemsets().show();

    // Display generated association rules.
    model.associationRules().show();

    // transform examines the input items against all the association rules and summarize the
    // consequents as prediction
    model.transform(itemsDF).show();
    // $example off$

    spark.stop();
  }
}
