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
import org.apache.spark.ml.feature.RFormula;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.*;
// $example off$

public class JavaRFormulaExample {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("JavaRFormulaExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(jsc);

    // $example on$
    StructType schema = createStructType(new StructField[]{
      createStructField("id", IntegerType, false),
      createStructField("country", StringType, false),
      createStructField("hour", IntegerType, false),
      createStructField("clicked", DoubleType, false)
    });

    JavaRDD<Row> rdd = jsc.parallelize(Arrays.asList(
      RowFactory.create(7, "US", 18, 1.0),
      RowFactory.create(8, "CA", 12, 0.0),
      RowFactory.create(9, "NZ", 15, 0.0)
    ));

    DataFrame dataset = sqlContext.createDataFrame(rdd, schema);
    RFormula formula = new RFormula()
      .setFormula("clicked ~ country + hour")
      .setFeaturesCol("features")
      .setLabelCol("label");
    DataFrame output = formula.fit(dataset).transform(dataset);
    output.select("features", "label").show();
    // $example off$
    jsc.stop();
  }
}

