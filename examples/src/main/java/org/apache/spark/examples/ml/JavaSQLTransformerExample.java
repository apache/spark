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
import org.apache.spark.ml.feature.SQLTransformer;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.*;
// $example off$

public class JavaSQLTransformerExample {
  public static void main(String[] args) {

    SparkConf conf = new SparkConf().setAppName("JavaSQLTransformerExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(jsc);

    // $example on$
    JavaRDD<Row> jrdd = jsc.parallelize(Arrays.asList(
      RowFactory.create(0, 1.0, 3.0),
      RowFactory.create(2, 2.0, 5.0)
    ));
    StructType schema = new StructType(new StructField [] {
      new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
      new StructField("v1", DataTypes.DoubleType, false, Metadata.empty()),
      new StructField("v2", DataTypes.DoubleType, false, Metadata.empty())
    });
    DataFrame df = sqlContext.createDataFrame(jrdd, schema);

    SQLTransformer sqlTrans = new SQLTransformer().setStatement(
      "SELECT *, (v1 + v2) AS v3, (v1 * v2) AS v4 FROM __THIS__");

    sqlTrans.transform(df).show();
    // $example off$
  }
}
