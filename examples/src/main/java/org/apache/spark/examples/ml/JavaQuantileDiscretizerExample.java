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
import org.apache.spark.ml.feature.QuantileDiscretizer;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
// $example off$

public class JavaQuantileDiscretizerExample {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("JavaQuantileDiscretizerExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(jsc);

    // $example on$
    JavaRDD<Row> jrdd = jsc.parallelize(
      Arrays.asList(
        RowFactory.create(0, 18.0),
        RowFactory.create(1, 19.0),
        RowFactory.create(2, 8.0),
        RowFactory.create(3, 5.0),
        RowFactory.create(4, 2.2)
      )
    );

    StructType schema = new StructType(new StructField[]{
      new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
      new StructField("hour", DataTypes.DoubleType, false, Metadata.empty())
    });

    DataFrame df = sqlContext.createDataFrame(jrdd, schema);

    QuantileDiscretizer discretizer = new QuantileDiscretizer()
      .setInputCol("hour")
      .setOutputCol("result")
      .setNumBuckets(3);

    DataFrame result = discretizer.fit(df).transform(df);
    result.show();
    // $example off$
    jsc.stop();
  }
}
