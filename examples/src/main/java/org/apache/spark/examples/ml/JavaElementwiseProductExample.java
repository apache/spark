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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.ElementwiseProduct;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
// $example off$

public class JavaElementwiseProductExample {
  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("JavaElementwiseProductExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(jsc);

    // $example on$
    // Create some vector data; also works for sparse vectors
    JavaRDD<Row> jrdd = jsc.parallelize(Arrays.asList(
      RowFactory.create("a", Vectors.dense(1.0, 2.0, 3.0)),
      RowFactory.create("b", Vectors.dense(4.0, 5.0, 6.0))
    ));

    List<StructField> fields = new ArrayList<StructField>(2);
    fields.add(DataTypes.createStructField("id", DataTypes.StringType, false));
    fields.add(DataTypes.createStructField("vector", new VectorUDT(), false));

    StructType schema = DataTypes.createStructType(fields);

    DataFrame dataFrame = sqlContext.createDataFrame(jrdd, schema);

    Vector transformingVector = Vectors.dense(0.0, 1.0, 2.0);

    ElementwiseProduct transformer = new ElementwiseProduct()
      .setScalingVec(transformingVector)
      .setInputCol("vector")
      .setOutputCol("transformedVector");

    // Batch transform the vectors to create new column:
    transformer.transform(dataFrame).show();
    // $example off$
    jsc.stop();
  }
}