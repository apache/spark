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

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

// $example on$
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.feature.ElementwiseProduct;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
// $example off$

public class JavaElementwiseProductExample {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaElementwiseProductExample")
      .getOrCreate();

    // $example on$
    // Create some vector data; also works for sparse vectors
    List<Row> data = Arrays.asList(
      RowFactory.create("a", Vectors.dense(1.0, 2.0, 3.0)),
      RowFactory.create("b", Vectors.dense(4.0, 5.0, 6.0))
    );

    List<StructField> fields = new ArrayList<>(2);
    fields.add(DataTypes.createStructField("id", DataTypes.StringType, false));
    fields.add(DataTypes.createStructField("vector", new VectorUDT(), false));

    StructType schema = DataTypes.createStructType(fields);

    Dataset<Row> dataFrame = spark.createDataFrame(data, schema);

    Vector transformingVector = Vectors.dense(0.0, 1.0, 2.0);

    ElementwiseProduct transformer = new ElementwiseProduct()
      .setScalingVec(transformingVector)
      .setInputCol("vector")
      .setOutputCol("transformedVector");

    // Batch transform the vectors to create new column:
    transformer.transform(dataFrame).show();
    // $example off$
    spark.stop();
  }
}
