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

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.DCT;
import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * An example demonstrating a discrete cosine transform.
 * Run with
 * <pre>
 * bin/run-example ml.JavaDCT <file> <k>
 * </pre>
 */
public class JavaDCT {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("JavaDCT");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext jsql = new SQLContext(jsc);

    JavaRDD<Row> data = jsc.parallelize(Arrays.asList(
        RowFactory.create(Vectors.dense(0.0, 1.0, -2.0, 3.0)),
        RowFactory.create(Vectors.dense(-1.0, 2.0, 4.0, -7.0)),
        RowFactory.create(Vectors.dense(14.0, -2.0, -5.0, 1.0))
    ));
    StructType schema = new StructType(new StructField[]{
        new StructField("features", new VectorUDT(), false, Metadata.empty()),
    });
    DataFrame df = jsql.createDataFrame(data, schema);
    DCT dct = new DCT()
        .setInputCol("features")
        .setOutputCol("featuresDCT")
        .setInverse(false);
    DataFrame dctDf = dct.transform(df);
        dctDf.select("featuresDCT").show(3);
  }
}

