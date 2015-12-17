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

import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
// $example on$
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
// $example off$


/**
 * An example demonstrating a k-means clustering.
 * Run with
 * <pre>
 * bin/run-example ml.JavaKMeansExample <file> <k>
 * </pre>
 */
public class JavaKMeansExample {

  private static class ParsePoint implements Function<String, Row> {
    private static final Pattern separator = Pattern.compile(" ");

    @Override
    public Row call(String line) {
      String[] tok = separator.split(line);
      double[] point = new double[tok.length];
      for (int i = 0; i < tok.length; ++i) {
        point[i] = Double.parseDouble(tok[i]);
      }
      Vector[] points = {Vectors.dense(point)};
      return new GenericRow(points);
    }
  }

  public static void main(String[] args) {
    if (args.length != 2) {
      System.err.println("Usage: ml.JavaKMeansExample <file> <k>");
      System.exit(1);
    }
    String inputFile = args[0];
    int k = Integer.parseInt(args[1]);

    // Parses the arguments
    SparkConf conf = new SparkConf().setAppName("JavaKMeansExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(jsc);

    // $example on$
    // Loads data
    JavaRDD<Row> points = jsc.textFile(inputFile).map(new ParsePoint());
    StructField[] fields = {new StructField("features", new VectorUDT(), false, Metadata.empty())};
    StructType schema = new StructType(fields);
    DataFrame dataset = sqlContext.createDataFrame(points, schema);

    // Trains a k-means model
    KMeans kmeans = new KMeans()
      .setK(k);
    KMeansModel model = kmeans.fit(dataset);

    // Shows the result
    Vector[] centers = model.clusterCenters();
    System.out.println("Cluster Centers: ");
    for (Vector center: centers) {
      System.out.println(center);
    }
    // $example off$

    jsc.stop();
  }
}
