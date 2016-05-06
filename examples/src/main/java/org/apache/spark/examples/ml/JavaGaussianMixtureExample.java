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

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
// $example on$
import org.apache.spark.ml.clustering.GaussianMixture;
import org.apache.spark.ml.clustering.GaussianMixtureModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
// $example off$


/**
 * An example demonstrating a Gaussian Mixture Model.
 * Run with
 * <pre>
 * bin/run-example ml.JavaGaussianMixtureExample
 * </pre>
 */
public class JavaGaussianMixtureExample {

  private static class ParsePoint implements Function<String, Row> {
    private static final Pattern separator = Pattern.compile(" ");

    @Override
    public Row call(String line) {
      String[] tok = separator.split(line.trim());
      double[] point = new double[tok.length];
      for (int i = 0; i < tok.length; ++i) {
        point[i] = Double.parseDouble(tok[i]);
      }
      Vector[] points = {Vectors.dense(point)};
      return new GenericRow(points);
    }
  }

  public static void main(String[] args) {

    String inputFile = "data/mllib/gmm_data.txt";
    int k = 2;

    // Parses the arguments
    SparkSession spark = SparkSession
            .builder()
            .appName("JavaGaussianMixtureExample")
            .getOrCreate();

    // $example on$
    // Loads data
    JavaRDD<Row> points = spark.read().text(inputFile).javaRDD().map(new ParsePoint());
    StructField[] fields = {new StructField("features", new VectorUDT(), false, Metadata.empty())};
    StructType schema = new StructType(fields);
    Dataset<Row> dataset = spark.createDataFrame(points, schema);

    // Trains a GaussianMixture model
    GaussianMixture gmm = new GaussianMixture()
      .setK(k);
    GaussianMixtureModel model = gmm.fit(dataset);

    // Output the parameters of the mixture model
    for (int j = 0; j < model.getK(); j++) {
      System.out.printf("weight=%f\nmu=%s\nsigma=\n%s\n",
              model.weights()[j], model.gaussians()[j].mean(), model.gaussians()[j].cov());
    }
    // $example off$

    spark.stop();
  }
}
