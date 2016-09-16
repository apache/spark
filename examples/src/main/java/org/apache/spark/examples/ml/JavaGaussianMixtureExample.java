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
import org.apache.spark.ml.clustering.GaussianMixture;
import org.apache.spark.ml.clustering.GaussianMixtureModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
// $example off$
import org.apache.spark.sql.SparkSession;


/**
 * An example demonstrating Gaussian Mixture Model.
 * Run with
 * <pre>
 * bin/run-example ml.JavaGaussianMixtureExample
 * </pre>
 */
public class JavaGaussianMixtureExample {

  public static void main(String[] args) {

    // Creates a SparkSession
    SparkSession spark = SparkSession
            .builder()
            .appName("JavaGaussianMixtureExample")
            .getOrCreate();

    // $example on$
    // Loads data
    Dataset<Row> dataset = spark.read().format("libsvm").load("data/mllib/sample_kmeans_data.txt");

    // Trains a GaussianMixture model
    GaussianMixture gmm = new GaussianMixture()
      .setK(2);
    GaussianMixtureModel model = gmm.fit(dataset);

    // Output the parameters of the mixture model
    for (int i = 0; i < model.getK(); i++) {
      System.out.printf("Gaussian %d:\nweight=%f\nmu=%s\nsigma=\n%s\n\n",
              i, model.weights()[i], model.gaussians()[i].mean(), model.gaussians()[i].cov());
    }
    // $example off$

    spark.stop();
  }
}
