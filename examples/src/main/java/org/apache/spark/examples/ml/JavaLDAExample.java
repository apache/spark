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
import org.apache.spark.ml.clustering.LDA;
import org.apache.spark.ml.clustering.LDAModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
// $example off$

/**
 * An example demonstrating LDA.
 * Run with
 * <pre>
 * bin/run-example ml.JavaLDAExample
 * </pre>
 */
public class JavaLDAExample {

  public static void main(String[] args) {
    // Creates a SparkSession
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaLDAExample")
      .getOrCreate();

    // $example on$
    // Loads data.
    Dataset<Row> dataset = spark.read().format("libsvm")
      .load("data/mllib/sample_lda_libsvm_data.txt");

    // Trains a LDA model.
    LDA lda = new LDA().setK(10).setMaxIter(10);
    LDAModel model = lda.fit(dataset);

    double ll = model.logLikelihood(dataset);
    double lp = model.logPerplexity(dataset);
    System.out.println("The lower bound on the log likelihood of the entire corpus: " + ll);
    System.out.println("The upper bound on perplexity: " + lp);

    // Describe topics.
    Dataset<Row> topics = model.describeTopics(3);
    System.out.println("The topics described by their top-weighted terms:");
    topics.show(false);

    // Shows the result.
    Dataset<Row> transformed = model.transform(dataset);
    transformed.show(false);
    // $example off$

    spark.stop();
  }
}
