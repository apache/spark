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

package org.apache.spark.examples.mllib;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

// $example on$
import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.DistributedLDAModel;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.clustering.LDAModel;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
// $example off$

public class JavaLatentDirichletAllocationExample {
  public static void main(String[] args) {

    SparkConf conf = new SparkConf().setAppName("JavaKLatentDirichletAllocationExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);

    // $example on$
    // Load and parse the data
    String path = "data/mllib/sample_lda_data.txt";
    JavaRDD<String> data = jsc.textFile(path);
    JavaRDD<Vector> parsedData = data.map(s -> {
      String[] sarray = s.trim().split(" ");
      double[] values = new double[sarray.length];
      for (int i = 0; i < sarray.length; i++) {
        values[i] = Double.parseDouble(sarray[i]);
      }
      return Vectors.dense(values);
    });
    // Index documents with unique IDs
    JavaPairRDD<Long, Vector> corpus =
      JavaPairRDD.fromJavaRDD(parsedData.zipWithIndex().map(Tuple2::swap));
    corpus.cache();

    // Cluster the documents into three topics using LDA
    LDAModel ldaModel = new LDA().setK(3).run(corpus);

    // Output topics. Each is a distribution over words (matching word count vectors)
    System.out.println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize()
      + " words):");
    Matrix topics = ldaModel.topicsMatrix();
    for (int topic = 0; topic < 3; topic++) {
      System.out.print("Topic " + topic + ":");
      for (int word = 0; word < ldaModel.vocabSize(); word++) {
        System.out.print(" " + topics.apply(word, topic));
      }
      System.out.println();
    }

    ldaModel.save(jsc.sc(),
      "target/org/apache/spark/JavaLatentDirichletAllocationExample/LDAModel");
    DistributedLDAModel sameModel = DistributedLDAModel.load(jsc.sc(),
      "target/org/apache/spark/JavaLatentDirichletAllocationExample/LDAModel");
    // $example off$

    jsc.stop();
  }
}
