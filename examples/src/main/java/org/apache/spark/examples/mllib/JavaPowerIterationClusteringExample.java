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

import org.apache.spark.api.java.JavaSparkContext;
// $example on$
import scala.Tuple3;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.PowerIterationClustering;
import org.apache.spark.mllib.clustering.PowerIterationClusteringModel;
// $example off$

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SQLContext;

public class JavaPowerIterationClusteringExample {
  public static void main(String[] args) {

    SparkConf conf = new SparkConf().setAppName("JavaPowerIterationClusteringExample")
            .setMaster("local[*]");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(jsc);

    // $example on$
    // Load and parse the data
    JavaRDD<String> data = jsc.textFile("data/mllib/pic_data.txt");
    JavaRDD<Tuple3<Long, Long, Double>> similarities = data.map(
            new Function<String, Tuple3<Long, Long, Double>>() {
              public Tuple3<Long, Long, Double> call(String line) {
                String[] parts = line.split(" ");
                return new Tuple3<>(new Long(parts[0]), new Long(parts[1]), new Double(parts[2]));
              }
            }
    );

    // Cluster the data into two classes using PowerIterationClustering
    PowerIterationClustering pic = new PowerIterationClustering()
            .setK(2)
            .setMaxIterations(10);
    PowerIterationClusteringModel model = pic.run(similarities);

    for (PowerIterationClustering.Assignment a: model.assignments().toJavaRDD().collect()) {
      System.out.println(a.id() + " -> " + a.cluster());
    }

    // Save and load model
    model.save(jsc.sc(), "myModelPath");
    PowerIterationClusteringModel sameModel = PowerIterationClusteringModel
            .load(jsc.sc(), "myModelPath");
    // $example off$

    jsc.stop();
  }
}
