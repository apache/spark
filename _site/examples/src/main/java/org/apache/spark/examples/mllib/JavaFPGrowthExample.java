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

import java.util.ArrayList;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;

/**
 * Java example for mining frequent itemsets using FP-growth.
 * Example usage:  ./bin/run-example mllib.JavaFPGrowthExample ./data/mllib/sample_fpgrowth.txt
 */
public class JavaFPGrowthExample {

  public static void main(String[] args) {
    String inputFile;
    double minSupport = 0.3;
    int numPartition = -1;
    if (args.length < 1) {
      System.err.println(
        "Usage: JavaFPGrowth <input_file> [minSupport] [numPartition]");
      System.exit(1);
    }
    inputFile = args[0];
    if (args.length >= 2) {
      minSupport = Double.parseDouble(args[1]);
    }
    if (args.length >= 3) {
      numPartition = Integer.parseInt(args[2]);
    }

    SparkConf sparkConf = new SparkConf().setAppName("JavaFPGrowthExample");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    JavaRDD<ArrayList<String>> transactions = sc.textFile(inputFile).map(
      new Function<String, ArrayList<String>>() {
        @Override
        public ArrayList<String> call(String s) {
          return Lists.newArrayList(s.split(" "));
        }
      }
    );

    FPGrowthModel<String> model = new FPGrowth()
      .setMinSupport(minSupport)
      .setNumPartitions(numPartition)
      .run(transactions);

    for (FPGrowth.FreqItemset<String> s: model.freqItemsets().toJavaRDD().collect()) {
      System.out.println("[" + Joiner.on(",").join(s.javaItems()) + "], " + s.freq());
    }

    sc.stop();
  }
}
