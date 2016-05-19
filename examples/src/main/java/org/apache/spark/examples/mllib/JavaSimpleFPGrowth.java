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

// $example on$
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
// $example off$
import org.apache.spark.api.java.function.Function;
// $example on$
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
// $example off$

import org.apache.spark.SparkConf;

public class JavaSimpleFPGrowth {

  public static void main(String[] args) {
    SparkConf conf = new SparkConf().setAppName("FP-growth Example");
    JavaSparkContext sc = new JavaSparkContext(conf);

    // $example on$
    JavaRDD<String> data = sc.textFile("data/mllib/sample_fpgrowth.txt");

    JavaRDD<List<String>> transactions = data.map(
      new Function<String, List<String>>() {
        public List<String> call(String line) {
          String[] parts = line.split(" ");
          return Arrays.asList(parts);
        }
      }
    );

    FPGrowth fpg = new FPGrowth()
      .setMinSupport(0.2)
      .setNumPartitions(10);
    FPGrowthModel<String> model = fpg.run(transactions);

    for (FPGrowth.FreqItemset<String> itemset: model.freqItemsets().toJavaRDD().collect()) {
      System.out.println("[" + itemset.javaItems() + "], " + itemset.freq());
    }

    double minConfidence = 0.8;
    for (AssociationRules.Rule<String> rule
      : model.generateAssociationRules(minConfidence).toJavaRDD().collect()) {
      System.out.println(
        rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + rule.confidence());
    }
    // $example off$

    sc.stop();
  }
}
