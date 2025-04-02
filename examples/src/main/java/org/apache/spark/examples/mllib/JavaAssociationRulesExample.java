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

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset;
// $example off$

import org.apache.spark.SparkConf;

public class JavaAssociationRulesExample {

  public static void main(String[] args) {

    SparkConf sparkConf = new SparkConf().setAppName("JavaAssociationRulesExample");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    // $example on$
    JavaRDD<FPGrowth.FreqItemset<String>> freqItemsets = sc.parallelize(Arrays.asList(
      new FreqItemset<>(new String[] {"a"}, 15L),
      new FreqItemset<>(new String[] {"b"}, 35L),
      new FreqItemset<>(new String[] {"a", "b"}, 12L)
    ));

    AssociationRules arules = new AssociationRules()
      .setMinConfidence(0.8);
    JavaRDD<AssociationRules.Rule<String>> results = arules.run(freqItemsets);

    for (AssociationRules.Rule<String> rule : results.collect()) {
      System.out.println(
        rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + rule.confidence());
    }
    // $example off$

    sc.stop();
  }
}
