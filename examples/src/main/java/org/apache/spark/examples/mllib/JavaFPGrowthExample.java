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
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;

/**
 * Java example for mining frequent itemsets using FP-growth.
 */
public class JavaFPGrowthExample {

  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf().setAppName("JavaFPGrowthExample");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);


    // TODO: Read a user-specified input file.
    @SuppressWarnings("unchecked")
    JavaRDD<ArrayList<String>> transactions = sc.parallelize(Lists.newArrayList(
      Lists.newArrayList("r z h k p".split(" ")),
      Lists.newArrayList("z y x w v u t s".split(" ")),
      Lists.newArrayList("s x o n r".split(" ")),
      Lists.newArrayList("x z y m t s q e".split(" ")),
      Lists.newArrayList("z".split(" ")),
      Lists.newArrayList("x z y r q t p".split(" "))), 2);

    FPGrowth fpg = new FPGrowth()
      .setMinSupport(0.3);
    FPGrowthModel<String> model = fpg.run(transactions);

    for (FPGrowth.FreqItemset<String> s: model.freqItemsets().toJavaRDD().collect()) {
      System.out.println("[" + Joiner.on(",").join(s.javaItems()) + "], " + s.freq());
    }

    sc.stop();
  }
}
