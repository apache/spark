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
import java.util.Arrays;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.mllib.stat.test.KolmogorovSmirnovTestResult;
// $example off$

public class JavaHypothesisTestingKolmogorovSmirnovTestExample {
  public static void main(String[] args) {

    SparkConf conf =
      new SparkConf().setAppName("JavaHypothesisTestingKolmogorovSmirnovTestExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);

    // $example on$
    JavaDoubleRDD data = jsc.parallelizeDoubles(Arrays.asList(0.1, 0.15, 0.2, 0.3, 0.25));
    KolmogorovSmirnovTestResult testResult =
      Statistics.kolmogorovSmirnovTest(data, "norm", 0.0, 1.0);
    // summary of the test including the p-value, test statistic, and null hypothesis
    // if our p-value indicates significance, we can reject the null hypothesis
    System.out.println(testResult);
    // $example off$

    jsc.stop();
  }
}

