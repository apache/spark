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

// scalastyle:off println
package org.apache.spark.examples.ml

// $example on$
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD

// $example off$
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object HypothesisTestingKolmogorovSmirnovTestExample {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("HypothesisTestingKolmogorovSmirnovTestExample").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // $example on$
    // @note: todo

    val data: RDD[Double] = ... // an RDD of sample data

    // run a KS test for the sample versus a standard normal distribution
    val testResult = Statistics.kolmogorovSmirnovTest(data, "norm", 0, 1)
    println(testResult) // summary of the test including the p-value, test statistic,
    // and null hypothesis
    // if our p-value indicates significance, we can reject the null hypothesis

    // perform a KS test using a cumulative distribution function of our making
    val myCDF: Double => Double = ...
    val testResult2 = Statistics.kolmogorovSmirnovTest(data, myCDF)

    // $example off$

    sc.stop()
  }
}
// scalastyle:on println

