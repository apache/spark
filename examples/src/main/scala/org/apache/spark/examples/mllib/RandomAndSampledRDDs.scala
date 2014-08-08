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

package org.apache.spark.examples.mllib

import org.apache.spark.mllib.random.RandomRDDGenerators
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import scopt.OptionParser

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

/**
 * An example app for randomly generated and sampled RDDs. Run with
 * {{{
 * bin/run-example org.apache.spark.examples.mllib.RandomAndSampledRDDs
 * }}}
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 */
object RandomAndSampledRDDs extends App {

  case class Params(input: String = "data/mllib/sample_binary_classification_data.txt")

  val defaultParams = Params()

  val parser = new OptionParser[Params]("RandomAndSampledRDDs") {
    head("RandomAndSampledRDDs: an example app for randomly generated and sampled RDDs.")
    opt[String]("input")
      .text(s"Input path to labeled examples in LIBSVM format, default: ${defaultParams.input}")
      .action((x, c) => c.copy(input = x))
    note(
      """
        |For example, the following command runs this app:
        |
        | bin/spark-submit --class org.apache.spark.examples.mllib.RandomAndSampledRDDs \
        |  examples/target/scala-*/spark-examples-*.jar
      """.stripMargin)
  }

  parser.parse(args, defaultParams).map { params =>
    run(params)
  } getOrElse {
    sys.exit(1)
  }

  def run(params: Params) {
    val conf = new SparkConf().setAppName(s"RandomAndSampledRDDs with $params")
    val sc = new SparkContext(conf)

    val numExamples = 10000 // number of examples to generate
    val fraction = 0.1 // fraction of data to sample

    // Example: RandomRDDGenerators
    val normalRDD: RDD[Double] = RandomRDDGenerators.normalRDD(sc, numExamples)
    println(s"Generated RDD of ${normalRDD.count()} examples sampled from a unit normal distribution")
    val normalVectorRDD =
      RandomRDDGenerators.normalVectorRDD(sc, numRows = numExamples, numCols = 2)
    println(s"Generated RDD of ${normalVectorRDD.count()} examples of length-2 vectors.")

    println()

    // Example: RDD.sample() and RDD.takeSample()
    val exactSampleSize = (numExamples * fraction).toInt
    println(s"Sampling RDD using fraction $fraction.  Expected sample size = $exactSampleSize.")
    val sampledRDD = normalRDD.sample(withReplacement = true, fraction = fraction)
    println(s"  RDD.sample(): sample has ${sampledRDD.count()} examples")
    val sampledArray = normalRDD.takeSample(withReplacement = true, num = exactSampleSize)
    println(s"  RDD.takeSample(): sample has ${sampledArray.size} examples")

    println()

    // Example: RDD.sampleByKey()
    val examples = MLUtils.loadLibSVMFile(sc, params.input)
    val sizeA = examples.count()
    println(s"Loaded data with $sizeA examples from file: ${params.input}")
    val keyedRDD = examples.map { lp => (lp.label.toInt, lp.features) }
    println(s"  Keyed data using label (Int) as key ==> Orig")
    //  Count examples per label in original data.
    val keyCountsA = keyedRDD.countByKey()
    //  Subsample, and count examples per label in sampled data.
    val fractions = keyCountsA.keys.map((_, fraction)).toMap
    val sampledByKeyRDD =
      keyedRDD.sampleByKey(withReplacement = true, fractions = fractions, exact = true)
    val keyCountsB = sampledByKeyRDD.countByKey()
    val sizeB = keyCountsB.values.sum
    println(s"  Sampled $sizeB examples using exact stratified sampling (by label). ==> Sample")
    println(s"   \tFractions of examples with key")
    println(s"Key\tOrig\tSample")
    keyCountsA.keys.toSeq.sorted.foreach { key =>
      println(s"$key\t${keyCountsA(key) / sizeA.toDouble}\t${keyCountsB(key) / sizeB.toDouble}")
    }

    sc.stop()
  }
}
