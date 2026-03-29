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

package org.apache.spark.examples.ml

// scalastyle:off println
// $example on$
import org.apache.spark.ml.clustering.LDA
// $example off$
import org.apache.spark.sql.SparkSession

/**
 * An example demonstrating LDA.
 * Run with
 * {{{
 * bin/run-example ml.LDAExample
 * }}}
 */
object LDAExample {
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession
    val spark = SparkSession
      .builder()
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()

    // $example on$
    // Loads data.
    val dataset = spark.read.format("libsvm")
      .load("data/mllib/sample_lda_libsvm_data.txt")

    // Trains a LDA model.
    val lda = new LDA().setK(10).setMaxIter(10)
    val model = lda.fit(dataset)

    val ll = model.logLikelihood(dataset)
    val lp = model.logPerplexity(dataset)
    println(s"The lower bound on the log likelihood of the entire corpus: $ll")
    println(s"The upper bound on perplexity: $lp")

    // Describe topics.
    val topics = model.describeTopics(3)
    println("The topics described by their top-weighted terms:")
    topics.show(false)

    // Shows the result.
    val transformed = model.transform(dataset)
    transformed.show(false)
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println
