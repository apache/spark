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
import java.io.File

import org.apache.spark.ml.clustering.{LDA, LDAModel, LocalLDAModel}
import org.apache.spark.sql.DataFrame
// $example off$
import org.apache.spark.sql.SparkSession

/**
 * An example demonstrating incremental update of LDA, with setInitialModel parameter.
 * Run with
 * {{{
 * bin/run-example ml.OnlineLDAIncrementalExample
 * }}}
 */
object OnlineLDAIncrementalExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
        .builder()
        .appName(s"${this.getClass.getSimpleName}")
        .getOrCreate()

    import spark.implicits._

    // $example on$
    // Loads data.
    val dataset: DataFrame = spark.read.format("libsvm")
        .load("data/mllib/sample_lda_libsvm_data.txt")

    // ---------------------------------------
    // Build a LDA incrementally
    // - here we're simulating data coming incrementally, in chunks
    // - this assumes vocabulary is fixed (same words as columns of the matrix)

    val nbChunks = 3
    val chunks = dataset.randomSplit(Array.fill(nbChunks)(1D / nbChunks), 7L)

    // LDA model params
    val k = 10
    val iter = 30

    // To pass a trained LDA from one iteration to the other, we persist it to a file
    val modelPath = File.createTempFile("./incrModel", null).getPath
    var previousModelPath: String = null

    var idx = 0

    for (chunk <- chunks) {
      idx += 1
      println(s"Incremental, chunk=$idx, k=$k, maxIterations=$iter")

      // Build LDA model as usual
      val lda = new LDA()
          .setK(k)
          .setMaxIter(iter)
          .setOptimizer("online")

      // and point to the previous model, when there's one
      if (previousModelPath != null) {
        lda.setInitialModel(previousModelPath)
      }

      val model = lda.fit(dataset)

      // Check perplexity at each iteration
      val lp = model.logPerplexity(dataset)
      println(s"Log Perplexity=$lp")
      println("---------------------------------")

      // persist for next chunk
      previousModelPath = s"$modelPath-$idx"
      model.save(previousModelPath)
    }

    val finalModel = LocalLDAModel.load(previousModelPath)

    // Describe topics.
    val topics = finalModel.describeTopics(3)
    println("The topics described by their top-weighted terms:")
    topics.show(false)

    // Shows the result.
    val transformed = finalModel.transform(dataset)
    transformed.show(false)
    // $example off$

    spark.stop()
  }
}

// scalastyle:on println
