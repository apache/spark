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
import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, RegexTokenizer,
StopWordsRemover}
import org.apache.spark.ml.Pipeline
// $example off$

/**
 * An example demonstrating a LDA of ML pipeline.
 * Run with
 * {{{
 * bin/run-example ml.LDAExample <file> <k>
 * }}}
 */
object LDAExample {

  final val FEATURES_COL = "features"
  // $example on$
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: ml.LDAExample <file> <k>")
      System.exit(1)
    }
    val input = args(0)
    val k = args(1).toInt

    // Creates a Spark context and a SQL context
    val conf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
    val sc = new SparkContext(conf)

    // Loads data
    val (dataset, vocab) = preprocess(sc, input)

    // Trains a LDA model
    val lda = new LDA()
      .setK(k)
      .setMaxIter(10)
      .setFeaturesCol(FEATURES_COL)
    val model = lda.fit(dataset)
    val transformed = model.transform(dataset)

    val ll = model.logLikelihood(dataset)
    val lp = model.logPerplexity(dataset)

    // describeTopics
    println(vocab.mkString(", "))
    val topics = model.describeTopics(3)

    // Shows the result
    topics.show(false)
    transformed.show(false)
    sc.stop()
  }

  /**
   * Load documents, tokenize them, create vocabulary, and prepare documents as term count vectors.
   * @return (corpus, vocabulary as array)
   */
  private def preprocess(sc: SparkContext,
                         path: String): (DataFrame, Array[String]) = {

    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val df = sc.textFile(path).toDF("docs")
    val tokenizer = new RegexTokenizer()
      .setInputCol("docs")
      .setOutputCol("rawTokens")
    val stopWordsRemover = new StopWordsRemover()
      .setInputCol("rawTokens")
      .setOutputCol("tokens")
    val countVectorizer = new CountVectorizer()
      .setInputCol("tokens")
      .setOutputCol("features")
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, stopWordsRemover, countVectorizer))

    val model = pipeline.fit(df)

    (model.transform(df), model.stages(2).asInstanceOf[CountVectorizerModel].vocabulary)
  }
  // $example off$
}
// scalastyle:on println
