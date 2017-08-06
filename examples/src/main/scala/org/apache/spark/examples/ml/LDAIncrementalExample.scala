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

import scala.collection.mutable

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.clustering.{LDA, LDAModel}
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, RegexTokenizer, StopWordsRemover}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{length, udf}
// $example off$
import org.apache.spark.sql.SparkSession

/**
 * An example demonstrating incremental update of LDA, with setInitialModel parameter.
 * Run with
 * {{{
 * bin/run-example ml.LDAIncrementalExample
 * }}}
 */
object LDAIncrementalExample {

  val modelPath = File.createTempFile("./incrModel", null).getPath

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
        .builder()
        .master("local[*]")
        .appName(s"${this.getClass.getSimpleName}")
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val dataset = spark.read.text("/home/mde/workspaces/spark-project/spark/docs/*md").toDF("doc")
        .where(length($"doc") > 0)

    println(s"Nb documents = ${dataset.count()}")
    dataset.show()

    // Let's prepare a test set for LDA perplexity eval throughout this example
    val splits = dataset.randomSplit(Array(0.8, 0.2), 15L)
    val (train, test) = (splits(0), splits(1))

    // Build a LDA in one-shot
    val vocabSize = 500
    val k = 10
    val iter = 30
    println(s"One-Shot build, vocabSize=$vocabSize, k=$k, maxIterations=$iter")

    // Prepare dataset : tokenize and build a vocabulary
    val (dataprep, vocab) = buildDataPrepPipeline(train, vocabSize)

    // Build a LDA
    val vectorized = dataprep.transform(train)
    val ldaModel = buildModel(vectorized, k, iter)

    showTopics(spark, vocab, ldaModel)

    // evaluate
    val testVect = dataprep.transform(test)
    val perplexity = ldaModel.logPerplexity(ldaModel.transform(testVect))
    println(s"Perplexity=$perplexity")
    println("---------------------------------")
    println("")

    // ---------------------------------------
    // Build a LDA incrementally
    // - we assume the same tokenisation, and that vocabulary is stable
    //     (we reuse the one previously built)
    // - let's say the data will come incrementally, in 10 chunks

    val nbChunks = 10
    val chunks = train.randomSplit(Array.fill(nbChunks)(1D / nbChunks), 7L)

    var previousModelPath: String = null

    var idx = 0
    for (chunk <- chunks) {
      idx += 1
      println(s"Incremental, chunk=$idx, vocabSize=$vocabSize, k=$k, maxIterations=$iter")

      val chunkVect = dataprep.transform(chunk)
      val model = buildModel(chunkVect, k, iter, previousModelPath)

      showTopics(spark, vocab, model)

      val perplexity = model.logPerplexity(testVect)
      println(s"Perplexity=$perplexity")
      println("---------------------------------")

      previousModelPath = s"$modelPath-$idx"
      model.save(previousModelPath)
    }

    spark.stop()
  }

  def buildDataPrepPipeline(dataset: DataFrame, vocabSize: Int): (PipelineModel, Array[String]) = {
    val countTokens = udf { (words: Seq[String]) => words.length }

    val stop = StopWordsRemover.loadDefaultStopWords("english") ++
        Array("tr", "td", "div", "class", "table", "html", "div")


    val tokenizer = new RegexTokenizer().setInputCol("doc").setOutputCol("rawwords")
        .setGaps(false).setPattern("[a-zA-Z]{3,}")
    val stopremover = new StopWordsRemover().setInputCol("rawwords")
        .setOutputCol("words").setStopWords(stop)
    val vectorizer = new CountVectorizer().setInputCol("words").setOutputCol("features")
        .setVocabSize(vocabSize)
        .setMinDF(0.01)

    val stages = Array(
      tokenizer, stopremover, vectorizer)

    val pipeline = new Pipeline().setStages(stages)

    val model = pipeline.fit(dataset)

    (model, model.stages(2).asInstanceOf[CountVectorizerModel].vocabulary)
  }

  def buildModel(dataset: DataFrame, k: Int, maxIter: Int,
      previousModelPath: String = null): LDAModel = {

    val lda = new LDA()
        .setK(k)
        .setFeaturesCol("features")
        .setMaxIter(maxIter)
        .setOptimizer("online")

    if (previousModelPath != null) {
      lda.setInitialModel(previousModelPath)
    }

    lda.fit(dataset)
  }

  def showTopics(spark: SparkSession, vocab: Array[String], ldaModel: LDAModel): Unit = {
    import spark.implicits._
    val bc = spark.sparkContext.broadcast(vocab)
    val topicWords = udf { (indices: mutable.WrappedArray[_]) =>
      indices.map {
        case v: Int => bc.value(v)
      }
    }
    ldaModel.describeTopics().select(topicWords($"termIndices").as("topics")).show(false)
  }


}

// scalastyle:on println
