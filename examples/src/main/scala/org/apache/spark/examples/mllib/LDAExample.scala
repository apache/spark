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

import scala.collection.mutable.ArrayBuffer

import java.text.BreakIterator

import scopt.OptionParser

import org.apache.log4j.{Level, Logger}

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.clustering.LDA.Document
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.rdd.RDD


/**
 * An example Latent Dirichlet Allocation (LDA) app. Run with
 * {{{
 * ./bin/run-example mllib.DenseKMeans [options] <input>
 * }}}
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 */
object LDAExample {

  private case class Params(
      input: Seq[String] = Seq.empty,
      k: Int = 20,
      maxIterations: Int = 10,
      topicSmoothing: Double = 0.1,
      termSmoothing: Double = 0.1,
      vocabSize: Int = 10000,
      stopwordFile: String = "") extends AbstractParams[Params]

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("LDAExample") {
      head("LDAExample: an example LDA app for plain text data.")
      opt[Int]("k")
        .text(s"number of topics. default: ${defaultParams.k}")
        .action((x, c) => c.copy(k = x))
      opt[Int]("maxIterations")
        .text(s"number of iterations of learning. default: ${defaultParams.maxIterations}")
        .action((x, c) => c.copy(maxIterations = x))
      opt[Double]("topicSmoothing")
        .text(s"amount of topic smoothing to use.  default: ${defaultParams.topicSmoothing}")
        .action((x, c) => c.copy(topicSmoothing = x))
      opt[Double]("termSmoothing")
        .text(s"amount of word smoothing to use.  default: ${defaultParams.termSmoothing}")
        .action((x, c) => c.copy(termSmoothing = x))
      opt[Int]("vocabSize")
        .text(s"number of distinct word types to use, chosen by frequency." +
          s"  default: ${defaultParams.vocabSize}")
        .action((x, c) => c.copy(vocabSize = x))
      opt[String]("stopwordFile")
        .text(s"filepath for a list of stopwords. Note: This must fit on a single machine." +
        s"  default: ${defaultParams.stopwordFile}")
        .action((x, c) => c.copy(stopwordFile = x))
      arg[String]("<input>...")
        .text("input paths (directories) to plain text corpora")
        .unbounded()
        .required()
        .action((x, c) => c.copy(input = c.input :+ x))
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      parser.showUsageAsError
      sys.exit(1)
    }
  }

  private def run(params: Params) {
    val conf = new SparkConf().setAppName(s"LDAExample with $params")
    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)

    // Load documents, and prepare them for LDA.
    val (corpus, vocabArray) = preprocess(sc, params.input, params.vocabSize, params.stopwordFile)
    corpus.cache() // cache since LDA is iterative

    // Run LDA.
    val lda = new LDA()
    lda.setK(params.k)
      .setMaxIterations(params.maxIterations)
      .setTopicSmoothing(params.topicSmoothing)
      .setTermSmoothing(params.termSmoothing)
    val ldaModel = lda.run(corpus)

    // Print the topics, showing the top-weighted terms for each topic.
    val topicIndices = ldaModel.getTopics(maxTermsPerTopic = 10)
    val topics = topicIndices.map { topic =>
      topic.map { case (weight, term) => (weight, vocabArray(term)) }
    }
    println(s"${params.k} topics:")
    topics.zipWithIndex.foreach { case (topic, i) =>
      println(s"TOPIC $i")
      topic.foreach { case (weight, term) =>
        println(s"$term\t$weight")
      }
    }

    // TODO: print log likelihood

  }

  /**
   * Load documents, tokenize them, create vocabulary, and prepare documents as word count vectors.
   */
  private def preprocess(
      sc: SparkContext,
      paths: Seq[String],
      vocabSize: Int,
      stopwordFile: String): (RDD[Document], Array[String]) = {

    val files: Seq[RDD[(String, String)]] = for (p <- paths) yield {
      sc.wholeTextFiles(p)
    }

    // Dataset of document texts
    val textRDD: RDD[String] =
      files.reduce(_ ++ _) // combine results from multiple paths
      .map { case (path, text) => text }

    // Split text into words
    val tokenizer = new SimpleTokenizer(sc, stopwordFile)
    val tokenized: RDD[(Long, IndexedSeq[String])] = textRDD.zipWithIndex().map { case (text, id) =>
      id -> tokenizer.getWords(text)
    }

    // Counts words: RDD[(word, wordCount)]
    val wordCounts: RDD[(String, Int)] = tokenized
      .flatMap { case (_, tokens) => tokens.map(_ -> 1) }
      .reduceByKey(_ + _)

    // Choose vocabulary: Map[word -> id]
    val vocab: Map[String, Int] = wordCounts
      .sortBy(_._2, ascending = false)
      .take(vocabSize)
      .map(_._1)
      .zipWithIndex
      .toMap

    val documents = tokenized.map { case (id, tokens) =>
      // Filter tokens by vocabulary, and create word count vector representation of document.
      val wc = new scala.collection.mutable.HashMap[Int, Int]()
      tokens.foreach { term =>
        if (vocab.contains(term)) {
          val termIndex = vocab(term)
          wc(termIndex) = wc.getOrElse(termIndex, 0) + 1
        }
      }
      val indices = wc.keys.toArray.sorted
      val values = indices.map(i => wc(i).toDouble)

      val sb = new SparseVector(vocab.size, indices, values)
      LDA.Document(sb, id)
    }

    val vocabArray = new Array[String](vocab.size)
    vocab.foreach { case (term, i) => vocabArray(i) = term }

    (documents, vocabArray)
  }
}

/**
 * Simple Tokenizer.
 *
 * TODO: Formalize the interface, and make this a public class in mllib.feature
 */
private class SimpleTokenizer(sc: SparkContext, stopwordFile: String) extends Serializable {

  private val stopwords: Set[String] = if (stopwordFile.isEmpty) {
    Set.empty[String]
  } else {
    val stopwordText = sc.textFile(stopwordFile).collect()
    stopwordText.flatMap(_.stripMargin.split("\\s+")).toSet
  }

  // Matches sequences of Unicode letters
  private val allWordRegex = "^(\\p{L}*)$".r

  // Ignore words shorter than this length.
  private val minWordLength = 3

  def getWords(text: String): IndexedSeq[String] = {

    val words = new ArrayBuffer[String]()

    // Use Java BreakIterator to tokenize text into words.
    val wb = BreakIterator.getWordInstance
    wb.setText(text)

    // current,end index start,end of each word
    var current = wb.first()
    var end = wb.next()
    while (end != BreakIterator.DONE) {
      // Convert to lowercase
      val word: String = text.substring(current, end).toLowerCase
      // Remove short words and strings that aren't only letters
      word match {
        case allWordRegex(w) if w.length >= minWordLength && !stopwords.contains(w) =>
          words += word
        case _ =>
      }

      current = end
      end = wb.next()
    }
    words
  }

}
