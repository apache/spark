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

import scala.collection.mutable

import java.text.BreakIterator

import scopt.OptionParser

import org.apache.log4j.{Level, Logger}

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD


/**
 * An app for timing Latent Dirichlet Allocation (LDA).
 *
 * This takes vectors of parameters:
 *  - corpusSize
 *  - vocabSize
 *  - k
 * For each combination of values, it runs LDA and prints the time for iterations,
 * the topics estimated, etc.
 *
 * Run with
 * {{{
 * ./bin/run-example mllib.LDATiming [options] <input>
 * }}}
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 */
object LDATiming {

  private case class Params(
      input: Seq[String] = Seq.empty,
      corpusSizes: Array[Long] = Array(-1),
      ks: Array[Int] = Array(20),
      numPartitions: Int = 16,
      maxIterations: Int = 10,
      docConcentration: Double = -1,
      topicConcentration: Double = -1,
      vocabSizes: Array[Int] = Array(10000),
      stopwordFile: String = "",
      checkpointDir: Option[String] = None,
      checkpointInterval: Int = 10) extends AbstractParams[Params]

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("LDATiming") {
      head("LDATiming: an example LDA timing app for plain text data.")
      opt[String]("corpusSizes")
        .text(s"numbers of documents to test. default: ${defaultParams.corpusSizes}")
        .action((x, c) => c.copy(corpusSizes = x.split("\\s").map(_.toLong)))
      opt[String]("ks")
        .text(s"numbers of topics to test. default: ${defaultParams.ks}")
        .action((x, c) => c.copy(ks = x.split("\\s").map(_.toInt)))
      opt[Int]("maxIterations")
        .text(s"number of iterations of learning. default: ${defaultParams.maxIterations}")
        .action((x, c) => c.copy(maxIterations = x))
      opt[Int]("numPartitions")
        .text(s"number of partitions. default: ${defaultParams.numPartitions}")
        .action((x, c) => c.copy(numPartitions = x))
      opt[Double]("docConcentration")
        .text(s"amount of topic smoothing to use.  default: ${defaultParams.docConcentration}")
        .action((x, c) => c.copy(docConcentration = x))
      opt[Double]("topicConcentration")
        .text(s"amount of word smoothing to use.  default: ${defaultParams.topicConcentration}")
        .action((x, c) => c.copy(topicConcentration = x))
      opt[String]("vocabSizes")
        .text(s"numbers of distinct word types to use, chosen by frequency." +
        s"  default: ${defaultParams.vocabSizes}")
        .action((x, c) => c.copy(vocabSizes = x.split("\\s").map(_.toInt)))
      opt[String]("stopwordFile")
        .text(s"filepath for a list of stopwords. Note: This must fit on a single machine." +
        s"  default: ${defaultParams.stopwordFile}")
        .action((x, c) => c.copy(stopwordFile = x))
      opt[String]("checkpointDir")
        .text(s"Directory for checkpointing intermediate results." +
        s"  Checkpointing helps with recovery and eliminates temporary shuffle files on disk." +
        s"  default: ${defaultParams.checkpointDir}")
        .action((x, c) => c.copy(checkpointDir = Some(x)))
      opt[Int]("checkpointInterval")
        .text(s"Iterations between each checkpoint.  Only used if checkpointDir is set." +
        s" default: ${defaultParams.checkpointInterval}")
        .action((x, c) => c.copy(checkpointInterval = x))
      arg[String]("<input>...")
        .text("input paths (directories) to plain text corpora."
        + "  Each text file line should hold 1 document.")
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
    val conf = new SparkConf().setAppName(s"LDATiming with $params")
    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)

    // Load documents, and prepare them for LDA.
    for (corpusSize <- params.corpusSizes) {
      for (vocabSize <- params.vocabSizes) {

        val preprocessStart = System.nanoTime()
        val (corpus, vocabArray, actualNumTokens) =
          preprocess(sc, params.input, corpusSize, vocabSize, params.stopwordFile)
        corpus.repartition(params.numPartitions).cache()
        val actualCorpusSize = corpus.count()
        val actualVocabSize = vocabArray.size
        val preprocessElapsed = (System.nanoTime() - preprocessStart) / 1e9

        println()
        println(s"DATASET with corpusSize=$corpusSize, vocabSize=$vocabSize:")
        println(s"\t Training set size: $actualCorpusSize documents")
        println(s"\t Vocabulary size: $actualVocabSize terms")
        println(s"\t Training set size: $actualNumTokens tokens")
        println(s"\t Preprocessing time: $preprocessElapsed sec")
        println()

        for (k <- params.ks) {
          // Run LDA.
          val lda = new LDA()
          lda.setK(k)
            .setMaxIterations(params.maxIterations)
            .setDocConcentration(params.docConcentration)
            .setTopicConcentration(params.topicConcentration)
            .setCheckpointInterval(params.checkpointInterval)
          if (params.checkpointDir.nonEmpty) {
            lda.setCheckpointDir(params.checkpointDir.get)
          }
          val startTime = System.nanoTime()
          val ldaModel = lda.run(corpus)
          val elapsed = (System.nanoTime() - startTime) / 1e9

          println(s"Finished training LDA model.  Summary:")
          println(s"\t Training time: $elapsed sec")
          val avgLogLikelihood = ldaModel.logLikelihood / actualCorpusSize.toDouble
          println(s"\t Training data average log likelihood: $avgLogLikelihood")
          println(s"\t Training times per iteration (sec):\n" +
            s"${ldaModel.iterationTimes.mkString("\t", "\n\t", "\n")}")
          println()

          // Print the topics, showing the top-weighted terms for each topic.
          val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
          val topics = topicIndices.map { case (terms, termWeights) =>
            terms.zip(termWeights).map { case (term, weight) => (vocabArray(term.toInt), weight) }
          }
          println(s"$k topics:")
          topics.zipWithIndex.foreach { case (topic, i) =>
            println(s"TOPIC $i")
            topic.foreach { case (term, weight) =>
              println(s"$term\t$weight")
            }
            println()
          }
          println()
          println("--------------------------------------------------------------")
          println()
          println(s"RESULTS: $corpusSize $vocabSize $actualCorpusSize $actualVocabSize" +
            s" $actualNumTokens $k $elapsed $avgLogLikelihood" +
            s" ${ldaModel.iterationTimes.mkString(" ")}")
          println()
          println("==============================================================")
          println()
        }
      }
    }

    sc.stop()
  }

  /**
   * Load documents, tokenize them, create vocabulary, and prepare documents as word count vectors.
   * @return (corpus, vocabulary as array, total token count in corpus)
   */
  private def preprocess(
      sc: SparkContext,
      paths: Seq[String],
      corpusSize: Long,
      vocabSize: Int,
      stopwordFile: String): (RDD[(Long, Vector)], Array[String], Long) = {

    // Get dataset of document texts
    // One document per line in each text file.
    val files: Seq[RDD[String]] = for (p <- paths) yield {
      sc.textFile(p)
    }
    val textRDD_tmp: RDD[String] = files.reduce(_ ++ _) // combine results from multiple paths
    val origSize = textRDD_tmp.count()

    // Subsample data.
    val textRDD: RDD[String] = if (corpusSize == -1 || corpusSize >= origSize) {
      textRDD_tmp
    } else {
      textRDD_tmp.sample(withReplacement = true, fraction = corpusSize.toDouble / origSize,
        seed = 123456)
    }

    // Split text into words
    val tokenizer = new Tokenizer(sc, stopwordFile)
    val tokenized: RDD[(Long, IndexedSeq[String])] = textRDD.zipWithIndex().map { case (text, id) =>
      id -> tokenizer.getWords(text)
    }
    tokenized.cache()

    // Counts words: RDD[(word, wordCount)]
    val wordCounts: RDD[(String, Long)] = tokenized
      .flatMap { case (_, tokens) => tokens.map(_ -> 1L) }
      .reduceByKey(_ + _)
    // Sort words, and select vocab
    val sortedWC = wordCounts.sortBy(_._2, ascending = false)
    val selectedWC: Array[(String, Long)] = if (vocabSize == -1) {
      sortedWC.collect()
    } else {
      sortedWC.take(vocabSize)
    }
    val totalTokenCount = selectedWC.map(_._2).sum
    // vocabulary: Map[word -> id]
    val vocab: Map[String, Int] =
      selectedWC.map(_._1)
        .zipWithIndex
        .toMap

    val documents = tokenized.map { case (id, tokens) =>
      // Filter tokens by vocabulary, and create word count vector representation of document.
      val wc = new mutable.HashMap[Int, Int]()
      tokens.foreach { term =>
        if (vocab.contains(term)) {
          val termIndex = vocab(term)
          wc(termIndex) = wc.getOrElse(termIndex, 0) + 1
        }
      }
      val indices = wc.keys.toArray.sorted
      val values = indices.map(i => wc(i).toDouble)

      val sb = Vectors.sparse(vocab.size, indices, values)
      (id, sb)
    }

    val vocabArray = new Array[String](vocab.size)
    vocab.foreach { case (term, i) => vocabArray(i) = term }

    (documents, vocabArray, totalTokenCount)
  }
}

class Tokenizer(sc: SparkContext, stopwordFile: String) extends Serializable {

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
    val words = new mutable.ArrayBuffer[String]()

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
          words += w
        case _ =>
      }

      current = end
      try {
        end = wb.next()
      } catch {
        case e: Exception =>
          // Ignore remaining text in line.
          // This is a known bug in BreakIterator (for some Java versions),
          // which fails when it sees certain characters.
          end = BreakIterator.DONE
      }
    }
    words
  }

}
