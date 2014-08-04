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

package org.apache.spark.mllib.feature

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.apache.spark.{HashPartitioner, Logging}
import org.apache.spark.SparkContext._
import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel

/**
 *  Entry in vocabulary 
 */
private case class VocabWord(
  var word: String,
  var cn: Int,
  var point: Array[Int],
  var code: Array[Int],
  var codeLen:Int
)

/**
 * :: Experimental ::
 * Word2Vec creates vector representation of words in a text corpus.
 * The algorithm first constructs a vocabulary from the corpus
 * and then learns vector representation of words in the vocabulary. 
 * The vector representation can be used as features in 
 * natural language processing and machine learning algorithms.
 * 
 * We used skip-gram model in our implementation and hierarchical softmax 
 * method to train the model. The variable names in the implementation
 * matches the original C implementation.
 *
 * For original C implementation, see https://code.google.com/p/word2vec/ 
 * For research papers, see 
 * Efficient Estimation of Word Representations in Vector Space
 * and 
 * Distributed Representations of Words and Phrases and their Compositionality.
 * @param size vector dimension
 * @param startingAlpha initial learning rate
 * @param parallelism number of partitions to run Word2Vec (using a small number for accuracy)
 * @param numIterations number of iterations to run, should be smaller than or equal to parallelism
 */
@Experimental
class Word2Vec(
    val size: Int,
    val startingAlpha: Double,
    val parallelism: Int,
    val numIterations: Int) extends Serializable with Logging {

  /**
   * Word2Vec with a single thread.
   */
  def this(size: Int, startingAlpha: Int) = this(size, startingAlpha, 1, 1)

  private val EXP_TABLE_SIZE = 1000
  private val MAX_EXP = 6
  private val MAX_CODE_LENGTH = 40
  private val MAX_SENTENCE_LENGTH = 1000
  private val layer1Size = size 
  private val modelPartitionNum = 100

  /** context words from [-window, window] */
  private val window = 5

  /** minimum frequency to consider a vocabulary word */
  private val minCount = 5

  private var trainWordsCount = 0
  private var vocabSize = 0
  private var vocab: Array[VocabWord] = null
  private var vocabHash = mutable.HashMap.empty[String, Int]
  private var alpha = startingAlpha

  private def learnVocab(words:RDD[String]): Unit = {
    vocab = words.map(w => (w, 1))
      .reduceByKey(_ + _)
      .map(x => VocabWord(
        x._1, 
        x._2, 
        new Array[Int](MAX_CODE_LENGTH), 
        new Array[Int](MAX_CODE_LENGTH), 
        0))
      .filter(_.cn >= minCount)
      .collect()
      .sortWith((a, b) => a.cn > b.cn)
    
    vocabSize = vocab.length
    var a = 0
    while (a < vocabSize) {
      vocabHash += vocab(a).word -> a
      trainWordsCount += vocab(a).cn
      a += 1
    }
    logInfo("trainWordsCount = " + trainWordsCount)
  }

  private def createExpTable(): Array[Float] = {
    val expTable = new Array[Float](EXP_TABLE_SIZE)
    var i = 0
    while (i < EXP_TABLE_SIZE) {
      val tmp = math.exp((2.0 * i / EXP_TABLE_SIZE - 1.0) * MAX_EXP)
      expTable(i) = (tmp / (tmp + 1.0)).toFloat
      i += 1
    }
    expTable
  }

  private def createBinaryTree(): Unit = {
    val count = new Array[Long](vocabSize * 2 + 1)
    val binary = new Array[Int](vocabSize * 2 + 1)
    val parentNode = new Array[Int](vocabSize * 2 + 1)
    val code = new Array[Int](MAX_CODE_LENGTH)
    val point = new Array[Int](MAX_CODE_LENGTH)
    var a = 0
    while (a < vocabSize) {
      count(a) = vocab(a).cn
      a += 1
    }
    while (a < 2 * vocabSize) {
      count(a) = 1e9.toInt
      a += 1
    }
    var pos1 = vocabSize - 1
    var pos2 = vocabSize
    
    var min1i = 0 
    var min2i = 0

    a = 0
    while (a < vocabSize - 1) {
      if (pos1 >= 0) {
        if (count(pos1) < count(pos2)) {
          min1i = pos1
          pos1 -= 1
        } else {
          min1i = pos2
          pos2 += 1
        }
      } else {
        min1i = pos2
        pos2 += 1
      }
      if (pos1 >= 0) {
        if (count(pos1) < count(pos2)) {
          min2i = pos1
          pos1 -= 1
        } else {
          min2i = pos2
          pos2 += 1
        }
      } else {
        min2i = pos2
        pos2 += 1
      }
      count(vocabSize + a) = count(min1i) + count(min2i)
      parentNode(min1i) = vocabSize + a
      parentNode(min2i) = vocabSize + a
      binary(min2i) = 1
      a += 1
    }
    // Now assign binary code to each vocabulary word
    var i = 0
    a = 0
    while (a < vocabSize) {
      var b = a
      i = 0
      while (b != vocabSize * 2 - 2) {
        code(i) = binary(b)
        point(i) = b
        i += 1
        b = parentNode(b)
      }
      vocab(a).codeLen = i
      vocab(a).point(0) = vocabSize - 2
      b = 0
      while (b < i) {
        vocab(a).code(i - b - 1) = code(b)
        vocab(a).point(i - b) = point(b) - vocabSize
        b += 1
      }
      a += 1
    }
  }
  
  /**
   * Computes the vector representation of each word in vocabulary.
   * @param dataset an RDD of words
   * @return a Word2VecModel
   */
  def fit[S <: Iterable[String]](dataset: RDD[S]): Word2VecModel = {

    val words = dataset.flatMap(x => x)

    learnVocab(words)
    
    createBinaryTree()
    
    val sc = dataset.context

    val expTable = sc.broadcast(createExpTable())
    val bcVocab = sc.broadcast(vocab)
    val bcVocabHash = sc.broadcast(vocabHash)
    
    val sentences: RDD[Array[Int]] = words.mapPartitions { iter =>
      new Iterator[Array[Int]] {
        def hasNext: Boolean = iter.hasNext

        def next(): Array[Int] = {
          var sentence = new ArrayBuffer[Int]
          var sentenceLength = 0
          while (iter.hasNext && sentenceLength < MAX_SENTENCE_LENGTH) {
            val word = bcVocabHash.value.get(iter.next())
            word match {
              case Some(w) =>
                sentence += w
                sentenceLength += 1
              case None =>
            }
          }
          sentence.toArray
        }
      }
    }
    
    val newSentences = sentences.repartition(parallelism).cache()
    var syn0Global =
      Array.fill[Float](vocabSize * layer1Size)((Random.nextFloat() - 0.5f) / layer1Size)
    var syn1Global = new Array[Float](vocabSize * layer1Size)
    
    for(iter <- 1 to numIterations) {
      val (aggSyn0, aggSyn1, _, _) =
        // TODO: broadcast temp instead of serializing it directly
        // or initialize the model in each executor
        newSentences.treeAggregate((syn0Global, syn1Global, 0, 0))(
          seqOp = (c, v) => (c, v) match { 
          case ((syn0, syn1, lastWordCount, wordCount), sentence) =>
            var lwc = lastWordCount
            var wc = wordCount 
            if (wordCount - lastWordCount > 10000) {
              lwc = wordCount
              alpha = startingAlpha * (1 - parallelism * wordCount.toDouble / (trainWordsCount + 1))
              if (alpha < startingAlpha * 0.0001) alpha = startingAlpha * 0.0001
              logInfo("wordCount = " + wordCount + ", alpha = " + alpha)
            }
            wc += sentence.size
            var pos = 0
            while (pos < sentence.size) {
              val word = sentence(pos)
              // TODO: fix random seed
              val b = Random.nextInt(window)
              // Train Skip-gram
              var a = b
              while (a < window * 2 + 1 - b) {
                if (a != window) {
                  val c = pos - window + a
                  if (c >= 0 && c < sentence.size) {
                    val lastWord = sentence(c)
                    val l1 = lastWord * layer1Size
                    val neu1e = new Array[Float](layer1Size)
                    // Hierarchical softmax 
                    var d = 0
                    while (d < bcVocab.value(word).codeLen) {
                      val l2 = bcVocab.value(word).point(d) * layer1Size
                      // Propagate hidden -> output
                      var f = blas.sdot(layer1Size, syn0, l1, 1, syn1, l2, 1)
                      if (f > -MAX_EXP && f < MAX_EXP) {
                        val ind = ((f + MAX_EXP) * (EXP_TABLE_SIZE / MAX_EXP / 2.0)).toInt
                        f = expTable.value(ind)
                        val g = ((1 - bcVocab.value(word).code(d) - f) * alpha).toFloat
                        blas.saxpy(layer1Size, g, syn1, l2, 1, neu1e, 0, 1)
                        blas.saxpy(layer1Size, g, syn0, l1, 1, syn1, l2, 1)
                      }
                      d += 1
                    }
                    blas.saxpy(layer1Size, 1.0f, neu1e, 0, 1, syn0, l1, 1)
                  }
                }
                a += 1
              }
              pos += 1
            }
            (syn0, syn1, lwc, wc)
          },
          combOp = (c1, c2) => (c1, c2) match { 
            case ((syn0_1, syn1_1, lwc_1, wc_1), (syn0_2, syn1_2, lwc_2, wc_2)) =>
              val n = syn0_1.length
              val weight1 = 1.0f * wc_1 / (wc_1 + wc_2)
              val weight2 = 1.0f * wc_2 / (wc_1 + wc_2)
              blas.sscal(n, weight1, syn0_1, 1)
              blas.sscal(n, weight1, syn1_1, 1)
              blas.saxpy(n, weight2, syn0_2, 1, syn0_1, 1)
              blas.saxpy(n, weight2, syn1_2, 1, syn1_1, 1)
              (syn0_1, syn1_1, lwc_1 + lwc_2, wc_1 + wc_2)
          })
      syn0Global = aggSyn0
      syn1Global = aggSyn1
    }
    newSentences.unpersist()
    
    val wordMap = new Array[(String, Array[Float])](vocabSize)
    var i = 0
    while (i < vocabSize) {
      val word = bcVocab.value(i).word
      val vector = new Array[Float](layer1Size)
      Array.copy(syn0Global, i * layer1Size, vector, 0, layer1Size)
      wordMap(i) = (word, vector)
      i += 1
    }
    val modelRDD = sc.parallelize(wordMap, modelPartitionNum)
      .partitionBy(new HashPartitioner(modelPartitionNum))
      .persist(StorageLevel.MEMORY_AND_DISK)
    
    new Word2VecModel(modelRDD)
  }
}

/**
* Word2Vec model
*/
class Word2VecModel(private val model: RDD[(String, Array[Float])]) extends Serializable {

  private def cosineSimilarity(v1: Array[Float], v2: Array[Float]): Double = {
    require(v1.length == v2.length, "Vectors should have the same length")
    val n = v1.length
    val norm1 = blas.snrm2(n, v1, 1)
    val norm2 = blas.snrm2(n, v2, 1)
    if (norm1 == 0 || norm2 == 0) return 0.0
    blas.sdot(n, v1, 1, v2,1) / norm1 / norm2
  }
  
  /**
   * Transforms a word to its vector representation
   * @param word a word 
   * @return vector representation of word
   */
  def transform(word: String): Vector = {
    val result = model.lookup(word) 
    if (result.isEmpty) {
      throw new IllegalStateException(s"$word not in vocabulary")
    }
    else Vectors.dense(result(0).map(_.toDouble))
  }
  
  /**
   * Transforms an RDD to its vector representation
   * @param dataset a an RDD of words 
   * @return RDD of vector representation 
   */
  def transform(dataset: RDD[String]): RDD[Vector] = {
    dataset.map(word => transform(word))
  }
  
  /**
   * Find synonyms of a word
   * @param word a word
   * @param num number of synonyms to find  
   * @return array of (word, similarity)
   */
  def findSynonyms(word: String, num: Int): Array[(String, Double)] = {
    val vector = transform(word)
    findSynonyms(vector,num)
  }
  
  /**
   * Find synonyms of the vector representation of a word
   * @param vector vector representation of a word
   * @param num number of synonyms to find  
   * @return array of (word, cosineSimilarity)
   */
  def findSynonyms(vector: Vector, num: Int): Array[(String, Double)] = {
    require(num > 0, "Number of similar words should > 0")
    val topK = model.map { case(w, vec) => 
      (cosineSimilarity(vector.toArray.map(_.toFloat), vec), w) }
    .sortByKey(ascending = false)
    .take(num + 1)
    .map(_.swap)
    .tail
    
    topK
  }
}

object Word2Vec{
  /**
   * Train Word2Vec model
   * @param input RDD of words
   * @param size vector dimension
   * @param startingAlpha initial learning rate
   * @param parallelism number of partitions to run Word2Vec (using a small number for accuracy)
   * @param numIterations number of iterations, should be smaller than or equal to parallelism
   * @return Word2Vec model
   */
  def train[S <: Iterable[String]](
    input: RDD[S],
    size: Int,
    startingAlpha: Double,
    parallelism: Int = 1,
    numIterations:Int = 1): Word2VecModel = {
    new Word2Vec(size,startingAlpha, parallelism, numIterations).fit[S](input)
  }
}
