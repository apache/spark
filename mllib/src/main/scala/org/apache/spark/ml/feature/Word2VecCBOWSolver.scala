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

package org.apache.spark.ml.feature

import com.github.fommil.netlib.BLAS.{getInstance => blas}

import org.apache.spark.internal.Logging
import org.apache.spark.mllib.feature
import org.apache.spark.rdd.RDD
import org.apache.spark.util.random.XORShiftRandom

object Word2VecCBOWSolver extends Logging {
  // learning rate is updated for every batch of size batchSize
  private val batchSize = 10000

  // power to raise the unigram distribution with
  private val power = 0.75

  case class Vocabulary(
    totalWordCount: Long,
    vocabMap: Map[String, Int],
    unigramTable: Array[Int],
    samplingTable: Array[Float])

  /**
   * This method implements Word2Vec Continuous Bag Of Words based implementation using
   * negative sampling optimization, using BLAS for vectorizing operations where applicable.
   * The algorithm is parallelized in the same way as the skip-gram based estimation.
   * We divide input data into N equally sized random partitions.
   * We then generate initial weights and broadcast them to the N partitions. This way
   * all the partitions start with the same initial weights. We then run N independent
   * estimations that each estimate a model on a partition. The weights learned
   * from each of the N models are averaged and rebroadcast the weights.
   * This process is repeated `maxIter` number of times.
   *
   * @param input A RDD of strings. Each string would be considered a sentence.
   * @return Estimated word2vec model
   */
  def fitCBOW[S <: Iterable[String]](
      word2Vec: Word2Vec,
      input: RDD[S]): feature.Word2VecModel = {

    val negativeSamples = word2Vec.getNegativeSamples
    val sample = word2Vec.getSample

    val Vocabulary(totalWordCount, vocabMap, uniTable, sampleTable) =
      generateVocab(input, word2Vec.getMinCount, sample, word2Vec.getUnigramTableSize)
    val vocabSize = vocabMap.size

    assert(negativeSamples < vocabSize, s"Vocab size ($vocabSize) cannot be smaller" +
      s" than negative samples($negativeSamples)")

    val seed = word2Vec.getSeed
    val initRandom = new XORShiftRandom(seed)

    val vectorSize = word2Vec.getVectorSize
    val syn0Global = Array.fill(vocabSize * vectorSize)(initRandom.nextFloat - 0.5f)
    val syn1Global = Array.fill(vocabSize * vectorSize)(0.0f)

    val sc = input.context

    val vocabMapBroadcast = sc.broadcast(vocabMap)
    val unigramTableBroadcast = sc.broadcast(uniTable)
    val sampleTableBroadcast = sc.broadcast(sampleTable)

    val windowSize = word2Vec.getWindowSize
    val maxSentenceLength = word2Vec.getMaxSentenceLength
    val numPartitions = word2Vec.getNumPartitions

    val digitSentences = input.flatMap { sentence =>
      val wordIndexes = sentence.flatMap(vocabMapBroadcast.value.get)
      wordIndexes.grouped(maxSentenceLength).map(_.toArray)
    }.repartition(numPartitions).cache()

    val learningRate = word2Vec.getStepSize

    val wordsPerPartition = totalWordCount / numPartitions

    logInfo(s"VocabSize: ${vocabMap.size}, TotalWordCount: $totalWordCount")

    val maxIter = word2Vec.getMaxIter
    for {iteration <- 1 to maxIter} {
      logInfo(s"Starting iteration: $iteration")
      val iterationStartTime = System.nanoTime()

      val syn0bc = sc.broadcast(syn0Global)
      val syn1bc = sc.broadcast(syn1Global)

      val partialFits = digitSentences.mapPartitionsWithIndex { case (i_, iter) =>
        logInfo(s"Iteration: $iteration, Partition: $i_")
        val random = new XORShiftRandom(seed ^ ((i_ + 1) << 16) ^ ((-iteration - 1) << 8))
        val contextWordPairs = iter.flatMap { s =>
          val doSample = sample > Double.MinPositiveValue
          generateContextWordPairs(s, windowSize, doSample, sampleTableBroadcast.value, random)
        }

        val groupedBatches = contextWordPairs.grouped(batchSize)

        val negLabels = 1.0f +: Array.fill(negativeSamples)(0.0f)
        val syn0 = syn0bc.value
        val syn1 = syn1bc.value
        val unigramTable = unigramTableBroadcast.value

        // initialize intermediate arrays
        val contextVector = new Array[Float](vectorSize)
        val l2Vectors = new Array[Float](vectorSize * (negativeSamples + 1))
        val gb = new Array[Float](negativeSamples + 1)
        val hiddenLayerUpdate = new Array[Float](vectorSize * (negativeSamples + 1))
        val neu1e = new Array[Float](vectorSize)
        val wordIndices = new Array[Int](negativeSamples + 1)

        val time = System.nanoTime
        var batchTime = System.nanoTime
        var idx = -1L
        for (batch <- groupedBatches) {
          idx = idx + 1

          val wordRatio =
            idx.toFloat * batchSize /
              (maxIter * (wordsPerPartition.toFloat + 1)) + ((iteration - 1).toFloat / maxIter)
          val alpha = math.max(learningRate * 0.0001, learningRate * (1 - wordRatio)).toFloat

          if(idx % 10 == 0 && idx > 0) {
            logInfo(s"Partition: $i_, wordRatio = $wordRatio, alpha = $alpha")
            val wordCount = batchSize * idx
            val timeTaken = (System.nanoTime - time) / 1e6
            val batchWordCount = 10 * batchSize
            val currentBatchTime = (System.nanoTime - batchTime) / 1e6
            batchTime = System.nanoTime
            logDebug(s"Partition: $i_, Batch time: $currentBatchTime ms, batch speed: " +
              s"${batchWordCount / currentBatchTime * 1000} words/s")
            logDebug(s"Partition: $i_, Cumulative time: $timeTaken ms, cumulative speed: " +
              s"${wordCount / timeTaken * 1000} words/s")
          }

          val errors = for ((contextIds, word) <- batch) yield {
            // initialize vectors to 0
            zeroVector(contextVector)
            zeroVector(l2Vectors)
            zeroVector(gb)
            zeroVector(hiddenLayerUpdate)
            zeroVector(neu1e)

            val scale = 1.0f / contextIds.length

            // feed forward
            contextIds.foreach { c =>
              blas.saxpy(vectorSize, scale, syn0, c * vectorSize, 1, contextVector, 0, 1)
            }

            generateNegativeSamples(random, word, unigramTable, negativeSamples, wordIndices)

            wordIndices.view.zipWithIndex.foreach { case (wordId, i) =>
              blas.scopy(vectorSize, syn1, vectorSize * wordId, 1, l2Vectors, vectorSize * i, 1)
            }

            val rows = negativeSamples + 1
            val cols = vectorSize
            blas
              .sgemv("T", cols, rows, 1.0f, l2Vectors, 0, cols, contextVector, 0, 1, 0.0f, gb, 0, 1)

            {
              var i = 0
              while(i < gb.length) {
                val v = 1.0f / (1 + math.exp(-gb(i)).toFloat)
                val err = (negLabels(i) - v) * alpha
                gb.update(i, err)
                i+=1
              }
            }

            // update for hidden -> output layer
            blas.sger(cols, rows, 1.0f, contextVector, 1, gb, 1, hiddenLayerUpdate, cols)

            // update hidden -> output layer, syn1
            wordIndices.view.zipWithIndex.foreach { case (w, i) =>
              blas.saxpy(vectorSize,
                1.0f,
                hiddenLayerUpdate,
                i * vectorSize,
                1,
                syn1,
                w * vectorSize,
                1)
            }

            // update for word vectors
            blas.sgemv("N", cols, rows, scale, l2Vectors, 0, cols, gb, 0, 1, 1.0f, neu1e, 0, 1)

            // update input -> hidden layer, syn0
            contextIds.foreach { i =>
              blas.saxpy(vectorSize, 1.0f, neu1e, 0, 1, syn0, i * vectorSize, 1)
            }
            gb.map(math.abs).sum / alpha
          }
          logInfo(s"Partition: $i_, Average Batch Error = ${errors.sum / batchSize}")
        }
        Iterator((0, syn0), (1, syn1))
      }

      val aggedMatrices = partialFits.reduceByKey { case (v1, v2) =>
        blas.saxpy(vocabSize, 1.0f, v2, 1, v1, 1)
        v1
      }.collect

      assert(aggedMatrices.length == 2)
      val norm = 1.0f / numPartitions
      aggedMatrices.foreach {case (i, syn) =>
        blas.sscal(syn.length, norm, syn, 0, 1)
        if (i == 0) {
          // copy syn0
          blas.scopy(syn.length, syn, 0, 1, syn0Global, 0, 1)
        } else {
          // copy syn1
          blas.scopy(syn.length, syn, 0, 1, syn1Global, 0, 1)
        }
      }
      syn0bc.destroy(false)
      syn1bc.destroy(false)
      val timePerIteration = (System.nanoTime() - iterationStartTime) / 1e6
      logInfo(s"Total time taken per iteration: ${timePerIteration} ms")
    }
    digitSentences.unpersist()
    vocabMapBroadcast.destroy()
    unigramTableBroadcast.destroy()
    sampleTableBroadcast.destroy()

    new feature.Word2VecModel(vocabMap, syn0Global)
  }

  /**
   * Similar to InitUnigramTable in the original code.
   */
  private def generateUnigramTable(normalizedWeights: Array[Double], tableSize: Int): Array[Int] = {
    val table = new Array[Int](tableSize)
    var index = 0
    var wordId = 0
    while (index < table.length) {
      table.update(index, wordId)
      if (index.toFloat / table.length >= normalizedWeights(wordId)) {
        wordId = math.min(normalizedWeights.length - 1, wordId + 1)
      }
      index += 1
    }
    table
  }

  private def generateVocab[S <: Iterable[String]](
      input: RDD[S],
      minCount: Int,
      sample: Double,
      unigramTableSize: Int): Vocabulary = {
    val sc = input.context

    val words = input.flatMap(x => x)

    val sortedWordCounts = words.map(w => (w, 1L))
      .reduceByKey(_ + _)
      .filter{case (w, c) => c >= minCount}
      .collect()
      .sortWith{case ((w1, c1), (w2, c2)) => c1 > c2}
      .zipWithIndex

    val totalWordCount = sortedWordCounts.map(_._1._2).sum

    val vocabMap = sortedWordCounts.map{case ((w, c), i) =>
      w -> i
    }.toMap

    val samplingTable = new Array[Float](vocabMap.size)

    if (sample > Double.MinPositiveValue) {
      sortedWordCounts.foreach { case ((w, c), i) =>
        val samplingRatio = sample * totalWordCount / c
        samplingTable.update(i, (math.sqrt(samplingRatio) + samplingRatio).toFloat)
      }
    }

    val weights = sortedWordCounts.map{ case((_, x), _) => scala.math.pow(x, power)}
    val totalWeight = weights.sum

    val normalizedCumWeights = weights.scanLeft(0.0)(_ + _).tail.map(x => x / totalWeight)

    val unigramTable = generateUnigramTable(normalizedCumWeights, unigramTableSize)

    Vocabulary(totalWordCount, vocabMap, unigramTable, samplingTable)
  }

  private def zeroVector(v: Array[Float]): Unit = {
    var i = 0
    while(i < v.length) {
      v.update(i, 0.0f)
      i+= 1
    }
  }

  private def generateContextWordPairs(
      sentence: Array[Int],
      window: Int,
      doSample: Boolean,
      samplingTable: Array[Float],
      random: XORShiftRandom): Iterator[(Array[Int], Int)] = {
    val reducedSentence = if (doSample) {
      sentence.filter(i => samplingTable(i) > random.nextFloat)
    } else {
      sentence
    }
    reducedSentence.iterator.zipWithIndex.map { case (word, i) =>
      val b = window - random.nextInt(window) // (window - a) in original code
      // pick b words around the current word index
      val start = math.max(0, i - b) // c in original code, floor ar 0
      val end = math.min(sentence.length, i + b + 1) // cap at sentence length
      // make sure current word is not a part of the context
      val contextIds = sentence.view.zipWithIndex.slice(start, end)
        .filter{case (_, pos) => pos != i}.map(_._1)
      (contextIds.toArray, word)
    }
  }

  // This essentially helps translate from uniform distribution to a distribution
  // resembling uni-gram frequency distribution.
  private def generateNegativeSamples(
      random: XORShiftRandom,
      word: Int,
      unigramTable: Array[Int],
      numSamples: Int,
      arr: Array[Int]): Unit = {
    assert(numSamples + 1 == arr.length,
      s"Input array should be large enough to hold ${numSamples} negative samples")
    arr.update(0, word)
    var i = 1
    while (i <= numSamples) {
      val negSample = unigramTable(random.nextInt(unigramTable.length))
      if(negSample != word) {
        arr.update(i, negSample)
        i += 1
      }
    }
  }
}
