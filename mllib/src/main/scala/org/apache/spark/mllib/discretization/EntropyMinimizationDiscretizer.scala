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

package org.apache.spark.mllib.discretization

import scala.collection.mutable
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.regression.LabeledPoint

/**
 * This class contains methods to calculate thresholds to discretize continuous values with the
 * method proposed by Fayyad and Irani in Multi-Interval Discretization of Continuous-Valued
 * Attributes (1993).
 *
 * @param continuousFeaturesIndexes Indexes of features to be discretized.
 * @param elementsPerPartition Maximum number of thresholds to treat in each RDD partition.
 * @param maxBins Maximum number of bins for each discretized feature.
 */
class EntropyMinimizationDiscretizer private (
    val continuousFeaturesIndexes: Seq[Int],
    val elementsPerPartition: Int,
    val maxBins: Int)
  extends Serializable {

  private val partitions = { x: Long => math.ceil(x.toDouble / elementsPerPartition).toInt }
  private val log2 = { x: Double => math.log(x) / math.log(2) }

  /**
   * Run the algorithm with the configured parameters on an input.
   * @param data RDD of LabeledPoint's.
   * @return A EntropyMinimizationDiscretizerModel with the thresholds to discretize.
   */
  def run(data: RDD[LabeledPoint]) = {
    val labels2Int = data.context.broadcast(data.map(_.label).distinct.collect.zipWithIndex.toMap)
    val nLabels = labels2Int.value.size

    var thresholds = Map.empty[Int, Seq[Double]]
    for (i <- continuousFeaturesIndexes) {
      val featureValues = data.map({
        case LabeledPoint(label, values) => (values(i), labels2Int.value(label))
      })
      val sortedValues = featureValues.sortByKey()
      val initialCandidates = initialThresholds(sortedValues, nLabels)
      val thresholdsForFeature = this.getThresholds(initialCandidates, nLabels)
      thresholds += ((i, thresholdsForFeature))
    }

    new EntropyMinimizationDiscretizerModel(thresholds)

  }

  /**
   * Calculates the initial candidate treholds for a feature
   * @param data RDD of (value, label) pairs.
   * @param nLabels Number of distinct labels in the dataset.
   * @return RDD of (candidate, class frequencies between last and current candidate) pairs.
   */
  private def initialThresholds(data: RDD[(Double, Int)], nLabels: Int) = {
    data.mapPartitions({ it =>
      var lastX = Double.NegativeInfinity
      var lastY = Int.MinValue
      var result = Seq.empty[(Double, Array[Long])]
      var freqs = Array.fill[Long](nLabels)(0L)

      for ((x, y) <- it) {
        if (x != lastX && y != lastY && lastX != Double.NegativeInfinity) {
          // new candidate and interval
          result = ((x + lastX) / 2, freqs) +: result
          freqs = Array.fill[Long](nLabels)(0L)
          freqs(y) = 1L
        } else {
          // we continue on the same interval
          freqs(y) += 1
        }
        lastX = x
        lastY = y
      }

      // we add last element as a candidate threshold for convenience
      result = (lastX, freqs) +: result

      result.reverse.toIterator
    }).persist(StorageLevel.MEMORY_AND_DISK)
  }
  
  /**
   * Returns a sequence of doubles that define the intervals to make the discretization.
   *
   * @param candidates RDD of (value, label) pairs
   */
  private def getThresholds(candidates: RDD[(Double, Array[Long])], nLabels: Int): Seq[Double] = {

    // Create queue
    val stack = new mutable.Queue[((Double, Double), Option[Double])]

    // Insert first in the stack
    stack.enqueue(((Double.NegativeInfinity, Double.PositiveInfinity), None))
    var result = Seq(Double.NegativeInfinity)

    // While more elements to eval, continue
    while(stack.length > 0 && result.size < this.maxBins){

      val (bounds, lastThresh) = stack.dequeue

      var cands = candidates.filter({ case (th, _) => th > bounds._1 && th <= bounds._2 })
      val nCands = cands.count
      if (nCands > 0) {
        cands = cands.coalesce(partitions(nCands))

        evalThresholds(cands, lastThresh, nLabels) match {
          case Some(th) =>
            result = th +: result
            stack.enqueue(((bounds._1, th), Some(th)))
            stack.enqueue(((th, bounds._2), Some(th)))
          case None => /* criteria not fulfilled, finish */
        }
      }
    }
    (Double.PositiveInfinity +: result).sorted
  }

  /**
   * Selects one final thresholds among the candidates and returns two partitions to recurse
   *
   * @param candidates RDD of (candidate, class frequencies between last and current candidate)
   * @param lastSelected last selected threshold to avoid selecting it again
   */
  private def evalThresholds(
      candidates: RDD[(Double, Array[Long])],
      lastSelected : Option[Double],
      nLabels: Int) = {

    val numPartitions = candidates.partitions.size

    val sc = candidates.sparkContext

    // store total frequencies for each partition
    val totals = sc.runJob(candidates, { case it =>
      val accum = Array.fill(nLabels)(0L)
      for ((_, freqs) <- it) {
        for (i <- 0 until nLabels) accum(i) += freqs(i)
      }
      accum
    }: (Iterator[(Double, Array[Long])]) => Array[Long])

    val bcTotals = sc.broadcast(totals)

    val result =
      candidates.mapPartitionsWithIndex({ (slice, it) =>

        // accumulate freqs from left to right
        val leftTotal = Array.fill(nLabels)(0L)
        for (i <- 0 until slice) {
          for (j <- 0 until nLabels) leftTotal(j) += bcTotals.value(i)(j)
        }

        var leftAccum = Seq.empty[(Double, Array[Long], Array[Long])]

        for ((cand, freqs) <- it) {
          for (i <- 0 until nLabels) leftTotal(i) += freqs(i)
          leftAccum = (cand, freqs, leftTotal.clone) +: leftAccum
        }

        // accumulate freqs from right to left
        val rightTotal = Array.fill(nLabels)(0L)
        for (i <- slice + 1 until numPartitions) {
          for (j <- 0 until nLabels) leftTotal(j) += bcTotals.value(i)(j)
        }

        var leftAndRightAccum = Seq.empty[(Double, Array[Long], Array[Long], Array[Long])]

        for ((cand, freqs, leftFreqs) <- leftAccum) {
          leftAndRightAccum = (cand, freqs, leftFreqs, rightTotal.clone) +: leftAndRightAccum
          for (i <- 0 until nLabels) rightTotal(i) += freqs(i)
        }

        leftAndRightAccum.iterator
      })

    // calculate h(S)
    // s: number of elements
    // k: number of distinct classes
    // hs: entropy

    val total = Array.fill(nLabels)(0L)
    for (partition_total <- totals) {
      for (i <- 0 until nLabels) total(i) += partition_total(i)
    }

    val s  = total.reduce(_ + _)
    val hs = InfoTheory.entropy(total.toSeq, s)
    val k  = total.filter(_ != 0).size

    // select best threshold according to the criteria
    val finalCandidates =
      result.flatMap({
        case (cand, _, leftFreqs, rightFreqs) =>

          val k1  = leftFreqs.filter(_ != 0).size
          val s1  = if (k1 > 0) leftFreqs.reduce(_ + _) else 0
          val hs1 = InfoTheory.entropy(leftFreqs, s1)

          val k2  = rightFreqs.filter(_ != 0).size
          val s2  = if (k2 > 0) rightFreqs.reduce(_ + _) else 0
          val hs2 = InfoTheory.entropy(rightFreqs, s2)

          val weightedHs = (s1 * hs1 + s2 * hs2) / s
          val gain       = hs - weightedHs
          val delta      = log2(math.pow(3, k) - 2) - (k * hs - k1 * hs1 - k2 * hs2)
          var criterion  = (gain - (log2(s - 1) + delta) / s) > -1e-5

          lastSelected match {
              case None =>
              case Some(last) => criterion = criterion && (cand != last)
          }

          if (criterion) {
            Seq((weightedHs, cand))
          } else {
            Seq.empty[(Double, Double)]
          }
      })

    // choose best candidates and partition data to make recursive calls
    if (finalCandidates.count > 0) {
      val selectedThreshold = finalCandidates.reduce({ case ((whs1, cand1), (whs2, cand2)) =>
        if (whs1 < whs2) (whs1, cand1) else (whs2, cand2)
      })._2
      Some(selectedThreshold)
    } else {
      None
    }

  }

}

object EntropyMinimizationDiscretizer {

  /**
   * Train a EntropyMinimizationDiscretizerModel given an RDD of LabeledPoint's.
   * @param input RDD of LabeledPoint's.
   * @param continuousFeaturesIndexes Indexes of features to be discretized.
   * @param maxBins Maximum number of bins for each discretized feature.
   * @param elementsPerPartition Maximum number of thresholds to treat in each RDD partition.
   * @return A EntropyMinimizationDiscretizerModel which has the thresholds to discretize.
   */
  def train(
      input: RDD[LabeledPoint],
      continuousFeaturesIndexes: Seq[Int],
      maxBins: Int = Int.MaxValue,
      elementsPerPartition: Int = 20000)
    : EntropyMinimizationDiscretizerModel = {

    new EntropyMinimizationDiscretizer(continuousFeaturesIndexes, elementsPerPartition, maxBins)
      .run(input)

  }

}
