/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements. See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.spark.mllib.discretization

import scala.collection.mutable.Stack
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.util.InfoTheory
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.regression.LabeledPoint
import scala.collection.mutable


/**
 * This class contains methods to discretize continuous values with the method proposed in
 * [Fayyad and Irani, Multi-Interval Discretization of Continuous-Valued Attributes, 1993]
 */
class EntropyMinimizationDiscretizer private (
    @transient var data: RDD[LabeledPoint],
    @transient var continousFeatures: Seq[Int],
    var elementsPerPartition: Int = 18000,
    var maxBins: Int = Int.MaxValue)
  extends DiscretizerModel {

  private var thresholds = Map.empty[Int, Seq[Double]]
  private val partitions = { x : Long => math.ceil(x.toDouble / elementsPerPartition).toInt }

  def this() = this(null, null)

  /**
   * Sets the RDD[LabeledPoint] to be discretized
   *
   * @param data RDD[LabeledPoint] to be discretized
   */
  def setData(data: RDD[LabeledPoint]): EntropyMinimizationDiscretizer = {
    this.data = data
    this
  }

  /**
   * Sets the indexes of the features to be discretized
   *
   * @param continuousFeatures Indexes of features to be discretized
   */
  def setContinuousFeatures(continuousFeatures: Seq[Int]): EntropyMinimizationDiscretizer = {
    this.continousFeatures = continuousFeatures
    this
  }

  /**
   * Sets the maximum number of elements that a partition should have
   *
   * @param ratio Maximum number of elements for a partition
   * @return The Discretizer with the parameter changed
   */
  def setElementsPerPartition(ratio: Int): EntropyMinimizationDiscretizer = {
    this.elementsPerPartition = ratio
    this
  }

  /**
   * Sets the maximum number of discrete values
   *
   * @param maxBins Maximum number of discrete values
   * @return The Discretizer with the parameter changed
   */
  def setMaxBins(maxBins: Int): EntropyMinimizationDiscretizer = {
    this.maxBins = maxBins
    this
  }
  
  /**
   * Returns the thresholds used to discretized the given feature
   *
   * @param feature The number of the feature to discretize
   */
  def getThresholdsForFeature(feature: Int): Seq[Double] = {
    thresholds.get(feature) match {
      case Some(a) => a
      case None =>
        val featureValues = data.map({
          case LabeledPoint(label, values) => (values(feature), label.toString)
        })
        val sortedValues = featureValues.sortByKey()
        val initial_candidates = initialThresholds(sortedValues)
        val thresholdsForFeature = this.getThresholds(initial_candidates)
        this.thresholds += ((feature, thresholdsForFeature))
        thresholdsForFeature
    }
  }

  /**
   * Returns the thresholds used to discretized the given features
   *
   * @param features The number of the feature to discretize
   */
  def getThresholdsForFeature(features: Seq[Int]): Map[Int, Seq[Double]] = {
    for (feature <- features diff this.thresholds.keys.toSeq) {
      getThresholdsForFeature(feature)
    }

    this.thresholds.filter({ features.contains(_) })
  }

  /**
   * Returns the thresholds used to discretized the continuous features
   */
  def getThresholdsForContinuousFeatures: Map[Int, Seq[Double]] = {
    for (feature <- continousFeatures diff this.thresholds.keys.toSeq) {
      getThresholdsForFeature(feature)
    }

    this.thresholds
  }

  /**
   * Calculates the initial candidate treholds for a feature
   * @param data RDD of (value, label) pairs
   * @return RDD of (candidate, class frequencies between last and current candidate) pairs
   */
  private def initialThresholds(data: RDD[(Double, String)]): RDD[(Double, Map[String,Int])] = {
    data.mapPartitions({ it =>
      var lastX = Double.NegativeInfinity
      var lastY = ""
      var result = Seq.empty[(Double, Map[String, Int])]
      var freqs = Map.empty[String, Int]

      for ((x, y) <- it) {
        if (x != lastX && y != lastY && lastX != Double.NegativeInfinity) {
          // new candidate and interval
          result = ((x + lastX) / 2, freqs) +: result
          freqs = freqs.empty + ((y, 1))
        } else {
          // we continue on the same interval
          freqs = freqs.updated(y, freqs.getOrElse(y, 0) + 1)
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
  private def getThresholds(candidates: RDD[(Double, Map[String,Int])]): Seq[Double] = {

    //Create queue
    val stack = new mutable.Queue[((Double, Double), Option[Double])]

    //Insert first in the stack
    stack.enqueue(((Double.NegativeInfinity, Double.PositiveInfinity), None))
    var result = Seq(Double.NegativeInfinity)

    // While more elements to eval, continue
    while(stack.length > 0 && result.size < this.maxBins){

      val (bounds, lastThresh) = stack.dequeue

      var cands = candidates.filter({ case (th, _) => th > bounds._1 && th <= bounds._2 })
      val nCands = cands.count
      if (nCands > 0) {
        cands = cands.coalesce(partitions(nCands))

        evalThresholds(cands, lastThresh) match {
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
      candidates: RDD[(Double, Map[String, Int])],
      lastSelected : Option[Double]) = {

    var result = candidates.map({
      case (cand, freqs) =>
        (cand, freqs, Seq.empty[Int], Seq.empty[Int])
    }).cache

    val numPartitions = candidates.partitions.size
    val bc_numPartitions = candidates.context.broadcast(numPartitions)

    // stores accumulated freqs from left to right
    val l_total = candidates.context.accumulator(Map.empty[String, Int])(MapAccumulator)
    // stores accumulated freqs from right to left
    val r_total = candidates.context.accumulator(Map.empty[String, Int])(MapAccumulator)

    // calculates accumulated frequencies for each candidate
    (0 until numPartitions) foreach { l2r_i =>

      val bc_l_total = l_total.value
      val bc_r_total = r_total.value

      result =
        result.mapPartitionsWithIndex({ (slice, it) =>

          val l2r = slice == l2r_i
          val r2l = slice == bc_numPartitions.value - 1 - l2r_i

          if (l2r && r2l) {

            // accumulate both from left to right and right to left
            var partialResult = Seq.empty[(Double, Map[String, Int], Seq[Int], Seq[Int])]
            var accum = Map.empty[String, Int]

            for ((cand, freqs, _, r_freqs) <- it) {
              accum = Utils.sumFreqMaps(accum, freqs)
              val l_freqs = Utils.sumFreqMaps(accum, bc_l_total).values.toList
              partialResult = (cand, freqs, l_freqs, r_freqs) +: partialResult
            }

            l_total += accum

            val r2lIt = partialResult.iterator
            partialResult = Seq.empty[(Double, Map[String, Int], Seq[Int], Seq[Int])]
            accum = Map.empty[String, Int]
            for ((cand, freqs, l_freqs, _) <- r2lIt) {
              val r_freqs = Utils.sumFreqMaps(accum, bc_r_total).values.toList
              accum = Utils.sumFreqMaps(accum, freqs)
              partialResult = (cand, freqs, l_freqs, r_freqs) +: partialResult
            }
            r_total += accum

            partialResult.iterator

          } else if (l2r) {

            // accumulate freqs from left to right
            var partialResult = Seq.empty[(Double, Map[String, Int], Seq[Int], Seq[Int])]
            var accum = Map.empty[String, Int]

            for ((cand, freqs, _, r_freqs) <- it) {
              accum = Utils.sumFreqMaps(accum, freqs)
              val l_freqs = Utils.sumFreqMaps(accum, bc_l_total).values.toList
              partialResult = (cand, freqs, l_freqs, r_freqs) +: partialResult
            }

            l_total += accum
            partialResult.reverseIterator

          } else if (r2l) {

            // accumulate freqs from right to left
            var partialResult = Seq.empty[(Double, Map[String, Int], Seq[Int], Seq[Int])]
            var accum = Map.empty[String, Int]
            val r2lIt = it.toSeq.reverseIterator

            for ((cand, freqs, l_freqs, _) <- r2lIt) {
              val r_freqs = Utils.sumFreqMaps(accum, bc_r_total).values.toList
              accum = Utils.sumFreqMaps(accum, freqs)
              partialResult = (cand, freqs, l_freqs, r_freqs) +: partialResult
            }

            r_total += accum

            partialResult.iterator

          } else {
            // do nothing in this iteration
            it
          }
        }, true) // important to maintain partitions within the loop
        .persist(StorageLevel.MEMORY_AND_DISK) // needed, otherwise spark repeats calculations

      result.foreachPartition({ _ => }) // Forces the iteration to be calculated
    }

    // calculate h(S)
    // s: number of elements
    // k: number of distinct classes
    // hs: entropy

    val s  = l_total.value.values.reduce(_+_)
    val hs = InfoTheory.entropy(l_total.value.values.toSeq, s)
    val k  = l_total.value.values.size

    // select best threshold according to the criteria
    val final_candidates =
      result.flatMap({
        case (cand, _, l_freqs, r_freqs) =>

          val k1  = l_freqs.size
          val s1  = if (k1 > 0) l_freqs.reduce(_ + _) else 0
          val hs1 = InfoTheory.entropy(l_freqs, s1)

          val k2  = r_freqs.size
          val s2  = if (k2 > 0) r_freqs.reduce(_ + _) else 0
          val hs2 = InfoTheory.entropy(r_freqs, s2)

          val weighted_hs = (s1 * hs1 + s2 * hs2) / s
          val gain        = hs - weighted_hs
          val delta       = Utils.log2(3 ^ k - 2) - (k * hs - k1 * hs1 - k2 * hs2)
          var criterion   = (gain - (Utils.log2(s - 1) + delta) / s) > -1e-5

          lastSelected match {
              case None =>
              case Some(last) => criterion = criterion && (cand != last)
          }

          if (criterion) {
            Seq((weighted_hs, cand))
          } else {
            Seq.empty[(Double, Double)]
          }
      })

    // choose best candidates and partition data to make recursive calls
    if (final_candidates.count > 0) {
      val selected_threshold = final_candidates.reduce({ case ((whs1, cand1), (whs2, cand2)) =>
        if (whs1 < whs2) (whs1, cand1) else (whs2, cand2)
      })._2;
      Some(selected_threshold)
    } else {
      None
    }

  }

  /**
   * Discretizes a value with a set of intervals.
   *
   * @param value The value to be discretized
   * @param thresholds Thresholds used to asign a discrete value
   */
  private def assignDiscreteValue(value: Double, thresholds: Seq[Double]) = {
    var aux = thresholds.zipWithIndex
    while (value > aux.head._1) aux = aux.tail
    aux.head._2
  }

  /**
   * Discretizes an RDD of (label, array of values) pairs.
   */
  def discretize: RDD[LabeledPoint] = {
    // calculate thresholds that aren't already calculated
    getThresholdsForContinuousFeatures

    val bc_thresholds = this.data.context.broadcast(this.thresholds)

    // applies thresholds to discretize every continuous feature
    data.map {
      case LabeledPoint(label, values) =>
        LabeledPoint(label,
          values.zipWithIndex map {
            case (value, i) =>
              if (bc_thresholds.value.keySet contains i) {
                assignDiscreteValue(value, bc_thresholds.value(i))
              } else {
                value
              }
          })
    }
  }

}

object EntropyMinimizationDiscretizer {

  def apply(
      data: RDD[LabeledPoint],
      continuousFeatures: Seq[Int])
    : EntropyMinimizationDiscretizer = {
    new EntropyMinimizationDiscretizer(data, continuousFeatures)
  }

  def apply: EntropyMinimizationDiscretizer = new EntropyMinimizationDiscretizer()

}
