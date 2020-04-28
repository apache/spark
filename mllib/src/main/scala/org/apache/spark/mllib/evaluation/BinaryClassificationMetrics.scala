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

package org.apache.spark.mllib.evaluation

import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.evaluation.binary._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

/**
 * Evaluator for binary classification.
 *
 * @param scoreAndLabels an RDD of (score, label) or (score, label, weight) tuples.
 * @param numBins if greater than 0, then the curves (ROC curve, PR curve) computed internally
 *                will be down-sampled to this many "bins". If 0, no down-sampling will occur.
 *                This is useful because the curve contains a point for each distinct score
 *                in the input, and this could be as large as the input itself -- millions of
 *                points or more, when thousands may be entirely sufficient to summarize
 *                the curve. After down-sampling, the curves will instead be made of approximately
 *                `numBins` points instead. Points are made from bins of equal numbers of
 *                consecutive points. The size of each bin is
 *                `floor(scoreAndLabels.count() / numBins)`, which means the resulting number
 *                of bins may not exactly equal numBins. The last bin in each partition may
 *                be smaller as a result, meaning there may be an extra sample at
 *                partition boundaries.
 */
@Since("1.0.0")
class BinaryClassificationMetrics @Since("3.0.0") (
    @Since("1.3.0") val scoreAndLabels: RDD[_ <: Product],
    @Since("1.3.0") val numBins: Int = 1000)
  extends Logging {
  val scoreLabelsWeight: RDD[(Double, (Double, Double))] = scoreAndLabels.map {
    case (prediction: Double, label: Double, weight: Double) =>
      require(weight >= 0.0, s"instance weight, $weight has to be >= 0.0")
      (prediction, (label, weight))
    case (prediction: Double, label: Double) =>
      (prediction, (label, 1.0))
    case other =>
      throw new IllegalArgumentException(s"Expected tuples, got $other")
  }

  require(numBins >= 0, "numBins must be nonnegative")

  /**
   * Defaults `numBins` to 0.
   */
  @Since("1.0.0")
  def this(scoreAndLabels: RDD[(Double, Double)]) = this(scoreAndLabels, 0)

  /**
   * An auxiliary constructor taking a DataFrame.
   * @param scoreAndLabels a DataFrame with two double columns: score and label
   */
  private[mllib] def this(scoreAndLabels: DataFrame) =
    this(scoreAndLabels.rdd.map {
      case Row(prediction: Double, label: Double, weight: Double) =>
        (prediction, label, weight)
      case Row(prediction: Double, label: Double) =>
        (prediction, label, 1.0)
      case other =>
        throw new IllegalArgumentException(s"Expected Row of tuples, got $other")
    })

  /**
   * Unpersist intermediate RDDs used in the computation.
   */
  @Since("1.0.0")
  def unpersist(): Unit = {
    cumulativeCounts.unpersist()
  }

  /**
   * Returns thresholds in descending order.
   */
  @Since("1.0.0")
  def thresholds(): RDD[Double] = cumulativeCounts.map(_._1)

  /**
   * Returns the receiver operating characteristic (ROC) curve,
   * which is an RDD of (false positive rate, true positive rate)
   * with (0.0, 0.0) prepended and (1.0, 1.0) appended to it.
   * @see <a href="http://en.wikipedia.org/wiki/Receiver_operating_characteristic">
   * Receiver operating characteristic (Wikipedia)</a>
   */
  @Since("1.0.0")
  def roc(): RDD[(Double, Double)] = {
    val rocCurve = createCurve(FalsePositiveRate, Recall)
    val numParts = rocCurve.getNumPartitions
    rocCurve.mapPartitionsWithIndex { case (pid, iter) =>
      if (numParts == 1) {
        require(pid == 0)
        Iterator.single((0.0, 0.0)) ++ iter ++ Iterator.single((1.0, 1.0))
      } else if (pid == 0) {
        Iterator.single((0.0, 0.0)) ++ iter
      } else if (pid == numParts - 1) {
        iter ++ Iterator.single((1.0, 1.0))
      } else {
        iter
      }
    }
  }

  /**
   * Computes the area under the receiver operating characteristic (ROC) curve.
   */
  @Since("1.0.0")
  def areaUnderROC(): Double = AreaUnderCurve.of(roc())

  /**
   * Returns the precision-recall curve, which is an RDD of (recall, precision),
   * NOT (precision, recall), with (0.0, p) prepended to it, where p is the precision
   * associated with the lowest recall on the curve.
   * @see <a href="http://en.wikipedia.org/wiki/Precision_and_recall">
   * Precision and recall (Wikipedia)</a>
   */
  @Since("1.0.0")
  def pr(): RDD[(Double, Double)] = {
    val prCurve = createCurve(Recall, Precision)
    val (_, firstPrecision) = prCurve.first()
    prCurve.mapPartitionsWithIndex { case (pid, iter) =>
      if (pid == 0) {
        Iterator.single((0.0, firstPrecision)) ++ iter
      } else {
        iter
      }
    }
  }

  /**
   * Computes the area under the precision-recall curve.
   */
  @Since("1.0.0")
  def areaUnderPR(): Double = AreaUnderCurve.of(pr())

  /**
   * Returns the (threshold, F-Measure) curve.
   * @param beta the beta factor in F-Measure computation.
   * @return an RDD of (threshold, F-Measure) pairs.
   * @see <a href="http://en.wikipedia.org/wiki/F1_score">F1 score (Wikipedia)</a>
   */
  @Since("1.0.0")
  def fMeasureByThreshold(beta: Double): RDD[(Double, Double)] = createCurve(FMeasure(beta))

  /**
   * Returns the (threshold, F-Measure) curve with beta = 1.0.
   */
  @Since("1.0.0")
  def fMeasureByThreshold(): RDD[(Double, Double)] = fMeasureByThreshold(1.0)

  /**
   * Returns the (threshold, precision) curve.
   */
  @Since("1.0.0")
  def precisionByThreshold(): RDD[(Double, Double)] = createCurve(Precision)

  /**
   * Returns the (threshold, recall) curve.
   */
  @Since("1.0.0")
  def recallByThreshold(): RDD[(Double, Double)] = createCurve(Recall)

  private lazy val (
    cumulativeCounts: RDD[(Double, BinaryLabelCounter)],
    confusions: RDD[(Double, BinaryConfusionMatrix)]) = {
    // Create a bin for each distinct score value, count weighted positives and
    // negatives within each bin, and then sort by score values in descending order.
    val counts = scoreLabelsWeight.combineByKey(
      createCombiner = (labelAndWeight: (Double, Double)) =>
        new BinaryLabelCounter(0.0, 0.0) += (labelAndWeight._1, labelAndWeight._2),
      mergeValue = (c: BinaryLabelCounter, labelAndWeight: (Double, Double)) =>
        c += (labelAndWeight._1, labelAndWeight._2),
      mergeCombiners = (c1: BinaryLabelCounter, c2: BinaryLabelCounter) => c1 += c2
    ).sortByKey(ascending = false)

    val binnedCounts =
      // Only down-sample if bins is > 0
      if (numBins == 0) {
        // Use original directly
        counts
      } else {
        val countsSize = counts.count()
        // Group the iterator into chunks of about countsSize / numBins points,
        // so that the resulting number of bins is about numBins
        val grouping = countsSize / numBins
        if (grouping < 2) {
          // numBins was more than half of the size; no real point in down-sampling to bins
          logInfo(s"Curve is too small ($countsSize) for $numBins bins to be useful")
          counts
        } else {
          counts.mapPartitions { iter =>
            if (iter.hasNext) {
              var score = Double.NaN
              var agg = new BinaryLabelCounter()
              var cnt = 0L
              iter.flatMap { pair =>
                score = pair._1
                agg += pair._2
                cnt += 1
                if (cnt == grouping) {
                  // The score of the combined point will be just the last one's score,
                  // which is also the minimal in each chunk since all scores are already
                  // sorted in descending.
                  // The combined point will contain all counts in this chunk. Thus, calculated
                  // metrics (like precision, recall, etc.) on its score (or so-called threshold)
                  // are the same as those without sampling.
                  val ret = (score, agg)
                  agg = new BinaryLabelCounter()
                  cnt = 0
                  Some(ret)
                } else None
              } ++ {
                if (cnt > 0) {
                  Iterator.single((score, agg))
                } else Iterator.empty
              }
            } else Iterator.empty
          }
        }
      }

    val agg = binnedCounts.values.mapPartitions { iter =>
      val agg = new BinaryLabelCounter()
      iter.foreach(agg += _)
      Iterator(agg)
    }.collect()
    val partitionwiseCumulativeCounts =
      agg.scanLeft(new BinaryLabelCounter())((agg, c) => agg.clone() += c)
    val totalCount = partitionwiseCumulativeCounts.last
    logInfo(s"Total counts: $totalCount")
    val cumulativeCounts = binnedCounts.mapPartitionsWithIndex(
      (index: Int, iter: Iterator[(Double, BinaryLabelCounter)]) => {
        val cumCount = partitionwiseCumulativeCounts(index)
        iter.map { case (score, c) =>
          cumCount += c
          (score, cumCount.clone())
        }
      }, preservesPartitioning = true)
    cumulativeCounts.persist()
    val confusions = cumulativeCounts.map { case (score, cumCount) =>
      (score, BinaryConfusionMatrixImpl(cumCount, totalCount).asInstanceOf[BinaryConfusionMatrix])
    }
    (cumulativeCounts, confusions)
  }

  /** Creates a curve of (threshold, metric). */
  private def createCurve(y: BinaryClassificationMetricComputer): RDD[(Double, Double)] = {
    confusions.map { case (s, c) =>
      (s, y(c))
    }
  }

  /** Creates a curve of (metricX, metricY). */
  private def createCurve(
      x: BinaryClassificationMetricComputer,
      y: BinaryClassificationMetricComputer): RDD[(Double, Double)] = {
    confusions.map { case (_, c) =>
      (x(c), y(c))
    }
  }
}
