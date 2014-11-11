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


package org.apache.spark.mllib.grouped

import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.{UnionRDD, RDD}
import org.apache.spark.SparkContext._
import org.apache.spark.Logging
import org.apache.spark.mllib.evaluation.binary._
import org.apache.spark.mllib.evaluation.binary.BinaryConfusionMatrixImpl
import scala.reflect.ClassTag
import scala.collection.mutable.HashMap

/**
 * :: Experimental ::
 * Grouped Evaluator for binary classification.
 *
 * @param scoreAndLabels an RDD of (groupKey, (score, label)) pairs.
 */
@Experimental
class GroupedBinaryClassificationMetrics[K](scoreAndLabels: RDD[(K,(Double, Double))])
                                           (implicit tag:ClassTag[K])
  extends Logging {

  /** Unpersist intermediate RDDs used in the computation. */
  def unpersist() {
    cumulativeCounts.unpersist()
  }

  /** Returns thresholds in descending order. */
  def thresholds(): RDD[(K,Double)] = cumulativeCounts.map(x => (x._1, x._2._1))

  /**
   * Returns the receiver operating characteristic (ROC) curve,
   * which is an RDD of (false positive rate, true positive rate)
   * with (0.0, 0.0) prepended and (1.0, 1.0) appended to it.
   * @see http://en.wikipedia.org/wiki/Receiver_operating_characteristic
   */
  def roc(): RDD[(K,(Double, Double))] = {
    val rocCurve = createCurve(FalsePositiveRate, Recall)
    val sc = confusions.context
    val keys = cumulativeCounts.keys
    val firsts = keys.distinct().map( x => (x, (0.0,0.0) ) )
    val lasts = keys.distinct().map( x => (x, (1.0,1.0) ) )
    new UnionRDD[(K, (Double, Double))](sc, Seq(firsts, rocCurve, lasts))
  }

  /**
   * Computes the area under the receiver operating characteristic (ROC) curve.
   */
  def areaUnderROC(): Map[K,Double] = GroupedAreaUnderCurve.of(roc())

  /**
   * Returns the precision-recall curve, which is an RDD of (recall, precision),
   * NOT (precision, recall), with (0.0, 1.0) prepended to it.
   * @see http://en.wikipedia.org/wiki/Precision_and_recall
   */
  def pr()(implicit tag:ClassTag[K]) : RDD[(K,(Double, Double))] = {
    val prCurve = createCurve(Recall, Precision)
    val sc = confusions.context
    val firsts = cumulativeCounts.keys.distinct().map( x => (x, (0.0,1.0) ) )
    firsts.union(prCurve)
  }

  /**
   * Computes the area under the precision-recall curve.
   */
  def areaUnderPR(): Map[K,Double] = GroupedAreaUnderCurve.of(pr())

  /**
   * Returns the (threshold, F-Measure) curve.
   * @param beta the beta factor in F-Measure computation.
   * @return an RDD of (threshold, F-Measure) pairs.
   * @see http://en.wikipedia.org/wiki/F1_score
   */
  def fMeasureByThreshold(beta: Double): RDD[(K,(Double, Double))] = createCurve(FMeasure(beta))

  /** Returns the (threshold, F-Measure) curve with beta = 1.0. */
  def fMeasureByThreshold(): RDD[(K,(Double, Double))] = fMeasureByThreshold(1.0)

  /** Returns the (threshold, precision) curve. */
  def precisionByThreshold(): RDD[(K,(Double, Double))] = createCurve(Precision)

  /** Returns the (threshold, recall) curve. */
  def recallByThreshold(): RDD[(K,(Double, Double))] = createCurve(Recall)

  def matthewsByThreshold(): RDD[(K,(Double,Double))] = createCurve(MatthewsCorrelationCoefficient)

  lazy val (
    cumulativeCounts: RDD[(K,(Double, BinaryLabelCounter))],
    confusions: RDD[(K,(Double, BinaryConfusionMatrix))]) = {
    // Create a bin for each distinct score value, count positives and negatives within each bin,
    // and then sort by score values in descending order.
    val counts = scoreAndLabels.map( x => ((x._1,x._2._1), x._2._2)).combineByKey(
      createCombiner = (label: Double) => new BinaryLabelCounter(0L, 0L) += label,
      mergeValue = (c: BinaryLabelCounter, label: Double) => c += label,
      mergeCombiners = (c1: BinaryLabelCounter, c2: BinaryLabelCounter) => c1 += c2
    ).sortBy(x => x._1._2, ascending = false)
    val aggs = counts.mapPartitions( x => {
      val c = new HashMap[K,BinaryLabelCounter]()
      x.foreach( y => {
        var o = c.getOrElse(y._1._1, new BinaryLabelCounter())
        o += y._2
        c.put(y._1._1, o)
      })
      Iterator(c.toMap)
    }).collect()

    val keys = scoreAndLabels.keys.distinct().collect()
    val partitionwiseCumulativeCounts = aggs.scanLeft(keys.map(
      x => (x, new BinaryLabelCounter()) ).toMap) (
      (agg: Map[K,BinaryLabelCounter], c:Map[K, BinaryLabelCounter]) => {
        keys.map( x => {
          var b = agg(x).clone()
          b += c.getOrElse(x, new BinaryLabelCounter())
          (x, b)
        } ).toMap
      }
    )
    val totalCounts = partitionwiseCumulativeCounts.last

    val cumulativeCounts = counts.mapPartitionsWithIndex(
      (index: Int, iter: Iterator[ ((K,Double), BinaryLabelCounter)]) => {
        val cumCount = partitionwiseCumulativeCounts(index)
        val out = iter.map { case ((key,score), c:BinaryLabelCounter) =>
          val a = cumCount(key)
          a += c
          (key, (score, a.clone()))
        }
        out
      }, preservesPartitioning = true)
    cumulativeCounts.persist()

    val confusions = cumulativeCounts.map { case (key, (score, cumCount)) =>
      (key, (
        score,
        BinaryConfusionMatrixImpl(cumCount, totalCounts(key)).asInstanceOf[BinaryConfusionMatrix]
        )
      )
    }
    (cumulativeCounts, confusions)
  }

  /** Creates a curve of (threshold, metric). */
  private def createCurve(y: BinaryClassificationMetricComputer): RDD[(K, (Double, Double))] = {
    confusions.map { case (k, (s, c)) =>
      (k, (s, y(c)))
    }
  }

  /** Creates a curve of (metricX, metricY). */
  private def createCurve(
                           x: BinaryClassificationMetricComputer,
                           y: BinaryClassificationMetricComputer): RDD[(K, (Double, Double))] = {
    confusions.map { case (k, (s, c)) =>
      (k, (x(c), y(c)))
    }
  }


}
