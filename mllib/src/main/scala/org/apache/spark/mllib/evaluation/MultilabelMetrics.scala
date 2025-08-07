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

import scala.collection.mutable

import org.apache.spark.annotation.Since
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
 * Evaluator for multilabel classification.
 * @param predictionAndLabels an RDD of (predictions, labels) pairs,
 * both are non-null Arrays, each with unique elements.
 */
@Since("1.2.0")
class MultilabelMetrics @Since("1.2.0") (predictionAndLabels: RDD[(Array[Double], Array[Double])]) {

  /**
   * An auxiliary constructor taking a DataFrame.
   * @param predictionAndLabels a DataFrame with two double array columns: prediction and label
   */
  private[mllib] def this(predictionAndLabels: DataFrame) =
    this(predictionAndLabels.rdd.map { r =>
      (r.getSeq[Double](0).toArray, r.getSeq[Double](1).toArray)
    })

  /**
   * Use MultilabelSummarizer to calculate all summary statistics of predictions
   * and labels on one pass.
   */
  private val summary: MultilabelSummarizer = {
    predictionAndLabels.treeAggregate[MultilabelSummarizer](
      zeroValue = new MultilabelSummarizer,
      seqOp = (summary: MultilabelSummarizer,
               sample: (Array[Double], Array[Double])) => summary.add(sample._1, sample._2),
      combOp = (sum1: MultilabelSummarizer, sum2: MultilabelSummarizer) => sum1.merge(sum2),
      depth = 2,
      finalAggregateOnExecutor = true)
  }


  /**
   * Returns subset accuracy
   * (for equal sets of labels)
   */
  @Since("1.2.0")
  val subsetAccuracy: Double = summary.subsetAccuracy

  /**
   * Returns accuracy
   */
  @Since("1.2.0")
  val accuracy: Double = summary.accuracy


  /**
   * Returns Hamming-loss
   */
  @Since("1.2.0")
  val hammingLoss: Double = summary.hammingLoss

  /**
   * Returns document-based precision averaged by the number of documents
   */
  @Since("1.2.0")
  val precision: Double = summary.precision

  /**
   * Returns document-based recall averaged by the number of documents
   */
  @Since("1.2.0")
  val recall: Double = summary.recall

  /**
   * Returns document-based f1-measure averaged by the number of documents
   */
  @Since("1.2.0")
  val f1Measure: Double = summary.f1Measure

  /**
   * Returns precision for a given label (category)
   * @param label the label.
   */
  @Since("1.2.0")
  def precision(label: Double): Double = {
    val tp = summary.tpPerClass(label)
    val fp = summary.fpPerClass.getOrElse(label, 0L)
    if (tp + fp == 0) 0.0 else tp.toDouble / (tp + fp)
  }

  /**
   * Returns recall for a given label (category)
   * @param label the label.
   */
  @Since("1.2.0")
  def recall(label: Double): Double = {
    val tp = summary.tpPerClass(label)
    val fn = summary.fnPerClass.getOrElse(label, 0L)
    if (tp + fn == 0) 0.0 else tp.toDouble / (tp + fn)
  }

  /**
   * Returns f1-measure for a given label (category)
   * @param label the label.
   */
  @Since("1.2.0")
  def f1Measure(label: Double): Double = {
    val p = precision(label)
    val r = recall(label)
    if ((p + r) == 0) 0.0 else 2 * p * r / (p + r)
  }

  private lazy val sumTp = summary.tpPerClass.values.sum
  private lazy val sumFpClass = summary.fpPerClass.values.sum
  private lazy val sumFnClass = summary.fnPerClass.values.sum

  /**
   * Returns micro-averaged label-based precision
   * (equals to micro-averaged document-based precision)
   */
  @Since("1.2.0")
  lazy val microPrecision: Double = sumTp.toDouble / (sumTp + sumFpClass)

  /**
   * Returns micro-averaged label-based recall
   * (equals to micro-averaged document-based recall)
   */
  @Since("1.2.0")
  lazy val microRecall: Double = sumTp.toDouble / (sumTp + sumFnClass)

  /**
   * Returns micro-averaged label-based f1-measure
   * (equals to micro-averaged document-based f1-measure)
   */
  @Since("1.2.0")
  lazy val microF1Measure: Double = 2.0 * sumTp / (2 * sumTp + sumFnClass + sumFpClass)

  /**
   * Returns the sequence of labels in ascending order
   */
  @Since("1.2.0")
  lazy val labels: Array[Double] = summary.tpPerClass.keys.toArray.sorted
}


private[evaluation] class MultilabelSummarizer extends Serializable {

  private var docCnt = 0L
  private val labelSet = mutable.Set.empty[Double]
  private var subsetAccuracyCnt = 0L
  private var accuracySum = 0.0
  private var hammingLossSum = 0L
  private var precisionSum = 0.0
  private var recallSum = 0.0
  private var f1MeasureSum = 0.0
  val tpPerClass = mutable.Map.empty[Double, Long]
  val fpPerClass = mutable.Map.empty[Double, Long]
  val fnPerClass = mutable.Map.empty[Double, Long]

  /**
   * Add a new sample (predictions and labels) to this summarizer, and update
   * the statistical summary.
   *
   * @return This MultilabelSummarizer object.
   */
  def add(predictions: Array[Double], labels: Array[Double]): this.type = {
    val intersection = predictions.intersect(labels)

    docCnt += 1L

    labelSet ++= labels

    if (java.util.Arrays.equals(predictions, labels)) {
      subsetAccuracyCnt += 1
    }

    accuracySum += intersection.length.toDouble /
      (labels.length + predictions.length - intersection.length)

    hammingLossSum += labels.length + predictions.length - 2 * intersection.length

    if (predictions.length > 0) {
      precisionSum += intersection.length.toDouble / predictions.length
    }

    recallSum += intersection.length.toDouble / labels.length

    f1MeasureSum += 2.0 * intersection.length / (predictions.length + labels.length)

    intersection.foreach { k =>
      val v = tpPerClass.getOrElse(k, 0L)
      tpPerClass.update(k, v + 1)
    }

    predictions.diff(labels).foreach { k =>
      val v = fpPerClass.getOrElse(k, 0L)
      fpPerClass.update(k, v + 1)
    }

    labels.diff(predictions).foreach { k =>
      val v = fnPerClass.getOrElse(k, 0L)
      fnPerClass.update(k, v + 1)
    }

    this
  }

  /**
   * Merge another MultilabelSummarizer, and update the statistical summary.
   * (Note that it's in place merging; as a result, `this` object will be modified.)
   *
   * @param other The other MultilabelSummarizer to be merged.
   * @return This MultilabelSummarizer object.
   */
  def merge(other: MultilabelSummarizer): this.type = {
    if (other.docCnt > 0) {
      docCnt += other.docCnt

      labelSet ++= other.labelSet

      subsetAccuracyCnt += other.subsetAccuracyCnt

      accuracySum += other.accuracySum

      hammingLossSum += other.hammingLossSum

      precisionSum += other.precisionSum

      recallSum += other.recallSum

      f1MeasureSum += other.f1MeasureSum

      other.tpPerClass.foreach { case (k, v1) =>
        val v0 = tpPerClass.getOrElse(k, 0L)
        tpPerClass.update(k, v0 + v1)
      }

      other.fpPerClass.foreach { case (k, v1) =>
        val v0 = fpPerClass.getOrElse(k, 0L)
        fpPerClass.update(k, v0 + v1)
      }

      other.fnPerClass.foreach { case (k, v1) =>
        val v0 = fnPerClass.getOrElse(k, 0L)
        fnPerClass.update(k, v0 + v1)
      }
    }

    this
  }

  def numDocs: Long = docCnt

  def numLabels: Long = labelSet.size.toLong

  def subsetAccuracy: Double = subsetAccuracyCnt.toDouble / numDocs

  def accuracy: Double = accuracySum / numDocs

  def hammingLoss: Double = hammingLossSum.toDouble / numDocs / numLabels

  def precision: Double = precisionSum / numDocs

  def recall: Double = recallSum / numDocs

  def f1Measure: Double = f1MeasureSum / numDocs
}
