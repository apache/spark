package org.apache.spark.mllib.evaluation

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._


class MultilabelMetrics(predictionAndLabels:RDD[(Set[Double], Set[Double])]) extends Logging{

  private lazy val numDocs = predictionAndLabels.count()

  lazy val macroPrecisionDoc = (predictionAndLabels.map{ case(predictions, labels) =>
    if (predictions.size >0)
      predictions.intersect(labels).size.toDouble / predictions.size else 0}.fold(0.0)(_ + _)) / numDocs

  lazy val macroRecallDoc = (predictionAndLabels.map{ case(predictions, labels) =>
    predictions.intersect(labels).size.toDouble / labels.size}.fold(0.0)(_ + _)) / numDocs

  lazy val microPrecisionDoc = {
    val (sumTp, sumPredictions) = predictionAndLabels.map{ case(predictions, labels) =>
      (predictions.intersect(labels).size, predictions.size)}.
      fold((0, 0)){ case((tp1, predictions1), (tp2, predictions2)) =>
      (tp1 + tp2, predictions1 + predictions2)}
    sumTp.toDouble / sumPredictions
  }

  lazy val microRecallDoc = {
    val (sumTp, sumLabels) = predictionAndLabels.map{ case(predictions, labels) =>
      (predictions.intersect(labels).size, labels.size)}.
      fold((0, 0)){ case((tp1, labels1), (tp2, labels2)) =>
      (tp1 + tp2, labels1 + labels2)}
    sumTp.toDouble / sumLabels
  }

  private lazy val tpPerClass = predictionAndLabels.flatMap{ case(predictions, labels) =>
    predictions.intersect(labels).map(category => (category, 1))}.reduceByKey(_ + _).collectAsMap()

  private lazy val fpPerClass = predictionAndLabels.flatMap{ case(predictions, labels) =>
    predictions.diff(labels).map(category => (category, 1))}.reduceByKey(_ + _).collectAsMap()

  private lazy val fnPerClass = predictionAndLabels.flatMap{ case(predictions, labels) =>
    labels.diff(predictions).map(category => (category, 1))}.reduceByKey(_ + _).collectAsMap()

  def precisionClass(label: Double) = if((tpPerClass(label) + fpPerClass.getOrElse(label, 0)) == 0) 0 else
    tpPerClass(label).toDouble / (tpPerClass(label) + fpPerClass.getOrElse(label, 0))

  def recallClass(label: Double) = if((tpPerClass(label) + fnPerClass.getOrElse(label, 0)) == 0) 0 else
    tpPerClass(label).toDouble / (tpPerClass(label) + fnPerClass.getOrElse(label, 0))

  def f1MeasureClass(label: Double) = {
    val precision = precisionClass(label)
    val recall = recallClass(label)
    if((precision + recall) == 0) 0 else 2 * precision * recall / (precision + recall)
  }

  private lazy val sumTp = tpPerClass.foldLeft(0L){ case(sumTp, (_, tp)) => sumTp + tp}

  lazy val microPrecisionClass = {
    val sumFp = fpPerClass.foldLeft(0L){ case(sumFp, (_, fp)) => sumFp + fp}
    sumTp.toDouble / (sumTp + sumFp)
  }

  lazy val microRecallClass = {
    val sumFn = fnPerClass.foldLeft(0.0){ case(sumFn, (_, fn)) => sumFn + fn}
    sumTp.toDouble / (sumTp + sumFn)
  }

  lazy val microF1MeasureClass = {
    val precision = microPrecisionClass
    val recall = microRecallClass
    if((precision + recall) == 0) 0 else 2 * precision * recall / (precision + recall)
  }

}
