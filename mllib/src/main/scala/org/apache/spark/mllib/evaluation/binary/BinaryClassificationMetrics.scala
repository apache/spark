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

package org.apache.spark.mllib.evaluation.binary

/**
 * Trait for a binary classification evaluation metric.
 */
private[evaluation] trait BinaryClassificationMetric {
  def apply(c: BinaryConfusionMatrix): Double
}

/** Precision. */
private[evaluation] object Precision extends BinaryClassificationMetric {
  override def apply(c: BinaryConfusionMatrix): Double =
    c.tp.toDouble / (c.tp + c.fp)
}

/** False positive rate. */
private[evaluation] object FalsePositiveRate extends BinaryClassificationMetric {
  override def apply(c: BinaryConfusionMatrix): Double =
    c.fp.toDouble / c.n
}

/** Recall. */
private[evalution] object Recall extends BinaryClassificationMetric {
  override def apply(c: BinaryConfusionMatrix): Double =
    c.tp.toDouble / c.p
}

/**
 * F-Measure.
 * @param beta the beta constant in F-Measure
 * @see http://en.wikipedia.org/wiki/F1_score
 */
private[evaluation] case class FMeasure(beta: Double) extends BinaryClassificationMetric {
  private val beta2 = beta * beta
  override def apply(c: BinaryConfusionMatrix): Double = {
    val precision = Precision(c)
    val recall = Recall(c)
    (1.0 + beta2) * (precision * recall) / (beta2 * precision + recall)
  }
}
