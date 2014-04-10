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
 * Trait for a binary classification evaluation metric computer.
 */
private[evaluation] trait BinaryClassificationMetricComputer extends Serializable {
  def apply(c: BinaryConfusionMatrix): Double
}

/** Precision. */
private[evaluation] object Precision extends BinaryClassificationMetricComputer {
  override def apply(c: BinaryConfusionMatrix): Double =
    c.numTruePositives.toDouble / (c.numTruePositives + c.numFalsePositives)
}

/** False positive rate. */
private[evaluation] object FalsePositiveRate extends BinaryClassificationMetricComputer {
  override def apply(c: BinaryConfusionMatrix): Double =
    c.numFalsePositives.toDouble / c.numNegatives
}

/** Recall. */
private[evaluation] object Recall extends BinaryClassificationMetricComputer {
  override def apply(c: BinaryConfusionMatrix): Double =
    c.numTruePositives.toDouble / c.numPositives
}

/**
 * F-Measure.
 * @param beta the beta constant in F-Measure
 * @see http://en.wikipedia.org/wiki/F1_score
 */
private[evaluation] case class FMeasure(beta: Double) extends BinaryClassificationMetricComputer {
  private val beta2 = beta * beta
  override def apply(c: BinaryConfusionMatrix): Double = {
    val precision = Precision(c)
    val recall = Recall(c)
    (1.0 + beta2) * (precision * recall) / (beta2 * precision + recall)
  }
}
