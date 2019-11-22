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
 * A counter for positives and negatives.
 *
 * @param weightedNumPositives weighted number of positive labels
 * @param weightedNumNegatives weighted number of negative labels
 */
private[evaluation] class BinaryLabelCounter(
    var weightedNumPositives: Double = 0.0,
    var weightedNumNegatives: Double = 0.0) extends Serializable {

  /** Processes a label. */
  def +=(label: Double): BinaryLabelCounter = {
    // Though we assume 1.0 for positive and 0.0 for negative, the following check will handle
    // -1.0 for negative as well.
    if (label > 0.5) weightedNumPositives += 1.0 else weightedNumNegatives += 1.0
    this
  }

  /** Processes a label with a weight. */
  def +=(label: Double, weight: Double): BinaryLabelCounter = {
    // Though we assume 1.0 for positive and 0.0 for negative, the following check will handle
    // -1.0 for negative as well.
    if (label > 0.5) weightedNumPositives += weight else weightedNumNegatives += weight
    this
  }

  /** Merges another counter. */
  def +=(other: BinaryLabelCounter): BinaryLabelCounter = {
    weightedNumPositives += other.weightedNumPositives
    weightedNumNegatives += other.weightedNumNegatives
    this
  }

  override def clone: BinaryLabelCounter = {
    new BinaryLabelCounter(weightedNumPositives, weightedNumNegatives)
  }

  override def toString: String = s"{numPos: $weightedNumPositives, numNeg: $weightedNumNegatives}"
}
