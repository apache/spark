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
 * Trait for a binary confusion matrix.
 */
private[evaluation] trait BinaryConfusionMatrix {
  /** number of true positives */
  def numTruePositives: Long

  /** number of false positives */
  def numFalsePositives: Long

  /** number of false negatives */
  def numFalseNegatives: Long

  /** number of true negatives */
  def numTrueNegatives: Long

  /** number of positives */
  def numPositives: Long = numTruePositives + numFalseNegatives

  /** number of negatives */
  def numNegatives: Long = numFalsePositives + numTrueNegatives
}

/**
 * Implementation of [[org.apache.spark.mllib.evaluation.binary.BinaryConfusionMatrix]].
 *
 * @param count label counter for labels with scores greater than or equal to the current score
 * @param totalCount label counter for all labels
 */
private[evaluation] case class BinaryConfusionMatrixImpl(
    count: BinaryLabelCounter,
    totalCount: BinaryLabelCounter) extends BinaryConfusionMatrix {

  /** number of true positives */
  override def numTruePositives: Long = count.numPositives

  /** number of false positives */
  override def numFalsePositives: Long = count.numNegatives

  /** number of false negatives */
  override def numFalseNegatives: Long = totalCount.numPositives - count.numPositives

  /** number of true negatives */
  override def numTrueNegatives: Long = totalCount.numNegatives - count.numNegatives

  /** number of positives */
  override def numPositives: Long = totalCount.numPositives

  /** number of negatives */
  override def numNegatives: Long = totalCount.numNegatives
}
