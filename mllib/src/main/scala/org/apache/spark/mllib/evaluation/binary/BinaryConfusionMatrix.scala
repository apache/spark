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
