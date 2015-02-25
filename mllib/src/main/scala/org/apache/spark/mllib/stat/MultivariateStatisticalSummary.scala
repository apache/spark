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

package org.apache.spark.mllib.stat

import org.apache.spark.mllib.linalg.Vector

/**
 * Trait for multivariate statistical summary of a data matrix.
 */
trait MultivariateStatisticalSummary {

  /**
   * Sample mean vector.
   */
  def mean: Vector

  /**
   * Sample variance vector. Should return a zero vector if the sample size is 1.
   */
  def variance: Vector

  /**
   * Sample size.
   */
  def count: Long

  /**
   * Number of nonzero elements (including explicitly presented zero values) in each column.
   */
  def numNonzeros: Vector

  /**
   * Maximum value of each column.
   */
  def max: Vector

  /**
   * Minimum value of each column.
   */
  def min: Vector

  /**
   * Euclidean magnitude of each column
   */
  def normL2: Vector

  /**
   * L1 norm of each column
   */
  def normL1: Vector
}
