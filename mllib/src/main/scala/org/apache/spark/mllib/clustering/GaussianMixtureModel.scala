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

package org.apache.spark.mllib.clustering

import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.Vector

/**
 * Multivariate Gaussian mixture model consisting of k Gaussians, where points are drawn 
 * from each Gaussian i=1..k with probability w(i); mu(i) and sigma(i) are the respective 
 * mean and covariance for each Gaussian distribution i=1..k. 
 */
class GaussianMixtureModel(
  val w: Array[Double], 
  val mu: Array[Vector], 
  val sigma: Array[Matrix]) extends Serializable {
  
  /** Number of gaussians in mixture */
  def k: Int = w.length;
}
