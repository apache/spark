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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.Vector

@DeveloperApi
trait Statistics { //change trait to object
  val DEFAULT_CORR_METHOD = "pearson"
  val SUPPORTED_CORR_METHODS = Array("pearson", "kendall")

  def corr(X: RDD[Vector]): Matrix

  def corr(X: RDD[Vector], method: String): Matrix

  def corr(x: RDD[Double], y: RDD[Double], method: String): Matrix

  def corr(x: RDD[Double], y: RDD[Double]): Matrix

  //Use a Correlations class to instantiate the specific instance based on method value
  //Move default values to Correlations class (a "driver class").

  //Have an abstract class called Correlation that takes care of common behaviors
  // - for two column vectors, create a RDD[Vector] and use the implementation for computing the
  // matrix and vice versa, depending on which is more efficient.
  // - use dense vector when RDD[Double] passed in since we can't assume anything about missing values

}
