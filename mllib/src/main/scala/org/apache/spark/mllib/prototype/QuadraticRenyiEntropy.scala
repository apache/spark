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
package org.apache.spark.mllib.prototype

import breeze.linalg.DenseVector
import org.apache.spark.Logging
import org.apache.spark.mllib.kernels.DensityKernel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
 * Implements the quadratic Renyi Entropy
 */
class QuadraticRenyiEntropy(dist: DensityKernel)
  extends EntropyMeasure
  with Serializable
  with Logging {

  val log_e = scala.math.log _
  val sqrt = scala.math.sqrt _
  override protected val density: DensityKernel = dist

  /**
   * Calculate the quadratic Renyi entropy
   * within a distribution specific
   * proportionality constant. This can
   * be used to compare the entropy values of
   * different sets of data on the same
   * distribution.
   *
   * @param data The data set whose entropy is
   *             required.
   * @return The entropy of the dataset assuming
   *         it is distributed as given by the value
   *         parameter 'density'.
   * */

  override def entropy[K](data: RDD[(K, LabeledPoint)]): Double = {
    val dim = data.first()._2.features.size
    val root_two: breeze.linalg.Vector[Double] = DenseVector.fill(dim, sqrt(2))
    -1*log_e(data.cartesian(data).map((couple) =>
      density.eval(Vectors.fromBreeze(couple._1._2.features.toBreeze :/ root_two -
        couple._2._2.features.toBreeze :/ root_two))).reduce((a,b) => a + b))
  }
}
