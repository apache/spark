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

package org.apache.spark.mllib.api.python

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.GaussianMixtureModel
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.util.ArrayImplicits._

/**
 * Wrapper around GaussianMixtureModel to provide helper methods in Python
 */
private[python] class GaussianMixtureModelWrapper(model: GaussianMixtureModel) {
  val weights: Vector = Vectors.dense(model.weights)
  val k: Int = weights.size

  /**
   * Returns gaussians as a List of Vectors and Matrices corresponding each MultivariateGaussian
   */
  val gaussians: Array[Byte] = {
    val modelGaussians = model.gaussians.map { gaussian =>
      Array[Any](gaussian.mu, gaussian.sigma)
    }
    SerDe.dumps(modelGaussians.toImmutableArraySeq.asJava)
  }

  def predictSoft(point: Vector): Vector = {
    Vectors.dense(model.predictSoft(point))
  }

  def save(sc: SparkContext, path: String): Unit = model.save(sc, path)
}
