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

import java.util.{List => JList}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vector, Vectors, Matrix}
import org.apache.spark.mllib.clustering.GaussianMixtureModel

/**
  * Wrapper around GaussianMixtureModel to provide helper methods in Python
  */
private[python] class GaussianMixtureModelWrapper(model: GaussianMixtureModel) {
  val weights: Vector = Vectors.dense(model.weights)
  val k: Int = weights.size

  /**
    * Returns gaussians as a List of Vectors and Matrices corresponding each MultivariateGaussian
    */
  val gaussians: JList[Object] = {
    val modelGaussians = model.gaussians
    var i = 0
    var mu = ArrayBuffer.empty[Vector]
    var sigma = ArrayBuffer.empty[Matrix]
    while (i < k) {
      mu += modelGaussians(i).mu
      sigma += modelGaussians(i).sigma
      i += 1
    }
    List(mu.toArray, sigma.toArray).map(_.asInstanceOf[Object]).asJava
  }

  def save(sc: SparkContext, path: String): Unit = model.save(sc, path)
}
