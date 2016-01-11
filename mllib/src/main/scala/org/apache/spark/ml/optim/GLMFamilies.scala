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

package org.apache.spark.ml.optim

import org.apache.spark.rdd.RDD

/**
 * A description of the error distribution and link function to be used in the model.
 * @param link a link function instance
 */
private[ml] abstract class Family(val link: Link) extends Serializable {

  /**
   * Starting value for mu in the IRLS algorithm.
   */
  def startingMu(y: Double, yMean: Double): Double = (y + yMean) / 2.0

  /**
   * Deviance of (y, mu) pair.
   * Deviance is usually defined as twice the loglikelihood ratio.
   */
  def deviance(y: RDD[Double], mu: RDD[Double]): Double

  /** Weights for IRLS steps. */
  def weights(mu: Double): Double

  /** The adjusted response variable. */
  def adjusted(y: Double, mu: Double, eta: Double): Double = {
    eta + (y - mu) * link.deriv(mu)
  }

  /** Linear predictors based on given mu. */
  def predict(mu: Double): Double = this.link.link(mu)

  /** Fitted values based on linear predictors eta. */
  def fitted(eta: Double): Double = this.link.unlink(eta)
}

/**
 * Binomial exponential family distribution.
 * The default link for the Binomial family is the logit link.
 * @param link a link function instance
 */
private[ml] class Binomial(link: Link = Logit) extends Family(link) {

  override def startingMu(y: Double, yMean: Double): Double = (y + 0.5) / 2.0

  override def deviance(y: RDD[Double], mu: RDD[Double]): Double = {
    mu.zip(y).map { case (mu, y) =>
      val my = 1.0 - y
      y * math.log(math.max(y, 1.0) / mu) +
        my * math.log(math.max(my, 1.0) / (1.0 - mu))
    }.sum() * 2
  }

  override def weights(mu: Double): Double = {
    mu * (1 - mu)
  }
}

/**
 * Poisson exponential family.
 * The default link for the Poisson family is the log link.
 * @param link a link function instance
 */
private[ml] class Poisson(link: Link = Log) extends Family(link) {

  override def deviance(y: RDD[Double], mu: RDD[Double]): Double = {
    mu.zip(y).map { case (mu, y) =>
      y * math.log(math.max(y, 1.0) / mu) - (y - mu)
    }.sum() * 2
  }

  override def weights(mu: Double): Double = mu
}

/**
 * A description of the link function to be used in the model.
 */
private[ml] trait Link extends Serializable {

  /** The link function. */
  def link(mu: Double): Double

  /** Derivative function. */
  def deriv(mu: Double): Double

  /** Inverse link function. */
  def unlink(eta: Double): Double
}

private[ml] object Logit extends Link {

  override def link(mu: Double): Double = math.log(mu / (1.0 - mu))

  override def deriv(mu: Double): Double = 1.0 / (mu * (1.0 - mu))

  override def unlink(eta: Double): Double = 1.0 / (1.0 + math.exp(-1.0 * eta))
}

private[ml] object Log extends Link {

  override def link(mu: Double): Double = math.log(mu)

  override def deriv(mu: Double): Double = 1.0 / mu

  override def unlink(eta: Double): Double = math.exp(eta)
}
