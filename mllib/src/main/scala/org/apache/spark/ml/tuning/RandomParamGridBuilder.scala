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

package org.apache.spark.ml.tuning

import scala.collection.mutable

import org.apache.spark.ml.param._


/**
 * Builder for creating parameter grids for use in random search based model selection.
 */
class RandomParamGridBuilder(seed: Option[Long] = None) {

  val rng = seed match {
    case Some(seed) => new scala.util.Random(seed)
    case _ => new scala.util.Random()
  }

  private val distributionGrid = mutable.Map.empty[Param[_], () => _]

  /**
    * Add a function where each time it is called, returns a value from the intended distribution.
    */
  def addDistribution[T](param: Param[T], distribution: () => T): this.type = {
    distributionGrid.put(param, distribution)
    this
  }

  /**
    * Add a numeric parameter with lower and upper bounds.
    * Values used are sampled between these bounds (inclusive) using a uniform distribution.
    */
  def addUniformDistribution[T](param: Param[T], start: T, end: T): this.type = {
    (param, start, end) match {
      case (p: Param[Int], s: Int, e: Int) =>
        addDistribution[Int](p, uniformIntDistribution(s, e))
      case (p: Param[Long], s: Long, e: Long) =>
        addDistribution[Long](p, uniformLongDistribution(s, e))
      case (p: Param[Float], s: Float, e: Float) =>
        addDistribution[Float](p, uniformFloatDistribution(s, e))
      case (p: Param[Double], s: Double, e: Double) =>
        addDistribution[Double](p, uniformDoubleDistribution(s, e))
      case _ => throw new IllegalArgumentException(
        "Unsupported type provided, only compatible with Int, Long, Float and Double")
    }
    this
  }

  /**
    * Adds a parameter, where the values sampled are equally likely to be any entry within
    * the values array.
    */
  def addUniformChoice[T](param: Param[T], values: Array[T]): this.type = {
    if (values.length == 0) {
      throw new IllegalArgumentException("Choice array must have at least one element")
    }

    def choice(): T = {
      values(rng.nextInt(values.length))
    }
    addDistribution[T](param, choice)
  }

  /**
    * Adds a Int param, with start and end ranges.
    * Values are sampled from a uniform distribution within these limits
    * (inclusive of start and end).
    */
  def addUniformDistribution(param: IntParam, start: Int, end: Int): this.type = {
    addUniformDistribution[Int](param, start, end)
  }

  /**
    * Adds a Long param, with start and end ranges.
    * Values are sampled from a uniform distribution within these limits
    * (inclusive of start and end).
    */
  def addUniformDistribution(param: LongParam, start: Long, end: Long): this.type = {
    addUniformDistribution[Long](param, start, end)
  }

  /**
    * Adds a Float param, with start and end ranges.
    * Values are sampled from a uniform distribution within these limits.
    */
  def addUniformDistribution(param: FloatParam, start: Float, end: Float): this.type = {
    addUniformDistribution[Float](param, start, end)
  }

  /**
    * Adds a Double param, with start and end ranges.
    * Values are sampled from a uniform distribution within these limits.
    */
  def addUniformDistribution(param: DoubleParam, start: Double, end: Double): this.type = {
    addUniformDistribution[Double](param, start, end)
  }

  private [tuning] def uniformIntDistribution(start: Int, end: Int): () => Int = {
    def distribution(): Int = start + rng.nextInt(end - start + 1)
    distribution
  }

  private [tuning] def uniformLongDistribution(start: Long, end: Long): () => Long = {
    def distribution(): Long = {
      val zeroToOne = rng.nextDouble
      (start + (zeroToOne * (end - start))).round
    }
    distribution
  }

  private [tuning] def uniformFloatDistribution(start: Float, end: Float): () => Float = {
    def distribution(): Float = {
      val zeroToOne = rng.nextFloat
      start + (end - start) * zeroToOne
    }
    distribution
  }

  private [tuning] def uniformDoubleDistribution(start: Double, end: Double): () => Double = {
    def distribution() = {
      val zeroToOne = rng.nextDouble
      start + (end - start) * zeroToOne
    }
    distribution
  }

  /**
    * Adds a boolean param with true and false, will be equally likely to sample true or false.
    */
  def addUniformDistribution(param: BooleanParam): this.type = {
    addDistribution(param, () => rng.nextBoolean())
  }

  /**
    * Adds a boolean param with true and false, will be equally likely to sample true or false.
    */
  def addUniformDistribution(param: Param[Boolean]): this.type = {
    addDistribution(param, () => rng.nextBoolean())
  }

  /**
    * Builds and returns nIterations grids of randomly selected parameters.
    */
  def build(nIterations: Int): Array[ParamMap] = {

    val paramMaps = (0 until nIterations).map { _ =>
      val maps = distributionGrid.map { case (param, distribution) =>
        new ParamMap().put(param.asInstanceOf[Param[Any]], distribution())
      }
      maps.reduce(_ ++ _)
    }.toArray

    paramMaps
  }
}