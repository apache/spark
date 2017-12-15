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

import scala.annotation.varargs
import scala.collection.mutable
import org.apache.spark.annotation.Since
import org.apache.spark.ml.param._

import scala.collection.mutable.ArrayBuffer

/**
 * Builder for a param grid used in grid search-based model selection.
 */
@Since("1.2.0")
class ParamGridBuilder {
// class ParamGridBuilder @Since("1.2.0") { TODO

  private val paramGrid = mutable.Map.empty[Param[_], Iterable[_]]

  /**
   * Sets the given parameters in this grid to fixed values.
   */
  @Since("1.2.0")
  def baseOn(paramMap: ParamMap): this.type = {
    baseOn(paramMap.toSeq: _*)
    this
  }

  /**
   * Sets the given parameters in this grid to fixed values.
   */
  @Since("1.2.0")
  @varargs
  def baseOn(paramPairs: ParamPair[_]*): this.type = {
    paramPairs.foreach { p =>
      addGrid(p.param.asInstanceOf[Param[Any]], Seq(p.value))
    }
    this
  }

  /**
   * Adds a param with multiple values (overwrites if the input param exists).
   */
  @Since("1.2.0")
  def addGrid[T](param: Param[T], values: Iterable[T]): this.type = {
    paramGrid.put(param, values)
    this
  }

  // specialized versions of addGrid for Java.

  /**
   * Adds a double param with multiple values.
   */
  @Since("1.2.0")
  def addGrid(param: DoubleParam, values: Array[Double]): this.type = {
    addGrid[Double](param, values)
  }

  /**
   * Adds an int param with multiple values.
   */
  @Since("1.2.0")
  def addGrid(param: IntParam, values: Array[Int]): this.type = {
    addGrid[Int](param, values)
  }

  /**
   * Adds a float param with multiple values.
   */
  @Since("1.2.0")
  def addGrid(param: FloatParam, values: Array[Float]): this.type = {
    addGrid[Float](param, values)
  }

  /**
   * Adds a long param with multiple values.
   */
  @Since("1.2.0")
  def addGrid(param: LongParam, values: Array[Long]): this.type = {
    addGrid[Long](param, values)
  }

  /**
   * Adds a boolean param with true and false.
   */
  @Since("1.2.0")
  def addGrid(param: BooleanParam): this.type = {
    addGrid[Boolean](param, Array(true, false))
  }

  /**
   * Builds and returns all combinations of parameters specified by the param grid.
   */
  @Since("1.2.0")
  def build(): Array[ParamMap] = {
    var paramMaps = Array(new ParamMap)
    paramGrid.foreach { case (param, values) =>
      val newParamMaps = values.flatMap { v =>
        paramMaps.map(_.copy.put(param.asInstanceOf[Param[Any]], v))
      }
      paramMaps = newParamMaps.toArray
    }
    paramMaps
  }
}

/**
  *
  */
object ParamGridBuilder {

  /**
    *
    * @param paramMaps
    * @param params
    * @return
    */
  def splitOnParams(paramMaps: Array[ParamMap], params: Array[Param[_]]): (Array[ParamMap], Array[ParamMap])  = {
    val leftValues = mutable.Map.empty[Param[_], mutable.LinkedHashSet[Any]]
    val rightValues = mutable.Map.empty[Param[_], mutable.LinkedHashSet[Any]]

    paramMaps.foreach { paramMap =>
      paramMap.toSeq.foreach { case ParamPair(p, v) =>
        if (params.contains(p)) {
          leftValues.getOrElseUpdate(p, mutable.LinkedHashSet.empty[Any]) += v
        } else {
          rightValues.getOrElseUpdate(p, mutable.LinkedHashSet.empty[Any]) += v
        }
      }
    }

    val leftGrid = new ParamGridBuilder
    val rightGrid = new ParamGridBuilder

    leftValues.foreach { case (param, values) =>
      leftGrid.addGrid(param.asInstanceOf[Param[Any]], values.toArray)
    }

    rightValues.foreach { case (param, values) =>
      rightGrid.addGrid(param.asInstanceOf[Param[Any]], values.toArray)
    }

    (leftGrid.build(), rightGrid.build())
  }
}
