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
package org.apache.spark.sql.util

import java.{lang => jl}

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.util.AccumulatorV2

/**
 * An AccumulatorV2 counter for collecting a list of (mapper index, row count).
 *
 * @since 3.4.0
 */
class MapperRowCounter extends AccumulatorV2[jl.Long, java.util.List[(jl.Integer, jl.Long)]] {

  private var _agg: java.util.List[(jl.Integer, jl.Long)] = _

  private def getOrCreate = {
    _agg = Option(_agg).getOrElse(new java.util.ArrayList[(jl.Integer, jl.Long)]())
    _agg
  }

  /**
   * Returns false if this accumulator has had any values added to it or the sum is non-zero.
   */
  override def isZero: Boolean = this.synchronized(getOrCreate.isEmpty)

  override def copyAndReset(): MapperRowCounter = new MapperRowCounter

  override def copy(): MapperRowCounter = {
    val newAcc = new MapperRowCounter()
    this.synchronized {
      newAcc.getOrCreate.addAll(getOrCreate)
    }
    newAcc
  }

  override def reset(): Unit = {
    this.synchronized {
      _agg = null
    }
  }

  override def add(v: jl.Long): Unit = {
    this.synchronized {
      assert(getOrCreate.size() == 1, "agg must have been initialized")
      val p = getOrCreate.get(0)._1
      val n = getOrCreate.get(0)._2 + 1
      getOrCreate.set(0, (p, n))
    }
  }

  def setPartitionId(id: jl.Integer): Unit = {
    this.synchronized {
      assert(isZero, "agg must not have been initialized")
      getOrCreate.add((id, 0))
    }
  }

  override def merge(
      other: AccumulatorV2[jl.Long, java.util.List[(jl.Integer, jl.Long)]]): Unit
  = other match {
    case o: MapperRowCounter =>
      this.synchronized(getOrCreate.addAll(o.value))
    case _ =>
      throw new SparkUnsupportedOperationException(
        errorClass = "_LEGACY_ERROR_TEMP_3165",
        messageParameters = Map(
          "classA" -> this.getClass.getName,
          "classB" -> other.getClass.getName))
  }

  override def value: java.util.List[(jl.Integer, jl.Long)] = this.synchronized(getOrCreate)
}
