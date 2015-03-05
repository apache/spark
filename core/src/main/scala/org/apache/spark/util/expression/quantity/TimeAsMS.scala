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
package org.apache.spark.util.expression.quantity

/**
 * Utility class used for handling time quantities and converting between various units
 * @param baseValue The number of milliseconds
 */
case class TimeAsMS(baseValue: Double) {
  def this(baseValue: Double, unitOfScale: String) =
    this( baseValue * TimeAsMS.quantityScale(unitOfScale))

  override def hashCode = baseValue.hashCode

  /**
   * Given our base unit is a ms, two TimeQuantities will be considered equal if they
   * have the same number of integral ms
   */
  override def equals(other: Any) = other match {
    case that: TimeAsMS => math.round(this.baseValue) == math.round(that.baseValue)
    case _ => false
  }

  def toMs = TimeAsMS.toMs(this)
  def toSecs = TimeAsMS.toSecs(this)
  def toMins = TimeAsMS.toMins(this)
  def toHours = TimeAsMS.toHours(this)
  def toDays = TimeAsMS.toDays(this)
  def toWeeks = TimeAsMS.toWeeks(this)
}

object TimeAsMS {
  val secMs = 1000.0
  val minMs = 60 * secMs
  val hourMs = 60 * minMs
  val dayMs = 24 * hourMs
  val weekMS = 7 * dayMs

  val quantityScale: Map[String, Double] = Map(
    "ms" -> 1,
    "s" -> secMs, "sec" -> secMs, "second" -> secMs, "secs" -> secMs, "seconds" -> secMs,
    "m" -> minMs, "min" -> minMs, "minute" -> minMs, "mins" -> minMs, "minutes" -> minMs,
    "h" -> hourMs,"hour" -> hourMs, "hours" -> hourMs,
    "d" -> dayMs, "day" -> dayMs, "days" -> dayMs,
    "w" -> weekMS, "week" -> weekMS, "weeks" -> weekMS)

  def apply(baseValue: Double, unitOfScale: String) =
    new TimeAsMS(baseValue,unitOfScale.toLowerCase)

  def toMs(tq: TimeAsMS) = Math.round(tq.baseValue)
  def toSecs(tq: TimeAsMS) = tq.baseValue / quantityScale("s")
  def toMins(tq: TimeAsMS) = tq.baseValue / quantityScale("m")
  def toHours(tq: TimeAsMS) = tq.baseValue / quantityScale("h")
  def toDays(tq: TimeAsMS) = tq.baseValue / quantityScale("d")
  def toWeeks(tq: TimeAsMS) = tq.baseValue / quantityScale("w")
}