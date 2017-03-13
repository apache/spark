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
 * @param baseValue The Number of seconds
 */
private[spark] case class TimeAsSeconds(baseValue: Double) {
  def this(baseValue: Double, unitOfScale: String) =
    this( baseValue * TimeAsSeconds.quantityScale(unitOfScale.toLowerCase))

  override def hashCode = baseValue.hashCode

  /**
   * Given our base unit is a Second, two TimeQuantities will be considered equal if they
   * have the same number of integral second
   */
  override def equals(other: Any) = other match {
    case that: TimeAsSeconds => math.round(this.baseValue) == math.round(that.baseValue)
    case _ => false
  }

  def toMs = TimeAsSeconds.toMs(this)
  def toSecs = TimeAsSeconds.toSecs(this)
  def toMins = TimeAsSeconds.toMins(this)
  def toHours = TimeAsSeconds.toHours(this)
  def toDays = TimeAsSeconds.toDays(this)
  def toWeeks = TimeAsSeconds.toWeeks(this)
}

object TimeAsSeconds  {
  val minSec = 60
  val hourSec = 60 * minSec
  val daySec = 24 * hourSec
  val weekSec = 7 * daySec

  val quantityScale: Map[String, Double] = Map(
    "ms" -> 0.001,
    "s" -> 1, "sec" -> 1, "second" -> 1, "secs" -> 1, "seconds" -> 1,
    "m" -> minSec, "min" -> minSec, "minute" -> minSec, "mins" -> minSec, "minutes" -> minSec,
    "h" -> hourSec, "hour" -> hourSec, "hours" -> hourSec,
    "d" -> daySec, "day" -> daySec, "days" -> daySec,
    "w" -> weekSec, "week" -> weekSec, "weeks" -> weekSec)

  def apply(baseValue: Double, unitOfScale: String) =
    new TimeAsSeconds(baseValue, unitOfScale.toLowerCase)

  def toMs(tq: TimeAsSeconds) = Math.round(tq.baseValue * 1000)

  def toSecs(tq: TimeAsSeconds) = tq.baseValue

  def toMins(tq: TimeAsSeconds) = tq.baseValue / quantityScale("m")

  def toHours(tq: TimeAsSeconds) = tq.baseValue / quantityScale("h")

  def toDays(tq: TimeAsSeconds) = tq.baseValue / quantityScale("d")

  def toWeeks(tq: TimeAsSeconds) = tq.baseValue / quantityScale("w")
}
