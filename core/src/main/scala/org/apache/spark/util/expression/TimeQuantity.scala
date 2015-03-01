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
package org.apache.spark.util.expression

case class TimeQuantity(baseValue: Double) {
  def this(baseValue: Double, unitOfScale: String) =
    this( baseValue * TimeQuantity.quantityScale(unitOfScale))

  override def hashCode = baseValue.hashCode

  override def equals(other: Any) = other match {
    case that: TimeQuantity => math.abs(this.baseValue - that.baseValue) < 1
    case _ => false
  }

  def toMs = TimeQuantity.toMs(this)
  def toSec = TimeQuantity.toSec(this)
  def toMin = TimeQuantity.toMin(this)
  def toHour = TimeQuantity.toHour(this)
  def toDay = TimeQuantity.toDay(this)
}

object TimeQuantity {
  val secMs = 1000.0
  val minMs = 60 * secMs
  val hourMs = 60 * minMs
  val dayMs = 24 * hourMs
  val quantityScale: Map[String, Double] = Map(
    "ms" -> 1,
    "s" -> secMs, "sec" -> secMs, "second" -> secMs, "secs" -> secMs, "seconds" -> secMs,
    "m" -> minMs, "min" -> minMs, "minute" -> minMs, "mins" -> minMs, "minutes" -> minMs,
    "h" -> hourMs,"hour" -> hourMs, "hours" -> hourMs,
    "d" -> dayMs, "day" -> dayMs)

  def apply(baseValue: Double, unitOfScale: String) = new TimeQuantity(baseValue,unitOfScale.toLowerCase)

  def toMs(tq: TimeQuantity) = Math.round(tq.baseValue)
  def toSec(tq: TimeQuantity) = tq.baseValue / quantityScale("s")
  def toMin(tq: TimeQuantity) = tq.baseValue / quantityScale("m")
  def toHour(tq: TimeQuantity) = tq.baseValue / quantityScale("h")
  def toDay(tq: TimeQuantity) = tq.baseValue / quantityScale("d")
}