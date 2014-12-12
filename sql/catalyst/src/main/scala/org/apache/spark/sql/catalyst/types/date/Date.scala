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

package org.apache.spark.sql.catalyst.types.date

import java.util.{Calendar, TimeZone}

/**
 * A mutable implementation of java.sql.Date that holds an Int for days since epoch, like Hive.
 */
final class Date extends Ordered[Date] with Serializable {
  private var daysSinceEpoch: Int = 0

  /**
   * Set this Decimal to the given Long. Will have precision 20 and scale 0.
   */
  def set(days: Int): Date = {
    this.daysSinceEpoch = days
    this
  }

  /**
   * Set this Decimal to the given Int. Will have precision 10 and scale 0.
   */
  def set(days: Long): Date = {
    this.daysSinceEpoch = days.toInt
    this
  }

  /**
   * Set this Decimal to the given BigDecimal value, inheriting its precision and scale.
   */
  def set(date: Date): Date = {
    this.daysSinceEpoch = date.daysSinceEpoch
    this
  }

  def toDays: Int = {
    this.daysSinceEpoch
  }

  def toJavaDate: java.sql.Date = {
    new java.sql.Date(toLong)
  }

  override def toString: String = toJavaDate.toString()

  def toLong: Long = {
    val millisUtc = daysSinceEpoch.toLong * Date.MILLIS_PER_DAY
    millisUtc - Date.LOCAL_TIMEZONE.get().getOffset(millisUtc)
  }

  def toInt: Int = toLong.toInt

  def toShort: Short = toLong.toShort

  def toByte: Byte = toLong.toByte

  override def clone(): Date = new Date().set(this)

  override def compare(other: Date): Int = {
    daysSinceEpoch.compareTo(other.daysSinceEpoch)
  }

  override def equals(other: Any) = other match {
    case d: Date =>
      compare(d) == 0
    case _ =>
      false
  }

  override def hashCode(): Int = daysSinceEpoch
}

object Date {
  private val MILLIS_PER_DAY = 86400000

  // Java TimeZone has no mention of thread safety. Use thread local instance to be safe.
  private val LOCAL_TIMEZONE = new ThreadLocal[TimeZone] {
    override protected def initialValue: TimeZone = {
      Calendar.getInstance.getTimeZone
    }
  }

  private def javaDateToDays(d: java.sql.Date): Int = {
    millisToDays(d.getTime)
  }

  private def millisToDays(millisLocal: Long): Int = {
    ((millisLocal + LOCAL_TIMEZONE.get().getOffset(millisLocal)) / MILLIS_PER_DAY).toInt
  }

  def apply(value: java.sql.Date): Date = new Date().set(javaDateToDays(value))

  def apply(value: Int): Date = new Date().set(millisToDays(value))

  def apply(value: Long): Date = new Date().set(millisToDays(value))

  def apply(value: String): Date = new Date().set(javaDateToDays(java.sql.Date.valueOf(value)))

}
