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

package org.apache.spark.sql.types

import java.util.{Calendar, TimeZone}

import org.apache.spark.sql.catalyst.expressions.Cast

/**
 * A mutable implementation of [[java.sql.Date]] that holds an [[Int]] for days since epoch.
 *
 * An [[Int]] only takes 2 bytes, while [[java.sql.Date]] will take 4 bytes.
 * According to https://docs.oracle.com/javase/7/docs/api/java/sql/Date.html
 * To conform with the definition of SQL DATE, the millisecond values wrapped by a java.sql.Date
 * instance must be 'normalized' by setting the hours, minutes, seconds, and milliseconds to zero
 * in the particular time zone with which the instance is associated.
 * While user can create whatever [[java.sql.Date]] instance to a certain milliseconds, this
 * would lead to some potential problems. Also, the built-in comparing method will compare
 * milliseconds, which is not right. So we provide our own Date type here.
 */
final class Date extends Ordered[Date] with Serializable {
  private var daysSinceEpoch: Int = 0

  /**
   * Set this Date to the given Int (days since 1970-01-01).
   */
  def set(days: Int): Date = {
    this.daysSinceEpoch = days
    this
  }

  /**
   * Get the Int value of days since 1970-01-01.
   */
  def toDays: Int = {
    this.daysSinceEpoch
  }

  /**
   * get the corresponding java.sql.Date value of this Date object.
   */
  def toJavaDate: java.sql.Date = {
    new java.sql.Date(toMillisSinceEpoch)
  }

  override def toString: String = Cast.threadLocalDateFormat.get.format(toJavaDate)

  def toMillisSinceEpoch: Long = {
    val millisUtc = daysSinceEpoch.toLong * Date.MILLIS_PER_DAY
    millisUtc - Date.LOCAL_TIMEZONE.get().getOffset(millisUtc)
  }

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
