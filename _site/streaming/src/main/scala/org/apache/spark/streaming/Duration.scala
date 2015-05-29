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

package org.apache.spark.streaming

import org.apache.spark.util.Utils

case class Duration (private val millis: Long) {

  def < (that: Duration): Boolean = (this.millis < that.millis)

  def <= (that: Duration): Boolean = (this.millis <= that.millis)

  def > (that: Duration): Boolean = (this.millis > that.millis)

  def >= (that: Duration): Boolean = (this.millis >= that.millis)

  def + (that: Duration): Duration = new Duration(millis + that.millis)

  def - (that: Duration): Duration = new Duration(millis - that.millis)

  def * (times: Int): Duration = new Duration(millis * times)

  def / (that: Duration): Double = millis.toDouble / that.millis.toDouble

  // Java-friendlier versions of the above.

  def less(that: Duration): Boolean = this < that

  def lessEq(that: Duration): Boolean = this <= that

  def greater(that: Duration): Boolean = this > that

  def greaterEq(that: Duration): Boolean = this >= that

  def plus(that: Duration): Duration = this + that

  def minus(that: Duration): Duration = this - that

  def times(times: Int): Duration = this * times

  def div(that: Duration): Double = this / that

  def isMultipleOf(that: Duration): Boolean =
    (this.millis % that.millis == 0)

  def min(that: Duration): Duration = if (this < that) this else that

  def max(that: Duration): Duration = if (this > that) this else that

  def isZero: Boolean = (this.millis == 0)

  override def toString: String = (millis.toString + " ms")

  def toFormattedString: String = millis.toString

  def milliseconds: Long = millis

  def prettyPrint: String = Utils.msDurationToString(millis)

}

/**
 * Helper object that creates instance of [[org.apache.spark.streaming.Duration]] representing
 * a given number of milliseconds.
 */
object Milliseconds {
  def apply(milliseconds: Long): Duration = new Duration(milliseconds)
}

/**
 * Helper object that creates instance of [[org.apache.spark.streaming.Duration]] representing
 * a given number of seconds.
 */
object Seconds {
  def apply(seconds: Long): Duration = new Duration(seconds * 1000)
}

/**
 * Helper object that creates instance of [[org.apache.spark.streaming.Duration]] representing
 * a given number of minutes.
 */
object Minutes {
  def apply(minutes: Long): Duration = new Duration(minutes * 60000)
}

// Java-friendlier versions of the objects above.
// Named "Durations" instead of "Duration" to avoid changing the case class's implied API.

object Durations {

  /**
   * @return [[org.apache.spark.streaming.Duration]] representing given number of milliseconds.
   */
  def milliseconds(milliseconds: Long): Duration = Milliseconds(milliseconds)

  /**
   * @return [[org.apache.spark.streaming.Duration]] representing given number of seconds.
   */
  def seconds(seconds: Long): Duration = Seconds(seconds)

  /**
   * @return [[org.apache.spark.streaming.Duration]] representing given number of minutes.
   */
  def minutes(minutes: Long): Duration = Minutes(minutes)

}
