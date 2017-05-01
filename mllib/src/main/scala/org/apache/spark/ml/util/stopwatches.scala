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

package org.apache.spark.ml.util

import scala.collection.mutable

import org.apache.spark.SparkContext
import org.apache.spark.util.LongAccumulator

/**
 * Abstract class for stopwatches.
 */
private[spark] abstract class Stopwatch extends Serializable {

  @transient private var running: Boolean = false
  private var startTime: Long = _

  /**
   * Name of the stopwatch.
   */
  val name: String

  /**
   * Starts the stopwatch.
   * Throws an exception if the stopwatch is already running.
   */
  def start(): Unit = {
    assume(!running, "start() called but the stopwatch is already running.")
    running = true
    startTime = now
  }

  /**
   * Stops the stopwatch and returns the duration of the last session in milliseconds.
   * Throws an exception if the stopwatch is not running.
   */
  def stop(): Long = {
    assume(running, "stop() called but the stopwatch is not running.")
    val duration = now - startTime
    add(duration)
    running = false
    duration
  }

  /**
   * Checks whether the stopwatch is running.
   */
  def isRunning: Boolean = running

  /**
   * Returns total elapsed time in milliseconds, not counting the current session if the stopwatch
   * is running.
   */
  def elapsed(): Long

  override def toString: String = s"$name: ${elapsed()}ms"

  /**
   * Gets the current time in milliseconds.
   */
  protected def now: Long = System.currentTimeMillis()

  /**
   * Adds input duration to total elapsed time.
   */
  protected def add(duration: Long): Unit
}

/**
 * A local [[Stopwatch]].
 */
private[spark] class LocalStopwatch(override val name: String) extends Stopwatch {

  private var elapsedTime: Long = 0L

  override def elapsed(): Long = elapsedTime

  override protected def add(duration: Long): Unit = {
    elapsedTime += duration
  }
}

/**
 * A distributed [[Stopwatch]] using Spark accumulator.
 * @param sc SparkContext
 */
private[spark] class DistributedStopwatch(
    sc: SparkContext,
    override val name: String) extends Stopwatch {

  private val elapsedTime: LongAccumulator = sc.longAccumulator(s"DistributedStopwatch($name)")

  override def elapsed(): Long = elapsedTime.value

  override protected def add(duration: Long): Unit = {
    elapsedTime.add(duration)
  }
}

/**
 * A multiple stopwatch that contains local and distributed stopwatches.
 * @param sc SparkContext
 */
private[spark] class MultiStopwatch(@transient private val sc: SparkContext) extends Serializable {

  private val stopwatches: mutable.Map[String, Stopwatch] = mutable.Map.empty

  /**
   * Adds a local stopwatch.
   * @param name stopwatch name
   */
  def addLocal(name: String): this.type = {
    require(!stopwatches.contains(name), s"Stopwatch with name $name already exists.")
    stopwatches(name) = new LocalStopwatch(name)
    this
  }

  /**
   * Adds a distributed stopwatch.
   * @param name stopwatch name
   */
  def addDistributed(name: String): this.type = {
    require(!stopwatches.contains(name), s"Stopwatch with name $name already exists.")
    stopwatches(name) = new DistributedStopwatch(sc, name)
    this
  }

  /**
   * Gets a stopwatch.
   * @param name stopwatch name
   */
  def apply(name: String): Stopwatch = stopwatches(name)

  override def toString: String = {
    stopwatches.values.toArray.sortBy(_.name)
      .map(c => s"  $c")
      .mkString("{\n", ",\n", "\n}")
  }
}
