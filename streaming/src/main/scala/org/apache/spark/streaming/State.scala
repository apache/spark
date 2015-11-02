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

/**
 * Abstract class for getting and updating the tracked state in the `trackStateByKey` operation of
 * [[org.apache.spark.streaming.dstream.PairDStreamFunctions pair DStream]] and
 * [[org.apache.spark.streaming.api.java.JavaPairDStream]].
 * {{{
 *
 * }}}
 */
sealed abstract class State[S] {
  
  /** Whether the state already exists */
  def exists(): Boolean
  
  /**
   * Get the state if it exists, otherwise wise it will throw an exception.
   * Check with `exists()` whether the state exists or not before calling `get()`.
   */
  def get(): S

  /**
   * Update the state with a new value. Note that you cannot update the state if the state is
   * timing out (that is, `isTimingOut() return true`, or if the state has already been removed by
   * `remove()`.
   */
  def update(newState: S): Unit

  /** Remove the state if it exists. */
  def remove(): Unit

  /** Is the state going to be timed out by the system after this batch interval */
  def isTimingOut(): Boolean

  @inline final def getOption(): Option[S] = Option(get())

  /** Get the state if it exists, otherwise return the default value */
  @inline final def getOrElse[S1 >: S](default: => S1): S1 = {
    if (exists) this.get else default
  }

  @inline final override def toString() = getOption.map { _.toString }.getOrElse("<state not set>")
}

/** Internal implementation of the [[State]] interface */
private[streaming] class StateImpl[S] extends State[S] {

  private var state: S = null.asInstanceOf[S]
  private var defined: Boolean = true
  private var timingOut: Boolean = false
  private var updated: Boolean = false
  private var removed: Boolean = false

  // ========= Public API =========
  def exists(): Boolean = {
    defined
  }

  def get(): S = {
    state
  }

  def update(newState: S): Unit = {
    require(!removed, "Cannot update the state after it has been removed")
    require(!timingOut, "Cannot update the state that is timing out")
    state = newState
    updated = true
  }

  def isTimingOut(): Boolean = {
    timingOut
  }

  def remove(): Unit = {
    require(!timingOut, "Cannot remove the state that is timing out")
    require(!removed, "Cannot remove the state that has already been removed")
    removed = true
  }

  // ========= Internal API =========

  /** Whether the state has been marked for removing */
  def isRemoved(): Boolean = {
    removed
  }

  /** Whether the state has been been updated */
  def isUpdated(): Boolean = {
    updated
  }

  /**
   * Internal method to update the state data and reset internal flags in `this`.
   * This method allows `this` object to be reused across many state records.
   */
  def wrap(optionalState: Option[S]): Unit = {
    optionalState match {
      case Some(newState) =>
        this.state = newState
        defined = true

      case None =>
        this.state = null.asInstanceOf[S]
        defined = false
    }
    timingOut = false
    removed = false
    updated = false
  }

  /**
   * Internal method to update the state data and reset internal flags in `this`.
   * This method allows `this` object to be reused across many state records.
   */
  def wrapTiminoutState(newState: S): Unit = {
    this.state = newState
    defined = true
    timingOut = true
    removed = false
    updated = false
  }
}
