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

package org.apache.spark.sql.execution.streaming.state

import org.apache.spark.sql.streaming.State

/** Internal implementation of the [[State]] interface */
class StateImpl[S] extends State[S] {
  private var state: S = null.asInstanceOf[S]
  private var defined: Boolean = false
  private var timingOut: Boolean = false
  private var updated: Boolean = false
  private var removed: Boolean = false

  // ========= Public API =========
  override def exists(): Boolean = {
    defined
  }

  override def get(): S = {
    if (defined) {
      state
    } else {
      throw new NoSuchElementException("State is not set")
    }
  }

  override def update(newState: S): Unit = {
    require(!removed, "Cannot update the state after it has been removed")
    require(!timingOut, "Cannot update the state that is timing out")
    state = newState
    defined = true
    updated = true
  }

  override def isTimingOut(): Boolean = {
    timingOut
  }

  override def remove(): Unit = {
    require(!timingOut, "Cannot remove the state that is timing out")
    require(!removed, "Cannot remove the state that has already been removed")
    defined = false
    updated = false
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

  def wrapTimingOutState(newState: S): Unit = {
    this.state = newState
    defined = true
    timingOut = true
    removed = false
    updated = false
  }
}
