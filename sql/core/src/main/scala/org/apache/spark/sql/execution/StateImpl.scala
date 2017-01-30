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

package org.apache.spark.sql.execution

import org.apache.spark.sql.State

/** Internal implementation of the [[State]] interface */
private[sql] class StateImpl[S](optionalValue: Option[S]) extends State[S] {
  private var value: S = optionalValue.getOrElse(null.asInstanceOf[S])
  private var defined: Boolean = optionalValue.isDefined
  private var updated: Boolean = false  // whether value has been updated (but not removed)
  private var removed: Boolean = false  // whether value has eben removed

  // ========= Public API =========
  override def exists: Boolean = {
    defined
  }

  override def get(): S = {
    if (defined) {
      value
    } else {
      throw new NoSuchElementException("State is either not defined or has already been removed")
    }
  }

  override def update(newValue: S): Unit = {
    value = newValue
    defined = true
    updated = true
    removed = false
  }

  override def remove(): Unit = {
    defined = false
    updated = false
    removed = true
  }

  // ========= Internal API =========

  /** Whether the state has been marked for removing */
  def isRemoved: Boolean = {
    removed
  }

  /** Whether the state has been been updated */
  def isUpdated: Boolean = {
    updated
  }
}

object StateImpl {
  def apply[S](optionalValue: Option[S]): StateImpl[S] = new StateImpl[S](optionalValue)
}
