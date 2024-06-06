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

import org.apache.spark.annotation.Experimental

/**
 * :: Experimental ::
 * Abstract class for getting and updating the state in mapping function used in the `mapWithState`
 * operation of a [[org.apache.spark.streaming.dstream.PairDStreamFunctions pair DStream]] (Scala)
 * or a [[org.apache.spark.streaming.api.java.JavaPairDStream JavaPairDStream]] (Java).
 *
 * Scala example of using `State`:
 * {{{
 *    // A mapping function that maintains an integer state and returns a String
 *    def mappingFunction(key: String, value: Option[Int], state: State[Int]): Option[String] = {
 *      // Check if state exists
 *      if (state.exists) {
 *        val existingState = state.get  // Get the existing state
 *        val shouldRemove = ...         // Decide whether to remove the state
 *        if (shouldRemove) {
 *          state.remove()     // Remove the state
 *        } else {
 *          val newState = ...
 *          state.update(newState)    // Set the new state
 *        }
 *      } else {
 *        val initialState = ...
 *        state.update(initialState)  // Set the initial state
 *      }
 *      ... // return something
 *    }
 *
 * }}}
 *
 * Java example of using `State`:
 * {{{
 *    // A mapping function that maintains an integer state and returns a String
 *    Function3<String, Optional<Integer>, State<Integer>, String> mappingFunction =
 *       new Function3<String, Optional<Integer>, State<Integer>, String>() {
 *
 *         @Override
 *         public String call(String key, Optional<Integer> value, State<Integer> state) {
 *           if (state.exists()) {
 *             int existingState = state.get(); // Get the existing state
 *             boolean shouldRemove = ...; // Decide whether to remove the state
 *             if (shouldRemove) {
 *               state.remove(); // Remove the state
 *             } else {
 *               int newState = ...;
 *               state.update(newState); // Set the new state
 *             }
 *           } else {
 *             int initialState = ...; // Set the initial state
 *             state.update(initialState);
 *           }
 *           // return something
 *         }
 *       };
 * }}}
 *
 * @tparam S Class of the state
 */
@Experimental
sealed abstract class State[S] {

  /** Whether the state already exists */
  def exists(): Boolean

  /**
   * Get the state if it exists, otherwise it will throw `java.util.NoSuchElementException`.
   * Check with `exists()` whether the state exists or not before calling `get()`.
   *
   * @throws java.util.NoSuchElementException If the state does not exist.
   */
  def get(): S

  /**
   * Update the state with a new value.
   *
   * State cannot be updated if it has been already removed (that is, `remove()` has already been
   * called) or it is going to be removed due to timeout (that is, `isTimingOut()` is `true`).
   *
   * @throws java.lang.IllegalArgumentException If the state has already been removed, or is
   *                                            going to be removed
   */
  def update(newState: S): Unit

  /**
   * Remove the state if it exists.
   *
   * State cannot be updated if it has been already removed (that is, `remove()` has already been
   * called) or it is going to be removed due to timeout (that is, `isTimingOut()` is `true`).
   */
  def remove(): Unit

  /**
   * Whether the state is timing out and going to be removed by the system after the current batch.
   * This timeout can occur if timeout duration has been specified in the
   * [[org.apache.spark.streaming.StateSpec StatSpec]] and the key has not received any new data
   * for that timeout duration.
   */
  def isTimingOut(): Boolean

  /**
   * Get the state as a `scala.Option`. It will be `Some(state)` if it exists, otherwise `None`.
   */
  @inline final def getOption(): Option[S] = if (exists()) Some(get()) else None

  @inline final override def toString(): String = {
    getOption().map { _.toString }.getOrElse("<state not set>")
  }
}

/** Internal implementation of the [[State]] interface */
private[streaming] class StateImpl[S] extends State[S] {

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

  /** Whether the state has been updated */
  def isUpdated(): Boolean = {
    updated
  }

  /**
   * Update the internal data and flags in `this` to the given state option.
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
   * Update the internal data and flags in `this` to the given state that is going to be timed out.
   * This method allows `this` object to be reused across many state records.
   */
  def wrapTimingOutState(newState: S): Unit = {
    this.state = newState
    defined = true
    timingOut = true
    removed = false
    updated = false
  }
}
