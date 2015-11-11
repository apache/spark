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

package org.apache.spark.streaming.api.java

import com.google.common.base.Optional

import org.apache.spark.annotation.Experimental
import org.apache.spark.streaming.State

/**
 * :: Experimental ::
 * Class for getting and updating the tracked state in the `trackStateByKey` operation of a
 * [[JavaPairDStream]].
 *
 * Example of using `State`:
 * {{{
 *    // A tracking function that maintains an integer state and return a String
 *   Function2<Optional<Integer>, JavaState<Integer>, Optional<String>> trackStateFunc =
 *       new Function2<Optional<Integer>, JavaState<Integer>, Optional<String>>() {
 *
 *         @Override
 *         public Optional<String> call(Optional<Integer> one, JavaState<Integer> state) {
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
 */
@Experimental
final class JavaState[S](state: State[S]) extends Serializable {

  /** Whether the state already exists */
  def exists(): Boolean = state.exists()

  /**
   * Get the state if it exists, otherwise it will throw `java.util.NoSuchElementException`.
   * Check with `exists()` whether the state exists or not before calling `get()`.
   *
   * @throws java.util.NoSuchElementException If the state does not exist.
   */
  def get(): S = state.get()

  /**
   * Update the state with a new value.
   *
   * State cannot be updated if it has been already removed (that is, `remove()` has already been
   * called) or it is going to be removed due to timeout (that is, `isTimingOut()` is `true`).
   *
   * @throws java.lang.IllegalArgumentException If the state has already been removed, or is
   *                                            going to be removed
   */
  def update(newState: S): Unit = state.update(newState)

  /**
   * Remove the state if it exists.
   *
   * State cannot be updated if it has been already removed (that is, `remove()` has already been
   * called) or it is going to be removed due to timeout (that is, `isTimingOut()` is `true`).
   */
  def remove(): Unit = state.remove()

  /**
   * Whether the state is timing out and going to be removed by the system after the current batch.
   * This timeout can occur if timeout duration has been specified in the
   * [[org.apache.spark.streaming.StateSpec StatSpec]] and the key has not received any new data
   * for that timeout duration.
   */
  def isTimingOut(): Boolean = state.isTimingOut()

  /**
   * Get the state as an `Optional`. It will be `Optional.of(state)` if it exists, otherwise
   * `Optional.absent()`.
   */
  def getOption(): Optional[S] = if (exists) Optional.of(get) else Optional.absent()

  override def toString(): String = state.toString()
}
