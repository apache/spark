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

package org.apache.spark.sql

import org.apache.spark.annotation.{Experimental, InterfaceStability}
import org.apache.spark.sql.catalyst.plans.logical.LogicalState

/**
 * :: Experimental ::
 *
 * Wrapper class for interacting with state data in `mapGroupsWithState` and
 * `flatMapGroupsWithState` operations on
 * [[org.apache.spark.sql.KeyValueGroupedDataset KeyValueGroupedDataset]].
 *
 * @note Operations on state are not threadsafe.
 *
 * Scala example of using `State`:
 * {{{
 *    // A mapping function that maintains an integer state for string keys and returns a string.
 *    def mappingFunction(key: String, value: Iterable[Int], state: State[Int]): Option[String] = {
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
 *    // A mapping function that maintains an integer state for string keys and returns a string.
 *    Function3<String, Iterable<Integer>, State<Integer>, String> mappingFunction =
 *       new Function3<String, Iterable<Integer>, State<Integer>, String>() {
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
 *           ... // return something
 *         }
 *       };
 * }}}
 *
 * @tparam S Type of the state
 * @since 2.1.1
 */
@Experimental
@InterfaceStability.Evolving
trait State[S] extends LogicalState[S] {

  def exists: Boolean

  def get(): S

  def update(newState: S): Unit

  def remove(): Unit

  @inline final def getOption(): Option[S] = if (exists) Some(get()) else None

  @inline final override def toString(): String = {
    getOption.map { _.toString }.getOrElse("<undefined>")
  }
}
