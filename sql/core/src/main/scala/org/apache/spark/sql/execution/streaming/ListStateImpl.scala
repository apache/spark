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
package org.apache.spark.sql.execution.streaming

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.state.StateStore
import org.apache.spark.sql.streaming.ListState

/**
 * Provides concrete implementation for list of values associated with a state variable
 * used in the streaming transformWithState operator.
 *
 * @param store - reference to the StateStore instance to be used for storing state
 * @param stateName - name of logical state partition
 * @tparam S - data type of object that will be stored in the list
 */
class ListStateImpl[S](store: StateStore,
     stateName: String) extends ListState[S] with Logging {

   /** Whether state exists or not. */
   override def exists(): Boolean = {
     val stateValue = store.get(StateEncoder.encodeKey(stateName), stateName)
     stateValue != null
   }

   /** Get the state value if it exists. If the state does not exist in state store, an
    * empty iterator is returned. */
   override def get(): Iterator[S] = {
     val encodedKey = StateEncoder.encodeKey(stateName)
     val unsafeRowValuesIterator = store.valuesIterator(encodedKey, stateName)
     new Iterator[S] {
       override def hasNext: Boolean = {
         unsafeRowValuesIterator.hasNext
       }

       override def next(): S = {
         val valueUnsafeRow = unsafeRowValuesIterator.next()
         StateEncoder.decode(valueUnsafeRow)
       }
     }
   }

   /** Get the list value as an option if it exists and None otherwise. */
   override def getOption(): Option[Iterator[S]] = {
     Option(get())
   }

   /** Update the value of the list. */
   override def put(newState: Seq[S]): Unit = {
     validateNewState(newState)

     if (newState.isEmpty) {
       this.remove()
     } else {
       val encodedKey = StateEncoder.encodeKey(stateName)

       var isFirst = true
       newState.foreach { v =>
         val encodedValue = StateEncoder.encodeValue(v)
         if (isFirst) {
           store.put(encodedKey, encodedValue, stateName)
           isFirst = false
         } else {
            store.merge(encodedKey, encodedValue, stateName)
         }
       }
     }
   }

   /** Append an entry to the list. */
   override def appendValue(newState: S): Unit = {
     if (newState == null) {
       throw new IllegalArgumentException("value added to ListState should be non-null")
     }
     store.merge(StateEncoder.encodeKey(stateName),
         StateEncoder.encodeValue(newState), stateName)
   }

   /** Append an entire list to the existing value. */
   override def appendList(newState: Seq[S]): Unit = {
     validateNewState(newState)

     val encodedKey = StateEncoder.encodeKey(stateName)
     newState.foreach { v =>
       val encodedValue = StateEncoder.encodeValue(v)
       store.merge(encodedKey, encodedValue, stateName)
     }
   }

   /** Remove this state. */
   override def remove(): Unit = {
     store.remove(StateEncoder.encodeKey(stateName), stateName)
   }

   private def validateNewState(newState: Seq[S]): Unit = {
     if (newState == null) {
       throw new IllegalArgumentException("newState list should be non-null")
     }

     val containsNullElements = newState.contains(null)
     if (containsNullElements) {
       throw new IllegalArgumentException("value added to ListState should be non-null")
     }
   }
 }
