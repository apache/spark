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
package org.apache.spark.sql.streaming

import org.apache.spark.annotation.{Evolving, Experimental}

@Experimental
@Evolving
/**
 * Interface used for arbitrary stateful operations with the v2 API to capture list value state.
 */
private[sql] trait ListState[S] extends Serializable {

  /** Whether state exists or not. */
  def exists(): Boolean

  /** Get the state value. An empty iterator is returned if no value exists. */
  def get(): Iterator[S]

  /** Update the value of the list. */
  def put(newState: Array[S]): Unit

  /** Append an entry to the list */
  def appendValue(newState: S): Unit

  /** Append an entire list to the existing value */
  def appendList(newState: Array[S]): Unit

  /** Removes this state for the given grouping key. */
  def clear(): Unit
}
