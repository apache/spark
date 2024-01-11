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

/**
 * Similar usage as StatefulProcessor. Represents the arbitrary stateful logic that needs to
 * be provided by the user to perform stateful manipulations on keyed streams.
 * Accepts a user-defined type as initial state to be initialized in the first batch.
 */
@Experimental
@Evolving
trait StatefulProcessorWithInitialState[K, I, O, S] extends StatefulProcessor[K, I, O] {

  /**
   * Function that will be invoked only in the first batch for users to process initial states.
   *
   * @param key - grouping key
   * @param initialState - A row in the initial state to be processed
   */
  def handleInitialState(key: K, initialState: S): Unit
}

