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

package org.apache.spark

trait Service extends java.io.Closeable {

  /**
   * Service states
   */
  object State extends Enumeration {

    /**
     * Constructed but not initialized
     */
    val Uninitialized = Value(0, "Uninitialized")

    /**
     * Initialized but not started or stopped
     */
    val Initialized = Value(1, "Initialized")

    /**
     * started and not stopped
     */
    val Started = Value(2, "Started")

    /**
     * stopped. No further state transitions are permitted
     */
    val Stopped = Value(3, "Stopped")

    type State = Value
  }

  def conf: SparkConf

  def initialize(): Unit

  def start(): Unit

  def stop(): Unit

  def state: State.State

}
